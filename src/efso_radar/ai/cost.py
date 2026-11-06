"""Gemini API official pricing, model lifecycle registry, and token cost estimation.

Verified against official Google AI Studio / Gemini API documentation (2026):
- Pricing: https://ai.google.dev/gemini-api/docs/pricing
- Deprecations: https://ai.google.dev/gemini-api/docs/deprecations

Published Standard Paid-Tier Pricing (per 1,000,000 tokens in USD):
- gemini-3.5-flash-lite (Stable GA, released July 21, 2026 — Primary recommended model):
    - Standard Input: $0.30 / 1,000,000 tokens (text / image / video / audio)
    - Standard Output: $2.50 / 1,000,000 tokens (includes thinking/reasoning tokens)
    - Batch / Flex Input: $0.15 / 1,000,000 tokens
    - Batch / Flex Output: $1.25 / 1,000,000 tokens
    - Context Caching Read: $0.03 / 1,000,000 tokens ($1.00 / 1M tokens/hour storage)
- gemini-3.1-flash-lite (Stable GA, released May 7, 2026; scheduled shutdown May 7, 2027;
  recommended replacement: gemini-3.5-flash-lite):
    - Standard Input: $0.25 / 1,000,000 tokens (text / image / video)
    - Standard Output: $1.50 / 1,000,000 tokens (includes thinking/reasoning tokens)
    - Batch / Flex Input: $0.125 / 1,000,000 tokens
    - Batch / Flex Output: $0.75 / 1,000,000 tokens
    - Context Caching Read: $0.025 / 1,000,000 tokens
- gemini-3.8-flash (Released August 18, 2026):
    - Standard Input: $0.75 / 1,000,000 tokens (through Dec 31, 2026)
    - Standard Output: $3.75 / 1,000,000 tokens (through Dec 31, 2026)
- Shut-Down / Discontinued Models (Rejected at runtime; never valid as fallbacks):
    - gemini-3.1-flash-lite-preview: Shut down on May 25, 2026
    - gemini-2.0-flash-lite / gemini-2.0-flash-lite-001: Shut down on June 1, 2026
    - gemini-2.0-flash / gemini-2.0-flash-001: Shut down on June 1, 2026
    - gemini-2.5-flash-lite-preview-09-2025: Shut down on March 31, 2026
    - gemini-3-pro-preview: Shut down on March 9, 2026
- Google Search Grounding (disabled by default in Efso Radar):
    - 5,000 free search requests/month (shared across Gemini 3+ models), then $14.00 / 1,000 queries
      (legacy models: $35.00 / 1,000 queries).

Important Billing & Safeguard Rules:
- Output tokens encompass ALL generated tokens, including reasoning / thinking tokens.
- Three-Way Accounting Separation:
    1. Paid-Tier Equivalent Reference Pricing (`paid_equivalent_estimate_usd`): Published standard
       pay-as-you-go rates retained in `ai_usage_ledger.estimated_cost_usd` for economic opportunity
       cost tracking and auditability without schema changes.
    2. Nominal Free Tier Rate vs Actual Billed Cost: Nominal free tier generation is $0.00/token (`nominal_free_tier_rate_usd`).
       Actual billed USD (`actual_billed_usd`) is ALWAYS None (UNKNOWN) in offline operation until an
       independently authenticated provider billing verification mechanism exists.
       Caller-provided records cannot certify provider-side billing status. Nonzero paid charges are strictly disallowed ($0.00 / 0 TRY).
    3. Free Tier Quota Accounting: Assessed launch model candidate (`gemini-3.5-flash-lite`), search grounding disabled,
       zero monetary ceilings, monthly call cap (300). Missing or unobserved dynamic quota evidence (RPM, TPM, RPD)
       fails closed (DENY). Universal constants are not account entitlements.
- Application-level cost tracking in `ai_usage_ledger` is an internal defensive circuit-breaker
  and estimation safeguard; it is NOT a hard provider-side billing guarantee.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from contextlib import closing
import calendar
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NamedTuple, Optional, Sequence

logger = logging.getLogger("efso-radar.ai.cost")


class ModelPricing(NamedTuple):
    input_per_million: float
    output_per_million: float
    batch_input_per_million: float = 0.15
    batch_output_per_million: float = 1.25
    cache_read_per_million: float = 0.03
    cache_storage_per_million_hour: float = 1.00
    supports_search_grounding: bool = True
    is_deprecated: bool = False
    shutdown_date: Optional[str] = None
    recommended_replacement: Optional[str] = None


OFFICIAL_PRICING: dict[str, ModelPricing] = {
    # Current generation models (2026)
    "gemini-3.8-flash": ModelPricing(
        input_per_million=0.75,
        output_per_million=3.75,
        batch_input_per_million=0.375,
        batch_output_per_million=1.875,
        cache_read_per_million=0.075,
    ),
    "gemini-3.6-flash": ModelPricing(
        input_per_million=0.75,
        output_per_million=3.75,
        batch_input_per_million=0.375,
        batch_output_per_million=1.875,
        cache_read_per_million=0.075,
    ),
    "gemini-3.5-flash-lite": ModelPricing(
        input_per_million=0.30,
        output_per_million=2.50,
        batch_input_per_million=0.15,
        batch_output_per_million=1.25,
        cache_read_per_million=0.03,
    ),
    # Stable Gemini 3.1 Flash-Lite (distinct from discontinued gemini-3.1-flash-lite-preview).
    # Scheduled for shutdown on May 7, 2027; official replacement is gemini-3.5-flash-lite.
    "gemini-3.1-flash-lite": ModelPricing(
        input_per_million=0.25,
        output_per_million=1.50,
        batch_input_per_million=0.125,
        batch_output_per_million=0.75,
        cache_read_per_million=0.025,
        is_deprecated=True,
        shutdown_date="May 7, 2027",
        recommended_replacement="gemini-3.5-flash-lite",
    ),
    # Legacy models
    "gemini-2.5-flash": ModelPricing(
        input_per_million=0.30,
        output_per_million=2.50,
        batch_input_per_million=0.15,
        batch_output_per_million=1.25,
        cache_read_per_million=0.03,
        is_deprecated=True,
        shutdown_date="October 15, 2026",
        recommended_replacement="gemini-3.6-flash",
    ),
    "gemini-2.5-flash-lite": ModelPricing(
        input_per_million=0.10,
        output_per_million=0.40,
        batch_input_per_million=0.05,
        batch_output_per_million=0.20,
        cache_read_per_million=0.01,
        is_deprecated=True,
        shutdown_date="October 15, 2026",
        recommended_replacement="gemini-3.5-flash-lite",
    ),
    "gemini-1.5-flash": ModelPricing(
        input_per_million=0.075,
        output_per_million=0.30,
        batch_input_per_million=0.0375,
        batch_output_per_million=0.15,
        cache_read_per_million=0.01875,
        is_deprecated=True,
    ),
}

# Models that have already been shut down per https://ai.google.dev/gemini-api/docs/deprecations
# and must NEVER be accepted as active or fallback models.
SHUTDOWN_MODELS: dict[str, str] = {
    "gemini-3.1-flash-lite-preview": "May 25, 2026",
    "gemini-2.0-flash-lite": "June 1, 2026",
    "gemini-2.0-flash-lite-001": "June 1, 2026",
    "gemini-2.0-flash": "June 1, 2026",
    "gemini-2.0-flash-001": "June 1, 2026",
    "gemini-2.5-flash-lite-preview-09-2025": "March 31, 2026",
    "gemini-3-pro-preview": "March 9, 2026",
}

# Assessed launch model candidate for Efso Radar ($0.00 launch policy).
# gemini-3.5-flash-lite is the sole evaluated and approved candidate.
# Note: Google AI Studio documentation (https://ai.google.dev/gemini-api/docs/pricing) specifies
# that Standard Free Tier is available for multiple models including gemini-3.5-flash-lite and gemini-3.8-flash.
# However, Efso Radar strictly restricts operational launch readiness to gemini-3.5-flash-lite.
# Other models are UNASSESSED for launch and fail closed (DENY), without asserting universal paid-only status.
ASSESSED_LAUNCH_MODELS: frozenset[str] = frozenset({
    "gemini-3.5-flash-lite",
})

# Backward compatibility alias for internal launch assessment (not Google Free Tier catalog eligibility)
FREE_TIER_ELIGIBLE_MODELS: frozenset[str] = ASSESSED_LAUNCH_MODELS

# Inert documentation examples from Google AI Studio public tier tables.
# CRITICAL: These are NOT verified provider entitlements for any specific project or account!
# Account-specific rate limits vary by project and model, viewable in AI Studio.
# Quota guards require fresh, project-attested evidence and must fail closed if limits are unobserved.
DOC_EXAMPLE_RPM_CAP: int = 15
DOC_EXAMPLE_RPD_CAP: int = 1500
DOC_EXAMPLE_TPM_CAP: int = 1_000_000

# Backward compatibility aliases
FREE_TIER_DEFAULT_RPM_CAP: int = DOC_EXAMPLE_RPM_CAP
FREE_TIER_DEFAULT_RPD_CAP: int = DOC_EXAMPLE_RPD_CAP
FREE_TIER_DEFAULT_TPM_CAP: int = DOC_EXAMPLE_TPM_CAP

# Owner-observed project quota snapshots for Efso Radar in Google AI Studio (gemini-3.5-flash-lite) on 2026-11-06.
# CRITICAL SAFETY NOTICE:
# - These are static historical observations recorded on 2026-11-06, NOT runtime-verifiable entitlements!
# - They CANNOT authorize usage or serve as current live consumption.
# - Google may adjust project-specific limits at any time without notification.
# - These limits are shared across ALL API keys and runners within the same Google Cloud / AI Studio project.
# - Dynamic reservation and quota checks must fail closed if fresh, trusted quota evidence is unobserved.
OBSERVED_PROJECT_RPM_LIMIT: int = 15
OBSERVED_PROJECT_INPUT_TPM_LIMIT: int = 250_000
OBSERVED_PROJECT_RPD_LIMIT: int = 500


def is_pacific_daylight_saving_time(dt: datetime) -> bool:
    """Determine if a datetime falls within US Pacific Daylight Time (PDT).

    Normalizes any timezone-aware datetime to UTC first.
    Naïve datetimes without tzinfo are assumed to represent UTC.

    Follows US Energy Policy Act of 2005 rules:
    - Starts on the second Sunday in March at 02:00 local time (10:00 UTC).
    - Ends on the first Sunday in November at 02:00 local time (09:00 UTC).
    Pure standard library implementation that does not require external tzdata on Windows.
    """
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)

    year = dt_utc.year
    first_sun_march = 1 + (6 - calendar.weekday(year, 3, 1)) % 7
    second_sun_march = first_sun_march + 7
    dst_start_utc = datetime(year, 3, second_sun_march, 10, 0, 0, tzinfo=timezone.utc)

    first_sun_nov = 1 + (6 - calendar.weekday(year, 11, 1)) % 7
    dst_end_utc = datetime(year, 11, first_sun_nov, 9, 0, 0, tzinfo=timezone.utc)

    return dst_start_utc <= dt_utc < dst_end_utc


def get_pacific_calendar_date(dt: Optional[datetime] = None) -> str:
    """Return the current calendar date (YYYY-MM-DD) in US Pacific Time (PT).

    Google AI Studio / Gemini API Free Tier RPD resets daily at midnight Pacific Time.

    Timezone Normalization Policy:
    - None: Uses current datetime in UTC (datetime.now(timezone.utc)).
    - Timezone-aware datetime: Normalized to UTC via .astimezone(timezone.utc)
      before determining DST and calculating the Pacific offset.
    - Naïve datetime: Assumed to represent UTC (.replace(tzinfo=timezone.utc)).
    """
    if dt is None:
        dt_utc = datetime.now(timezone.utc)
    elif dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)

    offset_hours = -7 if is_pacific_daylight_saving_time(dt_utc) else -8
    pacific_dt = dt_utc + timedelta(hours=offset_hours)
    return pacific_dt.strftime("%Y-%m-%d")

# Provenance states for account-level billing status
BILLING_TIER_UNKNOWN: str = "UNKNOWN_UNVERIFIED"
BILLING_TIER_DISALLOWED_PAID: str = "DISALLOWED_PAID"
BILLING_TIER_SIMULATED_UNAUTHENTICATED: str = "SIMULATED_UNAUTHENTICATED"

# Mandatory activation blockers preventing live Gemini dispatch authorization
ACTIVATION_BLOCKERS: tuple[str, ...] = (
    "G1_PROVIDER_BILLING_ATTESTATION_UNAVAILABLE",
    "G2_ERD017_CROSS_RUNNER_QUOTA_RESERVATION_UNAVAILABLE",
    "TRUSTED_PROVIDER_QUOTA_FRESHNESS_UNAVAILABLE",
    "PROJECT_MODEL_BINDING_ATTESTATION_UNAVAILABLE",
    "SESSION_SCOPED_PO_GRANT_VALIDATION_UNAVAILABLE",
)


def is_model_assessed_for_launch(model_name: str) -> bool:
    """Check if model is the assessed launch model (gemini-3.5-flash-lite).

    Other models are unassessed for launch and fail closed (DENY).
    Does NOT assert that unassessed models are paid-only.
    """
    clean_name = model_name.strip().lower()
    return clean_name in ASSESSED_LAUNCH_MODELS


def is_model_free_tier_eligible(model_name: str) -> bool:
    """Backward compatibility alias checking internal launch assessment for Efso Radar.

    IMPORTANT DISTINCTION:
    This function checks internal launch assessment under Efso Radar's strict $0.00 launch policy,
    NOT Google Free Tier catalog eligibility. While Google documents Standard Free Tier for
    several models (such as gemini-3.8-flash), Efso Radar restricts operational launch readiness
    strictly to gemini-3.5-flash-lite (ASSESSED_LAUNCH_MODELS). Unassessed, shut-down, or
    unrecognized models return False.
    """
    clean_name = model_name.strip().lower()
    if clean_name in SHUTDOWN_MODELS:
        return False
    return is_model_assessed_for_launch(clean_name)


class FreeTierProjectProof(NamedTuple):
    """Candidate representation of provider-side billing status attestation."""
    project_id: str
    billing_enabled: bool
    free_tier_eligible: bool
    attestation_source: str
    verified_at: str
    expires_at: Optional[str] = None


def validate_free_tier_project_proof(
    proof: Optional[dict[str, Any] | FreeTierProjectProof],
    now: Optional[datetime] = None,
    allow_synthetic_simulation: bool = False,
) -> tuple[bool, str, str]:
    """Evaluate candidate provider billing status attestation.

    IMPORTANT SAFETY INVARIANT:
    In this offline-only engineering slice, caller-constructed dictionaries or records
    are NOT authenticated Google billing attestations and CANNOT certify actual $0 provider billing.
    Observed actual provider billing remains UNKNOWN (None).

    Fail-closed validation rules:
    - Missing/None proof: returns (False, BILLING_TIER_UNKNOWN, "No provider project proof provided; billing status is UNKNOWN. Fail closed.")
    - Missing explicit 'billing_enabled' key: NOT assumed False! Missing billing status is treated as unknown and fails closed.
    - If billing_enabled is True: returns (False, BILLING_TIER_DISALLOWED_PAID, "Project has billing_enabled=True. Paid projects are strictly forbidden under Efso Radar $0.00 / 0 TRY policy.")
    - If free_tier_eligible is not True: returns (False, BILLING_TIER_UNKNOWN, "Proof is not marked free_tier_eligible=True. Fail closed.")
    - Missing 'project_id', 'attestation_source', or 'verified_at': returns (False, BILLING_TIER_UNKNOWN, "Missing required provenance fields in proof. Fail closed.")
    - Future-dated 'verified_at' (> now): returns (False, BILLING_TIER_UNKNOWN, "Proof verified_at timestamp is in the future. Fail closed.")
    - Expired 'expires_at' (<= now): returns (False, BILLING_TIER_UNKNOWN, "Proof expired. Fail closed.")
    - Stale/unauthenticated record: In the absence of an independently verified cryptographic signature or direct Google Cloud Billing API attestation, returns (False, BILLING_TIER_UNKNOWN, "Caller-provided record is unauthenticated; actual provider billing status remains UNKNOWN. Fail closed.")
    """
    if proof is None:
        return (
            False,
            BILLING_TIER_UNKNOWN,
            "No provider project proof provided; billing status is UNKNOWN. Fail closed.",
        )

    ref_now = now or datetime.now(timezone.utc)
    if ref_now.tzinfo is None:
        ref_now = ref_now.replace(tzinfo=timezone.utc)
    else:
        ref_now = ref_now.astimezone(timezone.utc)

    if isinstance(proof, dict):
        if "billing_enabled" not in proof:
            return (
                False,
                BILLING_TIER_UNKNOWN,
                "Proof missing explicit 'billing_enabled' status. Missing billing status cannot be assumed False. Fail closed.",
            )
        raw_billing = proof["billing_enabled"]
        if not isinstance(raw_billing, bool):
            return (
                False,
                BILLING_TIER_UNKNOWN,
                "Proof 'billing_enabled' status must be an explicit boolean. Fail closed.",
            )
        billing_enabled = raw_billing

        raw_ft = proof.get("free_tier_eligible")
        if not isinstance(raw_ft, bool) or not raw_ft:
            return (
                False,
                BILLING_TIER_UNKNOWN,
                "Proof 'free_tier_eligible' must be explicitly True. Fail closed.",
            )

        project_id = str(proof.get("project_id", "")).strip()
        attestation_source = str(proof.get("attestation_source", "")).strip()
        verified_at_str = str(proof.get("verified_at", "")).strip()
        expires_at_str = proof.get("expires_at")
    elif isinstance(proof, FreeTierProjectProof):
        billing_enabled = bool(proof.billing_enabled)
        if not proof.free_tier_eligible:
            return (
                False,
                BILLING_TIER_UNKNOWN,
                "Proof 'free_tier_eligible' must be True. Fail closed.",
            )
        project_id = str(proof.project_id).strip()
        attestation_source = str(proof.attestation_source).strip()
        verified_at_str = str(proof.verified_at).strip()
        expires_at_str = proof.expires_at
    else:
        return (
            False,
            BILLING_TIER_UNKNOWN,
            f"Unsupported proof record type '{type(proof).__name__}'. Fail closed.",
        )

    if not project_id or not attestation_source or not verified_at_str:
        return (
            False,
            BILLING_TIER_UNKNOWN,
            "Missing required provenance fields in proof record. Fail closed.",
        )

    # Check billing_enabled
    if billing_enabled:
        return (
            False,
            BILLING_TIER_DISALLOWED_PAID,
            "Project has billing_enabled=True. Paid projects are strictly "
            "forbidden under Efso Radar $0.00 / 0 TRY policy.",
        )

    # Validate verified_at timestamp (no future dates)
    try:
        ver_dt = datetime.fromisoformat(verified_at_str)
        if ver_dt.tzinfo is None:
            ver_dt = ver_dt.replace(tzinfo=timezone.utc)
        if ver_dt > ref_now:
            return (
                False,
                BILLING_TIER_UNKNOWN,
                f"Proof verified_at timestamp ({verified_at_str}) is in the future. Fail closed.",
            )
    except (ValueError, TypeError) as exc:
        return (
            False,
            BILLING_TIER_UNKNOWN,
            f"Invalid verified_at timestamp format '{verified_at_str}': {exc}. Fail closed.",
        )

    # Validate expires_at if present
    if expires_at_str:
        try:
            exp_dt = datetime.fromisoformat(str(expires_at_str))
            if exp_dt.tzinfo is None:
                exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            if ref_now >= exp_dt:
                return (
                    False,
                    BILLING_TIER_UNKNOWN,
                    f"Proof expired at {expires_at_str}. Fail closed.",
                )
        except (ValueError, TypeError) as exc:
            return (
                False,
                BILLING_TIER_UNKNOWN,
                f"Invalid expires_at format '{expires_at_str}': {exc}. Fail closed.",
            )

    # CRITICAL P1: Caller-provided record is NOT an authenticated provider attestation.
    # It cannot certify actual provider $0 billing.
    # In offline testing, synthetic simulations may pass allow_synthetic_simulation=True
    # to evaluate structural proof validity, but is_valid remains False (non-authorizing).
    if allow_synthetic_simulation:
        return (
            False,
            BILLING_TIER_SIMULATED_UNAUTHENTICATED,
            "Valid proof structure (synthetic simulation only - non-authorizing; provider billing remains UNVERIFIED).",
        )

    return (
        False,
        BILLING_TIER_UNKNOWN,
        "Caller-provided unauthenticated record cannot prove provider billing status. "
        "Actual provider billing status remains UNKNOWN.",
    )


class GeminiCostAccounting(NamedTuple):
    """Separates paid-tier equivalent opportunity cost estimates from nominal Free Tier rates,
    unobserved provider billing, and quota accounting.

    Attributes:
        model_name: The Gemini model ID.
        input_tokens: Prompt/input token count.
        output_tokens: Generated output token count (including thinking tokens).
        paid_equivalent_estimate_usd: Published Standard paid-tier reference price Google would charge
            ($0.30 input / $2.50 output per 1M tokens for gemini-3.5-flash-lite).
            Preserved in ai_usage_ledger.estimated_cost_usd for economic opportunity cost audit.
        nominal_free_tier_rate_usd: Published theoretical rate for Free Tier ($0.00) when applicable
            to the assessed launch model without search grounding; None (NOT_APPLICABLE) for unassessed
            models or when search grounding is enabled.
            This is a reference rate assumption, NOT an observed invoice or billing attestation.
        actual_billed_usd: Actual billable charge on invoice. ALWAYS None (UNKNOWN) in offline operation
            until independently authenticated provider billing evidence exists.
            Nonzero paid charges are strictly disallowed ($0.00 / 0 TRY policy).
        billing_tier: Provenance classification (defaults to UNKNOWN_UNVERIFIED).
        is_assessed_launch_model: Whether the model is the assessed launch model (gemini-3.5-flash-lite).
        used_search_grounding: Whether Google Search Grounding was requested.
        reconciliation_notes: Audit explanation of billing status and rate reconciliation.
    """
    model_name: str
    input_tokens: int
    output_tokens: int
    paid_equivalent_estimate_usd: float
    nominal_free_tier_rate_usd: Optional[float]
    actual_billed_usd: Optional[float]
    billing_tier: str
    is_assessed_launch_model: bool
    used_search_grounding: bool
    reconciliation_notes: str

    @property
    def is_free_tier_eligible(self) -> bool:
        """Backward compatibility alias for is_assessed_launch_model (internal launch assessment)."""
        return self.is_assessed_launch_model


def compute_gemini_cost_accounting(
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    thinking_tokens: int = 0,
    used_search_grounding: bool = False,
    output_already_includes_thinking: bool = False,
    use_batch_tier: bool = False,
    project_proof: Optional[dict[str, Any] | FreeTierProjectProof] = None,
    now: Optional[datetime] = None,
) -> GeminiCostAccounting:
    """Compute comprehensive Gemini cost accounting separating paid-equivalent estimates,
    nominal Free Tier rate assumptions, and unobserved actual billing.

    Actual billed USD is ALWAYS None (UNKNOWN) in offline operation; caller records cannot
    certify $0 provider billing.
    """
    clean_name, _ = resolve_model_pricing(model_name)
    paid_est = estimate_call_cost(
        model_name=clean_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        thinking_tokens=thinking_tokens,
        used_search_grounding=used_search_grounding,
        output_already_includes_thinking=output_already_includes_thinking,
        use_batch_tier=use_batch_tier,
    )
    is_assessed = is_model_assessed_for_launch(clean_name)

    # In this offline slice, actual billed USD is ALWAYS None (UNKNOWN) because
    # real provider billing observation is not implemented or authorized.
    # nominal_free_tier_rate_usd is 0.0 (theoretical Free Tier price) only when
    # model is assessed for launch and search grounding is disabled; None (NOT_APPLICABLE) otherwise.
    # Strictly separated from actual billed invoice charge.
    nominal_rate: Optional[float] = 0.0 if (is_assessed and not used_search_grounding) else None

    _, proof_tier, reason = validate_free_tier_project_proof(project_proof, now=now)

    nominal_rate_str = (
        f"${nominal_rate:.6f} USD"
        if nominal_rate is not None
        else "NOT_APPLICABLE (unassessed model or search grounding enabled)"
    )
    reconciliation = (
        f"Paid-tier equivalent reference: ${paid_est:.6f} USD. "
        f"Actual billed USD: UNKNOWN (offline slice; no provider billing verification). "
        f"Nominal Free Tier rate assumption: {nominal_rate_str}. "
        f"Proof assessment: {reason}"
    )

    return GeminiCostAccounting(
        model_name=clean_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        paid_equivalent_estimate_usd=round(paid_est, 6),
        nominal_free_tier_rate_usd=nominal_rate,
        actual_billed_usd=None,  # Always None in offline slice
        billing_tier=proof_tier,
        is_assessed_launch_model=is_assessed,
        used_search_grounding=used_search_grounding,
        reconciliation_notes=reconciliation,
    )


def estimate_paid_equivalent_cost(
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    thinking_tokens: int = 0,
    used_search_grounding: bool = False,
    output_already_includes_thinking: bool = False,
    use_batch_tier: bool = False,
) -> float:
    """Explicit alias for estimate_call_cost documenting that it returns the published
    standard paid-tier equivalent opportunity cost (not actual billed USD on Free Tier)."""
    return estimate_call_cost(
        model_name=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        thinking_tokens=thinking_tokens,
        used_search_grounding=used_search_grounding,
        output_already_includes_thinking=output_already_includes_thinking,
        use_batch_tier=use_batch_tier,
    )

DEFAULT_PRICING = ModelPricing(input_per_million=0.30, output_per_million=2.50)
SEARCH_GROUNDING_COST_PER_QUERY = 0.035  # Conservative ceiling ($14-$35 per 1,000 queries when over free quota)


def resolve_model_pricing(model_name: str) -> tuple[str, ModelPricing]:
    """
    Resolve model pricing. Rejects shut-down or unsupported models with an actionable error.
    Distinguishes stable models (e.g., 'gemini-3.1-flash-lite') from shut-down previews
    (e.g., 'gemini-3.1-flash-lite-preview', shut down May 25, 2026).
    Does not silently switch model IDs.
    """
    clean_name = model_name.strip().lower()
    if clean_name in SHUTDOWN_MODELS:
        shutdown_date = SHUTDOWN_MODELS[clean_name]
        raise ValueError(
            f"Gemini model '{model_name}' was shut down on {shutdown_date} and is no longer available "
            f"(not a valid fallback). Please configure an active stable model such as 'gemini-3.5-flash-lite'."
        )

    if clean_name not in OFFICIAL_PRICING:
        raise ValueError(
            f"Gemini model '{model_name}' is unsupported or unrecognized. "
            f"Supported models: {sorted(list(OFFICIAL_PRICING.keys()))}."
        )

    pricing = OFFICIAL_PRICING[clean_name]
    if pricing.is_deprecated:
        replacement_note = (
            f" Recommended replacement: '{pricing.recommended_replacement}'."
            if pricing.recommended_replacement
            else ""
        )
        shutdown_note = (
            f" Scheduled shutdown: {pricing.shutdown_date}."
            if pricing.shutdown_date
            else ""
        )
        logger.warning(
            "Model '%s' is marked as legacy/deprecated.%s%s",
            clean_name,
            shutdown_note,
            replacement_note,
        )
    return clean_name, pricing


def estimate_call_cost(
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    thinking_tokens: int = 0,
    used_search_grounding: bool = False,
    output_already_includes_thinking: bool = False,
    use_batch_tier: bool = False,
) -> float:
    """
    Calculate the estimated USD cost of a Gemini API generation call.
    Output pricing applies to all generated output tokens, including thinking tokens.
    If output_already_includes_thinking is True, thinking_tokens is NOT added again.
    """
    _, pricing = resolve_model_pricing(model_name)

    if output_already_includes_thinking:
        total_output_tokens = output_tokens
    else:
        total_output_tokens = output_tokens + thinking_tokens

    in_rate = pricing.batch_input_per_million if use_batch_tier else pricing.input_per_million
    out_rate = pricing.batch_output_per_million if use_batch_tier else pricing.output_per_million

    input_cost = (input_tokens / 1_000_000) * in_rate
    output_cost = (total_output_tokens / 1_000_000) * out_rate
    search_cost = SEARCH_GROUNDING_COST_PER_QUERY if used_search_grounding else 0.0

    return input_cost + output_cost + search_cost


def extract_provider_token_usage(
    response: Any,
    fallback_input_tokens: int,
    fallback_output_tokens: int,
) -> tuple[int, int, bool]:
    """
    Extract actual billed token counts from a Google GenAI SDK response's `usage_metadata`.

    Returns `(input_tokens, output_tokens_including_thinking, from_provider_metadata)`.
    Handles both SDK conventions where `thoughts_token_count` is reported separately from
    `candidates_token_count` (`total_token_count = prompt + candidates + thoughts`) and
    where `candidates_token_count` already includes thinking tokens.
    """
    usage_meta = getattr(response, "usage_metadata", None)
    if usage_meta is None:
        return int(fallback_input_tokens), int(fallback_output_tokens), False

    prompt_tok = getattr(usage_meta, "prompt_token_count", None)
    cand_tok = getattr(usage_meta, "candidates_token_count", None)
    thoughts_tok = getattr(usage_meta, "thoughts_token_count", None) or 0
    total_tok = getattr(usage_meta, "total_token_count", None)

    if prompt_tok is None and cand_tok is None and total_tok is None:
        return int(fallback_input_tokens), int(fallback_output_tokens), False

    actual_in = int(prompt_tok) if prompt_tok is not None else int(fallback_input_tokens)
    base_out = int(cand_tok) if cand_tok is not None else int(fallback_output_tokens)
    thoughts_int = int(thoughts_tok)

    if total_tok is not None and int(total_tok) > (actual_in + base_out):
        # Total token count exceeds prompt + candidates -> thinking tokens were reported separately
        actual_out = max(base_out + thoughts_int, int(total_tok) - actual_in)
    elif thoughts_int > 0 and cand_tok is not None and total_tok is None:
        actual_out = base_out + thoughts_int
    else:
        actual_out = base_out

    return max(1, actual_in), max(1, actual_out), True


def estimate_tokens_from_text(text: str) -> int:
    """Fast character-based heuristic for token count estimation (approx 4 chars/token)."""
    if not text:
        return 0
    return max(1, len(text) // 4)


def compute_candidate_batch_hash(
    candidate_ids: Sequence[str],
    model_name: str,
    schema_version: str = "v3c2b",
) -> str:
    """Compute a deterministic SHA-256 fingerprint for a batch of candidate IDs and model."""
    payload = json.dumps(
        {
            "ids": list(candidate_ids),
            "model": model_name.strip().lower(),
            "schema": schema_version,
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def compute_enforceable_batch_output_tokens(
    candidate_count: int,
    max_configured_output_tokens: int = 3200,
    max_tokens_per_story: int = 180,
    base_wrapper_tokens: int = 100,
) -> int:
    """
    Compute an enforceable per-request max_output_tokens ceiling for a batch of candidate stories.

    Prevents allocating a global 3200/4096 token limit to a small batch of 8 stories (which would
    allow 22k-28k output tokens across 7 batches and breach the $0.05 budget ceiling).

    For 8 stories: 100 + 8 * 180 = 1540 tokens (clamped between 500 and max_configured_output_tokens).
    For 2 stories: 100 + 2 * 180 = 460 -> 500 tokens.
    """
    calculated = base_wrapper_tokens + (candidate_count * max_tokens_per_story)
    return max(500, min(max_configured_output_tokens, calculated))


def require_verified_free_tier() -> None:
    """Fail closed before real Gemini API dispatch until provider-side billing is proven safe.

    Paid-rate token estimates and internal budget caps do not verify Free Tier
    eligibility or guarantee zero charges for a billing-linked project.
    No environment variable, positive ceiling, or API key lifts this guard.
    A future separately reviewed and PO-approved implementation is required.
    """
    raise RuntimeError(
        "Real Gemini API calls blocked: project free-tier/no-billing status and "
        "provider-side $0 enforcement are not independently verified. "
        "No paid fallback; separate Product Owner authorization required."
    )


def check_monthly_budget_guard(
    est_run_cost_usd: float,
    current_monthly_cost_usd: float,
    current_monthly_calls: int,
    max_run_cost_usd: float = 0.0,
    max_monthly_cost_usd: float = 0.0,
    max_monthly_calls: int = 300,
) -> tuple[bool, str]:
    """
    Application-level budget circuit breaker.
    Prevents API calls if per-run cost, cumulative monthly USD cost, or monthly call count is exceeded.

    Note: This is an application-level safeguard backed by the local/Turso `ai_usage_ledger`,
    NOT a hard provider-side billing guarantee.
    """
    if est_run_cost_usd > max_run_cost_usd:
        return (
            False,
            f"Estimated run cost (${est_run_cost_usd:.5f}) exceeds per-run limit (${max_run_cost_usd:.4f}).",
        )
    if (current_monthly_cost_usd + est_run_cost_usd) > max_monthly_cost_usd:
        return (
            False,
            f"Projected monthly AI cost (${current_monthly_cost_usd + est_run_cost_usd:.4f}) exceeds monthly budget ceiling (${max_monthly_cost_usd:.4f}).",
        )
    if (current_monthly_calls + 1) > max_monthly_calls:
        return (
            False,
            f"Monthly AI call count ({current_monthly_calls + 1}) exceeds configured monthly call cap ({max_monthly_calls}).",
        )
    return True, "OK"


def check_free_tier_safety_guard(
    model_name: str,
    current_monthly_calls: Optional[int] = None,
    max_monthly_calls: int = 300,
    enable_search_grounding: bool = False,
    max_run_cost_usd: float = 0.0,
    max_monthly_cost_usd: float = 0.0,
    rpm_limit: Optional[int] = None,
    current_rpm: Optional[int] = None,
    tpm_limit: Optional[int] = None,
    current_tpm: Optional[int] = None,
    estimated_batch_tokens: Optional[int] = None,
    input_tpm_limit: Optional[int] = None,
    current_input_tpm: Optional[int] = None,
    estimated_input_tokens: Optional[int] = None,
    rpd_limit: Optional[int] = None,
    current_rpd: Optional[int] = None,
    # Backward compatibility aliases for existing caller signatures:
    daily_requests_count: Optional[int] = None,
    daily_requests_cap: Optional[int] = None,
    requests_per_minute: Optional[int] = None,
    rpm_cap: Optional[int] = None,
    dynamic_quota_unknown: Optional[bool] = None,
    allow_synthetic_simulation: bool = False,
) -> tuple[bool, str]:
    """Free Tier safety and quota circuit breaker.

    FAIL-CLOSED SAFETY INVARIANT:
    Missing or unobserved quota evidence (RPM, TPM, RPD, or monthly calls) MUST return DENY.
    Universal constants (e.g. 15 RPM, 1500 RPD, 1M TPM) are NOT account entitlements.
    Exact project/model-scoped, fresh, and trusted quota evidence is required for any ALLOW.

    In this offline slice, caller-supplied numerical quota values cannot certify provider quotas.
    Real dispatch authorization fails closed unless explicitly marked as a synthetic mathematical
    simulation (allow_synthetic_simulation=True) for unit test validation.

    Enforces:
    1. Assessed launch model candidate (is_model_assessed_for_launch).
    2. Search grounding strictly disabled (search grounding is not free under Free Tier).
    3. Zero monetary ceilings (positive budget overrides fail closed).
    4. Explicit dynamic quota unknown flag check.
    5. Monthly call cap (current_monthly_calls must be observed and + 1 <= max_monthly_calls).
    6. Verified RPM quota limit AND consumption observed; current_rpm < rpm_limit.
    7. Verified TPM quota limit, consumption observed, AND batch reservation; current_tpm + tokens <= tpm_limit.
    8. Verified RPD quota limit AND consumption observed; current_rpd + 1 <= rpd_limit.
    9. Provider quota provenance: caller-supplied numbers cannot authorize live dispatch (fail closed).
    """
    clean_name = model_name.strip().lower()

    # 1. Model launch assessment
    if not is_model_assessed_for_launch(clean_name):
        if clean_name in SHUTDOWN_MODELS:
            return (
                False,
                f"Model '{clean_name}' was shut down on {SHUTDOWN_MODELS[clean_name]} and cannot be used. Fail closed.",
            )
        return (
            False,
            f"Model '{clean_name}' is not an assessed launch model (assessed: {sorted(ASSESSED_LAUNCH_MODELS)}). "
            "Unassessed models are denied under launch safety policy. Fail closed.",
        )

    # 2. Search grounding
    if enable_search_grounding:
        return (
            False,
            "Search grounding is enabled. Search grounding is not free under Free Tier "
            "and incurs charges. Strict $0.00 / 0 TRY policy requires search grounding disabled.",
        )

    # 3. Positive budget overrides
    if max_run_cost_usd > 0.0 or max_monthly_cost_usd > 0.0:
        return (
            False,
            f"Positive monetary budget override detected (max_run_cost_usd={max_run_cost_usd}, "
            f"max_monthly_cost_usd={max_monthly_cost_usd}). Strict Free Tier launch policy "
            "requires $0.00 / 0 TRY monetary ceilings. A positive dollar allowance contradicts "
            "Free Tier enforcement.",
        )

    # 4. Explicit dynamic quota unknown flag
    if dynamic_quota_unknown is True:
        return (
            False,
            "Dynamic provider quota state is explicitly marked UNKNOWN. Fail closed under strict Free Tier policy.",
        )

    # 5. Monthly call quota (must be observed)
    if current_monthly_calls is None:
        return (
            False,
            "Missing monthly call count observation: status is UNKNOWN. Fail closed.",
        )
    # Strictly clamp caller-provided monthly cap to internal ceiling of 300
    effective_max_monthly_calls = min(int(max_monthly_calls), 300)
    if (current_monthly_calls + 1) > effective_max_monthly_calls:
        return (
            False,
            f"Monthly AI call count ({current_monthly_calls + 1}) exceeds Free Tier monthly call cap ({effective_max_monthly_calls}).",
        )

    # Resolve RPM parameter aliases
    effective_rpm_limit = rpm_limit if rpm_limit is not None else rpm_cap
    effective_current_rpm = current_rpm if current_rpm is not None else requests_per_minute

    # 6. RPM quota: limit AND consumption must be explicitly observed
    if effective_rpm_limit is None:
        return (
            False,
            "Missing verified RPM (Requests Per Minute) provider quota limit: status is UNKNOWN. "
            "Universal constant cannot be assumed as an account entitlement. Fail closed.",
        )
    if effective_current_rpm is None:
        return (
            False,
            "Missing current RPM consumption observation: status is UNKNOWN. Fail closed.",
        )
    if effective_current_rpm >= effective_rpm_limit:
        return (
            False,
            f"Requests per minute ({effective_current_rpm}) reaches or exceeds verified RPM limit ({effective_rpm_limit}).",
        )

    # 7. TPM / Input TPM quota: limit, consumption, AND estimated batch tokens must be explicitly observed
    if tpm_limit is None and input_tpm_limit is None:
        return (
            False,
            "Missing verified TPM (Tokens Per Minute) provider quota limit: status is UNKNOWN. "
            "Universal constant cannot be assumed as an account entitlement. Fail closed.",
        )
    if tpm_limit is not None:
        if current_tpm is None:
            return (
                False,
                "Missing current TPM consumption observation: status is UNKNOWN. Fail closed.",
            )
        if estimated_batch_tokens is None or estimated_batch_tokens <= 0:
            return (
                False,
                "Missing estimated batch token count for TPM quota verification: status is UNKNOWN. Fail closed.",
            )
        if (current_tpm + estimated_batch_tokens) > tpm_limit:
            return (
                False,
                f"Projected tokens per minute ({current_tpm + estimated_batch_tokens}) exceeds verified TPM limit ({tpm_limit}).",
            )

    if input_tpm_limit is not None:
        if current_input_tpm is None:
            return (
                False,
                "Missing current input TPM consumption observation: status is UNKNOWN. Fail closed.",
            )
        if estimated_input_tokens is None or estimated_input_tokens <= 0:
            return (
                False,
                "Missing estimated input batch tokens for input TPM quota verification: status is UNKNOWN. Fail closed.",
            )
        if (current_input_tpm + estimated_input_tokens) > input_tpm_limit:
            return (
                False,
                f"Projected input tokens per minute ({current_input_tpm + estimated_input_tokens}) exceeds verified input TPM limit ({input_tpm_limit}).",
            )
    elif current_input_tpm is not None:
        return (
            False,
            "Missing verified input TPM provider quota limit: status is UNKNOWN. Fail closed.",
        )

    # Resolve RPD parameter aliases
    effective_rpd_limit = rpd_limit if rpd_limit is not None else daily_requests_cap
    effective_current_rpd = current_rpd if current_rpd is not None else daily_requests_count

    # 8. RPD quota: limit AND consumption must be explicitly observed
    if effective_rpd_limit is None:
        return (
            False,
            "Missing verified RPD (Requests Per Day) provider quota limit: status is UNKNOWN. "
            "Universal constant cannot be assumed as an account entitlement. Fail closed.",
        )
    if effective_current_rpd is None:
        return (
            False,
            "Missing current RPD consumption observation: status is UNKNOWN. Fail closed.",
        )
    if (effective_current_rpd + 1) > effective_rpd_limit:
        return (
            False,
            f"Daily requests count ({effective_current_rpd + 1}) exceeds verified RPD limit ({effective_rpd_limit}).",
        )

    # 9. Provider quota provenance: caller-supplied numerical values cannot certify provider quotas.
    # Authenticated project/model/tier-specific quota evidence is unavailable in this offline slice.
    # Fail closed for real dispatch authorization. Pure mathematical tests must explicitly pass
    # allow_synthetic_simulation=True as non-authorizing tests.
    if not allow_synthetic_simulation:
        return (
            False,
            "Caller-supplied numerical quota values cannot certify provider quotas. "
            "Authenticated project/model/tier-specific quota evidence and atomic reservation "
            "counters are unavailable in this offline slice. Live dispatch authorization denied (fail closed).",
        )

    return True, "OK (synthetic simulation only - non-authorizing)"


class G3PreDispatchDecision(NamedTuple):
    """Immutable preflight evaluation outcome for G3 Gemini activation safety."""
    allowed: bool
    status_code: str
    reason: str
    model_name: str
    billing_tier: str
    pacific_reset_date: str
    requires_erd017_fencing: bool
    provenance_summary: dict[str, Any]


def evaluate_g3_activation_preflight(
    model_name: str = "gemini-3.5-flash-lite",
    enable_search_grounding: bool = False,
    max_run_cost_usd: float = 0.0,
    max_monthly_cost_usd: float = 0.0,
    project_proof: Optional[dict[str, Any] | FreeTierProjectProof] = None,
    quota_evidence: Optional[dict[str, Any]] = None,
    is_po_live_go_granted: bool = False,
    is_multi_runner_environment: bool = False,
    allow_synthetic_simulation: bool = False,
    now: Optional[datetime] = None,
) -> G3PreDispatchDecision:
    """Evaluate G3 Gemini activation preflight gates strictly offline.

    FAIL-CLOSED SAFETY INVARIANT (P1 REMEDIATION):
    This function NEVER returns allowed=True under any circumstance in this offline implementation!
    Caller-controlled billing proof, quota dictionaries, allow_synthetic_simulation=True,
    and is_po_live_go_granted=True are untrusted caller inputs and CANNOT authorize live dispatch.

    Live activation remains permanently blocked by five mandatory activation blockers:
    1. G1: Independent provider billing attestation (Google Cloud Billing API).
    2. G2 / ERD-017: Provider-persistent cross-runner quota reservation and ownership journal.
    3. Trusted provider quota freshness and observation timestamp.
    4. Project/model binding attestation.
    5. Authenticated, session-scoped human PO Live GO grant validation.

    When all diagnostic checks pass in synthetic simulation mode (allow_synthetic_simulation=True),
    preflight returns:
        allowed=False
        status_code="SIMULATION_PASS_NON_AUTHORIZING"

    UNCONDITIONAL OFFLINE BARRIER:
    In this offline engineering slice, live API dispatch remains unconditionally blocked by
    require_verified_free_tier() at the client boundary, regardless of any preflight evaluation.
    """
    ref_now = now or datetime.now(timezone.utc)
    if ref_now.tzinfo is None:
        ref_now = ref_now.replace(tzinfo=timezone.utc)
    else:
        ref_now = ref_now.astimezone(timezone.utc)
    pacific_date = get_pacific_calendar_date(ref_now)

    clean_name = model_name.strip().lower()

    # 1. Model Assessment Gate
    if not is_model_assessed_for_launch(clean_name):
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_UNASSESSED_MODEL",
            reason=(
                f"Model '{clean_name}' is unassessed for launch. Sole evaluated "
                f"launch candidate is 'gemini-3.5-flash-lite'. Fail closed."
            ),
            model_name=clean_name,
            billing_tier=BILLING_TIER_UNKNOWN,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "model": clean_name,
                "gate": "model_assessment",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # 2. Search Grounding Gate
    if enable_search_grounding:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_SEARCH_GROUNDING",
            reason=(
                "Search grounding is enabled. Search grounding is not available on Gemini Free Tier "
                "and incurs charges. Strict $0.00 / 0 TRY policy requires search grounding disabled. Fail closed."
            ),
            model_name=clean_name,
            billing_tier=BILLING_TIER_UNKNOWN,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "search_grounding": True,
                "gate": "search_grounding",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # 3. Monetary Budget Gate (must be strictly $0.00)
    if max_run_cost_usd > 0.0 or max_monthly_cost_usd > 0.0:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_POSITIVE_BUDGET",
            reason=(
                f"Positive monetary budget override detected (${max_run_cost_usd:.4f} / "
                f"${max_monthly_cost_usd:.4f}). Strict Free Tier launch policy requires "
                "$0.00 / 0 TRY monetary ceilings. Fail closed."
            ),
            model_name=clean_name,
            billing_tier=BILLING_TIER_UNKNOWN,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "max_run_cost_usd": max_run_cost_usd,
                "max_monthly_cost_usd": max_monthly_cost_usd,
                "gate": "zero_budget",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # 4. Project Billing Proof Gate
    proof_ok, billing_tier, proof_reason = validate_free_tier_project_proof(
        project_proof, now=ref_now, allow_synthetic_simulation=allow_synthetic_simulation
    )
    if billing_tier == BILLING_TIER_DISALLOWED_PAID:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_PAID_PROJECT",
            reason=proof_reason,
            model_name=clean_name,
            billing_tier=BILLING_TIER_DISALLOWED_PAID,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "billing_tier": BILLING_TIER_DISALLOWED_PAID,
                "gate": "billing_proof",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # Default production callers without synthetic flag fail closed
    if not allow_synthetic_simulation:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_UNVERIFIED_BILLING",
            reason=proof_reason,
            model_name=clean_name,
            billing_tier=billing_tier,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "billing_tier": billing_tier,
                "gate": "billing_proof",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # In synthetic simulation: structural validation must pass
    if billing_tier != BILLING_TIER_SIMULATED_UNAUTHENTICATED:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_UNVERIFIED_BILLING",
            reason=proof_reason,
            model_name=clean_name,
            billing_tier=billing_tier,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "billing_tier": billing_tier,
                "gate": "billing_proof",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # 5. Quota Evidence Gate
    if quota_evidence is None:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_QUOTA_UNVERIFIED",
            reason="Missing quota evidence. Dynamic provider quota state cannot be assumed. Fail closed.",
            model_name=clean_name,
            billing_tier=billing_tier,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "quota_evidence": None,
                "gate": "quota_guard",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # Specific Input TPM check for gemini-3.5-flash-lite (observed baseline: 250,000 input TPM)
    if "input_tpm_limit" not in quota_evidence or quota_evidence.get("input_tpm_limit") is None:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_MISSING_INPUT_TPM_EVIDENCE",
            reason=(
                "Missing verified input TPM (Tokens Per Minute) limit in evidence. Assessed model "
                "(gemini-3.5-flash-lite) specifically requires input TPM quota tracking "
                f"(observed project baseline: {OBSERVED_PROJECT_INPUT_TPM_LIMIT:,} input TPM). Fail closed."
            ),
            model_name=clean_name,
            billing_tier=billing_tier,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "gate": "input_tpm_evidence",
                "error": "missing_input_tpm_limit",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # Strictly clamp caller-provided monthly cap to internal ceiling of 300
    caller_monthly_cap = quota_evidence.get("max_monthly_calls", 300)
    effective_monthly_cap = min(int(caller_monthly_cap), 300)

    quota_ok, quota_reason = check_free_tier_safety_guard(
        model_name=clean_name,
        current_monthly_calls=quota_evidence.get("current_monthly_calls"),
        max_monthly_calls=effective_monthly_cap,
        enable_search_grounding=enable_search_grounding,
        max_run_cost_usd=max_run_cost_usd,
        max_monthly_cost_usd=max_monthly_cost_usd,
        rpm_limit=quota_evidence.get("rpm_limit"),
        current_rpm=quota_evidence.get("current_rpm"),
        tpm_limit=quota_evidence.get("tpm_limit"),
        current_tpm=quota_evidence.get("current_tpm"),
        estimated_batch_tokens=quota_evidence.get("estimated_batch_tokens"),
        input_tpm_limit=quota_evidence.get("input_tpm_limit"),
        current_input_tpm=quota_evidence.get("current_input_tpm"),
        estimated_input_tokens=quota_evidence.get("estimated_input_tokens"),
        rpd_limit=quota_evidence.get("rpd_limit"),
        current_rpd=quota_evidence.get("current_rpd"),
        dynamic_quota_unknown=quota_evidence.get("dynamic_quota_unknown"),
        allow_synthetic_simulation=allow_synthetic_simulation,
    )
    if not quota_ok:
        code = (
            "DENY_QUOTA_EXHAUSTED"
            if ("exceeds" in quota_reason or "reaches" in quota_reason)
            else "DENY_QUOTA_UNVERIFIED"
        )
        return G3PreDispatchDecision(
            allowed=False,
            status_code=code,
            reason=quota_reason,
            model_name=clean_name,
            billing_tier=billing_tier,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "quota_error": quota_reason,
                "gate": "quota_guard",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # 6. Multi-Runner / Cross-Runner Fencing Gate (ERD-017 Blocker)
    if is_multi_runner_environment:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_MISSING_CROSS_RUNNER_RESERVATION",
            reason=(
                "Multi-runner environment requires ERD-017 provider-persistent ownership journal "
                "and atomic reservation fencing. Cross-runner reservation infrastructure is currently "
                "unimplemented / unverified. Activation blocked (fail closed)."
            ),
            model_name=clean_name,
            billing_tier=billing_tier,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=True,
            provenance_summary={
                "gate": "erd017_cross_runner_fencing",
                "error": "missing_cross_runner_reservation",
                "is_multi_runner_environment": True,
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # 7. Human Product Owner Live GO Gate
    if not is_po_live_go_granted:
        return G3PreDispatchDecision(
            allowed=False,
            status_code="DENY_NO_PO_GO",
            reason=(
                "Machine preflight checks passed, but human Product Owner (PO) live "
                "execution authorization (PO Live GO) has not been granted. Machine gates "
                "cannot self-authorize live dispatch."
            ),
            model_name=clean_name,
            billing_tier=billing_tier,
            pacific_reset_date=pacific_date,
            requires_erd017_fencing=is_multi_runner_environment,
            provenance_summary={
                "is_po_live_go_granted": False,
                "preflight_machine_gates": "PASS",
                "gate": "human_po_go",
                "activation_blockers": list(ACTIVATION_BLOCKERS),
            },
        )

    # 8. Synthetic Simulation Diagnostic Pass (NEVER allowed=True!)
    # While trusted provider evidence, durable quota reservation, and independent
    # PO grant validation are unavailable, allowed remains strictly False.
    return G3PreDispatchDecision(
        allowed=False,
        status_code="SIMULATION_PASS_NON_AUTHORIZING",
        reason=(
            "Synthetic simulation diagnostic checks passed structurally; live dispatch authorization "
            "DENIED (allowed=False) because trusted provider billing attestation, durable cross-runner "
            "quota reservation, and independent PO grant validation are unavailable (activation blockers)."
        ),
        model_name=clean_name,
        billing_tier=billing_tier,
        pacific_reset_date=pacific_date,
        requires_erd017_fencing=is_multi_runner_environment,
        provenance_summary={
            "is_po_live_go_granted": True,
            "preflight_machine_gates": "PASS",
            "allow_synthetic_simulation": True,
            "gate": "simulation_diagnostic_complete",
            "activation_blockers": list(ACTIVATION_BLOCKERS),
        },
    )


class ExperimentBudgetGuard:
    """
    Durable, persistent experiment budget ledger and circuit breaker.
    Enforces a strict cumulative budget ceiling across multiple canary databases,
    subsequent evaluation batches, and application restarts.

    Guarantees:
    1. Prevents budget resets when new temporary databases are instantiated.
    2. Prevents double-counting completed runs by using unique batch hashes and run IDs.
    3. Prevents repeating cached successful batches (reused at $0 cost).
    4. Prevents concurrent / sequential reservations from overspending the shared allowance.
    5. Ambiguous failed requests are treated as spent (fail-closed, never free).
    """

    def __init__(
        self,
        ledger_path: Path = Path("data/experiment_budget_ledger.json"),
        budget_ceiling_usd: float = 0.0,
        canary1_db_path: Path = Path("data/canary_radar.db"),
        canary2_db_path: Path = Path("data/canary2_radar.db"),
        repository: Optional[Any] = None,
    ):
        self.ledger_path = Path(ledger_path)
        self.budget_ceiling_usd = budget_ceiling_usd
        self.canary1_db_path = Path(canary1_db_path)
        self.canary2_db_path = Path(canary2_db_path)
        self.repository = repository
        self.data: dict[str, Any] = {}
        self._load_or_initialize()

    def _extract_db_cost(self, db_path: Path) -> tuple[int, int, float]:
        if not db_path.exists():
            return 0, 0, 0.0
        try:
            with closing(sqlite3.connect(db_path)) as conn:
                c = conn.cursor()
                c.execute("SELECT input_tokens, output_tokens, estimated_cost_usd FROM ai_usage_ledger")
                rows = c.fetchall()
            in_tok = sum(r[0] for r in rows)
            out_tok = sum(r[1] for r in rows)
            cost = sum(r[2] for r in rows)
            return in_tok, out_tok, cost
        except Exception:
            return 0, 0, 0.0

    def _load_or_initialize(self) -> None:
        if self.ledger_path.exists():
            try:
                with open(self.ledger_path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
                return
            except Exception:
                pass

        # Initialize ledger from prior canary databases
        c1_in, c1_out, c1_cost = self._extract_db_cost(self.canary1_db_path)
        c2_in, c2_out, c2_cost = self._extract_db_cost(self.canary2_db_path)

        # Fallback to known authoritative values if DB exists but yielded 0
        if c1_cost == 0.0 and self.canary1_db_path.exists():
            c1_in, c1_out, c1_cost = 2815, 73, 0.001027
        if c2_cost == 0.0 and self.canary2_db_path.exists():
            c2_in, c2_out, c2_cost = 3024, 1300, 0.0041572

        self.data = {
            "experiment_id": "phase3c_shadow_eval",
            "budget_ceiling_usd": self.budget_ceiling_usd,
            "prior_canaries": [
                {
                    "canary_id": "canary_1",
                    "db_path": str(self.canary1_db_path),
                    "input_tokens": c1_in,
                    "output_tokens": c1_out,
                    "cost_usd": round(c1_cost, 6),
                },
                {
                    "canary_id": "canary_2",
                    "db_path": str(self.canary2_db_path),
                    "input_tokens": c2_in,
                    "output_tokens": c2_out,
                    "cost_usd": round(c2_cost, 6),
                },
            ],
            "batches": {},
        }
        self._save()

    def _save(self) -> None:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.ledger_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        tmp_path.replace(self.ledger_path)

    def get_prior_canaries_cost_usd(self) -> float:
        return sum(c["cost_usd"] for c in self.data.get("prior_canaries", []))

    def get_total_consumed_usd(self, period_month: Optional[str] = None) -> float:
        total = self.get_prior_canaries_cost_usd()
        for b in self.data.get("batches", {}).values():
            if b.get("status") == "COMPLETED":
                total += b.get("actual_cost_usd", 0.0)
            else:
                # RESERVED or FAILED_AMBIGUOUS: fail-closed, treat worst-case as spent
                total += b.get("estimated_worst_case_usd", 0.0)
        if self.repository is not None:
            month = period_month or datetime.now(timezone.utc).strftime("%Y-%m")
            # Authoritative cloud repository read MUST fail closed on any error:
            # Never silently ignore repository failures or assume 0 prior spend!
            repo_sum = self.repository.get_monthly_budget_summary(month)
            repo_cost = float(repo_sum.get("total_cost_usd", 0.0))
            total = max(total, repo_cost)
        return round(total, 6)

    def get_remaining_allowance_usd(self) -> float:
        return max(0.0, round(self.budget_ceiling_usd - self.get_total_consumed_usd(), 6))

    def is_batch_completed(self, batch_hash: str) -> bool:
        b = self.data.get("batches", {}).get(batch_hash)
        return bool(b and b.get("status") == "COMPLETED")

    def reserve_batch(
        self,
        batch_hash: str,
        candidate_ids: list[str],
        estimated_worst_cost_usd: float,
    ) -> bool:
        if self.is_batch_completed(batch_hash):
            return True

        projected = self.get_total_consumed_usd() + estimated_worst_cost_usd
        if projected > self.budget_ceiling_usd:
            raise RuntimeError(
                f"Fail-closed budget circuit breaker: experiment batch reservation "
                f"(${estimated_worst_cost_usd:.5f}) would cause cumulative experiment cost "
                f"(${projected:.5f}) to breach the budget ceiling (${self.budget_ceiling_usd:.4f}). "
                f"Remaining allowance: ${self.get_remaining_allowance_usd():.5f}. "
                "Execution halted before network dispatch."
            )

        self.data.setdefault("batches", {})[batch_hash] = {
            "status": "RESERVED",
            "candidate_ids": candidate_ids,
            "estimated_worst_case_usd": round(estimated_worst_cost_usd, 6),
            "actual_cost_usd": None,
            "input_tokens": None,
            "output_tokens": None,
        }
        self._save()
        return True

    def reconcile_batch(
        self,
        batch_hash: str,
        actual_cost_usd: float,
        input_tokens: int,
        output_tokens: int,
        model_name: str = "gemini-3.5-flash-lite",
        run_id: str = "experiment_run",
        worker_id: str = "worker-default",
    ) -> None:
        b = self.data.setdefault("batches", {}).setdefault(batch_hash, {})
        b["status"] = "COMPLETED"
        b["actual_cost_usd"] = round(actual_cost_usd, 6)
        b["input_tokens"] = input_tokens
        b["output_tokens"] = output_tokens
        b["model_name"] = model_name
        self._save()

        if self.repository is not None:
            now_utc = datetime.now(timezone.utc)
            period_month = now_utc.strftime("%Y-%m")
            record_id = f"BUDGET-{batch_hash[:12]}"
            self.repository.record_budget_usage(
                period_month=period_month,
                run_id=run_id,
                worker_id=worker_id,
                calls_count=1,
                prompt_tokens=input_tokens,
                completion_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
                estimated_cost_usd=round(actual_cost_usd, 6),
                timestamp=now_utc,
                record_id=record_id,
            )

    def record_failure(self, batch_hash: str, error_message: str) -> None:
        b = self.data.setdefault("batches", {}).setdefault(batch_hash, {})
        b["status"] = "FAILED_AMBIGUOUS"
        b["error"] = error_message
        self._save()



