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
- Application-level cost tracking in `ai_usage_ledger` is an internal defensive circuit-breaker
  and estimation safeguard; it is NOT a hard provider-side billing guarantee.
"""
from __future__ import annotations

import hashlib
import json
import logging
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


def check_monthly_budget_guard(
    est_run_cost_usd: float,
    current_monthly_cost_usd: float,
    current_monthly_calls: int,
    max_run_cost_usd: float = 0.05,
    max_monthly_cost_usd: float = 1.20,
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


