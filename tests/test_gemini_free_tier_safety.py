"""Hermetic test suite for Issue #86: Gemini Free Tier Accounting & Safety Guards.

Remediation suite addressing independent review findings (P1/P2):
1. Untrusted caller-constructed billing proofs cannot certify actual $0 provider billing.
   - actual_billed_usd is ALWAYS None (UNKNOWN) in offline operation.
   - nominal_free_tier_rate_usd is explicitly labeled as a nominal reference rate ($0.00), NOT an invoice.
   - Missing 'billing_enabled' is never assumed False; fails closed.
   - Future-dated, expired, malformed, or unauthenticated proofs fail closed.
   - Project IDs are never leaked in error/reconciliation messages.
2. Complete quota fail-closed enforcement:
   - Missing ANY quota dimension (RPM, TPM, RPD, monthly calls) returns DENY.
   - Universal constants are NOT treated as provider entitlements.
   - Missing consumption observations return DENY.
   - Default production callers without quota evidence are safely denied.
3. Accurate model classification:
   - gemini-3.5-flash-lite is the sole assessed launch model candidate.
   - gemini-3.8-flash and legacy models are rejected as UNASSESSED for launch, without asserting paid-only status.
   - Shut-down models are rejected fail-closed.
4. Hard live dispatch barrier integrity:
   - require_verified_free_tier() unconditionally raises RuntimeError.
   - Zero network dispatch; offline caching returns picks with 0 tokens and 0 API calls.
   - Ambiguous 429/504 errors recorded in ai_usage_ledger without polluting cache.
   - Schema v6 preserved without Schema v7.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from efso_radar.ai.client import AIEvaluationError, GeminiEditorialEngine
from efso_radar.ai.cost import (
    ASSESSED_LAUNCH_MODELS,
    BILLING_TIER_DISALLOWED_PAID,
    BILLING_TIER_UNKNOWN,
    DOC_EXAMPLE_RPD_CAP,
    DOC_EXAMPLE_RPM_CAP,
    DOC_EXAMPLE_TPM_CAP,
    FreeTierProjectProof,
    GeminiCostAccounting,
    check_free_tier_safety_guard,
    check_monthly_budget_guard,
    compute_candidate_batch_hash,
    compute_gemini_cost_accounting,
    estimate_call_cost,
    estimate_paid_equivalent_cost,
    is_model_assessed_for_launch,
    is_model_free_tier_eligible,
    require_verified_free_tier,
    resolve_model_pricing,
    validate_free_tier_project_proof,
    OBSERVED_PROJECT_RPM_LIMIT,
    OBSERVED_PROJECT_INPUT_TPM_LIMIT,
    OBSERVED_PROJECT_RPD_LIMIT,
    is_pacific_daylight_saving_time,
    get_pacific_calendar_date,
    G3PreDispatchDecision,
    evaluate_g3_activation_preflight,
)
from efso_radar.config import Settings
from efso_radar.domain import Article
from efso_radar.repositories.sqlite import SqliteStateRepository


class TestBillingProvenanceAndAccounting(unittest.TestCase):
    """Verifies separation of paid-tier reference cost, nominal Free Tier rates,
    unobserved provider billing, and adversarial proof rejection."""

    def setUp(self):
        self.now = datetime(2026, 11, 6, 12, 0, 0, tzinfo=timezone.utc)

    def test_paid_equivalent_positive_while_actual_billed_is_unknown(self):
        """Paid-equivalent estimate is positive for economic opportunity cost audit,
        while actual_billed_usd is strictly None (UNKNOWN) and nominal rate is $0.00."""
        proof = FreeTierProjectProof(
            project_id="test-prj",
            billing_enabled=False,
            free_tier_eligible=True,
            attestation_source="mock-audit",
            verified_at="2026-11-06T10:00:00Z",
            expires_at="2026-11-07T10:00:00Z",
        )
        accounting = compute_gemini_cost_accounting(
            model_name="gemini-3.5-flash-lite",
            input_tokens=2000,
            output_tokens=1000,
            project_proof=proof,
            now=self.now,
        )
        # Paid-tier rate: $0.30/1M input + $2.50/1M output -> 0.0006 + 0.0025 = 0.0031 USD
        self.assertGreater(accounting.paid_equivalent_estimate_usd, 0.0)
        self.assertAlmostEqual(accounting.paid_equivalent_estimate_usd, 0.0031, places=5)

        # CRITICAL P1 invariant: actual_billed_usd must NEVER be 0.0 from caller records;
        # it is ALWAYS None (UNKNOWN) in offline operation
        self.assertIsNone(accounting.actual_billed_usd)
        self.assertEqual(accounting.nominal_free_tier_rate_usd, 0.0)
        self.assertEqual(accounting.billing_tier, BILLING_TIER_UNKNOWN)
        self.assertTrue(accounting.is_assessed_launch_model)
        self.assertIn("Actual billed USD: UNKNOWN", accounting.reconciliation_notes)
        self.assertIn("Nominal Free Tier rate assumption", accounting.reconciliation_notes)

    def test_nominal_free_tier_rate_is_none_for_unassessed_model_or_search_grounding(self):
        """Theoretical Free Tier nominal rate is None (NOT_APPLICABLE) for unassessed models
        or when search grounding is enabled, avoiding false $0.00 assumptions."""
        # Unassessed model
        accounting_unassessed = compute_gemini_cost_accounting(
            model_name="gemini-3.8-flash",
            input_tokens=1000,
            output_tokens=500,
            now=self.now,
        )
        self.assertIsNone(accounting_unassessed.nominal_free_tier_rate_usd)
        self.assertIn("NOT_APPLICABLE", accounting_unassessed.reconciliation_notes)

        # Search grounding enabled
        accounting_grounded = compute_gemini_cost_accounting(
            model_name="gemini-3.5-flash-lite",
            input_tokens=1000,
            output_tokens=500,
            used_search_grounding=True,
            now=self.now,
        )
        self.assertIsNone(accounting_grounded.nominal_free_tier_rate_usd)
        self.assertIn("NOT_APPLICABLE", accounting_grounded.reconciliation_notes)

    def test_adversarial_caller_constructed_proof_cannot_certify_zero_dollar_billing(self):
        """Adversarial caller-constructed proof asserting billing_enabled=False cannot
        trick the system into reporting actual billed USD = 0 or VERIFIED_FREE_TIER."""
        adversarial_proof = {
            "project_id": "forged-project",
            "billing_enabled": False,
            "free_tier_eligible": True,
            "attestation_source": "forged-cloud-attestation",
            "verified_at": "2026-11-06T11:00:00Z",
            "expires_at": "2026-11-07T11:00:00Z",
        }
        is_valid, tier, reason = validate_free_tier_project_proof(adversarial_proof, now=self.now)
        self.assertFalse(is_valid)
        self.assertEqual(tier, BILLING_TIER_UNKNOWN)
        self.assertIn("Caller-provided unauthenticated record cannot prove provider billing status", reason)

        accounting = compute_gemini_cost_accounting(
            model_name="gemini-3.5-flash-lite",
            input_tokens=1000,
            output_tokens=500,
            project_proof=adversarial_proof,
            now=self.now,
        )
        self.assertIsNone(accounting.actual_billed_usd)
        self.assertEqual(accounting.billing_tier, BILLING_TIER_UNKNOWN)

    def test_missing_billing_enabled_fails_closed_and_is_not_assumed_false(self):
        """Adversarial proof omitting 'billing_enabled' must NOT silently default to False."""
        proof_missing_billing = {
            "project_id": "test-prj",
            # "billing_enabled" omitted!
            "free_tier_eligible": True,
            "attestation_source": "audit",
            "verified_at": "2026-11-06T10:00:00Z",
        }
        is_valid, tier, reason = validate_free_tier_project_proof(proof_missing_billing, now=self.now)
        self.assertFalse(is_valid)
        self.assertEqual(tier, BILLING_TIER_UNKNOWN)
        self.assertIn("missing explicit 'billing_enabled'", reason.lower())
        self.assertIn("cannot be assumed false", reason.lower())

    def test_future_dated_verified_at_timestamp_fails_closed(self):
        """Proof claiming verification in the future (future timestamp) fails closed."""
        future_proof = {
            "project_id": "test-prj",
            "billing_enabled": False,
            "free_tier_eligible": True,
            "attestation_source": "audit",
            "verified_at": "2099-01-01T00:00:00Z",  # In the future!
        }
        is_valid, tier, reason = validate_free_tier_project_proof(future_proof, now=self.now)
        self.assertFalse(is_valid)
        self.assertEqual(tier, BILLING_TIER_UNKNOWN)
        self.assertIn("in the future", reason.lower())

    def test_expired_project_proof_fails_closed(self):
        """Expired project proof fails closed as UNKNOWN_UNVERIFIED."""
        expired_proof = {
            "project_id": "test-prj",
            "billing_enabled": False,
            "free_tier_eligible": True,
            "attestation_source": "audit",
            "verified_at": "2026-11-01T10:00:00Z",
            "expires_at": "2026-11-05T10:00:00Z",  # Expired yesterday
        }
        is_valid, tier, reason = validate_free_tier_project_proof(expired_proof, now=self.now)
        self.assertFalse(is_valid)
        self.assertEqual(tier, BILLING_TIER_UNKNOWN)
        self.assertIn("expired", reason.lower())

    def test_billing_enabled_true_fails_closed_as_disallowed_paid(self):
        """Proof indicating billing_enabled=True fails closed as DISALLOWED_PAID."""
        paid_proof = {
            "project_id": "paid-corp-project",
            "billing_enabled": True,
            "free_tier_eligible": True,
            "attestation_source": "audit",
            "verified_at": "2026-11-06T10:00:00Z",
        }
        is_valid, tier, reason = validate_free_tier_project_proof(paid_proof, now=self.now)
        self.assertFalse(is_valid)
        self.assertEqual(tier, BILLING_TIER_DISALLOWED_PAID)
        self.assertIn("Paid projects are strictly forbidden", reason)

    def test_project_id_not_leaked_in_proof_validation_messages(self):
        """Sensitive or distinctive project IDs must never be echoed into reason or logs."""
        sensitive_id = "super-secret-internal-gcp-project-12345"
        proof = {
            "project_id": sensitive_id,
            "billing_enabled": True,
            "free_tier_eligible": True,
            "attestation_source": "audit",
            "verified_at": "2026-11-06T10:00:00Z",
        }
        _, _, reason = validate_free_tier_project_proof(proof, now=self.now)
        self.assertNotIn(sensitive_id, reason)

    def test_absent_project_proof_results_in_unknown_billing_tier(self):
        """When project proof is absent (None), actual billed USD is UNKNOWN (None)."""
        accounting = compute_gemini_cost_accounting(
            model_name="gemini-3.5-flash-lite",
            input_tokens=2000,
            output_tokens=1000,
            project_proof=None,
            now=self.now,
        )
        self.assertGreater(accounting.paid_equivalent_estimate_usd, 0.0)
        self.assertIsNone(accounting.actual_billed_usd)
        self.assertEqual(accounting.billing_tier, BILLING_TIER_UNKNOWN)

    def test_estimate_paid_equivalent_cost_alias_matches_estimate_call_cost(self):
        """estimate_paid_equivalent_cost is an explicit alias for estimate_call_cost."""
        c1 = estimate_call_cost("gemini-3.5-flash-lite", 2500, 1200)
        c2 = estimate_paid_equivalent_cost("gemini-3.5-flash-lite", 2500, 1200)
        self.assertEqual(c1, c2)


class TestModelClassificationAndTruth(unittest.TestCase):
    """Verifies launch model assessment and removal of false paid-only claims."""

    def test_gemini_3_5_flash_lite_is_sole_assessed_launch_model(self):
        """gemini-3.5-flash-lite is the sole evaluated and approved candidate for launch."""
        self.assertTrue(is_model_assessed_for_launch("gemini-3.5-flash-lite"))
        self.assertEqual(ASSESSED_LAUNCH_MODELS, frozenset({"gemini-3.5-flash-lite"}))

    def test_gemini_3_8_flash_rejected_as_unassessed_without_claiming_paid_only(self):
        """gemini-3.8-flash has Free Tier in Google AI Studio per official docs.
        It is rejected in Efso Radar because it is UNASSESSED for launch, NOT because it is paid-only."""
        self.assertFalse(is_model_assessed_for_launch("gemini-3.8-flash"))
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.8-flash",
            current_monthly_calls=0,
            rpm_limit=15,
            current_rpm=0,
            tpm_limit=1_000_000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=0,
        )
        self.assertFalse(allowed)
        self.assertIn("not an assessed launch model", reason)
        # Must NOT falsely claim gemini-3.8-flash is paid-only or shut down
        self.assertNotIn("paid-only", reason.lower())
        self.assertNotIn("shut down", reason.lower())

    def test_shut_down_models_rejected_with_shutdown_date(self):
        """Shut-down models are rejected with actionable shutdown date provenance."""
        shut_down = [
            "gemini-3.1-flash-lite-preview",
            "gemini-2.0-flash",
            "gemini-2.0-flash-lite",
            "gemini-2.0-flash-lite-001",
            "gemini-2.5-flash-lite-preview-09-2025",
        ]
        for m in shut_down:
            self.assertFalse(is_model_assessed_for_launch(m))
            with self.assertRaises(ValueError) as ctx:
                resolve_model_pricing(m)
            self.assertIn("shut down", str(ctx.exception).lower())

    def test_arbitrary_unrecognized_model_rejected(self):
        """Arbitrary unrecognized model strings fail closed."""
        self.assertFalse(is_model_assessed_for_launch("gpt-4o-mini"))
        self.assertFalse(is_model_assessed_for_launch("claude-3-haiku"))
        allowed, reason = check_free_tier_safety_guard(
            model_name="gpt-4o-mini",
            current_monthly_calls=0,
            rpm_limit=15,
            current_rpm=0,
            tpm_limit=1_000_000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=0,
        )
        self.assertFalse(allowed)
        self.assertIn("not an assessed launch model", reason)


class TestQuotaAndCircuitBreakers(unittest.TestCase):
    """Verifies fail-closed behavior when ANY quota dimension (RPM, TPM, RPD, monthly)
    is unobserved, missing, or exceeded."""

    def test_missing_rpm_limit_fails_closed(self):
        """Missing verified RPM limit fails closed (universal constants are not entitlements)."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=None,  # Missing!
            current_rpm=2,
            tpm_limit=1_000_000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("Missing verified RPM", reason)
        self.assertIn("Universal constant cannot be assumed as an account entitlement", reason)

    def test_missing_current_rpm_consumption_fails_closed(self):
        """Missing RPM consumption observation fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=None,  # Missing!
            tpm_limit=1_000_000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("Missing current RPM consumption observation", reason)

    def test_rpm_exhaustion_fails_closed(self):
        """Reaching or exceeding the verified RPM limit blocks calls."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=15,  # Reached limit!
            tpm_limit=1_000_000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("reaches or exceeds verified RPM limit", reason)

    def test_missing_tpm_limit_fails_closed(self):
        """Missing verified TPM limit fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=None,  # Missing!
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("Missing verified TPM", reason)

    def test_missing_estimated_batch_tokens_for_tpm_fails_closed(self):
        """Missing estimated batch tokens fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=5000,
            estimated_batch_tokens=None,  # Missing!
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("Missing estimated batch token count", reason)

    def test_tpm_exhaustion_fails_closed(self):
        """Projected batch tokens exceeding TPM limit fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=999_000,
            estimated_batch_tokens=2000,  # 999k + 2k = 1,001,000 > 1,000,000!
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("exceeds verified TPM limit", reason)

    def test_missing_rpd_limit_fails_closed(self):
        """Missing verified RPD limit fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=5000,
            estimated_batch_tokens=1000,
            rpd_limit=None,  # Missing!
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("Missing verified RPD", reason)

    def test_missing_current_rpd_consumption_fails_closed(self):
        """Missing current RPD consumption fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=5000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=None,  # Missing!
        )
        self.assertFalse(allowed)
        self.assertIn("Missing current RPD consumption observation", reason)

    def test_rpd_exhaustion_fails_closed(self):
        """Reaching the RPD cap blocks further calls."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=5000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=1500,  # 1500 + 1 > 1500 -> DENY
        )
        self.assertFalse(allowed)
        self.assertIn("exceeds verified RPD limit", reason)

    def test_missing_monthly_call_count_fails_closed(self):
        """Missing monthly call count observation fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=None,  # Missing!
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("Missing monthly call count observation", reason)

    def test_monthly_call_cap_exhaustion_fails_closed(self):
        """Reaching the monthly call cap (300) blocks further calls."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=300,  # 300 + 1 > 300
            max_monthly_calls=300,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed)
        self.assertIn("exceeds Free Tier monthly call cap", reason)

    def test_missing_current_tpm_consumption_fails_closed(self):
        """Missing current TPM consumption observation fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=None,  # Missing!
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
            allow_synthetic_simulation=True,
        )
        self.assertFalse(allowed)
        self.assertIn("Missing current TPM consumption observation", reason)

        # Also fails closed under default (non-synthetic) caller mode
        allowed_default, reason_default = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=None,
            estimated_batch_tokens=1000,
            rpd_limit=1500,
            current_rpd=10,
        )
        self.assertFalse(allowed_default)
        self.assertIn("Missing current TPM consumption observation", reason_default)

    def test_caller_supplied_numerical_quotas_fail_closed_without_synthetic_flag(self):
        """Caller-supplied numerical quota values cannot certify provider quotas for real dispatch."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            max_monthly_calls=300,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=5000,
            estimated_batch_tokens=1500,
            rpd_limit=1500,
            current_rpd=50,
            # allow_synthetic_simulation defaults to False
        )
        self.assertFalse(allowed)
        self.assertIn("Caller-supplied numerical quota values cannot certify provider quotas", reason)

    def test_synthetic_mathematical_quota_calculation_within_limits_returns_synthetic_ok(self):
        """When explicitly marked as synthetic simulation for pure mathematical unit-testing,
        numbers within bounds return synthetic OK."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            max_monthly_calls=300,
            rpm_limit=15,
            current_rpm=2,
            tpm_limit=1_000_000,
            current_tpm=5000,
            estimated_batch_tokens=1500,
            rpd_limit=1500,
            current_rpd=50,
            allow_synthetic_simulation=True,
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "OK (synthetic simulation only - non-authorizing)")

    def test_default_production_caller_receives_deny_due_to_unobserved_quotas(self):
        """Production callers without dynamic quota observations receive DENY by default."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            # No RPM, TPM, or RPD limits/counts passed!
        )
        self.assertFalse(allowed)
        self.assertIn("Missing verified RPM", reason)

    def test_search_grounding_enabled_fails_closed(self):
        """Search grounding is not free under Free Tier and must fail closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=0,
            enable_search_grounding=True,
        )
        self.assertFalse(allowed)
        self.assertIn("Search grounding is enabled", reason)
        self.assertIn("Strict $0.00 / 0 TRY policy", reason)


class TestPositiveBudgetOverrideRejection(unittest.TestCase):
    """Verifies that positive monetary budget overrides fail closed under strict Free Tier policy."""

    def test_positive_max_run_cost_override_fails_closed(self):
        """Positive max_run_cost_usd indicates a paid allowance expectation and fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=0,
            max_run_cost_usd=0.05,
            max_monthly_cost_usd=0.0,
        )
        self.assertFalse(allowed)
        self.assertIn("Positive monetary budget override detected", reason)
        self.assertIn("Strict Free Tier launch policy requires $0.00 / 0 TRY monetary ceilings", reason)

    def test_positive_max_monthly_cost_override_fails_closed(self):
        """Positive max_monthly_cost_usd indicates a paid allowance expectation and fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=0,
            max_run_cost_usd=0.0,
            max_monthly_cost_usd=1.50,
        )
        self.assertFalse(allowed)
        self.assertIn("Positive monetary budget override detected", reason)

    def test_evaluate_rejects_positive_operational_budget_ceiling_even_if_hard_barrier_mocked(self):
        """Even if require_verified_free_tier is mocked, evaluate() directly rejects positive
        operational monetary ceilings under strict $0.00 / 0 TRY policy."""
        settings = Settings(
            gemini_api_key="synthetic-key",
            gemini_model="gemini-3.5-flash-lite",
            max_estimated_run_cost_usd=0.05,
            max_monthly_ai_cost_usd=1.20,
        )
        engine = GeminiEditorialEngine(settings)
        engine.client = MagicMock()
        article = Article.create("Test", "https://example.com/test", "Source", datetime.now(timezone.utc))

        with patch("efso_radar.ai.client.require_verified_free_tier"):
            with self.assertRaises(AIEvaluationError) as ctx:
                engine.evaluate([article], [], datetime.now(timezone.utc))
            self.assertIn("Positive operational monetary budget override rejected", str(ctx.exception))

        engine.client.models.generate_content.assert_not_called()


class TestDispatchSafetyAndLedgerContract(unittest.TestCase):
    """Verifies hard live barrier, zero network dispatch, caching, and ledger preservation."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_safety.db"
        self.repo = SqliteStateRepository(self.db_path)
        self.now = datetime(2026, 11, 6, 12, 0, 0, tzinfo=timezone.utc)
        self.article = Article.create("Başlık", "https://example.com/h1", "Kaynak", self.now)

    def tearDown(self):
        self.repo.close()
        self.tmpdir.cleanup()

    def test_require_verified_free_tier_cannot_be_bypassed_by_env_or_config(self):
        """require_verified_free_tier() unconditionally raises RuntimeError; no env or config bypass."""
        with patch.dict(os.environ, {"GEMINI_FREE_TIER_VERIFIED": "1", "ALLOW_LIVE_AI": "true"}):
            with self.assertRaises(RuntimeError) as ctx:
                require_verified_free_tier()
            self.assertIn("Real Gemini API calls blocked", str(ctx.exception))
            self.assertIn("No paid fallback", str(ctx.exception))

    def test_zero_network_dispatch_when_unverified(self):
        """GeminiEditorialEngine.evaluate() raises AIEvaluationError before any network dispatch."""
        settings = Settings(
            gemini_api_key="synthetic-key",
            gemini_model="gemini-3.5-flash-lite",
            max_estimated_run_cost_usd=0.0,
            max_monthly_ai_cost_usd=0.0,
        )
        engine = GeminiEditorialEngine(settings, repository=self.repo)
        engine.client = MagicMock()

        with self.assertRaisesRegex(AIEvaluationError, "free-tier/no-billing"):
            engine.evaluate([self.article], [], self.now)

        engine.client.models.generate_content.assert_not_called()

    def test_quota_guard_denies_evaluate_even_if_dispatch_barrier_patched_under_default_settings(self):
        """Defense-in-depth: If require_verified_free_tier were bypassed under default $0.00 settings,
        check_free_tier_safety_guard still halts evaluate() because dynamic quotas are unobserved."""
        settings = Settings(
            gemini_api_key="synthetic-key",
            gemini_model="gemini-3.5-flash-lite",
            max_estimated_run_cost_usd=0.0,
            max_monthly_ai_cost_usd=0.0,
        )
        engine = GeminiEditorialEngine(settings, repository=self.repo)
        engine.client = MagicMock()

        with patch("efso_radar.ai.client.require_verified_free_tier"):
            with self.assertRaises(AIEvaluationError) as ctx:
                engine.evaluate([self.article], [], self.now)
            self.assertIn("Missing verified RPM", str(ctx.exception))

        engine.client.models.generate_content.assert_not_called()

    def test_offline_caching_returns_picks_with_zero_network_dispatch_and_zero_tokens(self):
        """When batch is cached in ai_usage_ledger, evaluate() returns cached picks with 0 API calls."""
        batch_hash = compute_candidate_batch_hash([self.article.id], "gemini-3.5-flash-lite")
        cached_json = (
            '{"picks": [{"article_ids": ["' + self.article.id + '"], '
            '"headline_tr": "Önbellekten Gelen Haber", "cover_title_tr": "KAPAK", '
            '"caption_idea_tr": "Açıklama", "relevance_reason_tr": "Önemli gelişme", '
            '"why_efso_tr": "Çok konuşuluyor", "score": 90, "kind": "NEW", '
            '"suggested_format": "POST", "category": "GLOBAL", '
            '"verification_status": "OFFICIAL_CONFIRMATION"}]}'
        )

        # Pre-seed cache
        self.repo.record_ai_usage(
            usage_id="AI-CACHE-TEST",
            run_mode="PRODUCTION",
            model_name="gemini-3.5-flash-lite",
            batch_hash=batch_hash,
            candidate_count=1,
            input_tokens=1500,
            output_tokens=300,
            estimated_cost_usd=0.0012,
            response_json=cached_json,
            created_at=self.now,
        )

        settings = Settings(
            gemini_api_key="synthetic-key",
            gemini_model="gemini-3.5-flash-lite",
        )
        engine = GeminiEditorialEngine(settings, repository=self.repo)
        engine.client = MagicMock()

        # Execute evaluation: must return cached picks without calling generate_content
        picks = engine.evaluate([self.article], [], self.now)
        self.assertEqual(len(picks), 1)
        self.assertEqual(picks[0].headline_tr, "Önbellekten Gelen Haber")
        engine.client.models.generate_content.assert_not_called()

    def test_ambiguous_failure_records_failed_ambiguous_without_polluting_cache(self):
        """When API call fails with 429/504, FAILED_AMBIGUOUS is recorded in ai_usage_ledger,
        cache is not polluted, and AIEvaluationError is raised."""
        settings = Settings(
            gemini_api_key="synthetic-key",
            gemini_model="gemini-3.5-flash-lite",
            max_estimated_run_cost_usd=0.0,
            max_monthly_ai_cost_usd=0.0,
            max_monthly_ai_calls=10,
        )
        engine = GeminiEditorialEngine(settings, repository=self.repo)
        engine.client = MagicMock()
        engine.client.models.generate_content.side_effect = RuntimeError("504 Gateway Timeout")

        # Mock the live dispatch barrier and quota guard to test the API error handling path
        with patch("efso_radar.ai.client.require_verified_free_tier"), \
             patch("efso_radar.ai.client.check_free_tier_safety_guard", return_value=(True, "OK")):
            with self.assertRaises(AIEvaluationError):
                engine.evaluate([self.article], [], self.now)

        with self.repo._connection() as conn:
            row = conn.execute("SELECT run_mode, response_json, estimated_cost_usd FROM ai_usage_ledger").fetchone()
            self.assertEqual(row["run_mode"], "FAILED_AMBIGUOUS")
            self.assertIsNone(row["response_json"])
            # Estimated cost is recorded for audit even on failure
            self.assertGreater(float(row["estimated_cost_usd"]), 0.0)

    def test_ledger_preserves_schema_v6_without_schema_v7(self):
        """ai_usage_ledger preserves Schema v6 columns and does not introduce Schema v7."""
        self.assertEqual(self.repo.get_schema_version(), 6)
        with self.repo._connection() as conn:
            cursor = conn.execute("PRAGMA table_info(ai_usage_ledger)")
            cols = {row["name"]: row["type"] for row in cursor.fetchall()}

        expected_cols = {
            "usage_id": "TEXT",
            "run_mode": "TEXT",
            "model_name": "TEXT",
            "batch_hash": "TEXT",
            "candidate_count": "INTEGER",
            "input_tokens": "INTEGER",
            "output_tokens": "INTEGER",
            "estimated_cost_usd": "REAL",
            "response_json": "TEXT",
            "created_at": "TEXT",
        }
        for col_name, col_type in expected_cols.items():
            self.assertIn(col_name, cols)
            self.assertEqual(cols[col_name].upper(), col_type)


class TestG3ActivationContractAndPreflight(unittest.TestCase):
    """Hermetic test suite for Issue #88: G3 Gemini activation safety contract and preflight gates."""

    def setUp(self):
        self.now_utc = datetime(2026, 11, 6, 12, 0, 0, tzinfo=timezone.utc)
        self.valid_synthetic_proof = {
            "project_id": "test-gemini-project",
            "billing_enabled": False,
            "free_tier_eligible": True,
            "attestation_source": "synthetic-offline-test",
            "verified_at": "2026-11-06T11:00:00Z",
            "expires_at": "2026-11-07T11:00:00Z",
        }
        self.valid_synthetic_quota = {
            "current_monthly_calls": 5,
            "max_monthly_calls": 300,
            "rpm_limit": 15,
            "current_rpm": 2,
            "tpm_limit": 1_000_000,
            "current_tpm": 10_000,
            "estimated_batch_tokens": 1_500,
            "input_tpm_limit": 250_000,
            "current_input_tpm": 5_000,
            "estimated_input_tokens": 1_200,
            "rpd_limit": 500,
            "current_rpd": 20,
        }

    def test_observed_project_limits_constants(self):
        """Observed project limits are defined as project-level baselines (not universal entitlements)."""
        self.assertEqual(OBSERVED_PROJECT_RPM_LIMIT, 15)
        self.assertEqual(OBSERVED_PROJECT_INPUT_TPM_LIMIT, 250_000)
        self.assertEqual(OBSERVED_PROJECT_RPD_LIMIT, 500)

    def test_pacific_calendar_date_and_midnight_reset(self):
        """RPD calendar date resets at midnight US Pacific Time (PT), correctly accounting for PST and PDT."""
        # 1. Winter (PST, UTC-8): 2026-11-06 07:59:59 UTC is 2026-11-05 23:59:59 PST
        dt_pre_reset_winter = datetime(2026, 11, 6, 7, 59, 59, tzinfo=timezone.utc)
        self.assertFalse(is_pacific_daylight_saving_time(dt_pre_reset_winter))
        self.assertEqual(get_pacific_calendar_date(dt_pre_reset_winter), "2026-11-05")

        # Exactly at 08:00:00 UTC it becomes midnight PST (2026-11-06)
        dt_post_reset_winter = datetime(2026, 11, 6, 8, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(get_pacific_calendar_date(dt_post_reset_winter), "2026-11-06")

        # 2. Summer (PDT, UTC-7): 2026-07-15 06:59:59 UTC is 2026-07-14 23:59:59 PDT
        dt_pre_reset_summer = datetime(2026, 7, 15, 6, 59, 59, tzinfo=timezone.utc)
        self.assertTrue(is_pacific_daylight_saving_time(dt_pre_reset_summer))
        self.assertEqual(get_pacific_calendar_date(dt_pre_reset_summer), "2026-07-14")

        # Exactly at 07:00:00 UTC it becomes midnight PDT (2026-07-15)
        dt_post_reset_summer = datetime(2026, 7, 15, 7, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(get_pacific_calendar_date(dt_post_reset_summer), "2026-07-15")

    def test_input_tpm_missing_consumption_fails_closed(self):
        """When input_tpm_limit is specified, missing current_input_tpm fails closed."""
        is_ok, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=1,
            rpm_limit=15,
            current_rpm=1,
            input_tpm_limit=250_000,
            current_input_tpm=None,  # Missing!
            estimated_input_tokens=500,
            rpd_limit=500,
            current_rpd=1,
            allow_synthetic_simulation=True,
        )
        self.assertFalse(is_ok)
        self.assertIn("Missing current input TPM consumption", reason)

    def test_input_tpm_exhaustion_fails_closed(self):
        """When projected input tokens exceed input_tpm_limit, check fails closed."""
        is_ok, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=1,
            rpm_limit=15,
            current_rpm=1,
            input_tpm_limit=250_000,
            current_input_tpm=245_000,
            estimated_input_tokens=6_000,  # 245k + 6k = 251k > 250k!
            rpd_limit=500,
            current_rpd=1,
            allow_synthetic_simulation=True,
        )
        self.assertFalse(is_ok)
        self.assertIn("exceeds verified input TPM limit", reason)

    def test_g3_preflight_denies_without_human_po_go(self):
        """Preflight gates pass all machine checks, but DENY when PO Live GO is not granted."""
        decision = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            enable_search_grounding=False,
            max_run_cost_usd=0.0,
            max_monthly_cost_usd=0.0,
            project_proof=self.valid_synthetic_proof,
            quota_evidence=self.valid_synthetic_quota,
            is_po_live_go_granted=False,  # PO GO NOT GRANTED!
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, "DENY_NO_PO_GO")
        self.assertIn("PO Live GO", decision.reason)
        self.assertIn("Machine gates cannot self-authorize", decision.reason)

    def test_g3_preflight_denies_unassessed_model(self):
        """Preflight rejects models that are not the sole assessed launch candidate."""
        decision = evaluate_g3_activation_preflight(
            model_name="gemini-3.8-flash",
            is_po_live_go_granted=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, "DENY_UNASSESSED_MODEL")
        self.assertIn("unassessed for launch", decision.reason)

    def test_g3_preflight_denies_search_grounding(self):
        """Preflight rejects search grounding under strict $0.00 Free Tier policy."""
        decision = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            enable_search_grounding=True,
            is_po_live_go_granted=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, "DENY_SEARCH_GROUNDING")
        self.assertIn("Search grounding is enabled", decision.reason)

    def test_g3_preflight_denies_positive_budget_ceiling(self):
        """Preflight rejects positive monetary ceilings."""
        decision = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            max_run_cost_usd=0.05,
            is_po_live_go_granted=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, "DENY_POSITIVE_BUDGET")

    def test_g3_preflight_denies_paid_project(self):
        """Preflight rejects projects marked billing_enabled=True as DISALLOWED_PAID."""
        paid_proof = dict(self.valid_synthetic_proof)
        paid_proof["billing_enabled"] = True
        decision = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            project_proof=paid_proof,
            is_po_live_go_granted=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, "DENY_PAID_PROJECT")
        self.assertEqual(decision.billing_tier, BILLING_TIER_DISALLOWED_PAID)

    def test_g3_preflight_denies_unverified_billing_by_default(self):
        """Preflight denies unauthenticated caller proofs when allow_synthetic_simulation is False."""
        decision = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            project_proof=self.valid_synthetic_proof,
            quota_evidence=self.valid_synthetic_quota,
            is_po_live_go_granted=True,
            allow_synthetic_simulation=False,  # Default production / live evaluation mode
            now=self.now_utc,
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, "DENY_UNVERIFIED_BILLING")
        self.assertEqual(decision.billing_tier, BILLING_TIER_UNKNOWN)

    def test_g3_preflight_denies_missing_or_exhausted_quota(self):
        """Preflight denies when quota evidence is missing or exceeded."""
        # 1. Missing quota evidence
        decision_missing = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            project_proof=self.valid_synthetic_proof,
            quota_evidence=None,
            is_po_live_go_granted=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertFalse(decision_missing.allowed)
        self.assertEqual(decision_missing.status_code, "DENY_QUOTA_UNVERIFIED")

        # 2. Exhausted quota (RPD exceeded)
        exhausted_quota = dict(self.valid_synthetic_quota)
        exhausted_quota["current_rpd"] = 500  # 500 + 1 = 501 > 500
        decision_exhausted = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            project_proof=self.valid_synthetic_proof,
            quota_evidence=exhausted_quota,
            is_po_live_go_granted=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertFalse(decision_exhausted.allowed)
        self.assertEqual(decision_exhausted.status_code, "DENY_QUOTA_EXHAUSTED")

    def test_g3_preflight_tracks_erd017_fencing_requirement(self):
        """Preflight decision accurately notes ERD-017 cross-runner fencing for multi-runner mode."""
        decision = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            project_proof=self.valid_synthetic_proof,
            quota_evidence=self.valid_synthetic_quota,
            is_po_live_go_granted=True,
            is_multi_runner_environment=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.status_code, "ALLOW_PREFLIGHT_READY")
        self.assertTrue(decision.requires_erd017_fencing)

    def test_evaluate_still_unconditionally_denied_even_if_g3_preflight_ready(self):
        """GeminiEditorialEngine.evaluate() still unconditionally blocks API dispatch
        via require_verified_free_tier() even when preflight decision returns ALLOW_PREFLIGHT_READY."""
        preflight = evaluate_g3_activation_preflight(
            model_name="gemini-3.5-flash-lite",
            project_proof=self.valid_synthetic_proof,
            quota_evidence=self.valid_synthetic_quota,
            is_po_live_go_granted=True,
            allow_synthetic_simulation=True,
            now=self.now_utc,
        )
        self.assertTrue(preflight.allowed)
        self.assertEqual(preflight.status_code, "ALLOW_PREFLIGHT_READY")

        # Now test that evaluate() cannot be bypassed
        settings = Settings(
            gemini_api_key="synthetic-key",
            gemini_model="gemini-3.5-flash-lite",
            max_estimated_run_cost_usd=0.0,
            max_monthly_ai_cost_usd=0.0,
            max_monthly_ai_calls=10,
        )
        engine = GeminiEditorialEngine(settings)
        engine.client = MagicMock()

        article = Article.create("Test", "https://example.com/test", "Source", self.now_utc)

        with self.assertRaises(AIEvaluationError) as ctx:
            engine.evaluate([article], [], self.now_utc)
        self.assertIn("free-tier/no-billing", str(ctx.exception))
        engine.client.models.generate_content.assert_not_called()


if __name__ == "__main__":
    unittest.main()
