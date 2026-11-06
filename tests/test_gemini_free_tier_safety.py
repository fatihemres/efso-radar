"""Hermetic test suite for Issue #86: Gemini Free Tier Accounting & Safety Guards.

Verifies:
1. Separation of paid-tier equivalent opportunity cost estimates from actual billable cost ($0 Free Tier).
2. Fail-closed rejection of unverified billing status, expired proofs, and paid billing projects.
3. Model eligibility rejection for unsupported models (e.g., gemini-3.8-flash) and shut-down models.
4. Quota exhaustion circuit breakers: monthly call cap, daily requests (RPD), RPM, and unknown dynamic quota.
5. Strict $0.00 / 0 TRY policy enforcement: positive monetary budget overrides fail closed.
6. Search grounding prohibition under Free Tier.
7. Unconditional fail-closed barrier of require_verified_free_tier() (no env or config bypass).
8. Offline batch caching returns results with zero network dispatch and 0 API tokens billed.
9. Ambiguous failures (429/504) recorded as FAILED_AMBIGUOUS in ai_usage_ledger without cache pollution.
10. Preservation of Schema v6 ai_usage_ledger semantics without Schema v7.
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
    BILLING_TIER_DISALLOWED_PAID,
    BILLING_TIER_UNKNOWN,
    BILLING_TIER_VERIFIED_FREE,
    FREE_TIER_DEFAULT_RPD_CAP,
    FREE_TIER_DEFAULT_RPM_CAP,
    FREE_TIER_ELIGIBLE_MODELS,
    FreeTierProjectProof,
    GeminiCostAccounting,
    check_free_tier_safety_guard,
    check_monthly_budget_guard,
    compute_candidate_batch_hash,
    compute_gemini_cost_accounting,
    estimate_call_cost,
    estimate_paid_equivalent_cost,
    is_model_free_tier_eligible,
    require_verified_free_tier,
    resolve_model_pricing,
)
from efso_radar.config import Settings
from efso_radar.domain import Article
from efso_radar.repositories.sqlite import SqliteStateRepository


class TestBillingProvenanceAndAccounting(unittest.TestCase):
    """Verifies separation of paid-tier reference cost, actual billed USD, and proof validation."""

    def setUp(self):
        self.now = datetime(2026, 11, 6, 12, 0, 0, tzinfo=timezone.utc)

    def test_paid_equivalent_positive_while_verified_free_tier_is_zero(self):
        """Paid-tier equivalent estimate (> $0) does NOT imply nonzero billed USD on verified Free Tier."""
        proof = FreeTierProjectProof(
            project_id="efso-radar-free-prj",
            billing_enabled=False,
            free_tier_eligible=True,
            attestation_source="gcloud-billing-api-audit",
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
        # Actual billed USD must be exactly 0.0 on verified Free Tier
        self.assertEqual(accounting.actual_billed_usd, 0.0)
        self.assertEqual(accounting.billing_tier, BILLING_TIER_VERIFIED_FREE)
        self.assertTrue(accounting.is_free_tier_eligible)
        self.assertIn("Verified Free Tier ($0.00 billed)", accounting.reconciliation_notes)

    def test_absent_project_proof_results_in_unknown_billing_tier(self):
        """When project proof is absent (None), actual billed USD is UNKNOWN (None), never assumed $0."""
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
        self.assertIn("Project billing unverified", accounting.reconciliation_notes)

    def test_contradictory_proof_with_billing_enabled_fails_closed(self):
        """Proof indicating billing_enabled=True fails closed as DISALLOWED_PAID."""
        proof = {
            "project_id": "paid-corp-project",
            "billing_enabled": True,  # Paid billing active!
            "free_tier_eligible": True,
            "attestation_source": "cloud-console",
            "verified_at": "2026-11-06T10:00:00Z",
        }
        accounting = compute_gemini_cost_accounting(
            model_name="gemini-3.5-flash-lite",
            input_tokens=1500,
            output_tokens=800,
            project_proof=proof,
            now=self.now,
        )
        self.assertIsNone(accounting.actual_billed_usd)
        self.assertEqual(accounting.billing_tier, BILLING_TIER_DISALLOWED_PAID)
        self.assertIn("Paid projects are strictly forbidden", accounting.reconciliation_notes)

    def test_expired_project_proof_fails_closed(self):
        """Expired project proof fails closed as UNKNOWN_UNVERIFIED."""
        expired_proof = FreeTierProjectProof(
            project_id="efso-radar-free-prj",
            billing_enabled=False,
            free_tier_eligible=True,
            attestation_source="gcloud-billing-api-audit",
            verified_at="2026-11-01T10:00:00Z",
            expires_at="2026-11-05T10:00:00Z",  # Expired yesterday
        )
        accounting = compute_gemini_cost_accounting(
            model_name="gemini-3.5-flash-lite",
            input_tokens=1000,
            output_tokens=500,
            project_proof=expired_proof,
            now=self.now,  # 2026-11-06
        )
        self.assertIsNone(accounting.actual_billed_usd)
        self.assertEqual(accounting.billing_tier, BILLING_TIER_UNKNOWN)
        self.assertIn("expired", accounting.reconciliation_notes)

    def test_incomplete_project_proof_fails_closed(self):
        """Proof missing required provenance fields fails closed."""
        for missing_field in ("project_id", "attestation_source", "verified_at"):
            data = {
                "project_id": "test-prj",
                "billing_enabled": False,
                "free_tier_eligible": True,
                "attestation_source": "audit",
                "verified_at": "2026-11-06T00:00:00Z",
            }
            data[missing_field] = ""
            accounting = compute_gemini_cost_accounting(
                model_name="gemini-3.5-flash-lite",
                input_tokens=1000,
                output_tokens=500,
                project_proof=data,
                now=self.now,
            )
            self.assertIsNone(accounting.actual_billed_usd)
            self.assertEqual(accounting.billing_tier, BILLING_TIER_UNKNOWN)

    def test_search_grounding_invalidates_free_tier_zero_dollar_accounting(self):
        """Even with valid project proof, requesting search grounding sets billed USD to UNKNOWN."""
        proof = FreeTierProjectProof(
            project_id="efso-radar-free-prj",
            billing_enabled=False,
            free_tier_eligible=True,
            attestation_source="audit",
            verified_at="2026-11-06T10:00:00Z",
        )
        accounting = compute_gemini_cost_accounting(
            model_name="gemini-3.5-flash-lite",
            input_tokens=1000,
            output_tokens=500,
            used_search_grounding=True,
            project_proof=proof,
            now=self.now,
        )
        self.assertIsNone(accounting.actual_billed_usd)
        self.assertTrue(accounting.used_search_grounding)
        self.assertIn("search grounding", accounting.reconciliation_notes.lower())

    def test_estimate_paid_equivalent_cost_alias_matches_estimate_call_cost(self):
        """estimate_paid_equivalent_cost is an explicit alias for estimate_call_cost."""
        c1 = estimate_call_cost("gemini-3.5-flash-lite", 2500, 1200)
        c2 = estimate_paid_equivalent_cost("gemini-3.5-flash-lite", 2500, 1200)
        self.assertEqual(c1, c2)


class TestModelEligibilityAndShutdownRejection(unittest.TestCase):
    """Verifies model eligibility for Free Tier and rejection of shut-down / unsupported models."""

    def test_official_free_tier_eligible_models_accepted(self):
        """Standard Flash-Lite and Flash models are officially eligible for Free Tier."""
        for m in ("gemini-3.5-flash-lite", "gemini-3.1-flash-lite", "gemini-2.5-flash", "gemini-2.5-flash-lite"):
            self.assertTrue(
                is_model_free_tier_eligible(m),
                f"Model '{m}' should be recognized as Free Tier eligible.",
            )

    def test_paid_only_model_gemini_3_8_flash_rejected_for_free_tier(self):
        """gemini-3.8-flash has paid pricing only; rejected under Free Tier accounting."""
        self.assertFalse(is_model_free_tier_eligible("gemini-3.8-flash"))
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.8-flash",
            current_monthly_calls=0,
        )
        self.assertFalse(allowed)
        self.assertIn("not eligible for Google AI Studio Free Tier", reason)

    def test_shut_down_models_rejected_at_registry_and_eligibility(self):
        """Shut-down models are rejected at both eligibility check and resolve_model_pricing."""
        shut_down = [
            "gemini-3.1-flash-lite-preview",
            "gemini-2.0-flash",
            "gemini-2.0-flash-lite",
            "gemini-2.0-flash-lite-001",
            "gemini-2.5-flash-lite-preview-09-2025",
        ]
        for m in shut_down:
            self.assertFalse(is_model_free_tier_eligible(m))
            with self.assertRaises(ValueError) as ctx:
                resolve_model_pricing(m)
            self.assertIn("shut down", str(ctx.exception).lower())

    def test_arbitrary_unrecognized_model_rejected(self):
        """Arbitrary unrecognized model strings fail closed."""
        self.assertFalse(is_model_free_tier_eligible("gpt-4o-mini"))
        self.assertFalse(is_model_free_tier_eligible("unknown-model-xyz"))
        allowed, reason = check_free_tier_safety_guard(
            model_name="gpt-4o-mini",
            current_monthly_calls=0,
        )
        self.assertFalse(allowed)
        self.assertIn("not eligible", reason)


class TestQuotaAndCircuitBreakers(unittest.TestCase):
    """Verifies circuit breakers for monthly, daily (RPD), RPM, and dynamic quota exhaustion."""

    def test_monthly_call_cap_exhaustion_fails_closed(self):
        """Reaching or exceeding the configured monthly call cap blocks further calls."""
        # Cap is 300; current calls is 300 -> 300 + 1 > 300 -> DENY
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=300,
            max_monthly_calls=300,
        )
        self.assertFalse(allowed)
        self.assertIn("exceeds Free Tier monthly call cap", reason)

        # 299 calls -> 299 + 1 = 300 <= 300 -> ALLOW
        allowed_ok, _ = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=299,
            max_monthly_calls=300,
        )
        self.assertTrue(allowed_ok)

    def test_daily_requests_cap_rpd_exhaustion_fails_closed(self):
        """Reaching the Free Tier daily request quota (1500 RPD) blocks further calls."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            daily_requests_count=1500,
            daily_requests_cap=1500,
        )
        self.assertFalse(allowed)
        self.assertIn("exceeds Free Tier daily cap", reason)

    def test_rpm_cap_exhaustion_fails_closed(self):
        """Reaching the Free Tier RPM cap (15 RPM) blocks further calls."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            requests_per_minute=15,
            rpm_cap=15,
        )
        self.assertFalse(allowed)
        self.assertIn("reaches or exceeds Free Tier RPM cap", reason)

    def test_unknown_dynamic_quota_fails_closed(self):
        """When dynamic provider quota state is marked unknown, safety guard fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=10,
            dynamic_quota_unknown=True,
        )
        self.assertFalse(allowed)
        self.assertIn("Dynamic provider quota state is UNKNOWN", reason)

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
        """Under Free Tier accounting, positive max_run_cost_usd indicates a paid allowance expectation and fails closed."""
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
        """Under Free Tier accounting, positive max_monthly_cost_usd indicates a paid allowance expectation and fails closed."""
        allowed, reason = check_free_tier_safety_guard(
            model_name="gemini-3.5-flash-lite",
            current_monthly_calls=0,
            max_run_cost_usd=0.0,
            max_monthly_cost_usd=1.50,
        )
        self.assertFalse(allowed)
        self.assertIn("Positive monetary budget override detected", reason)


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

        # Mock the live dispatch barrier to test the API error handling path
        with patch("efso_radar.ai.client.require_verified_free_tier"):
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


if __name__ == "__main__":
    unittest.main()
