"""Hermetic unit tests for Phase 5B.3 / Issue #61: Bounded fail-closed Gemini shadow preflight.

Verifies:
1. Default offline execution fails closed without network, producing structured preview.
2. P1 Security Proof: Network-capable SDK client injection is strictly forbidden and never invoked.
3. P2 Data Integrity: Only the fixed historical public sample (SHADOW_SAMPLE_ID) is permitted;
   arbitrary custom inputs are rejected fail-closed.
4. Model enforcement: strictly gemini-3.5-flash-lite only.
5. Grounding forbidden: enable_search_grounding=True is rejected.
6. Single-shot execution guard: repeated synthetic evaluations on same runner instance are blocked.
7. Synthetic JSON evaluation validates through application business rules fail-closed.
8. Application-level validation prevents hallucinated candidate IDs.
9. All reports and synthetic outputs carry explicit NOT REAL GEMINI disclaimers.
10. Output sanitization strips API keys, tokens, and credentials.
11. require_verified_free_tier() remains an unconditional block.
"""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.cost import require_verified_free_tier
from efso_radar.ai.shadow_preflight import (
    ALLOWED_SHADOW_MODEL,
    NOT_REAL_GEMINI_DISCLAIMER,
    SHADOW_SAMPLE_AS_OF_UTC,
    SHADOW_SAMPLE_ID,
    OneShotShadowPreflightRunner,
    get_historical_public_shadow_sample,
    sanitize_report_text,
)
from efso_radar.config import Settings
from efso_radar.domain import Article, Market


class TestGeminiShadowPreflight(unittest.TestCase):
    """Test suite for Issue #61 one-shot Gemini editorial shadow preflight harness."""

    def setUp(self) -> None:
        self.sample = get_historical_public_shadow_sample()
        self.settings = Settings(
            gemini_model=ALLOWED_SHADOW_MODEL,
            enable_search_grounding=False,
            max_output_tokens=2048,
        )

    def test_offline_preflight_default_fails_closed_without_network(self) -> None:
        """Default runner must fail closed via require_verified_free_tier() with zero network calls."""
        runner = OneShotShadowPreflightRunner(self.settings)
        result = runner.run_preflight()

        self.assertEqual(result.status, "PREFLIGHT_BLOCKED_OFFLINE_ONLY")
        self.assertEqual(result.ai_result, "AI_RESULT_NOT_AVAILABLE")
        self.assertEqual(result.provider_status, "PROVIDER_CALL_NOT_AUTHORIZED")
        self.assertEqual(result.free_tier_gate_status, "BLOCKED_BY_REQUIRE_VERIFIED_FREE_TIER")
        self.assertEqual(result.model_name, ALLOWED_SHADOW_MODEL)
        self.assertEqual(result.call_count, 0)
        self.assertFalse(result.sdk_network_executed)
        self.assertEqual(result.actual_cost_usd, 0.0)
        self.assertGreater(result.theoretical_paid_equivalent_usd, 0.0)
        self.assertTrue(result.grounding_forbidden)
        self.assertEqual(result.candidate_count, 7)
        self.assertEqual(result.sample_id, SHADOW_SAMPLE_ID)
        self.assertIn("NOT REAL GEMINI", result.disclaimer)
        self.assertEqual(len(result.decisions), 0)
        self.assertIsNotNone(result.error_message)
        self.assertIn("Real Gemini API calls blocked", result.error_message)

    def test_p1_regression_network_capable_client_injection_is_rejected_and_never_invoked(self) -> None:
        """Negative security proof: Passing a real or mock SDK client is rejected without invoking it."""
        mock_sdk_client = MagicMock()
        mock_sdk_client.models = MagicMock()
        mock_sdk_client.models.generate_content = MagicMock()

        runner = OneShotShadowPreflightRunner(self.settings)

        # Attempting to pass client or mock_client must raise AIEvaluationError
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.run_preflight(client=mock_sdk_client)
        self.assertIn("Stage A preflight strictly forbids network-capable client injection", str(ctx.exception))

        with self.assertRaises(AIEvaluationError) as ctx2:
            runner.run_preflight(mock_client=mock_sdk_client)
        self.assertIn("Stage A preflight strictly forbids network-capable client injection", str(ctx2.exception))

        # Crucial proof: generate_content was NEVER called
        mock_sdk_client.models.generate_content.assert_not_called()
        self.assertEqual(runner.call_count, 0)

    def test_p2_regression_arbitrary_custom_candidates_are_rejected(self) -> None:
        """Stage A preflight must reject arbitrary unverified candidate inputs."""
        custom_candidate = Article.create(
            title="Özel ve Yetkisiz Bir Haber",
            url="https://example.com/unauthorized-news",
            source="Bilinmeyen Kaynak",
            published_at=datetime(2026, 10, 30, 10, 0, tzinfo=timezone.utc),
            snippet="Yetkisiz özel içerik.",
        )
        runner = OneShotShadowPreflightRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.run_preflight(candidates=[custom_candidate])
        self.assertIn("Arbitrary candidates rejected: Stage A is strictly restricted", str(ctx.exception))

    def test_historical_public_sample_integrity_and_privacy(self) -> None:
        """Sample must contain exactly 7 public items with source URLs, published timestamps, and no private data."""
        self.assertEqual(len(self.sample), 7)

        # Check all items have public metadata
        for item in self.sample:
            self.assertTrue(item.title)
            self.assertTrue(item.url)
            self.assertTrue(item.url.startswith("http"))
            self.assertTrue(item.source)
            self.assertIsNotNone(item.published_at)
            self.assertTrue(item.snippet)
            self.assertIn(item.market, (Market.TR, Market.EN))

        # Check cross-publisher same-event pair
        ercel_items = [i for i in self.sample if "Hande Erçel" in i.title or "Hande Erçel" in i.snippet]
        self.assertGreaterEqual(len(ercel_items), 2, "Must contain Hande Erçel same-event pair")
        ercel_sources = {i.source for i in ercel_items}
        self.assertIn("Habertürk", ercel_sources)
        self.assertIn("Reflex Haber", ercel_sources)

        # Check sensitive story pair
        sansar_items = [i for i in self.sample if "Sansar Salvo" in i.title or "Sansar Salvo" in i.snippet]
        self.assertGreaterEqual(len(sansar_items), 2, "Must contain Sansar Salvo sensitive pair")
        sansar_sources = {i.source for i in sansar_items}
        self.assertIn("Habertürk", sansar_sources)
        self.assertIn("Sözcü", sansar_sources)

        # Check privacy: no private spreadsheet fields or calibration notes exist on Article
        for item in self.sample:
            self.assertFalse(hasattr(item, "owner_notes"))
            self.assertFalse(hasattr(item, "owner_decision"))
            self.assertFalse(hasattr(item, "review_id"))

    def test_model_restriction_enforces_gemini_3_5_flash_lite_only(self) -> None:
        """Any model other than gemini-3.5-flash-lite must be rejected immediately."""
        disallowed_models = [
            "gemini-1.5-pro",
            "gemini-2.0-flash",
            "gemini-2.5-flash-lite-preview-09-2025",
            "gemini-3.8-flash",
            "gpt-4o",
        ]
        for bad_model in disallowed_models:
            bad_settings = Settings(gemini_model=bad_model)
            runner = OneShotShadowPreflightRunner(bad_settings)
            with self.assertRaises(AIEvaluationError) as ctx:
                runner.run_preflight()
            self.assertIn("Shadow preflight only permits model", str(ctx.exception))

    def test_search_grounding_forbidden(self) -> None:
        """Search grounding must be rejected fail-closed in shadow preflight."""
        bad_settings = Settings(
            gemini_model=ALLOWED_SHADOW_MODEL,
            enable_search_grounding=True,
        )
        runner = OneShotShadowPreflightRunner(bad_settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.run_preflight()
        self.assertIn("Search grounding is strictly forbidden", str(ctx.exception))

    def test_empty_candidate_list_fails_closed(self) -> None:
        """Empty candidate list must fail closed."""
        runner = OneShotShadowPreflightRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.run_preflight(candidates=[])
        self.assertIn("Candidate list is empty", str(ctx.exception))

    def test_single_shot_execution_blocks_repeated_calls(self) -> None:
        """Runner must enforce max 1 evaluation; repeated call on same runner instance is blocked."""
        runner = OneShotShadowPreflightRunner(self.settings)
        payload = json.dumps({"picks": [], "run_editorial_summary_tr": "Boş özet."})

        # 1. First execution succeeds
        result1 = runner.evaluate_synthetic_response(payload)
        self.assertEqual(result1.call_count, 1)
        self.assertEqual(runner.call_count, 1)

        # 2. Second execution must be blocked
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.evaluate_synthetic_response(payload)
        self.assertIn("Repeated execution blocked: shadow harness enforces single-shot execution", str(ctx.exception))
        self.assertEqual(runner.call_count, 1, "Call count must not increment after block")

    def test_synthetic_malformed_json_fails_closed(self) -> None:
        """Malformed JSON response must fail closed."""
        runner = OneShotShadowPreflightRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.evaluate_synthetic_response("{not valid json at all")
        self.assertIn("Malformed synthetic structured output", str(ctx.exception))

    def test_synthetic_empty_payload_fails_closed(self) -> None:
        """Empty synthetic response payload must fail closed."""
        runner = OneShotShadowPreflightRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.evaluate_synthetic_response("")
        self.assertIn("Synthetic response payload is empty", str(ctx.exception))

    def test_synthetic_hallucinated_candidate_id_rejected(self) -> None:
        """Pick referencing a fabricated/hallucinated candidate ID must be rejected by application validation."""
        payload = json.dumps({
            "picks": [
                {
                    "article_ids": ["hallucinated-article-id-999"],
                    "score": 95,
                    "kind": "NEW",
                    "verification_status": "REPUTABLE_REPORTING",
                    "headline_tr": "Uydurma Bir Haber Başlığı",
                    "cover_title_tr": "UYDURMA BAŞLIK",
                    "caption_idea_tr": "Uydurma metin açıklaması.",
                    "why_efso_tr": "Test için uyduruldu.",
                    "suggested_format": "POST",
                    "category": "GLOBAL",
                }
            ],
            "run_editorial_summary_tr": "Tek uydurma seçim.",
        })
        runner = OneShotShadowPreflightRunner(self.settings)
        result = runner.evaluate_synthetic_response(payload)

        self.assertEqual(result.status, "SYNTHETIC_EVALUATION_COMPLETED_TEST_ONLY")
        self.assertEqual(len(result.decisions), 0, "Hallucinated candidate ID must be rejected")

    def test_synthetic_valid_pick_approved_with_disclaimer(self) -> None:
        """Valid pick referencing authentic candidate ID must be approved, carrying explicit NOT REAL GEMINI disclaimer."""
        real_cand = self.sample[0]
        payload = json.dumps({
            "picks": [
                {
                    "article_ids": [real_cand.id],
                    "score": 89,
                    "kind": "NEW",
                    "verification_status": "REPUTABLE_REPORTING",
                    "headline_tr": "Devin Booker ve Hande Erçel Sosyal Medyada Gündem Oldu",
                    "cover_title_tr": "DİKKAT ÇEKEN BEĞENİ",
                    "caption_idea_tr": "Devin Booker'ın Hande Erçel'in paylaşımını beğenmesi magazin dünyasında konuşuluyor.",
                    "why_efso_tr": "A-list pop-kültür etkileşimi, yüksek viral potansiyel.",
                    "suggested_format": "POST",
                    "category": "YERLI",
                }
            ],
            "run_editorial_summary_tr": "1 geçerli seçim yapıldı.",
        })

        runner = OneShotShadowPreflightRunner(self.settings)
        result = runner.evaluate_synthetic_response(payload)

        self.assertEqual(result.status, "SYNTHETIC_EVALUATION_COMPLETED_TEST_ONLY")
        self.assertEqual(len(result.decisions), 1)
        dec = result.decisions[0]
        self.assertEqual(dec["headline_tr"], "Devin Booker ve Hande Erçel Sosyal Medyada Gündem Oldu")
        self.assertEqual(dec["score"], 89)
        self.assertEqual(result.actual_cost_usd, 0.0)
        self.assertIn("NOT REAL GEMINI", result.disclaimer)

    def test_sanitize_report_text_utility(self) -> None:
        """sanitize_report_text must redact Google API keys, bearer tokens, and credentials."""
        self.assertEqual(sanitize_report_text(""), "")
        self.assertEqual(
            sanitize_report_text("Found key AIzaSyA12345678901234567890123456789012 in header"),
            "Found key [REDACTED_API_KEY] in header",
        )
        self.assertEqual(
            sanitize_report_text("Authorization: Bearer secret-token-value-xyz"),
            "Authorization: Bearer [REDACTED_TOKEN]",
        )
        self.assertEqual(
            sanitize_report_text("api_key='my-super-secret-key-123'"),
            "api_key=[REDACTED]",
        )

    def test_markdown_report_sanitization(self) -> None:
        """to_markdown_report() must render clean report with NOT REAL GEMINI disclaimer and zero secrets."""
        runner = OneShotShadowPreflightRunner(self.settings)
        result = runner.run_preflight()
        report = result.to_markdown_report()

        self.assertIn(SHADOW_SAMPLE_ID, report)
        self.assertIn(ALLOWED_SHADOW_MODEL, report)
        self.assertIn("NOT REAL GEMINI", report)
        self.assertIn("AI_RESULT_NOT_AVAILABLE", report)
        self.assertIn("PROVIDER_CALL_NOT_AUTHORIZED", report)
        self.assertIn("BLOCKED_BY_REQUIRE_VERIFIED_FREE_TIER", report)
        self.assertIn("Actual Cost Incurred:** `$0.0000` USD ($0.00 / 0 TRY)", report)
        self.assertIn("Theoretical Paid-Equivalent", report)
        self.assertIn("Kendall Jenner'ın eski sevgilisi", report)
        self.assertIn("Rapçi Sansar Salvo", report)

        # Safety check: no API keys or token fragments
        self.assertNotIn("AIza", report)

    def test_require_verified_free_tier_unconditional_block_preserved(self) -> None:
        """require_verified_free_tier() in cost.py must raise RuntimeError unconditionally."""
        with self.assertRaises(RuntimeError) as ctx:
            require_verified_free_tier()
        self.assertIn("Real Gemini API calls blocked", str(ctx.exception))
        self.assertIn("provider-side $0 enforcement are not independently verified", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
