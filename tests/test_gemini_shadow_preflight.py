"""Hermetic unit tests for Phase 5B.3 / Issue #61: Bounded fail-closed Gemini shadow preflight.

Verifies:
1. Default offline execution fails closed without network, producing structured preview.
2. Historical public news sample integrity, provenance, and absence of private labels.
3. Model enforcement: strictly gemini-3.5-flash-lite only.
4. Grounding forbidden: enable_search_grounding=True is rejected.
5. Single-shot execution guard: repeated calls in same runner instance are blocked.
6. Zero retries: types.HttpRetryOptions(attempts=1) is passed to config.
7. Mock client transport failures (timeout, 429 rate limit, 503) fail closed.
8. Malformed JSON and MAX_TOKENS truncation fail closed.
9. Application-level validation prevents hallucinated candidate IDs.
10. All reports and mock outputs carry explicit NOT REAL GEMINI disclaimers.
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
        """Default runner without mock client must fail closed via require_verified_free_tier()."""
        runner = OneShotShadowPreflightRunner(self.settings)
        result = runner.run_preflight()

        self.assertEqual(result.status, "PREFLIGHT_READY_OFFLINE_ONLY")
        self.assertEqual(result.ai_result, "AI_RESULT_NOT_AVAILABLE")
        self.assertEqual(result.provider_status, "PROVIDER_CALL_NOT_AUTHORIZED")
        self.assertEqual(result.free_tier_gate_status, "BLOCKED_BY_REQUIRE_VERIFIED_FREE_TIER")
        self.assertEqual(result.model_name, ALLOWED_SHADOW_MODEL)
        self.assertEqual(result.call_count, 0)
        self.assertEqual(result.max_allowed_calls, 1)
        self.assertTrue(result.zero_retry_enforced)
        self.assertTrue(result.grounding_forbidden)
        self.assertEqual(result.candidate_count, 7)
        self.assertEqual(result.sample_id, SHADOW_SAMPLE_ID)
        self.assertIn("NOT REAL GEMINI", result.disclaimer)
        self.assertEqual(len(result.decisions), 0)
        self.assertIsNotNone(result.error_message)
        self.assertIn("Real Gemini API calls blocked", result.error_message)

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
        """Runner must enforce max 1 inference call; repeated call on same runner instance is blocked."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = json.dumps({"picks": [], "run_editorial_summary_tr": "Boş özet."})
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        runner = OneShotShadowPreflightRunner(self.settings)

        # 1. First execution succeeds
        result1 = runner.run_preflight(mock_client=mock_client)
        self.assertEqual(result1.call_count, 1)
        self.assertEqual(runner.call_count, 1)

        # 2. Second execution must be blocked
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.run_preflight(mock_client=mock_client)
        self.assertIn("Repeated execution blocked: shadow harness enforces single-shot execution", str(ctx.exception))
        self.assertEqual(runner.call_count, 1, "Call count must not increment after block")

    def test_zero_retry_enforced_in_http_options(self) -> None:
        """Injected client must receive types.GenerateContentConfig with attempts=1 and bounded tokens."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = json.dumps({"picks": [], "run_editorial_summary_tr": "Boş özet."})
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        runner = OneShotShadowPreflightRunner(self.settings)
        runner.run_preflight(mock_client=mock_client)

        call_kwargs = mock_client.models.generate_content.call_args.kwargs
        config = call_kwargs["config"]

        # Verify zero SDK retry: attempts=1
        self.assertIsNotNone(config.http_options)
        self.assertEqual(config.http_options.retry_options.attempts, 1)

        # Verify automatic function calling disabled
        self.assertIsNotNone(config.automatic_function_calling)
        self.assertTrue(config.automatic_function_calling.disable)

        # Verify bounded output tokens for 7 candidates (100 + 7 * 180 = 1360 tokens)
        self.assertEqual(config.max_output_tokens, 1360)

        # Verify minimal thinking level
        if hasattr(config, "thinking_config") and config.thinking_config is not None:
            self.assertEqual(str(config.thinking_config.thinking_level).upper(), "THINKINGLEVEL.MINIMAL")

    def test_mock_client_transport_failure_fails_closed(self) -> None:
        """Network timeout, 429 quota exhausted, or 503 service unavailable fail closed with zero retries."""
        for failure_exc in (
            TimeoutError("Transport timeout connecting to Google Gemini"),
            RuntimeError("429 ResourceExhausted: Quota exceeded for project"),
            RuntimeError("503 ServiceUnavailable: The model is overloaded"),
        ):
            mock_client = MagicMock()
            mock_client.models.generate_content.side_effect = failure_exc

            runner = OneShotShadowPreflightRunner(self.settings)
            with self.assertRaises(AIEvaluationError) as ctx:
                runner.run_preflight(mock_client=mock_client)
            self.assertIn("Injected mock call failed", str(ctx.exception))
            self.assertEqual(mock_client.models.generate_content.call_count, 1, "Must make exactly 1 call with zero retries")

    def test_mock_client_malformed_json_fails_closed(self) -> None:
        """Malformed JSON response must fail closed."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = "{not valid json at all"
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        runner = OneShotShadowPreflightRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.run_preflight(mock_client=mock_client)
        self.assertIn("Malformed Gemini structured output", str(ctx.exception))

    def test_mock_client_truncation_max_tokens_fails_closed(self) -> None:
        """Response truncated by MAX_TOKENS must fail closed."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = '{"picks": []}'
        mock_resp.candidates = [MagicMock(finish_reason="MAX_TOKENS")]
        mock_client.models.generate_content.return_value = mock_resp

        runner = OneShotShadowPreflightRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.run_preflight(mock_client=mock_client)
        self.assertIn("truncated due to MAX_TOKENS ceiling", str(ctx.exception))

    def test_mock_client_hallucinated_candidate_id_rejected(self) -> None:
        """Pick referencing a fabricated/hallucinated candidate ID must be rejected by application validation."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = json.dumps({
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
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        runner = OneShotShadowPreflightRunner(self.settings)
        result = runner.run_preflight(mock_client=mock_client)

        self.assertEqual(result.status, "MOCK_EVALUATION_COMPLETED")
        self.assertEqual(len(result.decisions), 0, "Hallucinated candidate ID must be rejected")

    def test_mock_client_valid_pick_approved_with_disclaimer(self) -> None:
        """Valid pick referencing authentic candidate ID must be approved, carrying explicit NOT REAL GEMINI disclaimer."""
        real_cand = self.sample[0]
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = json.dumps({
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
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        runner = OneShotShadowPreflightRunner(self.settings)
        result = runner.run_preflight(mock_client=mock_client)

        self.assertEqual(result.status, "MOCK_EVALUATION_COMPLETED")
        self.assertEqual(len(result.decisions), 1)
        dec = result.decisions[0]
        self.assertEqual(dec["headline_tr"], "Devin Booker ve Hande Erçel Sosyal Medyada Gündem Oldu")
        self.assertEqual(dec["score"], 89)
        self.assertIn("NOT REAL GEMINI", result.disclaimer)

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
        self.assertIn("Kendall Jenner'ın eski sevgilisi", report)
        self.assertIn("Rapçi Sansar Salvo", report)

        # Safety check: no API keys or token fragments
        self.assertNotIn("AIza", report)
        self.assertNotIn("secret", report.lower())

    def test_require_verified_free_tier_unconditional_block_preserved(self) -> None:
        """require_verified_free_tier() in cost.py must raise RuntimeError unconditionally."""
        with self.assertRaises(RuntimeError) as ctx:
            require_verified_free_tier()
        self.assertIn("Real Gemini API calls blocked", str(ctx.exception))
        self.assertIn("provider-side $0 enforcement are not independently verified", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
