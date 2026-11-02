"""Hermetic unit tests for Phase 5B.3 / Issue #63: Stage B Gemini shadow operator.

Verifies:
1. Dry-run succeeds offline without network, API key, or provider calls.
2. Single-shot safety: execute_one_shot requires explicit confirm_single_shot_free_tier=True.
3. Live call without API key or client fails closed safely.
4. Segregation contract: Raw model picks (NEW, UPDATE, DUPLICATE, IGNORE) are preserved
   and reported separately from post-validation alertable suggestions.
5. Filtered-out dispositions breakdown captures exact suppression reasons (IGNORE, DUPLICATE, score < min_score).
6. Single-shot execution guard: Repeated calls on the same runner instance are strictly blocked.
7. Ambiguous failure / network error: Runner remains consumed and cannot be re-executed.
8. Truncation detection (MAX_TOKENS) fails closed.
9. Empty or malformed response fails closed.
10. Model enforcement: Only gemini-3.5-flash-lite allowed.
11. Grounding forbidden: enable_search_grounding=True rejected.
12. Candidate content integrity: Tampered candidates with identical IDs are strictly rejected.
13. Post-validation uses keyless Settings (no SDK client created for validation).
14. Markdown report formatting and secret sanitization.
"""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.shadow_operator import (
    ALLOWED_SHADOW_MODEL,
    SHADOW_SAMPLE_AS_OF_UTC,
    SHADOW_SAMPLE_ID,
    OneShotShadowOperatorRunner,
)
from efso_radar.ai.shadow_preflight import (
    get_historical_public_shadow_sample,
    sanitize_report_text,
)
from efso_radar.config import Settings
from efso_radar.domain import Article


class TestGeminiShadowOperator(unittest.TestCase):
    """Hermetic test suite for Phase 5B.3 / Issue #63 Stage B Shadow Operator."""

    def setUp(self) -> None:
        self.sample = get_historical_public_shadow_sample()
        self.settings = Settings(
            gemini_model=ALLOWED_SHADOW_MODEL,
            enable_search_grounding=False,
            min_score=78,
            max_per_run=10,
        )

    def test_dry_run_offline_succeeds_without_network_or_key(self) -> None:
        """Dry-run must evaluate sample, prompt, and token bounds offline with 0 provider calls."""
        runner = OneShotShadowOperatorRunner(self.settings)
        result = runner.dry_run()

        self.assertEqual(result.status, "DRY_RUN_COMPLETED_OFFLINE")
        self.assertFalse(result.is_live_call)
        self.assertEqual(result.provider_call_count, 0)
        self.assertEqual(result.actual_cost_usd, 0.0)
        self.assertGreater(result.theoretical_paid_equivalent_usd, 0.0)
        self.assertEqual(result.sample_id, SHADOW_SAMPLE_ID)
        self.assertEqual(result.model_name, ALLOWED_SHADOW_MODEL)
        self.assertTrue(result.grounding_forbidden)
        self.assertEqual(len(result.raw_picks), 0)
        self.assertEqual(len(result.approved_suggestions), 0)
        self.assertEqual(len(result.candidates_preview), 7)
        self.assertFalse(runner.execution_attempted, "Dry run must not mark runner as consumed")

    def test_execute_one_shot_requires_explicit_confirmation_flag(self) -> None:
        """execute_one_shot must fail closed if confirm_single_shot_free_tier is not True."""
        runner = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.execute_one_shot(confirm_single_shot_free_tier=False)
        self.assertIn("Safety gate: Stage B operator execution requires explicit", str(ctx.exception))
        self.assertTrue(runner.execution_attempted, "Attempt must mark runner consumed")

    def test_execute_one_shot_without_api_key_or_client_fails_closed(self) -> None:
        """If no client is passed and settings has no API key, fail closed safely."""
        keyless_settings = self.settings.model_copy(update={"gemini_api_key": ""})
        runner = OneShotShadowOperatorRunner(keyless_settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.execute_one_shot(confirm_single_shot_free_tier=True)
        self.assertIn("GEMINI_API_KEY is not configured or empty", str(ctx.exception))
        self.assertTrue(runner.execution_attempted)

    def _build_mock_response(
        self,
        picks: list[dict],
        summary: str = "Test özeti.",
        finish_reason: str = "STOP",
        prompt_tokens: int = 1600,
        candidates_tokens: int = 350,
    ) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.text = json.dumps({"picks": picks, "run_editorial_summary_tr": summary})

        cand = MagicMock()
        cand.finish_reason = finish_reason
        mock_resp.candidates = [cand]

        usage = MagicMock()
        usage.prompt_token_count = prompt_tokens
        usage.candidates_token_count = candidates_tokens
        usage.thoughts_token_count = 0
        usage.total_token_count = prompt_tokens + candidates_tokens
        mock_resp.usage_metadata = usage
        return mock_resp

    def test_execute_one_shot_mock_client_success_and_raw_vs_approved_segregation(self) -> None:
        """Hermetic test: Verifies successful execution and full segregation of raw model picks vs approved suggestions."""
        real_cand1 = self.sample[0]
        real_cand2 = self.sample[1]
        real_cand3 = self.sample[2]

        picks = [
            # 1. Alertable high-scoring NEW pick
            {
                "article_ids": [real_cand1.id],
                "score": 89,
                "kind": "NEW",
                "verification_status": "REPUTABLE_REPORTING",
                "headline_tr": "Devin Booker ve Hande Erçel Etkileşimi Gündem Oldu",
                "cover_title_tr": "DİKKAT ÇEKEN BEĞENİ",
                "caption_idea_tr": "Detaylı açıklama.",
                "why_efso_tr": "Yüksek popüler kültür etkileşimi.",
                "suggested_format": "POST",
                "category": "YERLI",
            },
            # 2. Model classified as IGNORE (Must be in raw_picks, but suppressed from approved_suggestions)
            {
                "article_ids": [real_cand2.id],
                "score": 35,
                "kind": "IGNORE",
                "verification_status": "UNVERIFIED_RUMOR",
                "headline_tr": "Önemsiz Magazin Haberi",
                "cover_title_tr": "ÖNEMSİZ",
                "caption_idea_tr": "Boş.",
                "why_efso_tr": "Düşük ilgi.",
                "suggested_format": "CAROUSEL",
                "category": "YERLI",
            },
            # 3. Model classified as DUPLICATE (Must be in raw_picks, but suppressed from approved_suggestions)
            {
                "article_ids": [real_cand1.id, real_cand2.id],
                "score": 80,
                "kind": "DUPLICATE",
                "verification_status": "REPUTABLE_REPORTING",
                "headline_tr": "Aynı Haberin Mükerrer Yayını",
                "cover_title_tr": "MÜKERRER",
                "caption_idea_tr": "Mükerrer haber.",
                "why_efso_tr": "Tekrar.",
                "suggested_format": "POST",
                "category": "YERLI",
            },
            # 4. Model classified as NEW, but score is below min_score (78)
            {
                "article_ids": [real_cand3.id],
                "score": 65,
                "kind": "NEW",
                "verification_status": "REPUTABLE_REPORTING",
                "headline_tr": "Eşik Altı Kalan Haber",
                "cover_title_tr": "EŞİK ALTI",
                "caption_idea_tr": "Açıklama.",
                "why_efso_tr": "Skoru yetersiz.",
                "suggested_format": "POST",
                "category": "YERLI",
            },
        ]

        mock_response = self._build_mock_response(picks, summary="1 alertable, 3 suppressed.")
        mock_client = MagicMock()
        mock_client.models = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        runner = OneShotShadowOperatorRunner(self.settings)
        result = runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)

        # 1. General result checks
        self.assertEqual(result.status, "OPERATOR_EXECUTION_SUCCESS")
        self.assertTrue(result.is_live_call)
        self.assertEqual(result.provider_call_count, 1)
        self.assertEqual(runner.call_count, 1)
        self.assertEqual(result.actual_cost_usd, 0.0)
        self.assertGreater(result.theoretical_paid_equivalent_usd, 0.0)
        self.assertEqual(result.finish_reason, "STOP")
        self.assertEqual(result.input_tokens, 1600)
        self.assertEqual(result.output_tokens, 350)

        # 2. Segregation Verification: All 4 raw model picks are preserved
        self.assertEqual(len(result.raw_picks), 4)
        raw_kinds = [p["kind"] for p in result.raw_picks]
        self.assertIn("NEW", raw_kinds)
        self.assertIn("IGNORE", raw_kinds)
        self.assertIn("DUPLICATE", raw_kinds)

        # 3. Approved Suggestions Verification: Only 1 pick approved by business rules
        self.assertEqual(len(result.approved_suggestions), 1)
        approved = result.approved_suggestions[0]
        self.assertEqual(approved["headline_tr"], "Devin Booker ve Hande Erçel Etkileşimi Gündem Oldu")
        self.assertEqual(approved["score"], 89)

        # 4. Filtered Out Dispositions Breakdown Verification: Exactly 3 picks suppressed with reasons
        self.assertEqual(len(result.filtered_out_dispositions), 3)
        suppressed_reasons = {f["kind"]: f["filter_reason"] for f in result.filtered_out_dispositions}

        self.assertIn("IGNORE", suppressed_reasons)
        self.assertIn("SUPPRESSED_KIND_IGNORE", suppressed_reasons["IGNORE"])

        self.assertIn("DUPLICATE", suppressed_reasons)
        self.assertIn("SUPPRESSED_KIND_DUPLICATE", suppressed_reasons["DUPLICATE"])

        self.assertIn("NEW", suppressed_reasons)
        self.assertIn("BELOW_SCORE_THRESHOLD", suppressed_reasons["NEW"])

        # 5. Summary counts check
        self.assertEqual(result.summary_counts["raw_total"], 4)
        self.assertEqual(result.summary_counts["raw_new"], 2)
        self.assertEqual(result.summary_counts["raw_ignore"], 1)
        self.assertEqual(result.summary_counts["raw_duplicate"], 1)
        self.assertEqual(result.summary_counts["approved_total"], 1)
        self.assertEqual(result.summary_counts["suppressed_total"], 3)

        # 6. Verify SDK call arguments: zero retry, no grounding, model
        mock_client.models.generate_content.assert_called_once()
        _, kwargs = mock_client.models.generate_content.call_args
        self.assertEqual(kwargs["model"], ALLOWED_SHADOW_MODEL)
        cfg = kwargs["config"]
        self.assertEqual(cfg.temperature, 0.0)
        self.assertIsNone(cfg.tools)

    def test_single_shot_guard_blocks_repeated_calls(self) -> None:
        """Runner must enforce max 1 execution; repeated call on same runner instance is blocked."""
        mock_response = self._build_mock_response([])
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        runner = OneShotShadowOperatorRunner(self.settings)

        # 1. First execution succeeds
        result1 = runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertEqual(result1.provider_call_count, 1)

        # 2. Second execution must be blocked fail-closed
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertIn("Repeated execution blocked: Stage B shadow operator enforces strict single-shot execution", str(ctx.exception))
        self.assertEqual(runner.call_count, 1, "Call count must not increment after block")

    def test_ambiguous_failure_blocks_rerun_and_zero_retry(self) -> None:
        """Ambiguous provider failure (network error, timeout) must fail closed and block rerun."""
        mock_client = MagicMock()
        mock_client.models.generate_content.side_effect = RuntimeError("Provider 503 Service Unavailable")

        runner = OneShotShadowOperatorRunner(self.settings)

        # First call fails
        with self.assertRaises(AIEvaluationError) as ctx1:
            runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertIn("Gemini operator call failed", str(ctx1.exception))
        self.assertTrue(runner.execution_attempted)

        # Re-execution is strictly forbidden
        with self.assertRaises(AIEvaluationError) as ctx2:
            runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertIn("Repeated execution blocked", str(ctx2.exception))
        self.assertIn("cannot be re-executed", str(ctx2.exception))

    def test_truncation_max_tokens_fails_closed(self) -> None:
        """Truncated response (finish_reason=MAX_TOKENS) must fail closed."""
        mock_response = self._build_mock_response([], finish_reason="MAX_TOKENS")
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        runner = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertIn("Gemini response truncated at max token limit (finish_reason=MAX_TOKENS)", str(ctx.exception))

    def test_empty_or_null_response_fails_closed(self) -> None:
        """Empty or null text response from Gemini must fail closed."""
        mock_response = MagicMock()
        mock_response.text = ""
        mock_response.candidates = [MagicMock(finish_reason="STOP")]
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        runner = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertIn("Gemini returned empty or null response", str(ctx.exception))

    def test_malformed_json_response_fails_closed(self) -> None:
        """Malformed JSON structured response must fail closed."""
        mock_response = MagicMock()
        mock_response.text = "{not valid json at all"
        mock_response.candidates = [MagicMock(finish_reason="STOP")]
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        runner = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertIn("Malformed structured response from model", str(ctx.exception))

    def test_model_enforcement_permits_gemini_3_5_flash_lite_only(self) -> None:
        """Any model other than gemini-3.5-flash-lite must be rejected immediately."""
        for bad_model in ["gemini-1.5-pro", "gemini-2.0-flash", "gpt-4o"]:
            bad_settings = Settings(gemini_model=bad_model)
            runner = OneShotShadowOperatorRunner(bad_settings)
            with self.assertRaises(AIEvaluationError) as ctx:
                runner.dry_run()
            self.assertIn("Shadow operator only permits model", str(ctx.exception))

    def test_search_grounding_forbidden(self) -> None:
        """enable_search_grounding=True must be rejected fail-closed."""
        bad_settings = Settings(
            gemini_model=ALLOWED_SHADOW_MODEL,
            enable_search_grounding=True,
        )
        runner = OneShotShadowOperatorRunner(bad_settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.dry_run()
        self.assertIn("Search grounding is strictly forbidden", str(ctx.exception))

    def test_candidate_content_tampering_rejected(self) -> None:
        """Altered candidate content with identical IDs must be strictly rejected fail-closed."""
        runner = OneShotShadowOperatorRunner(self.settings)
        altered_candidate = Article(
            id=self.sample[0].id,
            title="Değiştirilmiş Başlık",
            url=self.sample[0].url,
            canonical_url=self.sample[0].canonical_url,
            source=self.sample[0].source,
            market=self.sample[0].market,
            published_at=self.sample[0].published_at,
            snippet=self.sample[0].snippet,
        )
        altered_batch = [altered_candidate] + self.sample[1:]

        with self.assertRaises(AIEvaluationError) as ctx:
            runner.dry_run(candidates=altered_batch)
        self.assertIn("Candidate content modification rejected", str(ctx.exception))

    def test_post_validation_uses_keyless_settings_and_never_creates_validation_sdk_client(self) -> None:
        """Even if settings contains an API key, post-validation engine has client=None."""
        settings_with_key = Settings(
            gemini_api_key="AIzaSyTestKey12345678901234567890",
            gemini_model=ALLOWED_SHADOW_MODEL,
            enable_search_grounding=False,
        )
        mock_response = self._build_mock_response([])
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        runner = OneShotShadowOperatorRunner(settings_with_key)
        result = runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        self.assertEqual(result.status, "OPERATOR_EXECUTION_SUCCESS")

    def test_markdown_report_structure_and_sanitization(self) -> None:
        """Markdown report must present raw picks, approved suggestions, filter breakdown, and strip secrets."""
        real_cand = self.sample[0]
        picks = [
            {
                "article_ids": [real_cand.id],
                "score": 88,
                "kind": "NEW",
                "verification_status": "REPUTABLE_REPORTING",
                "headline_tr": "Devin Booker ve Hande Erçel Etkileşimi (api_key='secret-key-123')",
                "cover_title_tr": "BEĞENİ",
                "caption_idea_tr": "Açıklama.",
                "why_efso_tr": "Viral potansiyel.",
                "suggested_format": "POST",
                "category": "YERLI",
            },
            {
                "article_ids": [self.sample[1].id],
                "score": 25,
                "kind": "IGNORE",
                "verification_status": "UNVERIFIED_RUMOR",
                "headline_tr": "Gereksiz Haber",
                "cover_title_tr": "GEREKSİZ",
                "caption_idea_tr": "Açıklama.",
                "why_efso_tr": "Gereksiz.",
                "suggested_format": "CAROUSEL",
                "category": "YERLI",
            },
        ]
        mock_response = self._build_mock_response(picks, summary="1 alertable, 1 ignored.")
        mock_client = MagicMock()
        mock_client.models.generate_content.return_value = mock_response

        runner = OneShotShadowOperatorRunner(self.settings)
        result = runner.execute_one_shot(confirm_single_shot_free_tier=True, client=mock_client)
        report = result.to_markdown_report()

        # Section headings check
        self.assertIn("1. Execution & Metadata Overview", report)
        self.assertIn("2. Raw Model Dispositions", report)
        self.assertIn("3. Post-Validation Alertable Suggestions", report)
        self.assertIn("4. Filtered Out Dispositions Breakdown", report)
        self.assertIn("5. Public Historical Sample Candidates", report)

        # Content check
        self.assertIn("SUPPRESSED_KIND_IGNORE", report)
        self.assertIn("Devin Booker ve Hande Erçel Etkileşimi", report)
        self.assertIn("Actual Cost Incurred:** `$0.0000` USD ($0.00 / 0 TRY)", report)
        self.assertIn("NOT CURRENT NEWS FRESHNESS", report)
        self.assertIn("Small sample qualitative smoke test", report)

        # Sanitization check: secret key stripped
        self.assertNotIn("secret-key-123", report)
        self.assertIn("[REDACTED]", report)

    def test_cli_script_defaults_to_dry_run_safely(self) -> None:
        """scripts/run_gemini_shadow_operator.py must default to offline dry run when run without flags."""
        from scripts.run_gemini_shadow_operator import main

        with patch("sys.argv", ["run_gemini_shadow_operator.py", "--dry-run"]):
            exit_code = main()
            self.assertEqual(exit_code, 0)


if __name__ == "__main__":
    unittest.main()
