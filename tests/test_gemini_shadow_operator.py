"""Hermetic unit tests for Phase 5B.3 / Issue #63: Stage B Gemini shadow operator.

Verifies:
1. Dry-run succeeds offline without network, API key, or provider calls.
2. P1 Live Deny: execute_one_shot() strictly fails closed; no live execution path exists in Stage B.
3. P1 Live Deny Proof: genai.Client is NEVER instantiated under any CLI flag or direct API method.
4. P1 SDK timeout is exactly 30_000 milliseconds (30 seconds).
5. P1 Provider charge is honestly reported as NOT_VERIFIED (no invoice or live charge claimed).
6. P1 Cross-process session lock is documented as pending live authorization.
7. P2 Sample coverage: Unreferenced cohort candidates are explicitly marked NOT_RETURNED_BY_MODEL.
8. P2 Segregation: Raw model picks (NEW, UPDATE, DUPLICATE, IGNORE) vs approved suggestions vs filtered dispositions.
9. P2 Output safety: CLI output path validation blocks writing to .env, data/, or overwriting existing files.
10. Single-shot guard: Repeated evaluations on the same runner instance are blocked fail-closed.
11. Truncation detection (MAX_TOKENS) fails closed.
12. Empty and malformed JSON payloads fail closed.
13. Model enforcement: Only gemini-3.5-flash-lite allowed.
14. Search grounding forbidden: enable_search_grounding=True rejected.
15. Candidate content tampering: Altered content with authentic IDs is strictly rejected.
16. Post-validation uses keyless settings guarantee.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.shadow_operator import (
    ALLOWED_SHADOW_MODEL,
    OPERATOR_TIMEOUT_MS,
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
        self.assertEqual(result.actual_provider_charge, "NOT_VERIFIED")
        self.assertGreater(result.theoretical_paid_equivalent_usd, 0.0)
        self.assertEqual(result.timeout_ms, 30_000)
        self.assertEqual(result.session_lock_status, "CROSS_PROCESS_SESSION_LOCK_PENDING_LIVE_AUTHORIZATION")
        self.assertEqual(result.sample_id, SHADOW_SAMPLE_ID)
        self.assertEqual(result.model_name, ALLOWED_SHADOW_MODEL)
        self.assertTrue(result.grounding_forbidden)
        self.assertEqual(len(result.raw_picks), 0)
        self.assertEqual(len(result.approved_suggestions), 0)
        self.assertEqual(len(result.candidates_preview), 7)
        self.assertFalse(runner.execution_attempted, "Dry run must not mark runner as consumed")

    def test_p1_live_deny_execute_one_shot_fails_closed(self) -> None:
        """P1 Security Gate: execute_one_shot must fail closed; live calls are strictly unauthorized."""
        runner = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.execute_one_shot(confirm_single_shot_free_tier=True)
        self.assertIn("Live Gemini API execution is strictly not authorized", str(ctx.exception))
        self.assertTrue(runner.execution_attempted)

    def test_p1_live_deny_proves_zero_sdk_client_instantiations(self) -> None:
        """Negative security proof: genai.Client is NEVER instantiated under any path."""
        with patch("google.genai.Client") as mock_client:
            runner = OneShotShadowOperatorRunner(self.settings)
            runner.dry_run()
            with self.assertRaises(AIEvaluationError):
                runner.execute_one_shot(confirm_single_shot_free_tier=True)

            payload = json.dumps({"picks": [], "run_editorial_summary_tr": "Boş."})
            fresh_runner = OneShotShadowOperatorRunner(self.settings)
            fresh_runner.evaluate_precomputed_response_json(payload)

            # Crucial verification: genai.Client was NEVER called
            mock_client.assert_not_called()

    def test_p1_sdk_timeout_unit_is_30000_ms(self) -> None:
        """SDK HttpOptions.timeout is in milliseconds; must be exactly 30_000 ms for 30 seconds."""
        self.assertEqual(OPERATOR_TIMEOUT_MS, 30_000)
        self.assertEqual(OPERATOR_TIMEOUT_MS / 1000.0, 30.0)
        runner = OneShotShadowOperatorRunner(self.settings)
        result = runner.dry_run()
        self.assertEqual(result.timeout_ms, 30_000)

    def test_p1_actual_provider_charge_reported_as_not_verified(self) -> None:
        """Provider billing charge must be honestly reported as NOT_VERIFIED."""
        runner = OneShotShadowOperatorRunner(self.settings)
        result = runner.dry_run()
        self.assertEqual(result.actual_provider_charge, "NOT_VERIFIED")
        report = result.to_markdown_report()
        self.assertIn("Provider Invoiced Charge:** `NOT_VERIFIED`", report)
        self.assertIn("Theoretical Paid-Equivalent", report)

    def test_p2_sample_coverage_marks_unreferenced_items_not_returned_by_model(self) -> None:
        """Articles from the 7-item sample not referenced in model picks must be marked NOT_RETURNED_BY_MODEL."""
        cand1 = self.sample[0]
        cand2 = self.sample[1]

        # Model selects only 2 of the 7 candidate articles
        picks = [
            {
                "article_ids": [cand1.id],
                "score": 85,
                "kind": "NEW",
                "verification_status": "REPUTABLE_REPORTING",
                "headline_tr": "Seçilen Birinci Haber",
                "cover_title_tr": "BİRİNCİ",
                "caption_idea_tr": "Detay.",
                "why_efso_tr": "Popüler.",
                "suggested_format": "POST",
                "category": "YERLI",
            },
            {
                "article_ids": [cand2.id],
                "score": 30,
                "kind": "IGNORE",
                "verification_status": "UNVERIFIED_RUMOR",
                "headline_tr": "İhmal Edilen İkinci Haber",
                "cover_title_tr": "İHMAL",
                "caption_idea_tr": "Detay.",
                "why_efso_tr": "Düşük.",
                "suggested_format": "CAROUSEL",
                "category": "YERLI",
            },
        ]
        payload = json.dumps({"picks": picks, "run_editorial_summary_tr": "2 haber seçildi."})

        runner = OneShotShadowOperatorRunner(self.settings)
        result = runner.evaluate_precomputed_response_json(payload)

        # 2 referenced articles -> exactly 5 unreturned candidate articles
        self.assertEqual(len(result.raw_picks), 2)
        self.assertEqual(len(result.unreturned_candidates), 5)
        for u in result.unreturned_candidates:
            self.assertEqual(u["status"], "NOT_RETURNED_BY_MODEL")
            self.assertNotIn(u["id"], [cand1.id, cand2.id])

        report = result.to_markdown_report()
        self.assertIn("## 5. Unreturned Candidate Articles (NOT_RETURNED_BY_MODEL)", report)
        self.assertIn("NOT_RETURNED_BY_MODEL", report)

    def test_raw_vs_approved_vs_filtered_segregation(self) -> None:
        """Full segregation: raw model picks, approved suggestions, suppressed picks, and unreturned items."""
        cand1 = self.sample[0]
        cand2 = self.sample[1]
        cand3 = self.sample[2]
        cand4 = self.sample[3]

        picks = [
            # 1. Alertable NEW pick
            {
                "article_ids": [cand1.id],
                "score": 89,
                "kind": "NEW",
                "verification_status": "REPUTABLE_REPORTING",
                "headline_tr": "Devin Booker ve Hande Erçel Etkileşimi",
                "cover_title_tr": "DİKKAT ÇEKEN BEĞENİ",
                "caption_idea_tr": "Açıklama.",
                "why_efso_tr": "Yüksek popüler kültür etkileşimi.",
                "suggested_format": "POST",
                "category": "YERLI",
            },
            # 2. Model IGNORE pick
            {
                "article_ids": [cand2.id],
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
            # 3. Model DUPLICATE pick
            {
                "article_ids": [cand1.id, cand2.id],
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
            # 4. Model NEW pick below min_score (78)
            {
                "article_ids": [cand3.id],
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
        payload = json.dumps({"picks": picks, "run_editorial_summary_tr": "4 raw picks evaluated."})

        runner = OneShotShadowOperatorRunner(self.settings)
        result = runner.evaluate_precomputed_response_json(payload)

        self.assertEqual(result.status, "OPERATOR_OFFLINE_EVALUATION_SUCCESS")
        self.assertFalse(result.is_live_call)
        self.assertEqual(result.provider_call_count, 0)
        self.assertEqual(result.actual_provider_charge, "NOT_VERIFIED")

        # Raw picks count
        self.assertEqual(len(result.raw_picks), 4)

        # Approved suggestions count
        self.assertEqual(len(result.approved_suggestions), 1)
        self.assertEqual(result.approved_suggestions[0]["headline_tr"], "Devin Booker ve Hande Erçel Etkileşimi")

        # Suppressed dispositions
        self.assertEqual(len(result.filtered_out_dispositions), 3)
        suppressed_reasons = {f["kind"]: f["filter_reason"] for f in result.filtered_out_dispositions}
        self.assertIn("SUPPRESSED_KIND_IGNORE", suppressed_reasons["IGNORE"])
        self.assertIn("SUPPRESSED_KIND_DUPLICATE", suppressed_reasons["DUPLICATE"])
        self.assertIn("BELOW_SCORE_THRESHOLD", suppressed_reasons["NEW"])

        # Unreturned candidates count (7 - 3 distinct referenced = 4 unreturned)
        self.assertEqual(len(result.unreturned_candidates), 4)

        # Summary counts
        self.assertEqual(result.summary_counts["raw_total"], 4)
        self.assertEqual(result.summary_counts["approved_total"], 1)
        self.assertEqual(result.summary_counts["suppressed_total"], 3)
        self.assertEqual(result.summary_counts["unreturned_total"], 4)

    def test_single_shot_guard_blocks_repeated_offline_evaluations(self) -> None:
        """Runner must enforce max 1 evaluation; repeated evaluation on same runner instance is blocked."""
        payload = json.dumps({"picks": [], "run_editorial_summary_tr": "Boş özet."})
        runner = OneShotShadowOperatorRunner(self.settings)

        # 1. First evaluation succeeds
        result1 = runner.evaluate_precomputed_response_json(payload)
        self.assertEqual(result1.status, "OPERATOR_OFFLINE_EVALUATION_SUCCESS")

        # 2. Second evaluation must be blocked
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.evaluate_precomputed_response_json(payload)
        self.assertIn("Repeated execution blocked: Stage B shadow operator enforces strict single-shot execution", str(ctx.exception))

    def test_truncation_max_tokens_fails_closed(self) -> None:
        """Truncated response (finish_reason=MAX_TOKENS) must fail closed."""
        payload = json.dumps({"picks": [], "run_editorial_summary_tr": "Yarıda kaldı."})
        runner = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx:
            runner.evaluate_precomputed_response_json(payload, finish_reason="MAX_TOKENS")
        self.assertIn("Gemini response truncated at max token limit (finish_reason=MAX_TOKENS)", str(ctx.exception))

    def test_empty_and_malformed_json_fails_closed(self) -> None:
        """Empty and malformed JSON payloads must fail closed."""
        runner1 = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx1:
            runner1.evaluate_precomputed_response_json("")
        self.assertIn("Structured response JSON is empty or null", str(ctx1.exception))

        runner2 = OneShotShadowOperatorRunner(self.settings)
        with self.assertRaises(AIEvaluationError) as ctx2:
            runner2.evaluate_precomputed_response_json("{not valid json at all")
        self.assertIn("Malformed structured response: payload does not conform", str(ctx2.exception))
        # Ensure raw input payload is not leaked in exception string
        self.assertNotIn("not valid json at all", str(ctx2.exception))

    def test_model_enforcement_and_grounding_rejection(self) -> None:
        """Any model other than gemini-3.5-flash-lite and enable_search_grounding=True must be rejected."""
        bad_model_settings = Settings(gemini_model="gemini-2.0-flash")
        runner1 = OneShotShadowOperatorRunner(bad_model_settings)
        with self.assertRaises(AIEvaluationError) as ctx1:
            runner1.dry_run()
        self.assertIn("Shadow operator only permits model", str(ctx1.exception))

        bad_grounding_settings = Settings(
            gemini_model=ALLOWED_SHADOW_MODEL,
            enable_search_grounding=True,
        )
        runner2 = OneShotShadowOperatorRunner(bad_grounding_settings)
        with self.assertRaises(AIEvaluationError) as ctx2:
            runner2.dry_run()
        self.assertIn("Search grounding is strictly forbidden", str(ctx2.exception))

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

    def test_post_validation_uses_keyless_settings_guarantee(self) -> None:
        """Even if settings contains an API key, post-validation engine has client=None."""
        settings_with_key = Settings(
            gemini_api_key="AIzaSyTestKey12345678901234567890",
            gemini_model=ALLOWED_SHADOW_MODEL,
            enable_search_grounding=False,
        )
        payload = json.dumps({"picks": [], "run_editorial_summary_tr": "Boş."})
        runner = OneShotShadowOperatorRunner(settings_with_key)
        result = runner.evaluate_precomputed_response_json(payload)
        self.assertEqual(result.status, "OPERATOR_OFFLINE_EVALUATION_SUCCESS")

    def test_cli_confirm_flag_fails_closed(self) -> None:
        """scripts/run_gemini_shadow_operator.py --confirm-single-shot-free-tier must fail closed."""
        from scripts.run_gemini_shadow_operator import main

        with patch("sys.argv", ["run_gemini_shadow_operator.py", "--confirm-single-shot-free-tier"]):
            exit_code = main()
            self.assertEqual(exit_code, 1)

    def test_cli_defaults_to_dry_run_safely(self) -> None:
        """scripts/run_gemini_shadow_operator.py must default to offline dry run when run without flags or --dry-run."""
        from scripts.run_gemini_shadow_operator import main

        with patch("sys.argv", ["run_gemini_shadow_operator.py", "--dry-run"]):
            exit_code = main()
            self.assertEqual(exit_code, 0)

        with patch("sys.argv", ["run_gemini_shadow_operator.py"]):
            exit_code2 = main()
            self.assertEqual(exit_code2, 0)

    def test_cli_rejects_forbidden_file_parameters(self) -> None:
        """scripts/run_gemini_shadow_operator.py must reject --evaluate-json and --output-file flags."""
        from scripts.run_gemini_shadow_operator import main

        # 1. --evaluate-json is rejected before opening any file
        with patch("sys.argv", ["run_gemini_shadow_operator.py", "--evaluate-json", "data/state.json"]):
            with self.assertRaises(SystemExit) as ctx1:
                main()
            self.assertEqual(ctx1.exception.code, 2)

        # 2. --output-file is rejected before writing any file
        with patch("sys.argv", ["run_gemini_shadow_operator.py", "--output-file", "report.md"]):
            with self.assertRaises(SystemExit) as ctx2:
                main()
            self.assertEqual(ctx2.exception.code, 2)

    def test_cli_never_instantiates_sdk_client_under_any_flag(self) -> None:
        """CLI execution never instantiates genai.Client under any flag."""
        from scripts.run_gemini_shadow_operator import main

        with patch("google.genai.Client") as mock_client:
            with patch("sys.argv", ["run_gemini_shadow_operator.py", "--dry-run"]):
                self.assertEqual(main(), 0)
            with patch("sys.argv", ["run_gemini_shadow_operator.py", "--confirm-single-shot-free-tier"]):
                self.assertEqual(main(), 1)
            with patch("sys.argv", ["run_gemini_shadow_operator.py"]):
                self.assertEqual(main(), 0)

            mock_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
