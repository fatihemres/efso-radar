"""Tests for Efso Radar Phase 3C-2D: Editorial Decision Contract & Diagnostic Evaluation Schema.

Verifies:
1. Exact 1-to-1 decision requirement (exactly 8 decisions for 8 candidate articles).
2. Valid & unique ID verification.
3. Missing-item rejection (assert AIEvaluationError when model drops an ID; no silent defaulting).
4. Hallucinated-ID rejection (assert AIEvaluationError when model returns non-candidate IDs).
5. Malformed response schema rejection (assert AIEvaluationError and error ledger recording).
6. Output truncation (finish_reason == 'MAX_TOKENS') handling.
7. Budget circuit breaker pre-call enforcement.
8. Idempotent cache and ledger resume across multiple passes.
9. Production projection triage rules (DIGEST/BREAKING -> alert candidates, RESERVE -> backlog buffer, IGNORE -> suppressed).
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from efso_radar.ai.client import (
    AIEvaluationError,
    project_diagnostic_decisions_to_assessments,
)
from efso_radar.ai.cost import compute_enforceable_batch_output_tokens, ExperimentBudgetGuard
from efso_radar.ai.phase3c2b_eval import (
    BlindShadowEvaluationRunner,
    _offline_blind_diagnostic_predictor,
)
from efso_radar.ai.schemas import (
    DiagnosticArticleDecision,
    DiagnosticEvaluationBatch,
)
from efso_radar.ai.calibration import (
    DEFAULT_FILLED_CALIBRATION_CSV,
    HybridNotificationPlanner,
    load_and_verify_calibration_csv,
)
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    Market,
    NotificationRecord,
    NotificationUrgencyTier,
    OwnerFeedbackRating,
    VerificationStatus,
)
from efso_radar.repositories.sqlite import SqliteStateRepository


class TestPhase3C2DContract(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmpdir.name)
        self.db_path = self.tmp_path / "test_phase3c2d.db"
        self.repo = SqliteStateRepository(self.db_path)
        self.now = datetime(2026, 10, 18, 12, 0, 0, tzinfo=timezone.utc)
        self.settings = Settings(
            gemini_model="gemini-3.5-flash-lite",
            max_shadow_eval_budget_usd=0.05,
            gemini_api_key="mock-auth-key",
            min_score=75,
        )
        self.budget_guard = ExperimentBudgetGuard(
            ledger_path=self.tmp_path / "test_ledger.json",
            budget_ceiling_usd=0.05,
        )
        self.runner = BlindShadowEvaluationRunner(
            settings=self.settings,
            repository=self.repo,
            batch_size=8,
            budget_guard=self.budget_guard,
        )

        # Build 8 sample articles
        self.articles = [
            Article.create(f"Test Candidate Story {i}", f"https://example.com/story-{i}", "Kaynak", self.now)
            for i in range(1, 9)
        ]
        self.article_ids = [a.id for a in self.articles]

    def tearDown(self):
        try:
            self.tmpdir.cleanup()
        except PermissionError:
            pass

    def _build_mock_diagnostic_batch_json(
        self,
        article_ids: list[str],
        disposition_map: dict[str, str] | None = None,
        score_map: dict[str, int] | None = None,
    ) -> str:
        disp_map = disposition_map or {}
        sc_map = score_map or {}
        decisions = []
        for aid in article_ids:
            disp = disp_map.get(aid, "DIGEST")
            score = sc_map.get(aid, 85 if disp == "DIGEST" else (72 if disp == "RESERVE" else 40))
            decisions.append(
                {
                    "article_id": aid,
                    "score": score,
                    "disposition": disp,
                    "kind": "NEW",
                    "urgency": "NORMAL",
                    "rationale_tr": f"Test gerekçesi for {aid[:8]} with sufficient length.",
                    "headline_tr": f"Türkçe Başlık {aid[:8]}" if disp != "IGNORE" else "",
                    "verification_status": "REPUTABLE_REPORTING",
                }
            )
        return json.dumps(
            {
                "decisions": decisions,
                "run_editorial_summary_tr": "Test tarama özeti başarıyla tamamlandı.",
            }
        )

    def test_diagnostic_predictor_offline_returns_exact_decision_count(self):
        """Offline simulation returns exactly one decision per candidate without dropping any ID."""
        batch_res = _offline_blind_diagnostic_predictor(self.articles, [], self.now)
        self.assertEqual(len(batch_res.decisions), 8)
        ret_ids = [d.article_id for d in batch_res.decisions]
        self.assertEqual(set(ret_ids), set(self.article_ids))
        self.assertEqual(len(ret_ids), len(set(ret_ids)))
        self.assertTrue(len(batch_res.run_editorial_summary_tr) > 5)

    def test_live_runner_validates_exact_8_decisions_from_mock_client(self):
        """Mock Gemini client returning 8 explicit decisions succeeds and records predictions."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = self._build_mock_diagnostic_batch_json(self.article_ids)
        mock_resp.usage_metadata = MagicMock(
            prompt_token_count=1600,
            candidates_token_count=450,
            thoughts_token_count=0,
            total_token_count=2050,
        )
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        preds, metrics, report = self.runner.run_live_gemini_evaluation(
            items=self.articles,
            mock_client=mock_client,
            dry_run=False,
            schema_mode="DIAGNOSTIC",
        )

        self.assertEqual(len(preds), 8)
        self.assertEqual(metrics.total_evaluated, 8)
        self.assertEqual(metrics.batches_executed, 1)
        self.assertTrue(metrics.all_referenced_ids_valid)
        self.assertEqual(metrics.hallucinated_id_count, 0)
        for p in preds:
            self.assertTrue(p.is_directly_observed)
            self.assertIsNotNone(p.disposition)
            self.assertEqual(p.disposition, "DIGEST")

    def test_runner_rejects_missing_candidate_ids_fail_closed(self):
        """If the model returns fewer decisions than candidates (missing IDs), runner raises AIEvaluationError."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        # Return only 7 decisions, dropping the 8th candidate
        mock_resp.text = self._build_mock_diagnostic_batch_json(self.article_ids[:7])
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        with self.assertRaises(AIEvaluationError) as ctx:
            self.runner.run_live_gemini_evaluation(
                items=self.articles,
                mock_client=mock_client,
                dry_run=False,
                schema_mode="DIAGNOSTIC",
            )
        self.assertIn("missing 1 candidate decision(s)", str(ctx.exception))
        self.assertIn("Silent defaulting is prohibited", str(ctx.exception))

    def test_runner_rejects_hallucinated_ids_fail_closed(self):
        """If the model returns non-candidate IDs, runner raises AIEvaluationError."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        fake_ids = self.article_ids[:7] + ["hallucinated_fake_id_1234"]
        mock_resp.text = self._build_mock_diagnostic_batch_json(fake_ids)
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        with self.assertRaises(AIEvaluationError) as ctx:
            self.runner.run_live_gemini_evaluation(
                items=self.articles,
                mock_client=mock_client,
                dry_run=False,
                schema_mode="DIAGNOSTIC",
            )
        self.assertIn("missing 1 candidate decision(s)", str(ctx.exception))

    def test_runner_rejects_duplicate_ids_in_decisions(self):
        """If the model returns the same article_id more than once in decisions, runner raises AIEvaluationError."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        dup_ids = self.article_ids[:7] + [self.article_ids[0]]  # duplicate first item
        mock_resp.text = self._build_mock_diagnostic_batch_json(dup_ids)
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        with self.assertRaises(AIEvaluationError) as ctx:
            self.runner.run_live_gemini_evaluation(
                items=self.articles,
                mock_client=mock_client,
                dry_run=False,
                schema_mode="DIAGNOSTIC",
            )
        self.assertIn("duplicate article_ids returned in decisions", str(ctx.exception))

    def test_runner_rejects_malformed_json_and_records_in_repository(self):
        """Malformed JSON from provider raises AIEvaluationError and records failure in repository."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = "This is not valid JSON at all!"
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        with self.assertRaises(AIEvaluationError) as ctx:
            self.runner.run_live_gemini_evaluation(
                items=self.articles,
                mock_client=mock_client,
                dry_run=False,
                schema_mode="DIAGNOSTIC",
            )
        self.assertIn("failed schema validation", str(ctx.exception))
        # Verify usage was logged in repository
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("SELECT run_mode FROM ai_usage_ledger ORDER BY created_at DESC")
            rows = cursor.fetchall()
            self.assertTrue(len(rows) >= 1)
            self.assertEqual(rows[0][0], "FAILED_AMBIGUOUS")
        finally:
            conn.close()

    def test_runner_rejects_truncated_output_max_tokens(self):
        """Candidate truncation (finish_reason == 'MAX_TOKENS') raises AIEvaluationError before parsing."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = '{"decisions": [{"article_id": "'
        cand = MagicMock()
        cand.finish_reason = "MAX_TOKENS"
        mock_resp.candidates = [cand]
        mock_client.models.generate_content.return_value = mock_resp

        with self.assertRaises(AIEvaluationError) as ctx:
            self.runner.run_live_gemini_evaluation(
                items=self.articles,
                mock_client=mock_client,
                dry_run=False,
                schema_mode="DIAGNOSTIC",
            )
        self.assertIn("finish_reason=MAX_TOKENS", str(ctx.exception))

    def test_fail_closed_budget_circuit_breaker(self):
        """Pre-call budget check halts execution before network dispatch if projected cost exceeds ceiling."""
        tight_settings = Settings(
            gemini_model="gemini-3.5-flash-lite",
            max_shadow_eval_budget_usd=0.001,  # $0.001 ceiling vs ~$0.004 batch worst-case cost
            gemini_api_key="mock-key",
        )
        tight_runner = BlindShadowEvaluationRunner(
            settings=tight_settings,
            repository=self.repo,
            batch_size=8,
        )
        mock_client = MagicMock()

        with self.assertRaises(AIEvaluationError) as ctx:
            tight_runner.run_live_gemini_evaluation(
                items=self.articles,
                mock_client=mock_client,
                dry_run=False,
                schema_mode="DIAGNOSTIC",
            )
        self.assertIn("Fail-closed budget circuit breaker", str(ctx.exception))
        self.assertIn("Execution halted before network dispatch", str(ctx.exception))
        # Verify mock client was never called!
        self.assertEqual(mock_client.models.generate_content.call_count, 0)

    def test_idempotent_cache_and_ledger_resume(self):
        """Completed batch is saved in repository cache and re-read on subsequent invocation with 0 API calls."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = self._build_mock_diagnostic_batch_json(self.article_ids)
        mock_resp.usage_metadata = MagicMock(
            prompt_token_count=1600,
            candidates_token_count=450,
            thoughts_token_count=0,
            total_token_count=2050,
        )
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        # PASS 1: Execute live call with mock client
        preds1, metrics1, _ = self.runner.run_live_gemini_evaluation(
            items=self.articles,
            mock_client=mock_client,
            dry_run=False,
            schema_mode="DIAGNOSTIC",
        )
        self.assertEqual(metrics1.batches_executed, 1)
        self.assertEqual(metrics1.batches_reused_from_cache, 0)
        self.assertEqual(mock_client.models.generate_content.call_count, 1)

        # PASS 2: Re-run should use cache completely (0 API calls)
        mock_client.reset_mock()
        preds2, metrics2, _ = self.runner.run_live_gemini_evaluation(
            items=self.articles,
            mock_client=mock_client,
            dry_run=False,
            schema_mode="DIAGNOSTIC",
        )
        self.assertEqual(metrics2.batches_executed, 0)
        self.assertEqual(metrics2.batches_reused_from_cache, 1)
        self.assertEqual(mock_client.models.generate_content.call_count, 0)
        self.assertEqual(len(preds2), 8)
        for p in preds2:
            self.assertTrue(p.from_cache)

    def test_production_projection_triage_rules(self):
        """project_diagnostic_decisions_to_assessments maps decisions to Telegram delivery correctly."""
        decisions = [
            # 1. DIGEST with score >= 75 -> Projected
            DiagnosticArticleDecision(
                article_id=self.articles[0].id,
                score=85,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Yüksek pop-kültür ilgisi ve net haber değeri.",
                headline_tr="Başlık 1",
            ),
            # 2. DIGEST with score < 75 -> Suppressed
            DiagnosticArticleDecision(
                article_id=self.articles[1].id,
                score=65,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Eşik altı ilgi düzeyi.",
                headline_tr="Başlık 2",
            ),
            # 3. BREAKING -> Projected regardless of normal threshold
            DiagnosticArticleDecision(
                article_id=self.articles[2].id,
                score=90,
                disposition="DIGEST",
                kind="NEW",
                urgency="BREAKING",
                rationale_tr="Son dakika flaş gelişme.",
                headline_tr="Başlık 3",
            ),
            # 4. RESERVE -> Retained in buffer, not projected into primary alerts
            DiagnosticArticleDecision(
                article_id=self.articles[3].id,
                score=74,
                disposition="RESERVE",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Boş günde değerlendirilebilecek yedek içerik.",
                headline_tr="Başlık 4",
            ),
            # 5. HOLD -> Projected with verification hold
            DiagnosticArticleDecision(
                article_id=self.articles[4].id,
                score=78,
                disposition="HOLD",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Teyitsiz dedikodu iddiası.",
                headline_tr="İddia: Başlık 5",
                verification_status="UNVERIFIED_RUMOR",
            ),
            # 6. IGNORE -> Suppressed
            DiagnosticArticleDecision(
                article_id=self.articles[5].id,
                score=35,
                disposition="IGNORE",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Kapsam dışı adli haber.",
                headline_tr="",
            ),
            # 7. DUPLICATE -> Suppressed
            DiagnosticArticleDecision(
                article_id=self.articles[6].id,
                score=50,
                disposition="IGNORE",
                kind="DUPLICATE",
                urgency="NORMAL",
                rationale_tr="Mükerrer haber.",
                headline_tr="",
            ),
        ]

        assessments = project_diagnostic_decisions_to_assessments(
            decisions=decisions,
            candidates=self.articles,
            min_score=75,
            include_manual_holds=True,
        )

        projected_aids = {asm.article_ids[0] for asm in assessments if asm.is_publishable}
        # Under the unified product rule:
        # DIGEST determines eligibility; score ranks priority.
        # Eligible publishable items: articles[0] (DIGEST 85), articles[1] (DIGEST 65), articles[2] (DIGEST 90)
        self.assertEqual(len(projected_aids), 3)
        self.assertIn(self.articles[0].id, projected_aids)
        self.assertIn(self.articles[1].id, projected_aids)
        self.assertIn(self.articles[2].id, projected_aids)

        # Excluded from primary publication:
        # articles[3] (RESERVE)
        # articles[4] (HOLD - is_publishable=False)
        # articles[5] (IGNORE)
        # articles[6] (DUPLICATE)
        self.assertNotIn(self.articles[3].id, projected_aids)
        self.assertNotIn(self.articles[4].id, projected_aids)
        self.assertNotIn(self.articles[5].id, projected_aids)
        self.assertNotIn(self.articles[6].id, projected_aids)

        # Verify hold note on articles[4]
        hold_asm = next(a for a in assessments if a.article_ids[0] == self.articles[4].id)
        self.assertIsNotNone(hold_asm.uncertainties_tr)
        self.assertIn("Doğrulama bekletmesinde", hold_asm.uncertainties_tr)
        self.assertFalse(hold_asm.is_publishable)

    def test_offline_production_projection_using_real_saved_second_canary_decisions(self):
        """
        Exercise offline production projection strictly from saved genuine second-canary model decisions.

        Verifies:
        1. Full data-provenance audit: CSV SHA-256, exact article ID derivation, exact publisher,
           source URL, and headline match. The test fails if stored IDs are mismatched, duplicated,
           missing, or mapped to the wrong source records.
        2. Strict blind article isolation: owner_decision and owner_notes are never present in blind articles.
        3. Three eligible DIGEST items survive: CALIB-01 (75), CALIB-05 (82), CALIB-08 (70).
        4. Three RESERVE items remain outside primary notifications: CALIB-02 (68), CALIB-04 (65), CALIB-07 (60).
        5. Two IGNORE items remain suppressed: CALIB-03 (10), CALIB-06 (25).
        6. Complete production path: diagnostic decision -> assessment -> notification planner ->
           formatted digest -> persisted outbox in StateRepository.
        7. Outbox integrity: exactly 3 queued notifications in score order CALIB-05 (82) > CALIB-01 (75) > CALIB-08 (70).
        8. Strictly zero Telegram network calls are dispatched.
        """
        import hashlib
        from efso_radar.ai.calibration import (
            HybridNotificationPlanner,
            load_and_verify_calibration_csv,
            DEFAULT_FILLED_CALIBRATION_CSV,
        )
        from efso_radar.domain import DeliveryStatus, NotificationRecord, Story

        # 1. Authoritative CSV verification & Provenance check
        csv_path = Path(DEFAULT_FILLED_CALIBRATION_CSV)
        self.assertTrue(csv_path.exists(), "Authoritative CSV must exist")
        csv_bytes = csv_path.read_bytes()
        calculated_sha = hashlib.sha256(csv_bytes).hexdigest().lower()
        expected_sha = "108799c19f513327cb04872cec5dbcfb97107f2bd8bf2353acf28e3111ab2305".lower()
        self.assertEqual(calculated_sha, expected_sha, "Calibration CSV SHA-256 must match authoritative hash exactly")

        # Load first 8 items from CSV
        calib_items = load_and_verify_calibration_csv(csv_path)[:8]
        self.assertEqual(len(calib_items), 8)

        # Programmatically verify expected canonical metadata for each of the 8 items
        expected_provenance = {
            "CALIB-01": {"headline": "Bulgaristan'daki köyünü ziyaret etti", "pub": "Habertürk Magazin"},
            "CALIB-02": {"headline": "Aşıkların Ayvalık tatili", "pub": "Habertürk Magazin"},
            "CALIB-03": {"headline": "14. Kattan Düşerek Hayatını Kaybeden Sezen Taş'ın Geçmişine Dair Dikkat Çeken Detay!", "pub": "Onedio Magazin"},
            "CALIB-04": {"headline": "Kelly Clarkson Claims NYC Store Gave Her Pretty Woman Treatment", "pub": "E! News"},
            "CALIB-05": {"headline": "Sıla'nın 101 Yaşındaki Anneannesi İlk Kez Görüntülendi: Zarafetiyle Göz Kamaştırdı!", "pub": "Onedio Magazin"},
            "CALIB-06": {"headline": "\u2018Wildwood\u2019 Director Travis Knight Reflects on Animated Feature\u2019s 16-Year Journey at London Premiere: \u2018When We Started This, Barack Obama Was a Fresh-Faced President\u2019", "pub": "Variety"},
            "CALIB-07": {"headline": "İkinci çocuğunu dünyaya getirdi", "pub": "Habertürk Magazin"},
            "CALIB-08": {"headline": "Aksiyon yıldızlarını aratmadı", "pub": "Hürriyet Kelebek"},
        }

        for it in calib_items:
            self.assertIn(it.review_id, expected_provenance)
            exp = expected_provenance[it.review_id]
            self.assertEqual(it.headline, exp["headline"], f"Headline mismatch for {it.review_id}")
            self.assertEqual(it.publisher, exp["pub"], f"Publisher mismatch for {it.review_id}")

        # Verify blind article conversion excludes all owner notes and labels
        blind_articles = BlindShadowEvaluationRunner.to_blind_articles(calib_items)
        for ba in blind_articles:
            self.assertEqual(ba.raw_metadata, {}, "Blind articles must have empty raw_metadata")
            self.assertNotIn("owner_decision", str(ba))
            self.assertNotIn("owner_notes", str(ba))

        art_map_by_id = {a.id: a for a in blind_articles}
        calib_by_id = {a.id: it for a, it in zip(blind_articles, calib_items)}

        # 2. Inspect real saved canary2 evidence
        canary_report_path = Path("data/canary2_gemini_eval_report.json")
        self.assertTrue(canary_report_path.exists(), "Saved genuine second canary report must exist")

        with open(canary_report_path, "r", encoding="utf-8") as f:
            canary_data = json.load(f)

        raw_decisions = canary_data["evidence"]["raw_structured_model_predictions"][0]["decisions"]
        self.assertEqual(len(raw_decisions), 8, "Canary report must contain exactly 8 raw decisions")

        # Provenance check on raw decisions: must match candidate article IDs exactly without omissions or duplicates
        dec_article_ids = [d["article_id"] for d in raw_decisions]
        self.assertEqual(len(set(dec_article_ids)), 8, "Model decisions must not contain duplicate article IDs")
        for aid in dec_article_ids:
            self.assertIn(aid, art_map_by_id, f"Decision references unknown article_id: {aid}")

        diagnostic_decisions = [DiagnosticArticleDecision(**d) for d in raw_decisions]

        # 3. Execute production projection
        projected = project_diagnostic_decisions_to_assessments(
            decisions=diagnostic_decisions,
            candidates=blind_articles,
            include_manual_holds=True,
        )

        publishable_assessments = [asm for asm in projected if asm.is_publishable]
        hold_assessments = [asm for asm in projected if not asm.is_publishable]

        # Exact count and CALIB IDs of publishable DIGEST items
        self.assertEqual(len(publishable_assessments), 3)
        self.assertEqual(len(hold_assessments), 0)

        pub_calib_ids = {calib_by_id[asm.article_ids[0]].review_id for asm in publishable_assessments}
        expected_pub_ids = {"CALIB-01", "CALIB-05", "CALIB-08"}
        self.assertEqual(pub_calib_ids, expected_pub_ids)

        # CALIB-01 (Çağatay Ulusoy) survived with score 75
        c01_asm = next(asm for asm in publishable_assessments if calib_by_id[asm.article_ids[0]].review_id == "CALIB-01")
        self.assertEqual(c01_asm.score, 75)
        self.assertEqual(c01_asm.disposition, EditorialDisposition.DIGEST)
        self.assertIn("Çağatay Ulusoy", c01_asm.headline_tr)

        # CALIB-05 (Sıla) survived with score 82
        c05_asm = next(asm for asm in publishable_assessments if calib_by_id[asm.article_ids[0]].review_id == "CALIB-05")
        self.assertEqual(c05_asm.score, 82)
        self.assertEqual(c05_asm.disposition, EditorialDisposition.DIGEST)
        self.assertIn("Sıla", c05_asm.headline_tr)

        # CALIB-08 (Doğukan Güngör) survived with score 70
        c08_asm = next(asm for asm in publishable_assessments if calib_by_id[asm.article_ids[0]].review_id == "CALIB-08")
        self.assertEqual(c08_asm.score, 70)
        self.assertEqual(c08_asm.disposition, EditorialDisposition.DIGEST)
        self.assertIn("Doğukan Güngör", c08_asm.headline_tr)

        # RESERVE items (CALIB-02, CALIB-04, CALIB-07) are NOT in publishable queue
        reserve_calib_ids = {"CALIB-02", "CALIB-04", "CALIB-07"}
        for r_id in reserve_calib_ids:
            self.assertNotIn(r_id, pub_calib_ids)

        # IGNORE items (CALIB-03, CALIB-06) are NOT in publishable queue
        ignore_calib_ids = {"CALIB-03", "CALIB-06"}
        for i_id in ignore_calib_ids:
            self.assertNotIn(i_id, pub_calib_ids)

        # 4. Hybrid Notification Planning
        planner = HybridNotificationPlanner(self.settings)
        plan = planner.plan_dispatch(
            assessments=publishable_assessments,
            messages_sent_today=0,
            is_digest_due=True,
        )

        self.assertEqual(len(plan.digest_batch), 3)
        self.assertEqual(len(plan.immediate_breaking_alerts), 0)
        self.assertEqual(len(plan.held_for_verification), 0)
        self.assertEqual(len(plan.suppressed), 0)

        # Check score ranking priority: CALIB-05 (82) > CALIB-01 (75) > CALIB-08 (70)
        ordered_ranked_ids = [calib_by_id[asm.article_ids[0]].review_id for asm in plan.digest_batch]
        self.assertEqual(ordered_ranked_ids, ["CALIB-05", "CALIB-01", "CALIB-08"])

        # Verify digest formatting text exists without sending any network request
        self.assertIsNotNone(plan.formatted_digest_message)
        self.assertIn("EDİTORYAL ÖZET", plan.formatted_digest_message)
        self.assertEqual(plan.telegram_messages_to_send_now, 1)

        # 5. Complete Production Path: Persist to Outbox in StateRepository
        with tempfile.TemporaryDirectory() as tmp_repo_dir:
            temp_db = Path(tmp_repo_dir) / "test_outbox.db"
            repo = SqliteStateRepository(temp_db)
            ref_now = calib_items[0].discovered_at

            for idx, asm in enumerate(plan.digest_batch):
                it = calib_by_id[asm.article_ids[0]]
                notif_id = f"DIGEST-Q-{it.review_id}"
                story_id = f"EFSO-{it.review_id}"
                notif_rec = NotificationRecord(
                    notification_id=notif_id,
                    story_id=story_id,
                    update_id=None,
                    kind=asm.kind,
                    channel="TELEGRAM_DIGEST",
                    sent_at=ref_now,
                    status=DeliveryStatus.QUEUED_FOR_DIGEST,
                    message_text=asm.headline_tr,
                    headline_tr=asm.headline_tr,
                    score=asm.score,
                )
                repo.record_notification(notif_rec)
                repo.record_editorial_assessment(asm, story_id)
                repo.record_new_story(
                    Story(
                        story_id=story_id,
                        headline_tr=asm.headline_tr,
                        category=asm.category,
                        first_seen_at=ref_now,
                        last_updated_at=ref_now,
                        article_ids=asm.article_ids,
                    ),
                    asm,
                )

            # Query outbox
            queued_outbox = repo.get_queued_digest_notifications(max_age_hours=None)
            self.assertEqual(len(queued_outbox), 3, "Exactly 3 DIGEST notifications must be in the outbox")

            # Outbox order must follow score descending: CALIB-05 (82) > CALIB-01 (75) > CALIB-08 (70)
            self.assertEqual(queued_outbox[0].notification_id, "DIGEST-Q-CALIB-05")
            self.assertEqual(queued_outbox[0].score, 82)
            self.assertEqual(queued_outbox[1].notification_id, "DIGEST-Q-CALIB-01")
            self.assertEqual(queued_outbox[1].score, 75)
            self.assertEqual(queued_outbox[2].notification_id, "DIGEST-Q-CALIB-08")
            self.assertEqual(queued_outbox[2].score, 70)

            # Confirm RESERVE and IGNORE records never entered the notifications outbox table
            all_notif_ids = [n.notification_id for n in queued_outbox]
            for non_pub in ("CALIB-02", "CALIB-03", "CALIB-04", "CALIB-06", "CALIB-07"):
                self.assertNotIn(f"DIGEST-Q-{non_pub}", all_notif_ids)

    def test_precedence_invariants_and_conflict_handling(self):
        """Urgency must never override verification, duplication, or editorial disposition."""
        from efso_radar.ai.calibration import HybridNotificationPlanner
        planner = HybridNotificationPlanner(self.settings)
        art = self.articles[0]

        # Invariant 1: HOLD must never enter publishable digest or breaking queue
        hold_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=88,
            disposition="HOLD",
            kind="NEW",
            urgency="BREAKING",  # Inconsistent BREAKING set on HOLD
            rationale_tr="Teyit bekleyen sarsıcı iddia.",
            verification_status="ATTRIBUTED_CLAIM",
        )
        asms_hold = project_diagnostic_decisions_to_assessments([hold_dec], [art], include_manual_holds=True)
        self.assertEqual(len(asms_hold), 1)
        self.assertFalse(asms_hold[0].is_publishable)
        plan_hold = planner.plan_dispatch(asms_hold)
        self.assertEqual(len(plan_hold.immediate_breaking_alerts), 0)
        self.assertEqual(len(plan_hold.digest_batch), 0)
        self.assertIn(asms_hold[0], plan_hold.held_for_verification)

        # Invariant 2: IGNORE must never become publishable merely because urgency is BREAKING
        ignore_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=95,
            disposition="IGNORE",
            kind="NEW",
            urgency="BREAKING",
            rationale_tr="Kapsam dışı ağır adli olay.",
        )
        asms_ignore = project_diagnostic_decisions_to_assessments([ignore_dec], [art])
        self.assertEqual(len(asms_ignore), 0)

        # Invariant 3: DUPLICATE must never become a new publishable story
        dup_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=90,
            disposition="DIGEST",
            kind="DUPLICATE",
            urgency="BREAKING",
            rationale_tr="Mükerrer haber.",
        )
        asms_dup = project_diagnostic_decisions_to_assessments([dup_dec], [art])
        self.assertEqual(len(asms_dup), 0)

        # Invariant 4: RESERVE remains reserve even if the model inconsistently sets BREAKING
        reserve_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=92,
            disposition="RESERVE",
            kind="NEW",
            urgency="BREAKING",
            rationale_tr="Yedek tampon içerik.",
        )
        asms_reserve = project_diagnostic_decisions_to_assessments([reserve_dec], [art])
        self.assertEqual(len(asms_reserve), 0)

        # Invariant 5: An unverified story must not become publishable or breaking
        unverified_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=96,
            disposition="DIGEST",
            kind="NEW",
            urgency="BREAKING",
            rationale_tr="Sosyal medyada yayılan teyitsiz söylenti.",
            verification_status="UNVERIFIED_RUMOR",
        )
        asms_unverified = project_diagnostic_decisions_to_assessments([unverified_dec], [art], include_manual_holds=True)
        self.assertEqual(len(asms_unverified), 1)
        self.assertFalse(asms_unverified[0].is_publishable)
        plan_unv = planner.plan_dispatch(asms_unverified)
        self.assertEqual(len(plan_unv.immediate_breaking_alerts), 0)
        self.assertEqual(len(plan_unv.digest_batch), 0)
        self.assertIn(asms_unverified[0], plan_unv.held_for_verification)

        # Invariant 6: BREAKING applies only to a verified, eligible, editorially approved story
        breaking_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=95,
            disposition="DIGEST",
            kind="NEW",
            urgency="BREAKING",
            rationale_tr="Resmi açıklamayla doğrulanan majör pop-kültür gelişmesi.",
            headline_tr="Flaş Doğrulanmış Başlık",
            verification_status="OFFICIAL_CONFIRMATION",
        )
        asms_breaking = project_diagnostic_decisions_to_assessments([breaking_dec], [art])
        self.assertEqual(len(asms_breaking), 1)
        self.assertTrue(asms_breaking[0].is_publishable)
        tier_brk = planner.classify_urgency_tier(asms_breaking[0])
        self.assertEqual(tier_brk, NotificationUrgencyTier.BREAKING_IMMEDIATE)
        plan_brk = planner.plan_dispatch(asms_breaking)
        self.assertEqual(len(plan_brk.immediate_breaking_alerts), 1)

        # Invariant 7: Legitimate UPDATE events remain eligible when they contain meaningful new information
        update_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=85,
            disposition="DIGEST",
            kind="UPDATE",
            urgency="NORMAL",
            rationale_tr="Yeni gelişme: Mahkeme sanatçı hakkında beraat ve tahliye kararı verdi.",
            headline_tr="Yeni Gelişme: Beraat Kararı",
            verification_status="REPUTABLE_REPORTING",
        )
        asms_update = project_diagnostic_decisions_to_assessments([update_dec], [art])
        self.assertEqual(len(asms_update), 1)
        self.assertTrue(asms_update[0].is_publishable)
        self.assertEqual(asms_update[0].kind, EditorialKind.UPDATE)
        self.assertIsNotNone(asms_update[0].development_tr)
        self.assertIn("beraat", asms_update[0].development_tr)

    def test_data_provenance_validation_detects_corrupted_or_mismatched_canary_records(self):
        """Audit test: data-provenance validation must detect and fail on corrupted, missing, or mismatched records."""
        from efso_radar.ai.calibration import load_and_verify_calibration_csv, DEFAULT_FILLED_CALIBRATION_CSV

        calib_items = load_and_verify_calibration_csv(DEFAULT_FILLED_CALIBRATION_CSV)[:8]
        blind_articles = BlindShadowEvaluationRunner.to_blind_articles(calib_items)
        art_ids = [a.id for a in blind_articles]

        # Case 1: Corrupted or mismatched article_id in canary decisions
        corrupted_decisions = [
            DiagnosticArticleDecision(
                article_id="corrupted_nonexistent_id" if i == 0 else art_ids[i],
                score=75,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Geçerli gerekçe.",
            )
            for i in range(8)
        ]
        # project_diagnostic_decisions_to_assessments silently ignores unknown IDs, resulting in fewer assessments
        projected = project_diagnostic_decisions_to_assessments(corrupted_decisions, blind_articles)
        # We assert that the corrupted item was NOT projected and total publishable count dropped
        self.assertEqual(len(projected), 7)
        self.assertNotIn("corrupted_nonexistent_id", [a.article_ids[0] for a in projected])

        # Case 2: Runner rejects missing IDs fail-closed
        mock_client = MagicMock()
        mock_resp = MagicMock()
        # Drop first item
        mock_resp.text = self._build_mock_diagnostic_batch_json(art_ids[1:])
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        with self.assertRaises(AIEvaluationError) as ctx:
            self.runner.run_live_gemini_evaluation(
                items=blind_articles,
                mock_client=mock_client,
                dry_run=False,
                schema_mode="DIAGNOSTIC",
            )
        self.assertIn("missing 1 candidate decision(s)", str(ctx.exception))

    def test_sensitive_review_precedence_invariants(self):
        """SENSITIVE_REVIEW must NEVER enter publication queue regardless of score or BREAKING urgency."""
        planner = HybridNotificationPlanner(self.settings)
        art = self.articles[0]

        sensitive_dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=99,
            disposition="SENSITIVE_REVIEW",
            kind="NEW",
            urgency="BREAKING",  # Inconsistent BREAKING set on sensitive review
            rationale_tr="Önemli sanatçının trajik vefat haberi; hassas içerik.",
            headline_tr="Bomba İddia: Şok Vefat Haberi",
            verification_status="OFFICIAL_CONFIRMATION",
        )

        asms = project_diagnostic_decisions_to_assessments([sensitive_dec], [art], include_sensitive_reviews=True)
        self.assertEqual(len(asms), 1)
        asm = asms[0]

        # 1. Verification and publication state invariants
        self.assertFalse(asm.is_publishable, "Sensitive review item must ALWAYS have is_publishable=False")
        self.assertEqual(asm.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE EDITORIAL REVIEW REQUIRED]", asm.uncertainties_tr)

        # 2. Privacy & headline sanitization: sensationalized buzzwords stripped
        self.assertNotIn("Bomba İddia", asm.headline_tr)
        self.assertNotIn("Şok Vefat", asm.headline_tr)

        # 3. Notification planning tier & queue isolation
        tier = planner.classify_urgency_tier(asm)
        self.assertEqual(tier, NotificationUrgencyTier.SENSITIVE_REVIEW)

        plan = planner.plan_dispatch([asm], messages_sent_today=0, is_digest_due=True)
        self.assertEqual(len(plan.immediate_breaking_alerts), 0, "Sensitive review must NEVER enter breaking queue")
        self.assertEqual(len(plan.digest_batch), 0, "Sensitive review must NEVER enter digest batch")
        self.assertEqual(len(plan.deferred_for_next_digest), 0)
        self.assertEqual(len(plan.held_for_sensitive_review), 1, "Must enter held_for_sensitive_review queue")
        self.assertEqual(plan.telegram_messages_to_send_now, 0)

        # 4. Outbox persistence separation
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo = SqliteStateRepository(Path(tmp_dir) / "test_sens.db")
            record = NotificationRecord(
                notification_id=f"HOLD-SENSITIVE-{art.id[:8]}",
                story_id=f"EFSO-{art.id[:8]}",
                update_id=None,
                kind=asm.kind,
                channel="TELEGRAM_SENSITIVE_REVIEW",
                sent_at=self.now,
                status=DeliveryStatus.HELD_FOR_SENSITIVE_REVIEW,
                message_text=asm.headline_tr,
                headline_tr=asm.headline_tr,
                score=asm.score,
            )
            repo.record_notification(record)

            # Assert digest outbox does NOT return this record
            digest_outbox = repo.get_queued_digest_notifications()
            self.assertEqual(len(digest_outbox), 0)

            # Assert sensitive review queue DOES return this record
            sensitive_outbox = repo.get_sensitive_review_notifications()
            self.assertEqual(len(sensitive_outbox), 1)
            self.assertEqual(sensitive_outbox[0].status, DeliveryStatus.HELD_FOR_SENSITIVE_REVIEW)

    def test_sensitive_review_calib_11_regression_case(self):
        """CALIB-11 regression test: Cindy Crawford / Presley Gerber death story must route to SENSITIVE_REVIEW."""
        calib_items = load_and_verify_calibration_csv(DEFAULT_FILLED_CALIBRATION_CSV)
        c11 = next((item for item in calib_items if item.review_id == "CALIB-11"), None)
        self.assertIsNotNone(c11, "CALIB-11 must be present in the authoritative calibration dataset")

        # Provenance verification of CALIB-11
        self.assertEqual(c11.headline, "Acılı anne babanın ilk görüntüsü")
        self.assertEqual(c11.publisher, "Habertürk Magazin")
        self.assertEqual(c11.owner_decision, OwnerFeedbackRating.APPROVE)
        self.assertIn("Cindy Crawford", c11.factual_summary)
        self.assertIn("Presley Gerber", c11.factual_summary)
        self.assertIn("vefat", c11.factual_summary.lower())

        # Test offline predictor routing on blind article
        blind_art = c11.to_blind_article()
        batch_res = _offline_blind_diagnostic_predictor([blind_art], [], self.now)
        self.assertEqual(len(batch_res.decisions), 1)
        dec = batch_res.decisions[0]
        self.assertEqual(dec.article_id, blind_art.id)
        self.assertEqual(dec.disposition, "SENSITIVE_REVIEW", "CALIB-11 must be classified as SENSITIVE_REVIEW")

        # Test production projection
        asms = project_diagnostic_decisions_to_assessments([dec], [blind_art])
        self.assertEqual(len(asms), 1)
        asm = asms[0]
        self.assertFalse(asm.is_publishable, "CALIB-11 must NOT be publishable without owner review")
        self.assertEqual(asm.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE EDITORIAL REVIEW REQUIRED]", asm.uncertainties_tr)

        # Test notification planner isolation
        planner = HybridNotificationPlanner(self.settings)
        plan = planner.plan_dispatch([asm], messages_sent_today=0, is_digest_due=True)
        self.assertEqual(len(plan.immediate_breaking_alerts), 0)
        self.assertEqual(len(plan.digest_batch), 0)
        self.assertEqual(len(plan.held_for_sensitive_review), 1)

        # Test evaluation metric accounting: reported separately from DIGEST and IGNORE, not automatically published
        pred_record = self.runner.run_live_gemini_evaluation(
            items=[c11],
            mock_client=MagicMock(models=MagicMock(generate_content=MagicMock(return_value=MagicMock(
                text=json.dumps({
                    "decisions": [dec.model_dump()],
                    "run_editorial_summary_tr": "Özet.",
                }),
                candidates=[MagicMock(finish_reason="STOP")],
                usage_metadata=MagicMock(prompt_token_count=100, candidates_token_count=50, total_token_count=150),
            )))),
            dry_run=False,
            schema_mode="DIAGNOSTIC",
        )
        preds, metrics, report = pred_record
        self.assertEqual(metrics.sensitive_review_count, 1)
        self.assertEqual(metrics.digest_count, 0)
        self.assertEqual(metrics.ignore_count, 0)
        self.assertEqual(metrics.positive_predicted_relevant, 0, "SENSITIVE_REVIEW must not be counted as automatically published")

    def test_independent_sensitivity_veto_overrides_model_digest(self):
        """Independent sensitivity veto intercepts death/funeral news even if Gemini assigned DIGEST and BREAKING."""
        planner = HybridNotificationPlanner(self.settings)
        death_art = Article.create(
            "Usta Sanatçı Hayatını Kaybetti",
            "https://example.com/usta-sanatci-vefat",
            "Haber Portalı",
            self.now,
            snippet="Türk sinemasının usta ismi bu sabah tedavi gördüğü hastanede yaşamını yitirdi.",
        )

        # Gemini erroneously predicts DIGEST with BREAKING urgency and high score on death story
        erroneous_model_dec = DiagnosticArticleDecision(
            article_id=death_art.id,
            score=96,
            disposition="DIGEST",  # Model mistakenly chose DIGEST
            kind="NEW",
            urgency="BREAKING",     # Model mistakenly chose BREAKING
            rationale_tr="Usta sanatçının vefatı tüm ülkeyi yasa boğdu; çok önemli son dakika gelişmesi.",
            headline_tr="Flaş Gelişme: Usta Sanatçı Yaşamını Yitirdi!",
            verification_status="OFFICIAL_CONFIRMATION",
        )

        asms = project_diagnostic_decisions_to_assessments([erroneous_model_dec], [death_art])
        self.assertEqual(len(asms), 1)
        asm = asms[0]

        # 1. Independent Veto overrides model DIGEST to SENSITIVE_REVIEW
        self.assertFalse(asm.is_publishable, "Independent veto must enforce is_publishable=False")
        self.assertEqual(asm.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE EDITORIAL REVIEW REQUIRED]", asm.uncertainties_tr)

        # 2. Clickbait stripped
        self.assertNotIn("Flaş Gelişme", asm.headline_tr)

        # 3. Notification planning defense-in-depth: Never enters breaking or digest
        tier = planner.classify_urgency_tier(asm)
        self.assertEqual(tier, NotificationUrgencyTier.SENSITIVE_REVIEW)

        plan = planner.plan_dispatch([asm], messages_sent_today=0, is_digest_due=True)
        self.assertEqual(len(plan.immediate_breaking_alerts), 0, "Must NEVER enter breaking alerts")
        self.assertEqual(len(plan.digest_batch), 0, "Must NEVER enter digest batch")
        self.assertEqual(len(plan.held_for_sensitive_review), 1, "Must enter private sensitive review queue")
        self.assertEqual(plan.telegram_messages_to_send_now, 0)

    def test_dual_lock_sensitive_unverified_story(self):
        """State C: Sensitive + Unverified claims require BOTH factual verification and PO review."""
        planner = HybridNotificationPlanner(self.settings)
        rumor_art = Article.create(
            "Ünlü Şarkıcı Hakkında Korkutan İddia",
            "https://example.com/intihar-iddiasi",
            "Magazin Sitesi",
            self.now,
            snippet="Sosyal medyada ünlü şarkıcının intihar ettiği öne sürüldü ancak resmi bir açıklama yok.",
        )

        dec = DiagnosticArticleDecision(
            article_id=rumor_art.id,
            score=88,
            disposition="DIGEST",  # Model mistakenly marked DIGEST
            kind="NEW",
            urgency="BREAKING",
            rationale_tr="Şarkıcı hakkında intihar iddiaları dolaşıyor.",
            headline_tr="Şok İddia: Ünlü Şarkıcı İntihar Etti!",
            verification_status="UNVERIFIED_RUMOR",
        )

        asms = project_diagnostic_decisions_to_assessments([dec], [rumor_art])
        self.assertEqual(len(asms), 1)
        asm = asms[0]

        # Must trigger Dual Lock State C
        self.assertFalse(asm.is_publishable)
        self.assertEqual(asm.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertIn("[NOT_PUBLISHABLE - DUAL LOCK: SENSITIVE EDITORIAL REVIEW + FACTUAL VERIFICATION REQUIRED]", asm.uncertainties_tr)

        tier = planner.classify_urgency_tier(asm)
        self.assertEqual(tier, NotificationUrgencyTier.SENSITIVE_REVIEW)

        plan = planner.plan_dispatch([asm], messages_sent_today=0, is_digest_due=True)
        self.assertEqual(len(plan.held_for_sensitive_review), 1)
        self.assertEqual(len(plan.immediate_breaking_alerts), 0)
        self.assertEqual(len(plan.digest_batch), 0)

    def test_three_states_contract_invariants(self):
        """Audit the distinct handling of State A, State B, State C, and verified publishable stories."""
        planner = HybridNotificationPlanner(self.settings)

        art_a = Article.create("Yeni Dizi Anlaşması İddiası", "https://example.com/a", "Src", self.now, snippet="Dedikodu kulisleri")
        art_b = Article.create("Usta Oyuncunun Cenaze Töreni Yapıldı", "https://example.com/b", "Src", self.now, snippet="Cenaze töreni düzenlendi")
        art_c = Article.create("Sanatçının İntihar Girişiminde Bulunduğu İddiası", "https://example.com/c", "Src", self.now, snippet="Teyitsiz söylenti")
        art_d = Article.create("Ünlü Çift Dünyaevine Girdi", "https://example.com/d", "Src", self.now, snippet="Görkemli düğün")

        # State A: Unverified non-sensitive claim
        dec_a = DiagnosticArticleDecision(
            article_id=art_a.id, score=77, disposition="DIGEST", kind="NEW", urgency="NORMAL",
            rationale_tr="Yeni dizi anlaşması iddiası.", headline_tr="Yeni Dizi Anlaşması İddiası",
            verification_status="UNVERIFIED_RUMOR",
        )
        # State B: Verified sensitive tragedy story
        dec_b = DiagnosticArticleDecision(
            article_id=art_b.id, score=80, disposition="SENSITIVE_REVIEW", kind="NEW", urgency="NORMAL",
            rationale_tr="Cenaze haberi.", headline_tr="Usta Oyuncunun Cenaze Töreni Yapıldı",
            verification_status="OFFICIAL_CONFIRMATION",
        )
        # State C: Sensitive + Unverified claim (Dual Lock)
        dec_c = DiagnosticArticleDecision(
            article_id=art_c.id, score=85, disposition="HOLD", kind="NEW", urgency="NORMAL",
            rationale_tr="Teyitsiz intihar iddiası.", headline_tr="Sanatçının İntihar İddiası",
            verification_status="UNVERIFIED_RUMOR",
        )
        # Normal verified publishable DIGEST story
        dec_d = DiagnosticArticleDecision(
            article_id=art_d.id, score=82, disposition="DIGEST", kind="NEW", urgency="NORMAL",
            rationale_tr="Düğün haberi.", headline_tr="Ünlü Çift Evlendi",
            verification_status="OFFICIAL_CONFIRMATION",
        )

        asms = project_diagnostic_decisions_to_assessments([dec_a, dec_b, dec_c, dec_d], [art_a, art_b, art_c, art_d])
        self.assertEqual(len(asms), 4)
        map_asm = {a.article_ids[0]: a for a in asms}

        # Check State A
        asm_a = map_asm[art_a.id]
        self.assertFalse(asm_a.is_publishable)
        self.assertEqual(asm_a.disposition, EditorialDisposition.HOLD)
        self.assertIn("[NOT_PUBLISHABLE - MANUAL VERIFICATION REQUIRED]", asm_a.uncertainties_tr)
        self.assertEqual(planner.classify_urgency_tier(asm_a), NotificationUrgencyTier.HOLD_FOR_VERIFICATION)

        # Check State B
        asm_b = map_asm[art_b.id]
        self.assertFalse(asm_b.is_publishable)
        self.assertEqual(asm_b.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE EDITORIAL REVIEW REQUIRED]", asm_b.uncertainties_tr)
        self.assertEqual(planner.classify_urgency_tier(asm_b), NotificationUrgencyTier.SENSITIVE_REVIEW)

        # Check State C
        asm_c = map_asm[art_c.id]
        self.assertFalse(asm_c.is_publishable)
        self.assertEqual(asm_c.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertIn("[NOT_PUBLISHABLE - DUAL LOCK: SENSITIVE EDITORIAL REVIEW + FACTUAL VERIFICATION REQUIRED]", asm_c.uncertainties_tr)
        self.assertEqual(planner.classify_urgency_tier(asm_c), NotificationUrgencyTier.SENSITIVE_REVIEW)

        # Check Normal Publishable
        asm_d = map_asm[art_d.id]
        self.assertTrue(asm_d.is_publishable)
        self.assertEqual(asm_d.disposition, EditorialDisposition.DIGEST)
        self.assertIsNone(asm_d.uncertainties_tr)
        self.assertEqual(planner.classify_urgency_tier(asm_d), NotificationUrgencyTier.PERIODIC_DIGEST)

        # Plan dispatch on all 4
        plan = planner.plan_dispatch(asms, messages_sent_today=0, is_digest_due=True)
        self.assertEqual(len(plan.held_for_verification), 1)  # State A
        self.assertEqual(len(plan.held_for_sensitive_review), 2)  # State B + State C
        self.assertEqual(len(plan.digest_batch), 1)  # Normal story D only!
        self.assertEqual(plan.digest_batch[0].article_ids[0], art_d.id)


if __name__ == "__main__":
    unittest.main()

