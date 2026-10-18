"""Tests for Phase 3C-2A: Human Editorial Calibration Integration,
Idempotent SQLite/Turso Feedback Import, Backlog & Pair Analysis,
Factual-Safety Gate Preservation, Hybrid Notification Strategy, and Shadow Evaluation Design.
"""
from __future__ import annotations

import csv
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from efso_radar.ai.calibration import (
    DEFAULT_CALIBRATION_ANALYSIS_MD,
    DEFAULT_FILLED_CALIBRATION_CSV,
    INDEPENDENT_HOLDOUT_SET,
    REQUIRED_CSV_COLUMNS,
    BlindShadowEvaluationRunner,
    CalibrationAnalyzer,
    HybridNotificationPlanner,
    design_shadow_gemini_evaluation,
    evaluate_discovery_ranking_comparison,
    evaluate_independent_holdout_set,
    evaluate_notification_quality,
    import_calibration_feedback,
    load_and_verify_calibration_csv,
    model_monthly_operating_costs,
    simulate_monitoring_schedules,
)
from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.cost import check_monthly_budget_guard
from efso_radar.ai.prompts import build_editorial_prompt
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    EditorialAssessment,
    EditorialKind,
    Market,
    NotificationUrgencyTier,
    OwnerFeedbackRating,
    StoryCategory,
    SuggestedFormat,
    VerificationStatus,
)
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from tests.test_repository_parity import create_mock_hrana_transport


def _write_synthetic_50_calibration_csv(target_path: Path) -> None:
    """Create a valid 50-row calibration CSV matching the exact distribution for CI-safe testing."""
    with open(target_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(REQUIRED_CSV_COLUMNS))
        writer.writeheader()
        for i in range(1, 51):
            review_id = f"CALIB-{i:02d}"
            enters_yn = "Y" if i <= 32 else "N"
            priority = round(13.0 - (i * 0.18), 2)

            if i in (31, 32):
                decision = "IMPORTANT_UPDATE"
            elif i in (3, 6, 14, 16, 22, 36, 39, 41, 44, 45):
                decision = "REJECT"
            elif i in (4, 7, 15, 27, 33, 48):
                decision = "TOO_WEAK"
            else:
                decision = "APPROVE"

            category = "RUMOR_UNVERIFIED" if i in (27, 47) else "MAGAZIN_TR"
            v_notes = (
                "Kulislere dayalı iddia ('insider claims')."
                if i == 28
                else ("Teyitsiz / Şüpheli dedikodu: kara büyü" if i in (27, 47) else "Doğrulanmış haber.")
            )
            notes = f"Editör notu {i}" if i <= 29 else ""

            writer.writerow(
                {
                    "review_id": review_id,
                    "headline": f"Örnek Haber Başlığı {i}",
                    "publisher": "Habertürk Magazin" if i % 2 == 0 else "Variety",
                    "source_url": f"https://news.google.com/rss/articles/item-{i}" if i > 32 else f"https://example.com/news/{i}",
                    "published_at": "2026-10-11 12:00:00+00:00",
                    "discovered_at": "2026-10-11 15:06:29.847108+00:00",
                    "category": category,
                    "factual_summary": f"Örnek haber özeti {i}.",
                    "supporting_sources": "Habertürk Magazin",
                    "verification_uncertainty_notes": v_notes,
                    "discovery_priority": str(priority),
                    "enters_ai_cutoff_yn": enters_yn,
                    "owner_decision": decision,
                    "owner_notes": notes,
                }
            )


class TestCalibrationPhase3C(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmpdir.name)
        self.csv_path = self.tmp_path / "test_calibration.csv"
        _write_synthetic_50_calibration_csv(self.csv_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_csv_validation_and_idempotent_import_sqlite_and_turso(self):
        """Verify 50-row CSV validation and idempotent import across both SQLite and Turso."""
        # 1. SQLite Repository
        sqlite_repo = SqliteStateRepository(self.tmp_path / "test_calib.db")
        summary1 = import_calibration_feedback(sqlite_repo, csv_path=self.csv_path)
        self.assertEqual(summary1.total_verified_rows, 50)
        self.assertEqual(summary1.imported_count, 50)
        self.assertEqual(summary1.repository_stored_count, 50)
        self.assertEqual(summary1.written_notes_count, 29)
        self.assertEqual(summary1.decision_counts["APPROVE"], 32)
        self.assertEqual(summary1.decision_counts["IMPORTANT_UPDATE"], 2)
        self.assertEqual(summary1.decision_counts["REJECT"], 10)
        self.assertEqual(summary1.decision_counts["TOO_WEAK"], 6)

        # Re-importing must be 100% idempotent
        summary2 = import_calibration_feedback(sqlite_repo, csv_path=self.csv_path)
        self.assertEqual(summary2.repository_stored_count, 50)

        # 2. Turso Repository (via Hrana v2 mock transport)
        mem_conn = sqlite3.connect(":memory:")
        try:
            transport = create_mock_hrana_transport(mem_conn)
            turso_repo = TursoStateRepository(
                database_url="libsql://efso-calib.turso.io",
                auth_token="test-token",
                transport=transport,
            )
            t_summary1 = import_calibration_feedback(turso_repo, csv_path=self.csv_path)
            self.assertEqual(t_summary1.repository_stored_count, 50)
            t_summary2 = import_calibration_feedback(turso_repo, csv_path=self.csv_path)
            self.assertEqual(t_summary2.repository_stored_count, 50)
        finally:
            mem_conn.close()

    def test_csv_validator_rejects_malformed_or_incomplete_files(self):
        """Validator raises clear errors if file is missing or decisions are missing/invalid."""
        with self.assertRaises(FileNotFoundError):
            load_and_verify_calibration_csv(self.tmp_path / "nonexistent.csv")

        bad_csv = self.tmp_path / "bad.csv"
        _write_synthetic_50_calibration_csv(bad_csv)
        with self.assertRaises(ValueError):
            load_and_verify_calibration_csv(bad_csv, expected_count=49)

    @unittest.skipUnless(
        DEFAULT_FILLED_CALIBRATION_CSV.exists(),
        "Authoritative owner calibration CSV is gitignored and only present locally",
    )
    def test_authoritative_local_owner_calibration_files(self):
        """Verify the actual local 50-item owner CSV and analysis Markdown."""
        self.assertTrue(DEFAULT_FILLED_CALIBRATION_CSV.exists())
        self.assertTrue(DEFAULT_CALIBRATION_ANALYSIS_MD.exists())

        # 1. Verify SHA-256 fingerprint matches authoritative reference file exactly
        import hashlib
        csv_bytes = DEFAULT_FILLED_CALIBRATION_CSV.read_bytes()
        csv_sha256 = hashlib.sha256(csv_bytes).hexdigest()
        self.assertEqual(
            csv_sha256,
            "108799c19f513327cb04872cec5dbcfb97107f2bd8bf2353acf28e3111ab2305",
            "Authoritative calibration CSV SHA-256 fingerprint mismatch!",
        )

        items = load_and_verify_calibration_csv(DEFAULT_FILLED_CALIBRATION_CSV)
        self.assertEqual(len(items), 50)

        # 2. Focused regression assertions confirming canonical event pairs reference correct people & events
        by_id = {item.review_id: item for item in items}

        # PAIR-01: CALIB-01 and CALIB-31 must reference Cagatay Ulusoy and his village/hometown visit
        text_01 = f"{by_id['CALIB-01'].headline} {by_id['CALIB-01'].factual_summary}".lower()
        text_31 = f"{by_id['CALIB-31'].headline} {by_id['CALIB-31'].factual_summary}".lower()
        self.assertTrue("cagatay" in text_01 or "çağatay" in text_01)
        self.assertIn("ulusoy", text_01)
        self.assertTrue("bulgaristan" in text_01 or "köy" in text_01)
        self.assertTrue("cagatay" in text_31 or "çağatay" in text_31)
        self.assertIn("ulusoy", text_31)

        # PAIR-02: CALIB-30 and CALIB-32 must reference Lvbel C5 court decision and reading requirement
        text_30 = f"{by_id['CALIB-30'].headline} {by_id['CALIB-30'].factual_summary}".lower()
        text_32 = f"{by_id['CALIB-32'].headline} {by_id['CALIB-32'].factual_summary}".lower()
        self.assertIn("lvbel", text_30)
        self.assertIn("lvbel", text_32)
        self.assertTrue("kitap" in text_30 or "edebiyat" in text_30)
        self.assertIn("kitap", text_32)

        # PAIR-03: CALIB-07 and CALIB-40 must reference Olivia Culpo & Christian McCaffrey second child birth
        text_07 = f"{by_id['CALIB-07'].headline} {by_id['CALIB-07'].factual_summary}".lower()
        text_40 = f"{by_id['CALIB-40'].headline} {by_id['CALIB-40'].factual_summary}".lower()
        self.assertIn("olivia culpo", text_07)
        self.assertIn("olivia culpo", text_40)
        self.assertTrue("ikinci" in text_07 or "çocuk" in text_07)
        self.assertTrue("second" in text_40 or "baby" in text_40)

        # PAIR-04: CALIB-03 and CALIB-44 must reference Sezen Tas tragedy
        text_03 = f"{by_id['CALIB-03'].headline} {by_id['CALIB-03'].factual_summary}".lower()
        text_44 = f"{by_id['CALIB-44'].headline} {by_id['CALIB-44'].factual_summary}".lower()
        self.assertTrue("sezen" in text_03)
        self.assertTrue("sezen" in text_44)
        self.assertIn("14. kat", text_03)

        # CALIB-28: Taylor Swift insider claims about referencing former partners in her songs
        text_28 = f"{by_id['CALIB-28'].headline} {by_id['CALIB-28'].factual_summary}".lower()
        self.assertIn("taylor swift", text_28)
        self.assertIn("exes", text_28)

        analyzer = CalibrationAnalyzer(items)
        comp = analyzer.compare_owner_preferences_with_priority()
        self.assertEqual(comp["total_items"], 50)
        self.assertEqual(comp["total_positive_owner_labels"], 34)  # 32 APPROVE + 2 IMPORTANT_UPDATE
        self.assertEqual(comp["total_negative_owner_labels"], 16)  # 10 REJECT + 6 TOO_WEAK
        self.assertEqual(comp["top_55_total"], 32)
        self.assertEqual(comp["top_55_positive"], 23)
        self.assertEqual(comp["top_55_negative_false_positives"], 9)
        self.assertEqual(comp["backlog_total"], 18)
        self.assertEqual(comp["backlog_positive_missed"], 11)
        self.assertEqual(
            comp["backlog_positive_ids"],
            [
                "CALIB-34", "CALIB-35", "CALIB-37", "CALIB-38", "CALIB-40",
                "CALIB-42", "CALIB-43", "CALIB-46", "CALIB-47", "CALIB-49", "CALIB-50",
            ],
        )

        # Verify the 11 backlog diagnostics
        backlog_diags = analyzer.analyze_backlog_positives()
        self.assertEqual(len(backlog_diags), 11)

        # Verify the 4 duplicate/inconsistent pair audits preserve human labels without mutation
        pairs = analyzer.audit_duplicate_and_inconsistent_pairs()
        self.assertEqual(len(pairs), 4)
        self.assertEqual(pairs[0].first_decision, "APPROVE")
        self.assertEqual(pairs[0].second_decision, "IMPORTANT_UPDATE")
        self.assertEqual(pairs[2].first_decision, "TOO_WEAK")
        self.assertEqual(pairs[2].second_decision, "APPROVE")

        # Verify factual-safety preservation on CALIB-28 and CALIB-47
        safety_items = {s.review_id: s for s in analyzer.evaluate_factual_safety_on_calibration()}
        self.assertIn("CALIB-28", safety_items)
        self.assertIn("CALIB-47", safety_items)
        self.assertEqual(safety_items["CALIB-28"].owner_decision, "APPROVE")
        self.assertTrue(safety_items["CALIB-28"].requires_verification_hold)
        self.assertFalse(safety_items["CALIB-28"].can_trigger_immediate_alert)
        self.assertEqual(safety_items["CALIB-47"].owner_decision, "APPROVE")
        self.assertTrue(safety_items["CALIB-47"].requires_verification_hold)
        self.assertFalse(safety_items["CALIB-47"].can_trigger_immediate_alert)

    def test_hybrid_notification_planner_distinguishes_urgency_and_preserves_overflow(self):
        """HybridNotificationPlanner separates breaking alerts, 2h digests, verification holds, and never drops overflow."""
        settings = Settings(
            notification_strategy="HYBRID",
            min_score=82,
            breaking_alert_min_score=92,
            digest_interval_minutes=120,
            target_daily_telegram_messages=6,
            max_items_per_digest=2,
            quiet_hours_enabled=False,
        )
        planner = HybridNotificationPlanner(settings)

        def _make_assessment(
            aid: str,
            score: int,
            v_status: VerificationStatus = VerificationStatus.REPUTABLE_REPORTING,
            kind: EditorialKind = EditorialKind.NEW,
        ) -> EditorialAssessment:
            art = Article(
                id=aid,
                title=f"Title {aid}",
                url=f"https://example.com/{aid}",
                source="Habertürk Magazin",
                market=Market.TR,
                published_at=datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc),
            )
            return EditorialAssessment(
                article_ids=[aid],
                score=score,
                kind=kind,
                headline_tr=f"Haber Başlığı {aid}",
                cover_title_tr="KAPAK",
                caption_idea_tr="Caption",
                why_efso_tr="Önemli gelişme",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
                verification_status=v_status,
                source_urls=[art.url],
                supporting_articles=[art],
            )

        breaking_pick = _make_assessment("brk1", score=95, v_status=VerificationStatus.OFFICIAL_CONFIRMATION)
        digest_pick1 = _make_assessment("dig1", score=88, v_status=VerificationStatus.REPUTABLE_REPORTING)
        digest_pick2 = _make_assessment("dig2", score=86, v_status=VerificationStatus.FIRST_PARTY_STATEMENT)
        digest_overflow = _make_assessment("dig3", score=84, v_status=VerificationStatus.REPUTABLE_REPORTING)
        rumor_approved = _make_assessment("rum1", score=94, v_status=VerificationStatus.UNVERIFIED_RUMOR)
        weak_pick = _make_assessment("weak1", score=75, v_status=VerificationStatus.REPUTABLE_REPORTING)

        plan = planner.plan_dispatch(
            [breaking_pick, digest_pick1, digest_pick2, digest_overflow, rumor_approved, weak_pick],
            messages_sent_today=0,
            is_digest_due=True,
        )

        # 1. Breaking verified story (95 >= 92) goes to immediate breaking alerts
        self.assertEqual(len(plan.immediate_breaking_alerts), 1)
        self.assertEqual(plan.immediate_breaking_alerts[0].article_ids[0], "brk1")

        # 2. Rumor story even with score 94 MUST be held for verification, never immediate alert!
        self.assertEqual(len(plan.held_for_verification), 1)
        self.assertEqual(plan.held_for_verification[0].article_ids[0], "rum1")

        # 3. Digest batch takes top 2 (max_items_per_digest=2), and overflow is deferred (NOT discarded!)
        self.assertEqual(len(plan.digest_batch), 2)
        self.assertEqual(len(plan.deferred_for_next_digest), 1)
        self.assertEqual(plan.deferred_for_next_digest[0].article_ids[0], "dig3")

        # 4. Below-threshold story is suppressed
        self.assertEqual(len(plan.suppressed), 1)
        self.assertEqual(plan.suppressed[0].article_ids[0], "weak1")

        # 5. Total Telegram messages sent now: 1 breaking + 1 digest = 2 messages
        self.assertEqual(plan.telegram_messages_to_send_now, 2)
        self.assertIsNotNone(plan.formatted_digest_message)
        self.assertIn("2 SAATLİK EDİTORYAL ÖZET", plan.formatted_digest_message or "")

    def test_hybrid_planner_never_discards_when_daily_cap_reached(self):
        """When daily message cap (6) is reached, even breaking stories are deferred to queue rather than lost."""
        settings = Settings(
            notification_strategy="HYBRID",
            target_daily_telegram_messages=6,
            breaking_alert_min_score=92,
        )
        planner = HybridNotificationPlanner(settings)
        art = Article(
            id="cap1",
            title="Major News",
            url="https://example.com/cap1",
            source="Variety",
            market=Market.EN,
            published_at=datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc),
        )
        pick = EditorialAssessment(
            article_ids=["cap1"],
            score=96,
            kind=EditorialKind.NEW,
            headline_tr="Çok önemli gelişme açıklandı",
            cover_title_tr="FLAŞ",
            caption_idea_tr="Detaylar",
            why_efso_tr="Büyük olay",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            source_urls=[art.url],
            supporting_articles=[art],
        )

        plan = planner.plan_dispatch([pick], messages_sent_today=6, is_digest_due=True)
        self.assertEqual(plan.telegram_messages_to_send_now, 0)
        self.assertEqual(len(plan.deferred_for_next_digest), 1)
        self.assertEqual(plan.deferred_for_next_digest[0].article_ids[0], "cap1")

    def test_shadow_gemini_evaluation_design_is_within_budget_and_requires_approval(self):
        """Shadow evaluation design computes token/cost estimates without making live API calls."""
        items = load_and_verify_calibration_csv(self.csv_path)
        design = design_shadow_gemini_evaluation(items)
        self.assertEqual(design.total_candidates, 50)
        self.assertGreater(design.estimated_input_tokens, 1000)
        self.assertLess(design.estimated_single_pass_cost_usd, 0.01)
        self.assertTrue(design.within_budget)
        self.assertTrue(design.requires_explicit_owner_approval)
        self.assertEqual(len(design.validation_criteria), 6)

    def test_phase3c2b_untuned_baseline_vs_calibrated_ranking_improvement(self):
        """Phase 3C-2B ranking resolves priority inversion, improves positive recall, and clusters duplicate pairs."""
        target_csv = DEFAULT_FILLED_CALIBRATION_CSV if DEFAULT_FILLED_CALIBRATION_CSV.exists() else self.csv_path
        items = load_and_verify_calibration_csv(target_csv)
        cmp = evaluate_discovery_ranking_comparison(items, cutoff_k=32)

        # 1. Untuned baseline matches recorded Phase 3C-2A numbers
        self.assertEqual(cmp.untuned_baseline.total_positive, 34)
        self.assertEqual(cmp.untuned_baseline.positive_in_cutoff, 23)
        self.assertEqual(cmp.untuned_baseline.negative_in_cutoff, 9)

        # 2. Calibrated ranking improves positive recall and drastically cuts false positives
        self.assertGreaterEqual(cmp.calibrated_raw_top32.positive_in_cutoff, 28)
        self.assertLessEqual(cmp.calibrated_raw_top32.negative_in_cutoff, 3)
        self.assertEqual(cmp.calibrated_raw_top32.reject_in_cutoff, 0)
        self.assertGreater(
            cmp.calibrated_raw_top32.mean_positive_priority,
            cmp.calibrated_raw_top32.mean_negative_priority + 4.0,
        )

        # 3. Clustered unique positive event recall reaches >= 90%
        self.assertGreaterEqual(cmp.calibrated_clustered_unique_positive_recall, 0.90)

        # 4. Duplicate pairs cluster together and select clear subject+event canonical headlines
        pair_map = {p.pair_id: p for p in cmp.pair_comparisons}
        self.assertIn("PAIR-01", pair_map)
        self.assertIn("PAIR-02", pair_map)
        self.assertIn("PAIR-03", pair_map)
        self.assertIn("PAIR-04", pair_map)
        self.assertTrue(pair_map["PAIR-01"].clustered_together_after)
        self.assertTrue(pair_map["PAIR-02"].clustered_together_after)
        self.assertTrue(pair_map["PAIR-03"].clustered_together_after)
        self.assertTrue(pair_map["PAIR-04"].clustered_together_after)
        if DEFAULT_FILLED_CALIBRATION_CSV.exists():
            self.assertEqual(pair_map["PAIR-01"].canonical_representative_id, "CALIB-31")
            self.assertEqual(pair_map["PAIR-02"].canonical_representative_id, "CALIB-30")
            self.assertEqual(pair_map["PAIR-03"].canonical_representative_id, "CALIB-40")

        # 5. Baseline comparability metadata is explicitly documented
        self.assertEqual(cmp.sample_size, 50)
        self.assertEqual(cmp.original_cutoff55_sampled_count, 32)
        self.assertEqual(cmp.original_backlog_sampled_count, 18)
        self.assertFalse(cmp.full_historical_pool_available)
        self.assertIn("stratified", cmp.comparability_limitation_note)
        self.assertGreaterEqual(cmp.calibrated_positives_above_baseline_cutoff_score, 32)

    def test_phase3c2b_independent_holdout_set_anti_overfitting(self):
        """Constructed 20-scenario regression suite is explicitly classified as non-independent regression coverage."""
        self.assertEqual(len(INDEPENDENT_HOLDOUT_SET), 20)
        res = evaluate_independent_holdout_set(cutoff_k=12)
        self.assertEqual(res["dataset_type"], "CONSTRUCTED_REGRESSION_SCENARIOS")
        self.assertFalse(res["is_independent_real_world_holdout"])
        self.assertEqual(res["total_holdout_items"], 20)
        self.assertEqual(res["positive_total"], 12)
        self.assertEqual(res["negative_total"], 8)
        self.assertGreaterEqual(res["positive_recall_at_k"], 0.91)
        self.assertEqual(res["negative_in_top_k"], 0)
        self.assertGreater(res["mean_positive_score"], res["mean_negative_score"] + 6.0)

    def test_phase3c2b_blind_shadow_runner_hygiene_batching_cache_and_budget(self):
        """BlindShadowEvaluationRunner enforces blind hygiene, bounded batches, caching, and $0.05 budget ceiling."""
        target_csv = DEFAULT_FILLED_CALIBRATION_CSV if DEFAULT_FILLED_CALIBRATION_CSV.exists() else self.csv_path
        items = load_and_verify_calibration_csv(target_csv)
        db_path = self.tmp_path / "shadow_eval_test.db"
        repo = SqliteStateRepository(db_path)
        settings = Settings(max_shadow_eval_budget_usd=0.05)
        runner = BlindShadowEvaluationRunner(settings=settings, repository=repo, batch_size=8)

        # 1. Verify blind articles strip all human labels and calibration hints
        blind_arts = runner.to_blind_articles(items)
        self.assertEqual(len(blind_arts), 50)
        for art in blind_arts:
            self.assertEqual(art.raw_metadata, {})
        sample_prompt = build_editorial_prompt(blind_arts[:8], [], blind_arts[0].published_at)
        self.assertTrue(runner.verify_prompt_is_blind(sample_prompt, items))
        self.assertFalse(runner.verify_prompt_is_blind(sample_prompt + "\nCALIB-01 owner_decision", items))

        # 2. Verify pre-run budget estimation across 7 bounded batches with explicit expected/worst-case formulas
        budget_est = runner.estimate_shadow_run_budget(items)
        self.assertEqual(budget_est["num_batches"], 7)
        self.assertEqual(budget_est["max_requests"], 7)
        self.assertEqual(budget_est["automatic_retries_permitted"], 0)
        self.assertEqual(budget_est["policy"], "SAFEST_SIMPLE_POLICY_SINGLE_PASS")
        self.assertEqual(budget_est["input_rate_per_million_usd"], 0.30)
        self.assertEqual(budget_est["output_rate_per_million_usd"], 2.50)
        self.assertEqual(budget_est["worst_case_retry_multiplier"], 1.00)
        self.assertIn("formula_expected", budget_est)
        self.assertIn("formula_worst_case", budget_est)
        self.assertTrue(budget_est["within_budget_ceiling"])
        self.assertLess(budget_est["expected_cost_usd"], 0.05)
        self.assertLessEqual(budget_est["worst_case_cost_usd"], 0.05)
        self.assertLessEqual(budget_est["single_pass_max_cost_usd"], 0.05)

        # 3. Execute first offline blind shadow evaluation pass (persists to cache)
        preds1, metrics1 = runner.run_blind_evaluation(items)
        self.assertEqual(len(preds1), 50)
        self.assertEqual(metrics1.evaluation_mode, "SIMULATED_OFFLINE_HEURISTIC")
        self.assertFalse(metrics1.is_measured_gemini_accuracy)
        self.assertEqual(metrics1.batches_executed, 7)
        self.assertEqual(metrics1.batches_reused_from_cache, 0)
        self.assertTrue(metrics1.blind_hygiene_verified)
        self.assertTrue(metrics1.all_referenced_ids_valid)
        self.assertTrue(metrics1.within_budget_ceiling)
        self.assertTrue(metrics1.safety_hold_calib_28_respected)
        self.assertTrue(metrics1.safety_hold_calib_47_respected)
        self.assertTrue(metrics1.duplicate_pairs_suppressed)
        self.assertEqual(metrics1.reject_false_positive_count, 0)

        # 4. Execute second pass -> all 7 batches reused from cache with 0 new API calls / 0 cost
        preds2, metrics2 = runner.run_blind_evaluation(items)
        self.assertEqual(len(preds2), 50)
        self.assertEqual(metrics2.batches_executed, 0)
        self.assertEqual(metrics2.batches_reused_from_cache, 7)
        self.assertEqual(metrics2.total_estimated_cost_usd, 0.0)
        self.assertTrue(all(p.from_cache for p in preds2))

        # 5. Verify strict budget ceiling circuit-breaker blocks when budget is tiny (0.001 USD)
        tiny_budget_runner = BlindShadowEvaluationRunner(
            settings=Settings(max_shadow_eval_budget_usd=0.001),
            repository=None,
            batch_size=8,
        )
        with self.assertRaises(AIEvaluationError):
            tiny_budget_runner.run_blind_evaluation(items)

    def test_phase3c2b_notification_quality_and_monitoring_schedule_simulations(self):
        """Notification thresholds, 2h/3h/4h scan schedules, and 30/31-day cost models meet PO targets."""
        target_csv = DEFAULT_FILLED_CALIBRATION_CSV if DEFAULT_FILLED_CALIBRATION_CSV.exists() else self.csv_path
        items = load_and_verify_calibration_csv(target_csv)
        runner = BlindShadowEvaluationRunner()
        preds, _ = runner.run_blind_evaluation(items)

        # 1. Separate notification quality evaluation
        notif_scenarios = evaluate_notification_quality(items, preds)
        self.assertEqual(len(notif_scenarios), 3)
        balanced = notif_scenarios[0]
        self.assertEqual(balanced.digest_min_score, 78)
        self.assertEqual(balanced.min_score, 82)
        self.assertEqual(balanced.breaking_min_score, 92)
        self.assertGreaterEqual(balanced.positive_recall_total, 0.85)
        self.assertGreaterEqual(balanced.verification_hold_count, 2)

        # 2. Schedule simulation across 2h, 3h (preferred), and 4h scan + 6h digest
        sims = simulate_monitoring_schedules()
        self.assertEqual([s.scan_interval_minutes for s in sims], [120, 180, 240])
        for s in sims:
            self.assertEqual(s.digest_interval_minutes, 360)
            self.assertEqual(s.quiet_day_telegram_messages, 0)
            self.assertLessEqual(s.normal_day_telegram_messages, 6)
            self.assertLessEqual(s.breaking_spike_telegram_messages, 6)
            self.assertEqual(s.missed_run_stories_lost, 0)
            self.assertEqual(s.duplicate_notifications_sent, 0)
            self.assertEqual(s.unverified_rumors_sent_as_facts, 0)
            self.assertEqual(len(s.scenario_verdicts), 11)

        # 3. Monthly operating cost model (30-day & 31-day, Quiet/Normal/High-Volume)
        projections = model_monthly_operating_costs(usd_to_try=36.0)
        self.assertEqual(len(projections), 18)  # 3 schedules * 2 month lengths * 3 volume scenarios
        for proj in projections:
            self.assertTrue(proj.within_preferred_0_try_on_free_tier)
            self.assertTrue(proj.within_100_try_max_on_paid_tier)
            if proj.scan_interval_minutes >= 180 and "High-Volume" not in proj.volume_scenario:
                self.assertTrue(proj.within_50_try_ideal_on_paid_tier)

    def test_phase3c2b_monthly_budget_guard_circuit_breaker(self):
        """check_monthly_budget_guard blocks AI evaluation when monthly call count or USD ceiling is reached."""
        allowed, reason = check_monthly_budget_guard(
            est_run_cost_usd=0.001,
            current_monthly_cost_usd=0.10,
            current_monthly_calls=50,
            max_monthly_cost_usd=1.20,
            max_monthly_calls=300,
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "OK")

        blocked_calls, reason_calls = check_monthly_budget_guard(
            est_run_cost_usd=0.001,
            current_monthly_cost_usd=0.10,
            current_monthly_calls=300,
            max_monthly_cost_usd=1.20,
            max_monthly_calls=300,
        )
        self.assertFalse(blocked_calls)
        self.assertIn("exceeds configured monthly call cap", reason_calls)

        blocked_cost, reason_cost = check_monthly_budget_guard(
            est_run_cost_usd=0.001,
            current_monthly_cost_usd=1.1995,
            current_monthly_calls=100,
            max_monthly_cost_usd=1.20,
            max_monthly_calls=300,
        )
        self.assertFalse(blocked_cost)
        self.assertIn("exceeds monthly budget ceiling", reason_cost)

    def test_phase3c2c_live_shadow_evaluation_requires_api_key_when_not_dry_run(self):
        """Calling run_live_gemini_evaluation without API key or authorization raises AIEvaluationError."""
        import os
        import unittest.mock
        target_csv = DEFAULT_FILLED_CALIBRATION_CSV if DEFAULT_FILLED_CALIBRATION_CSV.exists() else self.csv_path
        items = load_and_verify_calibration_csv(target_csv)
        runner = BlindShadowEvaluationRunner(settings=Settings(gemini_api_key=""))
        with unittest.mock.patch.dict(os.environ, {"GEMINI_API_KEY": ""}):
            with self.assertRaises(AIEvaluationError) as ctx:
                runner.run_live_gemini_evaluation(items, api_key="", dry_run=False)
            self.assertIn("GEMINI_API_KEY is required", str(ctx.exception))

    def test_phase3c2c_live_shadow_evaluation_mock_client_and_resume_and_confusion_matrix(self):
        """run_live_gemini_evaluation with mock_client enforces per-batch output tokens, records ledger, and resumes."""
        from unittest.mock import MagicMock
        from efso_radar.ai.schemas import EditorialDecisionBatch, EditorialPickSchema

        target_csv = DEFAULT_FILLED_CALIBRATION_CSV if DEFAULT_FILLED_CALIBRATION_CSV.exists() else self.csv_path
        items = load_and_verify_calibration_csv(target_csv)
        db_path = self.tmp_path / "live_shadow_test.db"
        repo = SqliteStateRepository(db_path)
        settings = Settings(
            gemini_api_key="mock-auth-key",
            gemini_model="gemini-3.5-flash-lite",
            max_shadow_eval_budget_usd=0.05,
        )
        runner = BlindShadowEvaluationRunner(settings=settings, repository=repo, batch_size=8)

        # Setup mock Google GenAI Client
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.text = '{"picks": []}'
        mock_resp.usage_metadata = MagicMock(
            prompt_token_count=1800,
            candidates_token_count=150,
            thoughts_token_count=0,
            total_token_count=1950,
        )
        mock_client.models.generate_content.return_value = mock_resp

        # PASS 1: Execute with mock client
        artifact_json = self.tmp_path / "test_shadow_report.json"
        preds1, metrics1, report1 = runner.run_live_gemini_evaluation(
            items=items,
            mock_client=mock_client,
            dry_run=False,
            output_artifact_path=artifact_json,
            schema_mode="SELECTIVE_PICKS",
        )

        self.assertEqual(len(preds1), 50)
        self.assertEqual(metrics1.batches_executed, 7)
        self.assertEqual(metrics1.batches_reused_from_cache, 0)
        self.assertEqual(metrics1.evaluation_mode, "LIVE_GEMINI_EVALUATION")
        self.assertTrue(metrics1.is_measured_gemini_accuracy)
        self.assertTrue(metrics1.within_budget_ceiling)

        # Verify confusion matrix keys in report
        self.assertIn("confusion_matrix", report1)
        cm = report1["confusion_matrix"]
        self.assertIn("true_positives", cm)
        self.assertIn("true_negatives", cm)
        self.assertIn("false_positives", cm)
        self.assertIn("false_negatives", cm)
        self.assertIn("financial_accounting", report1)
        self.assertTrue(artifact_json.exists())

        # Verify GenerateContentConfig passed to client enforced per-batch max_output_tokens
        # 6 calls for 8 stories (1540 tokens), 1 call for 2 stories (500 tokens)
        calls = mock_client.models.generate_content.call_args_list
        self.assertEqual(len(calls), 7)
        for c in calls[:6]:
            self.assertEqual(c.kwargs["config"].max_output_tokens, 1540)
        self.assertEqual(calls[6].kwargs["config"].max_output_tokens, 500)

        # PASS 2: Re-run should reuse all 7 batches from cache with 0 API calls!
        mock_client.reset_mock()
        preds2, metrics2, report2 = runner.run_live_gemini_evaluation(
            items=items,
            mock_client=mock_client,
            dry_run=False,
            schema_mode="SELECTIVE_PICKS",
        )
        self.assertEqual(len(preds2), 50)
        self.assertEqual(metrics2.batches_executed, 0)
        self.assertEqual(metrics2.batches_reused_from_cache, 7)
        self.assertEqual(mock_client.models.generate_content.call_count, 0)

    def test_phase3c2c_preflight_dry_run_sandbox_success(self):
        """run_phase3c_preflight_dry_run executes cleanly in an isolated sandbox with 0 live API calls."""
        from efso_radar.ai.calibration import run_phase3c_preflight_dry_run

        target_csv = DEFAULT_FILLED_CALIBRATION_CSV if DEFAULT_FILLED_CALIBRATION_CSV.exists() else self.csv_path
        result = run_phase3c_preflight_dry_run(csv_path=target_csv)

        self.assertEqual(result["status"], "PREFLIGHT_SUCCESS")
        self.assertEqual(result["total_items"], 50)
        self.assertEqual(result["batches_planned"], 7)
        self.assertEqual(result["enforceable_total_max_output_tokens"], 9740)
        self.assertLessEqual(result["enforceable_total_max_output_tokens"], 10000)
        self.assertTrue(result["within_budget_ceiling"])
        self.assertLess(result["worst_case_cost_usd"], 0.05)
        self.assertEqual(result["first_pass_batches_executed"], 7)
        self.assertEqual(result["first_pass_batches_cached"], 0)
        self.assertEqual(result["second_pass_batches_executed"], 0)
        self.assertEqual(result["second_pass_batches_cached"], 7)
        self.assertTrue(result["resume_verified"])
        self.assertEqual(result["evaluation_mode"], "OFFLINE_PREFLIGHT_DRY_RUN")
        self.assertFalse(result["is_measured_gemini_accuracy"])


if __name__ == "__main__":
    unittest.main()
