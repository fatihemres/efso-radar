"""Unit and integration regression tests for Phase 4D: Supervised Live RSS-to-Gemini Evaluation.

Verifies:
1. Evidence loading and candidate selection contract (12 RSS articles, 6 candidates chosen).
2. Sensitive tragedy screening and isolation (Soner Olgun, Süleyman Saim Tekcan, Can Bonomo).
3. Non-tragedy distinction for ordinary health/injury news (Gisele Bündchen).
4. Durable cumulative budget guard consistency ($0.033846 consumed, $0.016154 remaining).
5. Live Gemini evidence integrity and defense-in-depth projection:
   - 4 DIGEST items, 1 RESERVE, 1 IGNORE.
   - Zero tragedy items in publishable digest.
   - Formatted Telegram digest preview safety.
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from efso_radar.ai.client import (
    detect_sensitive_tragedy_content,
    project_diagnostic_decisions_to_assessments,
)
from efso_radar.ai.cost import ExperimentBudgetGuard
from efso_radar.ai.schemas import DiagnosticArticleDecision

HISTORICAL_DATA_SKIP_REASON = (
    "Historical authentic Phase 4D evidence in operational data/ is intentionally "
    "excluded from hermetic offline tests (Issue #102 / T1A); see "
    "TestPhase4DPortableContract for labeled synthetic regression coverage."
)


class TestPhase4DLiveGemini(unittest.TestCase):
    """Historical Phase 4D authentic-evidence tests (unconditionally skipped without touching data/)."""

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_rss_evidence_integrity(self) -> None:
        """Verify authentic saved RSS evidence has 12 items and correct fields."""
        raise AssertionError("Skipped unconditionally under Issue #102 / T1A")

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_sensitive_tragedy_isolation_in_rss(self) -> None:
        """Verify exactly 3 sensitive tragedy/bereavement stories are flagged and isolated."""
        raise AssertionError("Skipped unconditionally under Issue #102 / T1A")

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_ordinary_health_news_not_classified_as_tragedy(self) -> None:
        """Verify ordinary injury/health entertainment reporting is NOT classified as tragedy."""
        raise AssertionError("Skipped unconditionally under Issue #102 / T1A")

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_durable_budget_ledger_consistency(self) -> None:
        """Verify durable budget ledger tracks cumulative consumption correctly."""
        raise AssertionError("Skipped unconditionally under Issue #102 / T1A")

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_phase4d_gemini_evidence_and_decisions(self) -> None:
        """Verify Phase 4D live Gemini evidence integrity, decisions, and token tracking."""
        raise AssertionError("Skipped unconditionally under Issue #102 / T1A")

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_phase4d_final_editorial_review_override(self) -> None:
        """Verify final editorial review: Hurley overridden to HOLD, 3 final publishable items."""
        raise AssertionError("Skipped unconditionally under Issue #102 / T1A")


class TestPhase4DPortableContract(unittest.TestCase):
    """Deterministic, portable unit and contract tests for Phase 4D logic on every clean checkout."""

    def test_sensitive_tragedy_detection_logic_portable(self) -> None:
        """Verify sensitive tragedy detector accurately separates tragedies from ordinary news."""
        # True positives: death, funeral, tragedy, bereavement
        self.assertTrue(
            detect_sensitive_tragedy_content(
                "Usta Sanatçı Soner Olgun'un Acı Günü: Kardeşini Kaybetti",
                "Soner Olgun'un kardeşi hayatını kaybetti, cenaze namazı ikindi vakti kılınacak.",
            )
        )
        self.assertTrue(
            detect_sensitive_tragedy_content(
                "Süleyman Saim Tekcan vefat etti: Sanat dünyasının acı kaybı",
                "Usta ressam ve heykeltıraş yaşamını yitirdi.",
            )
        )
        self.assertTrue(
            detect_sensitive_tragedy_content(
                "Can Bonomo'nun intihar eden annesiyle ilgili duygusal paylaşımı",
                "Ünlü şarkıcı annesinin trajik kaybı sonrasında duygusal ifadeler kullandı.",
            )
        )

        # True negatives: ordinary sports/health/injury reporting
        self.assertFalse(
            detect_sensitive_tragedy_content(
                "Gisele Bündchen sakatlandı: Koltuk değnekleriyle görüntülendi",
                "Ünlü süpermodel antrenmanda bacağını incitmesi sonucu ortopedik bot kullandı.",
            )
        )
        self.assertFalse(
            detect_sensitive_tragedy_content(
                "Ünlü oyuncu sette küçük bir kaza geçirdi",
                "Hafif şekilde yaralanan oyuncu kontrol amaçlı hastaneye kaldırıldı, durumu iyi.",
            )
        )

    def test_editorial_override_and_preview_projection_portable(self) -> None:
        """Verify override logic, triage segregation, and preview HTML formatting on synthetic data."""
        from datetime import datetime, timezone
        from efso_radar.domain import Article, Market
        from efso_radar.notifications.formatters import format_telegram_digest

        now = datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
        articles = [
            Article.create("Şükrü Özyıldız kardeşinin düğününde duygulandı", "https://example.com/1", "Habertürk", now),
            Article.create("True Detective 5. Sezon: Nicolas Cage ve Antonio Banderas buluşuyor", "https://example.com/2", "Habertürk", now),
            Article.create("Gisele Bündchen sakatlandı: Koltuk değnekleriyle görüntülendi", "https://example.com/3", "Habertürk", now),
            Article.create("Elizabeth Hurley ve Billy Ray Cyrus aşk mı yaşıyor?", "https://example.com/4", "Habertürk", now),
        ]

        decisions = [
            DiagnosticArticleDecision(
                article_id=articles[0].id, score=85, disposition="DIGEST", kind="NEW", urgency="NORMAL",
                rationale_tr="Şükrü Özyıldız haberi.", headline_tr=articles[0].title, verification_status="FIRST_PARTY_STATEMENT",
            ),
            DiagnosticArticleDecision(
                article_id=articles[1].id, score=88, disposition="DIGEST", kind="NEW", urgency="NORMAL",
                rationale_tr="True Detective haberi.", headline_tr=articles[1].title, verification_status="REPUTABLE_REPORTING",
            ),
            DiagnosticArticleDecision(
                article_id=articles[2].id, score=80, disposition="DIGEST", kind="NEW", urgency="NORMAL",
                rationale_tr="Gisele Bündchen haberi.", headline_tr=articles[2].title, verification_status="REPUTABLE_REPORTING",
            ),
            # Overridden to HOLD
            DiagnosticArticleDecision(
                article_id=articles[3].id, score=82, disposition="HOLD", kind="NEW", urgency="NORMAL",
                rationale_tr="Teyitsiz aşk iddiası.", headline_tr=articles[3].title, verification_status="ATTRIBUTED_CLAIM",
            ),
        ]

        assessments = project_diagnostic_decisions_to_assessments(
            decisions=decisions,
            candidates=articles,
            include_manual_holds=True,
        )

        publishable = [a for a in assessments if a.is_publishable]
        holds = [a for a in assessments if not a.is_publishable]

        self.assertEqual(len(publishable), 3)
        self.assertEqual(len(holds), 1)
        self.assertEqual(holds[0].article_ids[0], articles[3].id)

        html = format_telegram_digest(publishable, window_label="Editoryal Seçki")
        self.assertIn("3 haber önerisi", html)
        self.assertIn("Şükrü Özyıldız", html)
        self.assertIn("True Detective", html)
        self.assertIn("Gisele Bündchen", html)
        self.assertNotIn("Elizabeth Hurley", html)
        self.assertLess(len(html), 4000)

    def test_durable_budget_guard_portable(self) -> None:
        """Verify ExperimentBudgetGuard tracks reservations, completions, and ceilings portably."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            ledger_file = Path(tmp_dir) / "test_budget_ledger.json"
            guard = ExperimentBudgetGuard(
                ledger_path=ledger_file,
                budget_ceiling_usd=0.010000,
                canary1_db_path=Path(tmp_dir) / "empty1.db",
                canary2_db_path=Path(tmp_dir) / "empty2.db",
            )
            self.assertEqual(guard.get_total_consumed_usd(), 0.0)
            self.assertEqual(guard.get_remaining_allowance_usd(), 0.010000)

            # Reserve a batch
            guard.reserve_batch(batch_hash="hash_01", candidate_ids=["cand_1"], estimated_worst_cost_usd=0.002000)
            self.assertEqual(guard.get_total_consumed_usd(), 0.002000)
            # Reconcile batch
            guard.reconcile_batch(
                batch_hash="hash_01",
                actual_cost_usd=0.001500,
                input_tokens=1000,
                output_tokens=300,
                model_name="gemini-3.5-flash-lite",
            )
            self.assertAlmostEqual(guard.get_total_consumed_usd(), 0.001500, places=6)
            self.assertAlmostEqual(guard.get_remaining_allowance_usd(), 0.008500, places=6)

    def test_editorial_review_script_isolated_synthetic_fixture(self) -> None:
        """Verify run_editorial_review() in strict temp isolation using explicitly labeled synthetic fixtures."""
        import scripts.review_phase4d_digest as review_mod

        orig_delivery_env = os.environ.get("TELEGRAM_DELIVERY_ENABLED")

        with tempfile.TemporaryDirectory(prefix="efso_t1a_phase4d_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            synthetic_rss_path = tmp_path / "synthetic_rss_evidence.json"
            synthetic_gemini_path = tmp_path / "synthetic_gemini_evidence.json"
            synthetic_review_out = tmp_path / "synthetic_review_evidence.json"
            synthetic_budget_path = tmp_path / "synthetic_budget_ledger.json"

            synthetic_rss_payload = {
                "synthetic_fixture": True,
                "articles": [
                    {
                        "article_id": "c12aebfb09cea9753d28a1cb",
                        "title": "[SYNTHETIC] Şükrü Özyıldız kardeşinin düğününden kareler paylaştı",
                        "url": "https://synthetic.example.invalid/news/sukru",
                        "publisher": "Synthetic Test Source",
                        "published_at_utc": "2026-10-18T12:00:00+00:00",
                        "snippet": "Sentetik test haberi: aile düğünü paylaşımı.",
                    },
                    {
                        "article_id": "bc038ac2e6c763dfd15c913f",
                        "title": "[SYNTHETIC] True Detective 5. Sezon için Nicolas Cage ve Antonio Banderas buluşuyor",
                        "url": "https://synthetic.example.invalid/news/true-detective-s5",
                        "publisher": "Synthetic Test Source",
                        "published_at_utc": "2026-10-18T12:05:00+00:00",
                        "snippet": "Sentetik test haberi: oyuncu kadrosu duyurusu.",
                    },
                    {
                        "article_id": "e96f2f49484d35370b64cec5",
                        "title": "[SYNTHETIC] Elizabeth Hurley ve Billy Ray Cyrus hakkında nişan iddiası",
                        "url": "https://synthetic.example.invalid/news/hurley-rumor",
                        "publisher": "Synthetic Test Source",
                        "published_at_utc": "2026-10-18T12:10:00+00:00",
                        "snippet": "Sentetik test haberi: yabancı basında doğrulama bekleyen söylenti.",
                    },
                    {
                        "article_id": "8262e6a3c4f2f3abf2534c98",
                        "title": "[SYNTHETIC] Gisele Bündchen koltuk değnekleriyle görüntülendi",
                        "url": "https://synthetic.example.invalid/news/gisele",
                        "publisher": "Synthetic Test Source",
                        "published_at_utc": "2026-10-18T12:15:00+00:00",
                        "snippet": "Sentetik test haberi: ortopedik bot ile görüntülendi.",
                    },
                ],
            }
            synthetic_gemini_payload = {
                "synthetic_fixture": True,
                "diagnostic_decisions": [
                    {
                        "article_id": "c12aebfb09cea9753d28a1cb",
                        "disposition": "DIGEST",
                        "score": 85,
                        "headline_tr": "Synthetic Şükrü Özyıldız Başlığı",
                        "verification_status": "FIRST_PARTY_STATEMENT",
                        "rationale_tr": "Sentetik özet: kardeşinin düğün karelerini paylaştı.",
                    },
                    {
                        "article_id": "bc038ac2e6c763dfd15c913f",
                        "disposition": "DIGEST",
                        "score": 88,
                        "headline_tr": "Synthetic True Detective Başlığı",
                        "verification_status": "REPUTABLE_REPORTING",
                        "rationale_tr": "Sentetik özet: 5. sezon kadrosu duyuruldu.",
                    },
                    {
                        "article_id": "e96f2f49484d35370b64cec5",
                        "disposition": "DIGEST",
                        "score": 82,
                        "headline_tr": "Synthetic Hurley İddiası",
                        "verification_status": "FIRST_PARTY_STATEMENT",
                        "rationale_tr": "Sentetik özet: teyitsiz nişan söylentisi.",
                    },
                    {
                        "article_id": "8262e6a3c4f2f3abf2534c98",
                        "disposition": "DIGEST",
                        "score": 80,
                        "headline_tr": "Synthetic Gisele Bündchen Başlığı",
                        "verification_status": "REPUTABLE_REPORTING",
                        "rationale_tr": "Sentetik özet: koltuk değnekleriyle görüntülendi.",
                    },
                ],
            }
            synthetic_budget_payload = {
                "synthetic_fixture": True,
                "experiment_id": "synthetic_t1a_phase4d_fixture",
                "budget_ceiling_usd": 0.05,
                "prior_canaries": [
                    {"canary_id": "synthetic_c1", "cost_usd": 0.001027},
                    {"canary_id": "synthetic_c2", "cost_usd": 0.004157},
                ],
                "batches": {
                    "synthetic_batch_01": {
                        "status": "COMPLETED",
                        "actual_cost_usd": 0.028662,
                    }
                },
            }

            synthetic_rss_path.write_text(json.dumps(synthetic_rss_payload, ensure_ascii=False), encoding="utf-8")
            synthetic_gemini_path.write_text(json.dumps(synthetic_gemini_payload, ensure_ascii=False), encoding="utf-8")
            synthetic_budget_path.write_text(json.dumps(synthetic_budget_payload, ensure_ascii=False), encoding="utf-8")

            def _synthetic_budget_guard(*, ledger_path: Path) -> ExperimentBudgetGuard:
                return ExperimentBudgetGuard(
                    ledger_path=ledger_path,
                    budget_ceiling_usd=0.050000,
                    canary1_db_path=tmp_path / "synthetic_canary1.db",
                    canary2_db_path=tmp_path / "synthetic_canary2.db",
                )

            with (
                patch.object(review_mod, "RSS_EVIDENCE_PATH", synthetic_rss_path),
                patch.object(review_mod, "GEMINI_EVIDENCE_PATH", synthetic_gemini_path),
                patch.object(review_mod, "REVIEW_EVIDENCE_PATH", synthetic_review_out),
                patch.object(review_mod, "BUDGET_LEDGER_PATH", synthetic_budget_path),
                patch.object(review_mod, "ExperimentBudgetGuard", side_effect=_synthetic_budget_guard),
                patch.dict(os.environ, {"TELEGRAM_DELIVERY_ENABLED": "sentinel_before_review"}, clear=False),
            ):
                review = review_mod.run_editorial_review()
                self.assertEqual(os.environ.get("TELEGRAM_DELIVERY_ENABLED"), "false")

            # Process environment is cleanly restored upon leaving test context
            self.assertEqual(os.environ.get("TELEGRAM_DELIVERY_ENABLED"), orig_delivery_env)

            # Assertions run against isolated synthetic output only
            self.assertTrue(synthetic_review_out.exists())
            self.assertEqual(review["final_publishable_digest_count"], 3)
            self.assertEqual(len(review["editorial_overrides"]), 1)

            override = review["editorial_overrides"][0]
            self.assertEqual(override["celebrity"], "Elizabeth Hurley & Billy Ray Cyrus")
            self.assertEqual(override["editorial_action"], "OVERRIDE_TO_HOLD_FOR_VERIFICATION")
            self.assertEqual(override["override_details"]["to_disposition"], "HOLD_FOR_VERIFICATION")
            self.assertFalse(override["override_details"]["is_publishable"])

            final_html = review["final_telegram_html"]
            self.assertIn("3 haber önerisi", final_html)
            self.assertIn("Şükrü Özyıldız", final_html)
            self.assertIn("True Detective", final_html)
            self.assertIn("5. Sezon", final_html)
            self.assertIn("Gisele Bündchen", final_html)
            self.assertNotIn("Elizabeth Hurley", final_html)
            self.assertNotIn("Billy Ray Cyrus", final_html)
            self.assertLess(len(final_html), 4000)

            # Exception-path negative control also restores TELEGRAM_DELIVERY_ENABLED
            with (
                patch.object(review_mod, "RSS_EVIDENCE_PATH", tmp_path / "missing_rss_evidence.json"),
                patch.object(review_mod, "GEMINI_EVIDENCE_PATH", synthetic_gemini_path),
                patch.object(review_mod, "REVIEW_EVIDENCE_PATH", synthetic_review_out),
                patch.object(review_mod, "BUDGET_LEDGER_PATH", synthetic_budget_path),
                patch.object(review_mod, "ExperimentBudgetGuard", side_effect=_synthetic_budget_guard),
                patch.dict(os.environ, {"TELEGRAM_DELIVERY_ENABLED": "sentinel_exception_path"}, clear=False),
            ):
                with self.assertRaises(FileNotFoundError):
                    review_mod.run_editorial_review()

            self.assertEqual(os.environ.get("TELEGRAM_DELIVERY_ENABLED"), orig_delivery_env)

    def test_historical_authentic_evidence_suite_skips_unconditionally_without_data_access(self) -> None:
        """Verify all historical authentic-evidence tests skip unconditionally without checking data/."""
        expected_skipped = (
            "test_rss_evidence_integrity",
            "test_sensitive_tragedy_isolation_in_rss",
            "test_ordinary_health_news_not_classified_as_tragedy",
            "test_durable_budget_ledger_consistency",
            "test_phase4d_gemini_evidence_and_decisions",
            "test_phase4d_final_editorial_review_override",
        )
        for method_name in expected_skipped:
            method = getattr(TestPhase4DLiveGemini, method_name)
            self.assertTrue(
                getattr(method, "__unittest_skip__", False),
                f"{method_name} must remain unconditionally skipped without reading data/",
            )
            self.assertEqual(getattr(method, "__unittest_skip_why__", ""), HISTORICAL_DATA_SKIP_REASON)


if __name__ == "__main__":
    unittest.main()
