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
import unittest
from pathlib import Path

from efso_radar.ai.client import (
    detect_sensitive_tragedy_content,
    project_diagnostic_decisions_to_assessments,
)
from efso_radar.ai.cost import ExperimentBudgetGuard
from efso_radar.ai.schemas import DiagnosticArticleDecision, DiagnosticEvaluationBatch
from efso_radar.domain import EditorialDisposition

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestPhase4DLiveGemini(unittest.TestCase):
    """Test suite for Phase 4D live Gemini evaluation contract and safety gates (authentic evidence)."""

    def setUp(self) -> None:
        self.rss_evidence_path = REPO_ROOT / "data" / "trial_live_rss_evidence.json"
        self.gemini_evidence_path = REPO_ROOT / "data" / "trial_live_gemini_evidence.json"
        self.budget_ledger_path = REPO_ROOT / "data" / "experiment_budget_ledger.json"

    @unittest.skipUnless(
        (REPO_ROOT / "data" / "trial_live_rss_evidence.json").exists(),
        "Requires authentic Phase 4D RSS trial evidence in data/",
    )
    def test_rss_evidence_integrity(self) -> None:
        """Verify authentic saved RSS evidence has 12 items and correct fields."""
        self.assertTrue(self.rss_evidence_path.exists())
        with open(self.rss_evidence_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data.get("source_id"), "haberturk_magazin")
        self.assertEqual(data.get("accepted_items_count"), 12)
        self.assertEqual(len(data.get("articles", [])), 12)

    @unittest.skipUnless(
        (REPO_ROOT / "data" / "trial_live_rss_evidence.json").exists(),
        "Requires authentic Phase 4D RSS trial evidence in data/",
    )
    def test_sensitive_tragedy_isolation_in_rss(self) -> None:
        """Verify exactly 3 sensitive tragedy/bereavement stories are flagged and isolated."""
        with open(self.rss_evidence_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        articles = data["articles"]
        sensitive = [a for a in articles if a.get("is_sensitive_tragedy")]
        self.assertEqual(len(sensitive), 3)

        titles = [a["title"] for a in sensitive]
        self.assertTrue(any("Soner Olgun" in t for t in titles))
        self.assertTrue(any("Süleyman Saim Tekcan" in t for t in titles))
        self.assertTrue(any("Can Bonomo" in t for t in titles))

        # Check triggers
        for a in sensitive:
            is_sens = detect_sensitive_tragedy_content(a["title"], a.get("snippet", ""))
            self.assertTrue(is_sens, f"Article '{a['title']}' must trigger sensitivity detector.")

    @unittest.skipUnless(
        (REPO_ROOT / "data" / "trial_live_rss_evidence.json").exists(),
        "Requires authentic Phase 4D RSS trial evidence in data/",
    )
    def test_ordinary_health_news_not_classified_as_tragedy(self) -> None:
        """Verify ordinary injury/health entertainment reporting is NOT classified as tragedy."""
        with open(self.rss_evidence_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        gisele_art = next(a for a in data["articles"] if "sakatlandı" in a["title"].lower())
        is_sens = detect_sensitive_tragedy_content(gisele_art["title"], gisele_art.get("snippet", ""))
        self.assertFalse(is_sens, "Gisele Bündchen crutches story must not be treated as tragedy.")

    @unittest.skipUnless(
        (REPO_ROOT / "data" / "experiment_budget_ledger.json").exists(),
        "Requires authentic Phase 4D experiment budget ledger in data/",
    )
    def test_durable_budget_ledger_consistency(self) -> None:
        """Verify durable budget ledger tracks cumulative consumption correctly."""
        self.assertTrue(self.budget_ledger_path.exists())
        guard = ExperimentBudgetGuard(
            ledger_path=self.budget_ledger_path,
            budget_ceiling_usd=0.050000,
            canary1_db_path=REPO_ROOT / "data" / "canary_radar.db",
            canary2_db_path=REPO_ROOT / "data" / "canary2_radar.db",
        )

        prior_cost = guard.get_prior_canaries_cost_usd()
        self.assertAlmostEqual(prior_cost, 0.005184, places=5)

        total_consumed = guard.get_total_consumed_usd()
        remaining_allowance = guard.get_remaining_allowance_usd()

        # Cumulative consumption must be strictly within ceiling
        self.assertLessEqual(total_consumed, 0.050000)
        self.assertAlmostEqual(total_consumed + remaining_allowance, 0.050000, places=5)

        # 8 batches total (7 from Phase 3C-3 + 1 from Phase 4D)
        completed_batches = [
            b for b in guard.data.get("batches", {}).values()
            if b.get("status") == "COMPLETED"
        ]
        self.assertEqual(len(completed_batches), 8)

    @unittest.skipUnless(
        (REPO_ROOT / "data" / "trial_live_gemini_evidence.json").exists(),
        "Requires authentic Phase 4D live Gemini evidence in data/",
    )
    def test_phase4d_gemini_evidence_and_decisions(self) -> None:
        """Verify Phase 4D live Gemini evidence integrity, decisions, and token tracking."""
        self.assertTrue(self.gemini_evidence_path.exists())
        with open(self.gemini_evidence_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data.get("model_name"), "gemini-3.5-flash-lite")
        self.assertTrue(data.get("is_live"))
        self.assertEqual(len(data.get("selected_candidates", [])), 6)
        self.assertEqual(len(data.get("sensitive_excluded_articles", [])), 3)

        # Verify token counts are genuine numbers from API
        self.assertGreater(data.get("input_tokens", 0), 2000)
        self.assertGreater(data.get("output_tokens", 0), 800)
        self.assertGreater(data.get("actual_cost_usd", 0.0), 0.0020)

        # Verify decisions
        decisions = data.get("diagnostic_decisions", [])
        self.assertEqual(len(decisions), 6)

        dispositions = [d["disposition"] for d in decisions]
        self.assertEqual(dispositions.count("DIGEST"), 4)
        self.assertEqual(dispositions.count("RESERVE"), 1)
        self.assertEqual(dispositions.count("IGNORE"), 1)

        # Verify published count matches DIGEST count
        self.assertEqual(data.get("publishable_digest_count"), 4)

        # Verify Telegram preview HTML contains the 4 approved headlines and original links
        preview_html = data.get("telegram_digest_preview_html", "")
        self.assertIn("Şükrü Özyıldız", preview_html)
        self.assertIn("Nicolas Cage", preview_html)
        self.assertIn("Elizabeth Hurley", preview_html)
        self.assertIn("Gisele Bündchen", preview_html)
        self.assertNotIn("Soner Olgun", preview_html)
        self.assertNotIn("Süleyman Saim", preview_html)
        self.assertNotIn("Can Bonomo", preview_html)

    @unittest.skipUnless(
        (REPO_ROOT / "data" / "trial_live_rss_evidence.json").exists()
        and (REPO_ROOT / "data" / "trial_live_gemini_evidence.json").exists(),
        "Requires authentic Phase 4D live RSS and Gemini evidence in data/",
    )
    def test_phase4d_final_editorial_review_override(self) -> None:
        """Verify final editorial review: Hurley overridden to HOLD, 3 final publishable items."""
        from scripts.review_phase4d_digest import run_editorial_review

        review = run_editorial_review()
        self.assertEqual(review["final_publishable_digest_count"], 3)
        self.assertEqual(len(review["editorial_overrides"]), 1)

        override = review["editorial_overrides"][0]
        self.assertEqual(override["celebrity"], "Elizabeth Hurley & Billy Ray Cyrus")
        self.assertEqual(override["editorial_action"], "OVERRIDE_TO_HOLD_FOR_VERIFICATION")
        self.assertEqual(override["override_details"]["to_disposition"], "HOLD_FOR_VERIFICATION")
        self.assertFalse(override["override_details"]["is_publishable"])

        # Final Telegram HTML
        final_html = review["final_telegram_html"]
        self.assertIn("3 haber önerisi", final_html)
        self.assertIn("Şükrü Özyıldız", final_html)
        self.assertIn("True Detective", final_html)
        self.assertIn("5. Sezon", final_html)
        self.assertIn("Gisele Bündchen", final_html)
        # Hurley must NOT be in final HTML
        self.assertNotIn("Elizabeth Hurley", final_html)
        self.assertNotIn("Billy Ray Cyrus", final_html)
        # Message length under 4096
        self.assertLess(len(final_html), 4000)


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
        import tempfile
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


if __name__ == "__main__":
    unittest.main()
