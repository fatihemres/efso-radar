"""Unit and integration tests for Phase 4B: Supervised Editorial Digest Preview.

Verifies:
1. Strict deduplication of editorial assessments (preferring higher-scoring primary story).
2. Clean integration with format_telegram_digest and [DEMO / KALİBRASYON] labeling.
3. Triage safety invariants:
   - RESERVE stays outside the main digest.
   - IGNORE is excluded.
   - HOLD_FOR_VERIFICATION stays unpublished.
   - SENSITIVE_REVIEW stays strictly outside normal DIGEST and BREAKING queues.
   - Zero synthetic/hallucinated data.
"""
from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from efso_radar.ai.calibration import REQUIRED_CSV_COLUMNS
from efso_radar.ai.client import (
    detect_sensitive_tragedy_content,
)
from efso_radar.ai.cost import ExperimentBudgetGuard
from efso_radar.ai.schemas import DiagnosticArticleDecision
from efso_radar.domain import (
    Article,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    Market,
    StoryCategory,
    SuggestedFormat,
    VerificationStatus,
)
from efso_radar.notifications.formatters import (
    deduplicate_assessments,
    format_telegram_digest,
)
from scripts.preview_editorial_digest import (
    build_editorial_digest_preview,
    load_genuine_project_decisions,
)

HISTORICAL_DATA_SKIP_REASON = (
    "Historical authentic Phase 4B calibration/eval/ledger files in operational data/ are "
    "intentionally excluded from hermetic offline tests (Issue #104 / T1B); see labeled "
    "synthetic counterparts in TestPhase4BDigestPreview."
)


class TestPhase4BDigestPreview(unittest.TestCase):
    """Test suite for Phase 4B Editorial Digest Preview."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmpdir.name)
        self.now = datetime(2026, 10, 18, 19, 45, tzinfo=timezone.utc)
        self.art_cagatay_1 = Article(
            id="cagatay_haberturk",
            source="Habertürk Magazin",
            market=Market.TR,
            title="Çağatay Ulusoy köklerine döndü: Ailesiyle memleketini ziyaret etti!",
            url="https://www.haberturk.com/magazin/cagatay-1",
            published_at=self.now,
            discovered_at=self.now,
            snippet="Oyuncu Çağatay Ulusoy, Bulgaristan'daki akrabalarını ve köyünü ziyaret etti.",
        )
        self.art_cagatay_2 = Article(
            id="cagatay_sabah",
            source="Sabah",
            market=Market.TR,
            title="Çağatay Ulusoy baba ocağında! Memleketindeki samimi anları kameralara yansıdı",
            url="https://www.sabah.com.tr/magazin/cagatay-2",
            published_at=self.now,
            discovered_at=self.now,
            snippet="Bulgaristan göçmeni olan Çağatay Ulusoy, doğup büyüdüğü ata topraklarını ziyaret etti.",
        )
        self.art_eda = Article(
            id="eda_haberturk",
            source="Habertürk Magazin",
            market=Market.TR,
            title="Eda Ece'den net kural: Anne olduktan sonra o sahneleri çekmiyor!",
            url="https://www.haberturk.com/magazin/eda-ece",
            published_at=self.now,
            discovered_at=self.now,
            snippet="Eda Ece anne olduktan sonra dizi setlerindeki kırmızı çizgilerini anlattı.",
        )
        self.art_sila = Article(
            id="sila_onedio",
            source="Onedio Magazin",
            market=Market.TR,
            title="Sıla'nın 101 yaşındaki anneannesi zarafetiyle sosyal medyanın gündeminde!",
            url="https://onedio.com/magazin/sila",
            published_at=self.now,
            discovered_at=self.now,
            snippet="Şarkıcı Sıla 101 yaşındaki anneannesi ile fotoğraf paylaştı.",
        )

        self.assess_cagatay_low = EditorialAssessment(
            article_ids=[self.art_cagatay_1.id],
            score=75,
            kind=EditorialKind.NEW,
            headline_tr=self.art_cagatay_1.title,
            cover_title_tr="Çağatay Ulusoy köyünde",
            caption_idea_tr="Çağatay Ulusoy memleketinde",
            why_efso_tr="Çağatay Ulusoy'un memleket ziyareti",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[self.art_cagatay_1.url],
            supporting_articles=[self.art_cagatay_1],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        self.assess_cagatay_high = EditorialAssessment(
            article_ids=[self.art_cagatay_2.id],
            score=78,
            kind=EditorialKind.NEW,
            headline_tr=self.art_cagatay_2.title,
            cover_title_tr="Çağatay Ulusoy baba ocağında",
            caption_idea_tr="Çağatay Ulusoy hemşehrileriyle buluştu",
            why_efso_tr="Çağatay Ulusoy'un samimi köy ziyareti görüntüleri",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[self.art_cagatay_2.url],
            supporting_articles=[self.art_cagatay_2],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        self.assess_eda = EditorialAssessment(
            article_ids=[self.art_eda.id],
            score=85,
            kind=EditorialKind.NEW,
            headline_tr=self.art_eda.title,
            cover_title_tr="Eda Ece'den kırmızı çizgi",
            caption_idea_tr="Eda Ece rol kuralını açıkladı",
            why_efso_tr="Eda Ece'nin set açıklamaları",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.FIRST_PARTY_STATEMENT,
            source_urls=[self.art_eda.url],
            supporting_articles=[self.art_eda],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        self.assess_sila = EditorialAssessment(
            article_ids=[self.art_sila.id],
            score=82,
            kind=EditorialKind.NEW,
            headline_tr=self.art_sila.title,
            cover_title_tr="Sıla'nın anneannesi",
            caption_idea_tr="Sıla'nın 101 yaşındaki anneannesi",
            why_efso_tr="Sıla ve anneannesi paylaşımı",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.FIRST_PARTY_STATEMENT,
            source_urls=[self.art_sila.url],
            supporting_articles=[self.art_sila],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_deduplicate_assessments_drops_lower_scoring_duplicate(self) -> None:
        """Deduplication drops the lower-scoring duplicate and keeps the higher-scoring primary story."""
        input_items = [self.assess_cagatay_low, self.assess_cagatay_high, self.assess_eda]
        selected, dropped = deduplicate_assessments(input_items)

        self.assertEqual(len(selected), 2)
        self.assertEqual(len(dropped), 1)

        # Higher scoring Çağatay (78) is kept
        kept_headlines = [a.headline_tr for a in selected]
        self.assertIn(self.assess_cagatay_high.headline_tr, kept_headlines)
        self.assertNotIn(self.assess_cagatay_low.headline_tr, kept_headlines)
        self.assertIn(self.assess_eda.headline_tr, kept_headlines)

        # Dropped reason contains reference to duplicate
        dropped_item, drop_reason = dropped[0]
        self.assertEqual(dropped_item.headline_tr, self.assess_cagatay_low.headline_tr)
        self.assertIn("Mükerrer haber birleştirildi", drop_reason)
        self.assertIn("cagatay ulusoy", drop_reason.lower())

    def test_deduplicate_assessments_preserves_distinct_stories(self) -> None:
        """Distinct stories with different celebrities are all preserved."""
        input_items = [self.assess_eda, self.assess_sila, self.assess_cagatay_high]
        selected, dropped = deduplicate_assessments(input_items)

        self.assertEqual(len(selected), 3)
        self.assertEqual(len(dropped), 0)

    def test_format_telegram_digest_applies_deduplication_and_demo_tag(self) -> None:
        """format_telegram_digest automatically filters duplicates and renders clean HTML."""
        input_items = [self.assess_cagatay_low, self.assess_cagatay_high, self.assess_eda]
        rendered = format_telegram_digest(
            input_items,
            window_label="[DEMO / KALİBRASYON] 6 Saatlik Editoryal Seçki",
            deduplicate=True,
        )

        # Must indicate 2 stories (because 1 was dropped as duplicate)
        self.assertIn("2 haber önerisi", rendered)
        self.assertIn("[DEMO / KALİBRASYON]", rendered)
        self.assertIn("01 · Eda Ece&#x27;den net kural", rendered)
        self.assertIn("02 · Çağatay Ulusoy baba ocağında", rendered)
        self.assertNotIn("Çağatay Ulusoy köklerine döndü", rendered)
        self.assertIn("Instagram'da otomatik paylaşım yapılmaz.", rendered)

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_build_editorial_digest_preview_with_genuine_project_data(self) -> None:
        """Integration test: build_editorial_digest_preview correctly loads genuine saved records."""
        raise AssertionError("Skipped unconditionally under Issue #104 / T1B")

    def test_deduplicate_merges_sources_for_true_duplicates(self) -> None:
        """True duplicates merge their supporting articles and source URLs into the primary representative."""
        selected, dropped = deduplicate_assessments([self.assess_cagatay_low, self.assess_cagatay_high])

        self.assertEqual(len(selected), 1)
        self.assertEqual(len(dropped), 1)

        # Primary kept item has consolidated sources from both Habertürk and Sabah
        kept = selected[0]
        self.assertEqual(kept.headline_tr, self.assess_cagatay_high.headline_tr)
        sources = [a.source for a in kept.supporting_articles]
        self.assertIn("Sabah", sources)
        self.assertIn("Habertürk Magazin", sources)

        # Verify drop message mentions consolidation
        _, reason = dropped[0]
        self.assertIn("Mükerrer haber birleştirildi", reason)
        self.assertIn("Kaynaklar ve makale ID'leri birleştirildi", reason)

    def test_deduplicate_preserves_legitimate_update(self) -> None:
        """Legitimate UPDATE with meaningful new development is NOT dropped as duplicate."""
        art_update = Article(
            id="cagatay_update",
            source="NTV Yaşam & Sanat",
            market=Market.TR,
            title="Çağatay Ulusoy'dan belgesel müjdesi: Köy ziyareti sonrası yeni proje",
            url="https://ntv.com.tr/cagatay-doc",
            published_at=self.now,
            discovered_at=self.now,
            snippet="Çağatay Ulusoy Bulgaristan ziyareti sonrası yeni bir belgesel projesi çekeceğini açıkladı.",
        )
        assess_update = EditorialAssessment(
            article_ids=[art_update.id],
            score=76,  # Lower score than initial 78, but is legitimate UPDATE
            kind=EditorialKind.UPDATE,
            development_tr="Ziyaret sonrasında memleketini anlatan kapsamlı bir belgesel projesine başladığını resmen duyurdu.",
            headline_tr=art_update.title,
            cover_title_tr="Çağatay Ulusoy'dan belgesel",
            caption_idea_tr="Çağatay Ulusoy yeni belgeselini açıkladı",
            why_efso_tr="Önemli yeni proje gelişmesi",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.FIRST_PARTY_STATEMENT,
            source_urls=[art_update.url],
            supporting_articles=[art_update],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        selected, dropped = deduplicate_assessments([
            self.assess_cagatay_low,   # Score 75 (NEW duplicate)
            self.assess_cagatay_high,  # Score 78 (NEW primary)
            assess_update,             # Score 76 (Legitimate UPDATE with new facts)
        ])

        # Both the primary NEW story (78) AND the legitimate UPDATE (76) must be selected!
        self.assertEqual(len(selected), 2)
        selected_kinds = [a.kind for a in selected]
        self.assertIn(EditorialKind.NEW, selected_kinds)
        self.assertIn(EditorialKind.UPDATE, selected_kinds)

        # Only the non-update duplicate (75) is dropped
        self.assertEqual(len(dropped), 1)
        self.assertEqual(dropped[0][0].headline_tr, self.assess_cagatay_low.headline_tr)

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_budget_ledger_reporting_matches_ledger_file(self) -> None:
        """Budget reporting in preview dynamically reads actual ledger evidence."""
        raise AssertionError("Skipped unconditionally under Issue #104 / T1B")

    def test_url_checks_separate_structural_and_remote(self) -> None:
        """URL validation reports separate structural verification from remote network verification."""
        bad_url_art = Article(
            id="bad_url_art",
            source="Sentetik Kaynak",
            market=Market.TR,
            title="Sentetik Geçersiz URL Haberi",
            url="ftp://invalid-no-dot/story",
            published_at=self.now,
            discovered_at=self.now,
            snippet="Sentetik test haberi.",
        )
        syn_decisions = [
            DiagnosticArticleDecision(
                article_id=self.art_eda.id,
                score=85,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Eda Ece açıklaması",
                headline_tr=self.art_eda.title,
                verification_status="FIRST_PARTY_STATEMENT",
            ),
            DiagnosticArticleDecision(
                article_id=self.art_sila.id,
                score=82,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Sıla paylaşımı",
                headline_tr=self.art_sila.title,
                verification_status="FIRST_PARTY_STATEMENT",
            ),
            DiagnosticArticleDecision(
                article_id=bad_url_art.id,
                score=80,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Geçersiz URL sentetik kontrolü",
                headline_tr=bad_url_art.title,
                verification_status="REPUTABLE_REPORTING",
            ),
        ]
        preview = build_editorial_digest_preview(
            limit=3,
            repo_root=self.tmp_path,
            decisions=syn_decisions,
            articles=[self.art_eda, self.art_sila, bad_url_art],
        )

        url_checks = preview.get("url_checks", [])
        self.assertEqual(len(url_checks), 3)
        by_id = {c["article_id"]: c for c in url_checks}
        self.assertTrue(by_id[self.art_eda.id]["structural_valid"])
        self.assertTrue(by_id[self.art_sila.id]["structural_valid"])
        self.assertFalse(by_id[bad_url_art.id]["structural_valid"])
        for check in url_checks:
            self.assertFalse(check["remote_checked"])

    def test_build_editorial_digest_preview_hermetic(self) -> None:
        """Hermetic portable test: build_editorial_digest_preview with synthetic decisions."""
        syn_decisions = [
            DiagnosticArticleDecision(
                article_id=self.art_cagatay_1.id,
                score=75,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Çağatay köy ziyareti 1",
                headline_tr=self.art_cagatay_1.title,
                verification_status="REPUTABLE_REPORTING",
            ),
            DiagnosticArticleDecision(
                article_id=self.art_cagatay_2.id,
                score=78,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Çağatay köy ziyareti 2",
                headline_tr=self.art_cagatay_2.title,
                verification_status="REPUTABLE_REPORTING",
            ),
            DiagnosticArticleDecision(
                article_id=self.art_eda.id,
                score=85,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Eda Ece set kuralları",
                headline_tr=self.art_eda.title,
                verification_status="FIRST_PARTY_STATEMENT",
            ),
        ]
        articles = [self.art_cagatay_1, self.art_cagatay_2, self.art_eda]
        preview = build_editorial_digest_preview(
            limit=2,
            preview_timestamp=self.now,
            repo_root=self.tmp_path,
            decisions=syn_decisions,
            articles=articles,
        )

        self.assertEqual(len(preview["selected_top"]), 2)
        self.assertEqual(preview["selected_top"][0].score, 85)
        self.assertEqual(preview["selected_top"][1].score, 78)
        self.assertEqual(len(preview["duplicate_drops"]), 1)
        self.assertIn("EDİTORYAL BÜLTEN", preview["telegram_html"])
        self.assertIn("2 haber önerisi", preview["telegram_html"])

    def test_build_editorial_digest_preview_loader_and_triage_synthetic(self) -> None:
        """Synthetic regression: loader + preview pipeline isolates sensitive/hold/reserve/duplicate items in temp dir."""
        syn_data_dir = self.tmp_path / "data"
        syn_data_dir.mkdir(parents=True, exist_ok=True)
        syn_csv = syn_data_dir / "Efso_Radar_Kalibrasyon_Doldurulmus.csv"

        rows_spec = {
            1: ("Çağatay Ulusoy köyünü ziyaret etti", "Habertürk Magazin", "https://synthetic.example.invalid/1", "Bulgaristan'daki köyünü ziyaret etti."),
            2: ("Çağatay Ulusoy baba ocağında görüntülendi", "Sabah", "https://synthetic.example.invalid/2", "Bulgaristan'daki köyünde ailesiyle buluştu."),
            3: ("Sentetik Sanatçı Vefat Etti: Cenaze Töreni Yarın", "Habertürk Magazin", "https://synthetic.example.invalid/3", "Ünlü sanatçının vefatı sonrası cenaze programı açıklandı."),
            4: ("Sentetik Oyuncu Yeni Dizi Anlaşması İddiası", "Variety", "https://synthetic.example.invalid/4", "Kulis iddialarına göre yeni dizi anlaşması gündemde."),
            5: ("Eda Ece Yeni Sezon Kurallarını Açıkladı", "Habertürk Magazin", "https://synthetic.example.invalid/5", "Ünlü oyuncu yeni sezon proje tercihlerini anlattı."),
            6: ("Sıla Konser Turnesi Tarihlerini Paylaştı", "Onedio Magazin", "https://synthetic.example.invalid/6", "Şarkıcı Sıla yaz konser takvimini duyurdu."),
            7: ("Sentetik Reserve Magazin Gelişmesi", "Posta", "https://synthetic.example.invalid/7", "Yedek tampon listede yer alan sentetik magazin haberi."),
        }

        with open(syn_csv, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(REQUIRED_CSV_COLUMNS))
            writer.writeheader()
            for i in range(1, 51):
                headline, pub, url, summary = rows_spec.get(
                    i,
                    (
                        f"Sentetik Magazin Başlığı {i}",
                        "Habertürk Magazin" if i % 2 == 0 else "Variety",
                        f"https://synthetic.example.invalid/item-{i}",
                        f"Sentetik haber özeti {i}.",
                    ),
                )
                writer.writerow(
                    {
                        "review_id": f"CALIB-{i:02d}",
                        "headline": headline,
                        "publisher": pub,
                        "source_url": url,
                        "published_at": "2026-10-18 12:00:00+00:00",
                        "discovered_at": "2026-10-18 15:00:00+00:00",
                        "category": "MAGAZIN_TR",
                        "factual_summary": summary,
                        "supporting_sources": pub,
                        "verification_uncertainty_notes": "Sentetik test fikstürü.",
                        "discovery_priority": str(round(12.0 - i * 0.1, 2)),
                        "enters_ai_cutoff_yn": "Y" if i <= 32 else "N",
                        "owner_decision": "APPROVE",
                        "owner_notes": "",
                    }
                )

        # Load synthetic articles via loader helper to obtain matching article IDs
        _, loaded_articles = load_genuine_project_decisions(repo_root=self.tmp_path)
        self.assertEqual(len(loaded_articles), 50)
        by_idx = {idx + 1: art for idx, art in enumerate(loaded_articles)}

        syn_eval_path = syn_data_dir / "canary2_gemini_eval_report.json"
        syn_decisions = [
            {
                "article_id": by_idx[5].id,
                "score": 88,
                "disposition": "DIGEST",
                "kind": "NEW",
                "urgency": "NORMAL",
                "rationale_tr": "Sentetik yüksek öncelikli haber.",
                "headline_tr": by_idx[5].title,
                "verification_status": "FIRST_PARTY_STATEMENT",
            },
            {
                "article_id": by_idx[6].id,
                "score": 84,
                "disposition": "DIGEST",
                "kind": "NEW",
                "urgency": "NORMAL",
                "rationale_tr": "Sentetik ikinci DIGEST haberi.",
                "headline_tr": by_idx[6].title,
                "verification_status": "REPUTABLE_REPORTING",
            },
            {
                "article_id": by_idx[1].id,
                "score": 75,
                "disposition": "DIGEST",
                "kind": "NEW",
                "urgency": "NORMAL",
                "rationale_tr": "Mükerrer düşük skorlu aday.",
                "headline_tr": by_idx[1].title,
                "verification_status": "REPUTABLE_REPORTING",
            },
            {
                "article_id": by_idx[2].id,
                "score": 79,
                "disposition": "DIGEST",
                "kind": "NEW",
                "urgency": "NORMAL",
                "rationale_tr": "Mükerrer yüksek skorlu aday.",
                "headline_tr": by_idx[2].title,
                "verification_status": "REPUTABLE_REPORTING",
            },
            {
                "article_id": by_idx[3].id,
                "score": 90,
                "disposition": "DIGEST",  # Deliberate mislabel to test independent sensitivity veto
                "kind": "NEW",
                "urgency": "BREAKING",
                "rationale_tr": "Hassas vefat içeriği testi.",
                "headline_tr": by_idx[3].title,
                "verification_status": "OFFICIAL_CONFIRMATION",
            },
            {
                "article_id": by_idx[4].id,
                "score": 80,
                "disposition": "DIGEST",
                "kind": "NEW",
                "urgency": "NORMAL",
                "rationale_tr": "Teyitsiz iddia testi.",
                "headline_tr": by_idx[4].title,
                "verification_status": "UNVERIFIED_RUMOR",
            },
            {
                "article_id": by_idx[7].id,
                "score": 68,
                "disposition": "RESERVE",
                "kind": "NEW",
                "urgency": "NORMAL",
                "rationale_tr": "Yedek tampon haber.",
                "headline_tr": by_idx[7].title,
                "verification_status": "REPUTABLE_REPORTING",
            },
        ]
        syn_eval_path.write_text(
            json.dumps({"evidence": {"raw_structured_model_predictions": [{"decisions": syn_decisions}]}}),
            encoding="utf-8",
        )

        preview = build_editorial_digest_preview(
            limit=2,
            preview_timestamp=self.now,
            repo_root=self.tmp_path,
        )

        self.assertEqual(preview["total_decisions_evaluated"], 7)
        self.assertEqual(len(preview["selected_top"]), 2)
        self.assertEqual([a.score for a in preview["selected_top"]], [88, 84])
        self.assertEqual(len(preview["reserve_backlog"]), 1)
        self.assertEqual(len(preview["duplicate_drops"]), 1)
        self.assertEqual(len(preview["sensitive_intercepted"]), 1)
        self.assertEqual(len(preview["hold_unverified"]), 1)
        self.assertEqual(preview["reserve_decisions_count"], 1)

        for selected in preview["selected_top"]:
            self.assertFalse(detect_sensitive_tragedy_content(selected.headline_tr, selected.why_efso_tr))
        self.assertIn("EDİTORYAL BÜLTEN", preview["telegram_html"])
        self.assertIn("[DEMO / KALİBRASYON]", preview["telegram_html"])
        self.assertIn("2 haber önerisi", preview["telegram_html"])
        self.assertIn("Instagram'da otomatik paylaşım yapılmaz.", preview["telegram_html"])

    def test_budget_ledger_reporting_synthetic_temp_ledger(self) -> None:
        """Synthetic regression: build_editorial_digest_preview reads temp synthetic ledger without touching data/."""
        syn_data_dir = self.tmp_path / "data"
        syn_data_dir.mkdir(parents=True, exist_ok=True)
        syn_ledger = syn_data_dir / "experiment_budget_ledger.json"

        guard = ExperimentBudgetGuard(
            ledger_path=syn_ledger,
            budget_ceiling_usd=0.05,
            canary1_db_path=self.tmp_path / "no_c1.db",
            canary2_db_path=self.tmp_path / "no_c2.db",
        )
        guard.reserve_batch("syn_batch_1", [self.art_eda.id], 0.003)
        guard.reconcile_batch("syn_batch_1", actual_cost_usd=0.0025, input_tokens=900, output_tokens=200)

        syn_decisions = [
            DiagnosticArticleDecision(
                article_id=self.art_eda.id,
                score=85,
                disposition="DIGEST",
                kind="NEW",
                urgency="NORMAL",
                rationale_tr="Sentetik karar.",
                headline_tr=self.art_eda.title,
                verification_status="FIRST_PARTY_STATEMENT",
            )
        ]
        preview = build_editorial_digest_preview(
            limit=2,
            repo_root=self.tmp_path,
            decisions=syn_decisions,
            articles=[self.art_eda],
        )

        budget = preview.get("budget_info")
        self.assertIsNotNone(budget)
        self.assertEqual(budget["budget_ceiling_usd"], 0.0)
        self.assertAlmostEqual(budget["total_consumed_usd"], guard.get_total_consumed_usd(), places=6)
        # Verify the synthetic batch cost (0.0025) was loaded dynamically from the temp ledger.
        self.assertAlmostEqual(budget["total_consumed_usd"], 0.0025, places=6)
        self.assertAlmostEqual(budget["prior_canaries_usd"], 0.0, places=6)
        self.assertEqual(budget["batches_count"], 1)

    def test_historical_phase4b_tests_skip_unconditionally(self) -> None:
        """Historical authentic-data Phase 4B tests must skip unconditionally without accessing data/."""
        for method_name in (
            "test_build_editorial_digest_preview_with_genuine_project_data",
            "test_budget_ledger_reporting_matches_ledger_file",
        ):
            method = getattr(self, method_name)
            self.assertTrue(getattr(method, "__unittest_skip__", False), f"{method_name} must skip")
            self.assertEqual(getattr(method, "__unittest_skip_why__", ""), HISTORICAL_DATA_SKIP_REASON)


if __name__ == "__main__":
    unittest.main()
