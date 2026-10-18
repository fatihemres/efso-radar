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

import unittest
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

from efso_radar.ai.client import (
    detect_sensitive_tragedy_content,
    project_diagnostic_decisions_to_assessments,
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


class TestPhase4BDigestPreview(unittest.TestCase):
    """Test suite for Phase 4B Editorial Digest Preview."""

    def setUp(self) -> None:
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

    def test_build_editorial_digest_preview_with_genuine_project_data(self) -> None:
        """Integration test: build_editorial_digest_preview correctly loads genuine saved records."""
        preview = build_editorial_digest_preview(limit=5, preview_timestamp=self.now)

        self.assertEqual(len(preview["selected_top"]), 5)
        self.assertGreaterEqual(len(preview["reserve_backlog"]), 10)
        self.assertGreaterEqual(len(preview["duplicate_drops"]), 1)

        # Check ranking: scores must be monotonically non-increasing
        scores = [a.score for a in preview["selected_top"]]
        self.assertEqual(scores, sorted(scores, reverse=True))

        # Check that top scores are genuinely 85, 85, 82, 82, 82
        self.assertEqual(scores, [85, 85, 82, 82, 82])

        # Verify no sensitive stories leaked into DIGEST
        for a in preview["selected_top"]:
            self.assertEqual(a.disposition, EditorialDisposition.DIGEST)
            self.assertTrue(a.is_publishable)
            self.assertFalse(detect_sensitive_tragedy_content(a.headline_tr, a.why_efso_tr))

        # Verify Çağatay Ulusoy duplicate is in duplicate_drops
        dup_headlines = [item.headline_tr for item, _ in preview["duplicate_drops"]]
        self.assertTrue(any("Çağatay Ulusoy" in h for h in dup_headlines))

        # Verify HTML content
        html_msg = preview["telegram_html"]
        self.assertIn("EDİTORYAL BÜLTEN", html_msg)
        self.assertIn("[DEMO / KALİBRASYON]", html_msg)
        self.assertIn("5 haber önerisi", html_msg)
        self.assertIn("<a href=", html_msg)
        self.assertIn("Instagram'da otomatik paylaşım yapılmaz.", html_msg)

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

    def test_budget_ledger_reporting_matches_ledger_file(self) -> None:
        """Budget reporting in preview dynamically reads actual ledger evidence."""
        preview = build_editorial_digest_preview(limit=5)
        budget = preview.get("budget_info")

        self.assertIsNotNone(budget)
        self.assertEqual(budget["budget_ceiling_usd"], 0.05)
        guard = ExperimentBudgetGuard(ledger_path=REPO_ROOT / "data" / "experiment_budget_ledger.json")
        self.assertAlmostEqual(budget["total_consumed_usd"], guard.get_total_consumed_usd(), places=6)
        self.assertAlmostEqual(budget["remaining_allowance_usd"], guard.get_remaining_allowance_usd(), places=6)
        self.assertAlmostEqual(budget["prior_canaries_usd"], guard.get_prior_canaries_cost_usd(), places=6)

    def test_url_checks_separate_structural_and_remote(self) -> None:
        """URL validation reports separate structural verification from remote network verification."""
        preview = build_editorial_digest_preview(limit=5)
        url_checks = preview.get("url_checks", [])

        self.assertGreaterEqual(len(url_checks), 5)
        for check in url_checks:
            # Structural validation passes 100%
            self.assertTrue(check["structural_valid"])
            self.assertTrue(check["url"].startswith("http://") or check["url"].startswith("https://"))
            # Remote check is strictly False (offline isolation)
            self.assertFalse(check["remote_checked"])


if __name__ == "__main__":
    unittest.main()
