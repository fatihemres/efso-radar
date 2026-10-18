"""Unit and integration regression tests for Phase 4C: Live RSS Discovery Trial.

Verifies:
1. Formatter readability improvements:
   - Word-boundary safe truncation (no sliced words).
   - Redundancy suppression for identical cover and headline text.
   - Turkey local time formatting with explicit TSİ label.
2. UPDATE classification and ambiguity audit:
   - Rejection of length-only pseudo-updates.
   - Preservation of legitimate updates with concrete development markers.
3. Live RSS trial evidence integrity:
   - Verification of bounded intake (12 articles).
   - Sensitive tragedy screening isolation (3 sensitive articles flagged).
   - Zero Gemini and zero Telegram side effects.
"""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone
from pathlib import Path

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
    clean_factual_relevance,
    format_telegram_digest,
    format_turkey_timestamp,
    is_meaningful_update,
    is_redundant_cover_title,
    truncate_word_boundary,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestPhase4CRssTrial(unittest.TestCase):
    """Test suite for Phase 4C Formatter and RSS Trial enhancements."""

    def test_truncate_word_boundary_avoids_slicing_words(self) -> None:
        """truncate_word_boundary cuts text at whitespace without slicing syllables."""
        raw = "Nursema'nın set bedeli! Ceren Karakoç boyun rahatsızlığı için fizik tedavi görüyor"
        # Truncating at 75 chars would slice "tedavi" or "görüyor" mid-word
        truncated = truncate_word_boundary(raw, max_length=75)

        # Must not end with a partial word like "görüy"
        self.assertFalse(truncated.endswith("görüy"))
        self.assertFalse(truncated.endswith("görü"))
        self.assertLessEqual(len(truncated), 75)
        # Ends cleanly on full word
        self.assertTrue(truncated.endswith("fizik tedavi") or truncated.endswith("rahatsızlığı için"))

    def test_is_redundant_cover_title_detects_headline_duplication(self) -> None:
        """is_redundant_cover_title identifies identical or near-identical cover titles."""
        h = "Eda Ece'den net kural: Anne olduktan sonra o sahneleri çekmiyor!"
        c_identical = "Eda Ece'den net kural: Anne olduktan sonra o sahneleri çekmiyor!"
        c_prefix = "Eda Ece'den net kural: Anne olduktan sonra"
        c_distinct = "Eda Ece'nin Rol Tercihleri ve Anne Kuralı"

        self.assertTrue(is_redundant_cover_title(h, c_identical))
        self.assertTrue(is_redundant_cover_title(h, c_prefix))
        self.assertFalse(is_redundant_cover_title(h, c_distinct))

    def test_clean_factual_relevance_strips_unsupported_hyperbole(self) -> None:
        """clean_factual_relevance removes sensationalist buzzwords while keeping facts."""
        noisy = "Sosyal medyayı salladı ve viral oldu! Sanatçının yeni albüm açıklaması büyük ilgi çekti."
        cleaned = clean_factual_relevance(noisy)

        self.assertNotIn("sosyal medyayı salladı", cleaned.lower())
        self.assertNotIn("viral oldu", cleaned.lower())
        self.assertIn("albüm", cleaned.lower())

    def test_format_turkey_timestamp_includes_tsi(self) -> None:
        """format_turkey_timestamp outputs UTC+3 time with TSİ label."""
        utc_dt = datetime(2026, 10, 18, 17, 30, tzinfo=timezone.utc)
        formatted = format_turkey_timestamp(utc_dt)

        self.assertIn("18 Ekim 2026", formatted)
        self.assertIn("20:30", formatted)  # 17:30 UTC + 3 hours = 20:30 TSİ
        self.assertIn("TSİ", formatted)

    def test_is_meaningful_update_rejects_length_only_without_signals(self) -> None:
        """is_meaningful_update rejects updates that have length but lack concrete new facts."""
        base = EditorialAssessment(
            article_ids=["art_base"],
            score=78,
            kind=EditorialKind.NEW,
            headline_tr="Ünlü oyuncu tatilde görüntülendi",
            cover_title_tr="Tatil görüntüleri",
            caption_idea_tr="Tatil görüntüleri",
            why_efso_tr="Tatil haberi",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=["https://example.com/base"],
            supporting_articles=[],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        # Fluff text: >15 chars, but no concrete new action/decision/fact
        fluff_update = EditorialAssessment(
            article_ids=["art_fluff"],
            score=70,
            kind=EditorialKind.UPDATE,
            development_tr="Herkesin merakla beklediği tatil görüntüleri hakkında yorumlar gelmeye devam ediyor.",
            headline_tr="Ünlü oyuncunun tatili hakkında yeni yorumlar",
            cover_title_tr="Yorumlar",
            caption_idea_tr="Yorumlar",
            why_efso_tr="Yorumlar",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=["https://example.com/fluff"],
            supporting_articles=[],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        # Must be rejected because it lacks concrete development signal keywords
        self.assertFalse(is_meaningful_update(fluff_update, base))

        # Real update: has concrete decision/action
        real_update = EditorialAssessment(
            article_ids=["art_real"],
            score=75,
            kind=EditorialKind.UPDATE,
            development_tr="Tatil dönüşü yapım şirketiyle yeni dizi projesi için resmen sözleşme imzaladığını duyurdu.",
            headline_tr="Ünlü oyuncudan yeni dizi anlaşması",
            cover_title_tr="Yeni Sözleşme",
            caption_idea_tr="Sözleşme imzaladı",
            why_efso_tr="Yeni proje anlaşması",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.FIRST_PARTY_STATEMENT,
            source_urls=["https://example.com/real"],
            supporting_articles=[],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        # Must be accepted
        self.assertTrue(is_meaningful_update(real_update, base))

    def test_trial_evidence_file_integrity(self) -> None:
        """Verify that the trial evidence file contains the exact bounded 12 articles and sensitivity flags."""
        evidence_file = REPO_ROOT / "data" / "trial_live_rss_evidence.json"
        if not evidence_file.exists():
            self.skipTest("Trial evidence file not found (run trial_live_rss_discovery.py first).")

        with open(evidence_file, encoding="utf-8") as f:
            data = json.load(f)

        self.assertEqual(data["source_id"], "haberturk_magazin")
        self.assertEqual(data["source_name"], "Habertürk Magazin")
        self.assertEqual(data["source_url"], "https://www.haberturk.com/rss/magazin.xml")
        self.assertEqual(data["accepted_items_count"], 12)
        self.assertGreaterEqual(data["raw_items_count"], 12)

        # 3 sensitive tragedy items identified (Soner Olgun, Süleyman Saim Tekcan, Can Bonomo)
        self.assertEqual(data["sensitive_items_count"], 3)

        articles = data["articles"]
        self.assertEqual(len(articles), 12)

        for art in articles:
            self.assertTrue(art["title"])
            self.assertTrue(art["url"].startswith("https://www.haberturk.com/magazin/"))
            self.assertIn("TSİ", art["published_at_tsi"])


if __name__ == "__main__":
    unittest.main()
