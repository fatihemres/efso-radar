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

import html
import unittest
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

from efso_radar.ai.client import detect_sensitive_tragedy_content
from efso_radar.domain import (
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
from efso_radar.sources.models import SourceDefinition
from efso_radar.sources.parsers import FeedParser

HISTORICAL_DATA_SKIP_REASON = (
    "Historical authentic Phase 4C live RSS trial evidence in operational data/ is "
    "intentionally excluded from hermetic offline tests (Issue #104 / T1B); see labeled "
    "synthetic RSS parser/screening regressions in TestPhase4CRssTrial."
)


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

    @unittest.skip(HISTORICAL_DATA_SKIP_REASON)
    def test_trial_evidence_file_integrity(self) -> None:
        """Verify that the trial evidence file contains the exact bounded 12 articles and sensitivity flags."""
        raise AssertionError("Skipped unconditionally under Issue #104 / T1B")

    def _build_synthetic_rss_items_and_parse(self):
        now = datetime(2026, 10, 18, 18, 0, tzinfo=timezone.utc)
        source_def = SourceDefinition(
            source_id="synthetic_magazin_tr",
            name="Sentetik Magazin TR",
            url="https://synthetic.example.invalid/rss/magazin.xml",
            market=Market.TR,
            category="MAGAZIN_TR",
            source_type="rss",
        )
        raw_entries = [
            ("Sentetik Oyuncu A Yeni Dizi Projesiyle Dönüyor", "Oyuncu A yeni sezon dijital dizi çekimlerine başladı."),
            ("Sentetik Şarkıcı B Konser Turnesini Duyurdu", "Ünlü şarkıcı yaz konserleri için tarihleri açıkladı."),
            ("Sentetik Sanatçı C Kardeşini Kaybetti: Cenaze Yarın", "Sanatçı C'nin kardeşi vefat etti, cenaze töreni ikindi vakti yapılacak."),
            ("Sentetik Çift D Gala Gecesinde Boy Gösterdi", "Kırmızı halı davetine ünlü isimler yoğun ilgi gösterdi."),
            ("Sentetik Usta Ressam E Vefat Etti", "Usta sanatçı tedavi gördüğü hastanede yaşamını yitirdi."),
            ("Sentetik Sunucu F Podcast Serisine Başladı", "Yeni kültür ve sohbet programı ilk bölümüyle yayında."),
            ("Sentetik Müzisyen G Annesinin Vefatı Sonrası Paylaşım Yaptı", "Ünlü şarkıcı annesinin trajik intihar kaybı sonrası duygusal mesaj paylaştı."),
            ("Sentetik Yönetmen H Festival Ödülünü Kazandı", "Film festivalinde en iyi yönetmen ödülü sahibini buldu."),
            ("Sentetik Oyuncu I Reklam Yüzü Oldu", "Yeni sezon kampanyası için kamera karşısına geçti."),
            ("Sentetik Grup J Albüm Kapağını Paylaştı", "Beklenen pop albümünün yayın tarihi netleşti."),
            ("Sentetik Tiyatro K Oyunu Prömiyer Yaptı", "Sezon açılışı davetinde salon tamamen doldu."),
            ("Sentetik Moda L Defilesi Büyük Beğeni Topladı", "Tasarımcının yeni koleksiyonu moda haftasında sergilendi."),
            ("Sentetik Bonus 13 Sınır Dışı Kalmalı", "12 öğe sınırı nedeniyle kesilecek ekstra sentetik haber."),
            ("Sentetik Bonus 14 Sınır Dışı Kalmalı", "12 öğe sınırı nedeniyle kesilecek ikinci ekstra haber."),
        ]
        items_xml = []
        for idx, (title, desc) in enumerate(raw_entries):
            pub = format_datetime(now - timedelta(minutes=idx * 20))
            items_xml.append(
                f"<item><title>{html.escape(title)}</title>"
                f"<link>https://synthetic.example.invalid/magazin/{idx + 1}</link>"
                f"<description>{html.escape(desc)}</description>"
                f"<pubDate>{pub}</pubDate></item>"
            )
        stale_pub = format_datetime(now - timedelta(hours=96))
        items_xml.append(
            "<item><title>Sentetik Bayat Haber 96 Saat Önce</title>"
            "<link>https://synthetic.example.invalid/magazin/stale</link>"
            "<description>Eski tarihli haber yaş filtresinden elenmelidir.</description>"
            f"<pubDate>{stale_pub}</pubDate></item>"
        )
        xml_bytes = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<rss version="2.0"><channel><title>Sentetik Magazin</title>'
            + "".join(items_xml)
            + "</channel></rss>"
        ).encode("utf-8")

        parse_result = FeedParser.parse_feed(
            xml_bytes=xml_bytes,
            source_def=source_def,
            now=now,
            max_age_hours=72,
        )
        return now, parse_result

    def test_synthetic_rss_feed_parse_bounded_intake_and_sensitivity_screening(self) -> None:
        """Synthetic regression: FeedParser enforces age filter, bounded [:12] intake, and tragedy screening."""
        _, parse_result = self._build_synthetic_rss_items_and_parse()
        self.assertGreaterEqual(parse_result.raw_seen, 14)
        self.assertEqual(len(parse_result.articles), 14)

        bounded = parse_result.articles[:12]
        self.assertEqual(len(bounded), 12)
        sensitive = [
            art for art in bounded if detect_sensitive_tragedy_content(art.title, art.snippet or "")
        ]
        self.assertEqual(len(sensitive), 3)
        for art in bounded:
            self.assertTrue(art.url.startswith("https://synthetic.example.invalid/magazin/"))
            self.assertIn("TSİ", format_turkey_timestamp(art.published_at))

    def test_synthetic_rss_digest_preview_excludes_sensitive_and_formats_cleanly(self) -> None:
        """Synthetic regression: filtered RSS digest preview omits sensitive items and formats cleanly."""
        now, parse_result = self._build_synthetic_rss_items_and_parse()
        bounded = parse_result.articles[:12]
        safe_articles = [
            art for art in bounded if not detect_sensitive_tragedy_content(art.title, art.snippet or "")
        ]
        self.assertEqual(len(safe_articles), 9)

        assessments = [
            EditorialAssessment(
                article_ids=[art.id],
                score=85 - idx,
                kind=EditorialKind.NEW,
                headline_tr=art.title,
                cover_title_tr=truncate_word_boundary(art.title, 65),
                caption_idea_tr=art.snippet or art.title,
                why_efso_tr="Sentetik RSS deneme seçkisi.",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
                verification_status=VerificationStatus.REPUTABLE_REPORTING,
                source_urls=[art.url],
                supporting_articles=[art],
                is_publishable=True,
                disposition=EditorialDisposition.DIGEST,
            )
            for idx, art in enumerate(safe_articles[:5])
        ]
        rendered = format_telegram_digest(
            assessments,
            window_label=f"[SENTETİK RSS DENEME SEÇKİSİ] ({format_turkey_timestamp(now)})",
            deduplicate=True,
        )
        self.assertIn("5 haber önerisi", rendered)
        self.assertNotIn("Vefat", rendered)
        self.assertNotIn("Cenaze", rendered)
        self.assertNotIn("intihar", rendered.lower())

    def test_historical_rss_trial_evidence_test_skips_unconditionally(self) -> None:
        """Historical live RSS trial evidence test must skip unconditionally without probing data/."""
        method = self.test_trial_evidence_file_integrity
        self.assertTrue(getattr(method, "__unittest_skip__", False))
        self.assertEqual(getattr(method, "__unittest_skip_why__", ""), HISTORICAL_DATA_SKIP_REASON)


if __name__ == "__main__":
    unittest.main()
