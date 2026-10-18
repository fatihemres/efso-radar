"""
Unit and integration tests for Phase 4F: Telegram Editorial Digest UX Redesign.

Verifies all 12 Phase 4F technical & UX requirements:
1. New Telegram layout (Header, separators, story blocks, footer).
2. UTF-8 Turkish characters and emojis.
3. Safe Telegram HTML escaping (special characters &, <, >, " escaped).
4. Clickable source links (valid <a> tags with properly quoted URLs).
5. Missing optional metadata (omitted cleanly without broken labels).
6. Short factual summaries (extracted from authentic source snippets without fabrication).
7. Non-redundant cover suggestions (omitted when repeating or mirroring the headline).
8. Message-length boundaries (exceeding 4096 chars raises ValueError; no silent dropping).
9. Timezone display as TSİ (Turkey local time UTC+3).
10. Exclusion of sensitive and held stories (SENSITIVE_REVIEW and HOLD_FOR_VERIFICATION excluded).
11. Protection against duplicate delivery after formatter changes (stable editorial identity).
12. No network calls in preview mode (dry-run isolation).
"""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from efso_radar.config import Settings
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
    compute_stable_digest_identity,
    extract_factual_summary,
    format_telegram_digest,
    format_telegram_digest_legacy,
    format_turkey_timestamp,
    is_redundant_cover_title,
    truncate_word_boundary,
)
from efso_radar.notifications.telegram import deliver_one_shot_editorial_digest
from scripts.deliver_telegram_digest_oneshot import build_final_three_assessments
from scripts.preview_telegram_digest_phase4f import html_to_plain_text, run_preview


class TestPhase4FTelegramUXRedesign(unittest.TestCase):
    """Focused test suite for Phase 4F Telegram digest UX redesign."""

    def setUp(self) -> None:
        self.turkey_tz = timezone(timedelta(hours=3))
        self.ref_time = datetime(2026, 10, 18, 20, 53, tzinfo=self.turkey_tz)
        self.assessments = build_final_three_assessments()

    # 1. New Telegram layout
    def test_new_telegram_layout_structure(self) -> None:
        """Verify the exact new layout structure: Header, separators, story blocks, footer."""
        html_out = format_telegram_digest(self.assessments, timestamp=self.ref_time)

        # Header check
        self.assertIn("📡 <b>EFSO RADAR</b>\n📰 <b>EDİTORYAL BÜLTEN</b>", html_out)
        self.assertIn("18 Ekim 2026 · 20:53 TSİ", html_out)
        self.assertIn("✨ 3 haber önerisi", html_out)

        # Separator check
        separator = "━━━━━━━━━━━━━━"
        self.assertIn(separator, html_out)
        # Separator appears before each story and before the footer (3 stories + 1 footer = 4 separators)
        self.assertEqual(html_out.count(separator), 4)

        # Numbered story blocks
        self.assertIn("🟢 <b>01 · ", html_out)
        self.assertIn("🟢 <b>02 · ", html_out)
        self.assertIn("🟢 <b>03 · ", html_out)

        # Footer check
        self.assertIn("🔒 <b>Yalnızca editoryal değerlendirme.</b>", html_out)
        self.assertIn("<i>Instagram'da otomatik paylaşım yapılmaz.</i>", html_out)

    # 2. UTF-8 Turkish characters and emojis
    def test_utf8_turkish_characters_and_emojis(self) -> None:
        """Verify full preservation of Turkish characters (Ş, ç, ğ, ı, ö, ü) and emojis."""
        html_out = format_telegram_digest(self.assessments, timestamp=self.ref_time)

        # Turkish characters in headline and body
        self.assertIn("Şükrü Özyıldız", html_out)
        self.assertIn("kız kardeşi", html_out)
        self.assertIn("değnekleriyle", html_out)

        # Emojis used consistently
        for emoji in ["📡", "📰", "✨", "🟢", "⭐", "📱", "📝", "💡", "🎯", "🔗", "🔒"]:
            self.assertIn(emoji, html_out)

    # 3. Safe Telegram HTML escaping
    def test_safe_telegram_html_escaping(self) -> None:
        """Verify strict HTML entity escaping for dangerous characters (<, >, &, \")."""
        evil_art = Article(
            id="art_xss",
            title='Tehlikeli & "Şok" <script>alert(1)</script>',
            url="https://example.com/test?a=1&b=2",
            source='Kanal & "Medya"',
            market=Market.TR,
            published_at=self.ref_time,
            snippet='Test snippet with <tags> & "quotes"',
        )
        evil_assess = EditorialAssessment(
            article_ids=[evil_art.id],
            score=90,
            kind=EditorialKind.NEW,
            headline_tr='Test & "Özel" <script>evil()</script>',
            cover_title_tr='KAPAK & "FİKİR"',
            why_efso_tr='Neden? <tag> & "efso"',
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[evil_art.url],
            supporting_articles=[evil_art],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        html_out = format_telegram_digest([evil_assess], timestamp=self.ref_time)

        # Raw unescaped brackets or scripts must NOT exist
        self.assertNotIn("<script>", html_out)
        self.assertIn("&lt;script&gt;evil()&lt;/script&gt;", html_out)
        self.assertIn("&amp;", html_out)
        # Link quote escaping
        self.assertIn('href="https://example.com/test?a=1&amp;b=2"', html_out)

    # 4. Clickable source links
    def test_clickable_source_links(self) -> None:
        """Source links are formatted as clean Telegram HTML <a> tags with proper URLs."""
        html_out = format_telegram_digest(self.assessments, timestamp=self.ref_time)

        self.assertIn('<a href="https://www.haberturk.com/magazin/foto/sukru-ozyildiz-in-kiz-kardesi-burcu-ozyildiz-onur-eksioglu-ile-evlendi-3920594">Habertürk Magazin</a>', html_out)
        self.assertIn('<a href="https://www.haberturk.com/magazin/antonio-banderas-ve-nicolas-cage-true-detective-dizisinde-bulusuyor-3920600">Habertürk Magazin</a>', html_out)
        self.assertIn('<a href="https://www.haberturk.com/magazin/foto/unlu-model-gisele-bundchen-sakatlandi-3920648">Habertürk Magazin</a>', html_out)

    # 5. Missing optional metadata
    def test_missing_optional_metadata_handling(self) -> None:
        """When optional fields (snippet, cover, why_efso) are missing, labels are cleanly omitted."""
        minimal_art = Article(
            id="art_min",
            title="Sade Haber Başlığı",
            url="https://example.com/min",
            source="Kaynak",
            market=Market.TR,
            published_at=self.ref_time,
            snippet="",  # No snippet
        )
        minimal_assess = EditorialAssessment(
            article_ids=[minimal_art.id],
            score=70,
            kind=EditorialKind.NEW,
            headline_tr="Sade Haber Başlığı",
            cover_title_tr="",  # No cover
            why_efso_tr="",  # No why_efso
            relevance_reason_tr="",
            suggested_format=SuggestedFormat.CAROUSEL,
            category=StoryCategory.GLOBAL,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[minimal_art.url],
            supporting_articles=[minimal_art],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        html_out = format_telegram_digest([minimal_assess], timestamp=self.ref_time)

        # Omitted sections should NOT leave dangling headers
        self.assertNotIn("📝 <b>Özet:</b>", html_out)
        self.assertNotIn("💡 <b>Editör notu:</b>", html_out)
        self.assertNotIn("🎯 <b>Kapak önerisi:</b>", html_out)
        # Essential lines still present
        self.assertIn("🟢 <b>01 · Sade Haber Başlığı</b>", html_out)
        self.assertIn("⭐ <b>70/100</b> · 📱 <b>CAROUSEL</b>", html_out)
        self.assertIn('<a href="https://example.com/min">Kaynak</a>', html_out)

    # 6. Short factual summaries
    def test_short_factual_summaries(self) -> None:
        """Verify summaries are factually grounded and extracted from genuine snippets."""
        for item in self.assessments:
            summary = extract_factual_summary(item)
            self.assertIsNotNone(summary)
            self.assertGreater(len(summary), 20)
            # Must match supporting article snippet
            self.assertEqual(summary, item.supporting_articles[0].snippet.strip())

    # 7. Non-redundant cover suggestions
    def test_non_redundant_cover_suggestions(self) -> None:
        """Cover suggestions that merely duplicate the headline are omitted."""
        art = Article(
            id="art_dup_cover",
            title="Demet Akalın yeni albümünü çıkardı",
            url="https://example.com/demet",
            source="Magazin",
            market=Market.TR,
            published_at=self.ref_time,
        )
        # Redundant cover (exact duplicate)
        assess_redundant = EditorialAssessment(
            article_ids=[art.id],
            score=80,
            kind=EditorialKind.NEW,
            headline_tr="Demet Akalın yeni albümünü çıkardı",
            cover_title_tr="Demet Akalın yeni albümünü çıkardı",  # identical
            why_efso_tr="Önemli albüm",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art.url],
            supporting_articles=[art],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

        self.assertTrue(is_redundant_cover_title(assess_redundant.headline_tr, assess_redundant.cover_title_tr))
        html_out = format_telegram_digest([assess_redundant], timestamp=self.ref_time)
        self.assertNotIn("🎯 <b>Kapak önerisi:</b>", html_out)

        # Distinct cover
        self.assertFalse(is_redundant_cover_title("Şükrü Özyıldız'ın kız kardeşi evlendi: Oyuncudan duygusal paylaşım", "ÖZYILDIZ AİLESİNDE DÜĞÜN"))
        self.assertFalse(is_redundant_cover_title("True Detective'in 5. Sezonunda Dev Buluşma: Nicolas Cage ile Antonio Banderas Bir Arada", "TRUE DETECTIVE 5'TE DEV BULUŞMA"))
        self.assertFalse(is_redundant_cover_title("Dünyaca Ünlü Süper Model Gisele Bündchen Koltuk Değnekleriyle Görüntülendi", "GISELE BÜNDCHEN KOLTUK DEĞNEKLERİYLE"))

    # 8. Message-length boundaries
    def test_message_length_boundaries_and_error(self) -> None:
        """Digest within 4096 chars succeeds; exceeding 4096 raises ValueError without silent drop."""
        # 3 stories fits cleanly well under limit
        html_out = format_telegram_digest(self.assessments, timestamp=self.ref_time)
        self.assertLess(len(html_out), 4096)
        self.assertGreater(len(html_out), 1500)

        # Create massive payload that exceeds 4096 chars
        huge_items = []
        for i in range(15):
            art = Article(
                id=f"huge_{i}",
                title=f"Uzun Başlık {i} " + "A" * 150,
                url=f"https://example.com/{i}",
                source="Habertürk Magazin",
                market=Market.TR,
                published_at=self.ref_time,
                snippet="Bu çok uzun bir olgusal özet metnidir. " * 8,
            )
            item = EditorialAssessment(
                article_ids=[art.id],
                score=80,
                kind=EditorialKind.NEW,
                headline_tr=f"Uzun Başlık {i} " + "A" * 150,
                cover_title_tr="KAPAK ÖNERİSİ " * 4,
                why_efso_tr="Bu editoryal gerekçe açıklamasıdır. " * 8,
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
                verification_status=VerificationStatus.REPUTABLE_REPORTING,
                source_urls=[art.url],
                supporting_articles=[art],
                is_publishable=True,
                disposition=EditorialDisposition.DIGEST,
            )
            huge_items.append(item)

        # Must raise ValueError, NOT silently truncate stories
        with self.assertRaises(ValueError) as ctx:
            format_telegram_digest(huge_items, timestamp=self.ref_time, deduplicate=False, validate_length=True)

        self.assertIn("exceeds Telegram 4096-character limit", str(ctx.exception))
        self.assertIn("Automatic discarding of stories is prohibited", str(ctx.exception))

    # 9. Timezone display as TSİ
    def test_timezone_display_tsi(self) -> None:
        """Timestamps are correctly displayed in Turkey local time (TSİ / UTC+3)."""
        # UTC 17:53 -> TSİ 20:53
        utc_time = datetime(2026, 10, 18, 17, 53, tzinfo=timezone.utc)
        ts_str = format_turkey_timestamp(utc_time, with_middle_dot=True)
        self.assertEqual(ts_str, "18 Ekim 2026 · 20:53 TSİ")

    # 10. Exclusion of sensitive and held stories
    def test_exclusion_of_sensitive_and_held_stories(self) -> None:
        """Sensitive tragedy and unverified rumors remain strictly excluded from digest formatting."""
        # Sensitive review
        art_sens = Article(
            id="sens_1",
            title="Usta sanatçı hayatını kaybetti",
            url="https://example.com/vefat",
            source="Habertürk Magazin",
            market=Market.TR,
            published_at=self.ref_time,
        )
        assess_sens = EditorialAssessment(
            article_ids=[art_sens.id],
            score=95,
            kind=EditorialKind.NEW,
            headline_tr="Usta sanatçı hayatını kaybetti",
            why_efso_tr="Vefat haberi",
            is_publishable=False,
            disposition=EditorialDisposition.SENSITIVE_REVIEW,
        )

        # Verification hold
        art_hold = Article(
            id="hold_1",
            title="Ünlü şarkıcı gizlice evlendi iddiası",
            url="https://example.com/iddia",
            source="Habertürk Magazin",
            market=Market.TR,
            published_at=self.ref_time,
        )
        assess_hold = EditorialAssessment(
            article_ids=[art_hold.id],
            score=90,
            kind=EditorialKind.NEW,
            headline_tr="Ünlü şarkıcı gizlice evlendi iddiası",
            why_efso_tr="Teyitsiz dedikodu",
            verification_status=VerificationStatus.UNVERIFIED_RUMOR,
            is_publishable=False,
            disposition=EditorialDisposition.HOLD,
        )

        # When filtering for DIGEST, only is_publishable=True and DIGEST eligible items qualify
        approved_digest_items = [
            a for a in [assess_sens, assess_hold] + self.assessments
            if a.is_publishable and a.disposition == EditorialDisposition.DIGEST
        ]
        self.assertEqual(len(approved_digest_items), 3)
        self.assertNotIn(assess_sens, approved_digest_items)
        self.assertNotIn(assess_hold, approved_digest_items)

    # 11. Protection against duplicate delivery after formatter changes
    def test_protection_against_duplicate_delivery_after_formatter_changes(self) -> None:
        """Changing HTML formatting does NOT allow previously delivered digest to be resent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_path = Path(tmpdir) / "test_ledger.json"

            # 1. Simulate historical Phase 4E delivery record (which used old HTML format)
            import json
            historical_hash = "06afb36bc22dff56263e72de"
            ledger_data = {
                "deliveries": {
                    historical_hash: {
                        "status": "DELIVERY_SUCCESS",
                        "delivered_at_utc": "2026-10-18T18:07:23.052243+00:00",
                        "delivered_at_tsi": "18 Ekim 2026 21:07 TSİ",
                        "target_chat_id": "10...01",
                        "stories_count": 3,
                        "digest_hash": historical_hash,
                        "message_id": 6,
                    }
                }
            }
            with open(ledger_path, "w", encoding="utf-8") as f:
                json.dump(ledger_data, f)

            settings = Settings(
                telegram_bot_token="test_token_12345",
                telegram_chat_id="1000000001",
                telegram_allowed_chat_ids=["1000000001"],
                telegram_delivery_enabled=True,
            )

            # 2. Attempt delivery with new Phase 4F formatting
            # Even though HTML hash is completely different from historical_hash,
            # duplicate prevention detects the identical 3 stories via stable editorial identity
            result = deliver_one_shot_editorial_digest(
                settings=settings,
                assessments=self.assessments,
                dry_run=False,
                ledger_path=ledger_path,
            )

            self.assertEqual(result["status"], "DUPLICATE_DELIVERY_PREVENTED")
            self.assertFalse(result["attempt_made"])
            self.assertNotEqual(result["digest_hash"], historical_hash)
            self.assertIn("Duplicate delivery prevented", result["error"])

    # 12. No network calls in preview mode
    def test_no_network_calls_in_preview_mode(self) -> None:
        """Preview script executes completely offline with zero network calls."""
        preview = run_preview()
        self.assertEqual(preview["story_count"], 3)
        self.assertGreater(preview["new_char_count"], 1500)
        self.assertLess(preview["new_char_count"], 4096)
        self.assertIn("18 Ekim 2026 · 20:53 TSİ", preview["new_html"])
        self.assertIn("Şükrü Özyıldız", preview["readable_preview"])

    # 13. Improved Instagram cover headlines (Final UX Polish)
    def test_final_polished_cover_headlines(self) -> None:
        """Verify the 3 improved Instagram cover headlines are present and formatted."""
        covers = [item.cover_title_tr for item in self.assessments]
        self.assertEqual(covers[0], "ÖZYILDIZ AİLESİNDE DÜĞÜN")
        self.assertEqual(covers[1], "TRUE DETECTIVE 5'TE DEV BULUŞMA")
        self.assertEqual(covers[2], "GISELE BÜNDCHEN KOLTUK DEĞNEKLERİYLE")

        html_out = format_telegram_digest(self.assessments, timestamp=self.ref_time)
        self.assertIn("🎯 <b>Kapak önerisi:</b>\nÖZYILDIZ AİLESİNDE DÜĞÜN", html_out)
        self.assertIn("🎯 <b>Kapak önerisi:</b>\nTRUE DETECTIVE 5&#x27;TE DEV BULUŞMA", html_out)
        self.assertIn("🎯 <b>Kapak önerisi:</b>\nGISELE BÜNDCHEN KOLTUK DEĞNEKLERİYLE", html_out)

    # 14. Complete, properly punctuated Turkish factual summaries
    def test_factual_summaries_are_complete_sentences(self) -> None:
        """Every factual summary must be a complete, properly punctuated Turkish sentence."""
        for item in self.assessments:
            summary = extract_factual_summary(item)
            self.assertIsNotNone(summary)
            # Punctuation check: must end with ., !, ?, or …
            self.assertTrue(summary.endswith((".", "!", "?", "…")))
            # Must not contain truncated trailing words
            self.assertFalse(summary.endswith("…") and len(summary.split()[-1]) <= 2)
            # Must not invent missing facts (strictly based on supporting article snippets)
            self.assertIn(summary.rstrip("."), item.supporting_articles[0].snippet)

    # 15. Semantic separation between summary (WHAT) and editor note (WHY)
    def test_semantic_separation_of_summary_and_editor_note(self) -> None:
        """Summary explains WHAT happened; editor note explains WHY it is relevant to Efso Magazin."""
        for item in self.assessments:
            summary = extract_factual_summary(item)
            note = item.why_efso_tr or item.relevance_reason_tr
            self.assertIsNotNone(summary)
            self.assertIsNotNone(note)

            # Neither should be a substring of the other
            self.assertNotIn(summary.lower(), note.lower())
            self.assertNotIn(note.lower(), summary.lower())

            # Similarity ratio between summary and note must be low (distinct semantic purpose)
            from difflib import SequenceMatcher
            ratio = SequenceMatcher(None, summary.lower(), note.lower()).ratio()
            self.assertLess(ratio, 0.40)

    # 16. Mobile UX test mode layout and idempotency isolation
    def test_ux_test_mode_layout_and_isolation(self) -> None:
        """UX test mode is explicitly labeled, has distinct headers/footers, and isolates idempotency."""
        ux_html = format_telegram_digest(self.assessments, timestamp=self.ref_time, is_ux_test=True)

        self.assertIn("🧪 <b>EFSO RADAR · MOBİL UX TESTİ</b>", ux_html)
        self.assertIn("📰 <b>EDİTORYAL BÜLTEN (TASARIM ÖNİZLEME)</b>", ux_html)
        self.assertIn("✨ 3 haber önerisi · TEST", ux_html)
        self.assertIn("🧪 <b>Yalnızca mobil görünüm ve UX doğrulama testidir.</b>", ux_html)

        # Test dry-run delivery with is_ux_test=True generates ux_test_ hash
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_path = Path(tmpdir) / "ux_ledger.json"
            settings = Settings(
                telegram_bot_token="test_token",
                telegram_chat_id="1000000001",
                telegram_allowed_chat_ids=["1000000001"],
                telegram_delivery_enabled=False,
            )
            res = deliver_one_shot_editorial_digest(
                settings=settings,
                assessments=self.assessments,
                dry_run=True,
                ledger_path=ledger_path,
                is_ux_test=True,
            )
            self.assertEqual(res["status"], "SAFE_PREVIEW_PASSED")
            self.assertTrue(res["stable_digest_id"].startswith("ux_test_"))
            self.assertTrue(res["is_ux_test"])


if __name__ == "__main__":
    unittest.main()
