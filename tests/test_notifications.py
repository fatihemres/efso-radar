"""Tests for notification formatters and Telegram channel."""
import unittest
from datetime import datetime, timezone

from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    StoryCategory,
    SuggestedFormat,
)
from efso_radar.notifications.formatters import format_telegram_alert
from efso_radar.notifications.telegram import TelegramNotificationChannel


class TestNotifications(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)
        self.article = Article.create("Sample Title", "https://news.com/art", "Test Source", self.now)
        self.assessment = EditorialAssessment(
            article_ids=[self.article.id],
            score=91,
            kind=EditorialKind.NEW,
            headline_tr="Harika <script>alert(1)</script> Haber",
            cover_title_tr="KAPAK & DETAY",
            caption_idea_tr="Caption metni 'alinti' içeriyor.",
            why_efso_tr="Neden değerli?",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
            supporting_articles=[self.article],
        )

    def test_telegram_html_escaping(self):
        text = format_telegram_alert(self.assessment)
        self.assertNotIn("<script>", text)
        self.assertIn("&lt;script&gt;", text)
        self.assertIn("&amp;", text)

    def test_telegram_update_development_block(self):
        update_assessment = EditorialAssessment(
            article_ids=[self.article.id],
            score=93,
            kind=EditorialKind.UPDATE,
            parent_story_id="STORY-1",
            development_tr="Önemli yeni gelişme yaşandı.",
            headline_tr="Gelişme Başlığı",
            cover_title_tr="GELİŞME KAPAĞI",
            caption_idea_tr="Caption metni.",
            why_efso_tr="Neden değerli?",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
            supporting_articles=[self.article],
        )
        text = format_telegram_alert(update_assessment)
        self.assertIn("Ne değişti?", text)
        self.assertIn("Önemli yeni gelişme yaşandı.", text)

    def test_telegram_dry_run_dispatch(self):
        settings = Settings(telegram_bot_token="", telegram_chat_id="")
        channel = TelegramNotificationChannel(settings)
        record = channel.send(self.assessment, "STORY-123", dry_run=True)
        self.assertEqual(record.status, DeliveryStatus.SENT)
        self.assertEqual(record.story_id, "STORY-123")


if __name__ == "__main__":
    unittest.main()
