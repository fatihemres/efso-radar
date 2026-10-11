"""Tests for RadarOrchestrator pipeline."""
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from efso_radar.ai.client import AIEditorialEngine
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    NotificationRecord,
    StoryCategory,
    SuggestedFormat,
    utc_now,
)
from efso_radar.notifications.base import NotificationChannel
from efso_radar.orchestrator import RadarOrchestrator
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.sources.base import NewsSource


class MockNewsSource(NewsSource):
    def __init__(self, articles=None, successes=1):
        self.articles = articles or []
        self.successes = successes

    def fetch_candidates(self, now, max_age_hours):
        return self.articles, self.successes


class MockAIEngine(AIEditorialEngine):
    def __init__(self, picks=None):
        self.picks = picks or []

    def evaluate(self, candidates, recent_stories, now):
        return self.picks


class MockNotifier(NotificationChannel):
    def __init__(self):
        self.sent_records = []

    def send(self, assessment, story_id, dry_run=False, now=None):

        record = NotificationRecord(
            notification_id=f"MOCK-{len(self.sent_records)}",
            story_id=story_id,
            kind=assessment.kind,
            sent_at=utc_now(),
            status=DeliveryStatus.SENT,
            message_text="Mock text",
            headline_tr=assessment.headline_tr,
            score=assessment.score,
        )
        self.sent_records.append(record)
        return record


class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_orch.db"
        self.settings = Settings(sqlite_db_path=self.db_path)
        self.repo = SqliteStateRepository(self.db_path)
        self.now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_pipeline_aborts_when_all_feeds_fail(self):
        source = MockNewsSource(articles=[], successes=0)
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=source,
            ai_engine=MockAIEngine(),
            notifier=MockNotifier(),
        )
        result = orch.run()
        self.assertFalse(result.success)
        self.assertIn("failed to respond", result.message)

    def test_pipeline_remains_silent_when_no_candidates(self):
        source = MockNewsSource(articles=[], successes=5)
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=source,
            ai_engine=MockAIEngine(),
            notifier=MockNotifier(),
        )
        result = orch.run()
        self.assertTrue(result.success)
        self.assertEqual(result.picks_approved, 0)
        self.assertEqual(result.notifications_dispatched, 0)

    def test_pipeline_approves_and_notifies(self):
        article = Article.create("Önemli Magazin Olayı", "https://url.com/magazin", "Kaynak", self.now)
        source = MockNewsSource(articles=[article], successes=1)
        pick = EditorialAssessment(
            article_ids=[article.id],
            score=92,
            kind=EditorialKind.NEW,
            headline_tr="Büyük Olay Yaşandı",
            cover_title_tr="KAPAK BAŞLIĞI",
            caption_idea_tr="Ayrıntılar burada.",
            why_efso_tr="Neden değerli.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
            supporting_articles=[article],
        )
        notifier = MockNotifier()
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=source,
            ai_engine=MockAIEngine(picks=[pick]),
            notifier=notifier,
        )

        result = orch.run()
        self.assertTrue(result.success)
        self.assertEqual(result.picks_approved, 1)
        self.assertEqual(result.notifications_dispatched, 1)
        self.assertEqual(len(notifier.sent_records), 1)

        # Verify article is now marked processed in repository
        self.assertTrue(self.repo.is_article_processed(article.id))

        # Verify story is recorded in repository
        stories = self.repo.get_recent_stories()
        self.assertEqual(len(stories), 1)
        self.assertEqual(stories[0].headline_tr, "Büyük Olay Yaşandı")


if __name__ == "__main__":
    unittest.main()
