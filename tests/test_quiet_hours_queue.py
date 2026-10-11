"""Tests for quiet-hours queuing, draining, run-lock lease renewal, and alert idempotency."""
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
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
    def __init__(self, articles=None):
        self.articles = articles or []

    def fetch_candidates(self, now, max_age_hours):
        return self.articles, 1


class MockAIEngine(AIEditorialEngine):
    def __init__(self, picks=None):
        self.picks = picks or []

    def evaluate(self, candidates, recent_stories, now):
        return self.picks


class MockNotifier(NotificationChannel):
    def __init__(self):
        self.sent_records = []

    def send(self, assessment, story_id, dry_run=False):
        record = NotificationRecord(
            notification_id=f"NOTIF-MOCK-{len(self.sent_records)}",
            story_id=story_id,
            kind=assessment.kind,
            sent_at=utc_now(),
            status=DeliveryStatus.SENT,
            message_text=f"Mock alert for {assessment.headline_tr}",
            headline_tr=assessment.headline_tr,
            score=assessment.score,
        )
        self.sent_records.append(record)
        return record


class TestQuietHoursQueueAndIdempotency(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "queue_test.db"
        self.repo = SqliteStateRepository(self.db_path)
        self.now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_run_lock_lease_renewal(self):
        worker = "worker-heartbeat"
        # Acquire lock for 5 minutes
        self.assertTrue(self.repo.acquire_run_lock(worker, timeout_minutes=5))

        # Active worker extends lease by 15 minutes
        self.assertTrue(self.repo.renew_run_lock(worker, extend_minutes=15))

        # Different worker cannot renew or acquire
        self.assertFalse(self.repo.renew_run_lock("worker-intruder", extend_minutes=15))
        self.assertFalse(self.repo.acquire_run_lock("worker-intruder", timeout_minutes=15))

    def test_has_story_been_alerted_idempotency(self):
        self.assertFalse(self.repo.has_story_been_alerted("EFSO-20261011-1234"))

        # Pre-record notification
        record = NotificationRecord(
            notification_id="NOTIF-1",
            story_id="EFSO-20261011-1234",
            kind=EditorialKind.NEW,
            channel="TELEGRAM",
            sent_at=self.now,
            status=DeliveryStatus.SENT,
            message_text="Alert text",
            headline_tr="Headline",
            score=90,
        )
        self.repo.record_notification(record)

        self.assertTrue(self.repo.has_story_been_alerted("EFSO-20261011-1234"))

    def test_quiet_hours_hold_and_subsequent_drain(self):
        article = Article.create("Gece Haberi", "https://news.com/gece", "Kaynak", self.now)
        pick = EditorialAssessment(
            article_ids=[article.id],
            score=89,
            kind=EditorialKind.NEW,
            headline_tr="Gece Yarısı Bomba Gelişme",
            cover_title_tr="GECE BOMBASI",
            caption_idea_tr="Gece saatlerinde önemli haber.",
            why_efso_tr="Büyük pop kültürü haberi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
            supporting_articles=[article],
        )

        # Run 1: Quiet hours ENABLED (simulating nighttime e.g. 23:00 UTC)
        night_time = datetime(2026, 10, 11, 23, 0, tzinfo=timezone.utc)
        night_settings = Settings(
            sqlite_db_path=self.db_path,
            quiet_hours_enabled=True,
            quiet_hours_start_utc="21:00",
            quiet_hours_end_utc="05:30",
            quiet_hours_behavior="HOLD",
        )
        orch_night = RadarOrchestrator(
            settings=night_settings,
            repository=self.repo,
            source=MockNewsSource([article]),
            ai_engine=MockAIEngine([pick]),
        )

        # Run at night
        result_night = orch_night.run(now=night_time)
        self.assertTrue(result_night.success)

        # Alert should be held in queue
        held_list = self.repo.get_held_notifications()
        self.assertEqual(len(held_list), 1)
        self.assertEqual(held_list[0].status, DeliveryStatus.HELD_QUIET_HOURS)

        # Run 2: Next morning outside quiet hours (simulating 09:00 UTC)
        morning_time = datetime(2026, 10, 12, 9, 0, tzinfo=timezone.utc)
        morning_settings = Settings(
            sqlite_db_path=self.db_path,
            quiet_hours_enabled=True,
            quiet_hours_start_utc="21:00",
            quiet_hours_end_utc="05:30",
            quiet_hours_behavior="HOLD",
        )
        orch_morning = RadarOrchestrator(
            settings=morning_settings,
            repository=self.repo,
            source=MockNewsSource([]),  # No new articles
            ai_engine=MockAIEngine([]),
        )

        # Run in morning should drain the held queue!
        result_morning = orch_morning.run(now=morning_time)
        self.assertTrue(result_morning.success)
        self.assertEqual(result_morning.notifications_dispatched, 1)

        # Queue should now be empty
        self.assertEqual(len(self.repo.get_held_notifications()), 0)

        # Subsequent run should not re-dispatch!
        result_afternoon = orch_morning.run(now=morning_time + timedelta(hours=2))
        self.assertEqual(result_afternoon.notifications_dispatched, 0)



if __name__ == "__main__":
    unittest.main()
