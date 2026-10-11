"""Tests for SqliteStateRepository."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    NotificationRecord,
    Story,
    StoryCategory,
    StoryUpdate,
    SuggestedFormat,
    utc_now,
)
from efso_radar.repositories.sqlite import SqliteStateRepository


class TestSqliteRepository(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_radar.db"
        self.repo = SqliteStateRepository(self.db_path)
        self.now = utc_now()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_run_lock_lifecycle(self):
        # Worker 1 acquires lock
        self.assertTrue(self.repo.acquire_run_lock("worker-1", timeout_minutes=10))

        # Worker 2 attempts while Worker 1 is active -> blocked
        self.assertFalse(self.repo.acquire_run_lock("worker-2", timeout_minutes=10))

        # Worker 1 releases lock
        self.repo.release_run_lock("worker-1")

        # Worker 2 now succeeds
        self.assertTrue(self.repo.acquire_run_lock("worker-2", timeout_minutes=10))

    def test_article_processed_tracking(self):
        art = Article.create(
            title="Örnek Haber",
            url="https://example.com/art1",
            source="Kaynak",
            published_at=self.now,
        )
        self.assertFalse(self.repo.is_article_processed(art.id))

        self.repo.mark_articles_processed([art])
        self.assertTrue(self.repo.is_article_processed(art.id))

    def test_story_and_update_persistence(self):
        story = Story(
            story_id="STORY-TEST-1",
            headline_tr="Zendaya yeni projede başrolde",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now,
            last_updated_at=self.now,
            article_ids=["art-100"],
        )
        assessment = EditorialAssessment(
            article_ids=["art-100"],
            score=89,
            kind=EditorialKind.NEW,
            headline_tr="Zendaya yeni projede başrolde",
            cover_title_tr="ZENDAYA BAŞROLDE",
            caption_idea_tr="Ünlü oyuncu yeni yapımda rol alacak.",
            why_efso_tr="Hollywood'un en popüler genç yıldızı.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
        )
        self.repo.record_new_story(story, assessment)

        fetched = self.repo.get_story_by_id("STORY-TEST-1")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.headline_tr, "Zendaya yeni projede başrolde")

        # Record update
        update = StoryUpdate(
            update_id="UPD-101",
            story_id="STORY-TEST-1",
            development_tr="Yönetmen açıklandı ve çekim tarihi belirlendi.",
            detected_at=self.now + timedelta(hours=2),
            article_ids=["art-101"],
            significance_score=91,
        )
        self.repo.record_story_update(update, assessment)

        updated_story = self.repo.get_story_by_id("STORY-TEST-1")
        self.assertEqual(updated_story.update_count, 1)

    def test_notification_recording(self):
        record = NotificationRecord(
            notification_id="NOTIF-1",
            story_id="STORY-TEST-1",
            kind=EditorialKind.NEW,
            channel="TELEGRAM",
            sent_at=self.now,
            status=DeliveryStatus.SENT,
            message_text="Test message",
            headline_tr="Test Başlık",
            score=90,
        )
        self.repo.record_notification(record)

    def test_migration_from_legacy_json(self):
        legacy_path = Path(self.temp_dir.name) / "state.json"
        legacy_data = {
            "processed": {"old_art_1": self.now.isoformat(), "old_art_2": self.now.isoformat()},
            "sent": [
                {
                    "story_id": "EFSO-OLD-1",
                    "headline_tr": "Eski bildirim başlığı",
                    "sent_at": self.now.isoformat(),
                    "kind": "NEW",
                }
            ],
        }
        legacy_path.write_text(json.dumps(legacy_data), encoding="utf-8")

        # Migrate into new repo
        migrated = self.repo.migrate_from_legacy_json(legacy_path)
        self.assertEqual(migrated, 2)
        self.assertTrue(self.repo.is_article_processed("old_art_1"))
        self.assertTrue(self.repo.is_article_processed("old_art_2"))

        stories = self.repo.get_recent_stories()
        self.assertEqual(len(stories), 1)
        self.assertEqual(stories[0].story_id, "EFSO-OLD-1")


if __name__ == "__main__":
    unittest.main()
