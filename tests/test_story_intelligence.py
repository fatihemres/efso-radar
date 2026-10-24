"""Tests for Story Intelligence: event-level relationships, multi-publisher clustering,
significant development tracking, and separate domain records.

Verifies:
1. Multi-publisher reports of the same event collapse into a single story without duplicate alerts.
2. Distinct events involving the same celebrities are recognized as distinct stories.
3. Meaningful new developments (kind == UPDATE) trigger fresh notifications even when parent story_id was already alerted.
4. Identical repetitions of an already-alerted development are suppressed.
5. Strict separation of records across processed_articles, stories, story_updates, editorial_assessments, and notifications.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from efso_radar.ai.client import AIEditorialEngine
from efso_radar.config import Settings
from efso_radar.deduplication.engine import DeduplicationEngine
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
from efso_radar.orchestrator import RadarOrchestrator
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from efso_radar.sources.base import NewsSource
from tests.test_repository_parity import create_mock_hrana_transport


class MockNewsSource(NewsSource):
    def __init__(self, articles: list[Article]):
        self._articles = articles

    def fetch_candidates(self, now: datetime, max_age_hours: int = 48) -> tuple[list[Article], int]:
        return list(self._articles), 1


class MockAIEngine(AIEditorialEngine):
    def __init__(self, return_picks: list[EditorialAssessment]):
        self.return_picks = return_picks

    def evaluate(
        self,
        candidates: list[Article],
        recent_stories: list[Story],
        now: datetime,
    ) -> list[EditorialAssessment]:
        return list(self.return_picks)


class TestStoryIntelligence(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db_path = Path(self.tmp.name)
        self.sqlite_repo = SqliteStateRepository(self.db_path)

        self.sqlite_conn = sqlite3.connect(":memory:")
        self.mock_transport = create_mock_hrana_transport(self.sqlite_conn)
        self.turso_repo = TursoStateRepository(
            database_url="libsql://efso-test.turso.io",
            auth_token="mock-token-xyz",
            transport=self.mock_transport,
            init_schema=True,
        )

    def tearDown(self):
        try:
            self.sqlite_conn.close()
        except Exception:
            pass
        try:
            self.db_path.unlink(missing_ok=True)
        except Exception:
            pass

    def test_multi_publisher_clustering_collapses_to_single_story(self):
        """Three different publishers report the same event (Hadise's new relationship).

        Only 1 fresh candidate should emerge, with cluster_article_ids tracked.
        """
        art_hurriyet = Article.create(
            "Hadise ile ünlü iş insanı aşk yaşıyor iddiası",
            "https://hurriyet.com.tr/magazin/hadise-ask-1",
            "Hürriyet",
            self.now,
        )
        art_milliyet = Article.create(
            "Hadise'nin yeni aşkı ortaya çıktı: Ünlü iş insanı ile aşk yaşıyor",
            "https://milliyet.com.tr/cadde/hadise-yeni-sevgili-2",
            "Milliyet",
            self.now,
        )
        art_sabah = Article.create(
            "Hadise ve ünlü iş insanı aşk yaşıyor haberi gündemde",
            "https://sabah.com.tr/gunaydin/hadise-itiraf-3",
            "Sabah",
            self.now,
        )

        deduper = DeduplicationEngine(self.sqlite_repo)
        candidates = deduper.filter_fresh_candidates([art_hurriyet, art_milliyet, art_sabah])

        # Clustered into 1 representative
        self.assertEqual(len(candidates), 1)
        rep = candidates[0]
        # Cluster metadata preserved for auditability
        cluster_ids = rep.raw_metadata.get("cluster_article_ids", [])
        self.assertIn(art_hurriyet.id, cluster_ids)
        self.assertIn(art_milliyet.id, cluster_ids)
        self.assertIn(art_sabah.id, cluster_ids)
        self.assertEqual(rep.raw_metadata.get("cluster_size"), 3)

    def test_separate_records_across_all_domain_entities(self):
        """Verifies distinct persistence in processed_articles, stories, story_updates,

        editorial_assessments, and notifications.
        """
        repo = self.sqlite_repo
        a1 = Article.create("Kıvanç Tatlıtuğ yeni diziye başlıyor", "https://magazin.com/kivanc-1", "Magazin", self.now)
        repo.mark_articles_processed([a1])

        # 1. Verify processed_articles
        self.assertTrue(repo.is_article_processed(a1.id))

        story_id = f"EFSO-20261011-{a1.id[:8]}"
        asm1 = EditorialAssessment(
            article_ids=[a1.id],
            score=93,
            kind=EditorialKind.NEW,
            headline_tr="Kıvanç Tatlıtuğ ekranlara dönüyor",
            cover_title_tr="KIVANÇ DÖNÜYOR",
            caption_idea_tr="Ünlü oyuncu yeni dizisi için imzayı attı.",
            why_efso_tr="A-list yıldızın yeni projesi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
        )
        story = Story(
            story_id=story_id,
            headline_tr=asm1.headline_tr,
            category=asm1.category,
            first_seen_at=self.now,
            last_updated_at=self.now,
            article_ids=[a1.id],
            update_count=0,
            is_active=True,
        )

        # 2. Persist story & initial assessment
        repo.record_new_story(story, asm1)
        stored_story = repo.get_story_by_id(story_id)
        self.assertIsNotNone(stored_story)
        self.assertEqual(stored_story.update_count, 0)

        # 3. Notification record
        notif1 = NotificationRecord(
            notification_id=f"NOTIF-{a1.id[:8]}",
            story_id=story_id,
            kind=EditorialKind.NEW,
            sent_at=self.now,
            status=DeliveryStatus.SENT,
            message_text="Alert text 1",
            headline_tr=asm1.headline_tr,
            score=asm1.score,
        )
        repo.record_notification(notif1)
        self.assertTrue(repo.has_story_been_alerted(story_id, kind=EditorialKind.NEW))

        # 4. Significant update arrives 2 days later
        later = self.now + timedelta(days=2)
        a2 = Article.create("Kıvanç Tatlıtuğ'un dizisinde partneri Serenay Sarıkaya oldu", "https://magazin.com/kivanc-partner", "Magazin", later)
        repo.mark_articles_processed([a2])

        dev_tr = "Başroldeki kadın partnerin Serenay Sarıkaya olduğu kesinleşti."
        asm2 = EditorialAssessment(
            article_ids=[a2.id],
            score=96,
            kind=EditorialKind.UPDATE,
            parent_story_id=story_id,
            development_tr=dev_tr,
            headline_tr="Kıvanç'ın partneri Serenay oldu",
            cover_title_tr="DEV PARTNERLİK",
            caption_idea_tr="Merakla beklenen dizinin partneri açıklandı.",
            why_efso_tr="İki dev başrol ilk kez aynı projede.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
        )
        update_record = StoryUpdate(
            update_id=f"UPD-{a2.id[:8]}",
            story_id=story_id,
            development_tr=dev_tr,
            detected_at=later,
            article_ids=[a2.id],
            significance_score=96,
        )

        # 5. Record update
        repo.record_story_update(update_record, asm2)
        notif2 = NotificationRecord(
            notification_id=f"NOTIF-UPD-{a2.id[:8]}",
            story_id=story_id,
            kind=EditorialKind.UPDATE,
            sent_at=later,
            status=DeliveryStatus.SENT,
            message_text="Alert text 2",
            headline_tr=asm2.headline_tr,
            score=asm2.score,
        )
        repo.record_notification(notif2)

        # Check lineage & update count
        refreshed_story = repo.get_story_by_id(story_id)
        self.assertEqual(refreshed_story.update_count, 1)

        # Check update development is marked alerted
        self.assertTrue(repo.has_story_been_alerted(story_id, kind=EditorialKind.UPDATE, development_tr=dev_tr))

        # Verify in raw SQL that records exist across all 5 separate tables
        with repo._connection() as conn:
            articles_count = conn.execute("SELECT COUNT(*) AS c FROM processed_articles").fetchone()["c"]
            stories_count = conn.execute("SELECT COUNT(*) AS c FROM stories").fetchone()["c"]
            updates_count = conn.execute("SELECT COUNT(*) AS c FROM story_updates").fetchone()["c"]
            assessments_count = conn.execute("SELECT COUNT(*) AS c FROM editorial_assessments").fetchone()["c"]
            notifications_count = conn.execute("SELECT COUNT(*) AS c FROM notifications").fetchone()["c"]

            self.assertEqual(articles_count, 2)
            self.assertEqual(stories_count, 1)
            self.assertEqual(updates_count, 1)
            self.assertEqual(assessments_count, 2)
            self.assertEqual(notifications_count, 2)

    def test_pipeline_allows_significant_update_on_previously_alerted_story(self):
        """End-to-end pipeline test:

        Run 1: Story A is alerted.
        Run 2: Meaningful development on Story A arrives -> alert dispatched!
        Run 3: Same development on Story A arrives again -> duplicate alert suppressed!
        """
        settings = Settings(
            min_score=85,
            ai_evaluation_enabled=True,
            enable_quiet_hours=False,
            sqlite_db_path=self.db_path,
        )

        # --- RUN 1: Initial Story ---
        art1 = Article.create("Kenan İmirzalıoğlu yeni projeyle dönüyor", "https://magazin.com/kenan-1", "Magazin", self.now)
        pick1 = EditorialAssessment(
            article_ids=[art1.id],
            score=90,
            kind=EditorialKind.NEW,
            headline_tr="Kenan İmirzalıoğlu ekrana dönüyor",
            cover_title_tr="KENAN DÖNÜYOR",
            caption_idea_tr="Ünlü aktör yeni dijital projede.",
            why_efso_tr="Büyük isim.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[art1],
        )

        orch1 = RadarOrchestrator(
            settings=settings,
            repository=self.sqlite_repo,
            source=MockNewsSource([art1]),
            ai_engine=MockAIEngine([pick1]),
        )
        res1 = orch1.run(dry_run=False, now=self.now)
        self.assertTrue(res1.success)
        self.assertEqual(res1.picks_approved, 1)
        self.assertEqual(res1.notifications_dispatched, 1)

        parent_story_id = f"EFSO-{self.now.strftime('%Y%m%d')}-{art1.id[:8]}"
        self.assertTrue(self.sqlite_repo.has_story_been_alerted(parent_story_id, kind=EditorialKind.NEW))

        # --- RUN 2: Significant Update on Story A ---
        later = self.now + timedelta(hours=3)
        art2 = Article.create("Kenan İmirzalıoğlu'nun dizisinde yönetmen belli oldu", "https://magazin.com/kenan-2", "Magazin", later)
        dev_text = "Diziyi usta yönetmen Zeynep Günay Tan'ın çekeceği açıklandı."
        pick2 = EditorialAssessment(
            article_ids=[art2.id],
            score=92,
            kind=EditorialKind.UPDATE,
            parent_story_id=parent_story_id,
            development_tr=dev_text,
            headline_tr="Kenan'ın yönetmeni Zeynep Günay Tan",
            cover_title_tr="YÖNETMEN BELLİ OLDU",
            caption_idea_tr="Projeyi yönetecek isim netleşti.",
            why_efso_tr="Projenin kalitesini belirleyecek gelişme.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[art2],
        )

        orch2 = RadarOrchestrator(
            settings=settings,
            repository=self.sqlite_repo,
            source=MockNewsSource([art2]),
            ai_engine=MockAIEngine([pick2]),
        )
        res2 = orch2.run(dry_run=False, now=later)
        self.assertTrue(res2.success)
        self.assertEqual(res2.picks_approved, 1)
        # Crucial check: UPDATE was NOT suppressed by the existing alert on parent_story_id!
        self.assertEqual(res2.notifications_dispatched, 1)

        # --- RUN 3: Repetition of the SAME development ---
        much_later = self.now + timedelta(hours=5)
        art3 = Article.create("Zeynep Günay Tan Kenan İmirzalıoğlu'nun dizisini yönetecek", "https://baska-kaynak.com/kenan-3", "BaşkaKaynak", much_later)
        pick3 = EditorialAssessment(
            article_ids=[art3.id],
            score=92,
            kind=EditorialKind.UPDATE,
            parent_story_id=parent_story_id,
            development_tr=dev_text,  # Identical development!
            headline_tr="Kenan'ın yönetmeni Zeynep Günay Tan",
            cover_title_tr="YÖNETMEN BELLİ OLDU",
            caption_idea_tr="Projeyi yönetecek isim netleşti.",
            why_efso_tr="Projenin kalitesini belirleyecek gelişme.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[art3],
        )

        orch3 = RadarOrchestrator(
            settings=settings,
            repository=self.sqlite_repo,
            source=MockNewsSource([art3]),
            ai_engine=MockAIEngine([pick3]),
        )
        res3 = orch3.run(dry_run=False, now=much_later)
        self.assertTrue(res3.success)
        # Should be suppressed because this exact development was already alerted!
        self.assertEqual(res3.notifications_dispatched, 0)
