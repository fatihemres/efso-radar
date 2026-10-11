"""Adversarial deduplication and editorial regression tests for Turkish entertainment news.

Tests edge cases:
1. Same event reported with different wording (must cluster together).
2. Different events involving the same celebrity (must NOT cluster together).
3. Breakup followed by reconciliation (must be treated as distinct/update, not duplicate).
4. Rumor followed by official confirmation (concrete development).
5. Same announcement republished days later (must be rejected as duplicate).
6. Significant new development on previously alerted story (accepted as UPDATE).
"""
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from efso_radar.ai.client import GeminiEditorialEngine, is_headline_too_similar
from efso_radar.ai.schemas import EditorialPickSchema
from efso_radar.config import Settings
from efso_radar.deduplication.engine import DeduplicationEngine
from efso_radar.domain import Article, EditorialKind, Story, StoryCategory
from efso_radar.repositories.sqlite import SqliteStateRepository


class TestAdversarialTurkishDeduplication(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "adv_test.db"
        self.repo = SqliteStateRepository(self.db_path)
        self.engine = DeduplicationEngine(self.repo)
        self.ai_engine = GeminiEditorialEngine(Settings(min_score=82))
        self.now = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_case_1_same_event_different_wording(self):
        """Same event reported with different phrasing by two publishers should cluster together."""
        art1 = Article.create(
            title="Hadise sevgilisinden ayrıldı",
            url="https://haber.com/hadise-ayrildi",
            source="Magazin Gazetesi",
            published_at=self.now,
            snippet="Ünlü şarkıcı Hadise sevgilisiyle yollarını ayırdı.",
        )
        art2 = Article.create(
            title="Ünlü şarkıcı Hadise ile sevgilisi ayrılık kararı aldı",
            url="https://baskahaber.com/hadise-ayrilik-karari",
            source="Pop Medya",
            published_at=self.now - timedelta(minutes=10),
            snippet="Hadise ve sevgilisi ilişkilerini noktaladı.",
        )

        candidates = self.engine.filter_fresh_candidates([art1, art2])
        # Must collapse into a single candidate cluster
        self.assertEqual(len(candidates), 1)
        cluster_ids = candidates[0].raw_metadata.get("cluster_article_ids", [])
        self.assertEqual(len(cluster_ids), 2)

    def test_case_2_different_events_same_celebrity(self):
        """Different events involving the same celebrity must NOT be clustered together."""
        album_news = Article.create(
            title="Hadise yeni albümünü duyurdu",
            url="https://haber.com/hadise-album",
            source="Müzik Dünyası",
            published_at=self.now,
            snippet="Hadise yeni şarkılarının yer aldığı albümü yakında çıkaracağını açıkladı.",
        )
        breakup_news = Article.create(
            title="Hadise sevgilisinden ayrıldı",
            url="https://haber.com/hadise-ayrildi",
            source="Magazin Masası",
            published_at=self.now,
            snippet="Hadise ve sevgilisi ayrılık kararı aldı.",
        )

        candidates = self.engine.filter_fresh_candidates([album_news, breakup_news])
        # Must remain TWO separate candidate stories
        self.assertEqual(len(candidates), 2)

    def test_case_3_breakup_followed_by_reconciliation(self):
        """Reconciliation after a reported breakup must NOT be dropped as a duplicate."""
        past_story = Story(
            story_id="STORY-HAD-1",
            headline_tr="Hadise sevgilisinden ayrıldı",
            category=StoryCategory.YERLI,
            first_seen_at=self.now - timedelta(days=5),
            last_updated_at=self.now - timedelta(days=5),
        )

        new_headline = "Barıştılar: Hadise ve sevgilisi el ele görüntülendi"
        # Semantically and lexically different; must not be considered too similar to the breakup
        self.assertFalse(is_headline_too_similar(new_headline, past_story.headline_tr))

    def test_case_4_rumor_followed_by_official_confirmation(self):
        """Official confirmation following a previous rumor should qualify as an UPDATE."""
        past_story = Story(
            story_id="STORY-EVL-1",
            headline_tr="İddia: Ünlü çift evlilik kararı aldı",
            category=StoryCategory.YERLI,
            first_seen_at=self.now - timedelta(days=2),
            last_updated_at=self.now - timedelta(days=2),
        )

        confirm_art = Article.create(
            title="Resmî açıklama: Ünlü çift dün akşam Roma'da evlendi",
            url="https://haber.com/roma-nikah",
            source="Resmî Bülten",
            published_at=self.now,
        )

        pick = EditorialPickSchema(
            article_ids=[confirm_art.id],
            score=93,
            kind="UPDATE",
            parent_story_id="STORY-EVL-1",
            development_tr="İddialar doğrulandı: Çift dün akşam Roma'da aile arasında kıyılan nikahla resmen evlendi.",
            headline_tr="Ünlü çift Roma'da resmen evlendi",
            cover_title_tr="RESMEN EVLENDİLER",
            caption_idea_tr="İddialar gerçek oldu. Çift Roma'da nikah masasına oturdu.",
            why_efso_tr="Önceki dedikoduların resmî sonuca bağlanması.",
            suggested_format="POST",
            category="YERLI",
        )

        approved = self.ai_engine._validate_and_transform_picks(
            [pick], [confirm_art], [past_story]
        )
        self.assertEqual(len(approved), 1)
        self.assertEqual(approved[0].kind, EditorialKind.UPDATE)
        self.assertEqual(approved[0].parent_story_id, "STORY-EVL-1")

    def test_case_5_same_announcement_republished_later(self):
        """The same announcement republished days later with slight variation should be rejected."""
        past_story = Story(
            story_id="STORY-DUA-1",
            headline_tr="Dua Lipa yeni dünya turnesini duyurdu",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now - timedelta(days=3),
            last_updated_at=self.now - timedelta(days=3),
        )

        candidate = Article.create(
            title="Dua Lipa dünya turnesini açıkladı",
            url="https://music.com/dua-tour-repost",
            source="Pop Blog",
            published_at=self.now,
        )

        # Re-evaluating the same announcement as a NEW story must be caught by duplicate detector
        pick = EditorialPickSchema(
            article_ids=[candidate.id],
            score=89,
            kind="NEW",
            headline_tr="Dua Lipa yeni dünya turnesini duyurdu",  # Duplicate of past story
            cover_title_tr="DÜNYA TURNESİ DUYURULDU",
            caption_idea_tr="Dua Lipa turneye çıkıyor.",
            why_efso_tr="Pop yıldızı turnesi.",
            suggested_format="POST",
            category="GLOBAL",
        )

        approved = self.ai_engine._validate_and_transform_picks(
            [pick], [candidate], [past_story]
        )
        self.assertEqual(len(approved), 0)

    def test_case_6_significant_development_justifies_update(self):
        """Meaningful new development (e.g. tour dates in Istanbul) is approved as an UPDATE."""
        past_story = Story(
            story_id="STORY-DUA-1",
            headline_tr="Dua Lipa yeni dünya turnesini duyurdu",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now - timedelta(days=3),
            last_updated_at=self.now - timedelta(days=3),
        )

        candidate = Article.create(
            title="Dua Lipa turne takvimine İstanbul'u da ekledi",
            url="https://music.com/dua-istanbul",
            source="Konser Ajansı",
            published_at=self.now,
        )

        pick = EditorialPickSchema(
            article_ids=[candidate.id],
            score=95,
            kind="UPDATE",
            parent_story_id="STORY-DUA-1",
            development_tr="Turne takvimine resmen İstanbul konseri eklendi; tarih 15 Temmuz.",
            headline_tr="Dua Lipa turnesine İstanbul'u ekledi",
            cover_title_tr="İSTANBUL'A GELİYOR",
            caption_idea_tr="Dua Lipa dünya turnesi kapsamında İstanbul'da sahne alacak.",
            why_efso_tr="Türkiye'deki pop müzik kitlesi için dev gelişme.",
            suggested_format="POST",
            category="GLOBAL",
        )

        approved = self.ai_engine._validate_and_transform_picks(
            [pick], [candidate], [past_story]
        )
        self.assertEqual(len(approved), 1)
        self.assertEqual(approved[0].kind, EditorialKind.UPDATE)
        self.assertIn("İstanbul", approved[0].development_tr)


if __name__ == "__main__":
    unittest.main()
