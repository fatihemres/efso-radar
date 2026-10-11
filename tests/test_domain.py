"""Tests for core domain models."""
import unittest
from datetime import datetime, timezone

from efso_radar.domain import (
    Article,
    EditorialAssessment,
    EditorialKind,
    Market,
    NotificationRecord,
    DeliveryStatus,
    Story,
    StoryCategory,
    StoryUpdate,
    SuggestedFormat,
    compute_article_id,
    normalize_text,
)


class TestDomainModels(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)

    def test_normalize_text(self):
        self.assertEqual(normalize_text("  ŞOK   GELİŞME!  "), "şok gelişme")
        self.assertEqual(normalize_text("Hello, World! 123"), "hello world 123")
        self.assertEqual(normalize_text(""), "")

    def test_compute_article_id_deterministic(self):
        id1 = compute_article_id("https://example.com/news?ref=1", "Title A", "Source A")
        id2 = compute_article_id("https://example.com/news?ref=2", "Title A", "Source A")
        # Query parameters are stripped so same canonical base yields identical ID
        self.assertEqual(id1, id2)
        self.assertEqual(len(id1), 24)

    def test_article_creation(self):
        art = Article.create(
            title="Hadise yeni albümünü duyurdu",
            url="https://haber.com/hadise-album?ref=rss",
            source="Pop Medya",
            published_at=self.now,
            snippet="Şarkıcı Hadise stüdyodan paylaşım yaptı.",
            market=Market.TR,
        )
        self.assertEqual(art.title, "Hadise yeni albümünü duyurdu")
        self.assertEqual(art.market, Market.TR)
        self.assertTrue(art.id)

    def test_story_and_update_models(self):
        story = Story(
            story_id="STORY-101",
            headline_tr="Dua Lipa İstanbul konseri netleşti",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now,
            last_updated_at=self.now,
            article_ids=["art-1", "art-2"],
        )
        self.assertEqual(story.story_id, "STORY-101")
        self.assertEqual(story.update_count, 0)

        update = StoryUpdate(
            update_id="UPD-1",
            story_id="STORY-101",
            development_tr="Konser biletleri satışa çıktı ve 5 dakikada tükendi.",
            detected_at=self.now,
            article_ids=["art-3"],
            significance_score=94,
        )
        self.assertEqual(update.story_id, "STORY-101")
        self.assertEqual(update.significance_score, 94)

    def test_editorial_assessment_validation(self):
        pick = EditorialAssessment(
            article_ids=["art-1"],
            score=88,
            kind=EditorialKind.NEW,
            headline_tr="Sabrina Carpenter yeni single yayınladı",
            cover_title_tr="YENİ SINGLE GELDİ",
            caption_idea_tr="Sabrina Carpenter'dan sürpriz şarkı. Klip YouTube'da yayında.",
            why_efso_tr="Pop müziğin en popüler isimlerinden biri.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
        )
        self.assertEqual(pick.score, 88)
        self.assertEqual(pick.kind, EditorialKind.NEW)


if __name__ == "__main__":
    unittest.main()
