"""Tests for AI components: pricing, schemas, prompts, and business rule validation."""
import unittest
from datetime import datetime, timezone

from pydantic import ValidationError

from efso_radar.ai.client import GeminiEditorialEngine, is_headline_too_similar
from efso_radar.ai.cost import estimate_call_cost, estimate_tokens_from_text
from efso_radar.ai.prompts import build_editorial_prompt
from efso_radar.ai.schemas import EditorialDecisionBatch, EditorialPickSchema
from efso_radar.config import Settings
from efso_radar.domain import Article, EditorialKind, Market, Story, StoryCategory, SuggestedFormat


class TestAIComponents(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)
        self.settings = Settings(min_score=82, max_per_run=3)
        self.engine = GeminiEditorialEngine(self.settings)

    def test_cost_calculation_gemini_3_5_flash_lite(self):
        # 1M input ($0.30) + 1M output ($2.50) = $2.80
        cost = estimate_call_cost("gemini-3.5-flash-lite", 1_000_000, 1_000_000)
        self.assertAlmostEqual(cost, 2.80, places=4)

        # 5,000 input tokens + 500 output tokens + 200 thinking tokens
        call_with_thinking = estimate_call_cost("gemini-3.5-flash-lite", 5_000, 500, thinking_tokens=200)
        # input: 5000/1M * 0.30 = 0.0015; output: 700/1M * 2.50 = 0.00175 -> total 0.00325
        self.assertAlmostEqual(call_with_thinking, 0.00325, places=5)

    def test_cost_calculation_gemini_3_8_flash(self):
        # 1M input ($0.75) + 1M output ($3.75) = $4.50
        cost = estimate_call_cost("gemini-3.8-flash", 1_000_000, 1_000_000)
        self.assertAlmostEqual(cost, 4.50, places=4)

    def test_decommissioned_model_rejected(self):
        # gemini-2.0-flash-lite was shut down on June 1, 2026; must reject with explicit ValueError
        with self.assertRaises(ValueError):
            estimate_call_cost("gemini-2.0-flash-lite", 1_000_000, 1_000_000)

    def test_search_grounding_cost(self):
        cost_with_search = estimate_call_cost("gemini-3.5-flash-lite", 10_000, 1_000, used_search_grounding=True)
        self.assertGreater(cost_with_search, 0.035)


    def test_pydantic_schema_validation(self):
        valid_json = """
        {
            "picks": [
                {
                    "article_ids": ["art1"],
                    "score": 90,
                    "kind": "NEW",
                    "parent_story_id": "",
                    "development": "",
                    "headline_tr": "Hadise yeni albümünü duyurdu",
                    "cover_title_tr": "YENİ ALBÜM GELİYOR",
                    "caption_idea_tr": "Pop yıldızı yeni albümünü resmen duyurdu.",
                    "why_efso_tr": "Büyük pop kültürü gündemi.",
                    "suggested_format": "POST",
                    "category": "YERLI"
                }
            ]
        }
        """
        batch = EditorialDecisionBatch.model_validate_json(valid_json)
        self.assertEqual(len(batch.picks), 1)
        self.assertEqual(batch.picks[0].score, 90)

    def test_pydantic_schema_rejects_invalid_score(self):
        with self.assertRaises(ValidationError):
            EditorialPickSchema(
                article_ids=["art1"],
                score=150,  # Invalid: score > 100
                kind="NEW",
                headline_tr="Başlık",
                cover_title_tr="Kapak",
                caption_idea_tr="Caption",
                why_efso_tr="Neden",
                suggested_format="POST",
                category="GLOBAL",
            )

    def test_business_rule_rejects_hallucinated_ids(self):
        candidate = Article.create("Gerçek Haber", "https://url.com/1", "Kaynak", self.now)
        pick = EditorialPickSchema(
            article_ids=["hallucinated_id_999"],  # Not in candidates!
            score=95,
            kind="NEW",
            headline_tr="Harika bir haber başlığı",
            cover_title_tr="KAPAK BAŞLIĞI",
            caption_idea_tr="Caption fikri yazıldı.",
            why_efso_tr="Efso için önemli.",
            suggested_format="POST",
            category="GLOBAL",
        )
        approved = self.engine._validate_and_transform_picks([pick], [candidate], [])
        self.assertEqual(len(approved), 0)

    def test_business_rule_rejects_low_score(self):
        candidate = Article.create("Gerçek Haber", "https://url.com/1", "Kaynak", self.now)
        pick = EditorialPickSchema(
            article_ids=[candidate.id],
            score=75,  # Below min_score (82)
            kind="NEW",
            headline_tr="Harika bir haber başlığı",
            cover_title_tr="KAPAK BAŞLIĞI",
            caption_idea_tr="Caption fikri yazıldı.",
            why_efso_tr="Efso için önemli.",
            suggested_format="POST",
            category="GLOBAL",
        )
        approved = self.engine._validate_and_transform_picks([pick], [candidate], [])
        self.assertEqual(len(approved), 0)

    def test_business_rule_rejects_duplicate_headline(self):
        candidate = Article.create("Gerçek Haber", "https://url.com/1", "Kaynak", self.now)
        existing_story = Story(
            story_id="STORY-1",
            headline_tr="Yeni albüm duyuruldu",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now,
            last_updated_at=self.now,
        )
        pick = EditorialPickSchema(
            article_ids=[candidate.id],
            score=90,
            kind="NEW",
            headline_tr="Yeni albüm duyuruldu",  # Duplicate!
            cover_title_tr="KAPAK BAŞLIĞI",
            caption_idea_tr="Caption fikri yazıldı.",
            why_efso_tr="Efso için önemli.",
            suggested_format="POST",
            category="GLOBAL",
        )
        approved = self.engine._validate_and_transform_picks([pick], [candidate], [existing_story])
        self.assertEqual(len(approved), 0)

    def test_business_rule_accepts_valid_update(self):
        candidate = Article.create("Gelişme Haberi", "https://url.com/update", "Kaynak", self.now)
        existing_story = Story(
            story_id="STORY-1",
            headline_tr="Yeni albüm duyuruldu",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now,
            last_updated_at=self.now,
        )
        pick = EditorialPickSchema(
            article_ids=[candidate.id],
            score=92,
            kind="UPDATE",
            parent_story_id="STORY-1",
            development_tr="Resmî albüm çıkış tarihi 20 Kasım olarak netleşti.",
            headline_tr="Albüm çıkış tarihi açıklandı",
            cover_title_tr="TARİH BELLİ OLDU",
            caption_idea_tr="Albümün çıkış tarihi resmen netleşti.",
            why_efso_tr="Albüm serüveninde kritik gelişme.",
            suggested_format="POST",
            category="GLOBAL",
        )
        approved = self.engine._validate_and_transform_picks([pick], [candidate], [existing_story])
        self.assertEqual(len(approved), 1)
        self.assertEqual(approved[0].kind, EditorialKind.UPDATE)
        self.assertEqual(approved[0].parent_story_id, "STORY-1")


if __name__ == "__main__":
    unittest.main()
