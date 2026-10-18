"""Tests for AI components: pricing, schemas, prompts, and business rule validation."""
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

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

        # gemini-3.1-flash-lite-preview was shut down on May 25, 2026; must NOT be treated as a valid fallback
        with self.assertRaises(ValueError) as ctx:
            estimate_call_cost("gemini-3.1-flash-lite-preview", 1_000_000, 1_000_000)
        self.assertIn("May 25, 2026", str(ctx.exception))

    def test_stable_gemini_3_1_flash_lite_distinguished_from_shutdown_preview(self):
        from efso_radar.ai.cost import OFFICIAL_PRICING
        # Stable gemini-3.1-flash-lite ($0.25 input / $1.50 output, scheduled shutdown May 7, 2027)
        cost = estimate_call_cost("gemini-3.1-flash-lite", 1_000_000, 1_000_000)
        self.assertAlmostEqual(cost, 1.75, places=4)
        meta = OFFICIAL_PRICING["gemini-3.1-flash-lite"]
        self.assertEqual(meta.shutdown_date, "May 7, 2027")
        self.assertEqual(meta.recommended_replacement, "gemini-3.5-flash-lite")

    def test_extract_provider_token_usage_handles_separate_thoughts_tokens(self):
        from types import SimpleNamespace
        from efso_radar.ai.cost import extract_provider_token_usage

        resp = SimpleNamespace(
            usage_metadata=SimpleNamespace(
                prompt_token_count=4200,
                candidates_token_count=480,
                thoughts_token_count=120,
                total_token_count=4800,
            )
        )
        in_tok, out_tok_total, from_meta = extract_provider_token_usage(
            resp, fallback_input_tokens=100, fallback_output_tokens=50
        )
        self.assertEqual(in_tok, 4200)
        # 480 candidate tokens + 120 thinking tokens = 600 total billed output tokens
        self.assertEqual(out_tok_total, 600)
        self.assertTrue(from_meta)

    def test_ledger_pre_reservation_and_failed_call_cost_accounting(self):
        """Failed or ambiguous API calls still record estimated cost in ai_usage_ledger without polluting cache."""
        import tempfile
        from pathlib import Path
        from unittest.mock import MagicMock
        from efso_radar.ai.client import AIEvaluationError
        from efso_radar.repositories.sqlite import SqliteStateRepository

        with tempfile.TemporaryDirectory() as tmp:
            repo = SqliteStateRepository(Path(tmp) / "ledger_test.db")
            settings = Settings(
                gemini_api_key="mock-test-key",
                gemini_model="gemini-3.5-flash-lite",
                max_estimated_run_cost_usd=0.05,
                max_monthly_ai_cost_usd=1.20,
                max_monthly_ai_calls=10,
            )
            engine = GeminiEditorialEngine(settings, repository=repo)
            engine.client = MagicMock()
            engine.client.models.generate_content.side_effect = RuntimeError("504 Gateway Timeout")
            art = Article.create("Test Haberi", "https://example.com/1", "Kaynak", self.now)

            with self.assertRaises(AIEvaluationError):
                engine.evaluate([art], [], self.now)

            # Verify cost was accounted in monthly usage summary
            usage = repo.get_monthly_ai_usage(now=self.now)
            self.assertEqual(int(usage["call_count"]), 1)
            self.assertGreater(float(usage["total_cost_usd"]), 0.0)

            # Verify failed call did NOT pollute batch cache (cached response must be None)
            with repo._connection() as conn:
                row = conn.execute("SELECT run_mode, response_json FROM ai_usage_ledger").fetchone()
                self.assertEqual(row["run_mode"], "FAILED_AMBIGUOUS")
                self.assertIsNone(row["response_json"])

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

    def test_compute_enforceable_batch_output_tokens(self):
        from efso_radar.ai.cost import compute_enforceable_batch_output_tokens

        # Batch of 8 stories with global config 3200 or 4096:
        # 100 base + 8 * 180 = 1540 tokens (never 3200 or 4096!)
        self.assertEqual(compute_enforceable_batch_output_tokens(8, 3200), 1540)
        self.assertEqual(compute_enforceable_batch_output_tokens(8, 4096), 1540)

        # Batch of 2 stories: 100 + 2 * 180 = 460 -> clamped to 500 minimum
        self.assertEqual(compute_enforceable_batch_output_tokens(2, 3200), 500)

        # 7 batches (6 of 8 stories + 1 of 2 stories) = 6 * 1540 + 500 = 9740 tokens (< 10,000!)
        total_7_batches = 6 * compute_enforceable_batch_output_tokens(8, 4096) + compute_enforceable_batch_output_tokens(2, 4096)
        self.assertEqual(total_7_batches, 9740)
        self.assertLessEqual(total_7_batches, 10000)

    def test_engine_passes_enforceable_batch_output_tokens_to_generate_content_config(self):
        """GeminiEditorialEngine.evaluate configures types.GenerateContentConfig with enforceable batch tokens."""
        from unittest.mock import MagicMock
        art1 = Article.create("Haber 1", "https://url.com/1", "Kaynak", self.now)
        art2 = Article.create("Haber 2", "https://url.com/2", "Kaynak", self.now)
        art3 = Article.create("Haber 3", "https://url.com/3", "Kaynak", self.now)

        settings = Settings(gemini_api_key="mock-key", max_output_tokens=4096)
        engine = GeminiEditorialEngine(settings)
        engine.client = MagicMock()

        # Mock successful empty pick batch
        mock_resp = MagicMock()
        mock_resp.text = '{"picks": []}'
        engine.client.models.generate_content.return_value = mock_resp

        engine.evaluate([art1, art2, art3], [], self.now)

        # Inspect the config passed to client.models.generate_content
        call_kwargs = engine.client.models.generate_content.call_args.kwargs
        config_passed = call_kwargs["config"]
        # For 3 articles: 100 + 3 * 180 = 640 tokens (strictly enforced, NOT 4096!)
        self.assertEqual(config_passed.max_output_tokens, 640)
        self.assertNotEqual(config_passed.max_output_tokens, 4096)
        if hasattr(config_passed, "thinking_config") and config_passed.thinking_config is not None:
            self.assertEqual(str(config_passed.thinking_config.thinking_level).upper(), "THINKINGLEVEL.MINIMAL")

    def test_sdk_level_request_controls_and_thinking_config(self):
        """Verifies explicit SDK request controls: attempts=1, automatic_function_calling disabled, and thinking_level='minimal'."""
        from efso_radar.ai.phase3c2b_eval import BlindShadowEvaluationRunner

        art1 = Article.create("Canary 1", "https://url.com/c1", "Kaynak", self.now)
        runner = BlindShadowEvaluationRunner(
            settings=Settings(gemini_model="gemini-3.5-flash-lite", max_shadow_eval_budget_usd=0.05),
            repository=None,
            batch_size=8,
        )
        mock_client = MagicMock()
        import json
        mock_resp = MagicMock()
        mock_resp.text = json.dumps({
            "decisions": [{
                "article_id": art1.id,
                "score": 85,
                "disposition": "DIGEST",
                "kind": "NEW",
                "urgency": "NORMAL",
                "rationale_tr": "Yüksek pop-kültür ilgisi.",
                "headline_tr": "Canary 1 Başlığı",
                "verification_status": "REPUTABLE_REPORTING",
            }],
            "run_editorial_summary_tr": "Canary özeti.",
        })
        mock_resp.candidates = [MagicMock(finish_reason="STOP")]
        mock_client.models.generate_content.return_value = mock_resp

        preds, metrics, report = runner.run_live_gemini_evaluation(
            items=[art1],
            mock_client=mock_client,
            max_batches=1,
            automatic_continuation=False,
        )

        call_kwargs = mock_client.models.generate_content.call_args.kwargs
        config = call_kwargs["config"]

        # 1. SDK-level HTTP retry disabled (attempts=1)
        self.assertIsNotNone(config.http_options)
        self.assertEqual(config.http_options.retry_options.attempts, 1)

        # 2. Automatic function calling disabled
        self.assertIsNotNone(config.automatic_function_calling)
        self.assertTrue(config.automatic_function_calling.disable)

        # 3. Documented supported thinking_level="minimal"
        self.assertIsNotNone(config.thinking_config)
        self.assertEqual(str(config.thinking_config.thinking_level).upper(), "THINKINGLEVEL.MINIMAL")

        # 4. Evidence report captures SDK controls
        self.assertIn("evidence", report)
        self.assertEqual(report["evidence"]["sdk_controls"]["http_retry_attempts"], 1)
        self.assertFalse(report["evidence"]["sdk_controls"]["automatic_continuation"])
        self.assertEqual(report["evidence"]["sdk_controls"]["thinking_level"], "minimal")


if __name__ == "__main__":
    unittest.main()
