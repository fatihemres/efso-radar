"""Comprehensive unit and integration tests for Efso Radar Phase 3B Editorial Intelligence.

Covers:
- Editorial pick validation and transformation (Pydantic schema to domain model)
- Quality gating: High score acceptance vs low-score silence
- Anti-hallucination gate: Rejection of fabricated candidate IDs
- Anti-duplicate gate: Cross-story deduplication against recent history
- Update intelligence: Validating material new development vs headline repeats
- Factual verification confidence: Rumor cautions and attribution tracking
- Outage resilience & backlog integrity: Preserving unprocessed candidates on AI failure
- Budget control: Enforcing per-run cost thresholds
- Human editorial feedback: Recording and querying calibration labels
- Offline benchmark evaluation: Full corpus metric execution
"""
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from efso_radar.ai.benchmark import OfflineBenchmarkEvaluator
from efso_radar.ai.client import (
    AIEvaluationError,
    GeminiEditorialEngine,
    is_headline_too_similar,
)
from efso_radar.ai.schemas import EditorialDecisionBatch, EditorialPickSchema
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    Market,
    NotificationRecord,
    OwnerFeedback,
    OwnerFeedbackRating,
    Story,
    StoryCategory,
    SuggestedFormat,
    VerificationStatus,
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


class MockFailingAIEngine:
    def evaluate(self, candidates, recent_stories, now):
        raise AIEvaluationError("Mock Gemini API outage 503 Service Unavailable")


class MockSilentNotifier(NotificationChannel):
    def __init__(self):
        self.sent_records = []

    def send(self, assessment, story_id, dry_run=False, now=None):
        rec = NotificationRecord(
            notification_id=f"MOCK-{len(self.sent_records)}",
            story_id=story_id,
            kind=assessment.kind,
            channel="TELEGRAM",
            sent_at=now or utc_now(),
            status=DeliveryStatus.SENT,
            message_text="Mock text",
            headline_tr=assessment.headline_tr,
            score=assessment.score,
        )
        self.sent_records.append(rec)
        return rec


class TestEditorialIntelligence(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_editorial.db"
        self.settings = Settings(
            sqlite_db_path=self.db_path,
            ai_evaluation_enabled=True,
            min_score=82,
            max_per_run=3,
            max_estimated_run_cost_usd=0.05,
        )
        self.repo = SqliteStateRepository(self.db_path)
        self.engine = GeminiEditorialEngine(self.settings)
        self.now = datetime(2026, 10, 11, 14, 0, tzinfo=timezone.utc)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _sample_article(self, aid="art1", title="Zendaya ve Tom Holland evlendi") -> Article:
        return Article.create(
            title=title,
            url=f"https://source.com/{aid}",
            source="Magazin Gazetesi",
            published_at=self.now,
            snippet="Detaylı magazin haberi özeti.",
            raw_metadata={"id": aid},
        )

    def test_high_scoring_editorial_approval_and_field_transformation(self):
        """High scoring editorial pick is correctly transformed into an EditorialAssessment domain object."""
        art = self._sample_article()
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=91,
            kind="NEW",
            verification_status="FIRST_PARTY_STATEMENT",
            headline_tr="Zendaya ve Tom Holland evlendi",
            cover_title_tr="EVLENDİLER",
            caption_idea_tr="Ünlü çift İtalya'da sade bir törenle dünyaevine girdi.",
            relevance_reason_tr="Küresel A-list çiftin evlilik haberi @efsomagazin kitlesi için yüksek etkileşimlidir.",
            why_efso_tr="Genç popüler kültür takipçilerinin en sevdiği çift.",
            uncertainties_tr="",
            suggested_format="POST",
            category="GLOBAL",
        )

        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        assessment = results[0]
        self.assertEqual(assessment.score, 91)
        self.assertEqual(assessment.kind, EditorialKind.NEW)
        self.assertEqual(assessment.verification_status, VerificationStatus.FIRST_PARTY_STATEMENT)
        self.assertEqual(assessment.headline_tr, "Zendaya ve Tom Holland evlendi")
        self.assertEqual(assessment.cover_title_tr, "EVLENDİLER")
        self.assertEqual(assessment.category, StoryCategory.GLOBAL)
        self.assertEqual(len(assessment.supporting_articles), 1)
        self.assertEqual(assessment.supporting_articles[0].id, art.id)

    def test_editorial_silence_when_no_strong_stories(self):
        """Low scoring picks (<82) or IGNORE picks result in silence (empty list)."""
        art = self._sample_article()
        low_score_pick = EditorialPickSchema(
            article_ids=[art.id],
            score=74,
            kind="NEW",
            headline_tr="Sıradan magazin haberi",
            cover_title_tr="BAŞLIK",
            caption_idea_tr="Açıklama",
            why_efso_tr="Sebep",
            suggested_format="POST",
            category="GLOBAL",
        )
        ignore_pick = EditorialPickSchema(
            article_ids=[art.id],
            score=85,
            kind="IGNORE",
            headline_tr="B2B Şirket birleşmesi",
            cover_title_tr="B2B BAŞLIK",
            caption_idea_tr="Açıklama",
            why_efso_tr="Sebep",
            suggested_format="POST",
            category="GLOBAL",
        )

        # Both picks should be rejected by business rules
        results = self.engine._validate_and_transform_picks([low_score_pick, ignore_pick], [art], [])
        self.assertEqual(results, [])

    def test_anti_hallucination_rejects_hallucinated_candidate_ids(self):
        """Model output citing non-existent candidate IDs must be rejected immediately."""
        art = self._sample_article("real_art")
        hallucinated_pick = EditorialPickSchema(
            article_ids=["fake_hallucinated_id_999"],
            score=95,
            kind="NEW",
            headline_tr="Uydurma magazin haberi",
            cover_title_tr="UYDURMA",
            caption_idea_tr="Açıklama",
            why_efso_tr="Sebep",
            suggested_format="POST",
            category="GLOBAL",
        )

        results = self.engine._validate_and_transform_picks([hallucinated_pick], [art], [])
        self.assertEqual(results, [])

    def test_anti_duplicate_gate_rejects_overly_similar_new_headline(self):
        """A NEW pick whose headline matches an already alerted story must be blocked as duplicate."""
        art = self._sample_article()
        existing_story = Story(
            story_id="STORY-EXISTING-01",
            headline_tr="Hadise ile Mehmet Dinçerler ayrılık kararı aldı",
            category=StoryCategory.YERLI,
            first_seen_at=self.now - timedelta(days=1),
            last_updated_at=self.now - timedelta(days=1),
        )
        repeat_pick = EditorialPickSchema(
            article_ids=[art.id],
            score=88,
            kind="NEW",
            headline_tr="Hadise ve Mehmet Dinçerler ayrılık kararı aldı",
            cover_title_tr="AYRILDILAR",
            caption_idea_tr="Detaylar burada.",
            why_efso_tr="Pop müzik gündemi.",
            suggested_format="POST",
            category="YERLI",
        )

        results = self.engine._validate_and_transform_picks([repeat_pick], [art], [existing_story])
        self.assertEqual(results, [])

    def test_update_requires_existing_story_and_material_development(self):
        """An UPDATE pick without a matching parent story or with trivial development is rejected."""
        art = self._sample_article()

        # Case A: Non-existent parent_story_id
        pick_orphan = EditorialPickSchema(
            article_ids=[art.id],
            score=90,
            kind="UPDATE",
            parent_story_id="STORY-NONEXISTENT",
            development_tr="Resmî mahkeme duruşma tarihi açıklandı.",
            headline_tr="Duruşma tarihi belli oldu",
            cover_title_tr="TARİH BELLİ OLDU",
            caption_idea_tr="Duruşma günü açıklandı.",
            why_efso_tr="Kritik gelişme.",
            suggested_format="POST",
            category="GLOBAL",
        )
        self.assertEqual(self.engine._validate_and_transform_picks([pick_orphan], [art], []), [])

        # Case B: Trivial development (too short or empty)
        existing_story = Story(
            story_id="STORY-PARENT-01",
            headline_tr="Ünlü şarkıcı hakkında soruşturma başlatıldı",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now,
            last_updated_at=self.now,
        )
        pick_trivial = EditorialPickSchema(
            article_ids=[art.id],
            score=90,
            kind="UPDATE",
            parent_story_id="STORY-PARENT-01",
            development_tr="Gelişme oldu.",  # < 10 characters
            headline_tr="Soruşturma devam ediyor",
            cover_title_tr="DEVAM EDİYOR",
            caption_idea_tr="Açıklama",
            why_efso_tr="Sebep",
            suggested_format="POST",
            category="GLOBAL",
        )
        self.assertEqual(
            self.engine._validate_and_transform_picks([pick_trivial], [art], [existing_story]), []
        )

    def test_update_rejected_if_development_repeats_parent_headline(self):
        """An UPDATE pick where development simply echoes the parent headline is rejected."""
        art = self._sample_article()
        existing_story = Story(
            story_id="STORY-PARENT-02",
            headline_tr="The Weeknd yeni albümünü ve dünya turnesini duyurdu",
            category=StoryCategory.GLOBAL,
            first_seen_at=self.now,
            last_updated_at=self.now,
        )
        pick_repeat = EditorialPickSchema(
            article_ids=[art.id],
            score=89,
            kind="UPDATE",
            parent_story_id="STORY-PARENT-02",
            development_tr="The Weeknd yeni albümünü ve turnesini duyurdu.",  # Repeats parent
            headline_tr="Albüm ve turne duyurusu",
            cover_title_tr="DUYURU",
            caption_idea_tr="Açıklama",
            why_efso_tr="Sebep",
            suggested_format="POST",
            category="GLOBAL",
        )
        results = self.engine._validate_and_transform_picks([pick_repeat], [art], [existing_story])
        self.assertEqual(results, [])

    def test_unverified_rumor_attribution_and_cautions(self):
        """Unverified rumors carry explicit cautious verification tags."""
        art = self._sample_article()
        rumor_pick = EditorialPickSchema(
            article_ids=[art.id],
            score=84,
            kind="NEW",
            verification_status="UNVERIFIED_RUMOR",
            headline_tr="Ünlü oyuncunun gizli nikah kıydığı iddia edildi",
            cover_title_tr="NİKAH İDDİASI",
            caption_idea_tr="Magazin kulislerinde konuşulan iddialara göre...",
            relevance_reason_tr="Viral dedikodu",
            why_efso_tr="Magazin ilgisi yüksek",
            uncertainties_tr="İddia taraflarca henüz teyit edilmemiştir.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([rumor_pick], [art], [])
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].verification_status, VerificationStatus.UNVERIFIED_RUMOR)
        self.assertIn("iddia", results[0].headline_tr.lower())
        self.assertTrue(results[0].uncertainties_tr)

    def test_cost_budget_exceeded_raises_ai_evaluation_error(self):
        """Exceeding configured max_estimated_run_cost_usd aborts via AIEvaluationError."""
        strict_settings = Settings(
            max_estimated_run_cost_usd=0.001,  # minimum valid budget setting
            enable_search_grounding=True,      # search grounding adds ~$0.035, exceeding $0.001
            gemini_api_key="mock_key",
        )
        engine = GeminiEditorialEngine(strict_settings)
        art = self._sample_article()

        with self.assertRaises(AIEvaluationError):
            engine.evaluate([art], [], self.now)

    def test_orchestrator_preserves_backlog_on_ai_failure(self):
        """When AI evaluation fails, the run reports failure and candidate backlog is NOT marked processed."""
        art1 = self._sample_article("art_backlog_1")
        art2 = self._sample_article("art_backlog_2")
        source = MockNewsSource(articles=[art1, art2], successes=1)

        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=source,
            ai_engine=MockFailingAIEngine(),
            notifier=MockSilentNotifier(),
        )

        result = orch.run()
        self.assertFalse(result.success)
        self.assertIn("AI editorial evaluation aborted", result.message)

        # CRITICAL BACKLOG INTEGRITY CHECK: Articles must NOT be marked processed
        self.assertFalse(self.repo.is_article_processed(art1.id))
        self.assertFalse(self.repo.is_article_processed(art2.id))

    def test_owner_feedback_storage_and_query_parity(self):
        """Human owner feedback can be stored and retrieved for offline calibration."""
        feedback1 = OwnerFeedback(
            feedback_id="FB-001",
            story_id="STORY-TEST-01",
            article_id="art1",
            rating=OwnerFeedbackRating.APPROVE,
            notes="Harika bir editoryal seçim, kitle çok sevdi.",
            created_at=self.now,
        )
        feedback2 = OwnerFeedback(
            feedback_id="FB-002",
            story_id="STORY-TEST-02",
            article_id="art2",
            rating=OwnerFeedbackRating.TOO_WEAK,
            notes="Haber değeri 82 barajının altında kalmalıydı.",
            created_at=self.now + timedelta(minutes=5),
        )

        self.repo.record_owner_feedback(feedback1)
        self.repo.record_owner_feedback(feedback2)

        retrieved = self.repo.get_owner_feedback()
        self.assertEqual(len(retrieved), 2)
        # Ordered newest first
        self.assertEqual(retrieved[0].feedback_id, "FB-002")
        self.assertEqual(retrieved[0].rating, OwnerFeedbackRating.TOO_WEAK)
        self.assertEqual(retrieved[1].feedback_id, "FB-001")
        self.assertEqual(retrieved[1].rating, OwnerFeedbackRating.APPROVE)

    def test_offline_benchmark_metrics_accuracy(self):
        """Offline benchmark evaluator achieves expected precision and recall across 42 articles."""
        evaluator = OfflineBenchmarkEvaluator()
        metrics = evaluator.run_evaluation()

        self.assertEqual(metrics.total_articles, 42)
        self.assertEqual(metrics.true_positives, 22)
        self.assertEqual(metrics.true_negatives, 20)
        self.assertEqual(metrics.false_positives, 0)
        self.assertEqual(metrics.false_negatives, 0)
        self.assertAlmostEqual(metrics.precision, 1.0, places=2)
        self.assertAlmostEqual(metrics.recall, 1.0, places=2)
        self.assertAlmostEqual(metrics.f1_score, 1.0, places=2)
        self.assertAlmostEqual(metrics.duplicate_accuracy, 1.0, places=2)
        self.assertAlmostEqual(metrics.update_recognition_rate, 1.0, places=2)

    def test_backlog_recovery_when_article_drops_off_rss(self):
        """Articles deprioritized in run 1 survive in backlog even if dropped from RSS in run 2."""
        titles = [
            "Tarkan yeni pop albümünü çıkardı",
            "Hadise Roma'da dünyaevine girdi",
            "Zendaya ve Tom Holland nişanlandı",
            "Serenay Sarıkaya İtalya tatilinde",
            "Kylie Jenner Paris Moda Haftası'nda",
            "Danla Bilic yeni ilişkisini duyurdu",
        ]
        articles = [
            self._sample_article(f"art_test_{i}", title=titles[i])
            for i in range(len(titles))
        ]
        # 6th article will be deprioritized when max_candidates=5
        art_deprioritized = articles[-1]

        # Configure max_candidates=5 (valid setting >= 5) to force 6th article into backlog
        orch_settings = Settings(
            sqlite_db_path=self.db_path,
            ai_evaluation_enabled=True,
            max_candidates=5,
            min_score=82,
        )

        # Run 1: Source returns all 6 articles; 5 are evaluated, 1 is saved into backlog
        orch1 = RadarOrchestrator(
            settings=orch_settings,
            repository=self.repo,
            source=MockNewsSource(articles=articles, successes=1),
            ai_engine=GeminiEditorialEngine(orch_settings),
            notifier=MockSilentNotifier(),
        )
        res1 = orch1.run(now=self.now)
        self.assertTrue(res1.success)
        self.assertEqual(res1.candidates_evaluated, 5)

        # Confirm the deprioritized article was NOT marked processed, but IS persisted in pending backlog
        self.assertFalse(self.repo.is_article_processed(art_deprioritized.id))
        pending = self.repo.get_pending_candidates(now=self.now)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].id, art_deprioritized.id)

        # Run 2: Publisher RSS feed drops the article! (MockNewsSource returns empty list)
        orch2 = RadarOrchestrator(
            settings=orch_settings,
            repository=self.repo,
            source=MockNewsSource(articles=[], successes=1),
            ai_engine=GeminiEditorialEngine(orch_settings),
            notifier=MockSilentNotifier(),
        )
        res2 = orch2.run(now=self.now)
        self.assertTrue(res2.success)
        # art_deprioritized was recovered from candidate_backlog and evaluated in run 2!
        self.assertEqual(res2.candidates_evaluated, 1)
        self.assertTrue(self.repo.is_article_processed(art_deprioritized.id))

    def test_factual_grounding_downgrades_hallucinated_official_confirmation_on_rumor(self):
        """Model claiming OFFICIAL_CONFIRMATION on a source text stating only 'iddia edildi' is downgraded."""
        art = Article.create(
            title="İddia: Ünlü çiftin gizlice evlendiği öne sürüldü",
            url="https://source.com/rumor",
            source="Dedikodu Sitesi",
            published_at=self.now,
            snippet="Sosyal medyada yayılan iddialara göre çiftin evlendiği öne sürüldü.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=88,
            kind="NEW",
            verification_status="OFFICIAL_CONFIRMATION",  # Hallucinated status
            headline_tr="Ünlü çiftin evlendiği iddia edildi",
            cover_title_tr="EVLİLİK İDDİASI",
            caption_idea_tr="İddialar gündeme bomba gibi düştü.",
            why_efso_tr="Viral magazin iddiası.",
            uncertainties_tr="",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        # Must be downgraded from OFFICIAL_CONFIRMATION to ATTRIBUTED_CLAIM
        self.assertEqual(results[0].verification_status, VerificationStatus.ATTRIBUTED_CLAIM)
        self.assertIn("resmi doğrulama bulunmuyor", results[0].uncertainties_tr)

    def test_factual_grounding_rejects_unverified_sensitive_allegations(self):
        """Unverified rumors touching sensitive topics (death, crime, assault) must be rejected."""
        art = Article.create(
            title="Şok İddia: Ünlü oyuncu gözaltına alındı",
            url="https://source.com/crime-rumor",
            source="Tabloid Blog",
            published_at=self.now,
            snippet="İddiaya göre ünlü oyuncu gece kulübü çıkışında gözaltına alındı.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=89,
            kind="NEW",
            verification_status="UNVERIFIED_RUMOR",
            headline_tr="Ünlü oyuncunun gözaltına alındığı iddia edildi",
            cover_title_tr="GÖZALTI İDDİASI",
            caption_idea_tr="Oyuncu hakkında şok iddia.",
            why_efso_tr="Adli gündem.",
            uncertainties_tr="Teyit yok.",
            suggested_format="POST",
            category="YERLI",
        )
        # Sensitive topic (gözaltı) on UNVERIFIED_RUMOR must be rejected
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(results, [])

    def test_factual_grounding_flags_unsupported_direct_quotes(self):
        """Unsupported direct quotes in headlines or developments must be flagged in uncertainties."""
        art = Article.create(
            title="Serenay Sarıkaya yeni projesini anlattı",
            url="https://source.com/interview",
            source="Kelebek",
            published_at=self.now,
            snippet="Ünlü oyuncu yeni bir tiyatro oyununa hazırlandığını açıkladı.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=88,
            kind="NEW",
            verification_status="REPUTABLE_REPORTING",
            headline_tr="Serenay Sarıkaya: 'Bu rol hayatımı tamamen değiştirecek'",  # Fabricated quote
            cover_title_tr="AÇIKLAMA GELDİ",
            caption_idea_tr="Oyuncunun tiyatro heyecanı.",
            why_efso_tr="Tiyatro projesi.",
            uncertainties_tr="",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        self.assertIn("Tırnak içi alıntı", results[0].uncertainties_tr)

    def test_semantic_matching_distinguishes_different_events_same_celebrity(self):
        """Headlines with divergent event concepts for the same celebrity are NOT treated as duplicates."""
        # Case A: Breakup vs Reconciliation
        self.assertFalse(is_headline_too_similar(
            "Kerem Bürsin sevgilisinden ayrıldı",
            "Kerem Bürsin sevgilisiyle barıştı"
        ))
        # Case B: Music album vs Film acting
        self.assertFalse(is_headline_too_similar(
            "Hadise yeni albümünü duyurdu",
            "Hadise yeni filminde rol aldı"
        ))
        # Case C: True duplicate with rephrased wording
        self.assertTrue(is_headline_too_similar(
            "Hadise yeni stüdyo albümünü duyurdu",
            "Hadise yeni stüdyo albümünü açıkladı"
        ))


if __name__ == "__main__":
    unittest.main()
