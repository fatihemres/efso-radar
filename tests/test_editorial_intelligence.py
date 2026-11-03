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
    detect_sensitive_health_content,
    is_headline_too_similar,
    project_diagnostic_decisions_to_assessments,
)
from efso_radar.ai.schemas import (
    DiagnosticArticleDecision,
    EditorialDecisionBatch,
    EditorialPickSchema,
)
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
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
        # Issue #69: Third-party syndicated news without authentic first-party proof is downgraded
        self.assertEqual(assessment.verification_status, VerificationStatus.REPUTABLE_REPORTING)
        self.assertTrue(assessment.is_publishable)
        self.assertEqual(assessment.disposition, EditorialDisposition.DIGEST)
        self.assertIn("birinci el doğrudan paylaşım/açıklama kanıtı bulunmuyor", assessment.uncertainties_tr)
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

    def test_offline_benchmark_report_explicitly_disclaims_real_model_accuracy(self):
        """Benchmark report explicitly disclaims independent accuracy and labels self-oracle origin."""
        evaluator = OfflineBenchmarkEvaluator()
        metrics = evaluator.run_evaluation()

        self.assertEqual(metrics.evaluation_kind, "SYNTHETIC_RULE_VALIDATION")
        self.assertEqual(metrics.prediction_source, "PREDICTIONS_DERIVED_FROM_EXPECTED_LABELS")
        self.assertEqual(metrics.model_accuracy_status, "NOT_MEASURED_MODEL_ACCURACY")

        report = metrics.summary()
        self.assertIn("SYNTHETIC_RULE_VALIDATION", report)
        self.assertIn("PREDICTIONS_DERIVED_FROM_EXPECTED_LABELS", report)
        self.assertIn("NOT_MEASURED_MODEL_ACCURACY", report)

    def test_offline_benchmark_report_with_caller_supplied_predictions(self):
        """Caller-supplied predictions are labeled dynamically and not described as derived from expected labels."""
        evaluator = OfflineBenchmarkEvaluator()
        caller_pick = EditorialPickSchema(
            article_ids=[evaluator.items[0].article_id],
            score=85,
            kind="NEW",
            verification_status="REPUTABLE_REPORTING",
            parent_story_id="",
            development_tr="",
            headline_tr="Caller supplied headline",
            cover_title_tr="COVER",
            caption_idea_tr="Caption",
            why_efso_tr="Why Efso",
            uncertainties_tr="",
            suggested_format="POST",
            category="YERLI",
        )
        metrics = evaluator.run_evaluation(predicted_picks=[caller_pick])

        self.assertEqual(metrics.prediction_source, "CALLER_SUPPLIED_PREDICTIONS")
        self.assertEqual(metrics.model_accuracy_status, "NOT_MEASURED_MODEL_ACCURACY")

        report = metrics.summary()
        self.assertIn("CALLER_SUPPLIED_PREDICTIONS", report)
        self.assertNotIn("PREDICTIONS_DERIVED_FROM_EXPECTED_LABELS", report)
        self.assertIn("NOT_MEASURED_MODEL_ACCURACY", report)

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

    def test_p1_social_media_like_downgraded_to_speculation_and_hold(self):
        """Social media engagement with speculative romance/motive claims is grounded to SPECULATION and HOLD."""
        art = Article.create(
            title="Kurgusal Fenomen A'dan B'nin Gönderisine Dikkat Çeken Beğeni",
            url="https://source.com/social-like",
            source="Magazin Blog",
            published_at=self.now,
            snippet="Sosyal medyada beğeni yaptı, aşk mı yaşıyorlar sorusu kulislerde konuşulmaya başlandı.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=88,
            kind="NEW",
            verification_status="FIRST_PARTY_STATEMENT",  # Overconfident model claim
            headline_tr="Fenomen A'dan B'nin Fotoğrafına Beğeni: Yeni Bir Aşk mı Başladı?",
            cover_title_tr="AŞK MI BAŞLADI",
            caption_idea_tr="İkili aşk mı yaşıyor soruları gündeme geldi.",
            why_efso_tr="Viral magazin ilişkisi.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        # Must NOT keep FIRST_PARTY_STATEMENT; must be grounded to SPECULATION due to unverified romance motive
        self.assertEqual(res.verification_status, VerificationStatus.SPECULATION)
        self.assertFalse(res.is_publishable, "Speculative social media interaction must have is_publishable=False")
        self.assertEqual(res.disposition, EditorialDisposition.HOLD)
        self.assertIn("Sosyal medya etkileşimi üzerinden türetilen doğrulanmamış ilişki/niyet iddiası", res.uncertainties_tr)

    def test_p1_neutral_documented_public_like_is_publishable_digest(self):
        """Neutral documented public social media interaction is downgraded from FIRST_PARTY to REPUTABLE_REPORTING and remains DIGEST."""
        art = Article.create(
            title="Kurgusal Oyuncu A'dan Rol Arkadaşı B'nin Paylaşımına Beğeni",
            url="https://source.com/public-like",
            source="Dizi Kulisi",
            published_at=self.now,
            snippet="Sosyal medyada rol arkadaşının yeni film afişi paylaşımını beğendi.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=87,
            kind="NEW",
            verification_status="FIRST_PARTY_STATEMENT",  # Like is not a personal statement
            headline_tr="Oyuncu A Rol Arkadaşı B'nin Paylaşımını Beğendi",
            cover_title_tr="DESTEK BEĞENİSİ",
            caption_idea_tr="Sosyal medyada rol arkadaşına beğeniyle destek verdi.",
            why_efso_tr="Popüler kültür sosyal medya takibi.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        # Must be downgraded from statement to reputable reporting, but NOT held as speculation
        self.assertEqual(res.verification_status, VerificationStatus.REPUTABLE_REPORTING)
        self.assertTrue(res.is_publishable, "Neutral documented public like must be publishable")
        self.assertEqual(res.disposition, EditorialDisposition.DIGEST)
        self.assertIn("Sosyal medya etkileşimi (beğeni/takip) birinci el beyan niteliğinde değildir", res.uncertainties_tr)

    def test_p1_neutral_like_with_reputable_reporting_stays_digest(self):
        """When model assigns REPUTABLE_REPORTING to neutral documented public like, it stays publishable DIGEST."""
        art = Article.create(
            title="Kurgusal Oyuncu A'dan Rol Arkadaşı B'nin Paylaşımına Beğeni",
            url="https://source.com/public-like-rep",
            source="Dizi Kulisi",
            published_at=self.now,
            snippet="Sosyal medyada rol arkadaşının yeni film afişi paylaşımını beğendi.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=87,
            kind="NEW",
            verification_status="REPUTABLE_REPORTING",  # Model chose REPUTABLE_REPORTING
            headline_tr="Oyuncu A Rol Arkadaşı B'nin Paylaşımını Beğendi",
            cover_title_tr="DESTEK BEĞENİSİ",
            caption_idea_tr="Sosyal medyada beğeniyle destek verdi.",
            why_efso_tr="Popüler kültür sosyal medya takibi.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertEqual(res.verification_status, VerificationStatus.REPUTABLE_REPORTING)
        self.assertTrue(res.is_publishable)
        self.assertEqual(res.disposition, EditorialDisposition.DIGEST)

    def test_p1_romantic_speculation_with_reputable_reporting_grounded_to_speculation_and_hold(self):
        """When model assigns REPUTABLE_REPORTING to romantic speculation, it is grounded to SPECULATION and HOLD."""
        art = Article.create(
            title="Kurgusal Fenomen A'dan B'nin Gönderisine Dikkat Çeken Beğeni",
            url="https://source.com/social-like-romance",
            source="Magazin Blog",
            published_at=self.now,
            snippet="Sosyal medyada beğeni yaptı, kulislerde aşk mı yaşıyorlar sorusu konuşuluyor.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=88,
            kind="NEW",
            verification_status="REPUTABLE_REPORTING",  # Model attempted to tag speculative romance as reputable
            headline_tr="Fenomen A'dan B'nin Fotoğrafına Beğeni: Yeni Bir Aşk mı Başladı?",
            cover_title_tr="AŞK MI BAŞLADI",
            caption_idea_tr="İkili aşk mı yaşıyor soruları gündeme geldi.",
            why_efso_tr="Viral magazin ilişkisi.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertEqual(res.verification_status, VerificationStatus.SPECULATION)
        self.assertFalse(res.is_publishable)
        self.assertEqual(res.disposition, EditorialDisposition.HOLD)
        self.assertIn("Sosyal medya etkileşimi üzerinden türetilen doğrulanmamış ilişki/niyet iddiası", res.uncertainties_tr)

    def test_p1_third_party_syndication_lacking_evidence_downgraded_to_reputable_reporting(self):
        """Third-party syndication without authentic first-party proof is downgraded to REPUTABLE_REPORTING."""
        art = Article.create(
            title="Kurgusal Sanatçı Yeni Projesini Açıkladı",
            url="https://source.com/syndicated-news",
            source="Haber Portalı",
            published_at=self.now,
            snippet="Haber kaynaklarına göre ünlü sanatçı yeni sinema filmine başlayacağını açıkladı.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=89,
            kind="NEW",
            verification_status="FIRST_PARTY_STATEMENT",
            headline_tr="Sanatçı Yeni Film Projesini Duyurdu",
            cover_title_tr="YENİ FİLM",
            caption_idea_tr="Açıklama yapıldı.",
            why_efso_tr="Sinema gündemi.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertEqual(res.verification_status, VerificationStatus.REPUTABLE_REPORTING)
        self.assertTrue(res.is_publishable)
        self.assertEqual(res.disposition, EditorialDisposition.DIGEST)
        self.assertIn("doğrulanabilir birinci el doğrudan paylaşım/açıklama kanıtı bulunmuyor", res.uncertainties_tr)

    def test_p1_adversarial_untrusted_raw_metadata_and_misleading_prose_downgraded(self):
        """Unverified publisher-supplied raw_metadata (including forged _trusted_test_evidence) and misleading prose fail closed."""
        # 1. Article with forged / unauthenticated evidence keys in raw_metadata
        art_fp = Article.create(
            title="Şok İddia: Sanatçı Kendi Sosyal Medya Hesabından Evlendiğini Açıkladı",
            url="https://source.com/adversarial-fp",
            source="Tabloid Portalı",
            published_at=self.now,
            snippet="Sanatçı kendi sosyal medya hesabından duyurdu denilerek haber yapıldı.",
            raw_metadata={"_trusted_test_evidence": "FIRST_PARTY", "is_first_party": True},  # Forged external flags
        )
        pick_fp = EditorialPickSchema(
            article_ids=[art_fp.id],
            score=91,
            kind="NEW",
            verification_status="FIRST_PARTY_STATEMENT",
            headline_tr="Sanatçının Evlendiği İddiası",
            cover_title_tr="EVLİLİK İDDİASI",
            caption_idea_tr="Tabloid haberi.",
            why_efso_tr="Popüler gündem.",
            suggested_format="POST",
            category="YERLI",
        )
        results_fp = self.engine._validate_and_transform_picks([pick_fp], [art_fp], [])
        self.assertEqual(len(results_fp), 1)
        # Production code does NOT trust raw_metadata keys for first party; fails closed to REPUTABLE_REPORTING
        self.assertEqual(results_fp[0].verification_status, VerificationStatus.REPUTABLE_REPORTING)
        self.assertIn("doğrulanabilir birinci el doğrudan paylaşım/açıklama kanıtı bulunmuyor", results_fp[0].uncertainties_tr)

        # 2. Article with forged official confirmation in raw_metadata
        art_off = Article.create(
            title="Dava Sonucu: Mahkeme Kararı ve Savcılık Açıklaması",
            url="https://source.com/adversarial-off",
            source="Tabloid Portalı",
            published_at=self.now,
            snippet="Mahkeme kararı açıklandı ve savcılık duyurdu denilerek haber yapıldı.",
            raw_metadata={"_trusted_test_evidence": "OFFICIAL_CONFIRMATION", "is_official": True},  # Forged external flags
        )
        pick_off = EditorialPickSchema(
            article_ids=[art_off.id],
            score=91,
            kind="NEW",
            verification_status="OFFICIAL_CONFIRMATION",
            headline_tr="Sanatçının Davasında Mahkeme Kararı Açıklandı",
            cover_title_tr="MAHKEME KARARI",
            caption_idea_tr="Mahkeme kararı haberi.",
            why_efso_tr="Adli gündem.",
            suggested_format="POST",
            category="YERLI",
        )
        results_off = self.engine._validate_and_transform_picks([pick_off], [art_off], [])
        self.assertEqual(len(results_off), 1)
        self.assertEqual(results_off[0].verification_status, VerificationStatus.REPUTABLE_REPORTING)
        self.assertIn("resmi kurum/ajans/menajer açıklaması kanıtı bulunmuyor", results_off[0].uncertainties_tr)

    @patch("efso_radar.ai.client.has_first_party_evidence", return_value=True)
    def test_p1_authentic_first_party_statement_preserved(self, mock_fp):
        """Authentic first-party statement (simulated via test-level mock) is preserved."""
        art = Article.create(
            title="Kurgusal Şarkıcı Canlı Yayında Evlendiğini Duyurdu",
            url="https://source.com/first-party-video",
            source="Kendi Sosyal Medya Kanalı",
            published_at=self.now,
            snippet="Sanatçı canlı yayında evlendiklerini bizzat duyurdu.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=94,
            kind="NEW",
            verification_status="FIRST_PARTY_STATEMENT",
            headline_tr="Kurgusal Şarkıcı Evlendiğini Bizzat Duyurdu",
            cover_title_tr="EVLENDİ",
            caption_idea_tr="Bizzat canlı yayında duyurdu.",
            why_efso_tr="Doğrudan birinci el açıklama.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertEqual(res.verification_status, VerificationStatus.FIRST_PARTY_STATEMENT)
        self.assertTrue(res.is_publishable)
        self.assertEqual(res.disposition, EditorialDisposition.DIGEST)

    @patch("efso_radar.ai.client.has_official_confirmation_evidence", return_value=True)
    def test_p1_official_confirmation_preserved_with_agency_or_spokesperson_evidence(self, mock_off):
        """Authentic official confirmation (simulated via test-level mock) is preserved."""
        art = Article.create(
            title="Menajerlik Ajansından Açıklama: Konser Turnesi İptal Edildi",
            url="https://source.com/press-release",
            source="Ajans Bülteni",
            published_at=self.now,
            snippet="Menajerlik ajansı tarafından yapılan resmi açıklamada dünya turnesinin ertelendiği doğrulandı.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=90,
            kind="NEW",
            verification_status="OFFICIAL_CONFIRMATION",
            headline_tr="Menajerlik Ajansı Konser Turnesinin İptalini Doğruladı",
            cover_title_tr="TURNE İPTAL",
            caption_idea_tr="Resmi açıklama yapıldı.",
            why_efso_tr="Resmi doğrulama.",
            suggested_format="POST",
            category="GLOBAL",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertEqual(res.verification_status, VerificationStatus.OFFICIAL_CONFIRMATION)
        self.assertTrue(res.is_publishable)
        self.assertEqual(res.disposition, EditorialDisposition.DIGEST)

    def test_p1_sensitive_medical_procedure_with_benign_model_headline_held(self):
        """Emergency hospitalization/surgery in source metadata fails closed as HOLD even with benign model headline."""
        art = Article.create(
            title="Kurgusal Oyuncu Aniden Rahatsızlanarak Hastaneye Kaldırıldı",
            url="https://source.com/hospital-emergency",
            source="Haber Merkezi",
            published_at=self.now,
            snippet="Gece saatlerinde hastaneye kaldırılan kurgusal oyuncu kalp krizi geçirerek acil ameliyata alındı ve yoğun bakımda müşahede altında tutuluyor.",
        )
        # Model crafts a benign, non-alarming headline
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=93,
            kind="NEW",
            verification_status="REPUTABLE_REPORTING",
            headline_tr="Ünlü Oyuncudan Yeni Proje Öncesi Hayranlarına Mesaj",
            cover_title_tr="HAYRANLARINA MESAJ",
            caption_idea_tr="Setlere kısa bir ara verdiğini duyurdu.",
            why_efso_tr="Ünlü popülerliği.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertFalse(res.is_publishable, "Sensitive medical hospitalization must ALWAYS have is_publishable=False")
        self.assertEqual(res.disposition, EditorialDisposition.HOLD)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE HEALTH REVIEW REQUIRED]", res.uncertainties_tr)

    @patch("efso_radar.ai.client.has_official_confirmation_evidence", return_value=True)
    def test_p1_sensitive_medical_verified_high_score_still_held(self, mock_off):
        """Model score 98 and simulated official confirmation cannot override medical HOLD."""
        art = Article.create(
            title="Hastaneden Resmi Açıklama: Sanatçının Kanser Tedavisi Süreci",
            url="https://source.com/hospital-cancer",
            source="Hastane Başhekimliği",
            published_at=self.now,
            snippet="Hastane başhekimliği tarafından yapılan resmi açıklamada sanatçının kemoterapi tedavisi gördüğü açıklandı.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=98,
            kind="NEW",
            verification_status="OFFICIAL_CONFIRMATION",
            headline_tr="Hastane Başhekimliği Sanatçının Tedavi Sürecini Açıkladı",
            cover_title_tr="TEDAVİ AÇIKLAMASI",
            caption_idea_tr="Başhekimlik kemoterapi sürecini paylaştı.",
            why_efso_tr="Hayati sağlık açıklaması.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertFalse(res.is_publishable, "Score 98 and official status cannot override medical HOLD")
        self.assertEqual(res.disposition, EditorialDisposition.HOLD)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE HEALTH REVIEW REQUIRED]", res.uncertainties_tr)

    def test_p1_sensitive_medical_rumor_fails_closed_hold(self):
        """Unverified rumor concerning critical health/ICU is held fail-closed."""
        art = Article.create(
            title="Şok İddia: Kurgusal Popçu Yoğun Bakıma mı Kaldırıldı?",
            url="https://source.com/icu-rumor",
            source="Magazin Kulis",
            published_at=self.now,
            snippet="Sosyal medyada kulis iddialarına göre ünlü popçunun yoğun bakımda yaşam savaşı verdiği öne sürüldü.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=86,
            kind="NEW",
            verification_status="UNVERIFIED_RUMOR",
            headline_tr="Kurgusal Popçunun Yoğun Bakımda Olduğu İddiası",
            cover_title_tr="YOĞUN BAKIM İDDİASI",
            caption_idea_tr="Kulis söylentisi.",
            why_efso_tr="Sosyal medya gündemi.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertFalse(res.is_publishable)
        self.assertEqual(res.disposition, EditorialDisposition.HOLD)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE HEALTH REVIEW REQUIRED]", res.uncertainties_tr)

    def test_p1_harmless_entertainment_medical_fiction_not_spuriously_blocked(self):
        """Medical roles in fiction, lifestyle tips, or harmless storylines are NOT blocked."""
        art = Article.create(
            title="Kurgusal Oyuncu Yeni Hastane Dizisinde Başrolü Üstlenecek",
            url="https://source.com/fiction-doctor",
            source="Dizi Magazin",
            published_at=self.now,
            snippet="Yeni sezonda ekrana gelecek tıp dramasında başarılı bir cerrahı canlandıracak. Sağlıklı beslenme sırlarını da anlattı.",
        )
        pick = EditorialPickSchema(
            article_ids=[art.id],
            score=88,
            kind="NEW",
            verification_status="REPUTABLE_REPORTING",
            headline_tr="Kurgusal Oyuncu Yeni Tıp Dizisinde Başrol Olacak",
            cover_title_tr="YENİ DİZİ",
            caption_idea_tr="Cerrah rolüyle ekrana dönüyor.",
            why_efso_tr="Televizyon projeleri.",
            suggested_format="POST",
            category="YERLI",
        )
        results = self.engine._validate_and_transform_picks([pick], [art], [])
        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertTrue(res.is_publishable, "Harmless entertainment fiction must be publishable")
        self.assertEqual(res.disposition, EditorialDisposition.DIGEST)

    def test_p1_diagnostic_projection_sensitive_health_parity(self):
        """project_diagnostic_decisions_to_assessments holds sensitive health even with DIGEST / BREAKING decision."""
        art = Article.create(
            title="Kurgusal Rock Sanatçısı Acil Ameliyata Alındı",
            url="https://source.com/surgery-diag",
            source="Müzik Haber",
            published_at=self.now,
            snippet="Hastaneye kaldırılan sanatçı acil ameliyata alındı ve yoğun bakımda tedavi görüyor.",
        )
        dec = DiagnosticArticleDecision(
            article_id=art.id,
            score=95,
            disposition="DIGEST",  # Model erroneously marked DIGEST
            kind="NEW",
            urgency="BREAKING",    # Model erroneously marked BREAKING
            rationale_tr="Önemli sanatçının hastaneye kaldırılması.",
            headline_tr="Kurgusal Sanatçı Acil Ameliyata Alındı",
            verification_status="REPUTABLE_REPORTING",
        )
        assessments = project_diagnostic_decisions_to_assessments([dec], [art], include_manual_holds=True)
        self.assertEqual(len(assessments), 1)
        asm = assessments[0]
        self.assertFalse(asm.is_publishable, "Diagnostic projection must hold sensitive health articles")
        self.assertEqual(asm.disposition, EditorialDisposition.HOLD)
        self.assertIn("[NOT_PUBLISHABLE - SENSITIVE HEALTH REVIEW REQUIRED]", asm.uncertainties_tr)

    def test_p1_orchestrator_routes_sensitive_health_hold_to_verification_queue(self):
        """RadarOrchestrator routes an assessment with sensitive health HOLD to HELD_FOR_VERIFICATION queue, never SENT or DIGEST."""
        art = Article.create(
            title="Kurgusal Oyuncu Hastaneye Kaldırıldı",
            url="https://source.com/hosp-orch",
            source="Gazete",
            published_at=self.now,
            snippet="Hastaneye kaldırılan sanatçı yoğun bakımda.",
        )
        assessment = EditorialAssessment(
            article_ids=[art.id],
            score=92,
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Oyuncu Hastaneye Kaldırıldı",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art.url],
            supporting_articles=[art],
            is_publishable=False,
            disposition=EditorialDisposition.HOLD,
            uncertainties_tr="[NOT_PUBLISHABLE - SENSITIVE HEALTH REVIEW REQUIRED] İnceleme bekletmesinde.",
        )
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art], successes=1),
            ai_engine=MagicMock(evaluate=MagicMock(return_value=[assessment])),
            notifier=MagicMock(send=MagicMock(return_value=NotificationRecord(
                notification_id="SENT-1", story_id="S1", kind=EditorialKind.NEW,
                channel="TELEGRAM", sent_at=self.now, status=DeliveryStatus.SENT,
                message_text="msg", headline_tr="hl", score=90
            ))),
        )
        # Run orchestrator
        result = orch.run(now=self.now)
        self.assertTrue(result.success)
        # Notifier.send must NOT be called for breaking alert
        orch.notifier.send.assert_not_called()
        # Notification record in repository must be HELD_FOR_VERIFICATION
        notifications = self.repo.get_verification_held_notifications()
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0].status, DeliveryStatus.HELD_FOR_VERIFICATION)

    def test_p2_orchestrator_source_text_health_veto_blocks_benign_assessment(self):
        """Orchestrator defense-in-depth inspects supporting article text and blocks a benign-looking assessment with emergency health source."""
        art = Article.create(
            title="Kurgusal Sanatçı Kalp Krizi Geçirerek Acilen Yoğun Bakıma Alındı",
            url="https://source.com/emergency-icu",
            source="Sağlık Portalı",
            published_at=self.now,
            snippet="Sanatçı aniden rahatsızlanarak acil ameliyata alındı ve yoğun bakım ünitesinde tedavi görüyor.",
        )
        # Flawed / mock AI engine returning a completely benign headline and default publishable flags
        flawed_assessment = EditorialAssessment(
            article_ids=[art.id],
            score=95,  # Above breaking alert min score
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Sanatçı Yeni Albüm Hazırlığına Başladı",  # Benign headline
            caption_idea_tr="Stüdyo kayıtlarına odaklandı.",
            why_efso_tr="Popüler müzik gündemi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art.url],
            supporting_articles=[art],
            is_publishable=True,  # Flawed engine declared publishable
            disposition=EditorialDisposition.DIGEST,
            uncertainties_tr=None,  # No health uncertainty from flawed engine
        )
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art], successes=1),
            ai_engine=MagicMock(evaluate=MagicMock(return_value=[flawed_assessment])),
            notifier=MagicMock(send=MagicMock(return_value=NotificationRecord(
                notification_id="SENT-FAIL", story_id="S1", kind=EditorialKind.NEW,
                channel="TELEGRAM", sent_at=self.now, status=DeliveryStatus.SENT,
                message_text="msg", headline_tr="hl", score=95
            ))),
        )
        result = orch.run(now=self.now)
        self.assertTrue(result.success)
        # Crucial defense-in-depth: Notifier.send must NOT be dispatched
        orch.notifier.send.assert_not_called()
        self.assertEqual(result.notifications_dispatched, 0)
        # Defense-in-depth veto placed it in HELD_FOR_VERIFICATION queue
        held_notifications = self.repo.get_verification_held_notifications()
        self.assertEqual(len(held_notifications), 1)
        self.assertEqual(held_notifications[0].status, DeliveryStatus.HELD_FOR_VERIFICATION)
        self.assertIn("Held for sensitive health review", held_notifications[0].error_message)

    def test_p2_orchestrator_candidate_map_veto_blocks_sanitized_engine_supporting_articles(self):
        """Authoritative candidate_map text triggers health veto even if engine supplies sanitized supporting_articles."""
        # Authoritative candidate in candidate_map contains urgent emergency health content
        art_auth = Article.create(
            title="Kurgusal Sanatçı Kalp Krizi Geçirerek Acilen Yoğun Bakıma Alındı",
            url="https://source.com/auth-icu",
            source="Acil Haber",
            published_at=self.now,
            snippet="Sanatçı aniden fenalaşarak acil ameliyata alındı ve yoğun bakımda tedavi görüyor.",
        )
        # Flawed / alternative AI engine supplies a sanitized supporting article copy where emergency is omitted
        art_sanitized = Article(
            id=art_auth.id,
            title="Kurgusal Sanatçı Yeni Albüm Tanıtımını Yaptı",
            url="https://source.com/sanitized-album",
            source="Müzik Haber",
            published_at=self.now,
            snippet="Sanatçı stüdyoda yeni parçalarını kaydettiğini açıkladı.",
        )
        flawed_assessment = EditorialAssessment(
            article_ids=[art_auth.id],
            score=96,
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Sanatçı Yeni Albüm Tanıtımını Yaptı",
            caption_idea_tr="Stüdyoda yeni parçalarını kaydetti.",
            why_efso_tr="Müzik gündemi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art_auth.url],
            supporting_articles=[art_sanitized],  # Sanitized engine copy!
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
            uncertainties_tr=None,
        )
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_auth], successes=1),  # candidate_map has art_auth!
            ai_engine=MagicMock(evaluate=MagicMock(return_value=[flawed_assessment])),
            notifier=MagicMock(send=MagicMock(return_value=NotificationRecord(
                notification_id="SENT-FAIL", story_id="S1", kind=EditorialKind.NEW,
                channel="TELEGRAM", sent_at=self.now, status=DeliveryStatus.SENT,
                message_text="msg", headline_tr="hl", score=96
            ))),
        )
        with patch.object(self.repo, "record_editorial_assessment", wraps=self.repo.record_editorial_assessment) as mock_record_assessment:
            result = orch.run(now=self.now)
        self.assertTrue(result.success)
        # Defense-in-depth: Authoritative candidate_map triggers health veto despite sanitized supporting_articles
        orch.notifier.send.assert_not_called()
        self.assertEqual(result.notifications_dispatched, 0)
        held_notifications = self.repo.get_verification_held_notifications()
        self.assertEqual(len(held_notifications), 1)
        self.assertEqual(held_notifications[0].status, DeliveryStatus.HELD_FOR_VERIFICATION)
        self.assertIn("Held for sensitive health review", held_notifications[0].error_message)

        # Issue #71 Acceptance: Captured assessment given to downstream state is the normalized effective assessment
        mock_record_assessment.assert_called_once()
        recorded_assessment = mock_record_assessment.call_args[0][0]
        self.assertFalse(recorded_assessment.is_publishable)
        self.assertEqual(recorded_assessment.disposition, EditorialDisposition.HOLD)
        self.assertIn("SENSITIVE HEALTH REVIEW", recorded_assessment.uncertainties_tr)
        # Authoritative candidate replaced sanitized copy
        self.assertEqual(len(recorded_assessment.supporting_articles), 1)
        self.assertEqual(recorded_assessment.supporting_articles[0].id, art_auth.id)
        self.assertEqual(recorded_assessment.supporting_articles[0].snippet, art_auth.snippet)
        self.assertIn("yoğun bakımda", recorded_assessment.supporting_articles[0].snippet)
        self.assertEqual(recorded_assessment.source_urls, [art_auth.url])
        # Incoming model object is not mutated
        self.assertFalse(flawed_assessment is recorded_assessment)
        self.assertTrue(flawed_assessment.is_publishable)
        self.assertEqual(flawed_assessment.disposition, EditorialDisposition.DIGEST)

    def test_p2_orchestrator_sensitive_tragedy_effective_hold_and_review_precedence(self):
        """Tragedy in source triggers SENSITIVE_REVIEW precedence over HOLD even if model claims DIGEST."""
        art_trag = Article.create(
            title="Kurgusal Sanatçı Trafik Kazasında Hayatını Kaybetti",
            url="https://source.com/tragedy-fatal",
            source="Ajans Haber",
            published_at=self.now,
            snippet="Otomobiliyle kaza yapan ünlü sanatçı vefat etti; cenaze töreni Teşvikiye Camii'nde düzenlenecek.",
        )
        flawed_model_assessment = EditorialAssessment(
            article_ids=[art_trag.id],
            score=98,
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Sanatçı Yeni Projesiyle Gündemde",
            caption_idea_tr="Yeni projeleri konuşuluyor.",
            why_efso_tr="Gündem maddesi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art_trag.url],
            supporting_articles=[art_trag],
            is_publishable=True,  # Flawed engine claimed publishable DIGEST
            disposition=EditorialDisposition.DIGEST,
            uncertainties_tr=None,
        )
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_trag], successes=1),
            ai_engine=MagicMock(evaluate=MagicMock(return_value=[flawed_model_assessment])),
            notifier=MagicMock(send=MagicMock(return_value=NotificationRecord(
                notification_id="SENT-FAIL", story_id="S1", kind=EditorialKind.NEW,
                channel="TELEGRAM", sent_at=self.now, status=DeliveryStatus.SENT,
                message_text="msg", headline_tr="hl", score=98
            ))),
        )
        with patch.object(self.repo, "record_editorial_assessment", wraps=self.repo.record_editorial_assessment) as mock_rec:
            result = orch.run(now=self.now)
        self.assertTrue(result.success)
        orch.notifier.send.assert_not_called()
        self.assertEqual(result.notifications_dispatched, 0)

        # Precedence check: SENSITIVE_REVIEW takes precedence over HOLD
        mock_rec.assert_called_once()
        effective_asm = mock_rec.call_args[0][0]
        self.assertFalse(effective_asm.is_publishable)
        self.assertEqual(effective_asm.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertIn("SENSITIVE EDITORIAL REVIEW", effective_asm.uncertainties_tr)

        # Notification recorded in sensitive review queue
        sens_notifs = self.repo.get_sensitive_review_notifications()
        self.assertEqual(len(sens_notifs), 1)
        self.assertEqual(sens_notifs[0].status, DeliveryStatus.HELD_FOR_SENSITIVE_REVIEW)
        self.assertEqual(sens_notifs[0].channel, "TELEGRAM_SENSITIVE_REVIEW")
        self.assertIn("ÖZEL EDİTORYAL İNCELEME", sens_notifs[0].message_text)

    def test_p2_orchestrator_explicitly_held_attributed_claim_remains_nonpublishable_despite_benign_wording(self):
        """Attributed claim or model HOLD status remains nonpublishable despite benign headline."""
        art_claim = Article.create(
            title="Kurgusal Çiftin Ayrıldığı İddia Edildi",
            url="https://source.com/claimed-breakup",
            source="Sosyal Magazin",
            published_at=self.now,
            snippet="Sosyal medyada yayılan iddialara göre ünlü çiftin ilişkisi sona erdi.",
        )
        pick_attributed = EditorialAssessment(
            article_ids=[art_claim.id],
            score=86,
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Çiftin Ayrıldığı İddia Edildi",
            caption_idea_tr="İddialar sosyal medyada yayıldı.",
            why_efso_tr="Viral magazin iddiası.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.ATTRIBUTED_CLAIM,  # Unverified / attributed
            source_urls=[art_claim.url],
            supporting_articles=[art_claim],
            is_publishable=True,  # Flawed engine erroneously marked publishable DIGEST
            disposition=EditorialDisposition.DIGEST,
            uncertainties_tr=None,
        )
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_claim], successes=1),
            ai_engine=MagicMock(evaluate=MagicMock(return_value=[pick_attributed])),
            notifier=MagicMock(send=MagicMock(return_value=NotificationRecord(
                notification_id="SENT-FAIL", story_id="S1", kind=EditorialKind.NEW,
                channel="TELEGRAM", sent_at=self.now, status=DeliveryStatus.SENT,
                message_text="msg", headline_tr="hl", score=86
            ))),
        )
        with patch.object(self.repo, "record_editorial_assessment", wraps=self.repo.record_editorial_assessment) as mock_rec:
            result = orch.run(now=self.now)
        self.assertTrue(result.success)
        orch.notifier.send.assert_not_called()
        self.assertEqual(result.notifications_dispatched, 0)

        # Attributed claim must become is_publishable=False and disposition=HOLD
        mock_rec.assert_called_once()
        effective_asm = mock_rec.call_args[0][0]
        self.assertFalse(effective_asm.is_publishable)
        self.assertEqual(effective_asm.disposition, EditorialDisposition.HOLD)

        held_notifs = self.repo.get_verification_held_notifications()
        self.assertEqual(len(held_notifs), 1)
        self.assertEqual(held_notifs[0].status, DeliveryStatus.HELD_FOR_VERIFICATION)
        self.assertIn("DOĞRULAMA BEKLEYEN İDDİA", held_notifs[0].message_text)

    def test_p2_orchestrator_normal_digest_story_remains_publishable_with_authoritative_sources(self):
        """Normal, non-sensitive, verified entertainment news remains publishable in DIGEST."""
        art_norm = Article.create(
            title="Kurgusal Yönetmen Yeni Film Çekimlerine İstanbul'da Başladı",
            url="https://source.com/film-shooting",
            source="Sanat Portalı",
            published_at=self.now,
            snippet="Ödüllü yönetmen yeni sinema projesinin çekimlerine Tarihi Yarımada'da başladı.",
        )
        pick_normal = EditorialAssessment(
            article_ids=[art_norm.id],
            score=76,  # Below breaking min score, eligible for digest
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Yönetmen Yeni Film Çekimlerine İstanbul'da Başladı",
            caption_idea_tr="Çekimler Tarihi Yarımada'da devam ediyor.",
            why_efso_tr="Kültür sanat ve sinema haberi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art_norm.url],
            supporting_articles=[art_norm],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
            uncertainties_tr=None,
        )
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_norm], successes=1),
            ai_engine=MagicMock(evaluate=MagicMock(return_value=[pick_normal])),
            notifier=MagicMock(send=MagicMock(return_value=NotificationRecord(
                notification_id="SENT-FAIL", story_id="S1", kind=EditorialKind.NEW,
                channel="TELEGRAM", sent_at=self.now, status=DeliveryStatus.SENT,
                message_text="msg", headline_tr="hl", score=76
            ))),
        )
        with patch.object(self.repo, "record_editorial_assessment", wraps=self.repo.record_editorial_assessment) as mock_rec:
            result = orch.run(now=self.now, use_hybrid_digest=True)
        self.assertTrue(result.success)
        mock_rec.assert_called_once()
        effective_asm = mock_rec.call_args[0][0]
        self.assertTrue(effective_asm.is_publishable)
        self.assertEqual(effective_asm.disposition, EditorialDisposition.DIGEST)
        self.assertEqual(effective_asm.supporting_articles[0].id, art_norm.id)
        self.assertEqual(effective_asm.source_urls, [art_norm.url])

        # Successfully queued for periodic digest
        queued = self.repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0].status, DeliveryStatus.QUEUED_FOR_DIGEST)

    def test_p2_orchestrator_unknown_or_mixed_candidate_id_fails_closed(self):
        """Unknown or mixed candidate ID fails closed to HOLD, suppressing sends and digest queues."""
        art_known = Article.create(
            title="Kurgusal Müzisyen Yeni Single Parçasını Yayınladı",
            url="https://source.com/single",
            source="Müzik Haber",
            published_at=self.now,
            snippet="Yeni single parçası tüm dijital müzik platformlarında yayına girdi.",
        )
        # Pick contains a valid ID alongside a hallucinated / unknown ID
        pick_mixed = EditorialAssessment(
            article_ids=[art_known.id, "hallucinated_unknown_art_999"],
            score=94,
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Müzisyen Yeni Single Parçasını Yayınladı",
            caption_idea_tr="Dijital platformlarda yayında.",
            why_efso_tr="Müzik gündemi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art_known.url, "https://unknown.com/fake"],
            supporting_articles=[art_known],
            is_publishable=True,  # Flawed engine claimed publishable
            disposition=EditorialDisposition.DIGEST,
            uncertainties_tr=None,
        )
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_known], successes=1),  # Only art_known in candidate_map!
            ai_engine=MagicMock(evaluate=MagicMock(return_value=[pick_mixed])),
            notifier=MagicMock(send=MagicMock(return_value=NotificationRecord(
                notification_id="SENT-FAIL", story_id="S1", kind=EditorialKind.NEW,
                channel="TELEGRAM", sent_at=self.now, status=DeliveryStatus.SENT,
                message_text="msg", headline_tr="hl", score=94
            ))),
        )
        with patch.object(self.repo, "record_editorial_assessment", wraps=self.repo.record_editorial_assessment) as mock_rec:
            result = orch.run(now=self.now, use_hybrid_digest=True)
        self.assertTrue(result.success)
        orch.notifier.send.assert_not_called()
        self.assertEqual(result.notifications_dispatched, 0)

        # Fails closed to HOLD
        mock_rec.assert_called_once()
        effective_asm = mock_rec.call_args[0][0]
        self.assertFalse(effective_asm.is_publishable)
        self.assertEqual(effective_asm.disposition, EditorialDisposition.HOLD)
        self.assertIn("Aday havuzunda bulunmayan makale ID'si", effective_asm.uncertainties_tr)

        # Authoritative supporting articles and URLs must contain zero unknown-ID material
        self.assertEqual(len(effective_asm.supporting_articles), 1)
        self.assertEqual(effective_asm.supporting_articles[0].id, art_known.id)
        self.assertEqual(effective_asm.source_urls, [art_known.url])
        self.assertNotIn("https://unknown.com/fake", effective_asm.source_urls)

        # Not queued for digest, placed in verification hold queue
        queued = self.repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 0)
        held = self.repo.get_verification_held_notifications()
        self.assertEqual(len(held), 1)
        self.assertEqual(held[0].status, DeliveryStatus.HELD_FOR_VERIFICATION)

    def test_p2_derive_effective_assessment_pure_unit_safety_precedence(self):
        """Pure unit test of RadarOrchestrator.derive_effective_assessment for all safety edges."""
        art1 = Article.create(
            title="Kurgusal Sanatçı Yoğun Bakımda Tedavi Görüyor",
            url="https://source.com/icu",
            source="Sağlık Haber",
            published_at=self.now,
            snippet="Sanatçı kalp krizi sonrası hastaneye kaldırılarak yoğun bakıma alındı.",
        )
        art2 = Article.create(
            title="Kurgusal Oyuncunun Vefatı Sanat Dünyasını Yasa Boğdu",
            url="https://source.com/death",
            source="Vefat Haber",
            published_at=self.now,
            snippet="Ünlü oyuncunun cenaze namazı ikindi vakti kılınacak.",
        )
        candidate_map = {art1.id: art1, art2.id: art2}

        # Case 1: Tragedy + Health both present -> SENSITIVE_REVIEW wins
        pick_both = EditorialAssessment(
            article_ids=[art1.id, art2.id],
            score=95,
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Sanatçılar Gündemde",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )
        eff_both, is_sens, is_health, is_unverif = RadarOrchestrator.derive_effective_assessment(pick_both, candidate_map)
        self.assertTrue(is_sens)
        self.assertTrue(is_health)
        self.assertTrue(is_unverif)
        self.assertFalse(eff_both.is_publishable)
        self.assertEqual(eff_both.disposition, EditorialDisposition.SENSITIVE_REVIEW)
        self.assertEqual(len(eff_both.supporting_articles), 2)

        # Case 2: Pure Health -> HOLD
        pick_health = EditorialAssessment(
            article_ids=[art1.id],
            score=90,
            kind=EditorialKind.NEW,
            headline_tr="Kurgusal Sanatçı Sağlık Durumu",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )
        eff_h, is_sens_h, is_health_h, is_unverif_h = RadarOrchestrator.derive_effective_assessment(pick_health, candidate_map)
        self.assertFalse(is_sens_h)
        self.assertTrue(is_health_h)
        self.assertTrue(is_unverif_h)
        self.assertFalse(eff_h.is_publishable)
        self.assertEqual(eff_h.disposition, EditorialDisposition.HOLD)

        # Case 3: Empty / missing ID in candidate_map -> Fails closed to HOLD
        pick_missing = EditorialAssessment(
            article_ids=["non_existent_id"],
            score=88,
            kind=EditorialKind.NEW,
            headline_tr="Normal Görünümlü Haber",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )
        eff_m, _, _, is_unverif_m = RadarOrchestrator.derive_effective_assessment(pick_missing, candidate_map)
        self.assertTrue(is_unverif_m)
        self.assertFalse(eff_m.is_publishable)
        self.assertEqual(eff_m.disposition, EditorialDisposition.HOLD)
        self.assertEqual(eff_m.supporting_articles, [])
        self.assertEqual(eff_m.source_urls, [])

        # Case 4: Original pick object remains completely unmutated
        self.assertTrue(pick_both.is_publishable)
        self.assertEqual(pick_both.disposition, EditorialDisposition.DIGEST)
        self.assertTrue(pick_health.is_publishable)
        self.assertEqual(pick_health.disposition, EditorialDisposition.DIGEST)


if __name__ == "__main__":
    unittest.main()
