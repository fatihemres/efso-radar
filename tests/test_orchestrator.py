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

    def send_digest_batch(
        self,
        assessments,
        notification_id=None,
        window_label="6 Saatlik Editoryal Özet",
        dry_run=False,
        now=None,
    ) -> NotificationRecord:
        record = NotificationRecord(
            notification_id=notification_id or f"MOCK-DIGEST-{len(self.sent_records)}",
            story_id=assessments[0].article_ids[0] if assessments else "DIGEST",
            kind=EditorialKind.NEW,
            sent_at=now or utc_now(),
            status=DeliveryStatus.SENT,
            message_text=f"Mock digest ({len(assessments)} items)",
            headline_tr=assessments[0].headline_tr if assessments else "Digest",
            score=85,
        )
        self.sent_records.append(record)
        return record


class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_orch.db"
        self.settings = Settings(sqlite_db_path=self.db_path, ai_evaluation_enabled=True)
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

    def test_scan_only_mode_decouples_discovery_from_gemini(self):
        """run_scan_only() persists discovered candidates to durable backlog with 0 Gemini calls and 0 alerts."""
        article = Article.create("Tarkan yeni albüm duyurdu", "https://url.com/tarkan", "Habertürk", self.now)
        source = MockNewsSource(articles=[article], successes=1)
        notifier = MockNotifier()
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=source,
            ai_engine=MockAIEngine(),
            notifier=notifier,
        )
        res = orch.run_scan_only(now=self.now)
        self.assertTrue(res.success)
        self.assertEqual(res.articles_collected, 1)
        self.assertEqual(res.candidates_evaluated, 0)
        self.assertEqual(res.notifications_dispatched, 0)
        self.assertEqual(len(notifier.sent_records), 0)

        # Candidate is stored in durable pending backlog, not lost
        pending = self.repo.get_pending_candidates(now=self.now)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].id, article.id)

    def test_default_settings_disable_live_ai_evaluation(self):
        """By default, Settings().ai_evaluation_enabled is False and orch.run() executes scan-only."""
        default_settings = Settings(sqlite_db_path=self.db_path)
        self.assertFalse(default_settings.ai_evaluation_enabled)

        article = Article.create("Serenay Sarıkaya ödül aldı", "https://url.com/serenay", "Hürriyet", self.now)
        pick = EditorialAssessment(
            article_ids=[article.id],
            score=95,
            kind=EditorialKind.NEW,
            headline_tr="Serenay Ödül Aldı",
            cover_title_tr="ÖDÜL",
            caption_idea_tr="Detay",
            why_efso_tr="Önemli",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[article],
        )
        notifier = MockNotifier()
        orch = RadarOrchestrator(
            settings=default_settings,
            repository=self.repo,
            source=MockNewsSource(articles=[article], successes=1),
            ai_engine=MockAIEngine(picks=[pick]),
            notifier=notifier,
        )
        res = orch.run(now=self.now)
        self.assertTrue(res.success)
        self.assertEqual(res.candidates_evaluated, 0)
        self.assertEqual(res.notifications_dispatched, 0)
        self.assertEqual(len(self.repo.get_pending_candidates(now=self.now)), 1)

    def test_3h_scan_and_6h_digest_recovery_after_missed_run_and_update_delivery(self):
        """3h scans + 6h digests recover after missed scheduled runs and deliver legitimate UPDATEs."""
        from datetime import timedelta
        from efso_radar.domain import VerificationStatus

        hybrid_settings = Settings(
            sqlite_db_path=self.db_path,
            ai_evaluation_enabled=True,
            notification_strategy="HYBRID",
            scan_interval_minutes=180,
            digest_interval_minutes=360,
            digest_min_score=78,
            min_score=82,
            breaking_alert_min_score=92,
            quiet_hours_enabled=False,
        )
        notifier = MockNotifier()

        # T0 (10:00 UTC): First digest window is due (last_digest_sent_at is None) -> dispatches 1 initial digest.
        art1 = Article.create("Hadise yeni turnesini açıkladı", "https://url.com/hadise-1", "Milliyet", self.now)
        pick1 = EditorialAssessment(
            article_ids=[art1.id],
            score=85,
            kind=EditorialKind.NEW,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            headline_tr="Hadise Avrupa turnesini duyurdu",
            cover_title_tr="TURNE DUYURUSU",
            caption_idea_tr="Avrupa turnesi başlıyor.",
            why_efso_tr="Konser gündemi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[art1],
        )
        orch1 = RadarOrchestrator(
            settings=hybrid_settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art1], successes=1),
            ai_engine=MockAIEngine(picks=[pick1]),
            notifier=notifier,
        )
        res1 = orch1.run(now=self.now, use_hybrid_digest=True)
        self.assertTrue(res1.success)
        self.assertEqual(res1.notifications_dispatched, 1)
        self.assertEqual(len(self.repo.get_queued_digest_notifications()), 0)

        # T+3h (13:00 UTC): 180 min elapsed (< 360 min digest_interval_minutes).
        # Digest is NOT due yet -> story is queued in durable digest queue, 0 messages sent now.
        t_plus_3h = self.now + timedelta(hours=3)
        art2 = Article.create(
            "Mabel Matiz yeni tekli yayınladı",
            "https://url.com/mabel-1",
            "Habertürk",
            t_plus_3h,
        )
        pick2 = EditorialAssessment(
            article_ids=[art2.id],
            score=86,
            kind=EditorialKind.NEW,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            headline_tr="Mabel Matiz yeni şarkısını çıkardı",
            cover_title_tr="YENİ ŞARKI",
            caption_idea_tr="Yeni tekli yayında.",
            why_efso_tr="Müzik gündemi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[art2],
        )
        orch2 = RadarOrchestrator(
            settings=hybrid_settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art2], successes=1),
            ai_engine=MockAIEngine(picks=[pick2]),
            notifier=notifier,
        )
        res2 = orch2.run(now=t_plus_3h, use_hybrid_digest=True)
        self.assertTrue(res2.success)
        self.assertEqual(res2.notifications_dispatched, 0)
        self.assertEqual(len(self.repo.get_queued_digest_notifications()), 1)

        # Suppose the 16:00 UTC (T+6h) scheduled run is MISSED completely, and art3 was published at T+5h.
        # At T+7h (17:00 UTC, 420 min >= 360 min since T0), the scheduler runs again:
        # - art3 (published 2h ago, well within 48h freshness window) is collected.
        # - queued digest item from T+3h + new digest item from T+7h are consolidated into 1 digest message.
        t_plus_7h = self.now + timedelta(hours=7)
        art_missed = Article.create(
            "Tarkan Harbiye konser takvimini açıkladı",
            "https://url.com/tarkan-1",
            "Milliyet",
            self.now + timedelta(hours=5),
        )
        pick_missed = EditorialAssessment(
            article_ids=[art_missed.id],
            score=87,
            kind=EditorialKind.NEW,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            headline_tr="Tarkan Harbiye konser tarihleri belli oldu",
            cover_title_tr="HARBİYE TAKVİMİ",
            caption_idea_tr="Konser tarihleri açıklandı.",
            why_efso_tr="Pop müzik gündemi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[art_missed],
        )
        orch_recover = RadarOrchestrator(
            settings=hybrid_settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_missed], successes=1),
            ai_engine=MockAIEngine(picks=[pick_missed]),
            notifier=notifier,
        )
        res_recover = orch_recover.run(now=t_plus_7h, use_hybrid_digest=True)
        self.assertTrue(res_recover.success)
        self.assertEqual(res_recover.notifications_dispatched, 1)
        self.assertEqual(len(self.repo.get_queued_digest_notifications()), 0)

        # At T+9h (19:00 UTC), a major verified UPDATE (score 94 >= 92) arrives on Story 1 (Hadise).
        # It must NOT be suppressed by the earlier notification on parent_story_id!
        t_plus_9h = self.now + timedelta(hours=9)
        parent_story_id = f"EFSO-{self.now.strftime('%Y%m%d')}-{art1.id[:8]}"
        art3 = Article.create(
            "Hadise turnesinin ilk konser biletleri 10 dakikada tükendi",
            "https://url.com/hadise-2",
            "Hürriyet",
            t_plus_9h,
        )
        pick3 = EditorialAssessment(
            article_ids=[art3.id],
            score=94,
            kind=EditorialKind.UPDATE,
            parent_story_id=parent_story_id,
            development_tr="Turnenin tüm Avrupa biletlerinin 10 dakika içinde tükendiği açıklandı.",
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            headline_tr="Hadise biletleri 10 dakikada tükendi",
            cover_title_tr="BİLETLER TÜKENDİ",
            caption_idea_tr="Rekor sürede tükendi.",
            why_efso_tr="Önemli güncelleme.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            supporting_articles=[art3],
        )
        orch3 = RadarOrchestrator(
            settings=hybrid_settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art3], successes=1),
            ai_engine=MockAIEngine(picks=[pick3]),
            notifier=notifier,
        )
        res3 = orch3.run(now=t_plus_9h, use_hybrid_digest=True)
        self.assertTrue(res3.success)
        self.assertEqual(res3.picks_approved, 1)
        self.assertEqual(res3.notifications_dispatched, 1)



    def test_pre_ai_noise_drop_filters_annotated_out_of_scope_reasons(self):
        """Fix B (Issue 73): Pre-AI noise filter drops both exact and annotated out-of-scope news."""
        settings = Settings(
            ai_batch_size=20,
            min_discovery_priority_for_ai=4.0,
            ai_evaluation_enabled=True,
            telegram_delivery_enabled=False,
        )
        # Articles triggering annotated out-of-scope dampening
        art_politics = Article.create(
            "Kabine Toplantısı Sona Erdi",
            "https://news.com/politics-1",
            "Haber A",
            self.now,
            snippet="Cumhurbaşkanlığı Kabinesi toplantısı sonrası yeni vergi düzenlemesi ve enflasyon kararları açıklandı.",
        )
        art_sports = Article.create(
            "Where to Watch Champions League Final Game Online Free",
            "https://news.com/sports-1",
            "Haber B",
            self.now,
            snippet="Live stream guide, where to watch, and starting lineup details.",
        )
        art_religion = Article.create(
            "Vatikan'dan Yeni Genelge Açıklandı",
            "https://news.com/religion-1",
            "Haber C",
            self.now,
            snippet="Katolik dünyası ve Papa yeni doktrin genelgesini yayımladı.",
        )
        art_blotter = Article.create(
            "Metro Hattı İhalesi Tamamlandı",
            "https://news.com/blotter-1",
            "Haber D",
            self.now,
            snippet="Şehir içi metro hattı ihalesi tamamlandı ve çeyrek bilançosu açıklandı.",
        )
        art_essay = Article.create(
            "What Second Acts Can Teach You About Life",
            "https://news.com/essay-1",
            "Haber E",
            self.now,
            snippet="How corporate executives can teach you about second acts in life.",
        )
        # Valid entertainment article that must survive
        art_valid = Article.create(
            "Hadise Yeni Albümünü ve Turnesini Resmen Açıkladı",
            "https://news.com/ent-valid",
            "Haber F",
            self.now,
            snippet="Ünlü şarkıcı Hadise yeni pop albümünü ve Avrupa turne tarihlerini duyurdu.",
        )

        all_articles = [art_politics, art_sports, art_religion, art_blotter, art_essay, art_valid]
        evaluated_candidates = []
        class RecordingAIEngine(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                evaluated_candidates.extend(candidates)
                return []

        orch = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=all_articles, successes=1),
            ai_engine=RecordingAIEngine(),
            notifier=MockNotifier(),
        )

        result = orch.run(now=self.now)
        self.assertTrue(result.success)

        # Only art_valid should reach AI evaluation; all 5 out-of-scope articles are dropped as noise
        evaluated_ids = {c.id for c in evaluated_candidates}
        self.assertIn(art_valid.id, evaluated_ids, "Valid entertainment article should reach AI evaluation")
        for noise_art in [art_politics, art_sports, art_religion, art_blotter, art_essay]:
            self.assertNotIn(
                noise_art.id,
                evaluated_ids,
                f"Expected {noise_art.title} to be filtered out before AI evaluation",
            )
        self.assertEqual(result.candidates_evaluated, 1)

    def test_pre_ai_noise_drop_preserves_sensitive_tragedy_and_health_counterexamples(self):
        """Fix B (Issue 73): Pre-AI noise filter must NOT drop sensitive tragedy or health news,
        allowing them to reach AI and SENSITIVE_REVIEW / HOLD queues."""
        settings = Settings(
            ai_batch_size=20,
            min_discovery_priority_for_ai=4.0,
            ai_evaluation_enabled=True,
            telegram_delivery_enabled=False,
        )
        art_tragedy = Article.create(
            "Liam Payne 3. kattan düşerek hayatını kaybetti",
            "https://news.com/liam-payne",
            "Haber A",
            self.now,
            snippet="One Direction grubunun eski üyesi otel balkonundan düşerek yaşamını yitirdi.",
        )
        art_funeral = Article.create(
            "Usta sanatçının cenaze töreni yarın Teşvikiye Camii'nde yapılacak",
            "https://news.com/usta-cenaze",
            "Haber B",
            self.now,
            snippet="Sanat dünyası acı haberle sarsıldı; cenaze töreni yarın öğle namazını müteakip düzenlenecek.",
        )
        art_health = Article.create(
            "Kurgusal Sanatçı Kalp Krizi Geçirerek Acilen Yoğun Bakıma Alındı",
            "https://news.com/sanatci-icu",
            "Haber C",
            self.now,
            snippet="Sanatçı aniden fenalaşarak acil ameliyata alındı ve yoğun bakımda tedavi görüyor.",
        )

        evaluated_candidates = []
        class RecordingAIEngine(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                evaluated_candidates.extend(candidates)
                return []

        orch = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_tragedy, art_funeral, art_health], successes=1),
            ai_engine=RecordingAIEngine(),
            notifier=MockNotifier(),
        )

        result = orch.run(now=self.now)
        self.assertTrue(result.success)

        # All 3 sensitive articles must have reached AI evaluation
        evaluated_ids = {c.id for c in evaluated_candidates}
        for art in [art_tragedy, art_funeral, art_health]:
            self.assertIn(art.id, evaluated_ids, f"{art.title} was omitted from AI evaluation")
        self.assertEqual(result.candidates_evaluated, 3)

    def test_is_out_of_scope_candidate_unit_contract(self):
        """Fix B (Issue 73): Unit contract for RadarOrchestrator.is_out_of_scope_candidate."""
        normal_art = Article.create("Hadise Klip Çekti", "https://a.com/1", "A", self.now, snippet="Normal klip.")
        politics_art = Article.create("Enflasyon Açıklandı", "https://a.com/2", "A", self.now, snippet="Bakanlık açıkladı.")
        tragedy_art = Article.create("Liam Payne hayatını kaybetti", "https://a.com/3", "A", self.now, snippet="Otelden düşerek yaşamını yitirdi.")
        health_art = Article.create("Sanatçı yoğun bakıma alındı", "https://a.com/4", "A", self.now, snippet="Acil servise kaldırıldı ve entübe edildi.")

        # Exact and annotated out-of-scope noise without sensitive keywords -> True
        self.assertTrue(RadarOrchestrator.is_out_of_scope_candidate(politics_art, ["out_of_scope_or_grim_news_dampened"]))
        self.assertTrue(RadarOrchestrator.is_out_of_scope_candidate(politics_art, ["out_of_scope_or_grim_news_dampened(politics_policy)"]))
        self.assertTrue(RadarOrchestrator.is_out_of_scope_candidate(politics_art, ["out_of_scope_or_grim_news_dampened(religion_clerical)"]))
        self.assertTrue(RadarOrchestrator.is_out_of_scope_candidate(politics_art, ["out_of_scope_or_grim_news_dampened(sports_watch_guide)"]))
        self.assertTrue(RadarOrchestrator.is_out_of_scope_candidate(politics_art, ["out_of_scope_or_grim_news_dampened(non_celebrity_blotter_or_geo)"]))
        self.assertTrue(RadarOrchestrator.is_out_of_scope_candidate(politics_art, ["out_of_scope_or_grim_news_dampened(generic_essay_or_b2b)"]))

        # tragic_local_mortality alone must NOT be classified as noise
        self.assertFalse(RadarOrchestrator.is_out_of_scope_candidate(normal_art, ["out_of_scope_or_grim_news_dampened(tragic_local_mortality)"]))

        # Sensitive tragedy and health content must NEVER be dropped as noise even if reason present
        self.assertFalse(RadarOrchestrator.is_out_of_scope_candidate(tragedy_art, ["out_of_scope_or_grim_news_dampened"]))
        self.assertFalse(RadarOrchestrator.is_out_of_scope_candidate(tragedy_art, ["out_of_scope_or_grim_news_dampened(non_celebrity_blotter_or_geo)"]))
        self.assertFalse(RadarOrchestrator.is_out_of_scope_candidate(health_art, ["out_of_scope_or_grim_news_dampened"]))

        # Regular entertainment news -> False
        self.assertFalse(RadarOrchestrator.is_out_of_scope_candidate(normal_art, ["core_turkish_entertainment", "has_visual_image"]))


if __name__ == "__main__":
    unittest.main()
