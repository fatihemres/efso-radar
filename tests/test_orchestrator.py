"""Tests for RadarOrchestrator pipeline."""
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
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



    def test_celebrity_legal_development_with_policy_phrase_survives_pre_ai_filter(self):
        """Regression B (PR #74 review): Editorially relevant celebrity legal news
        containing incidental policy wording ('vergi düzenlemesi') must NOT be dropped
        as noise before AI evaluation; it must reach AI evaluation while preserving
        downstream post-AI processing contracts."""
        settings = Settings(
            ai_batch_size=20,
            min_discovery_priority_for_ai=4.0,
            ai_evaluation_enabled=True,
            telegram_delivery_enabled=False,
        )
        # Celebrity legal story with a policy phrase ("vergi düzenlemesi")
        art_legal = Article.create(
            "Ünlü Komedyen Cem Yılmaz Hakkında Vergi Düzenlemesi Davası: Mahkemeden Karar Çıktı",
            "https://haber.com/cem-yilmaz-vergi-davasi-karar",
            "Haber Türk",
            self.now,
            snippet="Cem Yılmaz'ın şirketine yönelik vergi düzenlemesi davasında mahkeme beraat kararı verdi.",
        )
        # Unrelated general politics article that MUST still be dropped
        art_unrelated_politics = Article.create(
            "Hazine ve Maliye Bakanlığı Vergi Düzenlemesi Paketini Açıkladı",
            "https://haber.com/bakanlik-vergi-paketi",
            "Ekonomi Haber",
            self.now,
            snippet="Yeni vergi düzenlemesi ile gelir vergisi dilimleri ve kurumlar vergisi oranları güncellendi.",
        )

        evaluated_candidates = []
        class RecordingAIEngine(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                evaluated_candidates.extend(candidates)
                return []

        orch = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art_legal, art_unrelated_politics], successes=1),
            ai_engine=RecordingAIEngine(),
            notifier=MockNotifier(),
        )

        result = orch.run(now=self.now)
        self.assertTrue(result.success)

        # 1. Before AI evaluation: art_legal must NOT be filtered out through noise_dropped; it must reach AI!
        evaluated_ids = {c.id for c in evaluated_candidates}
        self.assertIn(art_legal.id, evaluated_ids, "Celebrity legal story must reach AI evaluation!")

        # Unrelated general politics must be filtered out before AI
        self.assertNotIn(art_unrelated_politics.id, evaluated_ids, "Unrelated politics must be filtered before AI")

        # 2. After AI evaluation: art_legal was evaluated, so repository marked it processed as an evaluated candidate
        self.assertTrue(self.repo.is_article_processed(art_legal.id), "Evaluated article should be marked processed at step 9")
        self.assertEqual(result.candidates_evaluated, 1)



    def test_in_scope_low_priority_candidate_soft_deferred_not_prematurely_processed(self):
        """Issue #75: An in-scope entertainment candidate excluded solely due to high
        volume (> ai_batch_size) and low priority score (< min_discovery_priority_for_ai)
        must enter candidate_backlog as QUEUED and NOT be marked processed prematurely.
        When competition decreases on a subsequent run, it must be recovered and evaluated."""
        settings = Settings(
            ai_batch_size=10,
            min_discovery_priority_for_ai=5.5,
            ai_evaluation_enabled=True,
            telegram_delivery_enabled=False,
            max_candidates=55,
        )

        # 10 high-priority entertainment candidates with strong affinity entities (score > 6.0)
        titles = [
            ("Tarkan Yeni Şarkısını Duyurdu", "Tarkan"),
            ("Hadise Harbiye Açıkhava Konseri İçin Hazırlanıyor", "Hadise"),
            ("Kıvanç Tatlıtuğ Başrollü Yeni Dizi Başlıyor", "Kıvanç Tatlıtuğ"),
            ("Serenay Sarıkaya Paris Moda Haftasında Boy Gösterdi", "Serenay Sarıkaya"),
            ("Kenan İmirzalıoğlu Yeni Sezon Açıklaması Yaptı", "Kenan İmirzalıoğlu"),
            ("Ezhel Dünya Turnesi Tarihlerini Açıkladı", "Ezhel"),
            ("Cem Yılmaz Stand-up Turnesi Başlıyor", "Cem Yılmaz"),
            ("Ata Demirer Yeni Komedi Filmi Çekimlerine Başladı", "Ata Demirer"),
            ("Mabel Matiz Yeni Şarkı Klibini Yayınladı", "Mabel Matiz"),
            ("Zeynep Bastık Yeni Akustik Albümünü Duyurdu", "Zeynep Bastık"),
        ]
        high_priority_articles = [
            Article.create(
                title,
                f"https://haber.com/hp-{i}",
                f"Kaynak {i}",
                self.now,
                snippet=f"{name} yeni çalışmaları hakkında açıklama yaptı.",
            )
            for i, (title, name) in enumerate(titles)
        ]

        # 1 in-scope entertainment candidate with low priority (score < 5.5, e.g. unknown indie film note)
        low_priority_in_scope = Article.create(
            "Bağımsız Kısa Film Gösterimi Hafta Sonu Başlıyor",
            "https://haber.com/bagimsiz-kisa-film-gosterimi",
            "Kültür Sanat Bülteni",
            self.now,
            snippet="Genç yönetmenlerin kısa film çalışmaları hafta sonu sinemaseverlerle buluşuyor.",
        )

        # 1 genuinely out-of-scope candidate (politics/policy)
        out_of_scope_politics = Article.create(
            "Hazine ve Maliye Bakanlığı Vergi Düzenlemesi Paketini Açıkladı",
            "https://haber.com/bakanlik-vergi-paketi",
            "Ekonomi Haber",
            self.now,
            snippet="Yeni vergi düzenlemesi ile gelir vergisi dilimleri ve kurumlar vergisi oranları güncellendi.",
        )

        all_articles = high_priority_articles + [low_priority_in_scope, out_of_scope_politics]

        evaluated_run1 = []
        class RecordingAIEngineRun1(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                evaluated_run1.extend(candidates)
                return []

        orch1 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=all_articles, successes=1),
            ai_engine=RecordingAIEngineRun1(),
            notifier=MockNotifier(),
        )

        res1 = orch1.run(now=self.now)
        self.assertTrue(res1.success)

        # 1. Exactly 10 high priority articles were evaluated by AI in Run 1
        self.assertEqual(res1.candidates_evaluated, 10)
        self.assertEqual(len(evaluated_run1), 10)

        # 2. Out-of-scope politics article was hard-rejected and marked processed
        self.assertTrue(self.repo.is_article_processed(out_of_scope_politics.id))

        # 3. Low-priority in-scope article must NOT be marked processed prematurely!
        self.assertFalse(
            self.repo.is_article_processed(low_priority_in_scope.id),
            "In-scope low priority candidate must NOT be marked processed before AI evaluation",
        )

        # 4. Low-priority in-scope article must be persisted in durable backlog as QUEUED!
        pending = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        pending_ids = {a.id for a in pending}
        self.assertIn(
            low_priority_in_scope.id,
            pending_ids,
            "In-scope low priority candidate must be queued in candidate_backlog",
        )
        self.assertNotIn(
            out_of_scope_politics.id,
            pending_ids,
            "Out-of-scope politics article must NOT be queued in candidate_backlog",
        )

        # 5. Run 2: Publisher feed returns NO new articles. Competition drops.
        # The queued in-scope article is recovered from backlog and evaluated!
        evaluated_run2 = []
        class RecordingAIEngineRun2(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                evaluated_run2.extend(candidates)
                return []

        orch2 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=[], successes=1),
            ai_engine=RecordingAIEngineRun2(),
            notifier=MockNotifier(),
        )

        res2 = orch2.run(now=self.now)
        self.assertTrue(res2.success)
        self.assertEqual(res2.candidates_evaluated, 1)
        self.assertEqual(len(evaluated_run2), 1)
        self.assertEqual(evaluated_run2[0].id, low_priority_in_scope.id)

        # 6. After evaluation in Run 2, it is marked processed and evaluated
        self.assertTrue(self.repo.is_article_processed(low_priority_in_scope.id))
        pending_after_run2 = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        self.assertNotIn(low_priority_in_scope.id, {a.id for a in pending_after_run2})

        # 7. Run 3: Remained silent, no double evaluation
        evaluated_run3 = []
        class RecordingAIEngineRun3(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                evaluated_run3.extend(candidates)
                return []

        orch3 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=[], successes=1),
            ai_engine=RecordingAIEngineRun3(),
            notifier=MockNotifier(),
        )
        res3 = orch3.run(now=self.now)
        self.assertTrue(res3.success)
        self.assertEqual(res3.candidates_evaluated, 0)
        self.assertEqual(len(evaluated_run3), 0)



    def test_sustained_high_volume_deferred_candidate_expires_after_24h_without_infinite_loop(self):
        """Safety contract: Under sustained high-volume candidate input (> ai_batch_size),
        in-scope low-priority candidates remain deferred without entering an infinite evaluation loop.
        Once 24 hours have elapsed since their publication, they naturally expire and exit the backlog."""
        settings = Settings(
            ai_batch_size=10,
            min_discovery_priority_for_ai=5.5,
            ai_evaluation_enabled=True,
            telegram_delivery_enabled=False,
            max_candidates=55,
        )

        titles_10 = [
            ("Tarkan Yeni Pop Şarkısını Çıkardı", "Tarkan"),
            ("Hadise Harbiye Açıkhava Sahnesinde Konser Verdi", "Hadise"),
            ("Kıvanç Tatlıtuğ Başrollü Sinema Filmi Vizyona Girdi", "Kıvanç Tatlıtuğ"),
            ("Serenay Sarıkaya Moda Haftasında Kırmızı Halıda Yürüdü", "Serenay Sarıkaya"),
            ("Kenan İmirzalıoğlu Yeni Televizyon Sezonu İçin Anlaştı", "Kenan İmirzalıoğlu"),
            ("Ezhel Avrupa Turnesi Kapsamında Berlin'de Sahne Aldı", "Ezhel"),
            ("Cem Yılmaz Yeni Stand-up Gösterisinin Prömiyerini Yaptı", "Cem Yılmaz"),
            ("Ata Demirer Komedi Filminin Çekimlerini Tamamladı", "Ata Demirer"),
            ("Mabel Matiz Klip Çekimleri İçin Kapadokya'ya Gitti", "Mabel Matiz"),
            ("Zeynep Bastık Akustik Konser Kayıtlarını Tamamladı", "Zeynep Bastık"),
        ]

        def make_high_priority_batch(now_time, run_index):
            return [
                Article.create(
                    f"{title} - Kaynak Haber {run_index}",
                    f"https://haber.com/hp-r{run_index}-{i}",
                    f"Kaynak {i}",
                    now_time,
                    snippet=f"{name} yeni çalışmaları hakkında açıklama yaptı.",
                )
                for i, (title, name) in enumerate(titles_10)
            ]

        # In-scope low priority candidate published at T=0
        low_priority_art = Article.create(
            "Bağımsız Kısa Film Gösterimi Hafta Sonu Başlıyor",
            "https://haber.com/bagimsiz-kisa-film-gosterimi-loop",
            "Kültür Sanat Bülteni",
            self.now,
            snippet="Genç yönetmenlerin kısa film çalışmaları hafta sonu sinemaseverlerle buluşuyor.",
        )

        evaluated_ids = set()
        class RecordingAIEngine(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                evaluated_ids.update(c.id for c in candidates)
                return []

        # Run 1 at T=0h: High volume (10 high priority) + 1 low priority art (total 11)
        orch1 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=make_high_priority_batch(self.now, 1) + [low_priority_art], successes=1),
            ai_engine=RecordingAIEngine(),
            notifier=MockNotifier(),
        )
        res1 = orch1.run(now=self.now)
        self.assertTrue(res1.success)
        self.assertEqual(res1.candidates_evaluated, 10)
        self.assertNotIn(low_priority_art.id, evaluated_ids)
        self.assertFalse(self.repo.is_article_processed(low_priority_art.id))

        # Run 2 at T=12h: High volume continues. Source delivers new articles; backlog retains low_priority_art.
        # It is still deferred because competition is >= 10 and priority < 5.5.
        t_12h = self.now + timedelta(hours=12)
        orch2 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=make_high_priority_batch(t_12h, 2), successes=1),
            ai_engine=RecordingAIEngine(),
            notifier=MockNotifier(),
        )
        res2 = orch2.run(now=t_12h)
        self.assertTrue(res2.success)
        self.assertEqual(res2.candidates_evaluated, 10)
        self.assertNotIn(low_priority_art.id, evaluated_ids)
        self.assertFalse(self.repo.is_article_processed(low_priority_art.id))

        # Run 3 at T=25h: Stale backlog candidates are expired in Step 2 (>24h).
        # low_priority_art transitions to EXPIRED and is not selected for evaluation.
        t_25h = self.now + timedelta(hours=25)
        orch3 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=make_high_priority_batch(t_25h, 3), successes=1),
            ai_engine=RecordingAIEngine(),
            notifier=MockNotifier(),
        )
        res3 = orch3.run(now=t_25h)
        self.assertTrue(res3.success)
        self.assertNotIn(low_priority_art.id, evaluated_ids)

        # Confirm candidate is expired in backlog and no longer pending
        pending = self.repo.get_pending_candidates(max_age_hours=24, now=t_25h)
        self.assertNotIn(low_priority_art.id, {a.id for a in pending})

    def test_ordinary_deferred_candidate_cannot_piggyback_on_update_retention(self):
        """Verifies that an ordinary deferred entertainment candidate cannot bypass the
        24-hour expiration rule, unlike genuine editorial UPDATE candidates."""
        settings = Settings(
            ai_batch_size=10,
            min_discovery_priority_for_ai=5.5,
            ai_evaluation_enabled=True,
            telegram_delivery_enabled=False,
            max_candidates=55,
        )

        # Ordinary deferred candidate published 30 hours ago
        art_ordinary = Article.create(
            "Eski Dizi Bölüm Değerlendirmesi",
            "https://haber.com/eski-dizi",
            "Dizi Bülteni",
            self.now - timedelta(hours=30),
            snippet="Eski dizinin geçmiş sezon bölümlerine ilişkin değerlendirme.",
            raw_metadata={"priority_score": 4.0},
        )
        # Legitimate UPDATE candidate published 30 hours ago
        art_update = Article.create(
            "Festival Programı İptal Güncellemesi",
            "https://haber.com/festival-update",
            "Festival Haber",
            self.now - timedelta(hours=30),
            snippet="Daha önce duyurulan festival programında son dakika değişikliği yapıldı.",
            raw_metadata={"kind": "UPDATE", "priority_score": 10.0},
        )

        self.repo.save_pending_candidates([art_ordinary, art_update])

        # Expire stale candidates at max_age_hours=24
        expired = self.repo.expire_stale_backlog_candidates(max_age_hours=24, now=self.now)
        self.assertEqual(expired, 1)

        pending = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        pending_ids = {a.id for a in pending}
        self.assertNotIn(art_ordinary.id, pending_ids, "Ordinary candidate >24h must be expired")
        self.assertIn(art_update.id, pending_ids, "UPDATE candidate must be retained despite age")


    def test_negative_regression_soft_deferred_candidate_with_update_hint_escapes_24h_expiration(self):
        """Acceptance test (Issue #75 Review B & PO Decision A.1+B.3):
        Verifies that soft-deferred candidates with an UPDATE hint but persisted priority below
        effective min_discovery_priority_for_ai strictly expire after the normal 24-hour window,
        while high-priority UPDATE candidates (priority >= min_discovery_priority_for_ai) retain
        their age exemption."""
        settings = Settings(
            ai_batch_size=10,
            min_discovery_priority_for_ai=5.5,
            ai_evaluation_enabled=True,
            telegram_delivery_enabled=False,
            max_candidates=55,
        )

        titles_10 = [
            ("Tarkan Yeni Pop Şarkısını Çıkardı", "Tarkan"),
            ("Hadise Harbiye Açıkhava Sahnesinde Konser Verdi", "Hadise"),
            ("Kıvanç Tatlıtuğ Başrollü Sinema Filmi Vizyona Girdi", "Kıvanç Tatlıtuğ"),
            ("Serenay Sarıkaya Moda Haftasında Kırmızı Halıda Yürüdü", "Serenay Sarıkaya"),
            ("Kenan İmirzalıoğlu Yeni Televizyon Sezonu İçin Anlaştı", "Kenan İmirzalıoğlu"),
            ("Ezhel Avrupa Turnesi Kapsamında Berlin'de Sahne Aldı", "Ezhel"),
            ("Cem Yılmaz Yeni Stand-up Gösterisinin Prömiyerini Yaptı", "Cem Yılmaz"),
            ("Ata Demirer Komedi Filminin Çekimlerini Tamamladı", "Ata Demirer"),
            ("Mabel Matiz Klip Çekimleri İçin Kapadokya'ya Gitti", "Mabel Matiz"),
            ("Zeynep Bastık Akustik Konser Kayıtlarını Tamamladı", "Zeynep Bastık"),
        ]

        def make_high_priority_batch(now_time, run_index):
            return [
                Article.create(
                    f"{title} - Kaynak Haber {run_index}",
                    f"https://haber.com/hp-r{run_index}-{i}",
                    f"Kaynak {i}",
                    now_time,
                    snippet=f"{name} yeni çalışmaları hakkında açıklama yaptı.",
                )
                for i, (title, name) in enumerate(titles_10)
            ]

        # In-scope, low-scoring candidate with an UPDATE hint (score 4.5 < 5.5)
        low_priority_update_art = Article.create(
            "Bağımsız Kısa Film Gösterimi Hafta Sonu Başlıyor",
            "https://haber.com/bagimsiz-kisa-film-gosterimi-update",
            "Kültür Sanat Bülteni",
            self.now,
            snippet="Genç yönetmenlerin kısa film çalışmaları hafta sonu sinemaseverlerle buluşuyor.",
            source_category="UPDATE",
        )

        # High-scoring candidate with an UPDATE hint (score 8.0 >= 5.5)
        high_priority_update_art = Article.create(
            "Festival Programı İptal Güncellemesi",
            "https://haber.com/festival-update",
            "Festival Haber",
            self.now,
            snippet="Daha önce duyurulan festival programında son dakika değişikliği yapıldı.",
            source_category="UPDATE",
            raw_metadata={"priority_score": 8.0},
        )

        class MockAI(AIEditorialEngine):
            def evaluate(self, candidates, recent_stories, now):
                return []

        # Run 1 at T=0h: 10 high priority + 1 low priority with UPDATE hint
        orch1 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=make_high_priority_batch(self.now, 1) + [low_priority_update_art], successes=1),
            ai_engine=MockAI(),
            notifier=MockNotifier(),
        )
        res1 = orch1.run(now=self.now)
        self.assertTrue(res1.success)
        self.assertEqual(res1.candidates_evaluated, 10)
        self.assertFalse(self.repo.is_article_processed(low_priority_update_art.id))

        # Also register high_priority_update_art into backlog at T=0h
        self.repo.save_pending_candidates([high_priority_update_art])

        # Confirmed: Both were soft-deferred/persisted into candidate_backlog with status QUEUED
        pending_t0 = self.repo.get_pending_candidates(
            max_age_hours=24,
            now=self.now,
            min_update_priority=settings.min_discovery_priority_for_ai,
        )
        pending_t0_ids = {a.id for a in pending_t0}
        self.assertIn(low_priority_update_art.id, pending_t0_ids)
        self.assertIn(high_priority_update_art.id, pending_t0_ids)

        # Run 2 at T=26h: Synthetic time moves beyond 24h retention window (>24h).
        # High volume of fresh news arrives; neither update candidate is in the new RSS batch.
        t_26h = self.now + timedelta(hours=26)
        orch2 = RadarOrchestrator(
            settings=settings,
            repository=self.repo,
            source=MockNewsSource(articles=make_high_priority_batch(t_26h, 2), successes=1),
            ai_engine=MockAI(),
            notifier=MockNotifier(),
        )
        res2 = orch2.run(now=t_26h)
        self.assertTrue(res2.success)

        # Acceptance check (B.3):
        # 1. low_priority_update_art (score 4.5 < 5.5) must be EXPIRED and omitted from pending
        # 2. high_priority_update_art (score 8.0 >= 5.5) retains genuine UPDATE age exemption
        pending_t26 = self.repo.get_pending_candidates(
            max_age_hours=24,
            now=t_26h,
            min_update_priority=settings.min_discovery_priority_for_ai,
        )
        pending_t26_ids = {a.id for a in pending_t26}
        self.assertNotIn(
            low_priority_update_art.id,
            pending_t26_ids,
            "Low-priority candidate with UPDATE hint must expire after 24 hours",
        )
        self.assertIn(
            high_priority_update_art.id,
            pending_t26_ids,
            "High-priority candidate with UPDATE hint must retain age exemption",
        )

        with self.repo._connection() as conn:
            row_low = conn.execute(
                "SELECT status FROM candidate_backlog WHERE article_id = ?",
                (low_priority_update_art.id,),
            ).fetchone()
            self.assertEqual(row_low["status"], "EXPIRED")

            row_high = conn.execute(
                "SELECT status FROM candidate_backlog WHERE article_id = ?",
                (high_priority_update_art.id,),
            ).fetchone()
            self.assertEqual(row_high["status"], "QUEUED")


if __name__ == "__main__":
    unittest.main()
