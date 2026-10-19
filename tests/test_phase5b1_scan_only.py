"""Automated tests for Phase 5B.1: Persistent State & Safe Scan-Only RSS Pipeline.

Verifies:
1. Complete isolation of scan-only pipeline from Telegram delivery and Gemini AI inference.
2. Zero-cost guarantee ($0.00 / 0 TRY, 0 Gemini calls, 0 alerts dispatched).
3. Pending quiet-hours notifications are NOT delivered during scan-only runs.
4. Candidate backlog persistence with priority scoring.
5. Repeat ingestion idempotency (duplicate RSS runs do not duplicate backlog items).
6. Backlog durability across worker and orchestrator restarts.
7. 24-hour editorial freshness rule: stale items (>24h) expire to EXPIRED, while UPDATEs are preserved.
8. Safe dry-run behavior: zero database mutations and zero locks acquired.
9. Resilient partial and complete RSS feed failure handling.
10. CLI entry point `python src/radar.py --scan-only` and structured summary logging.
11. Comprehensive secret masking for logging and auditing.
12. Parity between SQLite and Turso libSQL repositories.
"""
from __future__ import annotations

import hashlib
import io
import logging
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    Market,
    NotificationRecord,
    Story,
    StoryCategory,
    SuggestedFormat,
    VerificationStatus,
    utc_now,
)
from efso_radar.notifications.base import NotificationChannel
from efso_radar.orchestrator import RadarOrchestrator, RadarRunResult
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from efso_radar.sources.base import NewsSource
from efso_radar.sources.models import DiscoveryMetrics
import radar
from tests.test_repository_parity import create_mock_hrana_transport


class MockNewsSource(NewsSource):
    """Hermetic news source returning deterministic synthetic articles."""

    def __init__(self, articles: Optional[list[Article]] = None, successes: int = 1):
        self.articles = articles or []
        self.successes = successes
        self.last_metrics = DiscoveryMetrics(
            total_fetched=len(self.articles),
            successful_sources=successes,
            failed_sources=0 if successes > 0 else 3,
            parse_errors=0,
            duration_seconds=0.05,
        )

    def fetch_candidates(self, now: datetime, max_age_hours: int = 16) -> tuple[list[Article], int]:
        return list(self.articles), self.successes


class FailingAIEngine:
    """Mock AI engine that raises an assertion error if invoked, guaranteeing $0.00 / 0 calls."""

    def evaluate_candidates(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("CRITICAL VIOLATION: AI inference was invoked during scan-only mode!")


class RecordingNotifier(NotificationChannel):
    """Records any notification dispatch attempts to detect unauthorized leaks."""

    def __init__(self):
        self.sent_records: list[Any] = []

    def send(
        self,
        assessment: EditorialAssessment,
        story_id: str,
        update_id: Optional[str] = None,
        dry_run: bool = False,
        now: Optional[datetime] = None,
    ) -> NotificationRecord:
        self.sent_records.append(("send", story_id, assessment))
        raise AssertionError("CRITICAL VIOLATION: Notification channel send was called during scan-only mode!")

    def _dispatch_telegram(self, *args: Any, **kwargs: Any) -> Any:
        self.sent_records.append(("_dispatch_telegram", args, kwargs))
        raise AssertionError("CRITICAL VIOLATION: Telegram dispatch was called during scan-only mode!")


class TestPhase5B1ScanOnlyPipeline(unittest.TestCase):
    """Hermetic test suite verifying Phase 5B.1 persistent scan-only discovery."""

    def setUp(self):
        self.now = datetime(2026, 10, 19, 12, 17, tzinfo=timezone.utc)
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "radar_test.db"
        self.settings = Settings(
            sqlite_db_path=self.db_path,
            ai_evaluation_enabled=False,
            telegram_delivery_enabled=False,
            quiet_hours_enabled=False,
        )
        self.repo = SqliteStateRepository(self.db_path)

    def tearDown(self):
        try:
            self.tmp_dir.cleanup()
        except Exception:
            pass

    def test_scan_only_isolated_from_telegram_and_gemini(self):
        """Scan-only mode MUST NOT dispatch held quiet-hours alerts, flush digests, or invoke Gemini."""
        # Pre-seed a held quiet-hours notification that would normally be dispatched outside quiet hours
        held_notif = NotificationRecord(
            notification_id="NOTIF-HELD-001",
            story_id="STORY-HELD-001",
            kind=EditorialKind.NEW,
            sent_at=self.now - timedelta(hours=2),
            status=DeliveryStatus.HELD_QUIET_HOURS,
            message_text="[HELD] Acil magazin haberi",
            headline_tr="Acil Magazin Haberi",
            score=95,
        )
        self.repo.record_notification(held_notif)

        # Pre-seed a queued digest notification
        digest_notif = NotificationRecord(
            notification_id="DIGEST-Q-001",
            story_id="STORY-DIGEST-001",
            kind=EditorialKind.NEW,
            sent_at=self.now - timedelta(hours=3),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="[DIGEST] Günün özeti maddesi",
            headline_tr="Günün Özeti Maddesi",
            score=84,
        )
        self.repo.record_notification(digest_notif)

        # Synthetic fresh article from RSS
        art = Article.create(
            title="Tarkan 2026 Harbiye konser tarihlerini açıkladı",
            url="https://haberturk.com/tarkan-konser-2026",
            source="Habertürk",
            published_at=self.now - timedelta(hours=1),
            snippet="Megastar Tarkan Harbiye Açıkhava konser takvimini resmen ilan etti.",
        )

        notifier = RecordingNotifier()
        ai_engine = FailingAIEngine()
        source = MockNewsSource(articles=[art], successes=1)

        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=source,
            ai_engine=ai_engine,  # type: ignore
            notifier=notifier,
        )

        # Execute scan-only run outside quiet hours (12:17 UTC)
        result = orch.run_scan_only(now=self.now)

        # Assertions for 0 notifications and 0 AI evaluations
        self.assertTrue(result.success)
        self.assertEqual(result.articles_collected, 1)
        self.assertEqual(result.candidates_evaluated, 0, "Scan-only must strictly evaluate 0 candidates with AI")
        self.assertEqual(result.picks_approved, 0)
        self.assertEqual(result.notifications_dispatched, 0, "Scan-only must strictly dispatch 0 notifications")
        self.assertEqual(len(notifier.sent_records), 0, "No dispatch requests should ever reach the notifier")

        # Held notifications in repository must remain intact and NOT dispatched
        held_in_db = self.repo.get_held_notifications(max_age_hours=24, now=self.now)
        self.assertEqual(len(held_in_db), 1)
        self.assertEqual(held_in_db[0].notification_id, "NOTIF-HELD-001")
        self.assertEqual(held_in_db[0].status, DeliveryStatus.HELD_QUIET_HOURS)

        queued_digest_in_db = self.repo.get_queued_digest_notifications()
        self.assertEqual(len(queued_digest_in_db), 1)
        self.assertEqual(queued_digest_in_db[0].notification_id, "DIGEST-Q-001")

        # Fresh article must be safely queued in candidate_backlog
        pending = self.repo.get_pending_candidates(now=self.now)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].id, art.id)
        self.assertEqual(pending[0].title, art.title)

    def test_ai_token_ledger_untouched(self):
        """Scan-only pipeline must never write to ai_usage_ledger, maintaining strict $0.00 cost."""
        art = Article.create("Yeni albüm çıktı", "https://url.com/album", "Milliyet", self.now)
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art]),
            ai_engine=FailingAIEngine(),  # type: ignore
            notifier=RecordingNotifier(),
        )

        orch.run_scan_only(now=self.now)

        usage = self.repo.get_monthly_ai_usage("2026-10")
        self.assertEqual(usage["call_count"], 0)
        self.assertEqual(usage["input_tokens"], 0)
        self.assertEqual(usage["output_tokens"], 0)
        self.assertEqual(float(usage["total_cost_usd"]), 0.0)

    def test_repeat_ingestion_idempotency(self):
        """Scanning the same RSS articles across repeated scheduled runs must NOT duplicate backlog entries."""
        articles = [
            Article.create("Haber 1: Festival başladı", "https://news.com/fest", "Milliyet", self.now),
            Article.create("Haber 2: Film vizyona girdi", "https://news.com/film", "Hürriyet", self.now),
        ]
        source = MockNewsSource(articles=articles, successes=1)
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=source,
            ai_engine=FailingAIEngine(),  # type: ignore
            notifier=RecordingNotifier(),
        )

        # Run 1
        res1 = orch.run_scan_only(now=self.now)
        self.assertTrue(res1.success)
        self.assertEqual(len(self.repo.get_pending_candidates(now=self.now)), 2)

        # Run 2 (3 hours later, identical RSS feed content)
        res2 = orch.run_scan_only(now=self.now + timedelta(hours=3))
        self.assertTrue(res2.success)

        # Backlog must still have exactly 2 candidates without duplication
        pending = self.repo.get_pending_candidates(now=self.now + timedelta(hours=3))
        self.assertEqual(len(pending), 2)
        self.assertEqual({p.id for p in pending}, {a.id for a in articles})

    def test_backlog_survives_process_restart(self):
        """Candidates in backlog must survive complete worker or orchestrator restarts."""
        art = Article.create("Serenay Sarıkaya yeni proje", "https://url.com/serenay", "Milliyet", self.now)
        orch1 = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art]),
            ai_engine=FailingAIEngine(),  # type: ignore
            notifier=RecordingNotifier(),
        )
        orch1.run_scan_only(now=self.now)

        # Simulate fresh process startup with new repository and orchestrator instances
        fresh_repo = SqliteStateRepository(self.db_path)
        fresh_orch = RadarOrchestrator(
            settings=self.settings,
            repository=fresh_repo,
            source=MockNewsSource(articles=[]),
            ai_engine=FailingAIEngine(),  # type: ignore
            notifier=RecordingNotifier(),
        )

        pending = fresh_orch.repository.get_pending_candidates(now=self.now)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].id, art.id)

    def test_24h_freshness_rule_expires_stale_articles_except_updates(self):
        """Articles older than 24h must transition to EXPIRED, while genuine UPDATEs are preserved."""
        # 1. Fresh article (2 hours old)
        art_fresh = Article.create("Taze Haber", "https://url.com/fresh", "Milliyet", self.now - timedelta(hours=2))

        # 2. Stale article (>24h old, regular, not UPDATE)
        art_stale_regular = Article.create(
            "Eski Rutin Haber",
            "https://url.com/stale-regular",
            "Hürriyet",
            self.now - timedelta(hours=26),
            source_category="MAGAZIN_TR",
        )

        # 3. Stale article (>24h old, but genuine UPDATE)
        art_stale_update = Article.create(
            "Eski Ama Güncelleme Olan Haber",
            "https://url.com/stale-update",
            "Habertürk",
            self.now - timedelta(hours=28),
            source_category="UPDATE",
            raw_metadata={"kind": "UPDATE"},
        )

        self.repo.save_pending_candidates([art_fresh, art_stale_regular, art_stale_update])

        # Verify initial pending backlog contains fresh + update, but excludes >24h regular
        pending_before = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        pending_before_ids = {a.id for a in pending_before}
        self.assertIn(art_fresh.id, pending_before_ids)
        self.assertIn(art_stale_update.id, pending_before_ids)
        self.assertNotIn(art_stale_regular.id, pending_before_ids)

        # Run maintenance expiration
        expired_count = self.repo.expire_stale_backlog_candidates(max_age_hours=24, now=self.now)
        self.assertEqual(expired_count, 1, "Only the stale regular article should transition to EXPIRED")

        # Verify database status directly
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT article_id, status FROM candidate_backlog")
            statuses = dict(cur.fetchall())
        finally:
            conn.close()

        self.assertEqual(statuses[art_fresh.id], "QUEUED")
        self.assertEqual(statuses[art_stale_update.id], "QUEUED", "UPDATE must remain QUEUED despite being >24h old")
        self.assertEqual(statuses[art_stale_regular.id], "EXPIRED")

    def test_dry_run_leaves_database_completely_untouched(self):
        """--dry-run in scan-only mode must NOT acquire locks, save candidates, or mutate database state."""
        art = Article.create("Dry run test haberi", "https://url.com/dry", "Milliyet", self.now)
        orch = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art]),
            ai_engine=FailingAIEngine(),  # type: ignore
            notifier=RecordingNotifier(),
        )

        res = orch.run_scan_only(dry_run=True, now=self.now)
        self.assertTrue(res.success)
        self.assertTrue(res.dry_run)
        self.assertEqual(res.articles_collected, 1)

        # Candidate backlog must remain empty
        pending = self.repo.get_pending_candidates(now=self.now)
        self.assertEqual(len(pending), 0)

        # Run locks must be empty
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM run_locks")
            self.assertEqual(cur.fetchone()[0], 0)
        finally:
            conn.close()

    def test_feed_failure_resilience_partial_vs_complete(self):
        """Pipeline continues on partial feed failure; fails closed gracefully on complete feed outage."""
        # Partial failure (successes > 0)
        art = Article.create("Kısmi başarı haberi", "https://url.com/partial", "Milliyet", self.now)
        orch_partial = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[art], successes=1),
            ai_engine=FailingAIEngine(),  # type: ignore
            notifier=RecordingNotifier(),
        )
        res_partial = orch_partial.run_scan_only(now=self.now)
        self.assertTrue(res_partial.success)
        self.assertEqual(res_partial.sources_succeeded, 1)
        self.assertEqual(res_partial.articles_collected, 1)

        # Complete failure (successes == 0)
        orch_fail = RadarOrchestrator(
            settings=self.settings,
            repository=self.repo,
            source=MockNewsSource(articles=[], successes=0),
            ai_engine=FailingAIEngine(),  # type: ignore
            notifier=RecordingNotifier(),
        )
        res_fail = orch_fail.run_scan_only(now=self.now)
        self.assertFalse(res_fail.success)
        self.assertEqual(res_fail.sources_succeeded, 0)
        self.assertIn("All news feeds failed", res_fail.message)

    def test_radar_cli_scan_only_mode(self):
        """radar.run(scan_only=True) executes cleanly without errors and logs structured discovery summary."""
        with self.assertLogs("efso-radar", level="INFO") as log_capture:
            radar.run(dry_run=True, scan_only=True)

        log_output = "\n".join(log_capture.output)
        self.assertIn("=== EFSO RADAR SCAN-ONLY SUMMARY ===", log_output)
        self.assertIn("Strict 0 TRY / $0.00", log_output)
        self.assertIn("Zero Dispatch Guarantee", log_output)

    def test_secret_masking_in_config(self):
        """Config methods mask sensitive secrets to prevent accidental leakage in logs."""
        s = Settings(
            gemini_api_key="AIzaSyTest123456789SecretKey",
            turso_auth_token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.token.secret",
            turso_database_url="libsql://test-db.turso.io",
            telegram_bot_token="123456789:ABCdefGHIjklMNOpqrSTUvwxYZ",
            telegram_chat_id="987654321",
        )
        self.assertNotIn("SecretKey", s.get_masked_gemini_api_key())
        self.assertIn("...", s.get_masked_gemini_api_key())

        self.assertNotIn("secret", s.get_masked_turso_token())
        self.assertIn("...", s.get_masked_turso_token())

        self.assertNotIn("ABCdef", s.get_masked_telegram_token())
        self.assertIn("...", s.get_masked_telegram_token())

        self.assertNotIn("654", s.get_masked_chat_id())
        self.assertEqual(s.get_masked_chat_id(), "98...21")

    def test_turso_and_sqlite_parity_under_scan_only(self):
        """Turso (via Hrana v2 mock transport) and SQLite produce identical candidate backlog records."""
        mem_conn = sqlite3.connect(":memory:")
        turso_transport = create_mock_hrana_transport(mem_conn)
        turso_repo = TursoStateRepository(
            database_url="libsql://efso-test.turso.io",
            auth_token="test-token",
            transport=turso_transport,
        )

        art1 = Article.create("Parite Test Haberi 1", "https://url.com/par1", "Milliyet", self.now)
        art2 = Article.create(
            "Parite Güncelleme",
            "https://url.com/par2",
            "Hürriyet",
            self.now - timedelta(hours=30),
            raw_metadata={"kind": "UPDATE"},
        )

        # Save to both
        self.repo.save_pending_candidates([art1, art2])
        turso_repo.save_pending_candidates([art1, art2])

        # Retrieve from both
        sqlite_pending = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        turso_pending = turso_repo.get_pending_candidates(max_age_hours=24, now=self.now)

        self.assertEqual(len(sqlite_pending), len(turso_pending))
        self.assertEqual(
            [a.id for a in sqlite_pending],
            [a.id for a in turso_pending],
        )
        self.assertEqual(
            [a.title for a in sqlite_pending],
            [a.title for a in turso_pending],
        )

        mem_conn.close()


class TestPhase5B1DryRunIsolationRegression(unittest.TestCase):
    """Hermetic regression suite verifying dry-run persistence isolation and Turso fail-safe behavior."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.now = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
        self.mock_articles = [
            Article.create("Dry Run Test Haber 1", "https://example.com/dry1", "TestNews", self.now),
            Article.create("Dry Run Test Haber 2", "https://example.com/dry2", "TestNews", self.now),
        ]

    def tearDown(self):
        try:
            self.tmp_dir.cleanup()
        except Exception:
            pass

    def _compute_sha256(self, file_path: Path) -> str:
        h = hashlib.sha256()
        with file_path.open("rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
        return h.hexdigest()

    def test_dry_run_leaves_operational_sqlite_byte_identical(self):
        """Create a populated operational SQLite DB, record SHA-256 and size, run scan-only in dry-run mode, verify byte-identical and zero WAL/SHM."""
        op_db = self.tmp_path / "operational_radar.db"
        repo = SqliteStateRepository(op_db)
        # Populate operational DB with real records
        pre_art = Article.create("Pre-existing Article", "https://example.com/pre", "PreSource", self.now)
        repo.mark_articles_processed([pre_art])
        repo.record_new_story(
            Story(
                story_id="STORY-PRE-001",
                headline_tr="Pre-existing Story",
                category=StoryCategory.YERLI,
                first_seen_at=self.now,
                last_updated_at=self.now,
                article_ids=[pre_art.id],
            ),
            EditorialAssessment(
                article_ids=[pre_art.id],
                score=85,
                kind=EditorialKind.NEW,
                headline_tr="Pre-existing Story",
                cover_title_tr="PRE COVER",
                caption_idea_tr="Pre caption",
                why_efso_tr="Pre why",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
            ),
        )

        wal_file = op_db.with_name(op_db.name + "-wal")
        shm_file = op_db.with_name(op_db.name + "-shm")
        self.assertFalse(wal_file.exists(), "WAL file existed before dry-run")
        self.assertFalse(shm_file.exists(), "SHM file existed before dry-run")

        # Confirm initial state
        initial_hash = self._compute_sha256(op_db)
        initial_size = op_db.stat().st_size
        self.assertGreater(initial_size, 0)

        # Run scan-only in dry-run mode with mocked source to prevent external network calls
        settings = Settings(sqlite_db_path=op_db)
        with patch("efso_radar.sources.multi_source.MultiSourceDiscoveryEngine.fetch_candidates", return_value=(self.mock_articles, 2)):
            with self.assertLogs("efso-radar", level="INFO") as log_capture:
                radar.run(dry_run=True, scan_only=True, settings=settings)

        # Verify operational DB is byte-for-byte identical
        final_hash = self._compute_sha256(op_db)
        final_size = op_db.stat().st_size
        self.assertEqual(initial_hash, final_hash, "Operational database SHA-256 hash changed during dry-run!")
        self.assertEqual(initial_size, final_size, "Operational database byte size changed during dry-run!")

        # Verify no WAL or SHM temporary files were created/left behind
        self.assertFalse(wal_file.exists(), f"WAL file was created during dry-run: {wal_file}")
        self.assertFalse(shm_file.exists(), f"SHM file was created during dry-run: {shm_file}")

        # Verify dry-run log message
        log_output = "\n".join(log_capture.output)
        self.assertIn("[DRY-RUN ISOLATION]", log_output)
        self.assertIn("Ephemeral database cannot reflect production duplicate history", log_output)

    def test_dry_run_does_not_create_missing_database(self):
        """Verify dry-run does not create data/radar.db if it didn't exist."""
        missing_db = self.tmp_path / "nonexistent_folder" / "radar.db"
        self.assertFalse(missing_db.exists())
        self.assertFalse(missing_db.parent.exists())

        settings = Settings(sqlite_db_path=missing_db)
        with patch("efso_radar.sources.multi_source.MultiSourceDiscoveryEngine.fetch_candidates", return_value=(self.mock_articles, 2)):
            radar.run(dry_run=True, scan_only=True, settings=settings)

        self.assertFalse(missing_db.exists(), "Missing database was created during dry-run!")
        self.assertFalse(missing_db.parent.exists(), "Missing database directory was created during dry-run!")

    def test_dry_run_never_calls_turso_transport(self):
        """Verify Turso transport is never called during dry-run even if credentials are set."""
        mock_transport = MagicMock()
        settings = Settings(
            turso_database_url="libsql://test-db.turso.io",
            turso_auth_token="secret-token-xyz",
            sqlite_db_path=self.tmp_path / "operational_unused.db",
        )

        with patch("efso_radar.sources.multi_source.MultiSourceDiscoveryEngine.fetch_candidates", return_value=(self.mock_articles, 2)):
            with patch("efso_radar.repositories.turso.TursoStateRepository") as mock_turso_repo:
                radar.run(dry_run=True, scan_only=True, settings=settings)

                # TursoStateRepository MUST NOT be instantiated during dry-run
                mock_turso_repo.assert_not_called()
                mock_transport.handle_request.assert_not_called()

    def test_dry_run_does_not_import_legacy_json_state(self):
        """Ensure legacy state migration is not triggered against operational DB."""
        legacy_state_file = self.tmp_path / "legacy_state.json"
        legacy_state_file.write_text(
            '{"processed": {"legacy_article_123": "2026-10-19T10:00:00Z"}, "sent": []}',
            encoding="utf-8",
        )
        op_db = self.tmp_path / "op_no_legacy.db"
        op_repo = SqliteStateRepository(op_db)
        self.assertFalse(op_repo.is_article_processed("legacy_article_123"))

        settings = Settings(
            sqlite_db_path=op_db,
            legacy_state_json_path=legacy_state_file,
        )

        with patch("efso_radar.sources.multi_source.MultiSourceDiscoveryEngine.fetch_candidates", return_value=(self.mock_articles, 2)):
            radar.run(dry_run=True, scan_only=True, settings=settings)

        # Operational DB must NOT have the legacy state imported
        self.assertFalse(
            op_repo.is_article_processed("legacy_article_123"),
            "Legacy JSON state was migrated into operational database during dry-run!",
        )

    def test_partial_turso_configuration_fails_safely(self):
        """Validate partial Turso settings fail cleanly with ValueError."""
        # 1. URL set, token missing
        with self.assertRaises(ValueError) as ctx1:
            Settings(turso_database_url="libsql://test.turso.io", turso_auth_token="")
        self.assertIn("Partial Turso configuration detected", str(ctx1.exception))

        # 2. Token set, URL missing
        with self.assertRaises(ValueError) as ctx2:
            Settings(turso_database_url="", turso_auth_token="some-token")
        self.assertIn("Partial Turso configuration detected", str(ctx2.exception))

        # 3. Fail fast in radar.run if partial config somehow passed
        mock_settings = MagicMock()
        mock_settings.turso_database_url = "libsql://test.turso.io"
        mock_settings.turso_auth_token = ""
        mock_settings.sqlite_db_path = self.tmp_path / "dummy.db"

        with self.assertRaises(ValueError) as ctx3:
            radar.run(dry_run=False, scan_only=True, settings=mock_settings)
        self.assertIn("Partial Turso configuration detected", str(ctx3.exception))


if __name__ == "__main__":
    unittest.main()
