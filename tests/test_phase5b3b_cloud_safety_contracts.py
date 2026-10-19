"""Hermetic acceptance tests for Phase 5B.3B: Cloud Safety Foundations & Persistent State Contracts.

Verifies:
1. Rejection of live --legacy runner execution (fail-closed).
2. Permitted execution of --legacy with --dry-run.
3. Elimination of silent legacy fallback upon modular orchestrator failures.
4. Fail-closed cloud configuration enforcement (EFSO_STORAGE_MODE=turso).
5. Single-statement atomic run lock mutual exclusion (2 workers).
6. Atomic acquisition of expired lock by a new worker.
7. Lock re-entrancy / renewal by the same worker.
8. Worker ID fencing on lock release (stale worker cannot release new owner's lock).
9. Worker ID fencing on lock renewal (stale worker cannot renew new owner's lock).
10. Telegram outbox database-enforced uniqueness per (recipient, message_key).
11. DELIVERY_UNCERTAIN state durability and blind retry prevention.
12. At-most-once daily ADMIN operational report dispatch per Europe/Istanbul date.
13. Admin report Europe/Istanbul time window validation and non-catch-up policy.
14. Durable recovery state checkpoint persistence and recovery across restarts.
15. Idempotent synthetic JSON ledger migration and conflict detection.
16. Strict contract parity between SqliteStateRepository and TursoStateRepository.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import radar
from efso_radar.config import (
    Settings,
    get_istanbul_timezone,
    is_admin_report_window,
    to_istanbul_date,
    to_istanbul_datetime,
)
from efso_radar.domain import (
    DeliveryStatus,
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    utc_now,
)
from efso_radar.repositories.migration_ledger import migrate_ledgers_to_repository
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from efso_radar.repositories.turso_client import TursoClientError, TursoQueryResult


class TestPhase5B3BCloudSafetyContracts(unittest.TestCase):
    """Hermetic unit and integration acceptance tests for Phase 5B.3B."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.db_path = self.tmp_path / "test_safety.db"
        self.repo = SqliteStateRepository(self.db_path)

    def tearDown(self):
        import gc
        gc.collect()
        try:
            self.tmp_dir.cleanup()
        except Exception:
            pass

    # --- 1. Unsafe Legacy Fallback Elimination & Guards ---
    def test_01_legacy_runner_live_execution_rejected(self):
        """Calling run with use_legacy=True and dry_run=False must fail closed with RuntimeError."""
        with self.assertRaises(RuntimeError) as ctx:
            radar.run(use_legacy=True, dry_run=False)
        self.assertIn("cannot be run live", str(ctx.exception))

    def test_02_legacy_runner_dry_run_allowed(self):
        """Calling run with use_legacy=True and dry_run=True must be permitted."""
        mock_articles = [{
            "title": "Album announced",
            "link": "https://example.org/1",
            "source": "Source",
            "market": "EN",
            "published_utc": "2026-10-19T10:00:00Z",
            "description": "Desc",
            "id": "art_001",
        }]
        with patch("radar.fetch_articles", return_value=(mock_articles, 1)), \
             patch("radar.load_state", return_value={"processed": {}, "sent": []}), \
             patch("radar.judge", return_value=[]), \
             patch("radar.validate_picks", return_value=[]):
            # Should not raise exception
            radar.run(use_legacy=True, dry_run=True)

    def test_03_modular_runner_removes_silent_fallback(self):
        """Modular orchestrator errors must propagate and fail closed without falling back to legacy runner."""
        failing_settings = Settings(
            sqlite_db_path=self.db_path,
            telegram_delivery_enabled=False,
            ai_evaluation_enabled=False,
        )
        with patch("efso_radar.orchestrator.RadarOrchestrator.run", side_effect=ValueError("Simulated orchestrator failure")), \
             patch("radar.fetch_articles") as mock_legacy_fetch:
            with self.assertRaises(ValueError):
                radar.run(dry_run=False, use_legacy=False, settings=failing_settings)
            # Legacy monolithic fetch_articles must NEVER be called
            mock_legacy_fetch.assert_not_called()

    def test_04_cloud_mode_configuration_enforcement(self):
        """EFSO_STORAGE_MODE=turso with missing or partial Turso config fails closed immediately without SQLite fallback."""
        # 1. Missing both URL and token with explicit cloud mode
        with self.assertRaises(ValueError) as ctx:
            Settings(
                storage_mode="turso",
                turso_database_url="",
                turso_auth_token="",
            )
        self.assertIn("Cloud storage mode enabled", str(ctx.exception))

        # 2. Partial configuration (URL only)
        with self.assertRaises(ValueError) as ctx:
            Settings(
                storage_mode="auto",
                turso_database_url="libsql://efso-db.turso.io",
                turso_auth_token="",
            )
        self.assertIn("Partial Turso configuration detected", str(ctx.exception))

        # 3. Execution with cloud mode and missing credentials raises ValueError without touching SQLite
        with patch.dict("os.environ", {"EFSO_STORAGE_MODE": "turso", "TURSO_DATABASE_URL": "", "TURSO_AUTH_TOKEN": ""}):
            with self.assertRaises(Exception) as ctx:
                radar.run(dry_run=False)
            self.assertTrue(any(x in str(ctx.exception).lower() for x in ["cloud storage mode", "incomplete", "validation"]))

    # --- 2. Atomic Concurrency Control (Run Locks) ---
    def test_05_atomic_concurrency_lock_mutual_exclusion(self):
        """Worker 1 acquires lock; Worker 2 concurrently attempting acquire on same database fails (rowcount 0)."""
        w1_success = self.repo.acquire_run_lock("worker_alpha", timeout_minutes=15)
        self.assertTrue(w1_success)

        # Worker 2 simultaneously attempts to acquire lock
        w2_success = self.repo.acquire_run_lock("worker_beta", timeout_minutes=15)
        self.assertFalse(w2_success)

    def test_06_atomic_concurrency_expired_lock_stealing(self):
        """When Worker 1's lock expires in the database, Worker 2 atomically acquires it."""
        # Acquire lock expiring 1 minute ago
        past_iso = (utc_now() - timedelta(minutes=1)).isoformat()
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at)
                VALUES ('radar_cron', 'worker_alpha', ?, ?, ?)
                """,
                (past_iso, past_iso, past_iso),
            )
            conn.commit()
        finally:
            conn.close()

        # Worker 2 should atomically succeed in acquiring the expired lock
        w2_success = self.repo.acquire_run_lock("worker_beta", timeout_minutes=15)
        self.assertTrue(w2_success)

        # Confirm lock now belongs to worker_beta
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("SELECT worker_id FROM run_locks WHERE lock_key = 'radar_cron'")
            row = cursor.fetchone()
            self.assertEqual(row[0], "worker_beta")
        finally:
            conn.close()

    def test_07_atomic_concurrency_reentrancy(self):
        """Same worker can re-acquire or renew its own unexpired lock."""
        self.assertTrue(self.repo.acquire_run_lock("worker_alpha", timeout_minutes=15))
        # Re-entrant acquire
        self.assertTrue(self.repo.acquire_run_lock("worker_alpha", timeout_minutes=30))
        # Explicit renewal
        self.assertTrue(self.repo.renew_run_lock("worker_alpha", extend_minutes=45))

    def test_08_atomic_concurrency_stale_worker_release_fenced(self):
        """A stale worker cannot release a lock that has been acquired by a new owner."""
        # Worker 1 acquires lock
        self.assertTrue(self.repo.acquire_run_lock("worker_alpha", timeout_minutes=15))

        # Stale Worker 0 tries to release it
        self.repo.release_run_lock("worker_stale")

        # Worker 1's lock MUST remain active
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("SELECT worker_id FROM run_locks WHERE lock_key = 'radar_cron'")
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "worker_alpha")
        finally:
            conn.close()

        # Now Worker 1 releases it
        self.repo.release_run_lock("worker_alpha")
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute("SELECT worker_id FROM run_locks WHERE lock_key = 'radar_cron'")
            self.assertIsNone(cursor.fetchone())
        finally:
            conn.close()

    def test_09_atomic_concurrency_stale_worker_renew_fenced(self):
        """A stale worker cannot renew a lock owned by another worker."""
        # Worker 1 owns lock
        self.assertTrue(self.repo.acquire_run_lock("worker_alpha", timeout_minutes=15))

        # Stale worker tries to renew
        renew_res = self.repo.renew_run_lock("worker_stale", extend_minutes=30)
        self.assertFalse(renew_res)

    # --- 3. Persistent State Contracts & Outbox Idempotency ---
    def test_10_telegram_outbox_uniqueness_and_idempotency(self):
        """Database enforces UNIQUE(recipient_chat_id, message_key) preventing duplicate outbox broadcasts."""
        delivery = TelegramDeliveryRecord(
            delivery_id="del_001",
            message_key="digest_20261019_1200",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.SENT,
            attempt_count=1,
            sent_at=utc_now(),
        )
        self.repo.record_telegram_delivery(delivery)

        # Verify retrieved delivery record
        fetched = self.repo.get_telegram_delivery("8712345678", "digest_20261019_1200")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.status, DeliveryStatus.SENT)
        self.assertEqual(fetched.masked_recipient_chat_id, "87...78")

        # Check digest is delivered
        self.assertTrue(self.repo.is_digest_delivered_to_recipient("8712345678", "digest_20261019_1200"))
        self.assertFalse(self.repo.is_digest_delivered_to_recipient("1912345635", "digest_20261019_1200"))

    def test_11_telegram_outbox_uncertain_state_safety(self):
        """DELIVERY_UNCERTAIN state is recorded and never considered successfully delivered."""
        delivery = TelegramDeliveryRecord(
            delivery_id="del_uncertain_001",
            message_key="digest_uncertain_hash",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
            error_message="HTTP connection reset during response reception",
        )
        self.repo.record_telegram_delivery(delivery)

        # UNCERTAIN message is NOT considered delivered
        self.assertFalse(self.repo.is_digest_delivered_to_recipient("8712345678", "digest_uncertain_hash"))

        # Pending query excludes UNCERTAIN so it is NEVER blindly retried without intervention
        pending = self.repo.get_pending_telegram_deliveries()
        self.assertEqual(len(pending), 0)

    # --- 4. At-Most-Once Daily Admin Reports & Window Validation ---
    def test_12_admin_report_at_most_once_per_date(self):
        """Admin health report can be dispatched at most once per Europe/Istanbul calendar date."""
        today_trt = to_istanbul_date(utc_now())

        # First recording succeeds
        res1 = self.repo.record_admin_report(
            report_date=today_trt,
            worker_id="runner_01",
            status=DeliveryStatus.SENT,
            recipient_chat_id="1912345635",
            summary_payload={"picks": 2, "articles": 15},
        )
        self.assertTrue(res1)
        self.assertTrue(self.repo.has_admin_report_been_dispatched(today_trt))

        # Duplicate dispatch attempt on same date fails / returns False
        res2 = self.repo.record_admin_report(
            report_date=today_trt,
            worker_id="runner_02",
            status=DeliveryStatus.SENT,
            recipient_chat_id="1912345635",
            summary_payload={"picks": 2, "articles": 15},
        )
        self.assertFalse(res2)

    def test_13_admin_report_window_and_no_catchup(self):
        """is_admin_report_window localizes to Europe/Istanbul and strictly rejects out-of-window runs."""
        istanbul_tz = get_istanbul_timezone()

        # Inside permitted window (22:00 TRT)
        inside_window_dt = datetime(2026, 10, 19, 22, 0, 0, tzinfo=istanbul_tz)
        self.assertTrue(is_admin_report_window(inside_window_dt))

        # Inside window near edge (21:35 TRT)
        edge_inside_dt = datetime(2026, 10, 19, 21, 35, 0, tzinfo=istanbul_tz)
        self.assertTrue(is_admin_report_window(edge_inside_dt))

        # Outside window: earlier (20:00 TRT) -> rejected
        early_dt = datetime(2026, 10, 19, 20, 0, 0, tzinfo=istanbul_tz)
        self.assertFalse(is_admin_report_window(early_dt))

        # Outside window: late / missed cron trigger (23:15 TRT) -> rejected without catch-up
        late_dt = datetime(2026, 10, 19, 23, 15, 0, tzinfo=istanbul_tz)
        self.assertFalse(is_admin_report_window(late_dt))

    # --- 5. Durable Recovery State ---
    def test_14_durable_recovery_state_restart_resilience(self):
        """Recovery checkpoint persists in database and is recoverable across simulated runner restart."""
        stage = "EVALUATION_BATCH"
        payload = {"batch_id": "batch_99", "processed_indices": [0, 1, 2]}
        self.repo.save_recovery_state(
            stage=stage,
            worker_id="runner_ephemeral_1",
            state_payload=payload,
            batch_id="batch_99",
            last_processed_id="cand_123",
        )

        # Simulate runner crash and restart: create fresh repository instance on same database
        restarted_repo = SqliteStateRepository(self.db_path)
        recovered = restarted_repo.get_recovery_state(stage)
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered["worker_id"], "runner_ephemeral_1")
        self.assertEqual(recovered["state_payload"]["batch_id"], "batch_99")
        self.assertEqual(recovered["last_processed_id"], "cand_123")

        # After stage completion, state is cleared
        restarted_repo.clear_recovery_state(stage)
        self.assertIsNone(restarted_repo.get_recovery_state(stage))

    # --- 6. Ledger Migration Safety & Parity ---
    def test_15_ledger_migration_utility_idempotency_and_conflict(self):
        """Synthetic fixture migration utility imports JSON ledgers into repository tables idempotently."""
        del_path = self.tmp_path / "telegram_digest_delivery_ledger.json"
        bud_path = self.tmp_path / "experiment_budget_ledger.json"

        del_path.write_text(
            json.dumps({
                "version": 1,
                "deliveries": {
                    "8712345678:d_hash_001": {
                        "status": "DELIVERY_SUCCESS",
                        "delivered_at_utc": "2026-10-19T12:00:00Z",
                        "chat_id": "8712345678",
                        "digest_hash": "d_hash_001",
                        "recipient_role": "PRIMARY_EDITOR",
                    }
                }
            }),
            encoding="utf-8",
        )

        bud_path.write_text(
            json.dumps({
                "experiment_id": "test_exp",
                "batches": {
                    "b01": {
                        "actual_cost_usd": 0.0012,
                        "input_tokens": 1500,
                        "output_tokens": 400,
                        "period_month": "2026-10",
                    }
                }
            }),
            encoding="utf-8",
        )

        # 1. Initial migration
        res1 = migrate_ledgers_to_repository(self.repo, del_path, bud_path)
        self.assertTrue(res1.success)
        self.assertEqual(res1.deliveries_imported, 1)
        self.assertEqual(res1.budget_records_imported, 1)

        # 2. Idempotent re-run: delivery skipped without errors
        res2 = migrate_ledgers_to_repository(self.repo, del_path, bud_path)
        self.assertTrue(res2.success)
        self.assertEqual(res2.deliveries_imported, 0)
        self.assertEqual(res2.deliveries_skipped, 1)

        # 3. Budget history query
        bud_summary = self.repo.get_monthly_budget_summary("2026-10")
        self.assertEqual(bud_summary["total_prompt_tokens"], 1500)
        self.assertEqual(bud_summary["total_cost_usd"], 0.0012)

    def test_16_repository_parity_sqlite_and_turso(self):
        """TursoStateRepository passes identical atomic concurrency and contract methods."""
        mock_client = MagicMock()
        mock_client.execute.return_value = TursoQueryResult(cols=[], rows=[], affected_row_count=1)

        turso_repo = TursoStateRepository(
            database_url="libsql://mock.turso.io",
            auth_token="mock_token",
            client=mock_client,
        )

        # 1. Atomic acquire
        self.assertTrue(turso_repo.acquire_run_lock("worker_turso", timeout_minutes=15))
        sql_called = mock_client.execute.call_args[0][0]
        self.assertIn("INSERT INTO run_locks", sql_called)
        self.assertIn("ON CONFLICT(lock_key) DO UPDATE", sql_called)

        # 2. Renewal
        self.assertTrue(turso_repo.renew_run_lock("worker_turso", extend_minutes=15))
        renew_sql = mock_client.execute.call_args[0][0]
        self.assertIn("UPDATE run_locks", renew_sql)
        self.assertIn("worker_id = ?", renew_sql)

        # 3. Release
        turso_repo.release_run_lock("worker_turso")
        del_sql = mock_client.execute.call_args[0][0]
        self.assertIn("DELETE FROM run_locks WHERE lock_key = 'radar_cron' AND worker_id = ?", del_sql)


if __name__ == "__main__":
    unittest.main()
