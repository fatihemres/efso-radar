"""Hermetic acceptance tests for Phase 5B.3B: Cloud Safety Foundations & Persistent State Contracts.

Verifies:
1. Rejection of live --legacy runner execution (fail-closed).
2. Permitted execution of --legacy with --dry-run (strictly 0 network, 0 Gemini tokens).
3. Elimination of silent legacy fallback upon modular orchestrator failures.
4. Fail-closed cloud configuration enforcement (EFSO_STORAGE_MODE=turso).
5. Single-statement atomic run lock mutual exclusion (2 workers).
6. Atomic acquisition of expired lock by a new worker.
7. Lock re-entrancy / renewal by the same worker.
8. Worker ID fencing on lock release (stale worker cannot release new owner's lock).
9. Worker ID fencing on lock renewal (stale worker cannot renew new owner's lock).
10. Multi-threaded SQLite concurrency barrier test (10 threads simultaneously competing).
11. Telegram outbox database-enforced uniqueness per (recipient, message_key).
12. DELIVERY_UNCERTAIN state durability and blind retry prevention.
13. Prevention of Telegram delivery status regressions (SENT and UNCERTAIN cannot regress).
14. At-most-once daily ADMIN operational report dispatch per Europe/Istanbul date.
15. Admin report Europe/Istanbul time window validation and non-catch-up policy.
16. Durable recovery state checkpoint persistence and recovery across restarts.
17. Lossless synthetic JSON ledger migration, strict timestamps, status mapping, and dry-run report.
18. Budget ledger immutability, full event IDs, idempotent replay, and conflict rejection.
19. Schema v5 encrypted backup backwards-compatibility under v6 codebase.
20. Strict contract parity between SqliteStateRepository and TursoStateRepository.
"""
from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tarfile
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import radar
from efso_radar.backup.envelope import encrypt_envelope
from efso_radar.backup.restore import (
    SUPPORTED_BACKUP_SCHEMA_VERSIONS,
    RestoreVerificationError,
    verify_restore_archive,
    verify_staging_database,
)
from efso_radar.config import (
    Settings,
    get_istanbul_timezone,
    is_admin_report_window,
    to_istanbul_date,
    to_istanbul_datetime,
)
from efso_radar.domain import (
    BudgetHistoryConflictError,
    DeliveryIdentityConflictError,
    DeliveryStatus,
    DeliveryStatusRegressionError,
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

    # --- 1. Unsafe Legacy Fallback Elimination & Zero Network Guards ---
    def test_01_legacy_runner_live_execution_rejected(self):
        """Calling run with use_legacy=True and dry_run=False must fail closed with RuntimeError."""
        with self.assertRaises(RuntimeError) as ctx:
            radar.run(use_legacy=True, dry_run=False)
        self.assertIn("cannot be run live", str(ctx.exception))

    def test_02_legacy_runner_dry_run_zero_network_zero_ai(self):
        """Calling run with use_legacy=True and dry_run=True executes deterministic preview with strictly 0 network and 0 Gemini calls."""
        with patch("radar.fetch_articles", side_effect=AssertionError("Live RSS network call attempted in legacy dry-run")), \
             patch("radar.judge", side_effect=AssertionError("Live Gemini API call attempted in legacy dry-run")):
            # Must succeed without calling fetch_articles or judge
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

        # 3. Execution with cloud mode and missing credentials raises Exception without touching SQLite
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

    def test_10_multithreaded_sqlite_concurrency_barrier(self):
        """10 threads simultaneously competing via Barrier on separate connections: exactly 1 winner."""
        num_threads = 10
        barrier = threading.Barrier(num_threads)
        results: list[bool] = [False] * num_threads
        exceptions: list[Exception] = []

        def worker_task(idx: int):
            worker_id = f"thread_worker_{idx}"
            thread_repo = SqliteStateRepository(self.db_path)
            try:
                barrier.wait(timeout=5.0)
                res = thread_repo.acquire_run_lock(worker_id, timeout_minutes=15)
                results[idx] = res
            except Exception as e:
                exceptions.append(e)

        threads = [threading.Thread(target=worker_task, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        self.assertEqual(len(exceptions), 0, f"Thread exceptions: {exceptions}")
        self.assertEqual(sum(1 for r in results if r is True), 1, "Exactly one thread must acquire the lock")
        winner_idx = results.index(True)
        winner_id = f"thread_worker_{winner_idx}"

        # Competing renewal by loser fails; renewal by winner succeeds
        loser_idx = (winner_idx + 1) % num_threads
        loser_repo = SqliteStateRepository(self.db_path)
        self.assertFalse(loser_repo.renew_run_lock(f"thread_worker_{loser_idx}", extend_minutes=15))

        winner_repo = SqliteStateRepository(self.db_path)
        self.assertTrue(winner_repo.renew_run_lock(winner_id, extend_minutes=30))

        # Competing release by loser fails; lock remains owned by winner
        loser_repo.release_run_lock(f"thread_worker_{loser_idx}")
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT worker_id FROM run_locks WHERE lock_key = 'radar_cron'").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], winner_id)
        finally:
            conn.close()

        # Expiry and stealing under concurrency: expire winner's lock
        past_iso = (utc_now() - timedelta(minutes=5)).isoformat()
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("UPDATE run_locks SET expires_at = ? WHERE lock_key = 'radar_cron'", (past_iso,))
            conn.commit()
        finally:
            conn.close()

        # All 10 threads compete again on expired lock
        barrier_2 = threading.Barrier(num_threads)
        results_2: list[bool] = [False] * num_threads

        def steal_task(idx: int):
            worker_id = f"steal_worker_{idx}"
            thread_repo = SqliteStateRepository(self.db_path)
            try:
                barrier_2.wait(timeout=5.0)
                res = thread_repo.acquire_run_lock(worker_id, timeout_minutes=15)
                results_2[idx] = res
            except Exception as e:
                exceptions.append(e)

        threads_2 = [threading.Thread(target=steal_task, args=(i,)) for i in range(num_threads)]
        for t in threads_2:
            t.start()
        for t in threads_2:
            t.join(timeout=10.0)

        self.assertEqual(sum(1 for r in results_2 if r is True), 1, "Exactly one thread must acquire the expired lock")

    # --- 3. Persistent State Contracts & Outbox Idempotency ---
    def test_11_telegram_outbox_uniqueness_and_idempotency(self):
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

    def test_12_telegram_outbox_uncertain_state_safety(self):
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

    def test_13_delivery_status_regression_protection(self):
        """SENT and DELIVERY_UNCERTAIN cannot regress to PENDING or FAILED."""
        # 1. Record an initial SENT record
        rec_sent = TelegramDeliveryRecord(
            delivery_id="del_sent_001",
            message_key="key_alpha",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.SENT,
            attempt_count=1,
            sent_at=utc_now(),
        )
        self.repo.record_telegram_delivery(rec_sent)

        # 2. Regressing SENT -> PENDING must raise DeliveryStatusRegressionError
        rec_pending = TelegramDeliveryRecord(
            delivery_id="del_sent_001_bad",
            message_key="key_alpha",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.PENDING,
            attempt_count=1,
        )
        with self.assertRaises(DeliveryStatusRegressionError):
            self.repo.record_telegram_delivery(rec_pending)

        # 3. Regressing SENT -> FAILED must raise DeliveryStatusRegressionError
        rec_failed = TelegramDeliveryRecord(
            delivery_id="del_sent_001_bad2",
            message_key="key_alpha",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.FAILED,
            attempt_count=1,
            error_message="Network error",
        )
        with self.assertRaises(DeliveryStatusRegressionError):
            self.repo.record_telegram_delivery(rec_failed)

        # 4. Updating status via update_telegram_delivery_status must also fail
        with self.assertRaises(DeliveryStatusRegressionError):
            self.repo.update_telegram_delivery_status("del_sent_001", DeliveryStatus.PENDING)

        # 5. Conflicting identity (different category or role) must raise DeliveryIdentityConflictError
        rec_diff_cat = TelegramDeliveryRecord(
            delivery_id="del_sent_001_bad3",
            message_key="key_alpha",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            status=DeliveryStatus.SENT,
            attempt_count=1,
        )
        with self.assertRaises(DeliveryIdentityConflictError):
            self.repo.record_telegram_delivery(rec_diff_cat)

        # 6. UNCERTAIN cannot regress to PENDING or FAILED
        rec_unc = TelegramDeliveryRecord(
            delivery_id="del_unc_001",
            message_key="key_uncertain",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
        )
        self.repo.record_telegram_delivery(rec_unc)

        with self.assertRaises(DeliveryStatusRegressionError):
            self.repo.update_telegram_delivery_status("del_unc_001", DeliveryStatus.PENDING)

        with self.assertRaises(DeliveryStatusRegressionError):
            self.repo.update_telegram_delivery_status("del_unc_001", DeliveryStatus.FAILED)

        # 7. UNCERTAIN -> SENT is permitted (manual/verified reconciliation)
        self.repo.update_telegram_delivery_status("del_unc_001", DeliveryStatus.SENT)
        fetched = self.repo.get_telegram_delivery("8712345678", "key_uncertain")
        self.assertEqual(fetched.status, DeliveryStatus.SENT)

        # 8. Idempotent replay of SENT is safe
        self.repo.record_telegram_delivery(rec_sent)

    # --- 4. At-Most-Once Daily Admin Reports & Window Validation ---
    def test_14_admin_report_at_most_once_per_date(self):
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

    def test_15_admin_report_window_and_no_catchup(self):
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
    def test_16_durable_recovery_state_restart_resilience(self):
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

    # --- 6. Lossless Ledger Migration & Budget Immutability ---
    def test_17_lossless_ledger_migration_and_dryrun(self):
        """Synthetic fixture migration utility losslessly imports ledgers with strict timestamps, status mapping, and dry-run report."""
        del_path = self.tmp_path / "telegram_digest_delivery_ledger.json"
        bud_path = self.tmp_path / "experiment_budget_ledger.json"

        # 1. Non-existent file test: fail explicitly without crashing
        res_missing = migrate_ledgers_to_repository(
            self.repo,
            delivery_ledger_path=self.tmp_path / "does_not_exist_del.json",
            budget_ledger_path=self.tmp_path / "does_not_exist_bud.json",
        )
        self.assertFalse(res_missing.success)
        self.assertTrue(any("does not exist" in e for e in res_missing.errors))

        # 2. Write valid ledgers with mixed roles and historical statuses
        del_data = {
            "version": 1,
            "deliveries": {
                "8712345678:d_hash_001": {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": "2026-10-19T12:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_hash_001",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                },
                "1912345635:d_hash_admin": {
                    "status": "SENT",
                    "delivered_at_utc": "2026-10-19T22:00:00Z",
                    "chat_id": "1912345635",
                    "digest_hash": "d_hash_admin",
                    "recipient_role": "ADMIN",
                    "category": "ADMIN_HEALTH_REPORT",
                },
                "8712345678:d_hash_uncertain": {
                    "status": "DELIVERY_UNCERTAIN",
                    "delivered_at_utc": "2026-10-19T14:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_hash_uncertain",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                    "error_message": "Network timeout",
                },
                # Identical alias for d_hash_001 should be deduplicated
                "alias_001": {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": "2026-10-19T12:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_hash_001",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                },
            }
        }
        del_path.write_text(json.dumps(del_data), encoding="utf-8")

        bud_data = {
            "experiment_id": "test_exp",
            "batches": {
                "b01": {
                    "actual_cost_usd": 0.0012,
                    "input_tokens": 1500,
                    "output_tokens": 400,
                    "period_month": "2026-10",
                    "timestamp_utc": "2026-10-19T12:00:00Z",
                }
            }
        }
        bud_path.write_text(json.dumps(bud_data), encoding="utf-8")

        # 3. Pre-flight dry-run validation: returns report, does NOT touch database
        res_dry = migrate_ledgers_to_repository(self.repo, del_path, bud_path, dry_run=True)
        self.assertTrue(res_dry.success)
        self.assertTrue(res_dry.dry_run)
        self.assertEqual(res_dry.deliveries_imported, 3)
        self.assertEqual(res_dry.deliveries_skipped, 1)  # alias_001 deduplicated
        self.assertEqual(res_dry.budget_records_imported, 1)
        self.assertIn("DRY-RUN", res_dry.summary())

        # Verify database is completely empty after dry-run
        self.assertIsNone(self.repo.get_telegram_delivery("8712345678", "d_hash_001"))

        # 4. Live migration
        res_live = migrate_ledgers_to_repository(self.repo, del_path, bud_path, dry_run=False)
        self.assertTrue(res_live.success)
        self.assertEqual(res_live.deliveries_imported, 3)
        self.assertEqual(res_live.deliveries_skipped, 1)
        self.assertEqual(res_live.budget_records_imported, 1)

        # Verify preserved roles & categories
        rec_editor = self.repo.get_telegram_delivery("8712345678", "d_hash_001")
        self.assertIsNotNone(rec_editor)
        self.assertEqual(rec_editor.recipient_role, "PRIMARY_EDITOR")
        self.assertEqual(rec_editor.category, TelegramNotificationCategory.EDITORIAL_DIGEST)

        rec_admin = self.repo.get_telegram_delivery("1912345635", "d_hash_admin")
        self.assertIsNotNone(rec_admin)
        self.assertEqual(rec_admin.recipient_role, "ADMIN")
        self.assertEqual(rec_admin.category, TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT)

        rec_unc = self.repo.get_telegram_delivery("8712345678", "d_hash_uncertain")
        self.assertIsNotNone(rec_unc)
        self.assertEqual(rec_unc.status, DeliveryStatus.DELIVERY_UNCERTAIN)

        # 5. Idempotent re-run: all existing records are skipped safely
        res_replay = migrate_ledgers_to_repository(self.repo, del_path, bud_path, dry_run=False)
        self.assertTrue(res_replay.success)
        self.assertEqual(res_replay.deliveries_imported, 0)
        self.assertEqual(res_replay.deliveries_skipped, 4)  # 3 distinct + 1 alias
        self.assertEqual(res_replay.budget_records_imported, 0)
        self.assertEqual(res_replay.budget_records_skipped, 1)

        # 6. Reject missing/invalid timestamp in historical record (no invented utc_now())
        del_bad_ts = {
            "deliveries": {
                "8712345678:d_bad_ts": {
                    "status": "SENT",
                    "chat_id": "8712345678",
                    "digest_hash": "d_bad_ts",
                    # No timestamp!
                }
            }
        }
        del_bad_path = self.tmp_path / "del_bad_ts.json"
        del_bad_path.write_text(json.dumps(del_bad_ts), encoding="utf-8")

        res_bad_ts = migrate_ledgers_to_repository(self.repo, del_bad_path, dry_run=True)
        self.assertFalse(res_bad_ts.success)
        self.assertTrue(any("Missing delivery timestamp" in e for e in res_bad_ts.errors))

    def test_18_budget_ledger_immutability_and_conflict(self):
        """Full event IDs, idempotent replay, conflict rejection on budget ledger."""
        ts = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
        # 1. Record budget event with long run_id
        res1 = self.repo.record_budget_usage(
            period_month="2026-10",
            run_id="exp_super_long_event_identifier_1234567890_abcdef",
            worker_id="runner_1",
            calls_count=2,
            prompt_tokens=2000,
            completion_tokens=500,
            total_tokens=2500,
            estimated_cost_usd=0.0025,
            timestamp=ts,
        )
        self.assertTrue(res1)

        # 2. Check full run_id in database
        rec_id = "bud_2026-10_exp_super_long_event_identifier_1234567890_abcdef"
        budget_rec = self.repo.get_budget_record(rec_id)
        self.assertIsNotNone(budget_rec)
        self.assertEqual(budget_rec["run_id"], "exp_super_long_event_identifier_1234567890_abcdef")
        self.assertEqual(budget_rec["total_tokens"], 2500)
        self.assertEqual(budget_rec["timestamp"], ts.isoformat())

        # 3. Idempotent replay with identical parameters returns True
        res_replay = self.repo.record_budget_usage(
            period_month="2026-10",
            run_id="exp_super_long_event_identifier_1234567890_abcdef",
            worker_id="runner_1",
            calls_count=2,
            prompt_tokens=2000,
            completion_tokens=500,
            total_tokens=2500,
            estimated_cost_usd=0.0025,
            timestamp=ts,
        )
        self.assertTrue(res_replay)

        # 4. Conflicting replay with different prompt_tokens raises BudgetHistoryConflictError
        with self.assertRaises(BudgetHistoryConflictError):
            self.repo.record_budget_usage(
                period_month="2026-10",
                run_id="exp_super_long_event_identifier_1234567890_abcdef",
                worker_id="runner_1",
                calls_count=2,
                prompt_tokens=3000,  # Conflict!
                completion_tokens=500,
                total_tokens=3500,
                estimated_cost_usd=0.0035,
                timestamp=ts,
            )

    # --- 7. Encrypted Backup Compatibility Across Schema Versions ---
    def test_19_backup_schema_v5_compatibility(self):
        """Historical v5 encrypted backups verify cleanly under v6 codebase without ciphertext rewrites."""
        key = b"\x42" * 32
        v5_db_path = self.tmp_path / "v5_radar.db"

        # Create authentic SQLite DB with schema_migrations at version 5
        conn = sqlite3.connect(v5_db_path)
        try:
            conn.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT);")
            conn.execute("INSERT INTO schema_migrations (version, applied_at) VALUES (5, '2026-10-18T10:00:00Z');")
            conn.execute("CREATE TABLE processed_articles (article_id TEXT PRIMARY KEY, title TEXT);")
            conn.execute("INSERT INTO processed_articles (article_id, title) VALUES ('art_001', 'Test News');")
            conn.commit()
        finally:
            conn.close()

        # Build tar containing radar.db, telegram_digest_delivery_ledger.json, and experiment_budget_ledger.json
        del_json = json.dumps({"version": 1, "deliveries": {}}).encode("utf-8")
        bud_json = json.dumps({"batches": {}}).encode("utf-8")
        db_bytes = v5_db_path.read_bytes()

        tar_buf = io.BytesIO()
        with tarfile.open(fileobj=tar_buf, mode="w:gz") as tar:
            for name, data in [
                ("radar.db", db_bytes),
                ("telegram_digest_delivery_ledger.json", del_json),
                ("experiment_budget_ledger.json", bud_json),
            ]:
                ti = tarfile.TarInfo(name=name)
                ti.size = len(data)
                ti.mtime = int(datetime(2026, 10, 18, 10, 0, tzinfo=timezone.utc).timestamp())
                tar.addfile(ti, io.BytesIO(data))

        plaintext_tar = tar_buf.getvalue()
        sha256_hash = hashlib.sha256(plaintext_tar).hexdigest()

        # Manifest with schema_version: 5
        v5_manifest = {
            "backup_version": 1,
            "schema_version": 5,
            "created_at_utc": "2026-10-18T10:00:00Z",
            "plaintext_sha256": sha256_hash,
            "table_row_counts": {
                "schema_migrations": 1,
                "processed_articles": 1,
            },
        }

        envelope_v5 = encrypt_envelope(plaintext_tar, key, v5_manifest)

        # 1. Verification of v5 backup under v6 codebase must succeed
        res = verify_restore_archive(envelope_v5, key)
        self.assertTrue(res.success)
        self.assertEqual(res.manifest["schema_version"], 5)

        # 2. Incompatible schema version (e.g. 4 or 99) fails closed
        v4_manifest = dict(v5_manifest, schema_version=4)
        envelope_v4 = encrypt_envelope(plaintext_tar, key, v4_manifest)
        with self.assertRaises(RestoreVerificationError) as ctx:
            verify_restore_archive(envelope_v4, key)
        self.assertIn("Manifest schema version mismatch", str(ctx.exception))

    # --- 8. Database Parity (SQLite & Turso) ---
    def test_20_repository_parity_sqlite_and_turso(self):
        """TursoStateRepository satisfies identical atomic contracts, delivery regressions, and budget immutability."""
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

        # 4. Delivery status regression check on Turso
        mock_client.execute.return_value = TursoQueryResult(
            cols=["delivery_id", "message_key", "recipient_role", "recipient_chat_id", "category", "status", "attempt_count", "max_attempts", "error_message", "payload_json", "created_at", "updated_at", "sent_at"],
            rows=[{
                "delivery_id": "del_1",
                "message_key": "key_1",
                "recipient_role": "PRIMARY_EDITOR",
                "recipient_chat_id": "8712345678",
                "category": "EDITORIAL_DIGEST",
                "status": "SENT",
                "attempt_count": 1,
                "max_attempts": 3,
                "error_message": None,
                "payload_json": "{}",
                "created_at": "2026-10-19T10:00:00Z",
                "updated_at": "2026-10-19T10:00:00Z",
                "sent_at": "2026-10-19T10:00:00Z",
            }],
            affected_row_count=1,
        )
        rec_pending = TelegramDeliveryRecord(
            delivery_id="del_1",
            message_key="key_1",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.PENDING,
            attempt_count=1,
        )
        with self.assertRaises(DeliveryStatusRegressionError):
            turso_repo.record_telegram_delivery(rec_pending)

        # 5. Budget conflict check on Turso
        mock_client.execute.return_value = TursoQueryResult(
            cols=["record_id", "period_month", "run_id", "worker_id", "calls_count", "prompt_tokens", "completion_tokens", "total_tokens", "estimated_cost_usd"],
            rows=[{
                "record_id": "bud_2026-10_run_1",
                "period_month": "2026-10",
                "run_id": "run_1",
                "worker_id": "w1",
                "calls_count": 1,
                "prompt_tokens": 1000,
                "completion_tokens": 200,
                "total_tokens": 1200,
                "estimated_cost_usd": 0.001,
            }],
            affected_row_count=1,
        )
        with self.assertRaises(BudgetHistoryConflictError):
            turso_repo.record_budget_usage(
                period_month="2026-10",
                run_id="run_1",
                worker_id="w1",
                calls_count=1,
                prompt_tokens=9999,  # Conflict!
                completion_tokens=200,
                total_tokens=10199,
                estimated_cost_usd=0.005,
            )


if __name__ == "__main__":
    unittest.main()
