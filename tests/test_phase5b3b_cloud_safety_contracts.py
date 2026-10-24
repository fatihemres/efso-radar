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
from contextlib import contextmanager
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
    DeliveryStateError,
    DeliveryStatus,
    DeliveryStatusRegressionError,
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    utc_now,
)
from efso_radar.repositories.base import StateRepository
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
        self.repo = SqliteStateRepository.create_owned_synthetic_fixture(self.db_path)

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

        # 7. Generic update_telegram_delivery_status and record_telegram_delivery CANNOT transition UNCERTAIN to SENT
        with self.assertRaises(DeliveryStatusRegressionError):
            self.repo.update_telegram_delivery_status("del_unc_001", DeliveryStatus.SENT)

        rec_unc_sent_variant = TelegramDeliveryRecord(
            delivery_id="del_unc_001_variant",
            message_key="key_uncertain",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.SENT,
            attempt_count=2,
        )
        with self.assertRaises(DeliveryStatusRegressionError):
            self.repo.record_telegram_delivery(rec_unc_sent_variant)

        # 8. reconcile_uncertain_delivery requires complete proof and only works on UNCERTAIN records
        with self.assertRaises(DeliveryStateError):
            self.repo.reconcile_uncertain_delivery("del_sent_001", {"reconciliation_method": "check", "verified_by": "op", "proof_reference": "ref", "operator_authorized": True})

        with self.assertRaises(ValueError):
            self.repo.reconcile_uncertain_delivery("del_unc_001", {"reconciliation_method": "check"}, operator_authorized=True)  # Missing verifier & ref!

        # 9. Operator-supplied metadata strings alone do NOT automatically authorize reconciliation
        with self.assertRaises(DeliveryStateError):
            self.repo.reconcile_uncertain_delivery(
                "del_unc_001",
                {"reconciliation_method": "telegram_update_offset_inspection", "verified_by": "op", "proof_reference": "ref123"},
                operator_authorized=False,
            )

        # 10. Live operation (live_mode=True) permanently disables automatic reconciliation
        with self.assertRaises(DeliveryStateError):
            self.repo.reconcile_uncertain_delivery(
                "del_unc_001",
                {"reconciliation_method": "telegram_update_offset_inspection", "verified_by": "op", "proof_reference": "ref123", "operator_authorized": True},
                live_mode=True,
            )

        # 11. Explicit offline reconciliation with valid proof metadata and operator authorization succeeds
        valid_proof = {
            "reconciliation_method": "telegram_update_offset_inspection",
            "verified_by": "lead_operator",
            "proof_reference": "update_id_9928172",
            "proof_notes": "Bot update verified delivery to editor chat.",
            "operator_authorized": True,
        }
        reconciled = self.repo.reconcile_uncertain_delivery("del_unc_001", valid_proof, operator_authorized=True)
        self.assertEqual(reconciled.status, DeliveryStatus.SENT)
        self.assertIsNotNone(reconciled.sent_at)
        self.assertEqual(reconciled.payload["operator_reconciliation_metadata"]["verified_by"], "lead_operator")
        self.assertIn("original_uncertainty_evidence", reconciled.payload)

        fetched = self.repo.get_telegram_delivery("8712345678", "key_uncertain")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.status, DeliveryStatus.SENT)
        self.assertEqual(fetched.payload["operator_reconciliation_metadata"]["proof_reference"], "update_id_9928172")

        # 12. Idempotent replay of SENT is safe
        self.repo.record_telegram_delivery(rec_sent)

        # 13. Operational SQLite database path fails closed without filesystem side-effects
        # Construct an uninitialized repository instance via __new__ to guarantee ZERO filesystem access / _init_db()
        op_db_repo = SqliteStateRepository.__new__(SqliteStateRepository)
        op_db_repo.db_path = Path("data/radar.db")
        op_db_repo._is_owned_synthetic_fixture = False

        # Assert that public reconciliation fails closed BEFORE opening any database connection
        mock_op_conn = MagicMock(side_effect=AssertionError("Database connection must NEVER be opened on operational paths"))
        op_db_repo._connection = mock_op_conn
        with self.assertRaises(DeliveryStateError) as op_ctx:
            op_db_repo.reconcile_uncertain_delivery("del_unc_001", valid_proof, operator_authorized=True)
        self.assertIn("unowned or operational", str(op_ctx.exception))
        mock_op_conn.assert_not_called()

        # 14. Unregistered temporary database fails public reconciliation closed by default before connection
        unreg_temp_path = self.tmp_path / "unregistered_temp.db"
        unreg_repo = SqliteStateRepository(unreg_temp_path)
        mock_unreg_conn = MagicMock(side_effect=AssertionError("Database connection must NEVER be opened on unowned repository"))
        unreg_repo._connection = mock_unreg_conn
        with self.assertRaises(DeliveryStateError) as unreg_ctx:
            unreg_repo.reconcile_uncertain_delivery("del_unc_001", valid_proof, operator_authorized=True)
        self.assertIn("unowned or operational", str(unreg_ctx.exception))
        mock_unreg_conn.assert_not_called()

        # 15. Attempting to adopt pre-existing non-empty database as synthetic fixture is rejected
        pre_existing_path = self.tmp_path / "pre_existing_adopted.db"
        pre_existing_path.write_bytes(b"SQLite format 3\x00arbitrary pre-existing database content")
        with self.assertRaises(DeliveryStateError) as adopt_ctx:
            SqliteStateRepository.create_owned_synthetic_fixture(pre_existing_path)
        self.assertIn("Cannot adopt pre-existing non-empty database", str(adopt_ctx.exception))

        # 16. Operational paths cannot be adopted as synthetic fixtures
        with self.assertRaises(DeliveryStateError) as op_adopt_ctx:
            SqliteStateRepository.create_owned_synthetic_fixture(Path("data/radar.db"))
        self.assertIn("operational", str(op_adopt_ctx.exception).lower())

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

        # 2. Write valid ledgers with mixed roles and historical statuses (including HELD_QUIET_HOURS)
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
                "8712345678:d_hash_held": {
                    "status": "HELD_QUIET_HOURS",
                    "delivered_at_utc": "2026-10-19T01:30:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_hash_held",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
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
        self.assertEqual(res_dry.deliveries_imported, 4)
        self.assertEqual(res_dry.deliveries_skipped, 1)  # alias_001 deduplicated
        self.assertEqual(res_dry.budget_records_imported, 1)
        self.assertIn("DRY-RUN", res_dry.summary())

        # Verify database is completely empty after dry-run
        self.assertIsNone(self.repo.get_telegram_delivery("8712345678", "d_hash_001"))

        # 4. Live migration
        res_live = migrate_ledgers_to_repository(self.repo, del_path, bud_path, dry_run=False)
        self.assertTrue(res_live.success)
        self.assertEqual(res_live.deliveries_imported, 4)
        self.assertEqual(res_live.deliveries_skipped, 1)
        self.assertEqual(res_live.budget_records_imported, 1)

        # Verify preserved roles, categories, and HELD_QUIET_HOURS (not FAILED)
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

        rec_held = self.repo.get_telegram_delivery("8712345678", "d_hash_held")
        self.assertIsNotNone(rec_held)
        self.assertEqual(rec_held.status, DeliveryStatus.HELD_QUIET_HOURS)

        # 5. Idempotent re-run: all existing records are skipped safely
        res_replay = migrate_ledgers_to_repository(self.repo, del_path, bud_path, dry_run=False)
        self.assertTrue(res_replay.success)
        self.assertEqual(res_replay.deliveries_imported, 0)
        self.assertEqual(res_replay.deliveries_skipped, 5)  # 4 distinct + 1 alias
        self.assertEqual(res_replay.budget_records_imported, 0)
        self.assertEqual(res_replay.budget_records_skipped, 1)

        # 6. Reject missing/invalid timestamp in historical record (no invented utc_now())
        del_bad_ts = {
            "deliveries": {
                "8712345678:d_bad_ts": {
                    "status": "SENT",
                    "chat_id": "8712345678",
                    "digest_hash": "d_bad_ts",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                    # No timestamp!
                }
            }
        }
        del_bad_path = self.tmp_path / "del_bad_ts.json"
        del_bad_path.write_text(json.dumps(del_bad_ts), encoding="utf-8")

        res_bad_ts = migrate_ledgers_to_repository(self.repo, del_bad_path, dry_run=True)
        self.assertFalse(res_bad_ts.success)
        self.assertTrue(any("Missing delivery timestamp" in e for e in res_bad_ts.errors))

        # 7. Reject missing recipient role without defaulting to PRIMARY_EDITOR
        del_bad_role = {
            "deliveries": {
                "8712345678:d_bad_role": {
                    "status": "SENT",
                    "delivered_at_utc": "2026-10-19T12:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_bad_role",
                    "category": "EDITORIAL_DIGEST",
                    # No recipient_role!
                }
            }
        }
        del_bad_role_path = self.tmp_path / "del_bad_role.json"
        del_bad_role_path.write_text(json.dumps(del_bad_role), encoding="utf-8")
        res_bad_role = migrate_ledgers_to_repository(self.repo, del_bad_role_path, dry_run=True)
        self.assertFalse(res_bad_role.success)
        self.assertTrue(any("recipient role" in e for e in res_bad_role.errors))

        # 8. Reject missing category without defaulting to EDITORIAL_DIGEST
        del_bad_cat = {
            "deliveries": {
                "8712345678:d_bad_cat": {
                    "status": "SENT",
                    "delivered_at_utc": "2026-10-19T12:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_bad_cat",
                    "recipient_role": "PRIMARY_EDITOR",
                    # No category!
                }
            }
        }
        del_bad_cat_path = self.tmp_path / "del_bad_cat.json"
        del_bad_cat_path.write_text(json.dumps(del_bad_cat), encoding="utf-8")
        res_bad_cat = migrate_ledgers_to_repository(self.repo, del_bad_cat_path, dry_run=True)
        self.assertFalse(res_bad_cat.success)
        self.assertTrue(any("notification category" in e for e in res_bad_cat.errors))

        # 9. Conflicting payload test (existing record has different payload)
        del_conflict_payload = {
            "deliveries": {
                "8712345678:d_hash_001": {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": "2026-10-19T12:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_hash_001",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                    "extra_tampered_payload_field": "conflict_value",
                }
            }
        }
        del_conflict_payload_path = self.tmp_path / "del_conflict_payload.json"
        del_conflict_payload_path.write_text(json.dumps(del_conflict_payload), encoding="utf-8")
        res_conflict_p = migrate_ledgers_to_repository(self.repo, del_conflict_payload_path, dry_run=True)
        self.assertFalse(res_conflict_p.success)
        self.assertTrue(len(res_conflict_p.conflicts_detected) > 0)
        self.assertTrue(any("Delivery conflict" in c for c in res_conflict_p.conflicts_detected))

        # 10. Conflicting timestamp test (existing record has different timestamp)
        del_conflict_ts = {
            "deliveries": {
                "8712345678:d_hash_001": {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": "2026-10-19T19:00:00Z",  # Different timestamp!
                    "chat_id": "8712345678",
                    "digest_hash": "d_hash_001",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                }
            }
        }
        del_conflict_ts_path = self.tmp_path / "del_conflict_ts.json"
        del_conflict_ts_path.write_text(json.dumps(del_conflict_ts), encoding="utf-8")
        res_conflict_t = migrate_ledgers_to_repository(self.repo, del_conflict_ts_path, dry_run=True)
        self.assertFalse(res_conflict_t.success)
        self.assertTrue(len(res_conflict_t.conflicts_detected) > 0)

        # 11. Atomic pre-flight & partial-write safety: malformed later entry results in ZERO writes
        del_atomic = {
            "deliveries": {
                "8712345678:d_atomic_valid": {
                    "status": "SENT",
                    "delivered_at_utc": "2026-10-19T12:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_atomic_valid",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                },
                "8712345678:d_atomic_malformed_later": {
                    "status": "SENT",
                    # Missing timestamp!
                    "chat_id": "8712345678",
                    "digest_hash": "d_atomic_malformed_later",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                },
            }
        }
        del_atomic_path = self.tmp_path / "del_atomic.json"
        del_atomic_path.write_text(json.dumps(del_atomic), encoding="utf-8")
        res_atomic = migrate_ledgers_to_repository(self.repo, del_atomic_path, dry_run=False)
        self.assertFalse(res_atomic.success)
        self.assertIsNone(self.repo.get_telegram_delivery("8712345678", "d_atomic_valid"))

        # 12. Non-SQLite write lockout: Turso/mock repository fails closed in write-mode
        mock_turso_repo = MagicMock(spec=StateRepository)
        mock_turso_repo.get_telegram_delivery.return_value = None
        mock_turso_repo.get_budget_record.return_value = None
        # Preflight dry-run on mock/cloud repo succeeds without calling any write methods
        res_mock_dry = migrate_ledgers_to_repository(mock_turso_repo, del_path, bud_path, dry_run=True)
        self.assertTrue(res_mock_dry.success)
        self.assertTrue(res_mock_dry.dry_run)
        self.assertEqual(mock_turso_repo.record_telegram_delivery.call_count, 0)
        self.assertEqual(mock_turso_repo.record_budget_usage.call_count, 0)

        # Write-mode (dry_run=False) on non-SQLite repo MUST fail closed with 0 writes
        res_mock_live = migrate_ledgers_to_repository(mock_turso_repo, del_path, bud_path, dry_run=False)
        self.assertFalse(res_mock_live.success)
        self.assertEqual(res_mock_live.deliveries_imported, 0)
        self.assertEqual(res_mock_live.budget_records_imported, 0)
        self.assertTrue(any("production Turso migration is NOT IMPLEMENTED" in e for e in res_mock_live.errors))
        self.assertEqual(mock_turso_repo.record_telegram_delivery.call_count, 0)
        self.assertEqual(mock_turso_repo.record_budget_usage.call_count, 0)

        # 13. Phase 2 collision rollback: collision occurring between preflight and application triggers full rollback (0 writes)
        del_collision_batch = {
            "deliveries": {
                "8712345678:d_col_001": {
                    "status": "SENT",
                    "delivered_at_utc": "2026-10-19T15:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_col_001",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                },
                "8712345678:d_col_002": {
                    "status": "SENT",
                    "delivered_at_utc": "2026-10-19T15:00:00Z",
                    "chat_id": "8712345678",
                    "digest_hash": "d_col_002",
                    "recipient_role": "PRIMARY_EDITOR",
                    "category": "EDITORIAL_DIGEST",
                },
            }
        }
        del_col_path = self.tmp_path / "del_col.json"
        del_col_path.write_text(json.dumps(del_collision_batch), encoding="utf-8")

        # Hook into repo._connection to inject collision specifically during Phase 2
        original_conn = self.repo._connection
        original_get = self.repo.get_telegram_delivery
        phase_1_done = False

        def tracked_get(chat_id, key):
            res = original_get(chat_id, key)
            if key == "d_col_002":
                nonlocal phase_1_done
                phase_1_done = True
            return res

        @contextmanager
        def colliding_connection():
            with original_conn() as conn:
                if phase_1_done:
                    # In Phase 2: inject colliding record right before Phase 2 executes its batch
                    conn.execute(
                        """
                        INSERT INTO telegram_delivery_records (
                            delivery_id, message_key, recipient_role, recipient_chat_id,
                            category, status, attempt_count, max_attempts, payload_json,
                            created_at, updated_at, sent_at
                        ) VALUES ('del_injected', 'd_col_002', 'PRIMARY_EDITOR', '8712345678',
                                  'EDITORIAL_DIGEST', 'SENT', 1, 3, '{}', '2026-10-19T10:00:00Z',
                                  '2026-10-19T10:00:00Z', '2026-10-19T10:00:00Z')
                        """
                    )
                yield conn

        self.repo.get_telegram_delivery = tracked_get
        self.repo._connection = colliding_connection
        try:
            res_collision = migrate_ledgers_to_repository(self.repo, del_col_path, dry_run=False)
            self.assertFalse(res_collision.success)
            self.assertEqual(res_collision.deliveries_imported, 0)
            self.assertTrue(any("Transactional migration application failed" in e for e in res_collision.errors))
        finally:
            self.repo.get_telegram_delivery = original_get
            self.repo._connection = original_conn

        # Verify that d_col_001 was rolled back and is NOT in the database
        self.assertIsNone(self.repo.get_telegram_delivery("8712345678", "d_col_001"))

        # 14. Eliminate raw chat-ID leakage in migration error reports and conflict exceptions
        raw_secret_chat = "9988776655"
        masked_secret_chat = "99...55"
        del_leak_data = {
            "deliveries": {
                f"{raw_secret_chat}:d_leak_malformed": {
                    "status": "SENT",
                    "delivered_at_utc": "2026-10-19T12:00:00Z",
                    "chat_id": raw_secret_chat,
                    "digest_hash": "d_leak_malformed",
                    "recipient_role": "PRIMARY_EDITOR",
                    # Missing category to trigger validation error with chat ID
                }
            }
        }
        del_leak_path = self.tmp_path / "del_leak.json"
        del_leak_path.write_text(json.dumps(del_leak_data), encoding="utf-8")

        res_leak = migrate_ledgers_to_repository(self.repo, del_leak_path, dry_run=True)
        self.assertFalse(res_leak.success)
        for err in res_leak.errors:
            self.assertNotIn(raw_secret_chat, err)
        for conf in res_leak.conflicts_detected:
            self.assertNotIn(raw_secret_chat, conf)
        self.assertNotIn(raw_secret_chat, res_leak.summary())
        self.assertTrue(any(masked_secret_chat in err for err in res_leak.errors))

        # Also assert that DeliveryIdentityConflictError never leaks raw chat ID
        rec_leak_initial = TelegramDeliveryRecord(
            delivery_id="del_leak_init",
            message_key="key_leak_test",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=raw_secret_chat,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.SENT,
            attempt_count=1,
            sent_at=utc_now(),
        )
        self.repo.record_telegram_delivery(rec_leak_initial)

        rec_leak_conflict = TelegramDeliveryRecord(
            delivery_id="del_leak_conf",
            message_key="key_leak_test",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=raw_secret_chat,
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,  # Conflicting category
            status=DeliveryStatus.SENT,
            attempt_count=1,
        )
        with self.assertRaises(DeliveryIdentityConflictError) as leak_ctx:
            self.repo.record_telegram_delivery(rec_leak_conflict)
        self.assertNotIn(raw_secret_chat, str(leak_ctx.exception))
        self.assertIn(masked_secret_chat, str(leak_ctx.exception))

        # 15. Exception-log privacy leak elimination: capture logger output and verify raw chat IDs never appear
        import logging
        del_leak_syntax = self.tmp_path / "del_leak_syntax.json"
        del_leak_syntax.write_text(f'{{"{raw_secret_chat}:malformed": {{ invalid_json_syntax', encoding="utf-8")

        migration_logger = logging.getLogger("efso-radar.repository.migration_ledger")
        with self.assertLogs(migration_logger, level="ERROR") as log_ctx:
            res_syntax_leak = migrate_ledgers_to_repository(self.repo, delivery_ledger_path=del_leak_syntax, dry_run=True)
            self.assertFalse(res_syntax_leak.success)

        # Assert raw chat ID never appeared in any captured log message
        for log_record in log_ctx.output:
            self.assertNotIn(raw_secret_chat, log_record)
            self.assertIn("[ERR_DELIVERY_JSON_SYNTAX]", log_record)
        # Also assert summary and errors are sanitized
        for err in res_syntax_leak.errors:
            self.assertNotIn(raw_secret_chat, err)
        self.assertNotIn(raw_secret_chat, res_syntax_leak.summary())

        # 16. Strict JSON ledger schema validation: reject arrays, nulls, missing containers, wrong container types
        # a. JSON Array root rejected with 0 writes
        p_array = self.tmp_path / "ledger_array.json"
        p_array.write_text("[]", encoding="utf-8")
        res_arr = migrate_ledgers_to_repository(self.repo, delivery_ledger_path=p_array, dry_run=False)
        self.assertFalse(res_arr.success)
        self.assertTrue(any("top-level JSON must be an object/dict" in e for e in res_arr.errors))
        self.assertEqual(res_arr.deliveries_imported, 0)

        # b. JSON Null root rejected with 0 writes
        p_null = self.tmp_path / "ledger_null.json"
        p_null.write_text("null", encoding="utf-8")
        res_null = migrate_ledgers_to_repository(self.repo, delivery_ledger_path=p_null, dry_run=False)
        self.assertFalse(res_null.success)
        self.assertTrue(any("top-level JSON must be an object/dict" in e for e in res_null.errors))
        self.assertEqual(res_null.deliveries_imported, 0)

        # c. Missing required containers rejected with 0 writes
        p_del_missing = self.tmp_path / "del_missing_container.json"
        p_del_missing.write_text(json.dumps({"version": 1}), encoding="utf-8")
        res_del_m = migrate_ledgers_to_repository(self.repo, delivery_ledger_path=p_del_missing, dry_run=False)
        self.assertFalse(res_del_m.success)
        self.assertTrue(any("missing required 'deliveries' container field" in e for e in res_del_m.errors))
        self.assertEqual(res_del_m.deliveries_imported, 0)

        p_bud_missing = self.tmp_path / "bud_missing_container.json"
        p_bud_missing.write_text(json.dumps({"experiment_id": "exp"}), encoding="utf-8")
        res_bud_m = migrate_ledgers_to_repository(self.repo, budget_ledger_path=p_bud_missing, dry_run=False)
        self.assertFalse(res_bud_m.success)
        self.assertTrue(any("missing required 'batches' container field" in e for e in res_bud_m.errors))
        self.assertEqual(res_bud_m.budget_records_imported, 0)

        # d. Incorrectly typed containers rejected with 0 writes
        p_del_wrong_type = self.tmp_path / "del_wrong_type.json"
        p_del_wrong_type.write_text(json.dumps({"deliveries": [1, 2, 3]}), encoding="utf-8")
        res_del_w = migrate_ledgers_to_repository(self.repo, delivery_ledger_path=p_del_wrong_type, dry_run=False)
        self.assertFalse(res_del_w.success)
        self.assertTrue(any("'deliveries' field must be a dictionary/object" in e for e in res_del_w.errors))
        self.assertEqual(res_del_w.deliveries_imported, 0)

        p_bud_wrong_type = self.tmp_path / "bud_wrong_type.json"
        p_bud_wrong_type.write_text(json.dumps({"batches": "invalid_type"}), encoding="utf-8")
        res_bud_w = migrate_ledgers_to_repository(self.repo, budget_ledger_path=p_bud_wrong_type, dry_run=False)
        self.assertFalse(res_bud_w.success)
        self.assertTrue(any("'batches' field must be a dictionary/object" in e for e in res_bud_w.errors))
        self.assertEqual(res_bud_w.budget_records_imported, 0)

        # e. Genuinely empty but valid ledgers succeed with 0 imported, 0 skipped, 0 writes
        p_del_empty = self.tmp_path / "del_valid_empty.json"
        p_del_empty.write_text(json.dumps({"version": 1, "deliveries": {}}), encoding="utf-8")
        p_bud_empty = self.tmp_path / "bud_valid_empty.json"
        p_bud_empty.write_text(json.dumps({"batches": {}}), encoding="utf-8")

        res_empty = migrate_ledgers_to_repository(self.repo, p_del_empty, p_bud_empty, dry_run=False)
        self.assertTrue(res_empty.success)
        self.assertEqual(res_empty.deliveries_imported, 0)
        self.assertEqual(res_empty.deliveries_skipped, 0)
        self.assertEqual(res_empty.budget_records_imported, 0)
        self.assertEqual(res_empty.budget_records_skipped, 0)
        self.assertEqual(len(res_empty.errors), 0)

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

        # 5. Conflicting replay with different worker_id raises BudgetHistoryConflictError
        with self.assertRaises(BudgetHistoryConflictError):
            self.repo.record_budget_usage(
                period_month="2026-10",
                run_id="exp_super_long_event_identifier_1234567890_abcdef",
                worker_id="runner_2",  # Different worker!
                calls_count=2,
                prompt_tokens=2000,
                completion_tokens=500,
                total_tokens=2500,
                estimated_cost_usd=0.0025,
                timestamp=ts,
            )

        # 6. Conflicting replay with different timestamp raises BudgetHistoryConflictError
        with self.assertRaises(BudgetHistoryConflictError):
            self.repo.record_budget_usage(
                period_month="2026-10",
                run_id="exp_super_long_event_identifier_1234567890_abcdef",
                worker_id="runner_1",
                calls_count=2,
                prompt_tokens=2000,
                completion_tokens=500,
                total_tokens=2500,
                estimated_cost_usd=0.0025,
                timestamp=datetime(2026, 10, 19, 18, 0, 0, tzinfo=timezone.utc),  # Different ts!
            )

        # 7. Reject missing timestamp in historical budget ledger (no month-start 01T00:00:00 invention)
        bud_bad_ts = {
            "experiment_id": "test_exp",
            "batches": {
                "b_bad_ts": {
                    "actual_cost_usd": 0.001,
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "period_month": "2026-10",
                    # No timestamp!
                }
            }
        }
        bud_bad_path = self.tmp_path / "bud_bad_ts.json"
        bud_bad_path.write_text(json.dumps(bud_bad_ts), encoding="utf-8")
        res_bad_bud = migrate_ledgers_to_repository(self.repo, budget_ledger_path=bud_bad_path, dry_run=True)
        self.assertFalse(res_bad_bud.success)
        self.assertTrue(any("Missing required timestamp" in e for e in res_bad_bud.errors))

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
            init_schema=True,
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

        # 5. Delivery UNCERTAIN -> SENT locked down on Turso; reconcile_uncertain_delivery parity
        mock_client.execute.return_value = TursoQueryResult(
            cols=["delivery_id", "message_key", "recipient_role", "recipient_chat_id", "category", "status", "attempt_count", "max_attempts", "error_message", "payload_json", "created_at", "updated_at", "sent_at"],
            rows=[{
                "delivery_id": "del_unc_t",
                "message_key": "key_unc_t",
                "recipient_role": "PRIMARY_EDITOR",
                "recipient_chat_id": "8712345678",
                "category": "EDITORIAL_DIGEST",
                "status": "DELIVERY_UNCERTAIN",
                "attempt_count": 1,
                "max_attempts": 3,
                "error_message": "Network drop",
                "payload_json": '{"digest": "test"}',
                "created_at": "2026-10-19T10:00:00Z",
                "updated_at": "2026-10-19T10:00:00Z",
                "sent_at": None,
            }],
            affected_row_count=1,
        )
        with self.assertRaises(DeliveryStatusRegressionError):
            turso_repo.update_telegram_delivery_status("del_unc_t", DeliveryStatus.SENT)

        proof = {
            "reconciliation_method": "telegram_update_offset_check",
            "verified_by": "lead_operator",
            "proof_reference": "ref_123",
        }
        # Turso reconciliation is unconditionally disabled in Phase 5B.3B across all 4 caller flag combinations
        for lm, oa in [(False, False), (False, True), (True, False), (True, True)]:
            with self.assertRaises(DeliveryStateError) as t_ctx:
                turso_repo.reconcile_uncertain_delivery("del_unc_t", proof, operator_authorized=oa, live_mode=lm)
            self.assertIn("NOT IMPLEMENTED", str(t_ctx.exception))
            self.assertIn("unconditionally disabled", str(t_ctx.exception))

        # Assert no UPDATE SQL statement was ever executed on Turso client
        for call_args in mock_client.execute.call_args_list:
            sql = call_args[0][0]
            self.assertNotIn("SET status = 'SENT'", sql)

        # Identity conflict on Turso masks raw recipient chat ID
        rec_turso_conflict = TelegramDeliveryRecord(
            delivery_id="del_t_conflict",
            message_key="key_unc_t",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="8712345678",
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            status=DeliveryStatus.SENT,
            attempt_count=1,
        )
        with self.assertRaises(DeliveryIdentityConflictError) as turso_ctx:
            turso_repo.record_telegram_delivery(rec_turso_conflict)
        self.assertNotIn("8712345678", str(turso_ctx.exception))
        self.assertIn("87...78", str(turso_ctx.exception))

        # 6. Budget conflict check on Turso (prompt tokens conflict)
        mock_client.execute.return_value = TursoQueryResult(
            cols=["record_id", "period_month", "run_id", "worker_id", "timestamp", "calls_count", "prompt_tokens", "completion_tokens", "total_tokens", "estimated_cost_usd"],
            rows=[{
                "record_id": "bud_2026-10_run_1",
                "period_month": "2026-10",
                "run_id": "run_1",
                "worker_id": "w1",
                "timestamp": "2026-10-19T10:00:00+00:00",
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
                timestamp=datetime(2026, 10, 19, 10, 0, 0, tzinfo=timezone.utc),
            )

        # 7. Budget conflict check on Turso (worker_id conflict)
        with self.assertRaises(BudgetHistoryConflictError):
            turso_repo.record_budget_usage(
                period_month="2026-10",
                run_id="run_1",
                worker_id="different_worker",  # Conflict!
                calls_count=1,
                prompt_tokens=1000,
                completion_tokens=200,
                total_tokens=1200,
                estimated_cost_usd=0.001,
                timestamp=datetime(2026, 10, 19, 10, 0, 0, tzinfo=timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
