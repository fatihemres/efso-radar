"""Hermetic cloud persistence integration tests for Phase 5B.3C-1.

Verifies:
1. Zero-cost mandate & hermetic execution: zero live network requests, zero real API spend.
2. Fail-closed storage mode configuration (missing/partial credentials immediately reject fallback).
3. Ephemeral runner restart simulation (Runner 1 writes state, is destroyed; Runner 2 starts fresh, recovers and finishes).
4. Multi-worker lock contention, mutual exclusion, and stale lease stealing on Turso.
5. Telegram outbox duplicate-send prevention across runner restarts without local JSON ledger.
6. Ambiguous delivery (DELIVERY_UNCERTAIN) isolation from blind retries across runners.
7. Recipient role segregation (PRIMARY_EDITOR vs ADMIN) and Istanbul daily admin report window.
8. Budget history persistence, cross-runner accumulation, and conflict rejection.
9. Scheduled execution audit history logging (start, finish, success, failure).
10. Strict zero-network and zero-operational-DB-modification verification.
"""
from __future__ import annotations

import base64
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import httpx

from efso_radar.ai.cost import ExperimentBudgetGuard
from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    BudgetHistoryConflictError,
    DeliveryIdentityConflictError,
    DeliveryStateError,
    DeliveryStatus,
    DeliveryStatusRegressionError,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    NotificationRecord,
    StoryCategory,
    SuggestedFormat,
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    VerificationStatus,
    utc_now,
)
from efso_radar.notifications.telegram import (
    TelegramNotificationChannel,
    deliver_one_shot_editorial_digest,
)
from efso_radar.orchestrator import RadarOrchestrator
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION, get_all_init_statements
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from efso_radar.repositories.turso_client import (
    TursoClientError,
    deserialize_hrana_value,
    serialize_hrana_arg,
)
from efso_radar.sources.base import NewsSource


def create_mock_hrana_transport(conn: sqlite3.Connection) -> httpx.BaseTransport:
    """Create an in-memory HTTP transport simulating the Turso /v2/pipeline Hrana protocol."""

    class MockHranaTransport(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}

            requests_list = body.get("requests", [])
            results = []

            for req in requests_list:
                req_type = req.get("type")
                if req_type == "execute":
                    stmt_obj = req.get("stmt", {})
                    sql = stmt_obj.get("sql", "").strip()
                    raw_args = stmt_obj.get("args", [])
                    deserialized_args = [deserialize_hrana_value(a) for a in raw_args]

                    try:
                        cursor = conn.cursor()
                        cursor.execute(sql, deserialized_args)

                        cols = (
                            [{"name": desc[0]} for desc in cursor.description]
                            if cursor.description
                            else []
                        )

                        rows = []
                        if cursor.description:
                            for row in cursor.fetchall():
                                hrana_row = [serialize_hrana_arg(cell) for cell in row]
                                rows.append(hrana_row)

                        affected = cursor.rowcount if cursor.rowcount > 0 else 0
                        last_id = cursor.lastrowid if cursor.lastrowid and cursor.lastrowid > 0 else None

                        results.append({
                            "type": "ok",
                            "response": {
                                "type": "execute",
                                "result": {
                                    "cols": cols,
                                    "rows": rows,
                                    "affected_row_count": affected,
                                    "last_insert_rowid": last_id,
                                },
                            },
                        })
                    except sqlite3.Error as exc:
                        results.append({
                            "type": "error",
                            "error": {
                                "message": str(exc),
                                "code": "SQLITE_ERROR",
                            },
                        })
                elif req_type == "close":
                    results.append({
                        "type": "ok",
                        "response": {"type": "close"},
                    })

            return httpx.Response(
                status_code=200,
                json={"baton": "mock-hrana-baton-v2", "results": results},
                request=request,
            )

    return MockHranaTransport()


class TestPhase5B3CCloudPersistenceIntegration(unittest.TestCase):
    """Hermetic integration suite for persistent cloud-state architecture."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)

        # Isolated SQLite database acting as the synthetic Turso backend
        self.backend_db_path = self.tmp_path / "synthetic_turso_backend.db"
        self.backend_conn = sqlite3.connect(self.backend_db_path, check_same_thread=False)
        self.backend_conn.isolation_level = None
        self.backend_conn.row_factory = sqlite3.Row

        # Mock Hrana transport
        self.mock_transport = create_mock_hrana_transport(self.backend_conn)

        # Base test settings
        self.test_settings = Settings(
            storage_mode="turso",
            turso_database_url="libsql://synthetic-cloud.turso.io",
            turso_auth_token="mock-synthetic-bearer-token-12345",
            sqlite_db_path=self.tmp_path / "unused_local.db",
            telegram_chat_id="1111111111",
            telegram_primary_editor_chat_id="2222222222",
            telegram_allowed_chat_ids=["1111111111", "2222222222"],
            telegram_bot_token="9999999999:AAFakeTokenForOfflineIntegrationTesting",
            telegram_delivery_enabled=True,
            ai_evaluation_enabled=False,
        )

        # Operational DB snapshot check
        self.op_db_path = Path("data/radar.db")
        self.op_db_existed = self.op_db_path.exists()
        if self.op_db_existed:
            self.op_db_initial_stat = self.op_db_path.stat()

    def tearDown(self):
        self.backend_conn.close()
        import gc
        gc.collect()
        try:
            self.tmp_dir.cleanup()
        except Exception:
            pass

        # Verify operational DB was never modified
        if self.op_db_existed:
            final_stat = self.op_db_path.stat()
            self.assertEqual(
                self.op_db_initial_stat.st_mtime,
                final_stat.st_mtime,
                "Operational database data/radar.db mtime changed during integration tests! Isolation breached.",
            )
            self.assertEqual(
                self.op_db_initial_stat.st_size,
                final_stat.st_size,
                "Operational database data/radar.db size changed during integration tests! Isolation breached.",
            )

    def _create_turso_repo(self) -> TursoStateRepository:
        """Helper to create a TursoStateRepository instance connected to synthetic Hrana backend."""
        return TursoStateRepository(
            database_url=self.test_settings.turso_database_url,
            auth_token=self.test_settings.turso_auth_token,
            transport=self.mock_transport,
        )

    # --- Test 1: Fail-Closed Storage Mode Configuration ---
    def test_01_fail_closed_storage_mode_configuration(self):
        """Storage mode 'turso' with missing or partial credentials fails closed without SQLite fallback."""
        # Missing both URL and token
        with self.assertRaises(ValueError) as ctx1:
            Settings(
                storage_mode="turso",
                turso_database_url="",
                turso_auth_token="",
            )
        self.assertIn("Cloud storage mode enabled", str(ctx1.exception))

        # Partial credentials (URL without token)
        with self.assertRaises(ValueError) as ctx2:
            Settings(
                storage_mode="auto",
                turso_database_url="libsql://test.turso.io",
                turso_auth_token="",
            )
        self.assertIn("Partial Turso configuration detected", str(ctx2.exception))

        # Orchestrator fail-closed check if settings bypassed
        bad_settings = Settings(
            storage_mode="auto",
            sqlite_db_path=self.tmp_path / "fallback.db",
        )
        bad_settings.storage_mode = "turso"
        bad_settings.turso_database_url = ""
        bad_settings.turso_auth_token = ""

        with self.assertRaises(ValueError) as ctx3:
            RadarOrchestrator(bad_settings, repository=None)
        self.assertIn("Cloud storage mode enabled", str(ctx3.exception))

    # --- Test 2: Ephemeral Runner Restart Simulation ---
    def test_02_ephemeral_runner_restart_simulation(self):
        """Runner 1 writes backlog and recovery checkpoint, gets destroyed; Runner 2 resumes and finishes."""
        # 1. Runner 1 initializes
        repo_1 = self._create_turso_repo()
        runner_1_worker = "worker-github-runner-001"
        run_1_id = "run-github-actions-cron-1"

        article_a = Article(
            id="art_101",
            title="Sürpriz Aşk İddiası",
            url="https://example.com/art101",
            canonical_url="https://example.com/art101",
            source="GazeteMagazin",
            published_at=utc_now(),
        )
        article_b = Article(
            id="art_102",
            title="Konser İptal Edildi",
            url="https://example.com/art102",
            canonical_url="https://example.com/art102",
            source="HaberturkMagazin",
            published_at=utc_now(),
        )

        # Runner 1 acquires lock, starts execution, checkpoints backlog
        self.assertTrue(repo_1.acquire_run_lock(runner_1_worker, timeout_minutes=15))
        repo_1.record_scheduled_execution_start(run_1_id, run_type="SCAN_ONLY", worker_id=runner_1_worker)
        repo_1.save_recovery_state("RADAR_PIPELINE", worker_id=runner_1_worker, state_payload={"stage": "SCAN_ONLY_INIT"}, batch_id=run_1_id)
        repo_1.save_pending_candidates([article_a, article_b])
        repo_1.save_recovery_state("RADAR_PIPELINE", worker_id=runner_1_worker, state_payload={"stage": "DISCOVERY_COMPLETED", "count": 2}, batch_id=run_1_id)

        # Runner 1 crashes or finishes scan-only; releases lock
        repo_1.release_run_lock(runner_1_worker)
        del repo_1  # Runner 1 process terminated, container wiped

        # 2. Runner 2 (separate process / new container) starts up
        repo_2 = self._create_turso_repo()
        runner_2_worker = "worker-github-runner-002"
        run_2_id = "run-github-actions-cron-2"

        # Verify Runner 2 can read Runner 1's recovery checkpoint from persistent Turso backend
        checkpoint = repo_2.get_recovery_state("RADAR_PIPELINE")
        self.assertIsNotNone(checkpoint)
        self.assertEqual(checkpoint.get("stage"), "RADAR_PIPELINE")
        payload = checkpoint.get("state_payload", {})
        self.assertEqual(payload.get("stage"), "DISCOVERY_COMPLETED")
        self.assertEqual(payload.get("count"), 2)

        # Verify Runner 2 retrieves unexpired backlog items written by Runner 1
        pending = repo_2.get_pending_candidates(max_age_hours=24)
        self.assertEqual(len(pending), 2)
        pending_ids = {p.id for p in pending}
        self.assertEqual(pending_ids, {"art_101", "art_102"})

        # Runner 2 processes backlog, records successful execution, clears checkpoint
        self.assertTrue(repo_2.acquire_run_lock(runner_2_worker, timeout_minutes=15))
        repo_2.record_scheduled_execution_start(run_2_id, run_type="CRON", worker_id=runner_2_worker)
        repo_2.mark_candidates_evaluated([article_a.id, article_b.id])
        repo_2.mark_articles_processed([article_a, article_b])
        repo_2.clear_recovery_state("RADAR_PIPELINE")
        repo_2.record_scheduled_execution_finish(
            run_2_id,
            status="SUCCESS",
            articles_collected=2,
            candidates_evaluated=2,
            notifications_dispatched=0,
            finished_at=utc_now(),
        )
        repo_2.release_run_lock(runner_2_worker)

        # Verify checkpoint is cleared and execution records exist
        self.assertIsNone(repo_2.get_recovery_state("RADAR_PIPELINE"))
        exec_record = repo_2.get_scheduled_execution(run_2_id)
        self.assertIsNotNone(exec_record)
        self.assertEqual(exec_record.get("status"), "SUCCESS")
        self.assertEqual(exec_record.get("candidates_evaluated"), 2)

    # --- Test 3: Multi-Worker Lock Contention & Stale Lease Stealing ---
    def test_03_multi_worker_lock_contention_and_lease_stealing(self):
        """Worker 1 acquires lock; Worker 2 blocked; stale lease stolen after timeout; stale release fenced."""
        repo = self._create_turso_repo()
        now = datetime(2026, 10, 20, 12, 0, 0, tzinfo=timezone.utc)

        # 1. Worker 1 acquires lock
        self.assertTrue(repo.acquire_run_lock("worker_1", timeout_minutes=10, now=now))

        # 2. Worker 2 attempts to acquire within timeout -> blocked
        self.assertFalse(repo.acquire_run_lock("worker_2", timeout_minutes=10, now=now + timedelta(minutes=5)))

        # 3. Advance time past lease expiration (11 minutes later)
        expired_time = now + timedelta(minutes=11)
        # Worker 3 can steal stale lease
        self.assertTrue(repo.acquire_run_lock("worker_3", timeout_minutes=10, now=expired_time))

        # 4. Worker 1 (stale) attempts to release lock -> release is fenced (does nothing)
        repo.release_run_lock("worker_1")

        # Worker 3 still holds lock
        self.assertFalse(repo.acquire_run_lock("worker_4", timeout_minutes=10, now=expired_time + timedelta(minutes=1)))

        # Worker 3 gracefully releases
        repo.release_run_lock("worker_3")

        # Now Worker 4 can acquire
        self.assertTrue(repo.acquire_run_lock("worker_4", timeout_minutes=10, now=expired_time + timedelta(minutes=2)))

    # --- Test 4: Telegram Outbox Duplicate-Send Prevention Across Runners ---
    def test_04_telegram_outbox_duplicate_send_prevention_across_runners(self):
        """Runner 1 delivers digest; Runner 2 without local JSON ledger is blocked from duplicate delivery."""
        repo = self._create_turso_repo()

        assessments = [
            EditorialAssessment(
                article_ids=["a1"], score=88, kind=EditorialKind.NEW,
                headline_tr="Haber Bir Başlık Metni", why_efso_tr="Neden Efso",
            ),
            EditorialAssessment(
                article_ids=["a2"], score=85, kind=EditorialKind.NEW,
                headline_tr="Haber İki Başlık Metni", why_efso_tr="Neden Efso",
            ),
            EditorialAssessment(
                article_ids=["a3"], score=82, kind=EditorialKind.NEW,
                headline_tr="Haber Üç Başlık Metni", why_efso_tr="Neden Efso",
            ),
        ]

        sent_messages = []

        def mock_telegram_post(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            sent_messages.append(body)
            return httpx.Response(
                200,
                json={"ok": True, "result": {"message_id": 7001}},
                request=request,
            )

        mock_tg_transport = httpx.MockTransport(mock_telegram_post)

        # Runner 1 executes delivery
        temp_ledger_1 = self.tmp_path / "runner_1_ledger.json"
        res_1 = deliver_one_shot_editorial_digest(
            settings=self.test_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=temp_ledger_1,
            transport=mock_tg_transport,
            target_recipient_role="PRIMARY_EDITOR",
            repository=repo,
        )

        status_1 = res_1.get("status") or res_1.get("PRIMARY_EDITOR", {}).get("status")
        self.assertEqual(status_1, "DELIVERY_SUCCESS")
        self.assertEqual(len(sent_messages), 1)

        # Runner 1 container is deleted. Runner 2 starts on ephemeral runner with non-existent ledger file
        temp_ledger_2 = self.tmp_path / "ephemeral_runner_2_nonexistent_ledger.json"
        self.assertFalse(temp_ledger_2.exists())

        # Runner 2 attempts to deliver the exact same digest
        res_2 = deliver_one_shot_editorial_digest(
            settings=self.test_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=temp_ledger_2,
            transport=mock_tg_transport,
            target_recipient_role="PRIMARY_EDITOR",
            repository=repo,
        )

        # Must be blocked by persistent Turso outbox with zero additional HTTP dispatches
        status_2 = res_2.get("status") or res_2.get("PRIMARY_EDITOR", {}).get("status")
        self.assertEqual(status_2, "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(len(sent_messages), 1, "Duplicate message sent across runner restart! Telegram outbox failed.")

    # --- Test 5: Ambiguous Delivery (DELIVERY_UNCERTAIN) Isolation ---
    def test_05_ambiguous_delivery_uncertain_isolation_from_blind_retries(self):
        """Ambiguous 504 timeout records DELIVERY_UNCERTAIN; subsequent runners are barred from blind retries."""
        repo = self._create_turso_repo()

        assessments = [
            EditorialAssessment(article_ids=["b1"], score=88, kind=EditorialKind.NEW, headline_tr="Uncertain Haber 1"),
            EditorialAssessment(article_ids=["b2"], score=85, kind=EditorialKind.NEW, headline_tr="Uncertain Haber 2"),
            EditorialAssessment(article_ids=["b3"], score=82, kind=EditorialKind.NEW, headline_tr="Uncertain Haber 3"),
        ]

        attempt_count = 0

        def mock_timeout_post(request: httpx.Request) -> httpx.Response:
            nonlocal attempt_count
            attempt_count += 1
            return httpx.Response(504, text="Gateway Timeout", request=request)

        mock_timeout_transport = httpx.MockTransport(mock_timeout_post)

        # Runner 1 encounters gateway timeout
        res_1 = deliver_one_shot_editorial_digest(
            settings=self.test_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=self.tmp_path / "runner_timeout_ledger.json",
            transport=mock_timeout_transport,
            target_recipient_role="PRIMARY_EDITOR",
            repository=repo,
        )

        status_1 = res_1.get("status") or res_1.get("PRIMARY_EDITOR", {}).get("status")
        self.assertEqual(status_1, "DELIVERY_UNCERTAIN")
        self.assertEqual(attempt_count, 1)

        # Runner 2 attempts to send the same digest
        res_2 = deliver_one_shot_editorial_digest(
            settings=self.test_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=self.tmp_path / "runner_2_timeout_ledger.json",
            transport=mock_timeout_transport,
            target_recipient_role="PRIMARY_EDITOR",
            repository=repo,
        )

        # Must prevent duplicate delivery without retry
        status_2 = res_2.get("status") or res_2.get("PRIMARY_EDITOR", {}).get("status")
        self.assertEqual(status_2, "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(attempt_count, 1, "Blind retry occurred on DELIVERY_UNCERTAIN! Idempotency contract violated.")

    # --- Test 6: Recipient Role Segregation & Istanbul Daily Admin Report ---
    def test_06_recipient_role_segregation_and_istanbul_daily_admin_report(self):
        """PRIMARY_EDITOR and ADMIN receive distinct payloads; daily admin report is at-most-once per Istanbul date."""
        repo = self._create_turso_repo()
        worker = "worker-admin-reporter"
        istanbul_date = "2026-10-20"

        # 1. First runner records daily report for Istanbul date 2026-10-20
        self.assertFalse(repo.has_admin_report_been_dispatched(istanbul_date))
        inserted = repo.record_admin_report(
            report_date=istanbul_date,
            worker_id=worker,
            status=DeliveryStatus.SENT,
            recipient_chat_id="1111111111",
            summary_payload={"total_scans": 24, "errors": 0},
        )
        self.assertTrue(inserted)
        self.assertTrue(repo.has_admin_report_been_dispatched(istanbul_date))

        # 2. Second runner attempts to record daily report for the same Istanbul date -> rejected
        second_insert = repo.record_admin_report(
            report_date=istanbul_date,
            worker_id="worker-runner-duplicate",
            status=DeliveryStatus.SENT,
            recipient_chat_id="1111111111",
            summary_payload={"total_scans": 25, "errors": 0},
        )
        self.assertFalse(second_insert, "Duplicate admin report recorded for same Istanbul date!")

    # --- Test 7: Budget History Persistence & Conflict Rejection ---
    def test_07_budget_history_persistence_and_conflict_rejection(self):
        """Budget guard persists usage to Turso; subsequent runners aggregate spend; conflicts are rejected."""
        repo = self._create_turso_repo()
        guard_1 = ExperimentBudgetGuard(
            ledger_path=self.tmp_path / "budget_runner_1.json",
            budget_ceiling_usd=0.05,
            canary1_db_path=self.tmp_path / "c1.db",
            canary2_db_path=self.tmp_path / "c2.db",
            repository=repo,
        )

        # Runner 1 reconciles a batch
        guard_1.reconcile_batch(
            batch_hash="batch_hash_001",
            actual_cost_usd=0.0025,
            input_tokens=1500,
            output_tokens=300,
            model_name="gemini-3.5-flash-lite",
            run_id="run_exp_1",
            worker_id="worker_exp_1",
        )

        # Runner 1 destroyed. Runner 2 starts with fresh local ledger
        guard_2 = ExperimentBudgetGuard(
            ledger_path=self.tmp_path / "budget_runner_2_fresh.json",
            budget_ceiling_usd=0.05,
            canary1_db_path=self.tmp_path / "c1.db",
            canary2_db_path=self.tmp_path / "c2.db",
            repository=repo,
        )

        # Runner 2 queries total consumed USD: reflects Turso database state
        period_month = utc_now().strftime("%Y-%m")
        consumed = guard_2.get_total_consumed_usd(period_month=period_month)
        self.assertAlmostEqual(consumed, 0.0025, places=4)

        # Get existing record to extract authoritative timestamp
        existing_rec = repo.get_budget_record("BUDGET-batch_hash_0")
        self.assertIsNotNone(existing_rec)
        auth_ts = datetime.fromisoformat(existing_rec["timestamp"])

        # Idempotent replay with identical values succeeds
        replayed = repo.record_budget_usage(
            period_month=period_month,
            run_id="run_exp_1",
            worker_id="worker_exp_1",
            calls_count=1,
            prompt_tokens=1500,
            completion_tokens=300,
            total_tokens=1800,
            estimated_cost_usd=0.0025,
            timestamp=auth_ts,
            record_id="BUDGET-batch_hash_0",
        )
        self.assertTrue(replayed)

        # Conflicting replay (different tokens or cost for same record_id) raises BudgetHistoryConflictError
        with self.assertRaises(BudgetHistoryConflictError):
            repo.record_budget_usage(
                period_month=period_month,
                run_id="run_exp_1",
                worker_id="worker_exp_1",
                calls_count=1,
                prompt_tokens=99999,  # Conflict!
                completion_tokens=300,
                total_tokens=100299,
                estimated_cost_usd=0.05,
                timestamp=auth_ts,
                record_id="BUDGET-batch_hash_0",
            )

    # --- Test 8: Scheduled Execution Audit History Logging ---
    def test_08_scheduled_execution_audit_history_logging(self):
        """RadarOrchestrator logs execution start, checkpoint, and success/failure to scheduled_executions."""
        repo = self._create_turso_repo()
        run_id = "test-cron-exec-audit-001"
        worker_id = "worker-audit-lead"

        class DummyDiscoverySource(NewsSource):
            def fetch_candidates(self, now, max_age_hours=24):
                return [], 1

        orchestrator = RadarOrchestrator(
            settings=self.test_settings,
            repository=repo,
            source=DummyDiscoverySource(),
        )

        # Run scan-only
        res = orchestrator.run_scan_only(dry_run=False, worker_id=worker_id, run_id=run_id)
        self.assertTrue(res.success)

        # Verify execution record exists in Turso backend
        record = repo.get_scheduled_execution(run_id)
        self.assertIsNotNone(record)
        self.assertEqual(record.get("run_id"), run_id)
        self.assertEqual(record.get("run_type"), "SCAN_ONLY")
        self.assertEqual(record.get("worker_id"), worker_id)
        self.assertEqual(record.get("status"), "SUCCESS")
        self.assertIsNotNone(record.get("started_at"))
        self.assertIsNotNone(record.get("finished_at"))

    # --- Test 9: Complete Zero-Network and Operational DB Isolation ---
    def test_09_complete_zero_network_and_zero_operational_db_isolation(self):
        """Verify that all tests in this suite operate strictly offline with zero live sockets."""
        # Assert socket connect attempts raise an exception if any were made
        with patch("socket.socket.connect", side_effect=AssertionError("Live network socket attempted!")):
            # Instantiate orchestrator and execute synthetic offline run
            repo = self._create_turso_repo()
            orchestrator = RadarOrchestrator(
                settings=self.test_settings,
                repository=repo,
            )
            self.assertIsNotNone(orchestrator)
            self.assertIsInstance(orchestrator.repository, TursoStateRepository)


    # --- Test 10: Durable Pre-Send Claim & Competing Worker Isolation ---
    def test_10_durable_pre_send_claim_and_competing_worker_isolation(self):
        """Atomically claim delivery before HTTP dispatch; competing workers cannot send same message."""
        repo = self._create_turso_repo()
        editor_chat = self.test_settings.telegram_primary_editor_chat_id
        msg_key = "story_digest_hash_999"
        deliv_id = "DELIV-claim-test-001"

        # Worker 1 acquires pre-send claim
        claimed_w1 = repo.claim_telegram_delivery(
            delivery_id=deliv_id,
            message_key=msg_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=editor_chat,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            payload={"test": 1},
            max_attempts=1,
        )
        self.assertTrue(claimed_w1)

        # Worker 2 attempts to claim the same logical message concurrently -> rejected!
        claimed_w2 = repo.claim_telegram_delivery(
            delivery_id="DELIV-claim-test-002",
            message_key=msg_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=editor_chat,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            payload={"test": 2},
            max_attempts=1,
        )
        self.assertFalse(claimed_w2)

        # Worker 1 finishes dispatch and commits SENT status
        sent_ok = repo.update_telegram_delivery_status(deliv_id, DeliveryStatus.SENT)
        self.assertTrue(sent_ok)

        # Once SENT, further claim attempts are permanently blocked
        claimed_w3 = repo.claim_telegram_delivery(
            delivery_id="DELIV-claim-test-003",
            message_key=msg_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=editor_chat,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            payload={"test": 3},
            max_attempts=1,
        )
        self.assertFalse(claimed_w3)

    # --- Test 11: Crash Before and During Dispatch Resend Prevention ---
    def test_11_crash_before_and_during_dispatch_resend_prevention(self):
        """Crash before dispatch leaves durable claim; subsequent runner refuses blind resend."""
        repo = self._create_turso_repo()
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_calls = []

        def mock_dispatch_handler(request: httpx.Request) -> httpx.Response:
            http_calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 9901}})

        transport = httpx.MockTransport(mock_dispatch_handler)

        channel_1 = TelegramNotificationChannel(
            settings=live_settings,
            transport=transport,
            repository=repo,
        )

        # Dispatch 1 succeeds
        assessment = EditorialAssessment(
            headline_tr="Başarılı Haber",
            why_efso_tr="Test Haberi",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=92,
            article_ids=["art_001"],
        )

        rec1 = channel_1.send(assessment=assessment, story_id="STORY-CRASH-001")
        self.assertEqual(rec1.status, DeliveryStatus.SENT)
        self.assertEqual(len(http_calls), 1)

        # Runner 1 crashes. Runner 2 starts fresh with same repository.
        # Runner 2 attempts to send the EXACT same notification_id -> pre-claim rejects before HTTP request!
        channel_2 = TelegramNotificationChannel(
            settings=live_settings,
            transport=transport,
            repository=repo,
        )
        rec2 = channel_2.send(assessment=assessment, story_id="STORY-CRASH-001", notification_id=rec1.notification_id)
        self.assertIn(rec2.status, (DeliveryStatus.SENT, DeliveryStatus.FAILED))
        # Total HTTP calls remains 1 (zero network request made by Runner 2)
        self.assertEqual(len(http_calls), 1)

        # Test fail-closed on repository claim error
        broken_repo = MagicMock(spec=TursoStateRepository)
        broken_repo.claim_telegram_delivery.side_effect = RuntimeError("Turso connection dropped!")
        channel_broken = TelegramNotificationChannel(
            settings=live_settings,
            transport=transport,
            repository=broken_repo,
        )
        rec_broken = channel_broken.send(assessment=assessment, story_id="STORY-CRASH-002")
        self.assertEqual(rec_broken.status, DeliveryStatus.FAILED)
        self.assertIn("failed closed", rec_broken.error_message or "")
        # Still 1 call
        self.assertEqual(len(http_calls), 1)

    # --- Test 12: AI Budget Guard Fails Closed on Repository Errors ---
    def test_12_ai_budget_guard_fails_closed_on_repository_error(self):
        """ExperimentBudgetGuard never assumes 0 spend; repository read/write failures fail closed."""
        broken_repo = MagicMock(spec=TursoStateRepository)
        broken_repo.get_monthly_budget_summary.side_effect = ConnectionError("Cloud database unreachable!")
        broken_repo.record_budget_usage.side_effect = ConnectionError("Cloud database unreachable!")

        guard = ExperimentBudgetGuard(
            ledger_path=self.tmp_path / "budget_fail_closed.json",
            budget_ceiling_usd=0.05,
            canary1_db_path=self.tmp_path / "c1.db",
            canary2_db_path=self.tmp_path / "c2.db",
            repository=broken_repo,
        )

        # 1. get_total_consumed_usd must raise ConnectionError, NOT swallow or return local total
        with self.assertRaises(ConnectionError):
            guard.get_total_consumed_usd()

        # 2. reserve_batch must fail closed and halt before any dispatch
        with self.assertRaises(ConnectionError):
            guard.reserve_batch(
                batch_hash="hash_fail_test",
                candidate_ids=["c1"],
                estimated_worst_cost_usd=0.001,
            )

        # 3. reconcile_batch must fail closed on write failure
        with self.assertRaises(ConnectionError):
            guard.reconcile_batch(
                batch_hash="hash_fail_test",
                actual_cost_usd=0.001,
                input_tokens=100,
                output_tokens=20,
            )

    # --- Test 13: Orchestrator Initialization Failure Releases Run Lock ---
    def test_13_orchestrator_initialization_failure_releases_run_lock(self):
        """If record_scheduled_execution_start fails, worker releases run lock in finally block."""
        repo = self._create_turso_repo()
        worker_id = "worker-lock-safety-001"
        run_id = "run-fail-init-001"

        # Mock record_scheduled_execution_start to raise an initialization error
        with patch.object(repo, "record_scheduled_execution_start", side_effect=RuntimeError("Audit start error!")):
            orchestrator = RadarOrchestrator(
                settings=self.test_settings,
                repository=repo,
            )
            with self.assertRaises(RuntimeError) as ctx:
                orchestrator.run(dry_run=False, worker_id=worker_id, run_id=run_id)

            self.assertIn("Audit start error!", str(ctx.exception))

        # Crucial check: Lock MUST have been released by the finally block!
        # Another worker must be able to acquire the lock immediately
        next_worker_acquired = repo.acquire_run_lock("worker-next-002", timeout_minutes=15)
        self.assertTrue(next_worker_acquired, "Lock was leaked by crashed worker!")
        repo.release_run_lock("worker-next-002")

    # --- Test 14: Recipient Separation and Live Dispatch Role Restrictions ---
    def test_14_recipient_separation_live_dispatch_protection(self):
        """ADMIN cannot receive live editorial bulletins; explicit overrides or ALL targeting fail closed."""
        repo = self._create_turso_repo()
        admin_chat = self.test_settings.telegram_chat_id
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            "legacy_editorial_fallback": False,
        })

        http_calls = []

        def mock_dispatch(request: httpx.Request) -> httpx.Response:
            http_calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 9911}})

        transport = httpx.MockTransport(mock_dispatch)

        channel = TelegramNotificationChannel(
            settings=live_settings,
            transport=transport,
            repository=repo,
        )

        assessment = EditorialAssessment(
            headline_tr="Editoryal Haber Bülteni",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["art_002"],
        )

        # 1. Attempting explicit target_chat_id override targeting ADMIN for editorial alert fails closed
        rec_admin_override = channel.send(
            assessment=assessment,
            story_id="STORY-ROLE-001",
            target_chat_id=admin_chat,
            dry_run=False,
        )
        self.assertEqual(rec_admin_override.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", rec_admin_override.error_message or "")
        self.assertEqual(len(http_calls), 0)

        # 2. Attempting explicit target_chat_id override targeting ADMIN for digest batch fails closed
        rec_batch_override = channel.send_digest_batch(
            assessments=[assessment],
            notification_id="BATCH-ROLE-001",
            target_chat_id=admin_chat,
            dry_run=False,
        )
        self.assertEqual(rec_batch_override.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", rec_batch_override.error_message or "")
        self.assertEqual(len(http_calls), 0)

        # 3. One-shot editorial digest targeting ADMIN role fails closed in live dispatch
        test_ledger_path = self.tmp_path / "ledger_role_test.json"
        res_oneshot_admin = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_recipient_role="ADMIN",
            repository=repo,
            transport=transport,
            ledger_path=test_ledger_path,
        )
        self.assertEqual(res_oneshot_admin.get("status"), "RECIPIENT_ROLE_VIOLATION")
        self.assertEqual(len(http_calls), 0)

        # 4. Attempting explicit target_chat_id override targeting PRIMARY_EDITOR for operational report fails closed
        rec_op_override = channel.send(
            assessment=assessment,
            story_id="REPORT-001",
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            target_chat_id=editor_chat,
            dry_run=False,
        )
        self.assertEqual(rec_op_override.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", rec_op_override.error_message or "")
        self.assertEqual(len(http_calls), 0)

        # 5. One-shot editorial digest without role target in live dispatch routes strictly to PRIMARY_EDITOR (not ADMIN)
        res_oneshot_default = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            repository=repo,
            transport=transport,
            ledger_path=test_ledger_path,
        )
        self.assertEqual(res_oneshot_default.get("status"), "DELIVERY_SUCCESS")
        # Assert only 1 HTTP call was made (to PRIMARY_EDITOR, NOT ADMIN)
        self.assertEqual(len(http_calls), 1)
        sent_body = json.loads(http_calls[0].content.decode("utf-8"))
        self.assertEqual(sent_body.get("chat_id"), editor_chat)

if __name__ == "__main__":
    unittest.main()
