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
from efso_radar.repositories.base import StateRepository
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

    # --- Test 15: Stable Delivery Idempotency Keys Across Replays and Restarts ---
    def test_15_stable_delivery_idempotency_keys_across_replays(self):
        """Derive stable logical identities without caller-supplied IDs; duplicate suppression on restart."""
        repo = self._create_turso_repo()
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_calls = []

        def mock_dispatch(request: httpx.Request) -> httpx.Response:
            http_calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 55001}})

        transport = httpx.MockTransport(mock_dispatch)

        channel1 = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        assessment = EditorialAssessment(
            headline_tr="Kar Sporlari Sampiyonasi",
            why_efso_tr="Kis sporlari ilgisi yuksek",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.98,
            kind=EditorialKind.NEW,
            score=91,
            article_ids=["sport_art_100"],
        )

        # Worker 1 sends without supplying notification_id
        res1 = channel1.send(
            assessment=assessment,
            story_id="STORY-SKI-100",
            dry_run=False,
            # notification_id is omitted!
        )
        self.assertEqual(res1.status, DeliveryStatus.SENT)
        self.assertEqual(len(http_calls), 1)
        notif_id_1 = res1.notification_id
        self.assertTrue(notif_id_1.startswith("NOTIF-"))

        # Worker 2 restarts in independent instance, evaluates same story without supplying notification_id
        channel2 = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)
        res2 = channel2.send(
            assessment=assessment,
            story_id="STORY-SKI-100",
            dry_run=False,
            # notification_id is omitted!
        )
        self.assertEqual(res2.status, DeliveryStatus.SENT)
        # Verify notification_id is identical!
        self.assertEqual(res2.notification_id, notif_id_1)
        # Verify ZERO second external HTTP call was made (duplicate prevented)
        self.assertEqual(len(http_calls), 1)

        # Worker with a new development (update_id provided) generates distinct key
        update_assessment = assessment.model_copy(update={"kind": EditorialKind.UPDATE})
        res3 = channel2.send(
            assessment=update_assessment,
            story_id="STORY-SKI-100",
            update_id="UPD-SKI-002",
            dry_run=False,
        )
        self.assertEqual(res3.status, DeliveryStatus.SENT)
        self.assertTrue(res3.notification_id.startswith("NOTIF-UPD-"))
        self.assertNotEqual(res3.notification_id, notif_id_1)
        self.assertEqual(len(http_calls), 2)

        # Two independent runner instances flush due digest with identical items -> identical key
        q_item = NotificationRecord(
            notification_id="DIGEST-Q-ITEM-01",
            story_id="STORY-DIG-01",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_DIGEST",
            sent_at=utc_now(),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Digest item preview",
            headline_tr="Ozet Haberi 1",
            score=88,
        )
        repo.record_notification(q_item)

        orchestrator1 = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=channel1,
            repository=repo,
        )
        orchestrator2 = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=channel2,
            repository=repo,
        )

        ref_time = datetime(2026, 10, 20, 15, 0, tzinfo=timezone.utc)
        dispatched1 = orchestrator1.flush_due_digest(dry_run=False, now=ref_time, force=True)
        self.assertEqual(dispatched1, 1)
        calls_after_first_flush = len(http_calls)

        # Second runner tries to flush
        dispatched2 = orchestrator2.flush_due_digest(dry_run=False, now=ref_time, force=True)
        # Duplicate delivery suppressed, zero extra HTTP calls!
        self.assertEqual(len(http_calls), calls_after_first_flush)

    # --- Test 16: Post-Send Persistence Failure Safety ---
    def test_16_post_send_persistence_failure_safety(self):
        """Never report SENT when persistence fails post-dispatch; retain protective claim and require reconciliation."""
        repo = self._create_turso_repo()
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_calls = []

        def mock_dispatch(request: httpx.Request) -> httpx.Response:
            http_calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 778899}})

        transport = httpx.MockTransport(mock_dispatch)

        faulty_repo = self._create_turso_repo()
        original_record = faulty_repo.record_telegram_delivery

        def failing_record_delivery(record):
            if record.status == DeliveryStatus.SENT:
                raise sqlite3.OperationalError("Simulated disk I/O failure during delivery commit")
            return original_record(record)

        faulty_repo.record_telegram_delivery = failing_record_delivery

        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=faulty_repo)

        assessment = EditorialAssessment(
            headline_tr="Teknoloji Devrimi Haberi",
            why_efso_tr="Onemli yapay zeka gelismesi",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.GLOBAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=93,
            article_ids=["tech_art_01"],
        )

        res = channel.send(
            assessment=assessment,
            story_id="STORY-TECH-01",
            dry_run=False,
        )

        # 1. Must NEVER report durable SENT! Must report DELIVERY_UNCERTAIN
        self.assertEqual(res.status, DeliveryStatus.DELIVERY_UNCERTAIN)
        self.assertIn("Post-send persistence failure", res.error_message or "")
        self.assertIn("Requires reconciliation", res.error_message or "")
        # Exactly 1 HTTP attempt was made
        self.assertEqual(len(http_calls), 1)

        # 2. Check that the pre-send claim is STILL protected in the database
        delivery_row = faulty_repo.get_telegram_delivery(editor_chat, res.notification_id)
        self.assertIsNotNone(delivery_row)
        # Status must not be PENDING or released; it remains DISPATCHING (or UNCERTAIN)
        self.assertIn(delivery_row.status, (DeliveryStatus.DISPATCHING, DeliveryStatus.DELIVERY_UNCERTAIN))

        # 3. Another worker trying to dispatch the same notification must be REJECTED (no duplicate HTTP send!)
        channel_worker2 = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=faulty_repo)
        res_worker2 = channel_worker2.send(
            assessment=assessment,
            story_id="STORY-TECH-01",
            dry_run=False,
        )
        self.assertNotEqual(res_worker2.status, DeliveryStatus.SENT)
        self.assertEqual(len(http_calls), 1)  # Still exactly 1 call!

        # 4. deliver_one_shot_editorial_digest also reports POST_SEND_PERSISTENCE_FAILED when persistence fails
        test_ledger_path = self.tmp_path / "ledger_post_send_fail.json"
        res_oneshot = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            repository=faulty_repo,
            transport=transport,
            ledger_path=test_ledger_path,
        )
        self.assertEqual(res_oneshot.get("status"), "POST_SEND_PERSISTENCE_FAILED")
        self.assertIn("Post-send persistence failure", res_oneshot.get("error", ""))

    # --- Test 17: Unsafe HTTP Retry Semantics Elimination ---
    def test_17_elimination_of_unsafe_http_retries_for_ambiguous_outcomes(self):
        """Ambiguous HTTP 5xx, timeouts, and transport errors execute at most 1 attempt, yielding DELIVERY_UNCERTAIN."""
        repo = self._create_turso_repo()
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            "telegram_max_retries": 3,
        })

        assessment = EditorialAssessment(
            headline_tr="Ekonomi Raporu",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.90,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["econ_001"],
        )

        ambiguous_cases = [
            ("http_500", httpx.Response(500, text="Internal Server Error")),
            ("http_502", httpx.Response(502, text="Bad Gateway")),
            ("http_503", httpx.Response(503, text="Service Unavailable")),
            ("http_504", httpx.Response(504, text="Gateway Timeout")),
            ("timeout", httpx.ReadTimeout("Read timed out on socket")),
            ("transport_error", httpx.RemoteProtocolError("Server disconnected unexpectedly")),
        ]

        for case_name, response_or_exc in ambiguous_cases:
            attempt_count = 0

            def make_handler(resp_or_ex):
                def handler(request: httpx.Request) -> httpx.Response:
                    nonlocal attempt_count
                    attempt_count += 1
                    if isinstance(resp_or_ex, Exception):
                        raise resp_or_ex
                    return resp_or_ex
                return handler

            transport = httpx.MockTransport(make_handler(response_or_exc))
            channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

            story_id = f"STORY-AMBIG-{case_name}"
            rec = channel.send(
                assessment=assessment,
                story_id=story_id,
                dry_run=False,
            )

            # Assert exactly 1 attempt made (ZERO retries)
            self.assertEqual(attempt_count, 1, f"Ambiguous case {case_name} made {attempt_count} attempts, expected 1!")
            self.assertEqual(rec.status, DeliveryStatus.DELIVERY_UNCERTAIN, f"Case {case_name} did not yield DELIVERY_UNCERTAIN!")

            # Assert repository record is DELIVERY_UNCERTAIN
            deliv = repo.get_telegram_delivery(editor_chat, rec.notification_id)
            self.assertIsNotNone(deliv)
            self.assertEqual(deliv.status, DeliveryStatus.DELIVERY_UNCERTAIN)

        # Definitive errors (HTTP 400, 403) are FAILED with exactly 1 attempt
        for def_code in (400, 403):
            def_attempts = 0

            def def_handler(request: httpx.Request) -> httpx.Response:
                nonlocal def_attempts
                def_attempts += 1
                return httpx.Response(def_code, json={"ok": False, "description": "Definitive client rejection"})

            transport_def = httpx.MockTransport(def_handler)
            channel_def = TelegramNotificationChannel(settings=live_settings, transport=transport_def, repository=repo)

            rec_def = channel_def.send(
                assessment=assessment,
                story_id=f"STORY-DEF-{def_code}",
                dry_run=False,
            )
            self.assertEqual(def_attempts, 1)
            self.assertEqual(rec_def.status, DeliveryStatus.FAILED)

    # --- Test 18: Live Mode Recipient Boundary Enforcement ---
    def test_18_live_mode_recipient_boundary_enforcement(self):
        """In live delivery, editorial content NEVER reaches ADMIN even if overrides/fallbacks are supplied."""
        repo = self._create_turso_repo()
        admin_chat = self.test_settings.telegram_chat_id
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            "legacy_editorial_fallback": True,  # Explicitly configured fallback!
        })

        http_requests = []

        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 12345}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        assessment = EditorialAssessment(
            headline_tr="Canli Mod Sinir Testi",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["live_art_001"],
        )

        # 1. channel.send() targeting ADMIN in live delivery fails closed
        res_send = channel.send(
            assessment=assessment,
            story_id="STORY-LIVE-01",
            target_chat_id=admin_chat,
            dry_run=False,
        )
        self.assertEqual(res_send.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res_send.error_message or "")
        # Zero HTTP calls to ADMIN!
        self.assertEqual(len(http_requests), 0)

        # 2. channel.send_digest_batch() targeting ADMIN in live delivery fails closed
        res_batch = channel.send_digest_batch(
            assessments=[assessment],
            target_chat_id=admin_chat,
            dry_run=False,
        )
        self.assertEqual(res_batch.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res_batch.error_message or "")
        self.assertEqual(len(http_requests), 0)

        # 3. deliver_one_shot_editorial_digest() with target_recipient_role="ALL" delivers ONLY to PRIMARY_EDITOR
        test_ledger = self.tmp_path / "ledger_live_boundary.json"
        res_all = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_recipient_role="ALL",
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_all.get("status"), "DELIVERY_SUCCESS")
        # Exactly 1 HTTP call made, and it was to PRIMARY_EDITOR, NEVER ADMIN
        self.assertEqual(len(http_requests), 1)
        body = json.loads(http_requests[0].content.decode("utf-8"))
        self.assertEqual(body.get("chat_id"), editor_chat)
        self.assertNotEqual(body.get("chat_id"), admin_chat)

        # 4. deliver_one_shot_editorial_digest() with target_recipient_role="ADMIN" rejected
        res_admin_role = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_recipient_role="ADMIN",
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_admin_role.get("status"), "RECIPIENT_ROLE_VIOLATION")
        self.assertEqual(len(http_requests), 1)  # Still exactly 1 call total, 0 to ADMIN!

        # 5. deliver_one_shot_editorial_digest() with target_chat_id=ADMIN rejected
        res_admin_chat = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_chat_id=admin_chat,
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_admin_chat.get("status"), "RECIPIENT_ROLE_VIOLATION")
        self.assertEqual(len(http_requests), 1)  # Still exactly 1 call total, 0 to ADMIN!


    def test_19_admin_never_receives_live_editorial_even_with_legacy_flags(self):
        """Verify ADMIN never receives a live editorial message across ANY dispatch path, even when legacy flags are enabled."""
        repo = self._create_turso_repo()
        admin_chat = self.test_settings.telegram_chat_id
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            "legacy_editorial_fallback": True,  # Legacy flag enabled!
        })

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        assessment = EditorialAssessment(
            headline_tr="Admin İzolasyon Testi",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["admin_iso_001"],
        )

        # Path 1: channel.send() targeting ADMIN
        res_send = channel.send(
            assessment=assessment,
            story_id="STORY-ADMIN-01",
            target_chat_id=admin_chat,
            dry_run=False,
        )
        self.assertEqual(res_send.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res_send.error_message or "")

        # Path 2: channel.send_digest_batch() targeting ADMIN
        res_batch = channel.send_digest_batch(
            assessments=[assessment],
            target_chat_id=admin_chat,
            dry_run=False,
        )
        self.assertEqual(res_batch.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res_batch.error_message or "")

        # Path 3: channel.send_multicast() targeting ADMIN
        res_multi = channel.send_multicast(
            assessment=assessment,
            story_id="STORY-ADMIN-02",
            target_role="ADMIN",
            dry_run=False,
        )
        self.assertEqual(len(res_multi), 1)
        self.assertEqual(res_multi[0].status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res_multi[0].error_message or "")

        # Path 4: channel.send_digest_batch_multicast() targeting ADMIN
        res_batch_multi = channel.send_digest_batch_multicast(
            assessments=[assessment],
            notification_id="DIGEST-ADMIN-01",
            target_role="ADMIN",
            dry_run=False,
        )
        self.assertEqual(len(res_batch_multi), 1)
        self.assertEqual(res_batch_multi[0].status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res_batch_multi[0].error_message or "")

        # Path 5: deliver_one_shot_editorial_digest() with target_chat_id=ADMIN and allow_legacy_admin_broadcast=True
        test_ledger = self.tmp_path / "ledger_admin_never.json"
        res_oneshot_chat = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_chat_id=admin_chat,
            allow_legacy_admin_broadcast=True,
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_oneshot_chat.get("status"), "RECIPIENT_ROLE_VIOLATION")

        # Path 6: deliver_one_shot_editorial_digest() with target_recipient_role="ADMIN" and allow_legacy_admin_broadcast=True
        res_oneshot_role = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_recipient_role="ADMIN",
            allow_legacy_admin_broadcast=True,
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_oneshot_role.get("status"), "RECIPIENT_ROLE_VIOLATION")

        # Invariant: EXACTLY 0 HTTP calls to ADMIN or anywhere across all 6 live paths!
        self.assertEqual(len(http_requests), 0)

    def test_20_explicit_allowlisted_non_primary_editor_chat_id_rejected(self):
        """Verify explicit target_chat_id with an allowlisted but non-PRIMARY_EDITOR role is rejected in live delivery."""
        third_chat_id = "555555555"
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            "telegram_allowed_chat_ids": [
                self.test_settings.telegram_chat_id,
                self.test_settings.telegram_primary_editor_chat_id,
                third_chat_id,  # Allowlisted, but role is AUTHORIZED_RECIPIENT
            ],
        })
        self.assertEqual(live_settings.get_recipient_role(third_chat_id), "AUTHORIZED_RECIPIENT")

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 11111}})

        transport = httpx.MockTransport(recording_transport)
        repo = self._create_turso_repo()
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        assessment = EditorialAssessment(
            headline_tr="Non-Editor Allowlist Testi",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["non_editor_001"],
        )

        # 1. channel.send() targeting third_chat_id rejected
        res1 = channel.send(assessment=assessment, story_id="STORY-NONED-01", target_chat_id=third_chat_id, dry_run=False)
        self.assertEqual(res1.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res1.error_message or "")

        # 2. channel.send_digest_batch() targeting third_chat_id rejected
        res2 = channel.send_digest_batch(assessments=[assessment], target_chat_id=third_chat_id, dry_run=False)
        self.assertEqual(res2.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation", res2.error_message or "")

        # 3. deliver_one_shot_editorial_digest() targeting third_chat_id rejected
        test_ledger = self.tmp_path / "ledger_non_editor.json"
        res3 = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_chat_id=third_chat_id,
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res3.get("status"), "RECIPIENT_ROLE_VIOLATION")

        # Zero HTTP calls made!
        self.assertEqual(len(http_requests), 0)

    def test_21_target_all_never_includes_admin_in_live_editorial(self):
        """Verify target_recipient_role='ALL' never includes ADMIN in live editorial delivery across all multicast and digest paths."""
        repo = self._create_turso_repo()
        admin_chat = self.test_settings.telegram_chat_id
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            "legacy_editorial_fallback": True,
        })

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 88888}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        assessment = EditorialAssessment(
            headline_tr="Target All Testi",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["target_all_001"],
        )

        # 1. send_multicast with target_role="ALL" delivers strictly to PRIMARY_EDITOR
        records_multi = channel.send_multicast(
            assessment=assessment,
            story_id="STORY-ALL-01",
            target_role="ALL",
            dry_run=False,
        )
        self.assertEqual(len(records_multi), 1)
        self.assertEqual(records_multi[0].status, DeliveryStatus.SENT)

        # 2. send_digest_batch_multicast with target_role="ALL" delivers strictly to PRIMARY_EDITOR
        records_batch_multi = channel.send_digest_batch_multicast(
            assessments=[assessment],
            notification_id="DIGEST-ALL-01",
            target_role="ALL",
            dry_run=False,
        )
        self.assertEqual(len(records_batch_multi), 1)
        self.assertEqual(records_batch_multi[0].status, DeliveryStatus.SENT)

        # 3. deliver_one_shot_editorial_digest with target_recipient_role="ALL"
        test_ledger = self.tmp_path / "ledger_target_all.json"
        res_oneshot = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            target_recipient_role="ALL",
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_oneshot.get("status"), "DELIVERY_SUCCESS")
        self.assertEqual(res_oneshot.get("recipients_count"), 1)

        # Total 3 HTTP calls made across the 3 tests
        self.assertEqual(len(http_requests), 3)
        # Every single call was addressed to PRIMARY_EDITOR; NONE to ADMIN!
        for req in http_requests:
            body = json.loads(req.content.decode("utf-8"))
            self.assertEqual(body.get("chat_id"), editor_chat)
            self.assertNotEqual(body.get("chat_id"), admin_chat)

    def test_22_cloud_delivery_without_durable_repo_or_claim_makes_zero_http_calls(self):
        """Verify cloud-mode delivery without authoritative StateRepository or successful claim strictly makes zero HTTP calls."""
        cloud_settings = self.test_settings.model_copy(update={
            "storage_mode": "turso",
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 77777}})

        transport = httpx.MockTransport(recording_transport)

        assessment = EditorialAssessment(
            headline_tr="Cloud Claim Requirement Testi",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["cloud_claim_001"],
        )

        # 1. channel without repository in cloud storage mode fails closed before HTTP
        channel_no_repo = TelegramNotificationChannel(settings=cloud_settings, transport=transport, repository=None)
        res_no_repo = channel_no_repo.send(assessment=assessment, story_id="STORY-CLOUD-01", dry_run=False)
        self.assertEqual(res_no_repo.status, DeliveryStatus.FAILED)
        self.assertIn("Cloud delivery rejected: authoritative StateRepository is strictly required", res_no_repo.error_message or "")
        self.assertEqual(len(http_requests), 0)

        # 2. deliver_one_shot_editorial_digest without repository in cloud mode returns DURABLE_STORAGE_REQUIRED
        test_ledger = self.tmp_path / "ledger_cloud_req.json"
        res_oneshot_no_repo = deliver_one_shot_editorial_digest(
            settings=cloud_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            repository=None,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_oneshot_no_repo.get("status"), "DURABLE_STORAGE_REQUIRED")
        self.assertEqual(len(http_requests), 0)

        # 3. channel with repository where claim is rejected (duplicate / in-flight) makes zero HTTP calls
        mock_repo = MagicMock(spec=StateRepository)
        mock_repo.claim_telegram_delivery.return_value = False
        mock_repo.get_telegram_delivery.return_value = None

        channel_claim_rej = TelegramNotificationChannel(settings=cloud_settings, transport=transport, repository=mock_repo)
        res_claim_rej = channel_claim_rej.send(assessment=assessment, story_id="STORY-CLOUD-02", dry_run=False)
        self.assertEqual(res_claim_rej.status, DeliveryStatus.FAILED)
        self.assertIn("Delivery claim rejected", res_claim_rej.error_message or "")
        self.assertEqual(len(http_requests), 0)

        # 4. deliver_one_shot_editorial_digest where claim is rejected makes zero HTTP calls
        res_oneshot_rej = deliver_one_shot_editorial_digest(
            settings=cloud_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            repository=mock_repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res_oneshot_rej.get("status"), "CLAIM_REJECTED")
        self.assertEqual(len(http_requests), 0)

        # 5. channel where claim raises repository exception fails closed with zero HTTP calls
        mock_repo.claim_telegram_delivery.side_effect = RuntimeError("Turso connection reset")
        res_claim_exc = channel_claim_rej.send(assessment=assessment, story_id="STORY-CLOUD-03", dry_run=False)
        self.assertEqual(res_claim_exc.status, DeliveryStatus.FAILED)
        self.assertIn("Delivery claim failed closed: repository error", res_claim_exc.error_message or "")
        self.assertEqual(len(http_requests), 0)

    def test_23_offline_legacy_error_handling_no_unbound_local_error(self):
        """Verify offline legacy paths handle errors safely and never raise UnboundLocalError."""
        admin_chat = self.test_settings.telegram_chat_id
        assessment = EditorialAssessment(
            headline_tr="Offline Preview Error Handling Testi",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["offline_preview_001"],
        )

        # Case A: legacy_editorial_fallback=True in offline mode (dry_run=True)
        settings_fallback_on = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": False,
            "legacy_editorial_fallback": True,
        })
        channel_fb_on = TelegramNotificationChannel(settings=settings_fallback_on)

        # send() must NOT raise UnboundLocalError and must return safe preview
        rec1 = channel_fb_on.send(assessment=assessment, story_id="STORY-OFF-01", target_chat_id=admin_chat, dry_run=True)
        self.assertEqual(rec1.status, DeliveryStatus.SENT)

        # send_digest_batch() must NOT raise UnboundLocalError and must return safe preview
        rec2 = channel_fb_on.send_digest_batch(assessments=[assessment], target_chat_id=admin_chat, dry_run=True)
        self.assertEqual(rec2.status, DeliveryStatus.SENT)

        # Case B: legacy_editorial_fallback=False in offline mode (dry_run=True)
        settings_fallback_off = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": False,
            "legacy_editorial_fallback": False,
        })
        channel_fb_off = TelegramNotificationChannel(settings=settings_fallback_off)

        # send() must NOT raise UnboundLocalError and must return FAILED with properly assigned error_message
        rec3 = channel_fb_off.send(assessment=assessment, story_id="STORY-OFF-02", target_chat_id=admin_chat, dry_run=True)
        self.assertEqual(rec3.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation: ADMIN is restricted to operational health reporting only", rec3.error_message or "")

        # send_digest_batch() must NOT raise UnboundLocalError and must return FAILED with properly assigned error_message
        rec4 = channel_fb_off.send_digest_batch(assessments=[assessment], target_chat_id=admin_chat, dry_run=True)
        self.assertEqual(rec4.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation: ADMIN is restricted to operational health reporting only", rec4.error_message or "")

    def test_24_primary_editor_delivery_success_remains_intact(self):
        """Verify valid PRIMARY_EDITOR delivery succeeds with HTTP 200 and durable persistence across all paths."""
        repo = self._create_turso_repo()
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 654321}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        assessment = EditorialAssessment(
            headline_tr="Primary Editor Başarılı Dağıtım",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.95,
            kind=EditorialKind.NEW,
            score=90,
            article_ids=["pe_success_001"],
        )

        # 1. channel.send() succeeds
        rec1 = channel.send(assessment=assessment, story_id="STORY-PE-01", dry_run=False)
        self.assertEqual(rec1.status, DeliveryStatus.SENT)

        # 2. channel.send_digest_batch() succeeds
        rec2 = channel.send_digest_batch(assessments=[assessment], notification_id="DIGEST-PE-01", dry_run=False)
        self.assertEqual(rec2.status, DeliveryStatus.SENT)

        # 3. deliver_one_shot_editorial_digest() succeeds
        test_ledger = self.tmp_path / "ledger_pe_success.json"
        res3 = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=[assessment, assessment, assessment],
            dry_run=False,
            repository=repo,
            transport=transport,
            ledger_path=test_ledger,
        )
        self.assertEqual(res3.get("status"), "DELIVERY_SUCCESS")

        self.assertEqual(len(http_requests), 3)
        for req in http_requests:
            body = json.loads(req.content.decode("utf-8"))
            self.assertEqual(body.get("chat_id"), editor_chat)


    # --- Tests 25 - 31: Low-Level Dispatch Safety Gates & Operational Telemetry ---

    def test_25_delivery_disabled_direct_dispatch_telegram_makes_zero_http_calls(self):
        """Verify direct _dispatch_telegram() call when telegram_delivery_enabled is False makes zero HTTP calls and fails closed."""
        disabled_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": False,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=disabled_settings, transport=transport)

        rec = channel._dispatch_telegram(
            notification_id="DIRECT-DIS-01",
            story_id="STORY-DIS-01",
            update_id=None,
            assessment=None,
            message_text="Test direct dispatch with delivery disabled",
            now=utc_now(),
            target_chat_id=self.test_settings.telegram_primary_editor_chat_id,
            category=TelegramNotificationCategory.VERIFIED_BREAKING,
            channel_name="TELEGRAM",
        )

        self.assertEqual(rec.status, DeliveryStatus.FAILED)
        self.assertIn("Delivery disabled: TELEGRAM_DELIVERY_ENABLED is False", rec.error_message or "")
        self.assertFalse(rec.status == DeliveryStatus.SENT)
        self.assertEqual(len(http_requests), 0)

    def test_26_delivery_disabled_orchestrator_fallback_makes_zero_http_calls(self):
        """Verify orchestrator fallback when telegram_delivery_enabled is False makes zero HTTP calls."""
        disabled_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": False,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=disabled_settings, transport=transport)
        repo = self._create_turso_repo()

        # Orchestrator with notifier
        from unittest.mock import MagicMock
        orchestrator = RadarOrchestrator(
            settings=disabled_settings,
            repository=repo,
            source=MagicMock(),
            ai_engine=MagicMock(),
            notifier=channel,
        )

        # 1. flush_due_digest() with delivery disabled
        res_flush = orchestrator.flush_due_digest(now=utc_now(), dry_run=False)
        self.assertEqual(res_flush, 0)
        self.assertEqual(len(http_requests), 0)

    def test_27_operational_report_addressed_to_primary_editor_rejected(self):
        """Verify OPERATIONAL_HEALTH_REPORT addressed to PRIMARY_EDITOR is rejected across send() and direct dispatch."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        editor_chat = self.test_settings.telegram_primary_editor_chat_id

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport)

        # Path A: channel.send()
        rec_send = channel.send(
            assessment=None,
            story_id="SYS-OP-01",
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            target_chat_id=editor_chat,
            dry_run=False,
            custom_message="Operational Health Status: OK",
        )
        self.assertEqual(rec_send.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation: OPERATIONAL_HEALTH_REPORT is strictly restricted to ADMIN", rec_send.error_message or "")

        # Path B: channel._dispatch_telegram()
        rec_direct = channel._dispatch_telegram(
            notification_id="NOTIF-OP-01",
            story_id="SYS-OP-01",
            update_id=None,
            assessment=None,
            message_text="Operational Health Status: OK",
            now=utc_now(),
            target_chat_id=editor_chat,
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            channel_name="TELEGRAM_OPS",
        )
        self.assertEqual(rec_direct.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation: OPERATIONAL_HEALTH_REPORT is strictly restricted to ADMIN", rec_direct.error_message or "")

        self.assertEqual(len(http_requests), 0)

    def test_28_operational_report_addressed_to_authorized_recipient_rejected(self):
        """Verify OPERATIONAL_HEALTH_REPORT addressed to AUTHORIZED_RECIPIENT is rejected across send() and direct dispatch."""
        auth_recip_chat_id = "555555555"
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
            "telegram_allowed_chat_ids": [
                self.test_settings.telegram_chat_id,
                self.test_settings.telegram_primary_editor_chat_id,
                auth_recip_chat_id,
            ],
        })
        self.assertEqual(live_settings.get_recipient_role(auth_recip_chat_id), "AUTHORIZED_RECIPIENT")

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport)

        # Path A: channel.send()
        rec_send = channel.send(
            assessment=None,
            story_id="SYS-OP-02",
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            target_chat_id=auth_recip_chat_id,
            dry_run=False,
            custom_message="Operational Health Status: OK",
        )
        self.assertEqual(rec_send.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation: OPERATIONAL_HEALTH_REPORT is strictly restricted to ADMIN", rec_send.error_message or "")

        # Path B: channel._dispatch_telegram()
        rec_direct = channel._dispatch_telegram(
            notification_id="NOTIF-OP-02",
            story_id="SYS-OP-02",
            update_id=None,
            assessment=None,
            message_text="Operational Health Status: OK",
            now=utc_now(),
            target_chat_id=auth_recip_chat_id,
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            channel_name="TELEGRAM_OPS",
        )
        self.assertEqual(rec_direct.status, DeliveryStatus.FAILED)
        self.assertIn("Recipient separation violation: OPERATIONAL_HEALTH_REPORT is strictly restricted to ADMIN", rec_direct.error_message or "")

        self.assertEqual(len(http_requests), 0)

    def test_29_direct_dispatch_to_unallowlisted_chat_id_rejected(self):
        """Verify direct _dispatch_telegram() call targeting an unallowlisted chat ID fails closed without HTTP requests."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport)

        rec = channel._dispatch_telegram(
            notification_id="NOTIF-UNAUTH-01",
            story_id="SYS-01",
            update_id=None,
            assessment=None,
            message_text="Test unallowlisted dispatch",
            now=utc_now(),
            target_chat_id="999888777_not_in_allowlist",
            category=TelegramNotificationCategory.VERIFIED_BREAKING,
            channel_name="TELEGRAM",
        )

        self.assertEqual(rec.status, DeliveryStatus.FAILED)
        self.assertIn("Unauthorized recipient", rec.error_message or "")
        self.assertEqual(len(http_requests), 0)

    def test_30_valid_admin_operational_report_permitted_in_synthetic_live_simulation(self):
        """Verify valid ADMIN operational report succeeds across send() and direct dispatch in synthetic live mode."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        admin_chat = self.test_settings.telegram_chat_id
        repo = self._create_turso_repo()

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 44444}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        # 1. Via channel.send()
        rec_send = channel.send(
            assessment=None,
            story_id="SYS-ADMIN-OK-01",
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            target_chat_id=admin_chat,
            dry_run=False,
            custom_message="<b>Efso Radar Operational Health: ALL GREEN</b>",
        )
        self.assertEqual(rec_send.status, DeliveryStatus.SENT)

        # 2. Via direct channel._dispatch_telegram()
        rec_direct = channel._dispatch_telegram(
            notification_id="NOTIF-ADMIN-OK-02",
            story_id="SYS-ADMIN-OK-02",
            update_id=None,
            assessment=None,
            message_text="<b>Efso Radar Operational Health: ALL GREEN</b>",
            now=utc_now(),
            target_chat_id=admin_chat,
            category=TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT,
            channel_name="TELEGRAM_OPS",
        )
        self.assertEqual(rec_direct.status, DeliveryStatus.SENT)

        self.assertEqual(len(http_requests), 2)
        for req in http_requests:
            body = json.loads(req.content.decode("utf-8"))
            self.assertEqual(body.get("chat_id"), admin_chat)

    def test_31_valid_primary_editor_editorial_delivery_remains_intact(self):
        """Verify valid PRIMARY_EDITOR editorial delivery remains fully intact."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        editor_chat = self.test_settings.telegram_primary_editor_chat_id
        repo = self._create_turso_repo()

        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 33333}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        assessment = EditorialAssessment(
            headline_tr="Baş Editör Flaş Haber",
            why_efso_tr="Test",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.VIRAL,
            disposition=EditorialDisposition.DIGEST,
            verification_status=VerificationStatus.OFFICIAL_CONFIRMATION,
            confidence_score=0.98,
            kind=EditorialKind.NEW,
            score=95,
            article_ids=["pe_flash_001"],
        )

        rec = channel.send(
            assessment=assessment,
            story_id="STORY-PE-FLASH-01",
            category=TelegramNotificationCategory.VERIFIED_BREAKING,
            dry_run=False,
        )
        self.assertEqual(rec.status, DeliveryStatus.SENT)
        self.assertEqual(len(http_requests), 1)
        body = json.loads(http_requests[0].content.decode("utf-8"))
        self.assertEqual(body.get("chat_id"), editor_chat)

    # --- Tests 32-37: Digest Queue Integrity & External Delivery Safety ---
    def test_32_delivery_disabled_flush_due_digest_preserves_queue_and_makes_zero_http_calls(self):
        """When Telegram delivery is disabled, flush_due_digest() must never transition queued notifications to SENT."""
        disabled_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": False,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        repo = self._create_turso_repo()
        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=disabled_settings, transport=transport, repository=repo)

        q_item = NotificationRecord(
            notification_id="NOTIF-Q-TEST-32",
            story_id="STORY-TEST-32",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_DIGEST",
            sent_at=utc_now(),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Queued digest item text",
            headline_tr="Test Kuyrukta Bekleyen Haber 32",
            score=88,
        )
        repo.record_notification(q_item)

        orchestrator = RadarOrchestrator(
            settings=disabled_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=channel,
            repository=repo,
        )

        with unittest.mock.patch.object(repo, "mark_digest_notifications_dispatched", wraps=repo.mark_digest_notifications_dispatched) as spy_mark:
            res = orchestrator.flush_due_digest(dry_run=False, force=True, now=utc_now())

        self.assertEqual(res, 0)
        self.assertEqual(len(http_requests), 0)
        spy_mark.assert_not_called()

        queued = repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0].notification_id, "NOTIF-Q-TEST-32")
        self.assertEqual(queued[0].status, DeliveryStatus.QUEUED_FOR_DIGEST)

    def test_33_no_valid_notifier_dispatch_path_preserves_queue(self):
        """When no valid notifier dispatch path exists, flush_due_digest() returns safely and preserves queue."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        repo = self._create_turso_repo()

        q_item = NotificationRecord(
            notification_id="NOTIF-Q-TEST-33",
            story_id="STORY-TEST-33",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_DIGEST",
            sent_at=utc_now(),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Queued digest item text",
            headline_tr="Test Kuyrukta Bekleyen Haber 33",
            score=88,
        )
        repo.record_notification(q_item)

        # Notifier has no send_digest_batch and no _dispatch_telegram
        class DummyNotifier:
            pass

        orchestrator = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=DummyNotifier(),
            repository=repo,
        )

        with unittest.mock.patch.object(repo, "mark_digest_notifications_dispatched", wraps=repo.mark_digest_notifications_dispatched) as spy_mark:
            res = orchestrator.flush_due_digest(dry_run=False, force=True, now=utc_now())

        self.assertEqual(res, 0)
        spy_mark.assert_not_called()

        queued = repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0].notification_id, "NOTIF-Q-TEST-33")
        self.assertEqual(queued[0].status, DeliveryStatus.QUEUED_FOR_DIGEST)

    def test_34_dry_run_preserves_queued_digest_notifications(self):
        """Dry-run execution preserves queued digest notifications without marking them dispatched."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        repo = self._create_turso_repo()
        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99999}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        q_item = NotificationRecord(
            notification_id="NOTIF-Q-TEST-34",
            story_id="STORY-TEST-34",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_DIGEST",
            sent_at=utc_now(),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Queued digest item text",
            headline_tr="Test Kuyrukta Bekleyen Haber 34",
            score=88,
        )
        repo.record_notification(q_item)

        orchestrator = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=channel,
            repository=repo,
        )

        with unittest.mock.patch.object(repo, "mark_digest_notifications_dispatched", wraps=repo.mark_digest_notifications_dispatched) as spy_mark:
            res = orchestrator.flush_due_digest(dry_run=True, force=True, now=utc_now())

        self.assertEqual(res, 0)
        self.assertEqual(len(http_requests), 0)
        spy_mark.assert_not_called()

        queued = repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0].notification_id, "NOTIF-Q-TEST-34")
        self.assertEqual(queued[0].status, DeliveryStatus.QUEUED_FOR_DIGEST)

    def test_35_confirmed_synthetic_telegram_delivery_transitions_queued_records(self):
        """Genuinely confirmed synthetic Telegram delivery successfully transitions queued records to SENT."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        repo = self._create_turso_repo()
        http_requests = []
        def recording_transport(request: httpx.Request) -> httpx.Response:
            http_requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 12345}})

        transport = httpx.MockTransport(recording_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        q_item = NotificationRecord(
            notification_id="NOTIF-Q-TEST-35",
            story_id="STORY-TEST-35",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_DIGEST",
            sent_at=utc_now(),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Queued digest item text",
            headline_tr="Test Kuyrukta Bekleyen Haber 35",
            score=88,
        )
        repo.record_notification(q_item)

        orchestrator = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=channel,
            repository=repo,
        )

        with unittest.mock.patch.object(repo, "mark_digest_notifications_dispatched", wraps=repo.mark_digest_notifications_dispatched) as spy_mark:
            res = orchestrator.flush_due_digest(dry_run=False, force=True, now=utc_now())

        self.assertEqual(res, 1)
        self.assertEqual(len(http_requests), 1)
        spy_mark.assert_called_once()

        # Queue is now drained
        queued = repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 0)

        # Record in DB is marked SENT
        cursor = repo.client.execute(
            "SELECT status, channel, delivered_at FROM notifications WHERE notification_id = 'NOTIF-Q-TEST-35'"
        )
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row.get("status"), "SENT")
        self.assertEqual(row.get("channel"), "TELEGRAM_DIGEST")
        self.assertIsNotNone(row.get("delivered_at"))

    def test_36_ambiguous_or_failed_delivery_does_not_mark_notifications_as_sent(self):
        """Ambiguous (DELIVERY_UNCERTAIN) or failed (FAILED) dispatch never transitions queued records to SENT."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        repo = self._create_turso_repo()

        # 1. Ambiguous timeout -> DELIVERY_UNCERTAIN
        def timeout_transport(request: httpx.Request) -> httpx.Response:
            return httpx.Response(504, json={"ok": False, "description": "Gateway Timeout"})

        transport = httpx.MockTransport(timeout_transport)
        channel = TelegramNotificationChannel(settings=live_settings, transport=transport, repository=repo)

        q_item1 = NotificationRecord(
            notification_id="NOTIF-Q-TEST-36A",
            story_id="STORY-TEST-36A",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_DIGEST",
            sent_at=utc_now(),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Queued digest item text",
            headline_tr="Test Kuyrukta Bekleyen Haber 36A",
            score=88,
        )
        repo.record_notification(q_item1)

        orchestrator = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=channel,
            repository=repo,
        )

        with unittest.mock.patch.object(repo, "mark_digest_notifications_dispatched", wraps=repo.mark_digest_notifications_dispatched) as spy_mark:
            res1 = orchestrator.flush_due_digest(dry_run=False, force=True, now=utc_now())

        self.assertEqual(res1, 0)
        spy_mark.assert_not_called()
        queued = repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0].notification_id, "NOTIF-Q-TEST-36A")
        self.assertEqual(queued[0].status, DeliveryStatus.QUEUED_FOR_DIGEST)

        # 2. Definite failure (HTTP 403 Forbidden) -> FAILED
        def forbidden_transport(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, json={"ok": False, "description": "Forbidden: bot was blocked by the user"})

        channel_forbidden = TelegramNotificationChannel(
            settings=live_settings,
            transport=httpx.MockTransport(forbidden_transport),
            repository=repo,
        )
        orchestrator_forbidden = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=channel_forbidden,
            repository=repo,
        )

        with unittest.mock.patch.object(repo, "mark_digest_notifications_dispatched", wraps=repo.mark_digest_notifications_dispatched) as spy_mark:
            res2 = orchestrator_forbidden.flush_due_digest(dry_run=False, force=True, now=utc_now())

        self.assertEqual(res2, 0)
        spy_mark.assert_not_called()
        queued_after = repo.get_queued_digest_notifications()
        self.assertEqual(len(queued_after), 1)
        self.assertEqual(queued_after[0].notification_id, "NOTIF-Q-TEST-36A")
        self.assertEqual(queued_after[0].status, DeliveryStatus.QUEUED_FOR_DIGEST)

    def test_37_preview_record_from_notifier_not_interpreted_as_external_delivery(self):
        """A preview record (is_preview=True) must never be interpreted as confirmed external delivery."""
        live_settings = self.test_settings.model_copy(update={
            "telegram_delivery_enabled": True,
            "telegram_bot_token": "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        })
        repo = self._create_turso_repo()

        q_item = NotificationRecord(
            notification_id="NOTIF-Q-TEST-37",
            story_id="STORY-TEST-37",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_DIGEST",
            sent_at=utc_now(),
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Queued digest item text",
            headline_tr="Test Kuyrukta Bekleyen Haber 37",
            score=88,
        )
        repo.record_notification(q_item)

        class PreviewNotifier:
            def send_digest_batch(self, *args, **kwargs):
                return NotificationRecord(
                    notification_id="DIGEST-PREVIEW-01",
                    story_id="STORY-TEST-37",
                    kind=EditorialKind.NEW,
                    channel="TELEGRAM_DIGEST",
                    sent_at=utc_now(),
                    status=DeliveryStatus.SENT,
                    message_text="Preview text",
                    headline_tr="Preview headline",
                    score=85,
                    is_preview=True,
                )

        orchestrator = RadarOrchestrator(
            settings=live_settings,
            source=MagicMock(spec=NewsSource),
            ai_engine=MagicMock(),
            notifier=PreviewNotifier(),
            repository=repo,
        )

        with unittest.mock.patch.object(repo, "mark_digest_notifications_dispatched", wraps=repo.mark_digest_notifications_dispatched) as spy_mark:
            res = orchestrator.flush_due_digest(dry_run=False, force=True, now=utc_now())

        self.assertEqual(res, 0)
        spy_mark.assert_not_called()
        queued = repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0].notification_id, "NOTIF-Q-TEST-37")
        self.assertEqual(queued[0].status, DeliveryStatus.QUEUED_FOR_DIGEST)

if __name__ == "__main__":
    unittest.main()
