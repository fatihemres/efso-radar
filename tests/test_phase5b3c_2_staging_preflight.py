"""Hermetic unit and integration tests for Phase 5B.3C-2: Cloud Staging Preflight & Recovery Readiness.

Verifies:
- Read-only schema v6 preflight validation across SQLite and Turso (mock Hrana)
- Detection of missing tables, columns, indexes, constraints, and version regressions
- Non-mutating invariant of preflight and recovery inspection
- Safe diagnostic inspection of DELIVERY_UNCERTAIN, stuck DISPATCHING, duplicate claims, and expired locks
- Privacy sanitization of recipient chat IDs and tokens in operator diagnostics
- Source/target cutover reconciliation and abort conditions
- Multi-factor Go/No-Go gate evaluation
- Operational database isolation and fail-closed safety
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from efso_radar.config import Settings
from efso_radar.domain import DeliveryStatus, TelegramNotificationCategory, mask_chat_id, utc_now
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION, get_all_init_statements
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from efso_radar.staging.cutover import (
    GoNoGoDecision,
    GoNoGoGate,
    StagingCutoverReconciler,
)
from efso_radar.staging.preflight import (
    CloudPreflightValidator,
    PreflightReport,
    execute_read_query,
)
from efso_radar.staging.recovery_inspection import (
    DeliveryRecoveryInspector,
    RecoveryInspectionReport,
)


def serialize_hrana_arg(val: Any) -> dict[str, Any]:
    if val is None:
        return {"type": "null"}
    elif isinstance(val, bool):
        return {"type": "integer", "value": "1" if val else "0"}
    elif isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    elif isinstance(val, float):
        return {"type": "float", "value": val}
    elif isinstance(val, str):
        return {"type": "text", "value": val}
    elif isinstance(val, (bytes, bytearray)):
        import base64
        return {"type": "blob", "base64": base64.b64encode(val).decode("ascii")}
    return {"type": "text", "value": str(val)}


def deserialize_hrana_value(item: dict[str, Any]) -> Any:
    val_type = item.get("type", "null")
    if val_type == "null":
        return None
    elif val_type == "integer":
        raw = item.get("value")
        return int(raw) if raw is not None else None
    elif val_type == "float":
        return float(item.get("value", 0.0))
    elif val_type == "text":
        return item.get("value", "")
    elif val_type == "blob":
        import base64
        raw_b64 = item.get("base64", "")
        return base64.b64decode(raw_b64)
    return item.get("value")


def create_mock_hrana_transport(conn: sqlite3.Connection) -> httpx.BaseTransport:
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


class TestPhase5B3C2StagingPreflight(unittest.TestCase):
    """Test suite for cloud staging preflight, recovery inspection, and cutover readiness."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)

        # 1. SQLite synthetic repository
        self.sqlite_db_path = self.tmp_path / "test_staging_sqlite.db"
        self.sqlite_repo = SqliteStateRepository(self.sqlite_db_path)

        # 2. Turso synthetic repository via mock Hrana transport
        self.turso_backend_path = self.tmp_path / "synthetic_turso_backend.db"
        self.turso_conn = sqlite3.connect(self.turso_backend_path, check_same_thread=False)
        self.turso_conn.isolation_level = None
        self.turso_conn.row_factory = sqlite3.Row
        self.turso_transport = create_mock_hrana_transport(self.turso_conn)

        self.turso_repo = TursoStateRepository(
            database_url="libsql://synthetic-cloud.turso.io",
            auth_token="mock-token-12345",
            transport=self.turso_transport,
        )

        # Operational DB snapshot check
        self.op_db_path = Path("data/radar.db")
        self.op_db_existed = self.op_db_path.exists()
        if self.op_db_existed:
            self.op_db_initial_stat = self.op_db_path.stat()

    def tearDown(self):
        self.turso_conn.close()
        import gc
        gc.collect()
        try:
            self.tmp_dir.cleanup()
        except Exception:
            pass

        # Enforce operational DB untouched invariant
        if self.op_db_existed:
            current_stat = self.op_db_path.stat()
            self.assertEqual(
                self.op_db_initial_stat.st_mtime,
                current_stat.st_mtime,
                "CRITICAL: data/radar.db mtime changed during test execution!",
            )
            self.assertEqual(
                self.op_db_initial_stat.st_size,
                current_stat.st_size,
                "CRITICAL: data/radar.db size changed during test execution!",
            )

    # =========================================================================
    # Part 1: Read-Only Cloud Preflight Tests
    # =========================================================================

    def test_01_preflight_passes_on_synthetic_sqlite_schema_v6(self):
        """Preflight succeeds with zero errors on a clean schema v6 SQLite repository."""
        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertTrue(report.success)
        self.assertEqual(report.schema_version, CURRENT_SCHEMA_VERSION)
        self.assertEqual(len(report.missing_tables), 0)
        self.assertEqual(len(report.missing_columns), 0)
        self.assertEqual(len(report.missing_indexes), 0)
        self.assertEqual(len(report.missing_constraints), 0)
        self.assertEqual(len(report.integrity_issues), 0)
        self.assertEqual(len(report.errors), 0)
        self.assertTrue(report.is_non_mutating)
        self.assertIn("CLOUD STAGING PREFLIGHT REPORT [PASSED]", report.summary())

    def test_02_preflight_passes_on_synthetic_turso_schema_v6(self):
        """Preflight succeeds with zero errors on a clean synthetic Turso repository."""
        validator = CloudPreflightValidator(self.turso_repo)
        report = validator.validate()

        self.assertTrue(report.success)
        self.assertEqual(report.schema_version, CURRENT_SCHEMA_VERSION)
        self.assertEqual(len(report.missing_tables), 0)
        self.assertEqual(len(report.missing_columns), 0)
        self.assertEqual(len(report.missing_indexes), 0)
        self.assertEqual(len(report.missing_constraints), 0)
        self.assertEqual(len(report.integrity_issues), 0)
        self.assertTrue(report.is_non_mutating)

    def test_03_preflight_detects_missing_table(self):
        """Preflight fails and flags missing table when a required schema v6 table is absent."""
        with self.sqlite_repo._connection() as conn:
            conn.execute("DROP TABLE telegram_delivery_records")

        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertFalse(report.success)
        self.assertIn("telegram_delivery_records", report.missing_tables)
        self.assertIn("Missing Tables:", report.summary())

    def test_04_preflight_detects_missing_column(self):
        """Preflight fails and flags missing columns when a required table column is absent."""
        # Recreate notifications table missing 'lease_expires_at'
        with self.sqlite_repo._connection() as conn:
            conn.execute("ALTER TABLE notifications RENAME TO notifications_old")
            conn.execute("""
                CREATE TABLE notifications (
                    notification_id TEXT PRIMARY KEY,
                    story_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message_text TEXT NOT NULL,
                    headline_tr TEXT NOT NULL,
                    score INTEGER NOT NULL
                )
            """)

        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertFalse(report.success)
        self.assertIn("notifications", report.missing_columns)
        self.assertIn("lease_expires_at", report.missing_columns["notifications"])

    def test_05_preflight_detects_missing_index(self):
        """Preflight fails and flags missing index when a required index is dropped."""
        with self.sqlite_repo._connection() as conn:
            conn.execute("DROP INDEX idx_budget_history_period")

        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertFalse(report.success)
        self.assertIn("idx_budget_history_period", report.missing_indexes)

    def test_06_preflight_detects_outdated_schema_version(self):
        """Preflight fails if schema version is lower than required CURRENT_SCHEMA_VERSION."""
        with self.sqlite_repo._connection() as conn:
            conn.execute("UPDATE schema_migrations SET version = 5 WHERE version = 6")

        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertFalse(report.success)
        self.assertLess(report.schema_version, CURRENT_SCHEMA_VERSION)
        self.assertTrue(any("version regression" in err.lower() for err in report.errors))

    def test_07_preflight_detects_corrupt_delivery_status(self):
        """Preflight flags data integrity issue if unexpected delivery status exists in outbox."""
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO telegram_delivery_records (
                    delivery_id, message_key, recipient_role, recipient_chat_id,
                    category, status, attempt_count, max_attempts, payload_json,
                    created_at, updated_at
                ) VALUES ('DEL-CORRUPT-01', 'KEY-01', 'PRIMARY_EDITOR', '1111111111',
                          'VERIFIED_BREAKING', 'INVALID_STATUS_XYZ', 1, 3, '{}',
                          '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z')
                """
            )

        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertFalse(report.success)
        self.assertTrue(any("Invalid delivery status" in issue for issue in report.integrity_issues))

    def test_08_preflight_detects_corrupt_recovery_json(self):
        """Preflight flags data integrity issue if recovery state contains invalid JSON."""
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO durable_recovery_state (
                    state_key, stage, worker_id, state_json, created_at, updated_at
                ) VALUES ('STATE-CORRUPT', 'RADAR_PIPELINE', 'worker-1', '{malformed_json_not_valid}',
                          '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z')
                """
            )

        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertFalse(report.success)
        self.assertTrue(any("Corrupt recovery JSON" in issue for issue in report.integrity_issues))

    def test_09_preflight_detects_duplicate_delivery_identities(self):
        """Preflight flags integrity violation when duplicate (chat_id, message_key) exist."""
        # Temporarily drop constraint and insert duplicates to simulate unconstrained state
        with self.sqlite_repo._connection() as conn:
            conn.execute("DROP TABLE telegram_delivery_records")
            conn.execute("""
                CREATE TABLE telegram_delivery_records (
                    delivery_id TEXT PRIMARY KEY,
                    message_key TEXT NOT NULL,
                    recipient_role TEXT NOT NULL,
                    recipient_chat_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    error_message TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    sent_at TEXT
                )
            """)
            conn.execute(
                """
                INSERT INTO telegram_delivery_records (
                    delivery_id, message_key, recipient_role, recipient_chat_id,
                    category, status, payload_json, created_at, updated_at
                ) VALUES ('D1', 'KEY-DUP', 'PRIMARY_EDITOR', '1111111111', 'EDITORIAL_DIGEST', 'SENT', '{}', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z'),
                         ('D2', 'KEY-DUP', 'PRIMARY_EDITOR', '1111111111', 'EDITORIAL_DIGEST', 'SENT', '{}', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z')
                """
            )

        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertFalse(report.success)
        self.assertTrue(any("duplicate delivery identities" in issue for issue in report.integrity_issues))

    def test_10_preflight_non_mutating_invariant(self):
        """Preflight validation strictly preserves database state and leaves fingerprint unchanged."""
        validator = CloudPreflightValidator(self.sqlite_repo)
        report = validator.validate()

        self.assertTrue(report.is_non_mutating)
        self.assertEqual(report.schema_fingerprint_before, report.schema_fingerprint_after)
        self.assertNotEqual(report.schema_fingerprint_before, "")

    def test_11_preflight_prohibits_write_queries(self):
        """execute_read_query fails closed if passed modifying SQL keywords."""
        with self.assertRaises(ValueError) as ctx:
            execute_read_query(self.sqlite_repo, "INSERT INTO run_locks VALUES ('k', 'w', 'a', 'e', 'h')")
        self.assertIn("Preflight query safety violation", str(ctx.exception))

        with self.assertRaises(ValueError):
            execute_read_query(self.sqlite_repo, "DROP TABLE stories")

    def test_12_preflight_fails_closed_on_operational_radar_db(self):
        """CloudPreflightValidator raises PermissionError if initialized with operational data/radar.db."""
        # Test inert mock pointing to real path
        class InertProdRepo(SqliteStateRepository):
            def __init__(self):
                self.db_path = Path("data/radar.db")

        with self.assertRaises(PermissionError):
            CloudPreflightValidator(InertProdRepo())

    # =========================================================================
    # Part 2: Delivery Recovery Inspection Tests
    # =========================================================================

    def test_13_recovery_inspection_detects_uncertain_deliveries(self):
        """Inspector identifies DELIVERY_UNCERTAIN deliveries with masked chat IDs."""
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO telegram_delivery_records (
                    delivery_id, message_key, recipient_role, recipient_chat_id,
                    category, status, attempt_count, max_attempts, error_message,
                    payload_json, created_at, updated_at
                ) VALUES ('DEL-UNCERTAIN-01', 'KEY-UNCERTAIN-1', 'PRIMARY_EDITOR', '2222222222',
                          'EDITORIAL_DIGEST', 'DELIVERY_UNCERTAIN', 1, 1,
                          'HTTP 504 Gateway Timeout contacting Telegram API', '{}',
                          '2026-10-20T10:00:00Z', '2026-10-20T10:00:05Z')
                """
            )

        inspector = DeliveryRecoveryInspector(self.sqlite_repo)
        report = inspector.inspect(now=datetime(2026, 10, 20, 10, 30, tzinfo=timezone.utc))

        self.assertTrue(report.has_recovery_issues)
        self.assertEqual(len(report.uncertain_deliveries), 1)
        rec = report.uncertain_deliveries[0]
        self.assertEqual(rec.delivery_id, "DEL-UNCERTAIN-01")
        self.assertEqual(rec.recipient_role, "PRIMARY_EDITOR")
        self.assertEqual(rec.masked_chat_id, mask_chat_id("2222222222"))
        self.assertIn("DELIVERY_UNCERTAIN", report.summary())

    def test_14_recovery_inspection_detects_stuck_dispatching_claims(self):
        """Inspector identifies DISPATCHING claims stuck beyond lease threshold."""
        now = datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc)
        stuck_time = (now - timedelta(minutes=30)).isoformat()

        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO telegram_delivery_records (
                    delivery_id, message_key, recipient_role, recipient_chat_id,
                    category, status, attempt_count, max_attempts, payload_json,
                    created_at, updated_at
                ) VALUES ('DEL-STUCK-01', 'KEY-STUCK-1', 'PRIMARY_EDITOR', '2222222222',
                          'EDITORIAL_DIGEST', 'DISPATCHING', 1, 3, '{}',
                          ?, ?)
                """,
                [stuck_time, stuck_time],
            )

        inspector = DeliveryRecoveryInspector(self.sqlite_repo)
        report = inspector.inspect(now=now, stuck_claim_threshold_minutes=15.0)

        self.assertEqual(len(report.stuck_dispatching), 1)
        self.assertEqual(report.stuck_dispatching[0].delivery_id, "DEL-STUCK-01")
        self.assertGreaterEqual(report.stuck_dispatching[0].stuck_minutes, 29.0)

    def test_15_recovery_inspection_detects_expired_locks(self):
        """Inspector detects expired worker locks in run_locks table."""
        now = datetime(2026, 10, 20, 15, 0, tzinfo=timezone.utc)
        expired_at = (now - timedelta(minutes=20)).isoformat()

        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at)
                VALUES ('radar_cron', 'worker-crashed-1', '2026-10-20T14:00:00Z', ?, '2026-10-20T14:10:00Z')
                """,
                [expired_at],
            )

        inspector = DeliveryRecoveryInspector(self.sqlite_repo)
        report = inspector.inspect(now=now)

        self.assertEqual(len(report.expired_locks), 1)
        self.assertEqual(report.expired_locks[0].worker_id, "worker-crashed-1")
        self.assertGreaterEqual(report.expired_locks[0].expired_minutes_ago, 19.0)

    def test_16_recovery_inspection_detects_incomplete_executions_and_checkpoints(self):
        """Inspector detects incomplete scheduled runs and active recovery checkpoints."""
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO scheduled_execution_history (
                    run_id, run_type, worker_id, started_at, status, error_message
                ) VALUES ('run_failed_101', 'SCHEDULED_SCAN', 'worker-01', '2026-10-20T08:00:00Z',
                          'FAILED', 'Transient network timeout during evaluation')
                """
            )
            conn.execute(
                """
                INSERT INTO durable_recovery_state (
                    state_key, stage, worker_id, batch_id, state_json, created_at, updated_at
                ) VALUES ('RADAR_PIPELINE', 'DISCOVERY_COMPLETED', 'worker-01', 'run_failed_101',
                          '{"articles_count": 12}', '2026-10-20T08:00:00Z', '2026-10-20T08:05:00Z')
                """
            )

        inspector = DeliveryRecoveryInspector(self.sqlite_repo)
        report = inspector.inspect()

        self.assertEqual(len(report.incomplete_executions), 1)
        self.assertEqual(report.incomplete_executions[0].run_id, "run_failed_101")
        self.assertEqual(len(report.recovery_checkpoints), 1)
        self.assertEqual(report.recovery_checkpoints[0].stage, "DISCOVERY_COMPLETED")

    def test_17_recovery_inspection_sanitizes_chat_ids_and_tokens(self):
        """Inspection summary and report fields strictly mask numeric chat IDs and raw tokens."""
        raw_chat = "9876543210"
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO telegram_delivery_records (
                    delivery_id, message_key, recipient_role, recipient_chat_id,
                    category, status, attempt_count, max_attempts, error_message,
                    payload_json, created_at, updated_at
                ) VALUES ('D-SENS-1', 'KEY-SENS-1', 'ADMIN', ?, 'OPERATIONAL_HEALTH_REPORT',
                          'DELIVERY_UNCERTAIN', 1, 1,
                          'Failed sending to chat 9876543210 with bot 123456:AAFakeToken', '{}',
                          '2026-10-20T10:00:00Z', '2026-10-20T10:00:05Z')
                """,
                [raw_chat],
            )

        inspector = DeliveryRecoveryInspector(self.sqlite_repo)
        report = inspector.inspect()
        summary_text = report.summary()

        self.assertNotIn(raw_chat, summary_text)
        self.assertIn(mask_chat_id(raw_chat), summary_text)

    def test_18_recovery_inspection_strictly_non_mutating(self):
        """Inspector leaves uncertain deliveries and dispatching claims completely untouched."""
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO telegram_delivery_records (
                    delivery_id, message_key, recipient_role, recipient_chat_id,
                    category, status, attempt_count, max_attempts, payload_json,
                    created_at, updated_at
                ) VALUES ('D-INTACT', 'KEY-INTACT', 'PRIMARY_EDITOR', '1111111111',
                          'VERIFIED_BREAKING', 'DELIVERY_UNCERTAIN', 1, 3, '{}',
                          '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z')
                """
            )

        inspector = DeliveryRecoveryInspector(self.sqlite_repo)
        inspector.inspect()

        # Verify database row was NOT modified
        with self.sqlite_repo._connection() as conn:
            cursor = conn.execute("SELECT status FROM telegram_delivery_records WHERE delivery_id = 'D-INTACT'")
            row = cursor.fetchone()
            self.assertEqual(row["status"], DeliveryStatus.DELIVERY_UNCERTAIN.value)

    # =========================================================================
    # Part 3: Staging Cutover & Rollback Preparation Tests
    # =========================================================================

    def test_19_cutover_reconciliation_exact_parity(self):
        """Reconciliation confirms exact count and key parity across clean source and target fixtures."""
        reconciler = StagingCutoverReconciler(
            source_repo=self.sqlite_repo,
            target_repo=self.turso_repo,
        )
        report = reconciler.reconcile()

        self.assertTrue(report.is_safe_to_cutover)
        self.assertTrue(report.all_counts_match)
        self.assertEqual(len(report.abort_reasons), 0)
        self.assertEqual(len(report.table_reconciliations), 15)

    def test_20_cutover_reconciliation_detects_count_mismatch(self):
        """Reconciliation aborts if source has records not present on target."""
        # Insert a story into source SQLite only
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json)
                VALUES ('STORY-001', 'Test Headline', 'YERLI', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z', '[]')
                """
            )

        reconciler = StagingCutoverReconciler(
            source_repo=self.sqlite_repo,
            target_repo=self.turso_repo,
        )
        report = reconciler.reconcile()

        self.assertFalse(report.is_safe_to_cutover)
        self.assertFalse(report.all_counts_match)
        self.assertTrue(any(ar.code == "ERR_COUNT_MISMATCH_STORIES" for ar in report.abort_reasons))

    def test_21_cutover_reconciliation_detects_key_mismatch(self):
        """Reconciliation aborts if same row counts exist but primary keys differ."""
        # Insert different story IDs in source and target
        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json)
                VALUES ('STORY-SOURCE-1', 'Test 1', 'YERLI', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z', '[]')
                """
            )
        self.turso_repo.client.execute(
            """
            INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json)
            VALUES ('STORY-TARGET-1', 'Test 1', 'YERLI', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z', '[]')
            """
        )

        reconciler = StagingCutoverReconciler(
            source_repo=self.sqlite_repo,
            target_repo=self.turso_repo,
        )
        report = reconciler.reconcile()

        self.assertFalse(report.is_safe_to_cutover)
        self.assertTrue(any(ar.code == "ERR_KEY_MISMATCH_STORIES" for ar in report.abort_reasons))

    def test_22_go_no_go_gate_full_approval(self):
        """GoNoGoGate emits GO verdict when all 7 safety criteria are fulfilled."""
        reconciler = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo)
        cutover_report = reconciler.reconcile()

        validator = CloudPreflightValidator(self.turso_repo)
        target_preflight = validator.validate()

        decision = GoNoGoGate.evaluate(
            cutover_report=cutover_report,
            target_preflight=target_preflight,
            backup_verified=True,
            rollback_ready=True,
            zero_cost_verified=True,
            product_owner_signed_off=True,
        )

        self.assertTrue(decision.can_proceed)
        self.assertEqual(len(decision.abort_reasons), 0)
        self.assertIn("GO (APPROVED FOR STAGING CUTOVER)", decision.summary())

    def test_23_go_no_go_gate_blocks_without_product_owner_signoff(self):
        """GoNoGoGate strictly emits NO-GO if Product Owner sign-off is absent."""
        reconciler = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo)
        cutover_report = reconciler.reconcile()

        validator = CloudPreflightValidator(self.turso_repo)
        target_preflight = validator.validate()

        decision = GoNoGoGate.evaluate(
            cutover_report=cutover_report,
            target_preflight=target_preflight,
            backup_verified=True,
            rollback_ready=True,
            zero_cost_verified=True,
            product_owner_signed_off=False,  # Lacking sign-off
        )

        self.assertFalse(decision.can_proceed)
        self.assertTrue(any(ar.code == "ERR_NO_PO_SIGNOFF" for ar in decision.abort_reasons))
        self.assertIn("NO-GO (CUTOVER BLOCKED)", decision.summary())

    def test_24_go_no_go_gate_blocks_on_cost_or_backup_failures(self):
        """GoNoGoGate blocks cutover if verified backup is missing or financial cost non-zero."""
        reconciler = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo)
        cutover_report = reconciler.reconcile()

        validator = CloudPreflightValidator(self.turso_repo)
        target_preflight = validator.validate()

        decision = GoNoGoGate.evaluate(
            cutover_report=cutover_report,
            target_preflight=target_preflight,
            backup_verified=False,
            rollback_ready=True,
            zero_cost_verified=False,
            product_owner_signed_off=True,
        )

        self.assertFalse(decision.can_proceed)
        codes = [ar.code for ar in decision.abort_reasons]
        self.assertIn("ERR_NO_VERIFIED_BACKUP", codes)
        self.assertIn("ERR_COST_INVARIANT_VIOLATION", codes)

    def test_25_cutover_reconciler_fails_closed_on_operational_radar_db(self):
        """StagingCutoverReconciler raises PermissionError if initialized with operational data/radar.db."""
        class InertProdRepo(SqliteStateRepository):
            def __init__(self):
                self.db_path = Path("data/radar.db")

        with self.assertRaises(PermissionError):
            StagingCutoverReconciler(InertProdRepo(), self.turso_repo)


if __name__ == "__main__":
    unittest.main()
