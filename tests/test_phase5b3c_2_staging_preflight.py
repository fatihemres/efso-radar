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

               # Normalize synthetic migration metadata for exact parity tests.
        fixture_applied_at = "2026-10-20T10:00:00+00:00"
        fixture_description = "Synthetic schema v6 initialization"

        with self.sqlite_repo._connection() as conn:
            conn.execute(
                """
                UPDATE schema_migrations
                SET applied_at = ?, description = ?
                WHERE version = ?
                """,
                (fixture_applied_at, fixture_description, CURRENT_SCHEMA_VERSION),
            )

        self.turso_conn.execute(
            """
            UPDATE schema_migrations
            SET applied_at = ?, description = ?
            WHERE version = ?
            """,
            (fixture_applied_at, fixture_description, CURRENT_SCHEMA_VERSION),
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

    # =========================================================================
    # Part 4: Read-only SQL validation regression tests
    # =========================================================================

    FORBIDDEN_SQL = [
        "PRAGMA user_version(17)",
        "PRAGMA user_version = 17",
        "PRAGMA user_version=17",
        "pragma USER_VERSION ( 17 )",
        "PRAGMA writable_schema = ON",
        "PRAGMA query_only = OFF",
        "PRAGMA journal_mode(DELETE)",
        "PRAGMA main.user_version(5)",
        "PRAGMA table_info(stories); DROP TABLE stories",
        "PRAGMA table_info(1)",
        "WITH c AS (SELECT 1) INSERT INTO run_locks VALUES ('k','w','a','e','h')",
        "WITH c AS (SELECT 1) DELETE FROM stories",
        "WITH c AS (SELECT 1) UPDATE stories SET headline_tr = 'x'",
        "SELECT 1; DELETE FROM stories",
        "SELECT 1; SELECT 2",
        "SELECT 1 -- ; harmless\n; DROP TABLE stories",
        "SELECT '--'; DROP TABLE stories",
        "SELECT 1 /* x */ ; DROP TABLE stories",
        "REPLACE INTO stories VALUES (1)",
        "VACUUM",
        "ANALYZE",
        "BEGIN",
        "SELECT load_extension('x')",
        "SELECT * FROM stories WHERE 1 AND PRAGMA",
        "",
        "   ",
    ]

    def test_26_read_only_validator_rejects_mutating_sql_variants(self):
        """validate_read_only_sql rejects PRAGMA setters, CTE writes, comments and multi-statements."""
        from efso_radar.staging.preflight import validate_read_only_sql

        for sql in self.FORBIDDEN_SQL:
            with self.subTest(sql=sql):
                with self.assertRaises(ValueError):
                    validate_read_only_sql(sql)

    def test_27_read_only_validator_allows_legitimate_introspection(self):
        """Existing preflight introspection queries remain permitted."""
        from efso_radar.staging.preflight import validate_read_only_sql

        allowed = [
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
            "PRAGMA table_info(stories)",
            "PRAGMA index_list(telegram_delivery_records)",
            "PRAGMA index_info(sqlite_autoindex_telegram_delivery_records_2)",
            "PRAGMA user_version",
            "PRAGMA database_list",
            "SELECT COUNT(*) as cnt FROM stories;",
            "WITH c AS (SELECT 1 AS x) SELECT * FROM c",
            "SELECT * FROM stories WHERE headline_tr = 'DELETE ME'",
        ]
        for sql in allowed:
            with self.subTest(sql=sql):
                validate_read_only_sql(sql)

    def _schema_snapshot(self, execute):
        rows = execute("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
        uv = execute("PRAGMA user_version")
        return json.dumps([dict(r) if not isinstance(r, dict) else r for r in rows], sort_keys=True), uv

    def test_28_rejected_sql_cannot_modify_synthetic_sqlite_or_turso(self):
        """Every forbidden statement raises before reaching either synthetic database; state is unchanged."""
        def sqlite_exec(sql):
            with sqlite3.connect(self.sqlite_db_path) as c:
                c.row_factory = sqlite3.Row
                return [dict(r) for r in c.execute(sql).fetchall()]

        def turso_exec(sql):
            return [dict(r) for r in self.turso_conn.execute(sql).fetchall()]

        before_sqlite = self._schema_snapshot(sqlite_exec)
        before_turso = self._schema_snapshot(turso_exec)

        for sql in self.FORBIDDEN_SQL:
            for repo in (self.sqlite_repo, self.turso_repo):
                with self.subTest(sql=sql, repo=type(repo).__name__):
                    with self.assertRaises(ValueError):
                        execute_read_query(repo, sql)

        self.assertEqual(before_sqlite, self._schema_snapshot(sqlite_exec))
        self.assertEqual(before_turso, self._schema_snapshot(turso_exec))
        self.assertEqual(sqlite_exec("PRAGMA user_version")[0]["user_version"], before_sqlite[1][0]["user_version"])
        self.assertEqual(turso_exec("PRAGMA user_version")[0]["user_version"], before_turso[1][0]["user_version"])

    def test_29_valid_introspection_works_on_sqlite_and_mock_turso(self):
        """Equivalent safeguards: allowed queries still execute on both backends."""
        for repo in (self.sqlite_repo, self.turso_repo):
            with self.subTest(repo=type(repo).__name__):
                rows = execute_read_query(repo, "PRAGMA table_info(stories)")
                self.assertTrue(any(r["name"] == "story_id" for r in rows))
                rows = execute_read_query(repo, "SELECT COUNT(*) as cnt FROM stories")
                self.assertEqual(int(rows[0]["cnt"]), 0)

    # =========================================================================
    # Part 5: Lossless content hash regression tests
    # =========================================================================

    _DELIVERY_INSERT = """
        INSERT INTO telegram_delivery_records (
            delivery_id, message_key, recipient_role, recipient_chat_id,
            category, status, attempt_count, max_attempts, payload_json,
            created_at, updated_at
        ) VALUES ('D-1', 'K-1', 'PRIMARY_EDITOR', '1111111111', 'EDITORIAL_DIGEST', ?, 1, 3, ?,
                  '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z')
    """

    def _insert_delivery(self, repo, status, payload):
        if repo is self.sqlite_repo:
            with repo._connection() as conn:
                conn.execute(self._DELIVERY_INSERT, [status, payload])
        else:
            self.turso_conn.execute(self._DELIVERY_INSERT, [status, payload])

    def _gate(self, report):
        preflight = CloudPreflightValidator(self.turso_repo).validate()
        return GoNoGoGate.evaluate(
            cutover_report=report,
            target_preflight=preflight,
            backup_verified=True,
            rollback_ready=True,
            zero_cost_verified=True,
            product_owner_signed_off=True,
        )

    def test_30_content_hash_distinguishes_previously_colliding_values(self):
        """NULL/'__NULL__', 6/'6', nearby floats, whitespace, JSON-like text and BLOB/TEXT all differ."""
        from efso_radar.staging.cutover import compute_table_content_hash as h

        def one(v):
            return h([{"c": v}], ["c"])

        pairs = [
            (None, "__NULL__"),
            (6, "6"),
            (0.1234561, 0.1234562),
            (1, 1.0),
            ("abc", "abc "),
            ("{\"a\": 1}", "{\"a\":1}"),
            (" [1] ", "[1]"),
            (b"a", "a"),
            ("", None),
            (0, None),
        ]
        for a, b in pairs:
            with self.subTest(a=repr(a), b=repr(b)):
                self.assertNotEqual(one(a), one(b))

    def test_31_content_hash_boundaries_and_determinism(self):
        """Identical data hashes identically; row/column boundaries are unambiguous."""
        from efso_radar.staging.cutover import compute_table_content_hash as h

        rows = [{"a": "x", "b": 1}, {"a": None, "b": 2.5}]
        self.assertEqual(h(rows, ["a", "b"]), h([dict(r) for r in rows], ["a", "b"]))
        self.assertNotEqual(h([{"a": "ab", "b": "c"}], ["a", "b"]), h([{"a": "a", "b": "bc"}], ["a", "b"]))
        self.assertNotEqual(h([{"a": "x"}, {"a": "y"}], ["a"]), h([{"a": "xy"}], ["a"]))
        self.assertNotEqual(h([{"a": "x"}], ["a"]), h([{"a": "x"}, {"a": "x"}], ["a"]))
        self.assertNotEqual(h(rows, ["a", "b"]), h(rows, ["b", "a"]))

    def test_32_content_hash_fails_closed_without_leaking_content(self):
        """Unsupported values or missing columns raise ContentHashError with no record contents."""
        from efso_radar.staging.cutover import ContentHashError, compute_table_content_hash as h

        secret = "SECRET-1234567890"
        with self.assertRaises(ContentHashError) as ctx:
            h([{"a": {"k": secret}}], ["a"])
        self.assertNotIn(secret, str(ctx.exception))
        with self.assertRaises(ContentHashError):
            h([{"a": 1}], ["a", "missing"])
        with self.assertRaises(ContentHashError):
            h([{"a": "\ud800"}], ["a"])

    def test_33_same_keys_and_counts_different_delivery_status_is_no_go(self):
        """Matching PK and counts but different delivery status must produce NO-GO."""
        self._insert_delivery(self.sqlite_repo, "SENT", "{}")
        self._insert_delivery(self.turso_repo, "PENDING", "{}")
        report = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo).reconcile()

        self.assertTrue(report.all_counts_match)
        self.assertTrue(report.all_keys_match)
        self.assertFalse(report.is_safe_to_cutover)
        self.assertTrue(any(a.code == "ERR_CONTENT_MISMATCH_TELEGRAM_DELIVERY_RECORDS" for a in report.abort_reasons))
        self.assertFalse(self._gate(report).can_proceed)

    def test_34_same_keys_and_counts_different_payload_is_no_go(self):
        """Payload text differing only by whitespace/JSON formatting is still a mismatch."""
        self._insert_delivery(self.sqlite_repo, "SENT", '{"a":1}')
        self._insert_delivery(self.turso_repo, "SENT", '{"a": 1}')
        report = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo).reconcile()

        self.assertFalse(report.is_safe_to_cutover)
        self.assertTrue(any(a.code == "ERR_CONTENT_MISMATCH_TELEGRAM_DELIVERY_RECORDS" for a in report.abort_reasons))
        self.assertFalse(self._gate(report).can_proceed)

    def test_35_sqlite_and_mock_turso_identical_mixed_types_reconcile(self):
        """Identical rows (NULL, INTEGER, REAL, TEXT) reconcile across SQLite and mock Turso."""
        stmt = (
            "INSERT INTO candidate_backlog (article_id, url, canonical_url, title, source, market, "
            "category_hint, snippet, image_url, published_at, discovered_at, status, priority_score) "
            "VALUES ('A-1', 'u', NULL, ' Title ', 'src', 'TR', NULL, 'snip', NULL, 'p', 'd', 'QUEUED', 0.1234561)"
        )
        with self.sqlite_repo._connection() as conn:
            conn.execute(stmt)
        self.turso_conn.execute(stmt)
        self._insert_delivery(self.sqlite_repo, "SENT", '{"a":1}')
        self._insert_delivery(self.turso_repo, "SENT", '{"a":1}')

        report = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo).reconcile()
        self.assertTrue(report.is_safe_to_cutover, [a.message for a in report.abort_reasons])
        self.assertTrue(self._gate(report).can_proceed)

    def test_36_float_precision_difference_is_no_go(self):
        """REAL values differing beyond six decimals no longer reconcile."""
        tmpl = (
            "INSERT INTO candidate_backlog (article_id, url, title, source, market, snippet, "
            "published_at, discovered_at, status, priority_score) "
            "VALUES ('A-1', 'u', 't', 's', 'TR', 'sn', 'p', 'd', 'QUEUED', {})"
        )
        with self.sqlite_repo._connection() as conn:
            conn.execute(tmpl.format("0.1234561"))
        self.turso_conn.execute(tmpl.format("0.1234562"))
        report = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo).reconcile()
        self.assertFalse(report.is_safe_to_cutover)
        self.assertTrue(any(a.code == "ERR_CONTENT_MISMATCH_CANDIDATE_BACKLOG" for a in report.abort_reasons))

    def test_37_hash_serialization_failure_is_fail_closed_and_redacted(self):
        """A hashing failure aborts that table, blocks GO and does not echo record contents."""
        from unittest import mock
        from efso_radar.staging import cutover

        self._insert_delivery(self.sqlite_repo, "SENT", "{}")
        self._insert_delivery(self.turso_repo, "SENT", "{}")
        err = cutover.ContentHashError("payload SECRET-9876543210 leaked")
        with mock.patch.object(cutover, "compute_table_content_hash", side_effect=err):
            report = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo).reconcile()

        self.assertFalse(report.is_safe_to_cutover)
        self.assertTrue(any(a.code.startswith("ERR_TABLE_INSPECT_") for a in report.abort_reasons))
        self.assertFalse(any("SECRET-9876543210" in a.message for a in report.abort_reasons))
        self.assertFalse(self._gate(report).can_proceed)

    # =========================================================================
    # Part 6: Critical UNIQUE constraint validation
    # =========================================================================

    _TDR_COLS = """
        delivery_id TEXT PRIMARY KEY, message_key TEXT NOT NULL, recipient_role TEXT NOT NULL,
        recipient_chat_id TEXT NOT NULL, category TEXT NOT NULL, status TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL DEFAULT 3,
        error_message TEXT, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL, sent_at TEXT
    """

    def _rebuild_delivery_table(self, table_constraint="", extra_statements=()):
        """Recreate telegram_delivery_records with a chosen uniqueness definition (synthetic DB only)."""
        with self.sqlite_repo._connection() as conn:
            conn.execute("DROP TABLE telegram_delivery_records")
            tail = f", {table_constraint}" if table_constraint else ""
            conn.execute(f"CREATE TABLE telegram_delivery_records ({self._TDR_COLS}{tail})")
            conn.execute("CREATE INDEX idx_telegram_delivery_records_status ON telegram_delivery_records (status, updated_at)")
            conn.execute("CREATE INDEX idx_telegram_delivery_records_role ON telegram_delivery_records (recipient_role, created_at)")
            for stmt in extra_statements:
                conn.execute(stmt)

    def _unique_missing(self, report):
        return [c for c in report.missing_constraints if "UNIQUE(" in c]

    def test_38_composite_unique_constraint_passes(self):
        self._rebuild_delivery_table("CONSTRAINT uq_recipient_message UNIQUE (recipient_chat_id, message_key)")
        report = CloudPreflightValidator(self.sqlite_repo).validate()
        self.assertEqual(self._unique_missing(report), [])
        self.assertTrue(report.success, report.summary())

    def test_39_composite_unique_with_reversed_column_order_passes(self):
        self._rebuild_delivery_table("CONSTRAINT uq_recipient_message UNIQUE (message_key, recipient_chat_id)")
        report = CloudPreflightValidator(self.sqlite_repo).validate()
        self.assertEqual(self._unique_missing(report), [])

    def test_40_standalone_unique_index_passes(self):
        self._rebuild_delivery_table(
            "",
            ["CREATE UNIQUE INDEX uq_recipient_message ON telegram_delivery_records (recipient_chat_id, message_key)"],
        )
        report = CloudPreflightValidator(self.sqlite_repo).validate()
        self.assertEqual(self._unique_missing(report), [])
        self.assertTrue(report.success, report.summary())

    def test_41_invalid_unique_substitutes_fail(self):
        """Wrong columns, split uniques, non-unique, partial, expression and DDL-text-only all fail."""
        cases = {
            "different_column": ("CONSTRAINT uq_recipient_message UNIQUE (recipient_chat_id, category)", []),
            "two_single_uniques": (
                "CONSTRAINT uq_recipient_message_a UNIQUE (recipient_chat_id), UNIQUE (message_key)", []),
            "non_unique_index": (
                "", ["CREATE INDEX uq_recipient_message ON telegram_delivery_records (recipient_chat_id, message_key)"]),
            "partial_unique": (
                "", ["CREATE UNIQUE INDEX uq_recipient_message ON telegram_delivery_records "
                     "(recipient_chat_id, message_key) WHERE status = 'SENT'"]),
            "expression_unique": (
                "", ["CREATE UNIQUE INDEX uq_recipient_message ON telegram_delivery_records "
                     "(recipient_chat_id, lower(message_key))"]),
            "superset_unique": (
                "", ["CREATE UNIQUE INDEX uq_recipient_message ON telegram_delivery_records "
                     "(recipient_chat_id, message_key, category)"]),
            "ddl_text_only": ("CHECK (length('UNIQUE recipient_chat_id message_key uq_recipient_message') > 0)", []),
        }
        for name, (constraint, extra) in cases.items():
            with self.subTest(case=name):
                self._rebuild_delivery_table(constraint, extra)
                report = CloudPreflightValidator(self.sqlite_repo).validate()
                self.assertFalse(report.success)
                self.assertEqual(len(self._unique_missing(report)), 1, report.summary())

    def test_42_broken_or_missing_index_metadata_fails_closed(self):
        from unittest import mock
        from efso_radar.staging import preflight

        real = preflight.execute_read_query

        def raising(repo, sql, params=None):
            if "index_xinfo" in sql:
                raise RuntimeError("boom SECRET-5555555555")
            return real(repo, sql, params)

        def stripped(repo, sql, params=None):
            rows = real(repo, sql, params)
            if "index_xinfo" in sql:
                return [{k: v for k, v in r.items() if k != "key"} for r in rows]
            if "index_list" in sql:
                return [{k: v for k, v in r.items() if k != "partial"} for r in rows]
            return rows

        for name, fake in (("raising", raising), ("stripped", stripped)):
            for repo in (self.sqlite_repo, self.turso_repo):
                with self.subTest(case=name, repo=type(repo).__name__):
                    with mock.patch.object(preflight, "execute_read_query", side_effect=fake):
                        report = CloudPreflightValidator(repo).validate()
                    self.assertFalse(report.success)
                    self.assertEqual(len(self._unique_missing(report)), 1)
                    self.assertFalse(any("SECRET-5555555555" in e for e in report.errors))

    def test_43_correct_schema_passes_on_sqlite_and_mock_turso(self):
        for repo in (self.sqlite_repo, self.turso_repo):
            with self.subTest(repo=type(repo).__name__):
                report = CloudPreflightValidator(repo).validate()
                self.assertTrue(report.success, report.summary())
                self.assertEqual(self._unique_missing(report), [])

    def test_44_valid_unique_index_passes_regardless_of_index_or_constraint_name(self):
        """A full-table UNIQUE index on (recipient_chat_id, message_key) passes under any index/constraint name."""
        cases = {
            "unnamed_table_unique": ("UNIQUE (recipient_chat_id, message_key)", []),
            "custom_named_constraint": ("CONSTRAINT custom_delivery_identity_uq UNIQUE (recipient_chat_id, message_key)", []),
            "custom_named_standalone_index": (
                "",
                ["CREATE UNIQUE INDEX idx_custom_recipient_msg_unique ON telegram_delivery_records (recipient_chat_id, message_key)"],
            ),
            "reversed_custom_named_index": (
                "",
                ["CREATE UNIQUE INDEX idx_arbitrary_name_v2 ON telegram_delivery_records (message_key, recipient_chat_id)"],
            ),
        }
        for name, (constraint, extra) in cases.items():
            with self.subTest(case=name):
                self._rebuild_delivery_table(constraint, extra)
                report = CloudPreflightValidator(self.sqlite_repo).validate()
                self.assertTrue(report.success, report.summary())
                self.assertEqual(report.missing_constraints, [])
                self.assertEqual(report.errors, [])

    def test_45_nullable_identity_columns_fail_preflight_despite_unique_constraint(self):
        """SQLite UNIQUE allows duplicate NULLs; nullable recipient_chat_id or message_key must fail preflight."""
        nullable_variants = {
            "nullable_recipient_chat_id": self._TDR_COLS.replace(
                "recipient_chat_id TEXT NOT NULL", "recipient_chat_id TEXT"
            ),
            "nullable_message_key": self._TDR_COLS.replace(
                "message_key TEXT NOT NULL", "message_key TEXT"
            ),
            "both_nullable": (
                self._TDR_COLS.replace("recipient_chat_id TEXT NOT NULL", "recipient_chat_id TEXT")
                .replace("message_key TEXT NOT NULL", "message_key TEXT")
            ),
        }
        for name, cols_ddl in nullable_variants.items():
            with self.subTest(case=name):
                with self.sqlite_repo._connection() as conn:
                    conn.execute("DROP TABLE telegram_delivery_records")
                    conn.execute(
                        f"CREATE TABLE telegram_delivery_records ({cols_ddl}, "
                        "CONSTRAINT uq_recipient_message UNIQUE (recipient_chat_id, message_key))"
                    )
                    conn.execute(
                        "CREATE INDEX idx_telegram_delivery_records_status ON telegram_delivery_records (status, updated_at)"
                    )
                    conn.execute(
                        "CREATE INDEX idx_telegram_delivery_records_role ON telegram_delivery_records (recipient_role, created_at)"
                    )
                report = CloudPreflightValidator(self.sqlite_repo).validate()
                self.assertFalse(report.success)
                self.assertIn(
                    "telegram_delivery_records.UNIQUE(message_key, recipient_chat_id)",
                    report.missing_constraints,
                )
                self.assertTrue(any("NOT NULL" in err for err in report.errors), report.errors)

    def test_46_malformed_lock_expiration_timestamp_fails_closed(self):
        """Invalid or unparseable lock expiration timestamps must fail closed and never be silently ignored."""
        secret_token = "8887776665:AAH_SecretBotTokenValue1234567890"
        secret_chat = "-1009876543210"
        malformed_cases = {
            "unparseable_string_with_secrets": f"not-a-timestamp-{secret_token}-{secret_chat}",
            "empty_string": "",
            "whitespace_only": "   ",
            "invalid_month": "2026-99-99T00:00:00Z",
        }
        for case_name, bad_expires_at in malformed_cases.items():
            with self.subTest(case=case_name):
                with self.sqlite_repo._connection() as conn:
                    conn.execute("DELETE FROM run_locks")
                    conn.execute(
                        "INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) VALUES (?, ?, ?, ?, ?)",
                        ("radar_run", f"worker-{secret_chat}", "2026-03-01T00:00:00Z", bad_expires_at, "2026-03-01T00:00:00Z"),
                    )
                inspector = DeliveryRecoveryInspector(self.sqlite_repo)
                report = inspector.inspect()

                self.assertFalse(report.is_inspection_complete)
                self.assertFalse(report.is_all_clear)
                self.assertTrue(report.has_recovery_issues)
                self.assertGreaterEqual(len(report.inspection_errors), 1)
                self.assertLess(report.inspections_succeeded, report.inspections_attempted)

                summary_text = report.summary()
                self.assertIn("Status: INSPECTION FAILED / INCOMPLETE", summary_text)
                self.assertNotIn("ALL CLEAR", summary_text)
                self.assertNotIn(secret_token, summary_text)
                self.assertNotIn(secret_chat, summary_text)
                for err in report.inspection_errors:
                    self.assertNotIn(secret_token, err)
                    self.assertNotIn(secret_chat, err)

    def test_47_empty_or_incomplete_recovery_report_never_reports_all_clear(self):
        """An empty, never-executed, or partially completed RecoveryInspectionReport must never report ALL CLEAR."""
        # 1. Default / never-executed report
        empty_report = RecoveryInspectionReport()
        self.assertFalse(empty_report.is_inspection_complete)
        self.assertFalse(empty_report.is_all_clear)
        self.assertTrue(empty_report.has_recovery_issues)
        self.assertIn("Status: INSPECTION FAILED / INCOMPLETE", empty_report.summary())
        self.assertNotIn("ALL CLEAR", empty_report.summary())

        # 2. Report marked complete=True but with 0 attempted/succeeded inspections
        zero_count_report = RecoveryInspectionReport(
            is_inspection_complete=True,
            inspections_attempted=0,
            inspections_succeeded=0,
        )
        self.assertFalse(zero_count_report.is_all_clear)
        self.assertTrue(zero_count_report.has_recovery_issues)
        self.assertIn("Status: INSPECTION FAILED / INCOMPLETE", zero_count_report.summary())
        self.assertNotIn("ALL CLEAR", zero_count_report.summary())

        # 3. Report with fewer than all 6 required inspections succeeded
        partial_report = RecoveryInspectionReport(
            is_inspection_complete=True,
            inspections_attempted=5,
            inspections_succeeded=5,
        )
        self.assertFalse(partial_report.is_all_clear)
        self.assertTrue(partial_report.has_recovery_issues)
        self.assertIn("Status: INSPECTION FAILED / INCOMPLETE", partial_report.summary())
        self.assertNotIn("ALL CLEAR", partial_report.summary())

        # 4. Report where attempted == 6 but succeeded == 5 (even if inspection_errors were empty)
        mismatched_report = RecoveryInspectionReport(
            is_inspection_complete=True,
            inspections_attempted=6,
            inspections_succeeded=5,
        )
        self.assertFalse(mismatched_report.is_all_clear)
        self.assertTrue(mismatched_report.has_recovery_issues)
        self.assertIn("Status: INSPECTION FAILED / INCOMPLETE", mismatched_report.summary())

        # 5. Clean executed inspection (all 6 required inspections succeed)
        clean_report = DeliveryRecoveryInspector(self.sqlite_repo).inspect()
        self.assertTrue(clean_report.is_inspection_complete)
        self.assertEqual(clean_report.inspections_attempted, 6)
        self.assertEqual(clean_report.inspections_succeeded, 6)
        self.assertTrue(clean_report.is_all_clear)
        self.assertFalse(clean_report.has_recovery_issues)
        self.assertIn("Status: ALL CLEAR", clean_report.summary())

    def test_48_deleted_or_missing_sqlite_file_fails_closed_and_stays_absent(self):
        """Deleting the SQLite file after repo init must fail closed in execute_read_query, preflight, recovery, and cutover without recreating the file."""
        # Delete the initialized SQLite database file
        self.sqlite_db_path.unlink()
        self.assertFalse(self.sqlite_db_path.exists())

        # 1. Direct execute_read_query must raise FileNotFoundError and not create the file
        with self.assertRaises(FileNotFoundError):
            execute_read_query(self.sqlite_repo, "SELECT 1")
        self.assertFalse(self.sqlite_db_path.exists())

        # 2. CloudPreflightValidator must fail closed and not create the file
        preflight_report = CloudPreflightValidator(self.sqlite_repo).validate()
        self.assertFalse(preflight_report.success)
        self.assertGreaterEqual(len(preflight_report.errors), 1)
        self.assertFalse(self.sqlite_db_path.exists())

        # 3. DeliveryRecoveryInspector must fail closed and not create the file
        recovery_report = DeliveryRecoveryInspector(self.sqlite_repo).inspect()
        self.assertFalse(recovery_report.is_inspection_complete)
        self.assertFalse(recovery_report.is_all_clear)
        self.assertTrue(recovery_report.has_recovery_issues)
        self.assertEqual(recovery_report.inspections_succeeded, 0)
        self.assertEqual(len(recovery_report.inspection_errors), 6)
        self.assertFalse(self.sqlite_db_path.exists())

        # 4. StagingCutoverReconciler must fail closed and not create the file
        cutover_report = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo).reconcile()
        self.assertFalse(cutover_report.is_safe_to_cutover)
        self.assertGreaterEqual(len(cutover_report.abort_reasons), 1)
        self.assertFalse(self.sqlite_db_path.exists())

        # 5. Invalid/inaccessible db_path configurations (None, empty, :memory:, directory path)
        for bad_path in (None, "", "   ", ":memory:", str(self.tmp_dir)):
            with self.subTest(bad_path=bad_path):
                self.sqlite_repo.db_path = bad_path  # type: ignore[assignment]
                with self.assertRaises(FileNotFoundError):
                    execute_read_query(self.sqlite_repo, "SELECT 1")
        self.assertFalse(self.sqlite_db_path.exists())

    def test_49_eleventh_primary_key_mismatch_beyond_sample_window_rejected_by_reconciler_and_gate(self):
        """When >10 records exist with identical counts and first 10 PKs matching, a different 11th PK is rejected."""
        insert_sql = (
            "INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json) "
            "VALUES (?, ?, 'YERLI', '2026-10-20T10:00:00Z', '2026-10-20T10:00:00Z', '[]')"
        )
        # Insert 10 identical records (STORY-001 .. STORY-010) into both source and target
        with self.sqlite_repo._connection() as conn:
            for i in range(1, 11):
                pk = f"STORY-{i:03d}"
                conn.execute(insert_sql, (pk, f"Headline {i}"))
                self.turso_conn.execute(insert_sql, (pk, f"Headline {i}"))
            # Insert a diverging 11th record with identical non-PK columns
            conn.execute(insert_sql, ("STORY-011-SOURCE", "Headline 11"))
            self.turso_conn.execute(insert_sql, ("STORY-011-TARGET", "Headline 11"))

        reconciler = StagingCutoverReconciler(self.sqlite_repo, self.turso_repo)
        cutover_report = reconciler.reconcile()

        stories_rec = next(r for r in cutover_report.table_reconciliations if r.table_name == "stories")
        self.assertEqual(stories_rec.source_count, 11)
        self.assertEqual(stories_rec.target_count, 11)
        self.assertTrue(stories_rec.count_matches)
        # First 10 sample keys are identical, proving the 10-key sample alone would not catch this
        self.assertEqual(stories_rec.source_sample_keys, stories_rec.target_sample_keys)
        self.assertFalse(stories_rec.keys_match)
        self.assertFalse(stories_rec.content_matches)

        self.assertTrue(cutover_report.all_counts_match)
        self.assertFalse(cutover_report.all_keys_match)
        self.assertFalse(cutover_report.all_contents_match)
        self.assertFalse(cutover_report.is_safe_to_cutover)
        abort_codes = [ar.code for ar in cutover_report.abort_reasons]
        self.assertIn("ERR_KEY_MISMATCH_STORIES", abort_codes)
        self.assertIn("ERR_CONTENT_MISMATCH_STORIES", abort_codes)

        decision = self._gate(cutover_report)
        self.assertFalse(decision.can_proceed)
        self.assertFalse(decision.data_reconciliation_passed)
        gate_codes = [ar.code for ar in decision.abort_reasons]
        self.assertIn("ERR_KEY_MISMATCH_STORIES", gate_codes)
        self.assertIn("ERR_CONTENT_MISMATCH_STORIES", gate_codes)
        self.assertIn("NO-GO (CUTOVER BLOCKED)", decision.summary())

    def test_50_recovery_diagnostic_errors_report_safe_categories_without_leaking_arbitrary_secrets(self):
        """Recovery inspection errors report only safe error categories and never echo raw exception strings or arbitrary DB cell values."""
        from unittest import mock
        from efso_radar.staging import recovery_inspection

        arbitrary_db_secret = "UNPATTERNED_DB_CELL_SECRET_ALPHA_991122"
        arbitrary_exc_secret = "ARBITRARY_EXCEPTION_PAYLOAD_OMEGA_778899"

        # 1. Insert an unpatterned secret inside run_locks.expires_at
        with self.sqlite_repo._connection() as conn:
            conn.execute("DELETE FROM run_locks")
            conn.execute(
                "INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) VALUES (?, ?, ?, ?, ?)",
                ("radar_lock", "worker-1", "2026-03-01T00:00:00Z", arbitrary_db_secret, "2026-03-01T00:00:00Z"),
            )

        report_lock = DeliveryRecoveryInspector(self.sqlite_repo).inspect()
        self.assertFalse(report_lock.is_inspection_complete)
        self.assertFalse(report_lock.is_all_clear)
        self.assertNotIn(arbitrary_db_secret, report_lock.summary())
        for err in report_lock.inspection_errors:
            self.assertNotIn(arbitrary_db_secret, err)
            self.assertIn("invalid_iso_timestamp (ValueError)", err)

        # 2. Simulate query exception carrying arbitrary non-pattern secret text across all 6 inspections
        def raising_query(repo, sql, params=None):
            raise RuntimeError(f"driver dump: {arbitrary_exc_secret} row={arbitrary_db_secret}")

        with mock.patch.object(recovery_inspection, "execute_read_query", side_effect=raising_query):
            report_exc = DeliveryRecoveryInspector(self.sqlite_repo).inspect()

        self.assertFalse(report_exc.is_inspection_complete)
        self.assertFalse(report_exc.is_all_clear)
        self.assertEqual(len(report_exc.inspection_errors), 6)
        summary_exc = report_exc.summary()
        self.assertNotIn(arbitrary_exc_secret, summary_exc)
        self.assertNotIn(arbitrary_db_secret, summary_exc)
        for err in report_exc.inspection_errors:
            self.assertNotIn(arbitrary_exc_secret, err)
            self.assertNotIn(arbitrary_db_secret, err)
            self.assertTrue(err.endswith(": RuntimeError"), err)


if __name__ == "__main__":
    unittest.main()
