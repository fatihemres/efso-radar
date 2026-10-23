"""Negative tests: preflight diagnostics must be safe and query failures must fail closed."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.staging import preflight as pf
from efso_radar.staging.preflight import CloudPreflightValidator

SECRET = "SECRET-TOKEN-abc123-chat-987654321"


class TestPreflightSafeDiagnostics(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "synthetic.db"
        self.repo = SqliteStateRepository(self.path)

    def tearDown(self):
        import gc
        gc.collect()
        try:
            self.tmp.cleanup()
        except Exception:
            pass

    def _raw(self, sql, params=()):
        conn = sqlite3.connect(self.path)
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()

    def _failing(self, needle):
        real = pf.execute_read_query

        def fake(repo, sql, params=None):
            if needle in sql:
                raise RuntimeError(f"boom {SECRET}")
            return real(repo, sql, params)

        return mock.patch.object(pf, "execute_read_query", side_effect=fake)

    def _assert_no_secret(self, report):
        self.assertNotIn(SECRET, report.summary())
        self.assertNotIn(SECRET, repr(report.model_dump()))

    def test_legitimate_synthetic_preflight_passes(self):
        report = CloudPreflightValidator(self.repo).validate()
        self.assertTrue(report.success, report.summary())

    def test_state_integrity_query_failures_fail_closed_without_leak(self):
        for needle in (
            "SELECT DISTINCT status",
            "FROM durable_recovery_state",
            "GROUP BY recipient_chat_id",
        ):
            with self.subTest(needle=needle), self._failing(needle):
                report = CloudPreflightValidator(self.repo).validate()
                self.assertFalse(report.success)
                self.assertTrue(any("State integrity check failed" in e for e in report.errors))
                self.assertTrue(any("RuntimeError" in e for e in report.errors))
                self._assert_no_secret(report)

    def test_other_query_failures_do_not_leak(self):
        for needle in ("MAX(version)", "PRAGMA table_info", "PRAGMA index_list", "type='table' AND name NOT"):
            with self.subTest(needle=needle), self._failing(needle):
                report = CloudPreflightValidator(self.repo).validate()
                self.assertFalse(report.success)
                self._assert_no_secret(report)

    def test_fingerprint_failure_logs_no_secret_and_fails(self):
        with self._failing("sqlite_master WHERE name NOT LIKE"), self.assertLogs(
            "efso-radar.staging.preflight", level="WARNING"
        ) as cm:
            report = CloudPreflightValidator(self.repo).validate()
        self.assertFalse(report.success)
        self.assertNotIn(SECRET, "\n".join(cm.output))
        self._assert_no_secret(report)

    def test_row_values_never_appear_in_diagnostics(self):
        self._raw(
            "INSERT INTO durable_recovery_state (state_key, stage, worker_id, state_json, created_at, updated_at) "
            "VALUES (?, 's', 'w', ?, 't', 't')",
            (SECRET + "-key", "{not json " + SECRET),
        )
        self._raw("DROP TABLE telegram_delivery_records")
        self._raw(
            "CREATE TABLE telegram_delivery_records (delivery_id TEXT, message_key TEXT, recipient_role TEXT, "
            "recipient_chat_id TEXT, category TEXT, status TEXT, attempt_count INTEGER, max_attempts INTEGER, "
            "error_message TEXT, payload_json TEXT, created_at TEXT, updated_at TEXT, sent_at TEXT)"
        )
        self._raw(
            "INSERT INTO telegram_delivery_records (delivery_id, message_key, recipient_chat_id, status) "
            "VALUES ('d', 'k', 'c', ?)",
            (SECRET + "-status",),
        )
        report = CloudPreflightValidator(self.repo).validate()
        self.assertFalse(report.success)
        self.assertTrue(report.integrity_issues)
        self._assert_no_secret(report)

    def test_null_and_empty_delivery_statuses_are_invalid(self):
        real = pf.execute_read_query

        def mock_statuses(repo, sql, params=None):
            if "SELECT DISTINCT status" in sql:
                return [{"status": None}, {"status": ""}, {"status": "INVALID_STATUS"}]
            return real(repo, sql, params)

        with mock.patch.object(pf, "execute_read_query", side_effect=mock_statuses):
            report = CloudPreflightValidator(self.repo).validate()
        self.assertFalse(report.success)
        self.assertTrue(any("Invalid delivery status" in v and "3 distinct" in v for v in report.integrity_issues))

    def test_recovery_json_after_fiftieth_row_is_detected(self):
        for i in range(50):
            self._raw(
                "INSERT INTO durable_recovery_state "
                "(state_key, stage, worker_id, state_json, created_at, updated_at) "
                "VALUES (?, 'stage', 'worker', ?, 't', 't')",
                (f"STATE-{i:03d}", '{"valid": true}'),
            )
        self._raw(
            "INSERT INTO durable_recovery_state "
            "(state_key, stage, worker_id, state_json, created_at, updated_at) "
            "VALUES ('STATE-050', 'stage', 'worker', '{malformed-json}', 't', 't')"
        )
        report = CloudPreflightValidator(self.repo).validate()
        self.assertFalse(report.success)
        self.assertTrue(any("Corrupt recovery JSON" in issue for issue in report.integrity_issues))

    def test_empty_recovery_json_is_invalid(self):
        self._raw(
            "INSERT INTO durable_recovery_state "
            "(state_key, stage, worker_id, state_json, created_at, updated_at) "
            "VALUES ('STATE-EMPTY', 'stage', 'worker', '', 't', 't')"
        )
        report = CloudPreflightValidator(self.repo).validate()
        self.assertFalse(report.success)
        self.assertTrue(any("Corrupt recovery JSON" in issue for issue in report.integrity_issues))

    def test_missing_state_tables_are_not_query_errors(self):
        self._raw("DROP TABLE durable_recovery_state")
        report = CloudPreflightValidator(self.repo).validate()
        self.assertFalse(report.success)
        self.assertIn("durable_recovery_state", report.missing_tables)
        self.assertFalse(any("State integrity check failed" in e for e in report.errors))


if __name__ == "__main__":
    unittest.main()
