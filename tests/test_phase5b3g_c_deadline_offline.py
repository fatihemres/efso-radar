"""Hermetic Phase 5B.3G-C readiness checks; no live Turso connections."""
from __future__ import annotations

import math
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import TursoClientError, TursoHTTPClient
from efso_radar.staging.runner import (
    AuditOutcomeState,
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
    SessionStatus,
    StagingSessionRunner,
)


class TestOfflineSessionDeadline(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        for statement in get_all_init_statements():
            self.conn.execute(statement)
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-10-25T15:00:00Z', 'Schema v6')"
        )
        self.conn.commit()
        self.tempdir = tempfile.TemporaryDirectory()
        self.raw_transport = HermeticStagingTransport(self.conn)

    def tearDown(self):
        self.tempdir.cleanup()
        self.conn.close()

    def make_runner(self, clock, *, seconds=120.0):
        transport = BudgetTrackingTransport(
            self.raw_transport, session_timeout_seconds=seconds, clock=clock
        )
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock-only",
            transport=transport,
        )
        runner = StagingSessionRunner(
            client=client, manifest_dir=Path(self.tempdir.name)
        )
        return runner, transport

    def test_expiry_before_first_request_never_dispatches_and_retains_manifest(self):
        ticks = [0]

        def clock():
            ticks[0] += 1
            return 0.0 if ticks[0] == 1 else 121.0

        runner, transport = self.make_runner(clock)
        with patch.object(
            self.raw_transport, "handle_request", wraps=self.raw_transport.handle_request
        ) as spy:
            report = runner.run()
            self.assertEqual(spy.call_count, 0)

        self.assertEqual(transport.request_count, 0)
        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_mid_session_expiry_halts_requests_without_deleting_manifest(self):
        counter = [0]

        def clock():
            counter[0] += 1
            return float(counter[0] - 1)

        runner, transport = self.make_runner(clock, seconds=50.0)
        report = runner.run()

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertTrue(report.manifest_preserved)
        self.assertLess(transport.request_count, 50)
        self.assertTrue(
            any("SESSION_TIMEOUT_AUDIT_INCOMPLETE" in e for e in report.errors),
            report.errors,
        )

    def test_started_clock_cannot_be_reset_for_same_transport(self):
        transport = BudgetTrackingTransport(self.raw_transport, clock=lambda: 0.0)
        transport.begin_session()
        with self.assertRaisesRegex(TursoClientError, "SESSION_DEADLINE_ALREADY_STARTED"):
            transport.begin_session()

    def test_bad_deadlines_fail_before_transport_dispatch(self):
        for invalid in (0, -1, 120.001, math.inf, -math.inf, math.nan, True, "120"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                BudgetTrackingTransport(
                    self.raw_transport, session_timeout_seconds=invalid
                )

    def test_bad_clock_rejected(self):
        with self.assertRaises(TypeError):
            BudgetTrackingTransport(self.raw_transport, clock=123)


if __name__ == "__main__":
    unittest.main()
