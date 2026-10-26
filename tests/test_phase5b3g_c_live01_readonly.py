"""Safety tests for explicitly authorized Turso LIVE-01, all on in-memory SQLite."""
from __future__ import annotations

import io
import json
import sqlite3
import unittest
from unittest.mock import patch

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.staging.live01_readonly import (
    EXACT_PIPELINE,
    Live01ReadOnlyTransport,
    execute_live01_readonly,
    main,
)
from efso_radar.staging.runner import EXACT_STAGING_DESTINATION, HermeticStagingTransport


class TestLive01HermeticPreflight(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        for statement in get_all_init_statements():
            self.conn.execute(statement)
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-10-25T15:00:00Z', 'Schema v6')"
        )
        self.conn.commit()
        self.transport = HermeticStagingTransport(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_full_preflight_and_five_collision_checks_are_read_only(self):
        with patch.object(
            self.transport, "handle_request", wraps=self.transport.handle_request
        ) as spy:
            report = execute_live01_readonly("mock-secret-not-persisted", inner_transport=self.transport)
        self.assertTrue(report.passed, report.summary())
        self.assertEqual(report.schema_version, 6)
        self.assertEqual(report.tables_checked, 15)
        self.assertGreaterEqual(report.columns_checked, 129)
        self.assertEqual(report.collision_checks, 5)
        self.assertLessEqual(report.request_count, 60)
        self.assertEqual(spy.call_count, report.request_count)
        self.assertNotIn("mock-secret-not-persisted", report.summary())
        self.assertNotIn("synth_art", report.summary())
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM processed_articles").fetchone()[0], 0)

    def test_uninitialized_schema_aborts_before_collision_checks(self):
        conn = sqlite3.connect(":memory:")
        try:
            result = execute_live01_readonly(
                "mock-secret", inner_transport=HermeticStagingTransport(conn)
            )
            self.assertFalse(result.passed)
            self.assertEqual(result.collision_checks, 0)
            self.assertEqual(result.failure_category, "SCHEMA_OR_INTEGRITY_GATE")
        finally:
            conn.close()

    def test_wrong_target_denied_even_with_mock_transport(self):
        with self.assertRaises(PermissionError):
            execute_live01_readonly(
                "mock-secret",
                target="libsql://other-database.turso.io",
                inner_transport=self.transport,
            )

    def test_missing_token_denied_before_any_network(self):
        with self.assertRaises(ValueError):
            execute_live01_readonly("", inner_transport=self.transport)

    def test_transport_blocks_write_and_wrong_method_before_dispatch(self):
        called = []
        mock = httpx.MockTransport(lambda req: called.append(req) or httpx.Response(200, json={}))
        guard = Live01ReadOnlyTransport(mock)
        bad = httpx.Request(
            "POST", EXACT_PIPELINE,
            json={"requests": [
                {"type": "execute", "stmt": {"sql": "DELETE FROM stories"}},
                {"type": "close"},
            ]},
        )
        with self.assertRaises(PermissionError):
            guard.handle_request(bad)
        with self.assertRaises(PermissionError):
            guard.handle_request(httpx.Request("GET", EXACT_PIPELINE))
        with self.assertRaises(PermissionError):
            guard.handle_request(httpx.Request("POST", "https://other.turso.io/v2/pipeline", json={}))
        self.assertEqual(guard.sent, 0)
        self.assertEqual(called, [])

    def test_request_ceiling_is_hard_even_for_selects(self):
        seen = []
        guard = Live01ReadOnlyTransport(
            httpx.MockTransport(lambda req: seen.append(req) or httpx.Response(200, json={})),
            max_requests=1,
        )
        request = httpx.Request("POST", EXACT_PIPELINE, json={
            "requests": [
                {"type": "execute", "stmt": {"sql": "SELECT 1 AS found"}},
                {"type": "close"},
            ],
        })
        guard.handle_request(request)
        with self.assertRaises(PermissionError):
            guard.handle_request(request)
        self.assertEqual(guard.sent, 1)
        self.assertEqual(len(seen), 1)

    def test_elapsed_deadline_blocks_dispatch(self):
        now = [0.0]
        seen = []
        guard = Live01ReadOnlyTransport(
            httpx.MockTransport(lambda req: seen.append(req) or httpx.Response(200)),
            clock=lambda: now[0],
        )
        now[0] = 120.0
        req = httpx.Request("POST", EXACT_PIPELINE, json={
            "requests": [{"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}]
        })
        with self.assertRaises(PermissionError):
            guard.handle_request(req)
        self.assertEqual(guard.sent, 0)
        self.assertEqual(seen, [])

    def test_live_cli_does_not_contact_remote_without_explicit_flag_or_tty(self):
        with patch("efso_radar.staging.live01_readonly.execute_live01_readonly") as probe:
            with patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(main([]), 2)
            self.assertEqual(probe.call_count, 0)
            with patch("sys.stdin", io.StringIO("")):
                with patch("sys.stderr", new_callable=io.StringIO):
                    self.assertEqual(main(["--authorized-readonly-live01"]), 2)
            self.assertEqual(probe.call_count, 0)


if __name__ == "__main__":
    unittest.main()
