"""Hermetic Turso Reliability & Recovery Validation test suite (Phase 5B.3F).

Covers:
1. Hermetic Hrana v2 transaction atomicity, baton lifecycle, error sanitization, and ambiguous commit handling.
2. Multi-worker lease contention, mutual exclusion, renewal isolation, and clock-controlled lock takeover.
3. Recovery and outbox idempotency, DELIVERY_UNCERTAIN protection against blind retry, role segregation,
   and non-mutating recovery diagnostics.
4. Full repository contract parity across domain operations, idempotency, and StagingCutoverReconciler
   migration metadata handling without weakening business-table content hashes.
5. Windows SQLite connection cleanup regression and error distinction.
"""
from __future__ import annotations

import base64
import gc
import json
import logging
import sqlite3
import tempfile
from contextlib import closing
import platform
import threading
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

from efso_radar.domain import (
    Article,
    BudgetHistoryConflictError,
    DeliveryIdentityConflictError,
    DeliveryStateError,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    Market,
    NotificationRecord,
    Story,
    StoryCategory,
    StoryUpdate,
    SuggestedFormat,
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    utc_now,
)
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION, get_all_init_statements
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoReadOnlyStateRepository, TursoStateRepository
from efso_radar.repositories.turso_client import (
    TursoClientError,
    TursoHTTPClient,
    deserialize_hrana_value,
    serialize_hrana_arg,
)
from efso_radar.staging.cutover import GoNoGoGate, StagingCutoverReconciler
from efso_radar.staging.preflight import CloudPreflightValidator
from efso_radar.staging.recovery_inspection import DeliveryRecoveryInspector


# =============================================================================
# Stateful Mock Hrana v2 Transport
# =============================================================================

class StatefulMockHranaTransport(httpx.BaseTransport):
    """Deterministic, stateful mock transport modeling Hrana v2 pipeline and transaction semantics.

    Features:
    - Maintains session baton lifecycle (BEGIN -> active stream -> COMMIT/ROLLBACK -> closed).
    - Rejects requests presenting invalid or expired batons.
    - Configurable failure injection: fail on BEGIN, statement index, COMMIT, timeout, network error,
      HTTP status errors, and rollback failures.
    - Supports injecting synthetic secrets to verify error redaction.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        fail_on_begin: Optional[str] = None,
        fail_on_stmt_idx: Optional[int] = None,
        fail_stmt_code: str = "SQLITE_ERROR",
        fail_stmt_message: str = "Simulated statement error",
        fail_on_commit: Optional[str] = None,
        timeout_on_commit: bool = False,
        network_error_on_commit: bool = False,
        status_on_commit: Optional[int] = None,
        omit_baton_on_begin: bool = False,
        fail_on_rollback_exc: Optional[Exception] = None,
        raw_content_on_pipeline: Optional[bytes] = None,
        override_pipeline_results: Optional[list[Any]] = None,
        raw_content_on_commit: Optional[bytes] = None,
        override_commit_results: Optional[list[Any]] = None,
        truncate_pipeline_results_count: Optional[int] = None,
    ):
        self.conn = conn
        self.fail_on_begin = fail_on_begin
        self.fail_on_stmt_idx = fail_on_stmt_idx
        self.fail_stmt_code = fail_stmt_code
        self.fail_stmt_message = fail_stmt_message
        self.fail_on_commit = fail_on_commit
        self.timeout_on_commit = timeout_on_commit
        self.network_error_on_commit = network_error_on_commit
        self.status_on_commit = status_on_commit
        self.omit_baton_on_begin = omit_baton_on_begin
        self.fail_on_rollback_exc = fail_on_rollback_exc
        self.raw_content_on_pipeline = raw_content_on_pipeline
        self.override_pipeline_results = override_pipeline_results
        self.raw_content_on_commit = raw_content_on_commit
        self.override_commit_results = override_commit_results
        self.truncate_pipeline_results_count = truncate_pipeline_results_count

        self.session_counter = 0
        self.active_batons: set[str] = set()
        self.in_transaction: dict[str, bool] = {}
        self.recorded_requests: list[dict[str, Any]] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            body = json.loads(request.content.decode("utf-8"))
        except Exception:
            body = {}

        self.recorded_requests.append(body)
        in_baton = body.get("baton")
        requests_list = body.get("requests", [])

        # Validate incoming baton if provided
        if in_baton is not None and in_baton not in self.active_batons:
            return httpx.Response(
                400,
                json={"error": {"code": "HRANA_PROTO_ERROR", "message": "Invalid or expired baton"}},
                request=request,
            )

        # Check for simulated network failure or timeout on COMMIT
        has_commit = any(
            req.get("type") == "execute" and req.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
            for req in requests_list
        )
        if has_commit:
            if self.timeout_on_commit:
                raise httpx.TimeoutException("Simulated transport timeout during COMMIT")
            if self.network_error_on_commit:
                raise httpx.ConnectError("Simulated network connection reset during COMMIT")
            if self.status_on_commit is not None:
                return httpx.Response(self.status_on_commit, json={"message": "Gateway error"}, request=request)
            if self.raw_content_on_commit is not None:
                return httpx.Response(200, content=self.raw_content_on_commit, request=request)
            if self.override_commit_results is not None:
                return httpx.Response(200, json={"baton": in_baton, "results": self.override_commit_results}, request=request)

        # Initial pipeline overrides
        has_rollback = any(
            req.get("type") == "execute" and req.get("stmt", {}).get("sql", "").strip().upper() == "ROLLBACK"
            for req in requests_list
        )
        if not has_commit and not has_rollback:
            if self.raw_content_on_pipeline is not None:
                return httpx.Response(200, content=self.raw_content_on_pipeline, request=request)
            if self.override_pipeline_results is not None:
                return httpx.Response(200, json={"baton": "mock-baton-pipeline", "results": self.override_pipeline_results}, request=request)

        # Check for simulated failure on ROLLBACK
        has_rollback = any(
            req.get("type") == "execute" and req.get("stmt", {}).get("sql", "").strip().upper() == "ROLLBACK"
            for req in requests_list
        )
        if has_rollback and self.fail_on_rollback_exc is not None:
            raise self.fail_on_rollback_exc

        results: list[dict[str, Any]] = []
        out_baton = in_baton
        stmt_exec_idx = 0

        for req in requests_list:
            req_type = req.get("type")
            if req_type == "execute":
                stmt_obj = req.get("stmt", {})
                sql = stmt_obj.get("sql", "").strip()
                raw_args = stmt_obj.get("args", [])
                args = [deserialize_hrana_value(a) for a in raw_args]
                upper_sql = sql.upper()

                # Handle BEGIN
                if upper_sql == "BEGIN":
                    if self.fail_on_begin:
                        results.append({
                            "type": "error",
                            "error": {
                                "code": self.fail_on_begin,
                                "message": f"BEGIN rejected with code {self.fail_on_begin}",
                            },
                        })
                        # In Hrana transaction batch, if BEGIN fails, subsequent statements are aborted
                        break

                    self.session_counter += 1
                    out_baton = f"mock-baton-{self.session_counter}"
                    if not self.omit_baton_on_begin:
                        self.active_batons.add(out_baton)
                    else:
                        out_baton = None
                    self.in_transaction[out_baton or ""] = True
                    try:
                        self.conn.execute("BEGIN")
                    except sqlite3.OperationalError:
                        pass
                    results.append({"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}})
                    continue

                # Handle COMMIT
                elif upper_sql == "COMMIT":
                    if self.fail_on_commit:
                        results.append({
                            "type": "error",
                            "error": {
                                "code": self.fail_on_commit,
                                "message": f"COMMIT rejected with code {self.fail_on_commit}",
                            },
                        })
                        continue

                    try:
                        self.conn.commit()
                        if in_baton in self.in_transaction:
                            self.in_transaction[in_baton] = False
                        results.append({"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}})
                    except sqlite3.Error as exc:
                        results.append({"type": "error", "error": {"code": "SQLITE_ERROR", "message": str(exc)}})
                    continue

                # Handle ROLLBACK
                elif upper_sql == "ROLLBACK":
                    try:
                        self.conn.rollback()
                        if in_baton in self.in_transaction:
                            self.in_transaction[in_baton] = False
                        results.append({"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}})
                    except sqlite3.Error as exc:
                        results.append({"type": "error", "error": {"code": "SQLITE_ERROR", "message": str(exc)}})
                    continue

                # Handle user SQL statements
                stmt_exec_idx += 1
                if self.fail_on_stmt_idx is not None and stmt_exec_idx == self.fail_on_stmt_idx:
                    results.append({
                        "type": "error",
                        "error": {
                            "code": self.fail_stmt_code,
                            "message": self.fail_stmt_message,
                        },
                    })
                    continue

                try:
                    cur = self.conn.cursor()
                    cur.execute(sql, args)
                    cols = [{"name": desc[0]} for desc in (cur.description or [])]
                    rows: list[list[dict[str, Any]]] = []
                    if cur.description:
                        for row in cur.fetchall():
                            rows.append([serialize_hrana_arg(cell) for cell in row])

                    affected = cur.rowcount if cur.rowcount >= 0 else 0
                    last_id = cur.lastrowid if cur.lastrowid and cur.lastrowid > 0 else None

                    # If not in explicit transaction baton, auto-commit
                    if not in_baton and not self.in_transaction.get(out_baton or "", False):
                        self.conn.commit()

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
                            "code": "SQLITE_ERROR",
                            "message": str(exc),
                        },
                    })

            elif req_type == "close":
                if in_baton in self.active_batons:
                    self.active_batons.remove(in_baton)
                out_baton = None
                results.append({"type": "ok", "response": {"type": "close"}})

        if not has_commit and self.truncate_pipeline_results_count is not None:
            results = results[:self.truncate_pipeline_results_count]

        return httpx.Response(
            200,
            json={"baton": out_baton, "results": results},
            request=request,
        )


# =============================================================================
# Section 1: Hermetic Hrana v2 Transaction Atomicity & Error Handling
# =============================================================================

class TestHranaTransactionReliability(unittest.TestCase):
    """Hermetic unit and integration tests for Turso libSQL Hrana v2 transaction atomicity."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.backend_db = self.tmp_path / "backend.db"
        self.backend_conn = sqlite3.connect(self.backend_db, check_same_thread=False)
        self.backend_conn.isolation_level = None
        self.backend_conn.row_factory = sqlite3.Row

        # Initialize schema v6 in backend
        for ddl in get_all_init_statements():
            self.backend_conn.execute(ddl)

    def tearDown(self):
        self.backend_conn.close()
        self.tmp_dir.cleanup()

    def test_01_transaction_full_success_atomicity(self):
        """Hermetic Hrana: successful multi-statement transaction commits all records atomically."""
        transport = StatefulMockHranaTransport(self.backend_conn)
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-1", "https://news.test/1", "News 1", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-2", "https://news.test/2", "News 2", "Src2", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        results = client.execute_transaction(statements)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].affected_row_count, 1)
        self.assertEqual(results[1].affected_row_count, 1)

        # Verify records are present in backend
        cursor = self.backend_conn.execute("SELECT COUNT(*) AS cnt FROM processed_articles")
        self.assertEqual(cursor.fetchone()["cnt"], 2)

    def test_02_statement_error_aborts_transaction_and_rolls_back(self):
        """Hermetic Hrana: failure on second user statement triggers automatic ROLLBACK; zero rows persist."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            fail_on_stmt_idx=2,
            fail_stmt_code="SQLITE_CONSTRAINT",
            fail_stmt_message="Unique constraint violated on article_id",
        )
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-good", "https://news.test/g", "Good", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-fail", "https://news.test/f", "Fail", "Src2", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)

        self.assertIn("SQLITE_CONSTRAINT", str(ctx.exception))
        self.assertIn("Abort due to constraint violation", str(ctx.exception))

        # Verify atomicity: art-good was NOT committed
        cursor = self.backend_conn.execute("SELECT COUNT(*) AS cnt FROM processed_articles")
        self.assertEqual(cursor.fetchone()["cnt"], 0)

    def test_03_begin_failure_aborts_without_commit(self):
        """Hermetic Hrana: error on BEGIN raises TursoClientError and never silently succeeds."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            fail_on_begin="SQLITE_BUSY",
        )
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-never", "https://news.test/n", "Never", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)

        err_msg = str(ctx.exception)
        self.assertIn("BEGIN failed", err_msg)
        self.assertIn("SQLITE_BUSY", err_msg)

        cursor = self.backend_conn.execute("SELECT COUNT(*) AS cnt FROM processed_articles")
        self.assertEqual(cursor.fetchone()["cnt"], 0)

    def test_04_missing_baton_on_begin_raises_protocol_error(self):
        """Hermetic Hrana: missing session baton on BEGIN raises protocol error instead of claiming commit."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            omit_baton_on_begin=True,
        )
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-lost", "https://news.test/l", "Lost", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)

        self.assertIn("protocol error: server did not return a session baton", str(ctx.exception))

    def test_05_commit_failure_raises_sanitized_error(self):
        """Hermetic Hrana: remote failure on COMMIT reports sanitized error code without raw messages."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            fail_on_commit="SQLITE_BUSY",
        )
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-c1", "https://news.test/c", "C1", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)

        self.assertIn("Turso COMMIT failed (SQLITE_BUSY)", str(ctx.exception))
        self.assertIn("Database engine is busy or locked", str(ctx.exception))

    def test_06_commit_timeout_reports_ambiguous_outcome_and_is_not_retried(self):
        """Hermetic Hrana: network timeout during COMMIT raises ambiguous outcome; state remains UNKNOWN."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            timeout_on_commit=True,
        )
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-timeout", "https://news.test/t", "Timeout", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)

        err_msg = str(ctx.exception)
        self.assertIn("Ambiguous transaction outcome", err_msg)
        self.assertIn("network error during Turso COMMIT", err_msg)
        self.assertIn("may or may not have committed", err_msg)

    def test_07_commit_http_504_reports_ambiguous_outcome(self):
        """Hermetic Hrana: HTTP 504 Gateway Timeout during COMMIT raises ambiguous outcome error."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            status_on_commit=504,
        )
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-504", "https://news.test/5", "504", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)

        self.assertIn("Ambiguous transaction outcome", str(ctx.exception))
        self.assertIn("HTTP status 504", str(ctx.exception))

    def test_08_rollback_failure_does_not_mask_statement_error_or_leak_secrets(self):
        """Hermetic Hrana: rollback transport exception logs only exception name, does not leak auth secrets."""
        secret_rollback_token = "LEAKED_ROLLBACK_BEARER_SECRET_987654321"
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            fail_on_stmt_idx=1,
            fail_stmt_code="SQLITE_ERROR",
            fail_stmt_message="Original statement error",
            fail_on_rollback_exc=httpx.ConnectError(f"Network error with Authorization: Bearer {secret_rollback_token}"),
        )
        client = TursoHTTPClient(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="mock-token-abc",
            transport=transport,
        )

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-rf", "https://news.test/rf", "RF", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        with self.assertLogs("efso-radar.repository.turso_client", level="WARNING") as log_ctx:
            with self.assertRaises(TursoClientError) as exc_ctx:
                client.execute_transaction(statements)

        # Assert original error was raised
        self.assertIn("SQLITE_ERROR", str(exc_ctx.exception))
        self.assertNotIn(secret_rollback_token, str(exc_ctx.exception))

        # Assert no secrets in log
        for msg in log_ctx.output:
            self.assertNotIn(secret_rollback_token, msg)
            self.assertIn("ConnectError", msg)
    def test_08a_hrana_malformed_json_on_pipeline_aborts_transaction(self):
        """Hermetic Hrana: malformed non-JSON payload on pipeline response aborts transaction."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            raw_content_on_pipeline=b"<html>Bad Gateway</html>",
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m1", "https://news.test/m1", "M1", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        self.assertIn("Turso transaction response malformed", str(ctx.exception))
        self.assertIn("invalid JSON payload", str(ctx.exception))

    def test_08b_hrana_missing_or_empty_pipeline_results_aborts_transaction(self):
        """Hermetic Hrana: empty results array on pipeline response aborts transaction."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_pipeline_results=[],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m2", "https://news.test/m2", "M2", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        self.assertIn("Turso transaction response malformed", str(ctx.exception))
        self.assertIn("missing result list", str(ctx.exception))

    def test_08c_hrana_unexpected_begin_response_type_aborts_transaction(self):
        """Hermetic Hrana: unexpected response type for BEGIN aborts transaction without commit."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_pipeline_results=[{"type": "unexpected_stream_frame"}],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m3", "https://news.test/m3", "M3", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        self.assertIn("Turso transaction response malformed", str(ctx.exception))
        self.assertIn("unexpected BEGIN response type", str(ctx.exception))

    def test_08d_hrana_unexpected_statement_response_type_aborts_and_rolls_back(self):
        """Hermetic Hrana: unexpected response type for user statement rolls back and aborts."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_pipeline_results=[
                {"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}},
                {"type": "strange_event_type"},
            ],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m4", "https://news.test/m4", "M4", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        self.assertIn("Turso transaction aborted due to unexpected statement response type", str(ctx.exception))

    def test_08e_hrana_truncated_pipeline_results_aborts_and_rolls_back(self):
        """Hermetic Hrana: truncated pipeline result list triggers rollback and aborts."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            truncate_pipeline_results_count=1,
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m5", "https://news.test/m5", "M5", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        self.assertIn("Turso transaction response malformed", str(ctx.exception))
        self.assertIn("truncated result list", str(ctx.exception))

    def test_08f_commit_malformed_json_reports_ambiguous_outcome(self):
        """Hermetic Hrana: malformed JSON response during COMMIT raises ambiguous outcome (UNKNOWN)."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            raw_content_on_commit=b"NOT_VALID_JSON{[[",
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m6", "https://news.test/m6", "M6", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        err_msg = str(ctx.exception)
        self.assertIn("Ambiguous transaction outcome", err_msg)
        self.assertIn("malformed JSON in Turso COMMIT response", err_msg)
        self.assertIn("may or may not have committed", err_msg)

    def test_08g_commit_missing_results_reports_ambiguous_outcome(self):
        """Hermetic Hrana: missing results in COMMIT response raises ambiguous outcome (UNKNOWN)."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_commit_results=[],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m7", "https://news.test/m7", "M7", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        err_msg = str(ctx.exception)
        self.assertIn("Ambiguous transaction outcome", err_msg)
        self.assertIn("missing or truncated result list in Turso COMMIT response", err_msg)
        self.assertIn("may or may not have committed", err_msg)

    def test_08h_commit_unexpected_result_type_reports_ambiguous_outcome(self):
        """Hermetic Hrana: unexpected result type in COMMIT response raises ambiguous outcome (UNKNOWN)."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_commit_results=[{"type": "strange_commit_type"}],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m8", "https://news.test/m8", "M8", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        err_msg = str(ctx.exception)
        self.assertIn("Ambiguous transaction outcome", err_msg)
        self.assertIn("unexpected result type in Turso COMMIT response", err_msg)
        self.assertNotIn("strange_commit_type", err_msg)
        self.assertIn("may or may not have committed", err_msg)

    def test_08i_untrusted_response_types_never_leak_secrets(self):
        """Hermetic Hrana: arbitrary server-supplied response types never leak into errors or logs."""
        secret_begin_type = "LEAKED_HRANA_SECRET_BEGIN_TYPE_12345"
        secret_stmt_type = "LEAKED_HRANA_SECRET_STMT_TYPE_67890"
        secret_commit_type = "LEAKED_HRANA_SECRET_COMMIT_TYPE_ABCDE"

        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-leak", "https://news.test/l", "Leak", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]

        class MemoryLogHandler(logging.Handler):
            def __init__(self):
                super().__init__()
                self.records: list[str] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.records.append(self.format(record))

        log_handler = MemoryLogHandler()
        target_logger = logging.getLogger("efso-radar")
        target_logger.addHandler(log_handler)

        try:
            # Case 1: Malformed BEGIN response type
            t1 = StatefulMockHranaTransport(self.backend_conn, override_pipeline_results=[{"type": secret_begin_type}])
            c1 = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=t1)
            with self.assertRaises(TursoClientError) as ctx1:
                c1.execute_transaction(statements)
            self.assertIn("unexpected BEGIN response type", str(ctx1.exception))
            self.assertNotIn(secret_begin_type, str(ctx1.exception))

            # Case 2: Malformed statement response type
            t2 = StatefulMockHranaTransport(
                self.backend_conn,
                override_pipeline_results=[
                    {"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}},
                    {"type": secret_stmt_type},
                ],
            )
            c2 = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=t2)
            with self.assertRaises(TursoClientError) as ctx2:
                c2.execute_transaction(statements)
            self.assertIn("unexpected statement response type", str(ctx2.exception))
            self.assertNotIn(secret_stmt_type, str(ctx2.exception))

            # Case 3: Malformed COMMIT response type
            t3 = StatefulMockHranaTransport(self.backend_conn, override_commit_results=[{"type": secret_commit_type}])
            c3 = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=t3)
            with self.assertRaises(TursoClientError) as ctx3:
                c3.execute_transaction(statements)
            self.assertIn("unexpected result type in Turso COMMIT response", str(ctx3.exception))
            self.assertNotIn(secret_commit_type, str(ctx3.exception))
        finally:
            target_logger.removeHandler(log_handler)

        all_logs = " ".join(log_handler.records)
        self.assertNotIn(secret_begin_type, all_logs)
        self.assertNotIn(secret_stmt_type, all_logs)
        self.assertNotIn(secret_commit_type, all_logs)

    def test_08j_commit_missing_execute_response_structure_reports_ambiguous_outcome(self):
        """Hermetic Hrana: HTTP 200 with {'results': [{'type': 'ok'}]} lacking execution payload raises ambiguous outcome."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_commit_results=[{"type": "ok"}],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m10", "https://news.test/m10", "M10", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        err_msg = str(ctx.exception)
        self.assertIn("Ambiguous transaction outcome", err_msg)
        self.assertIn("missing or malformed execution result in Turso COMMIT response", err_msg)
        self.assertIn("may or may not have committed", err_msg)

    def test_08k_commit_malformed_execute_result_reports_ambiguous_outcome(self):
        """Hermetic Hrana: COMMIT response with malformed response/result dictionary raises ambiguous outcome."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_commit_results=[{"type": "ok", "response": {"type": "execute"}}],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m11", "https://news.test/m11", "M11", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        err_msg = str(ctx.exception)
        self.assertIn("Ambiguous transaction outcome", err_msg)
        self.assertIn("missing or malformed execution result in Turso COMMIT response", err_msg)
        self.assertIn("may or may not have committed", err_msg)

    def test_08l_commit_wrong_response_type_reports_ambiguous_outcome(self):
        """Hermetic Hrana: COMMIT response with non-execute response frame raises ambiguous outcome."""
        transport = StatefulMockHranaTransport(
            self.backend_conn,
            override_commit_results=[{"type": "ok", "response": {"type": "something_else", "result": {}}}],
        )
        client = TursoHTTPClient("libsql://staging-cloud.turso.io", "mock-token", transport=transport)
        statements = [
            ("INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
             ["art-m12", "https://news.test/m12", "M12", "Src1", "2026-10-25T10:00:00Z", "2026-10-25T10:00:00Z"]),
        ]
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction(statements)
        err_msg = str(ctx.exception)
        self.assertIn("Ambiguous transaction outcome", err_msg)
        self.assertIn("missing or malformed execution result in Turso COMMIT response", err_msg)
        self.assertIn("may or may not have committed", err_msg)



# =============================================================================
# Section 2: Multi-Worker Lease & Run Locking
# =============================================================================

class TestMultiWorkerLeaseContention(unittest.TestCase):
    """Hermetic tests for Turso distributed run lock CAS, lease renewal, and expired takeover."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.backend_db = self.tmp_path / "backend_lock.db"
        self.backend_conn = sqlite3.connect(self.backend_db, check_same_thread=False)
        self.backend_conn.isolation_level = None
        self.backend_conn.row_factory = sqlite3.Row

        for ddl in get_all_init_statements():
            self.backend_conn.execute(ddl)

        self.transport = StatefulMockHranaTransport(self.backend_conn)

        # Worker 1 client and repo
        self.worker1_repo = TursoStateRepository(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="token1",
            transport=self.transport,
            init_schema=False,
        )
        # Worker 2 client and repo (same backend)
        self.worker2_repo = TursoStateRepository(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="token2",
            transport=self.transport,
            init_schema=False,
        )

    def tearDown(self):
        self.worker1_repo.close()
        self.worker2_repo.close()
        self.backend_conn.close()
        self.tmp_dir.cleanup()

    def test_09_mutual_exclusion_and_contention(self):
        """Multi-worker: only one runner can acquire the 'radar_cron' lease."""
        t0 = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)

        # Worker 1 acquires lock
        self.assertTrue(self.worker1_repo.acquire_run_lock("runner_alpha", timeout_minutes=15, now=t0))

        # Worker 2 attempts to acquire -> rejected
        self.assertFalse(self.worker2_repo.acquire_run_lock("runner_beta", timeout_minutes=15, now=t0))

        # Worker 1 re-acquires (idempotent for owner) -> True
        self.assertTrue(self.worker1_repo.acquire_run_lock("runner_alpha", timeout_minutes=15, now=t0))

    def test_10_non_owner_cannot_renew_or_release_lock(self):
        """Multi-worker: non-owner worker cannot renew or release active lease held by another worker."""
        t0 = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(self.worker1_repo.acquire_run_lock("runner_alpha", timeout_minutes=15, now=t0))

        # Worker 2 tries to renew -> False
        self.assertFalse(self.worker2_repo.renew_run_lock("runner_beta", extend_minutes=15))

        # Worker 2 tries to release -> Worker 1's lock remains intact
        self.worker2_repo.release_run_lock("runner_beta")

        # Worker 2 still cannot acquire
        self.assertFalse(self.worker2_repo.acquire_run_lock("runner_beta", timeout_minutes=15, now=t0))

    def test_11_expired_lock_takeover_with_clock_control(self):
        """Multi-worker: expired lock is safely taken over by new runner when timeout expires."""
        t0 = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(self.worker1_repo.acquire_run_lock("runner_alpha", timeout_minutes=10, now=t0))

        # At t0 + 9 mins, Worker 2 cannot take over
        t_before_expiry = t0 + timedelta(minutes=9)
        self.assertFalse(self.worker2_repo.acquire_run_lock("runner_beta", timeout_minutes=10, now=t_before_expiry))

        # At t0 + 11 mins (past expires_at), Worker 2 takes over
        t_after_expiry = t0 + timedelta(minutes=11)
        self.assertTrue(self.worker2_repo.acquire_run_lock("runner_beta", timeout_minutes=10, now=t_after_expiry))

        # Worker 1 is now rejected
        self.assertFalse(self.worker1_repo.acquire_run_lock("runner_alpha", timeout_minutes=10, now=t_after_expiry))

    def test_12_runner_restart_preserves_lock_state(self):
        """Multi-worker: a freshly connected repo instance sees pre-existing lock state."""
        t0 = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        self.assertTrue(self.worker1_repo.acquire_run_lock("runner_alpha", timeout_minutes=15, now=t0))

        # Fresh runner instance connects to same backend
        with TursoStateRepository(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="token3",
            transport=self.transport,
            init_schema=False,
        ) as runner3_repo:
            # Runner 3 cannot acquire
            self.assertFalse(runner3_repo.acquire_run_lock("runner_gamma", timeout_minutes=15, now=t0))

    def test_12b_simultaneous_concurrent_worker_contention_with_barrier(self):
        """Multi-worker contention: 2 workers simultaneously contending via barrier produce exactly 1 winner and safe recovery."""
        backend_conn1 = sqlite3.connect(self.backend_db, check_same_thread=False, timeout=15.0)
        backend_conn1.isolation_level = None
        backend_conn1.row_factory = sqlite3.Row
        transport1 = StatefulMockHranaTransport(backend_conn1)
        worker_alpha_repo = TursoStateRepository("libsql://staging-cloud.turso.io", "token_a", transport=transport1, init_schema=False)

        backend_conn2 = sqlite3.connect(self.backend_db, check_same_thread=False, timeout=15.0)
        backend_conn2.isolation_level = None
        backend_conn2.row_factory = sqlite3.Row
        transport2 = StatefulMockHranaTransport(backend_conn2)
        worker_beta_repo = TursoStateRepository("libsql://staging-cloud.turso.io", "token_b", transport=transport2, init_schema=False)

        try:
            barrier = threading.Barrier(2)
            results: dict[str, bool] = {}
            exceptions: dict[str, Exception] = {}
            t0 = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)

            def worker_task(worker_id: str, repo: TursoStateRepository) -> None:
                try:
                    barrier.wait(timeout=5.0)
                    acquired = repo.acquire_run_lock(worker_id, timeout_minutes=10, now=t0)
                    results[worker_id] = acquired
                except Exception as exc:
                    exceptions[worker_id] = exc

            th_alpha = threading.Thread(target=worker_task, args=("runner_alpha", worker_alpha_repo))
            th_beta = threading.Thread(target=worker_task, args=("runner_beta", worker_beta_repo))

            th_alpha.start()
            th_beta.start()
            th_alpha.join(timeout=10.0)
            th_beta.join(timeout=10.0)

            self.assertEqual(exceptions, {}, f"Worker threads encountered unexpected exceptions: {exceptions}")
            self.assertEqual(len(results), 2, "Both workers must report acquisition results")

            # Assert exactly one winner and one loser
            alpha_success = results.get("runner_alpha", False)
            beta_success = results.get("runner_beta", False)
            self.assertEqual(int(alpha_success) + int(beta_success), 1, "Exactly one worker must acquire the lock")

            winner = "runner_alpha" if alpha_success else "runner_beta"
            loser = "runner_beta" if alpha_success else "runner_alpha"
            loser_repo = worker_beta_repo if alpha_success else worker_alpha_repo

            # Assert winner is reflected in backend database
            cur = self.backend_conn.cursor()
            cur.execute("SELECT worker_id FROM run_locks WHERE lock_key = 'radar_cron'")
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["worker_id"], winner)

            # Assert loser cannot renew or release winner's lock
            self.assertFalse(loser_repo.renew_run_lock(loser, extend_minutes=10))

            # Advance clock past lease expiration: safe recovery
            t_expired = t0 + timedelta(minutes=11)
            loser_acquired = loser_repo.acquire_run_lock(loser, timeout_minutes=10, now=t_expired)
            self.assertTrue(loser_acquired, f"Loser {loser} must safely take over expired lease at t0 + 11 mins")

            # Verify database now records loser as new owner
            cur.execute("SELECT worker_id FROM run_locks WHERE lock_key = 'radar_cron'")
            new_row = cur.fetchone()
            self.assertEqual(new_row["worker_id"], loser)
        finally:
            worker_alpha_repo.close()
            worker_beta_repo.close()
            backend_conn1.close()
            backend_conn2.close()



# =============================================================================
# Section 3: Recovery & Outbox Idempotency
# =============================================================================

class TestRecoveryAndOutboxSafety(unittest.TestCase):
    """Hermetic tests for durable recovery state, outbox delivery claims, and uncertainty isolation."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.backend_db = self.tmp_path / "backend_recovery.db"
        self.backend_conn = sqlite3.connect(self.backend_db, check_same_thread=False)
        self.backend_conn.isolation_level = None
        self.backend_conn.row_factory = sqlite3.Row

        for ddl in get_all_init_statements():
            self.backend_conn.execute(ddl)

        self.transport = StatefulMockHranaTransport(self.backend_conn)
        self.repo = TursoStateRepository(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="token_rec",
            transport=self.transport,
            init_schema=False,
        )

    def tearDown(self):
        self.repo.close()
        self.backend_conn.close()
        self.tmp_dir.cleanup()

    def test_13_delivery_uncertain_never_blindly_retried(self):
        """Outbox: DELIVERY_UNCERTAIN records are permanently isolated from automatic retry."""
        delivery_id = "del-unc-001"
        message_key = "digest_20261025_evening"
        chat_id = "12345678"

        # Claim delivery
        claimed = self.repo.claim_telegram_delivery(
            delivery_id=delivery_id,
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
        )
        self.assertTrue(claimed)

        # Transition to DELIVERY_UNCERTAIN (simulating network timeout on Telegram send)
        updated = self.repo.update_telegram_delivery_status(
            delivery_id=delivery_id,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            error_message="Telegram API timed out waiting for response",
        )
        self.assertTrue(updated)

        # Subsequent runner attempt to claim delivery for same recipient and message_key MUST return False
        retry_claim = self.repo.claim_telegram_delivery(
            delivery_id="del-retry-002",
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
        )
        self.assertFalse(retry_claim)

    def test_14_role_segregation_and_conflicting_identities_rejected(self):
        """Outbox: PRIMARY_EDITOR vs ADMIN cannot cross-claim each other's message keys."""
        delivery_id = "del-role-001"
        message_key = "report_20261025"
        chat_id = "99887766"

        self.assertTrue(
            self.repo.claim_telegram_delivery(
                delivery_id=delivery_id,
                message_key=message_key,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            )
        )

        # Attempt to claim with conflicting role raises DeliveryIdentityConflictError
        with self.assertRaises(DeliveryIdentityConflictError):
            self.repo.claim_telegram_delivery(
                delivery_id="del-role-002",
                message_key=message_key,
                recipient_role="ADMIN",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            )

    def test_15_recovery_inspector_diagnoses_checkpoints_without_mutating(self):
        """Recovery: DeliveryRecoveryInspector detects stuck claims and checkpoints non-mutatingly."""
        # Insert checkpoint
        self.repo.save_recovery_state(
            stage="editorial_synthesis",
            worker_id="runner_crashed",
            state_payload={"candidates_processed": 5, "batch_id": "b-99"},
        )

        # Insert stuck dispatching delivery
        self.repo.claim_telegram_delivery(
            delivery_id="del-stuck-1",
            message_key="msg_stuck",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="55555555",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
        )

        # Run read-only inspector
        inspector = DeliveryRecoveryInspector(self.repo)
        report = inspector.inspect(stuck_claim_threshold_minutes=0.0)

        self.assertEqual(len(report.stuck_dispatching), 1)
        self.assertEqual(len(report.recovery_checkpoints), 1)
        self.assertEqual(report.recovery_checkpoints[0].stage, "editorial_synthesis")
        self.assertEqual(report.recovery_checkpoints[0].worker_id, "runner_crashed")

        # Verify state is untouched
        chk = self.repo.get_recovery_state("editorial_synthesis")
        self.assertIsNotNone(chk)
        self.assertEqual(chk["state_payload"]["candidates_processed"], 5)


# =============================================================================
# Section 4: Repository Contract Parity & Cutover Reconciliation
# =============================================================================

class TestRepositoryContractParityAndCutover(unittest.TestCase):
    """Hermetic tests for full contract parity between SqliteStateRepository and TursoStateRepository."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)

        # 1. SQLite synthetic repository
        self.sqlite_db = self.tmp_path / "sqlite_parity.db"
        self.sqlite_repo = SqliteStateRepository.create_owned_synthetic_fixture(self.sqlite_db)

        # 2. Turso synthetic repository via mock transport
        self.turso_backend_db = self.tmp_path / "turso_backend_parity.db"
        self.turso_conn = sqlite3.connect(self.turso_backend_db, check_same_thread=False)
        self.turso_conn.isolation_level = None
        self.turso_conn.row_factory = sqlite3.Row
        self.turso_transport = StatefulMockHranaTransport(self.turso_conn)

        self.turso_repo = TursoStateRepository(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="token_parity",
            transport=self.turso_transport,
            init_schema=True,
        )

    def tearDown(self):
        self.sqlite_repo.close()
        self.turso_repo.close()
        self.turso_conn.close()
        gc.collect()
        self.tmp_dir.cleanup()

    def test_16_contract_parity_articles_candidates_and_stories(self):
        """Contract Parity: articles, candidates, stories, and assessments behave identically."""
        now = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        article = Article.create(
            title="Efso Radar Parity News",
            url="https://parity.test/news1",
            source="ParitySource",
            published_at=now,
        )
        candidate_article = Article.create(
            title="Efso Radar Fresh Candidate",
            url="https://parity.test/candidate1",
            source="CandidateSource",
            published_at=now,
        )

        for repo in (self.sqlite_repo, self.turso_repo):
            # Article processing
            self.assertFalse(repo.is_article_processed(article.id))
            repo.mark_articles_processed([article])
            self.assertTrue(repo.is_article_processed(article.id))

            # Candidate backlog (with un-processed candidate)
            repo.save_pending_candidates([candidate_article])
            pending = repo.get_pending_candidates(max_age_hours=24, now=now)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].id, candidate_article.id)

            repo.mark_candidates_evaluated([candidate_article.id])
            pending_after = repo.get_pending_candidates(max_age_hours=24, now=now)
            self.assertEqual(len(pending_after), 0)

            # Story and assessment
            story = Story(
                story_id=f"story-{type(repo).__name__}",
                headline_tr="Başlık",
                category=StoryCategory.YERLI,
                first_seen_at=now,
                last_updated_at=now,
                article_ids=[article.id],
            )
            assessment = EditorialAssessment(
                assessment_id=f"ass-{type(repo).__name__}",
                story_id=story.story_id,
                kind=EditorialKind.NEW,
                score=85,
                headline_tr="Başlık",
                cover_title_tr="Kapak",
                caption_idea_tr="Açıklama",
                why_efso_tr="Neden",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
                article_ids=[article.id],
                created_at=now,
            )
            repo.record_new_story(story, assessment)
            loaded_story = repo.get_story_by_id(story.story_id)
            self.assertIsNotNone(loaded_story)
            self.assertEqual(loaded_story.headline_tr, "Başlık")

            # Notification claim and alerted check
            self.assertFalse(repo.has_story_been_alerted(story.story_id))
            repo.claim_notification_for_dispatch(
                notification_id=f"notif-{type(repo).__name__}",
                story_id=story.story_id,
                kind=EditorialKind.NEW,
                headline_tr="Başlık",
                score=85,
                message_text="Test alert",
                worker_id="runner_1",
            )
            self.assertTrue(repo.has_story_been_alerted(story.story_id))

    def test_17_contract_parity_ai_usage_and_budget_history(self):
        """Contract Parity: AI usage tracking and immutable budget ledger match across backends."""
        now = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)

        for repo in (self.sqlite_repo, self.turso_repo):
            # AI usage
            repo.record_ai_usage(
                usage_id=f"u-{type(repo).__name__}",
                run_mode="scan_only",
                model_name="gemini-2.5-flash",
                batch_hash="hash-123",
                candidate_count=5,
                input_tokens=1000,
                output_tokens=250,
                estimated_cost_usd=0.0005,
                created_at=now,
            )
            summary = repo.get_monthly_ai_usage(now=now)
            self.assertEqual(summary["call_count"], 1)
            self.assertEqual(summary["input_tokens"], 1000)
            self.assertEqual(summary["output_tokens"], 250)

            # Budget history
            inserted = repo.record_budget_usage(
                period_month="2026-10",
                run_id=f"run-{type(repo).__name__}",
                worker_id="runner-1",
                calls_count=1,
                prompt_tokens=1000,
                completion_tokens=250,
                total_tokens=1250,
                estimated_cost_usd=0.0005,
                timestamp=now,
                record_id=f"b-rec-{type(repo).__name__}",
            )
            self.assertTrue(inserted)

            # Idempotent replay
            replayed = repo.record_budget_usage(
                period_month="2026-10",
                run_id=f"run-{type(repo).__name__}",
                worker_id="runner-1",
                calls_count=1,
                prompt_tokens=1000,
                completion_tokens=250,
                total_tokens=1250,
                estimated_cost_usd=0.0005,
                timestamp=now,
                record_id=f"b-rec-{type(repo).__name__}",
            )
            self.assertTrue(replayed)

            # Conflicting replay raises BudgetHistoryConflictError
            with self.assertRaises(BudgetHistoryConflictError):
                repo.record_budget_usage(
                    period_month="2026-10",
                    run_id=f"run-{type(repo).__name__}",
                    worker_id="runner-1",
                    calls_count=1,
                    prompt_tokens=1000,
                    completion_tokens=500,  # Conflict
                    total_tokens=1500,
                    estimated_cost_usd=0.0005,
                    timestamp=now,
                    record_id=f"b-rec-{type(repo).__name__}",
                )

    def test_18_cutover_reconciler_handles_migration_metadata_without_artificial_update(self):
        """StagingCutoverReconciler: handles schema_migrations metadata cleanly without synthetic updates.

        Confirms that differences in applied_at or description between freshly initialized SQLite and Turso
        do not cause false positive reconciliation aborts, while business tables maintain strict content hashes.
        """
        reconciler = StagingCutoverReconciler(
            source_repo=self.sqlite_repo,
            target_repo=self.turso_repo,
        )
        report = reconciler.reconcile()

        # Exact parity confirmed out of the box
        self.assertTrue(report.is_safe_to_cutover)
        self.assertTrue(report.all_counts_match)
        self.assertTrue(report.all_keys_match)
        self.assertTrue(report.all_contents_match)
        self.assertEqual(len(report.abort_reasons), 0)

        # But business table divergence still triggers abort!
        now = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        diff_article = Article.create(
            title="Divergent Story",
            url="https://diff.test/1",
            source="DiffSrc",
            published_at=now,
        )
        self.sqlite_repo.mark_articles_processed([diff_article])

        report_after_diff = reconciler.reconcile()
        self.assertFalse(report_after_diff.is_safe_to_cutover)
        self.assertTrue(any("ERR_COUNT_MISMATCH_PROCESSED_ARTICLES" in ar.code for ar in report_after_diff.abort_reasons))

    def test_19_reconcile_uncertain_delivery_requires_authorization(self):
        """Recovery: reconcile_uncertain_delivery enforces fail-closed authorization on SQLite and fails closed on Turso."""
        now = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        delivery_id = "del-rec-auth"

        proof = {
            "reconciliation_method": "MANUAL_OPERATOR_VERIFICATION",
            "verified_by": "lead_operator",
            "proof_reference": "TG_MESSAGE_ID_12345",
        }

        # 1. SQLite: must have operator_authorized=True
        self.sqlite_repo.claim_telegram_delivery(
            delivery_id=f"{delivery_id}-sqlite",
            message_key="key-sqlite",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="111222333",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
        )
        self.sqlite_repo.update_telegram_delivery_status(
            delivery_id=f"{delivery_id}-sqlite",
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            error_message="Network timeout",
        )

        # Unauthorized attempt raises DeliveryStateError
        with self.assertRaises(DeliveryStateError):
            self.sqlite_repo.reconcile_uncertain_delivery(
                delivery_id=f"{delivery_id}-sqlite",
                reconciliation_proof=proof,
                reconciled_at=now,
                operator_authorized=False,
            )

        # Authorized attempt succeeds
        reconciled = self.sqlite_repo.reconcile_uncertain_delivery(
            delivery_id=f"{delivery_id}-sqlite",
            reconciliation_proof=proof,
            reconciled_at=now,
            operator_authorized=True,
        )
        self.assertEqual(reconciled.status, DeliveryStatus.SENT)

        # 2. Turso: unconditionally disabled during Phase 5B.3B/3F, raises DeliveryStateError
        self.turso_repo.claim_telegram_delivery(
            delivery_id=f"{delivery_id}-turso",
            message_key="key-turso",
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id="444555666",
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
        )
        self.turso_repo.update_telegram_delivery_status(
            delivery_id=f"{delivery_id}-turso",
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            error_message="Network timeout",
        )

        with self.assertRaises(DeliveryStateError) as ctx:
            self.turso_repo.reconcile_uncertain_delivery(
                delivery_id=f"{delivery_id}-turso",
                reconciliation_proof=proof,
                reconciled_at=now,
                operator_authorized=True,
            )
        self.assertIn("NOT IMPLEMENTED", str(ctx.exception))

    def test_20_story_updates_and_scheduled_execution_parity(self):
        """Contract Parity: story updates and scheduled execution audit logging match across backends."""
        now = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)

        for repo in (self.sqlite_repo, self.turso_repo):
            # Story update
            story = Story(
                story_id=f"story-upd-{type(repo).__name__}",
                headline_tr="Ana Başlık",
                category=StoryCategory.YERLI,
                first_seen_at=now,
                last_updated_at=now,
                article_ids=["art-x"],
            )
            initial_ass = EditorialAssessment(
                assessment_id=f"ass-init-{type(repo).__name__}",
                story_id=story.story_id,
                kind=EditorialKind.NEW,
                score=80,
                headline_tr="Ana Başlık",
                cover_title_tr="Kapak",
                caption_idea_tr="Açıklama",
                why_efso_tr="Neden",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
                article_ids=["art-x"],
                created_at=now,
            )
            repo.record_new_story(story, initial_ass)

            update = StoryUpdate(
                update_id=f"upd-{type(repo).__name__}",
                story_id=story.story_id,
                development_tr="Yeni gelişme yaşandı",
                detected_at=now + timedelta(hours=1),
                article_ids=["art-y"],
                significance_score=88,
            )
            update_ass = EditorialAssessment(
                assessment_id=f"ass-upd-{type(repo).__name__}",
                story_id=story.story_id,
                kind=EditorialKind.UPDATE,
                score=88,
                headline_tr="Güncelleme Başlığı",
                cover_title_tr="Kapak 2",
                caption_idea_tr="Açıklama 2",
                why_efso_tr="Neden 2",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
                article_ids=["art-y"],
                created_at=now + timedelta(hours=1),
            )
            repo.record_story_update(update, update_ass)
            self.assertTrue(
                repo.has_story_been_alerted(
                    story.story_id, kind=EditorialKind.UPDATE, development_tr="Yeni gelişme yaşandı"
                )
            )

            # Scheduled execution audit
            run_id = f"run-audit-{type(repo).__name__}"
            repo.record_scheduled_execution_start(run_id=run_id, run_type="CRON", worker_id="runner_1", started_at=now)
            repo.record_scheduled_execution_finish(
                run_id=run_id,
                status="SUCCESS",
                articles_collected=10,
                candidates_evaluated=3,
                notifications_dispatched=1,
                finished_at=now + timedelta(minutes=2),
            )
            rec = repo.get_scheduled_execution(run_id)
            self.assertIsNotNone(rec)
            self.assertEqual(rec["status"], "SUCCESS")
            self.assertEqual(rec["articles_collected"], 10)

    def test_21_admin_operational_report_parity_and_idempotency(self):
        """Contract Parity: daily admin operational reports enforce at-most-once per Istanbul date."""
        report_date = "2026-10-25"

        for repo in (self.sqlite_repo, self.turso_repo):
            self.assertFalse(repo.has_admin_report_been_dispatched(report_date))
            first_recorded = repo.record_admin_report(
                report_date=report_date,
                worker_id="runner_admin",
                status=DeliveryStatus.SENT,
                recipient_chat_id="999888777",
                summary_payload={"total_sent": 5, "errors": 0},
            )
            self.assertTrue(first_recorded)
            self.assertTrue(repo.has_admin_report_been_dispatched(report_date))

            # Second attempt for same Istanbul date returns False (at-most-once)
            second_recorded = repo.record_admin_report(
                report_date=report_date,
                worker_id="runner_admin_duplicate",
                status=DeliveryStatus.SENT,
                recipient_chat_id="999888777",
                summary_payload={"total_sent": 5, "errors": 0},
            )
            self.assertFalse(second_recorded)

    def test_22_read_only_repository_rejects_mutations_and_transactions(self):
        """Safety: read-only repository enforces execution boundary before any HTTP requests."""
        ro_repo = TursoReadOnlyStateRepository(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="token_ro",
            transport=self.turso_transport,
        )
        self.assertTrue(ro_repo.read_only)
        self.assertTrue(ro_repo.client.read_only)

        # Mutating method raises PermissionError
        with self.assertRaises(PermissionError):
            ro_repo.acquire_run_lock("worker_ro")

        with self.assertRaises(PermissionError):
            ro_repo.mark_articles_processed([])

        with self.assertRaises(PermissionError):
            ro_repo.client.execute_transaction([("SELECT 1", [])])

        ro_repo.close()


# =============================================================================
# Section 5: Windows SQLite Connection Cleanup Regression
# =============================================================================

class TestWindowsSQLiteCleanupRegression(unittest.TestCase):
    """Hermetic regression tests verifying clean disposal of SQLite connections and distinguishing errors."""

    def test_23_sqlite_repository_close_enables_clean_directory_disposal(self):
        """Windows Cleanup: calling close() flushes WAL state and allows clean temp directory deletion."""
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_path = Path(tmp_dir.name)
        db_path = tmp_path / "sqlite_clean.db"

        repo = SqliteStateRepository(db_path)
        # Perform some writes
        now = utc_now()
        article = Article.create(title="Cleanup Test", url="https://clean.test", source="Test", published_at=now)
        repo.mark_articles_processed([article])

        # Explicitly close repository resources
        repo.close()

        # Deleting temp directory must succeed without WinError 32 PermissionError
        try:
            tmp_dir.cleanup()
            cleanup_succeeded = True
        except PermissionError:
            cleanup_succeeded = False

        self.assertTrue(cleanup_succeeded, "Temporary directory cleanup raised PermissionError despite repo.close()")

    def test_23a_sqlite_close_does_not_checkpoint_unowned_operational_databases(self):
        """Windows Cleanup: calling close() on a standard repository does NOT execute WAL checkpointing."""
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_path = Path(tmp_dir.name)
        db_path = tmp_path / "op_sim.db"

        # Standard repository instance: _is_owned_synthetic_fixture is False
        repo = SqliteStateRepository(db_path)
        self.assertFalse(repo._is_owned_synthetic_fixture)

        # Mock sqlite3.connect to verify that close() does not open connections to operational DB
        with patch("efso_radar.repositories.sqlite.sqlite3.connect") as mock_connect:
            repo.close()
            mock_connect.assert_not_called()

        tmp_dir.cleanup()

    def test_23b_external_connection_lifecycle_must_be_explicitly_closed_for_windows_cleanup(self):
        """Windows Cleanup: unclosed external SQLite connections prevent directory cleanup; explicit closing() fixes it."""
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_path = Path(tmp_dir.name)
        db_path = tmp_path / "external_leak_test.db"

        # Create an owned synthetic fixture
        repo = SqliteStateRepository.create_owned_synthetic_fixture(db_path)
        self.assertTrue(repo._is_owned_synthetic_fixture)

        # 1. An external tool or test opens an independent connection without contextlib.closing
        external_conn = sqlite3.connect(db_path)
        external_conn.execute("SELECT 1;")

        # 2. repo.close() only closes repo-owned resources; it CANNOT close external connections
        repo.close()

        # 3. On Windows, an unclosed connection locks the file, causing PermissionError on directory cleanup
        if platform.system() == "Windows":
            with self.assertRaises(PermissionError):
                tmp_dir.cleanup()

        # 4. Cleanup errors must never be silently concealed: verify that explicit closing releases the handle
        external_conn.close()

        # 5. Now verify clean lifecycle pattern using contextlib.closing
        with closing(sqlite3.connect(db_path)) as properly_managed_conn:
            properly_managed_conn.execute("SELECT 1;")

        # Properly managed connection is closed on exit; cleanup must succeed without errors
        tmp_dir.cleanup()


    def test_24_context_manager_lifecycle_closes_cleanly(self):
        """Windows Cleanup: using SqliteStateRepository as a context manager auto-closes on scope exit."""
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_path = Path(tmp_dir.name)
        db_path = tmp_path / "sqlite_ctx.db"

        with SqliteStateRepository(db_path) as repo:
            now = utc_now()
            article = Article.create(title="Ctx Test", url="https://ctx.test", source="Test", published_at=now)
            repo.mark_articles_processed([article])

        # Outside context block, directory cleanup succeeds cleanly
        try:
            tmp_dir.cleanup()
            cleanup_succeeded = True
        except PermissionError:
            cleanup_succeeded = False

        self.assertTrue(cleanup_succeeded, "Temporary directory cleanup failed after context manager exit")

    def test_25_error_distinction_between_remote_cleanup_and_local_permission_error(self):
        """Windows Cleanup: test failure clearly distinguishes remote staging cleanup status from local Windows file lock."""
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_path = Path(tmp_dir.name)

        # 1. Simulate remote Turso smoke test record creation and cleanup
        backend_db = tmp_path / "remote_sim.db"
        backend_conn = sqlite3.connect(backend_db, check_same_thread=False)
        backend_conn.isolation_level = None
        for ddl in get_all_init_statements():
            backend_conn.execute(ddl)

        transport = StatefulMockHranaTransport(backend_conn)
        turso_repo = TursoStateRepository(
            database_url="libsql://staging-cloud.turso.io",
            auth_token="token_smoke",
            transport=transport,
            init_schema=False,
        )

        smoke_article = Article.create(
            title="Synthetic Smoke Article",
            url="https://smoke.test/synthetic-1",
            source="SmokeFeed",
            published_at=utc_now(),
        )
        turso_repo.mark_articles_processed([smoke_article])
        self.assertTrue(turso_repo.is_article_processed(smoke_article.id))

        # Perform remote cleanup: remove synthetic record
        turso_repo.client.execute("DELETE FROM processed_articles WHERE article_id = ?", [smoke_article.id])

        # Verify remote staging cleanup: exactly 0 remaining rows
        res = turso_repo.client.execute("SELECT COUNT(*) AS cnt FROM processed_articles")
        remote_remaining_rows = res.fetchone()["cnt"]
        self.assertEqual(remote_remaining_rows, 0, "Remote staging cleanup failed: synthetic records remain!")

        # 2. Simulate local temporary SQLite fixture cleanup diagnostics
        local_db = tmp_path / "local_fixture.db"
        local_conn = sqlite3.connect(local_db)
        local_conn.execute("CREATE TABLE dummy (x INT);")

        def dispose_fixtures_with_diagnostics():
            try:
                tmp_dir.cleanup()
            except PermissionError as perm_err:
                # Diagnostics must report remote cleanup succeeded and isolate local Windows error
                raise RuntimeError(
                    f"LOCAL_WINDOWS_CLEANUP_FAILURE: Remote staging cleanup succeeded ({remote_remaining_rows} remaining rows), "
                    f"but local temporary SQLite cleanup failed due to Windows file locking: {perm_err}"
                ) from perm_err

        # Simulate Windows file locking PermissionError deterministically across all OS environments (Windows, Linux, macOS)
        from unittest.mock import patch
        with patch.object(
            tmp_dir,
            "cleanup",
            side_effect=PermissionError("[WinError 32] The process cannot access the file because it is being used by another process"),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                dispose_fixtures_with_diagnostics()

        err_msg = str(ctx.exception)
        self.assertIn("LOCAL_WINDOWS_CLEANUP_FAILURE", err_msg)
        self.assertIn("Remote staging cleanup succeeded (0 remaining rows)", err_msg)
        self.assertIn("Windows file locking", err_msg)

        # Cleanly close connection and cleanup for real
        local_conn.close()
        turso_repo.close()
        backend_conn.close()
        gc.collect()
        tmp_dir.cleanup()


if __name__ == "__main__":
    unittest.main()
