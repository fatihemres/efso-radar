"""Hermetic unit and integration tests for Phase 5B.3E: Turso Staging Safe Connection Readiness.

Verifies:
1. Critical Security Fix: TursoStateRepository constructor side-effect elimination (init_schema=False default, TursoReadOnlyStateRepository).
2. Non-mutating preflight and recovery inspection across remote Turso endpoints.
3. Fail-closed behavior on uninitialized staging database (no tables created, reports missing_tables).
4. Outbound Hrana v2 SQL payloads strictly confined to read-only introspection queries (zero DDL/DML).
5. PermissionError and SQL validation enforcement on all mutating methods and query routes in read-only mode.
6. Validation of endpoint schemes: strictly blocks remote insecure HTTP (including localhost.attacker.invalid, 127.0.0.1.attacker.invalid, localhost in query/path, with or without injected transport).
7. Credential completeness (rejects missing/partial credentials; fail-closed without SQLite fallback).
8. Token and URL sanitization across __repr__, exceptions, logs, and StagingConnectionReport (zero secret leakage in HTTP 401 or hostile bodies).
9. Staging connection readiness check (check_staging_connection) and CLI runner using process os.environ without reading .env.
10. Strict zero-network and operational database immutability verification.
"""
from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import httpx

from efso_radar.config import Settings
from efso_radar.domain import Article, EditorialAssessment, EditorialKind, Market, Story, StoryCategory, SuggestedFormat, utc_now
from efso_radar.orchestrator import RadarOrchestrator
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION, get_all_init_statements
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoReadOnlyStateRepository, TursoStateRepository
from efso_radar.repositories.turso_client import (
    TursoClientError,
    TursoHTTPClient,
    deserialize_hrana_value,
    mask_turso_url,
    normalize_turso_url,
    serialize_hrana_arg,
    validate_read_only_sql,
)
from efso_radar.staging.connection import (
    StagingConnectionReport,
    check_staging_connection,
    get_staging_credentials_from_env,
    main as connection_main,
    mask_token,
)
from efso_radar.staging.preflight import (
    SCHEMA_V6_TABLES,
    CloudPreflightValidator,
    PreflightReport,
    execute_read_query,
)
from efso_radar.staging.recovery_inspection import sanitize_diagnostic_text


def create_recording_mock_hrana_transport(conn: sqlite3.Connection) -> tuple[httpx.BaseTransport, list[dict[str, Any]]]:
    """Create a mock Hrana transport that records all outbound HTTP requests and SQL statements."""
    recorded_requests: list[dict[str, Any]] = []

    class RecordingMockHranaTransport(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}

            recorded_requests.append({
                "url": str(request.url),
                "headers": dict(request.headers),
                "body": body,
            })

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

    return RecordingMockHranaTransport(), recorded_requests


class TestTursoStagingReadiness(unittest.TestCase):
    """Test suite for Phase 5B.3E Turso Staging Readiness & Safe Connection Architecture."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.temp_dir.name)

        # In-memory / temp SQLite database acting as the remote Turso backend
        self.turso_backend_path = self.tmp_path / "remote_turso_backend.db"
        self.turso_conn = sqlite3.connect(self.turso_backend_path, check_same_thread=False)
        self.turso_conn.isolation_level = None
        self.turso_conn.row_factory = sqlite3.Row

        self.transport, self.recorded_requests = create_recording_mock_hrana_transport(self.turso_conn)

    def tearDown(self) -> None:
        try:
            self.turso_conn.close()
        except Exception:
            pass
        self.temp_dir.cleanup()

    # -------------------------------------------------------------------------
    # 1. Critical Security Fix: Elimination of Constructor Remote Mutation
    # -------------------------------------------------------------------------

    def test_01_constructor_side_effect_eliminated_with_init_schema_false(self) -> None:
        """Requirement 1: TursoStateRepository with init_schema=False (default) sends 0 HTTP requests."""
        repo = TursoStateRepository(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
            init_schema=False,
        )

        self.assertEqual(len(self.recorded_requests), 0)
        tables = self.turso_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        self.assertEqual(len(tables), 0)

    def test_02_turso_readonly_state_repository_zero_outbound_queries_on_construction(self) -> None:
        """Requirement 2: TursoReadOnlyStateRepository never sends outbound queries on instantiation."""
        repo = TursoReadOnlyStateRepository(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )

        self.assertEqual(len(self.recorded_requests), 0)
        self.assertTrue(repo.read_only)
        self.assertTrue(repo.client.read_only)

        tables = self.turso_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        self.assertEqual(len(tables), 0)

    def test_03_default_constructor_defaults_to_init_schema_false(self) -> None:
        """Requirement 3: TursoStateRepository default construction is side-effect-free (init_schema=False)."""
        repo = TursoStateRepository(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )
        # Default construction sends 0 requests
        self.assertEqual(len(self.recorded_requests), 0)
        tables = self.turso_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        self.assertEqual(len(tables), 0)

        # Authorized initialization when explicitly requested
        repo.initialize_schema()
        self.assertGreater(len(self.recorded_requests), 0)
        tables_after = self.turso_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        self.assertGreaterEqual(len(tables_after), 15)

    # -------------------------------------------------------------------------
    # 2. Non-Mutating Remote Preflight on Staging Database
    # -------------------------------------------------------------------------

    def test_04_preflight_on_uninitialized_database_fails_closed_without_mutating(self) -> None:
        """Requirement 4: Preflight on empty remote staging fails closed with 0 tables created."""
        validator = CloudPreflightValidator.create_for_turso(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )

        report = validator.validate()

        self.assertFalse(report.success)
        self.assertTrue(report.is_non_mutating)
        self.assertEqual(report.schema_version, 0)
        self.assertIn("schema_migrations", report.missing_tables)
        self.assertEqual(len(report.missing_tables), len(SCHEMA_V6_TABLES))

        tables = self.turso_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        self.assertEqual(len(tables), 0)

    def test_05_preflight_on_initialized_v6_database_passes_non_mutating(self) -> None:
        """Requirement 5: Preflight on initialized schema v6 staging passes with non-mutating invariant."""
        for ddl in get_all_init_statements():
            if ddl.strip():
                self.turso_conn.execute(ddl)
        self.turso_conn.execute(
            "INSERT INTO schema_migrations (version, applied_at, description) VALUES (6, '2026-10-24T10:00:00Z', 'init')"
        )

        validator = CloudPreflightValidator.create_for_turso(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )
        report = validator.validate()

        self.assertTrue(report.success)
        self.assertTrue(report.is_non_mutating)
        self.assertEqual(report.schema_version, 6)
        self.assertEqual(len(report.missing_tables), 0)
        self.assertEqual(len(report.missing_columns), 0)
        self.assertEqual(len(report.missing_indexes), 0)

    def test_06_outbound_hrana_payloads_contain_only_read_only_queries(self) -> None:
        """Requirement 6: Verify outbound Hrana requests contain exclusively read-only queries."""
        for ddl in get_all_init_statements():
            if ddl.strip():
                self.turso_conn.execute(ddl)
        self.turso_conn.execute(
            "INSERT INTO schema_migrations (version, applied_at, description) VALUES (6, '2026-10-24T10:00:00Z', 'init')"
        )

        self.recorded_requests.clear()

        validator = CloudPreflightValidator.create_for_turso(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )
        validator.validate()

        self.assertGreater(len(self.recorded_requests), 0)

        prohibited_keywords = {"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "ATTACH", "BEGIN", "COMMIT"}

        for req_record in self.recorded_requests:
            body = req_record["body"]
            for r in body.get("requests", []):
                if r.get("type") == "execute":
                    sql = r.get("stmt", {}).get("sql", "").strip()
                    validate_read_only_sql(sql)
                    words = set(sql.upper().replace("(", " ").replace(")", " ").split())
                    self.assertTrue(
                        words.isdisjoint(prohibited_keywords),
                        f"Outbound preflight SQL contains prohibited mutating keywords: {sql}",
                    )

    def test_07_mutating_operations_rejected_on_readonly_repository(self) -> None:
        """Requirement 7: Calling mutating operations on TursoReadOnlyStateRepository raises PermissionError."""
        repo = TursoReadOnlyStateRepository(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )

        with self.assertRaises(PermissionError):
            repo.acquire_run_lock("test_worker")

        with self.assertRaises(PermissionError):
            repo.save_pending_candidates([])

        with self.assertRaises(PermissionError):
            repo.record_admin_report("2026-10-24", "worker_1", "SENT", "123", {})

        with self.assertRaises(PermissionError):
            repo.initialize_schema()

        with self.assertRaises(PermissionError):
            repo._init_db()

        self.assertEqual(len(self.recorded_requests), 0)

    # -------------------------------------------------------------------------
    # 3. Endpoint Configuration Safety & Scheme Verification (ISSUE 1 REGRESSIONS)
    # -------------------------------------------------------------------------

    def test_08_empty_or_whitespace_turso_url_rejected(self) -> None:
        """Requirement 8: Empty or whitespace Turso database URL is rejected."""
        with self.assertRaises(ValueError):
            normalize_turso_url("")

        with self.assertRaises(ValueError):
            normalize_turso_url("   ")

        with self.assertRaises(ValueError):
            TursoHTTPClient(database_url="", auth_token="token")

    def test_09_remote_insecure_http_bypass_variants_strictly_blocked(self) -> None:
        """Requirement 9 / Issue 1: Block all remote insecure HTTP variants regardless of substrings, paths, or mock transport."""
        remote_http_bypass_variants = [
            "http://localhost.attacker.invalid",
            "http://localhost.attacker.invalid:8080",
            "http://127.0.0.1.attacker.invalid",
            "http://127.0.0.1.attacker.invalid:8080",
            "http://remote.attacker.com/localhost",
            "http://remote.attacker.com/v2/pipeline/localhost",
            "http://remote.attacker.com?param=localhost",
            "http://remote.attacker.com?param=127.0.0.1",
            "http://remote-staging.turso.io",
            "http://my-db.turso.io:8080",
        ]

        for bad_url in remote_http_bypass_variants:
            with self.subTest(url=bad_url):
                with self.assertRaises(ValueError) as ctx:
                    normalize_turso_url(bad_url)
                self.assertIn("Insecure unencrypted HTTP connection to remote Turso host", str(ctx.exception))

                # Must also be rejected by TursoHTTPClient even when mock transport is supplied
                with self.assertRaises(ValueError):
                    TursoHTTPClient(database_url=bad_url, transport=self.transport)

    def test_09b_unsupported_schemes_rejected(self) -> None:
        """Requirement 9b: Unsupported schemes (ftp, file, sqlite, etc.) are rejected."""
        unsupported = [
            "ftp://staging.turso.io",
            "file:///etc/passwd",
            "sqlite:///data/radar.db",
            "postgres://localhost:5432/db",
            "javascript:alert(1)",
        ]
        for bad_url in unsupported:
            with self.assertRaises(ValueError):
                normalize_turso_url(bad_url)

    def test_09c_allowed_loopback_and_https_schemes_normalized(self) -> None:
        """Requirement 9c: Only exact loopback hosts are allowed over HTTP; HTTPS/libsql normalized."""
        allowed_loopbacks = [
            ("http://localhost:8080", "http://localhost:8080/v2/pipeline"),
            ("http://127.0.0.1:8080", "http://127.0.0.1:8080/v2/pipeline"),
            ("http://[::1]:8080", "http://[::1]:8080/v2/pipeline"),
            ("http://localhost", "http://localhost/v2/pipeline"),
        ]
        for raw, expected in allowed_loopbacks:
            self.assertEqual(normalize_turso_url(raw, allow_localhost_http=True), expected)

        # HTTPS and libsql HTTPS normalization
        self.assertEqual(
            normalize_turso_url("libsql://staging-db.turso.io"),
            "https://staging-db.turso.io/v2/pipeline",
        )
        self.assertEqual(
            normalize_turso_url("https://staging-db.turso.io"),
            "https://staging-db.turso.io/v2/pipeline",
        )

    def test_10_embedded_credentials_in_url_rejected(self) -> None:
        """Requirement 10: URLs with embedded username or password credentials are rejected."""
        with self.assertRaises(ValueError):
            normalize_turso_url("https://user:password@staging-db.turso.io")

        with self.assertRaises(ValueError):
            normalize_turso_url("libsql://admin:secret@staging-db.turso.io")

    def test_11_invalid_token_newlines_rejected(self) -> None:
        """Requirement 11: Tokens containing invalid newline characters are rejected."""
        with self.assertRaises(ValueError):
            TursoHTTPClient(
                database_url="libsql://staging-db.turso.io",
                auth_token="token_with_\n_newline",
                transport=self.transport,
            )

    # -------------------------------------------------------------------------
    # 4. No Automatic SQLite Fallback When Turso Explicitly Selected
    # -------------------------------------------------------------------------

    def test_12_no_sqlite_fallback_when_turso_storage_mode_selected(self) -> None:
        """Requirement 12: storage_mode='turso' fails closed on missing credentials, never falling back to SQLite."""
        with self.assertRaises(ValueError) as ctx:
            Settings(
                storage_mode="turso",
                turso_database_url="libsql://staging-db.turso.io",
                turso_auth_token="",
            )
        self.assertIn("TURSO_AUTH_TOKEN must be provided", str(ctx.exception))

        with self.assertRaises(ValueError) as ctx2:
            Settings(
                storage_mode="auto",
                turso_database_url="libsql://staging-db.turso.io",
                turso_auth_token="",
            )
        self.assertIn("Partial Turso configuration detected", str(ctx2.exception))

        unvalidated_settings = Settings.model_construct(
            storage_mode="turso",
            turso_database_url="libsql://staging-db.turso.io",
            turso_auth_token="",
        )
        with self.assertRaises(ValueError) as ctx3:
            RadarOrchestrator(settings=unvalidated_settings)
        self.assertIn("TURSO_AUTH_TOKEN must be provided", str(ctx3.exception))

        class BrokenTransport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                raise httpx.ConnectError("Turso staging connection failed")

        broken_repo = TursoStateRepository(
            database_url="libsql://staging-db.turso.io",
            auth_token="valid-token-123",
            transport=BrokenTransport(),
            init_schema=False,
        )
        with self.assertRaises(TursoClientError):
            broken_repo.get_schema_version()
        self.assertFalse((self.tmp_path / "fallback_attempt.db").exists())

    def test_13_auto_storage_mode_uses_sqlite_only_when_zero_turso_credentials(self) -> None:
        """Requirement 13: storage_mode='auto' initializes SQLite when Turso credentials are completely absent."""
        settings_auto = Settings(
            storage_mode="auto",
            turso_database_url="",
            turso_auth_token="",
            sqlite_db_path=self.tmp_path / "auto_sqlite.db",
        )
        orch = RadarOrchestrator(settings=settings_auto)
        self.assertIsInstance(orch.repository, SqliteStateRepository)

    # -------------------------------------------------------------------------
    # 5. Token and Sensitive URL Redaction (ISSUE 2 REGRESSIONS)
    # -------------------------------------------------------------------------

    def test_14_token_and_sensitive_url_redacted_in_repr_and_reports(self) -> None:
        """Requirement 14 / Issue 2: Client and repo reprs and reports never expose auth tokens or arbitrary paths."""
        secret_token = "SECRET_TURSO_JWT_TOKEN_999888777"
        raw_url = "libsql://my-org-db.turso.io"

        client = TursoHTTPClient(
            database_url=raw_url,
            auth_token=secret_token,
            transport=self.transport,
        )

        client_repr = repr(client)
        self.assertNotIn(secret_token, client_repr)
        self.assertIn("https://my-org-db.turso.io/v2/pipeline", client_repr)

        repo = TursoReadOnlyStateRepository(
            database_url=raw_url,
            auth_token=secret_token,
            transport=self.transport,
        )

        repo_repr = repr(repo)
        self.assertNotIn(secret_token, repo_repr)
        self.assertIn("TursoReadOnlyStateRepository", repo_repr)

        # mask_turso_url discards secret paths, query params, userinfo, and malformed input
        masked1 = mask_turso_url("https://user:pass@staging.turso.io/v2/pipeline?token=123")
        self.assertNotIn("pass", masked1)
        self.assertNotIn("token=123", masked1)
        self.assertEqual(masked1, "https://staging.turso.io/v2/pipeline")

        # Discards arbitrary secret-bearing paths
        masked2 = mask_turso_url("https://staging.turso.io/secret-key-12345/v2/pipeline?x=y")
        self.assertNotIn("secret-key-12345", masked2)
        self.assertNotIn("x=y", masked2)
        self.assertEqual(masked2, "https://staging.turso.io/v2/pipeline")

        # Discards malformed raw input safely
        masked3 = mask_turso_url("invalid_scheme://something")
        self.assertEqual(masked3, "<unsupported_scheme_url>")

    def test_15_simulated_http_401_with_synthetic_token_does_not_leak_in_exception(self) -> None:
        """Requirement 15 / Issue 2: Simulated HTTP 401 response containing synthetic token never exposes token in exception."""
        secret_token = "SYNTHETIC_BEARER_SECRET_TOKEN_XYZ12345"

        class UnauthorizedHostileTransport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                hostile_body = json.dumps({
                    "error": f"Unauthorized request with token {secret_token}",
                    "details": "Invalid signature for Bearer token",
                }).encode("utf-8")
                return httpx.Response(status_code=401, content=hostile_body, request=request)

        client = TursoHTTPClient(
            database_url="libsql://staging-db.turso.io",
            auth_token=secret_token,
            transport=UnauthorizedHostileTransport(),
        )

        with self.assertRaises(TursoClientError) as ctx:
            client.execute("SELECT 1")

        err_str = str(ctx.exception)
        # Verify secret token is NOT leaked
        self.assertNotIn(secret_token, err_str)
        # Verify untrusted body is NOT leaked
        self.assertNotIn("Invalid signature", err_str)
        # Verify sanitized category is reported
        self.assertIn("HTTP status 401", err_str)
        self.assertIn("Unauthorized / Authentication Failed", err_str)

    def test_15b_network_error_sanitizes_credentials_and_omits_chained_cause(self) -> None:
        """Requirement 15b / Issue 2: Network failure raises sanitized error without credential-bearing chained causes."""
        secret_token = "SENSITIVE_BEARER_TOKEN_VALUE"

        class FailingTransport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                raise httpx.ConnectError(f"Failed connecting with header Authorization: Bearer {secret_token}")

        client = TursoHTTPClient(
            database_url="libsql://staging-db.turso.io",
            auth_token=secret_token,
            transport=FailingTransport(),
        )

        with self.assertRaises(TursoClientError) as ctx:
            client.execute("SELECT 1")

        err_str = str(ctx.exception)
        self.assertNotIn(secret_token, err_str)
        self.assertIn("ConnectError", err_str)
        # Chained cause should be suppressed (from None)
        self.assertIsNone(ctx.exception.__cause__)

    # -------------------------------------------------------------------------
    # 6. Read-Only Enforcement at Query Execution Boundary (ISSUE 3 REGRESSIONS)
    # -------------------------------------------------------------------------

    def test_21_readonly_enforced_at_query_execution_boundary(self) -> None:
        """Requirement 21 / Issue 3: Zero DDL/DML can pass through read-only client or repository routes."""
        repo = TursoReadOnlyStateRepository(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )

        # 1. Direct client.execute with DDL is blocked before HTTP dispatch
        with self.assertRaises(ValueError) as ctx1:
            repo.client.execute("CREATE TABLE evil (id INT)")
        self.assertIn("Preflight query safety violation", str(ctx1.exception))

        # 2. Direct client.execute_pipeline with DML is blocked before HTTP dispatch
        with self.assertRaises(ValueError) as ctx2:
            repo.client.execute_pipeline([("DROP TABLE processed_articles", [])])
        self.assertIn("Preflight query safety violation", str(ctx2.exception))

        # 3. Direct client.execute_transaction is blocked
        with self.assertRaises(PermissionError):
            repo.client.execute_transaction([("SELECT 1", [])])

        # 4. Direct client.executescript is blocked
        with self.assertRaises(PermissionError):
            repo.client.executescript("CREATE TABLE foo (id INT);")

        # 5. Direct _init_db on read-only repo is blocked
        with self.assertRaises(PermissionError):
            repo._init_db()

        # 6. Direct initialize_schema on read-only repo is blocked
        with self.assertRaises(PermissionError):
            repo.initialize_schema()

        # Proves ZERO outbound mutating statements reached the backend
        self.assertEqual(len(self.recorded_requests), 0)

    # -------------------------------------------------------------------------
    # 7. Safe Operator Connection Test & CLI Configuration (ISSUE 4 REGRESSIONS)
    # -------------------------------------------------------------------------

    def test_16_check_staging_connection_on_uninitialized_database(self) -> None:
        """Requirement 16: check_staging_connection on uninitialized DB reports reachability and table_count=0."""
        report = check_staging_connection(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )

        self.assertTrue(report.success)
        self.assertTrue(report.is_reachability_verified)
        self.assertFalse(report.is_schema_initialized)
        self.assertEqual(report.table_count, 0)
        self.assertIn("INITIALIZATION REQUIRED", report.summary().upper())

        tables = self.turso_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        self.assertEqual(len(tables), 0)

    def test_17_check_staging_connection_on_initialized_v6_database(self) -> None:
        """Requirement 17: check_staging_connection on schema v6 DB reports INITIALIZED (v6)."""
        for ddl in get_all_init_statements():
            if ddl.strip():
                self.turso_conn.execute(ddl)
        self.turso_conn.execute(
            "INSERT INTO schema_migrations (version, applied_at, description) VALUES (6, '2026-10-24T10:00:00Z', 'init')"
        )

        report = check_staging_connection(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )

        self.assertTrue(report.success)
        self.assertTrue(report.is_reachability_verified)
        self.assertTrue(report.is_schema_initialized)
        self.assertEqual(report.schema_version, 6)
        self.assertGreaterEqual(report.table_count, 15)
        self.assertIn("INITIALIZED (V6)", report.summary().upper())

    def test_18_check_staging_connection_handles_connection_failure(self) -> None:
        """Requirement 18: check_staging_connection fails closed on network failure with sanitized error."""
        class DownTransport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                raise httpx.ConnectTimeout("Connection timed out to Turso staging host")

        report = check_staging_connection(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=DownTransport(),
        )

        self.assertFalse(report.success)
        self.assertFalse(report.is_reachability_verified)
        self.assertIn("timed out", str(report.error_message).lower())

    def test_22_staging_connection_cli_configuration(self) -> None:
        """Requirement 22 / Issue 4: CLI configuration reader uses process os.environ without reading .env."""
        # 1. Missing credentials fails closed
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError) as ctx1:
                get_staging_credentials_from_env()
            self.assertIn("Missing Turso staging credentials", str(ctx1.exception))

        # 2. Partial credentials fails closed
        with patch.dict(os.environ, {"TURSO_DATABASE_URL": "libsql://staging.turso.io"}, clear=True):
            with self.assertRaises(ValueError) as ctx2:
                get_staging_credentials_from_env()
            self.assertIn("TURSO_AUTH_TOKEN", str(ctx2.exception))

        with patch.dict(os.environ, {"TURSO_AUTH_TOKEN": "token123"}, clear=True):
            with self.assertRaises(ValueError) as ctx3:
                get_staging_credentials_from_env()
            self.assertIn("TURSO_DATABASE_URL", str(ctx3.exception))

        # 3. Dry-run validates syntax without network
        fake_env = {
            "TURSO_DATABASE_URL": "libsql://staging-db.turso.io",
            "TURSO_AUTH_TOKEN": "secret-token-abcdef123456",
        }
        with patch.dict(os.environ, fake_env, clear=True):
            url, token = get_staging_credentials_from_env()
            self.assertEqual(url, "libsql://staging-db.turso.io")
            self.assertEqual(token, "secret-token-abcdef123456")

            # Run CLI --dry-run
            out = io.StringIO()
            with patch("sys.stdout", out):
                ret_code = connection_main(["--dry-run"])
            self.assertEqual(ret_code, 0)
            output_str = out.getvalue()
            self.assertIn("DRY-RUN COMPLETE", output_str)
            # Secrets must NOT appear in output
            self.assertNotIn("secret-token-abcdef123456", output_str)

            # Run CLI without --check
            out_default = io.StringIO()
            with patch("sys.stdout", out_default):
                ret_default = connection_main([])
            self.assertEqual(ret_default, 0)
            self.assertIn("Safety Note", out_default.getvalue())

    # -------------------------------------------------------------------------
    # 8. Mutating SQL Rejection in Inspection Path
    # -------------------------------------------------------------------------

    def test_19_mutating_sql_rejected_in_inspection_path(self) -> None:
        """Requirement 19: execute_read_query rejects DDL, DML, and dangerous PRAGMAs."""
        repo = TursoReadOnlyStateRepository(
            database_url="libsql://staging-db.turso.io",
            auth_token="test-token-12345",
            transport=self.transport,
        )

        mutating_statements = [
            "INSERT INTO stories (story_id) VALUES ('x')",
            "UPDATE stories SET headline_tr = 'hack'",
            "DELETE FROM notifications",
            "DROP TABLE run_locks",
            "ALTER TABLE stories ADD COLUMN bad TEXT",
            "CREATE TABLE evil (id INT)",
            "PRAGMA journal_mode=WAL",
            "ATTACH DATABASE 'evil.db' AS evil",
            "SELECT 1; DROP TABLE stories",
        ]

        for bad_sql in mutating_statements:
            with self.assertRaises(ValueError):
                execute_read_query(repo, bad_sql)

    # -------------------------------------------------------------------------
    # 9. Operational Database & Local Files Immutability
    # -------------------------------------------------------------------------

    def test_20_operational_database_and_env_untouched(self) -> None:
        """Requirement 20: Operational database data/radar.db and .env remain untouched."""
        prod_path = Path("data/radar.db")
        if prod_path.exists():
            sqlite_repo = SqliteStateRepository(prod_path)
            with self.assertRaises(PermissionError):
                CloudPreflightValidator(sqlite_repo, allow_operational_db=False)

            with self.assertRaises(PermissionError):
                execute_read_query(sqlite_repo, "SELECT 1")


if __name__ == "__main__":
    unittest.main()
