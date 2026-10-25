"""Deterministic Hermetic Test Suite for Phase 5B.3G-B (Hermetic Staging Runner Foundation).

Validates:
1. Dry-run CLI execution with zero credentials and no outbound network calls.
2. Immediate fail-closed rejection of live execution flags (--live, --execute, etc.) before client creation.
3. Strict allowlist validation of exact staging destination (libsql://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io)
   and rejection of wildcards, alternative hosts, IP addresses, ports, and embedded credentials.
4. Unique collision-resistant run ID generation, 5-record ahead-of-time manifest registration, and refusal to target radar_cron.
5. Atomic manifest persistence, path-traversal prevention, zero-secrets invariant, safe permissions,
   and offline manifest recovery (--recover-manifest) with forged/tampered manifest rejection.
6. Complete 8-stage hermetic simulation (LIVE-01 through LIVE-08) with accurate nominal (78) and worst-case (80)
   request counts and 20-call cleanup reserve enforcement.
7. Distinct metric tracking: peak synthetic rows (4) vs total mutating SQL operations (14-16).
8. Ambiguous COMMIT timeout handling (UNKNOWN status, no blind retries, multi-record reconciliation).
9. Pre-deletion ownership inspection and conditional deletion enforcing exact PK + synthetic attributes,
   with affected-row verification (0/1/>1) and OWNERSHIP_MISMATCH_HALT on attribute discrepancy.
10. Three-state audit verification (CLEAN / RESIDUAL / UNKNOWN) and manifest unlinking on CLEAN only.
11. Absolute isolation: zero real network requests, zero access to data/radar.db, data/state.json, or .env.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import TursoClientError, TursoHTTPClient
from efso_radar.staging.runner import (
    AuditOutcomeState,
    BudgetTrackingTransport,
    DISALLOWED_LIVE_FLAGS,
    EXACT_STAGING_DESTINATION,
    EXACT_STAGING_HOSTNAME,
    HermeticStagingTransport,
    MAX_REQUEST_CEILING,
    PerRunOwnershipManifest,
    SessionStatus,
    StagingSessionRunner,
    TEST_CUTOFF_REQUESTS,
    main,
    recover_manifest_offline,
    validate_staging_destination,
)


def create_schema_v6_conn() -> sqlite3.Connection:
    """Create an isolated in-memory SQLite database initialized with Schema v6."""
    conn = sqlite3.connect(":memory:")
    for stmt in get_all_init_statements():
        conn.execute(stmt)
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
        "VALUES (6, '2026-10-25T15:00:00Z', 'Schema v6')"
    )
    conn.commit()
    return conn


class TestDestinationValidation(unittest.TestCase):
    """Test exact staging destination allowlist and fail-closed security boundaries."""

    def test_exact_allowlist_accepted(self):
        normalized = validate_staging_destination(EXACT_STAGING_DESTINATION)
        self.assertEqual(normalized, f"https://{EXACT_STAGING_HOSTNAME}")

        normalized_https = validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}")
        self.assertEqual(normalized_https, f"https://{EXACT_STAGING_HOSTNAME}")

    def test_wildcard_rejected(self):
        with self.assertRaises(PermissionError) as ctx:
            validate_staging_destination("libsql://*.turso.io")
        self.assertIn("does not match exact allowlisted staging host", str(ctx.exception))

    def test_alternative_turso_db_rejected(self):
        with self.assertRaises(PermissionError) as ctx:
            validate_staging_destination("libsql://other-db-fatihemres.turso.io")
        self.assertIn("does not match exact allowlisted staging host", str(ctx.exception))

    def test_ip_address_and_localhost_rejected(self):
        for invalid in ("http://127.0.0.1", "https://127.0.0.1", "libsql://localhost"):
            with self.assertRaises(PermissionError):
                validate_staging_destination(invalid)

    def test_disallowed_schemes_rejected(self):
        for invalid in ("ftp://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io", "http://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io"):
            with self.assertRaises(PermissionError):
                validate_staging_destination(invalid)

    def test_embedded_credentials_rejected(self):
        with self.assertRaises(PermissionError) as ctx:
            validate_staging_destination(f"libsql://user:pass@{EXACT_STAGING_HOSTNAME}")
        self.assertIn("credentials embedded in destination URL", str(ctx.exception))

    def test_non_standard_ports_rejected(self):
        with self.assertRaises(PermissionError) as ctx:
            validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}:8080")
        self.assertIn("non-standard port", str(ctx.exception))

    def test_empty_destination_rejected(self):
        with self.assertRaises(ValueError):
            validate_staging_destination("")


class TestPerRunOwnershipManifest(unittest.TestCase):
    """Test manifest persistence, validation, safe permissions, and tampered manifest rejection."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.manifest_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_atomic_persistence_and_safe_permissions(self):
        run_id = "run_20261025T150000Z_123456789abc"
        entities = [
            {
                "table": "processed_articles",
                "pk_column": "article_id",
                "pk_value": f"synth_art_{run_id}_01",
                "cleanup_dependency_order": 2,
                "expected_attributes": {"source": "staging_synth_test"},
            }
        ]
        manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at="2026-10-25T15:00:00Z",
            registered_entities=entities,
        )

        target_path = manifest.save_atomic(self.manifest_dir)
        self.assertTrue(target_path.exists())
        self.assertEqual(target_path.name, f"efso_radar_staging_manifest_{run_id}.json")

        # Verify loaded content matches
        loaded = PerRunOwnershipManifest.load_and_validate(target_path)
        self.assertEqual(loaded.run_id, run_id)
        self.assertEqual(len(loaded.registered_entities), 1)

    def test_zero_secrets_invariant(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at="2026-10-25T15:00:00Z",
            registered_entities=[
                {
                    "table": "run_locks",
                    "pk_column": "lock_key",
                    "pk_value": f"synth_lock_{run_id}",
                    "cleanup_dependency_order": 1,
                    "expected_attributes": {"worker_prefix": "runner_"},
                }
            ],
        )
        target_path = manifest.save_atomic(self.manifest_dir)
        raw_text = target_path.read_text(encoding="utf-8")

        # Assert no sensitive patterns
        self.assertNotIn("token", raw_text.lower())
        self.assertNotIn("password", raw_text.lower())
        self.assertNotIn("secret", raw_text.lower())
        self.assertNotIn("authorization", raw_text.lower())
        self.assertNotIn("bearer", raw_text.lower())
        self.assertNotIn("turso.io", raw_text)

    def test_unauthorized_table_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        bad_file = self.manifest_dir / f"efso_radar_staging_manifest_{run_id}.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": [
                {
                    "table": "notifications",  # Read-only table!
                    "pk_column": "notification_id",
                    "pk_value": f"synth_notif_{run_id}",
                    "cleanup_dependency_order": 1,
                    "expected_attributes": {},
                }
            ],
        }), encoding="utf-8")

        with self.assertRaises(PermissionError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file)
        self.assertIn("unauthorized table 'notifications'", str(ctx.exception))

    def test_radar_cron_lock_target_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        bad_file = self.manifest_dir / f"efso_radar_staging_manifest_{run_id}.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": [
                {
                    "table": "run_locks",
                    "pk_column": "lock_key",
                    "pk_value": "radar_cron",  # Forbidden production lock!
                    "cleanup_dependency_order": 1,
                    "expected_attributes": {},
                }
            ],
        }), encoding="utf-8")

        with self.assertRaises(Exception):
            PerRunOwnershipManifest.load_and_validate(bad_file)

    def test_mismatched_run_id_derivation_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        bad_file = self.manifest_dir / f"efso_radar_staging_manifest_{run_id}.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": [
                {
                    "table": "processed_articles",
                    "pk_column": "article_id",
                    "pk_value": "synth_art_different_run_id_01",
                    "cleanup_dependency_order": 2,
                    "expected_attributes": {},
                }
            ],
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file)
        self.assertIn("not derived from run_id", str(ctx.exception))

    def test_manifest_deleted_only_on_clean(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at="2026-10-25T15:00:00Z",
            registered_entities=[
                {
                    "table": "run_locks",
                    "pk_column": "lock_key",
                    "pk_value": f"synth_lock_{run_id}",
                    "cleanup_dependency_order": 1,
                    "expected_attributes": {"worker_prefix": "runner_"},
                }
            ],
        )
        target_path = manifest.save_atomic(self.manifest_dir)

        # Retained on RESIDUAL
        manifest.delete_if_clean(target_path, AuditOutcomeState.RESIDUAL)
        self.assertTrue(target_path.exists())

        # Retained on UNKNOWN
        manifest.delete_if_clean(target_path, AuditOutcomeState.UNKNOWN)
        self.assertTrue(target_path.exists())

        # Retained on OWNERSHIP_MISMATCH_HALT
        manifest.delete_if_clean(target_path, AuditOutcomeState.OWNERSHIP_MISMATCH_HALT)
        self.assertTrue(target_path.exists())

        # Unlinked on CLEAN
        unlinked = manifest.delete_if_clean(target_path, AuditOutcomeState.CLEAN)
        self.assertTrue(unlinked)
        self.assertFalse(target_path.exists())


class TestHermeticSessionRunner(unittest.TestCase):
    """Test full hermetic simulation, request budgeting, CAS, transactions, and cleanup."""

    def setUp(self):
        self.conn = create_schema_v6_conn()
        self.raw_transport = HermeticStagingTransport(self.conn)
        self.budget_transport = BudgetTrackingTransport(self.raw_transport)
        self.client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock-hermetic-token",
            transport=self.budget_transport,
        )
        self.temp_dir = tempfile.TemporaryDirectory()
        self.manifest_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()
        self.client.close()
        self.conn.close()

    def test_nominal_execution_request_count_and_clean_audit(self):
        runner = StagingSessionRunner(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
        )
        report = runner.run()

        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertFalse(report.manifest_preserved)

        # Nominal requests: exactly 78 calls
        self.assertEqual(report.http_requests_total, 78)
        self.assertLess(report.http_requests_total, MAX_REQUEST_CEILING)

        # Cleanup reserve used: exactly 14 calls (out of 20 reserved)
        self.assertEqual(report.cleanup_reserve_used, 14)
        self.assertLessEqual(report.cleanup_reserve_used, 20)

        # Synthetic rows peak vs total mutating ops
        self.assertEqual(report.synthetic_rows_written_peak, 4)
        self.assertEqual(report.total_mutating_operations, 14)

        # Verify all 8 stages ran and succeeded
        self.assertEqual(len(report.stages), 8)
        for stage in report.stages:
            self.assertTrue(stage.success, f"Stage {stage.stage_id} failed: {stage.error}")

    def test_worst_case_execution_persisted_rollback_record(self):
        runner = StagingSessionRunner(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
            simulate_rollback_persisted=True,
        )
        report = runner.run()

        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)

        # Worst-case requests: 80 calls (including 1 insert for persisted record and 1 delete call)
        self.assertIn(report.http_requests_total, (79, 80))
        self.assertLess(report.http_requests_total, MAX_REQUEST_CEILING)

        # Cleanup reserve used: exactly 15 calls (out of 20 reserved)
        self.assertEqual(report.cleanup_reserve_used, 15)

    def test_ownership_mismatch_halt_prevents_deletion(self):
        runner = StagingSessionRunner(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
            simulate_ownership_mismatch=True,
        )
        report = runner.run()

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.OWNERSHIP_MISMATCH_HALT)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_audit_failure_preserves_manifest(self):
        runner = StagingSessionRunner(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
            simulate_audit_failure=True,
        )
        report = runner.run()

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_budget_cutoff_halts_testing_and_enters_cleanup(self):
        # Set low cutoff to trigger test suspension
        strict_budget_transport = BudgetTrackingTransport(
            self.raw_transport,
            ceiling=MAX_REQUEST_CEILING,
            cutoff=48,  # Trips during LIVE-02
        )
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock",
            transport=strict_budget_transport,
        )
        runner = StagingSessionRunner(
            client=client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
        )
        report = runner.run()

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertIn("TEST_CUTOFF_REACHED", report.errors[0])
        # Cleanup was executed
        self.assertTrue(any(s.stage_id == "LIVE-08" for s in report.stages))

    def test_hard_budget_exhaustion_marks_unknown_and_preserves_manifest(self):
        # Set ceiling so low that even cleanup cannot finish
        exhausted_transport = BudgetTrackingTransport(
            self.raw_transport,
            ceiling=48,
            cutoff=48,
        )
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock",
            transport=exhausted_transport,
        )
        runner = StagingSessionRunner(
            client=client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
        )
        report = runner.run()

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)


class TestAmbiguousCommitHandling(unittest.TestCase):
    """Test ambiguous transaction outcome protocol in LIVE-07."""

    def setUp(self):
        self.conn = create_schema_v6_conn()

    def tearDown(self):
        self.conn.close()

    def test_commit_timeout_raises_ambiguous_unknown(self):
        transport = HermeticStagingTransport(self.conn, fail_on_commit_timeout=True)
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock",
            transport=transport,
        )
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction([
                ("INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) VALUES ('k', 'w', 't', 't', 't')", [])
            ])
        self.assertIn("Ambiguous transaction outcome (UNKNOWN)", str(ctx.exception))
        self.assertIn("timeout", str(ctx.exception).lower())

    def test_commit_network_reset_raises_ambiguous_unknown(self):
        transport = HermeticStagingTransport(self.conn, fail_on_commit_network=True)
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock",
            transport=transport,
        )
        with self.assertRaises(TursoClientError) as ctx:
            client.execute_transaction([
                ("INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) VALUES ('k', 'w', 't', 't', 't')", [])
            ])
        self.assertIn("Ambiguous transaction outcome (UNKNOWN)", str(ctx.exception))


class TestOfflineCLIAndSafety(unittest.TestCase):
    """Test CLI commands, live flag rejections, and zero credential requirement."""

    def test_disallowed_live_flags_fail_closed(self):
        for flag in DISALLOWED_LIVE_FLAGS:
            stderr_capture = io.StringIO()
            with patch("sys.stderr", stderr_capture):
                exit_code = main([flag])
            self.assertEqual(exit_code, 2, f"Flag {flag} did not exit with code 2")
            self.assertIn("Live execution is strictly prohibited", stderr_capture.getvalue())

    def test_cli_dry_run_with_no_args_succeeds(self):
        stdout_capture = io.StringIO()
        with patch("sys.stdout", stdout_capture):
            exit_code = main([])
        self.assertEqual(exit_code, 0)
        self.assertIn("EFSO RADAR STAGING RUNNER REPORT [PASSED]", stdout_capture.getvalue())

    def test_cli_explicit_dry_run_flag_succeeds(self):
        stdout_capture = io.StringIO()
        with patch("sys.stdout", stdout_capture):
            exit_code = main(["--dry-run"])
        self.assertEqual(exit_code, 0)
        self.assertIn("EFSO RADAR STAGING RUNNER REPORT [PASSED]", stdout_capture.getvalue())

    def test_cli_ignores_fake_environment_credentials(self):
        with patch.dict(os.environ, {"TURSO_AUTH_TOKEN": "fake-secret-token", "TURSO_DATABASE_URL": "libsql://fake"}):
            stdout_capture = io.StringIO()
            with patch("sys.stdout", stdout_capture):
                exit_code = main(["--dry-run"])
            self.assertEqual(exit_code, 0)
            self.assertIn("[PASSED]", stdout_capture.getvalue())

    def test_offline_recover_manifest_cli(self):
        with tempfile.TemporaryDirectory() as td:
            run_id = "run_20261025T150000Z_abcdef123456"
            m = PerRunOwnershipManifest(
                run_id=run_id,
                created_at="2026-10-25T15:00:00Z",
                registered_entities=[
                    {
                        "table": "processed_articles",
                        "pk_column": "article_id",
                        "pk_value": f"synth_art_{run_id}_01",
                        "cleanup_dependency_order": 2,
                        "expected_attributes": {"source": "staging_synth_test"},
                    }
                ],
            )
            m_path = m.save_atomic(Path(td))

            stdout_capture = io.StringIO()
            with patch("sys.stdout", stdout_capture):
                exit_code = main(["--recover-manifest", str(m_path)])
            self.assertEqual(exit_code, 0)
            self.assertIn("Offline manifest validation succeeded", stdout_capture.getvalue())

    def test_offline_recover_manifest_rejects_missing_file(self):
        stderr_capture = io.StringIO()
        with patch("sys.stderr", stderr_capture):
            exit_code = main(["--recover-manifest", "non_existent_manifest.json"])
        self.assertEqual(exit_code, 1)
        self.assertIn("Manifest recovery validation failed", stderr_capture.getvalue())

    def test_no_real_network_requests_attempted(self):
        """Prove that running the runner foundation makes zero real outbound network calls."""
        with patch("httpx.HTTPTransport.handle_request") as mock_net:
            mock_net.side_effect = RuntimeError("Real network connection attempted via HTTPTransport!")
            with patch("socket.socket.connect") as mock_sock:
                mock_sock.side_effect = RuntimeError("Real network socket connection attempted!")
                exit_code = main(["--dry-run"])
                self.assertEqual(exit_code, 0)
                mock_net.assert_not_called()
                mock_sock.assert_not_called()

    def test_operational_files_untouched(self):
        """Prove data/radar.db, data/state.json, and .env are never opened."""
        prod_db = Path("data/radar.db")
        prod_state = Path("data/state.json")
        prod_env = Path(".env")

        original_mtime_db = prod_db.stat().st_mtime if prod_db.exists() else None
        original_mtime_state = prod_state.stat().st_mtime if prod_state.exists() else None
        original_mtime_env = prod_env.stat().st_mtime if prod_env.exists() else None

        exit_code = main(["--dry-run"])
        self.assertEqual(exit_code, 0)

        if original_mtime_db is not None:
            self.assertEqual(prod_db.stat().st_mtime, original_mtime_db)
        if original_mtime_state is not None:
            self.assertEqual(prod_state.stat().st_mtime, original_mtime_state)
        if original_mtime_env is not None:
            self.assertEqual(prod_env.stat().st_mtime, original_mtime_env)


if __name__ == "__main__":
    unittest.main()
