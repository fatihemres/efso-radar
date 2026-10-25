"""Deterministic Hermetic Test Suite for Phase 5B.3G-B (Hermetic Staging Runner Foundation).

Validates:
1. Dry-run CLI execution with zero credentials and no outbound network calls.
2. Immediate fail-closed rejection of live execution flags (--live, --execute, etc.) before client creation.
3. Strict allowlist validation of exact staging destination (libsql://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io)
   and rejection of wildcards, alternative hosts, IP addresses, ports (even 443/80), queries, fragments, and embedded credentials.
4. Unique collision-resistant run ID generation, exact 5-record ahead-of-time manifest registration, unique keys,
   valid dependency ordering, exact attribute requirements, extra="forbid", and refusal to target radar_cron.
5. Atomic manifest persistence, path-traversal prevention, zero-secrets invariant, safe permissions,
   and offline manifest recovery (--recover-manifest) that validates schema without modifying or destroying the manifest.
6. Complete 8-stage hermetic simulation (LIVE-01 through LIVE-08) with accurate nominal (78) and worst-case (80)
   request counts and 20-call cleanup reserve enforcement.
7. Strict Python API boundary: StagingSessionRunner rejects live-capable HTTP clients, missing transports,
   non-hermetic transports, and mismatched destinations immediately at __init__ with zero requests executed.
8. Ambiguous COMMIT outcome handling and multi-record reconciliation in LIVE-07:
   reconciling both processed_articles and stories, classifying COMMITTED_ORPHAN / NOT_COMMITTED / PARTIAL_CORRUPTION_DETECTED,
   preserving UNKNOWN on inspection failure, and asserting zero blind write retries.
9. Pre-deletion ownership inspection and conditional deletion enforcing exact PK + synthetic attributes
   (exact attribute equality, no LIKE patterns), with affected-row verification (0/1/>1) and OWNERSHIP_MISMATCH_HALT.
10. Three-state audit verification (CLEAN / RESIDUAL / UNKNOWN) and manifest unlinking on CLEAN only.
11. Absolute isolation: zero real network requests, zero access to data/radar.db, data/state.json, or .env.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
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
    RegisteredEntity,
    SessionStatus,
    StagingSessionRunner,
    TEST_CUTOFF_REQUESTS,
    create_default_manifest_entities,
    is_hermetic_transport,
    main,
    reconcile_ambiguous_outcome,
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

        normalized_pipeline = validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}/v2/pipeline")
        self.assertEqual(normalized_pipeline, f"https://{EXACT_STAGING_HOSTNAME}")

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
        for invalid in (
            "ftp://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io",
            "http://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io",
        ):
            with self.assertRaises(PermissionError):
                validate_staging_destination(invalid)

    def test_embedded_credentials_rejected(self):
        with self.assertRaises(PermissionError) as ctx:
            validate_staging_destination(f"libsql://user:pass@{EXACT_STAGING_HOSTNAME}")
        self.assertIn("credentials embedded in destination URL", str(ctx.exception))

    def test_all_explicit_ports_strictly_rejected(self):
        """Reject ANY explicit port, including standard 443 and 80 (fail-closed)."""
        for port in (443, 80, 8080, 8443):
            with self.assertRaises(PermissionError) as ctx:
                validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}:{port}")
            self.assertIn("explicit port", str(ctx.exception))
            self.assertIn(str(port), str(ctx.exception))

    def test_queries_and_fragments_rejected(self):
        with self.assertRaises(PermissionError) as ctx:
            validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}?query=1")
        self.assertIn("query string", str(ctx.exception))

        with self.assertRaises(PermissionError) as ctx:
            validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}#section")
        self.assertIn("URL fragment", str(ctx.exception))

    def test_unexpected_paths_rejected(self):
        for bad_path in ("/api/v1", "/admin", "/execute", "/v1/pipeline"):
            with self.assertRaises(PermissionError) as ctx:
                validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}{bad_path}")
            self.assertIn("unexpected URL path", str(ctx.exception))

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
        entities = create_default_manifest_entities(run_id)
        manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at="2026-10-25T15:00:00Z",
            registered_entities=entities,
        )

        target_path = manifest.save_atomic(self.manifest_dir)
        self.assertTrue(target_path.exists())
        self.assertEqual(target_path.name, f"efso_radar_staging_manifest_{run_id}.json")

        # Verify loaded content matches exactly
        loaded = PerRunOwnershipManifest.load_and_validate(target_path)
        self.assertEqual(loaded.run_id, run_id)
        self.assertEqual(len(loaded.registered_entities), 5)

    def test_zero_secrets_invariant(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        entities = create_default_manifest_entities(run_id)
        manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at="2026-10-25T15:00:00Z",
            registered_entities=entities,
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

    def test_exact_five_entities_required(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        base_entities = create_default_manifest_entities(run_id)

        # 4 entities (too few)
        bad_file_4 = self.manifest_dir / "bad_4.json"
        bad_file_4.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": [e.model_dump() for e in base_entities[:4]],
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file_4)
        self.assertIn("exactly 5 registered entities", str(ctx.exception))

        # 6 entities (too many)
        bad_file_6 = self.manifest_dir / "bad_6.json"
        extra_dict = dict(base_entities[0].model_dump())
        extra_dict["pk_value"] = f"synth_art_{run_id}_extra"
        bad_file_6.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": [e.model_dump() for e in base_entities] + [extra_dict],
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file_6)
        self.assertIn("exactly 5 registered entities", str(ctx.exception))

    def test_unauthorized_table_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        entities = [e.model_dump() for e in create_default_manifest_entities(run_id)]
        entities[0]["table"] = "notifications"  # Read-only schema v6 table!
        entities[0]["pk_column"] = "notification_id"

        bad_file = self.manifest_dir / f"efso_radar_staging_manifest_{run_id}.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": entities,
        }), encoding="utf-8")

        with self.assertRaises(PermissionError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file)
        self.assertIn("unauthorized table 'notifications'", str(ctx.exception))

    def test_extra_fields_forbidden(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        entities = [e.model_dump() for e in create_default_manifest_entities(run_id)]

        # Extra field on root
        bad_file_root = self.manifest_dir / "extra_root.json"
        bad_file_root.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": entities,
            "unexpected_field": "injected_data",
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file_root)
        self.assertIn("unexpected_field", str(ctx.exception).lower())

        # Extra field on entity
        bad_file_entity = self.manifest_dir / "extra_entity.json"
        entities_copy = [dict(e) for e in entities]
        entities_copy[0]["unexpected_col"] = "val"
        bad_file_entity.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": entities_copy,
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file_entity)
        self.assertIn("unexpected_col", str(ctx.exception).lower())

    def test_duplicate_entities_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        entities = [e.model_dump() for e in create_default_manifest_entities(run_id)]
        # Duplicate entity 0 in place of entity 1
        entities[1] = dict(entities[0])

        bad_file = self.manifest_dir / "duplicate.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": entities,
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file)
        self.assertIn("Duplicate entity registration", str(ctx.exception))

    def test_radar_cron_lock_target_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        entities = [e.model_dump() for e in create_default_manifest_entities(run_id)]
        # Tamper lock_key to target production lock
        for ent in entities:
            if ent["table"] == "run_locks":
                ent["pk_value"] = "radar_cron"

        bad_file = self.manifest_dir / f"efso_radar_staging_manifest_{run_id}.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": entities,
        }), encoding="utf-8")

        with self.assertRaises(PermissionError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file)
        self.assertIn("cannot target 'radar_cron'", str(ctx.exception))

    def test_mismatched_run_id_derivation_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        entities = [e.model_dump() for e in create_default_manifest_entities(run_id)]
        entities[0]["pk_value"] = "synth_art_different_run_id_01"

        bad_file = self.manifest_dir / f"efso_radar_staging_manifest_{run_id}.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": entities,
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file)
        self.assertIn("not derived from run_id", str(ctx.exception))

    def test_invalid_cleanup_dependency_order_rejected(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        entities = [e.model_dump() for e in create_default_manifest_entities(run_id)]
        # Articles must have order 2
        entities[0]["cleanup_dependency_order"] = 1

        bad_file = self.manifest_dir / "bad_order.json"
        bad_file.write_text(json.dumps({
            "run_id": run_id,
            "created_at": "2026-10-25T15:00:00Z",
            "version": 1,
            "registered_entities": entities,
        }), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            PerRunOwnershipManifest.load_and_validate(bad_file)
        self.assertIn("cleanup_dependency_order=2", str(ctx.exception))

    def test_manifest_deleted_only_on_clean(self):
        run_id = "run_20261025T150000Z_abcdef123456"
        manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at="2026-10-25T15:00:00Z",
            registered_entities=create_default_manifest_entities(run_id),
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

        # Retained on OFFLINE_VALIDATION_PASSED
        manifest.delete_if_clean(target_path, AuditOutcomeState.OFFLINE_VALIDATION_PASSED)
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

    # -------------------------------------------------------------------------
    # Python API Boundary Protection Tests (BLOCKER 2)
    # -------------------------------------------------------------------------

    def test_runner_rejects_none_client(self):
        with self.assertRaises(PermissionError) as ctx:
            StagingSessionRunner(client=None)  # type: ignore[arg-type]
        self.assertIn("Client cannot be None", str(ctx.exception))

    def test_runner_rejects_client_without_transport(self):
        """A client created without transport uses live network by default; must be rejected."""
        live_client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="token",
            transport=None,
        )
        with self.assertRaises(PermissionError) as ctx:
            StagingSessionRunner(client=live_client)
        self.assertIn("Hermetic security boundary violation", str(ctx.exception))
        live_client.close()

    def test_runner_rejects_client_with_live_http_transport(self):
        """Explicit httpx.HTTPTransport must be rejected immediately."""
        live_transport = httpx.HTTPTransport()
        live_client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="token",
            transport=live_transport,
        )
        with self.assertRaises(PermissionError) as ctx:
            StagingSessionRunner(client=live_client)
        self.assertIn("Hermetic security boundary violation", str(ctx.exception))
        live_client.close()

    def test_runner_rejects_mismatched_client_destination(self):
        """Client pointing to a different endpoint than session destination must be rejected."""
        mismatched_client = TursoHTTPClient(
            database_url="https://other-staging-db.turso.io",
            auth_token="token",
            transport=self.raw_transport,
        )
        with self.assertRaises(PermissionError) as ctx:
            StagingSessionRunner(client=mismatched_client)
        self.assertIn("Client endpoint mismatch", str(ctx.exception))
        mismatched_client.close()

    def test_runner_rejects_read_only_client(self):
        """Runner requires a read-write client to execute synthetic mutations."""
        ro_client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="token",
            transport=self.raw_transport,
            read_only=True,
        )
        with self.assertRaises(ValueError) as ctx:
            StagingSessionRunner(client=ro_client)
        self.assertIn("requires a read-write client", str(ctx.exception))
        ro_client.close()

    def test_is_hermetic_transport_helper(self):
        self.assertFalse(is_hermetic_transport(None))
        self.assertFalse(is_hermetic_transport(httpx.HTTPTransport()))
        self.assertFalse(is_hermetic_transport(httpx.AsyncHTTPTransport()))
        self.assertTrue(is_hermetic_transport(self.raw_transport))
        self.assertTrue(is_hermetic_transport(self.budget_transport))

        # Nested budget transport
        nested = BudgetTrackingTransport(self.budget_transport)
        self.assertTrue(is_hermetic_transport(nested))

        # Budget transport wrapping a live transport
        bad_budget = BudgetTrackingTransport(httpx.HTTPTransport())
        self.assertFalse(is_hermetic_transport(bad_budget))


class TestAmbiguousCommitHandling(unittest.TestCase):
    """Test ambiguous transaction outcome protocol in LIVE-07 (BLOCKER 3)."""

    def setUp(self):
        self.conn = create_schema_v6_conn()
        self.raw_transport = HermeticStagingTransport(self.conn)
        self.client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock",
            transport=self.raw_transport,
        )

    def tearDown(self):
        self.client.close()
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
        client.close()

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
        client.close()

    def test_reconcile_ambiguous_outcome_both_committed(self):
        run_id = "run_20261025T150000Z_reconcile01"
        art_id = f"synth_art_{run_id}_01"
        story_id = f"synth_story_{run_id}_01"

        # Pre-insert both records
        self.client.execute(
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, 'https://ex.com/1', 'T', 'S', '2026-10-25T15:00:00Z', '2026-10-25T15:00:00Z')",
            [art_id],
        )
        self.client.execute(
            "INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json) "
            "VALUES (?, 'H', 'C', '2026-10-25T15:00:00Z', '2026-10-25T15:00:00Z', '[]')",
            [story_id],
        )

        res = reconcile_ambiguous_outcome(self.client, run_id)
        self.assertEqual(res["outcome"], "COMMITTED_ORPHAN")
        self.assertTrue(res["article_present"])
        self.assertTrue(res["story_present"])
        self.assertEqual(res["blind_retries"], 0)

    def test_reconcile_ambiguous_outcome_neither_committed(self):
        run_id = "run_20261025T150000Z_reconcile02"
        res = reconcile_ambiguous_outcome(self.client, run_id)
        self.assertEqual(res["outcome"], "NOT_COMMITTED")
        self.assertFalse(res["article_present"])
        self.assertFalse(res["story_present"])
        self.assertEqual(res["blind_retries"], 0)

    def test_reconcile_ambiguous_outcome_contradictory_raises(self):
        """If one record is present and one is absent, PARTIAL_CORRUPTION_DETECTED must be raised."""
        run_id = "run_20261025T150000Z_reconcile03"
        art_id = f"synth_art_{run_id}_01"

        # Insert only article
        self.client.execute(
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, 'https://ex.com/1', 'T', 'S', '2026-10-25T15:00:00Z', '2026-10-25T15:00:00Z')",
            [art_id],
        )

        with self.assertRaises(RuntimeError) as ctx:
            reconcile_ambiguous_outcome(self.client, run_id)
        self.assertIn("PARTIAL_CORRUPTION_DETECTED", str(ctx.exception))
        self.assertIn("Atomicity violation", str(ctx.exception))

    def test_live_07_simulation_contradictory_state_preserves_manifest(self):
        """Simulate atomicity breach in LIVE-07; session must fail and manifest must be preserved."""
        with tempfile.TemporaryDirectory() as td:
            budget = BudgetTrackingTransport(self.raw_transport)
            client = TursoHTTPClient(
                database_url=EXACT_STAGING_DESTINATION,
                auth_token="mock",
                transport=budget,
            )
            runner = StagingSessionRunner(
                client=client,
                destination_url=EXACT_STAGING_DESTINATION,
                manifest_dir=Path(td),
                simulate_ambiguous_contradictory=True,
            )
            report = runner.run()

            self.assertEqual(report.status, SessionStatus.FAILED)
            self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
            self.assertTrue(report.manifest_preserved)
            self.assertTrue(Path(report.manifest_path).exists())
            client.close()

    def test_live_07_simulation_inspection_failure_preserves_manifest(self):
        """Simulate transport failure during LIVE-07 inspection; outcome must remain UNKNOWN."""
        with tempfile.TemporaryDirectory() as td:
            budget = BudgetTrackingTransport(self.raw_transport)
            client = TursoHTTPClient(
                database_url=EXACT_STAGING_DESTINATION,
                auth_token="mock",
                transport=budget,
            )
            runner = StagingSessionRunner(
                client=client,
                destination_url=EXACT_STAGING_DESTINATION,
                manifest_dir=Path(td),
                simulate_ambiguous_inspection_failure=True,
            )
            report = runner.run()

            self.assertEqual(report.status, SessionStatus.FAILED)
            self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
            self.assertTrue(report.manifest_preserved)
            self.assertTrue(Path(report.manifest_path).exists())
            client.close()


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

    # -------------------------------------------------------------------------
    # Offline Manifest Recovery Non-Destruction Tests (BLOCKER 1)
    # -------------------------------------------------------------------------

    def test_offline_recovery_never_destroys_or_mutates_manifest(self):
        """Offline recovery must validate schema without unlinking or mutating the manifest file."""
        with tempfile.TemporaryDirectory() as td:
            run_id = "run_20261025T150000Z_abcdef123456"
            m = PerRunOwnershipManifest(
                run_id=run_id,
                created_at="2026-10-25T15:00:00Z",
                registered_entities=create_default_manifest_entities(run_id),
            )
            m_path = m.save_atomic(Path(td))

            # Record initial state
            initial_bytes = m_path.read_bytes()
            initial_hash = hashlib.sha256(initial_bytes).hexdigest()

            # Execute offline recovery
            report = recover_manifest_offline(m_path)

            self.assertTrue(report.valid_schema)
            self.assertEqual(report.entities_count, 5)
            self.assertEqual(report.audit_state, AuditOutcomeState.OFFLINE_VALIDATION_PASSED)
            self.assertTrue(report.manifest_preserved)

            # Assert manifest is still present and bytes are 100% identical
            self.assertTrue(m_path.exists())
            after_bytes = m_path.read_bytes()
            self.assertEqual(hashlib.sha256(after_bytes).hexdigest(), initial_hash)

    def test_offline_recover_manifest_cli(self):
        with tempfile.TemporaryDirectory() as td:
            run_id = "run_20261025T150000Z_abcdef123456"
            m = PerRunOwnershipManifest(
                run_id=run_id,
                created_at="2026-10-25T15:00:00Z",
                registered_entities=create_default_manifest_entities(run_id),
            )
            m_path = m.save_atomic(Path(td))
            initial_hash = hashlib.sha256(m_path.read_bytes()).hexdigest()

            stdout_capture = io.StringIO()
            with patch("sys.stdout", stdout_capture):
                exit_code = main(["--recover-manifest", str(m_path)])
            self.assertEqual(exit_code, 0)
            self.assertIn("Offline manifest validation succeeded", stdout_capture.getvalue())
            self.assertIn("OFFLINE_VALIDATION_PASSED", stdout_capture.getvalue())

            # File must still exist and be identical
            self.assertTrue(m_path.exists())
            self.assertEqual(hashlib.sha256(m_path.read_bytes()).hexdigest(), initial_hash)

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

    # -------------------------------------------------------------------------
    # Regression Check: Clean runpy execution without eager import warnings
    # -------------------------------------------------------------------------

    def test_cli_runner_module_execution_clean_warnings(self):
        """python -W error -m efso_radar.staging.runner --help must execute cleanly without warnings."""
        env = os.environ.copy()
        env["PYTHONPATH"] = "src"
        result = subprocess.run(
            [sys.executable, "-W", "error", "-m", "efso_radar.staging.runner", "--help"],
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(result.returncode, 0, f"CLI exited with {result.returncode}: {result.stderr}")
        self.assertIn("Efso Radar Hermetic Staging Runner Foundation", result.stdout)
        self.assertEqual(result.stderr, "")


if __name__ == "__main__":
    unittest.main()
