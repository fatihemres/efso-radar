"""Controlled Staging Runner Offline Slice Integration Tests (Phase 5B.3G-C / Issue #13 / Issue #82).

Offline engineering verification of the transport-injected controlled-staging execution path
for LIVE-02 through LIVE-08:
- Nominal controlled slice (skip_live_01=True): exactly 32 HTTP requests, ahead-of-time manifest
  registration, atomic transaction, rollback, sequential CAS, expired lease takeover, recovery
  checkpoint, ambiguous reconciliation, conditional cleanup (affected == 1), 3-state audit, manifest unlinking on CLEAN.
- Full hermetic session (skip_live_01=False): 78 HTTP requests across all 8 stages.
- Transport injection: hermetic mock transport injection, custom response hooks, rejection of real network transports.
- Fault injection: COMMIT timeouts, connection drops, HTTP 500/502/503/504 errors, statement errors,
  contradictory outcomes, inspection failures, ownership mismatches, and audit failures.
- Safety invariants: exact destination allowlisting, strict request ceilings (<100) and cleanup reserves (20),
  ambiguous COMMIT -> UNKNOWN, no blind retries, residual manifest preservation.
- Non-proof contract: asserts that hermetic mock tests do NOT prove remote unattended cross-runner recovery
  or waive the unresolved ERD-017 architectural choice.
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import (
    TursoHTTPClient,
)
from efso_radar.staging import (
    AuditOutcomeState,
    EXACT_STAGING_DESTINATION,
    EXACT_STAGING_HOSTNAME,
    LiveReadyOperatorStatus,
    LiveReadyStagingOperator,
    SessionStatus,
    StagingSessionRunner,
    execute_controlled_staging,
    validate_staging_destination,
)
from efso_radar.staging.manifest_seal import OfflineManifestSeal
from efso_radar.staging.hrana_outbound_guard import HranaOutboundSecurityBoundary
from efso_radar.staging.runner import (
    BudgetTrackingTransport,
    HermeticStagingTransport,
    PerRunOwnershipManifest,
    build_parser,
    create_default_manifest_entities,
    main,
)


def _create_isolated_v6_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for stmt in get_all_init_statements():
        conn.execute(stmt)
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
        "VALUES (6, '2026-10-25T15:00:00Z', 'Schema v6')"
    )
    conn.commit()
    return conn


class TestControlledStagingSliceNominal(unittest.TestCase):
    """Test nominal execution of transport-injected controlled staging slice."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.manifest_dir = Path(self.temp_dir.name)

    def test_nominal_controlled_slice_live02_through_live08(self):
        """Nominal slice with skip_live_01=True executes LIVE-02..08 with exactly 32 requests."""
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)

        raw_transport = HermeticStagingTransport(conn)
        budget_transport = BudgetTrackingTransport(raw_transport)

        report = execute_controlled_staging(
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
            mock_transport=budget_transport,
            skip_live_01=True,
        )

        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertFalse(report.manifest_preserved)
        self.assertEqual(report.cleanup_reserve_used, 14)
        self.assertLessEqual(report.cleanup_reserve_used, 20)
        self.assertEqual(report.http_requests_total, 32)
        self.assertEqual(len(report.errors), 0)

        # Stage verification: LIVE-01 was skipped; LIVE-02..08 were executed
        stage_ids = [s.stage_id for s in report.stages]
        self.assertNotIn("LIVE-01", stage_ids)
        self.assertEqual(
            stage_ids,
            ["LIVE-02", "LIVE-03", "LIVE-04", "LIVE-05", "LIVE-06", "LIVE-07", "LIVE-08"],
        )
        self.assertTrue(all(s.success for s in report.stages))

        # Peak synthetic rows written tracked
        self.assertGreaterEqual(report.synthetic_rows_written_peak, 4)

        # Database is completely clean (zero synthetic remnants)
        cur = conn.cursor()
        for tbl in ("processed_articles", "stories", "run_locks", "durable_recovery_state"):
            cur.execute(f"SELECT COUNT(*) FROM {tbl};")
            self.assertEqual(cur.fetchone()[0], 0, f"Table {tbl} must be clean")

    def test_nominal_full_session_with_live01(self):
        """Full hermetic session (skip_live_01=False) executes all 8 stages with 78 requests."""
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)

        raw_transport = HermeticStagingTransport(conn)
        report = execute_controlled_staging(
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
            mock_transport=raw_transport,
            skip_live_01=False,
        )

        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertFalse(report.manifest_preserved)
        self.assertEqual(report.http_requests_total, 78)
        self.assertEqual(len(report.stages), 8)
        self.assertEqual(report.stages[0].stage_id, "LIVE-01")


class TestTransportInjectionAndSecurityBoundaries(unittest.TestCase):
    """Test transport injection safety, destination allowlist, and real network rejection."""

    def test_real_network_transport_rejected(self):
        """Real network transports (httpx.HTTPTransport) are strictly rejected immediately."""
        real_transport = httpx.HTTPTransport()
        self.addCleanup(real_transport.close)

        with self.assertRaises(PermissionError) as ctx:
            execute_controlled_staging(mock_transport=real_transport)
        self.assertIn("real network transports are strictly prohibited", str(ctx.exception).lower())

    def test_destination_allowlist_validation(self):
        """Destination must match exact allowlisted staging hostname."""
        # Exact valid destinations
        self.assertEqual(
            validate_staging_destination(EXACT_STAGING_DESTINATION),
            f"https://{EXACT_STAGING_HOSTNAME}",
        )
        self.assertEqual(
            validate_staging_destination(f"https://{EXACT_STAGING_HOSTNAME}"),
            f"https://{EXACT_STAGING_HOSTNAME}",
        )

        # Wildcards and alternative hosts rejected
        for bad_url in (
            "libsql://*.turso.io",
            "https://evil.turso.io",
            "https://turso.io",
            "http://127.0.0.1:8080",
            f"https://user:pass@{EXACT_STAGING_HOSTNAME}",
            f"https://{EXACT_STAGING_HOSTNAME}:8080",
            f"https://{EXACT_STAGING_HOSTNAME}/?query=1",
        ):
            with self.subTest(bad_url=bad_url):
                with self.assertRaises(PermissionError):
                    validate_staging_destination(bad_url)

    def test_custom_response_hook_injection_on_mock_transport(self):
        """Custom response hook on mock transport intercepts wire calls cleanly."""
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)

        intercepted_methods: list[str] = []

        def recording_hook(request: httpx.Request) -> httpx.Response | None:
            intercepted_methods.append(request.method)
            return None  # Pass through to HermeticStagingTransport

        raw_transport = HermeticStagingTransport(conn, custom_response_hook=recording_hook)
        report = execute_controlled_staging(
            mock_transport=raw_transport,
            skip_live_01=True,
        )

        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertEqual(len(intercepted_methods), 32)
        self.assertTrue(all(m == "POST" for m in intercepted_methods))


class TestFaultInjectionControlledSlice(unittest.TestCase):
    """Test deterministic fault injection and early-failure stop contracts in LIVE-02..08."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.manifest_dir = Path(self.temp_dir.name)

    def test_live02_commit_timeout_preserves_unknown_and_manifest(self):
        """Injected COMMIT timeout in LIVE-02 stops writes, marks UNKNOWN, preserves manifest, skips cleanup."""
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)

        timeout_transport = HermeticStagingTransport(conn, fail_on_commit_timeout=True)

        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            mock_transport=timeout_transport,
            skip_live_01=True,
        )

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

        # Stage LIVE-08 was skipped (zero calls to cleanup)
        stage_ids = [s.stage_id for s in report.stages]
        self.assertNotIn("LIVE-08", stage_ids)

    def test_live02_commit_network_drop_preserves_unknown(self):
        """Injected COMMIT connection drop in LIVE-02 preserves UNKNOWN and manifest."""
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)

        net_transport = HermeticStagingTransport(conn, fail_on_commit_network=True)

        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            mock_transport=net_transport,
            skip_live_01=True,
        )

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertNotIn("LIVE-08", [s.stage_id for s in report.stages])

    def test_live02_http_500_server_error_preserves_unknown(self):
        """HTTP 500 error on COMMIT preserves UNKNOWN state and skips cleanup."""
        for code in (500, 502, 503, 504):
            with self.subTest(status_code=code):
                conn = _create_isolated_v6_db()
                self.addCleanup(conn.close)

                err_transport = HermeticStagingTransport(conn, fail_on_commit_status=code)
                report = execute_controlled_staging(
                    manifest_dir=self.manifest_dir,
                    mock_transport=err_transport,
                    skip_live_01=True,
                )

                self.assertEqual(report.status, SessionStatus.FAILED)
                self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
                self.assertTrue(report.manifest_preserved)

    def test_live03_rollback_persisted_cleans_up_conditionally(self):
        """Simulated persisted rollback article is conditionally deleted and tracked in LIVE-08."""
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)
        raw_transport = HermeticStagingTransport(conn)

        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            mock_transport=raw_transport,
            skip_live_01=True,
            simulate_rollback_persisted=True,
        )

        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertFalse(report.manifest_preserved)

        # In LIVE-08, 5 entities were deleted (including the unexpectedly persisted rollback entity)
        live08_details = report.stages[-1].details
        self.assertEqual(live08_details.get("entities_deleted"), 5)

        # Database is completely clean: rollback entity and other registered synthetic rows are absent
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles;")
        self.assertEqual(cur.fetchone()[0], 0)

        cur.execute(
            "SELECT COUNT(*) FROM processed_articles WHERE article_id = ?",
            (f"synth_art_{report.run_id}_02_rollback",),
        )
        self.assertEqual(cur.fetchone()[0], 0)

        cur.execute(
            "SELECT COUNT(*) FROM processed_articles WHERE article_id = ?",
            (f"synth_art_{report.run_id}_01",),
        )
        self.assertEqual(cur.fetchone()[0], 0)

        cur.execute(
            "SELECT COUNT(*) FROM stories WHERE story_id = ?",
            (f"synth_story_{report.run_id}_01",),
        )
        self.assertEqual(cur.fetchone()[0], 0)

        cur.execute(
            "SELECT COUNT(*) FROM run_locks WHERE lock_key = ?",
            (f"synth_lock_{report.run_id}",),
        )
        self.assertEqual(cur.fetchone()[0], 0)

        cur.execute(
            "SELECT COUNT(*) FROM durable_recovery_state WHERE state_key = ?",
            (f"synth_stage_{report.run_id}::current",),
        )
        self.assertEqual(cur.fetchone()[0], 0)

    def test_live07_ambiguous_contradictory_halts_before_cleanup(self):
        """Contradictory state in LIVE-07 flags PARTIAL_CORRUPTION_DETECTED and halts before cleanup."""
        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            skip_live_01=True,
            simulate_ambiguous_contradictory=True,
        )

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertNotIn("LIVE-08", [s.stage_id for s in report.stages])

    def test_live07_ambiguous_inspection_failure_halts_fail_closed(self):
        """Reconciliation transport failure in LIVE-07 preserves UNKNOWN and manifest."""
        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            skip_live_01=True,
            simulate_ambiguous_inspection_failure=True,
        )

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertNotIn("LIVE-08", [s.stage_id for s in report.stages])

    def test_live08_ownership_mismatch_halts_immediately(self):
        """Ownership attribute mismatch in LIVE-08a halts with OWNERSHIP_MISMATCH_HALT and skips delete."""
        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            skip_live_01=True,
            simulate_ownership_mismatch=True,
        )

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.OWNERSHIP_MISMATCH_HALT)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_live08_audit_failure_marks_unknown_and_preserves_manifest(self):
        """Transport error during 3-state audit query in LIVE-08c marks UNKNOWN and preserves manifest."""
        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            skip_live_01=True,
            simulate_audit_failure=True,
        )

        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)


class TestCLIExecutionAndDisallowedLiveFlags(unittest.TestCase):
    """Test CLI parser, dry-run flags, and fail-closed rejection of live execution flags."""

    def test_cli_parser_defaults_and_skip_live01_flag(self):
        parser = build_parser()
        args = parser.parse_args([])
        self.assertTrue(args.dry_run)
        self.assertFalse(args.skip_live_01)
        self.assertIsNone(args.recover_manifest)

        args_skip = parser.parse_args(["--skip-live-01"])
        self.assertTrue(args_skip.skip_live_01)

    def test_cli_disallowed_live_flags_fail_closed_code_2(self):
        """Any live or prod flag immediately exits with status code 2 without running."""
        for flag in ("--live", "--execute", "--enable-writes", "--run-live", "--force-live", "--prod", "--production"):
            with self.subTest(flag=flag):
                exit_code = main([flag])
                self.assertEqual(exit_code, 2)

    def test_cli_skip_live01_execution(self):
        """CLI invocation with --skip-live-01 executes controlled staging hermetically."""
        exit_code = main(["--skip-live-01"])
        self.assertEqual(exit_code, 0)


class TestLiveReadyOperatorCoherence(unittest.TestCase):
    """Test that LiveReadyStagingOperator supports the controlled LIVE-02..08 slice with wire guards."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.manifest_dir = Path(self.temp_dir.name)

    def test_live_ready_operator_skip_live01_nominal(self):
        """LiveReadyStagingOperator executes LIVE-02..08 post-unblock with full wire and seal guards."""
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)

        raw = HermeticStagingTransport(conn)
        budget = BudgetTrackingTransport(raw)
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-offline-only",
            transport=budget,
        )
        self.addCleanup(client.close)

        operator = LiveReadyStagingOperator(
            client=client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.manifest_dir,
            live_authorized=False,
        )

        report = operator.run(
            auto_human_window=True,
            human_marker="MOCK_ONLY",
            skip_live_01=True,
        )

        self.assertEqual(report.status, LiveReadyOperatorStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertFalse(report.manifest_preserved)
        self.assertTrue(report.lockdown_recorded)
        self.assertTrue(report.human_window_confirmed)
        self.assertFalse(report.preflight_passed)
        self.assertTrue(report.preflight_assumed)
        self.assertEqual(report.preflight_provenance, "ASSUMED_OFFLINE_MOCK_ONLY")

        # 7 stages executed (LIVE-02 through LIVE-08)
        self.assertEqual(len(report.stages), 7)
        stage_ids = [s.stage_id for s in report.stages]
        self.assertNotIn("LIVE-01", stage_ids)
        self.assertIn("LIVE-02", stage_ids)
        self.assertIn("LIVE-08", stage_ids)


class TestNonProofContractAndArchitectureDistinction(unittest.TestCase):
    """Explicitly assert that hermetic mock tests do NOT prove remote unattended cross-runner recovery."""

    def test_offline_simulation_non_proof_assertion(self):
        """Deterministic assertion: mock transport execution does NOT constitute remote reliability proof."""
        report = execute_controlled_staging(skip_live_01=True)

        # The test passes hermetically
        self.assertEqual(report.status, SessionStatus.PASSED)

        # BUT: mock tests operate strictly on an in-memory SQLite fixture within a single process
        # They do not prove:
        # 1. Unattended cross-runner recovery on ephemeral GitHub Actions VMs (ERD-017 Choice required)
        # 2. Remote Turso cloud network latency, socket drops, or multi-region routing
        # 3. Remote Hrana v2 server-side baton retention across physical host failovers
        self.assertTrue(
            report.destination_url.startswith("https://efso-radar-staging-fatihemres"),
            "Destination must be allowlisted staging host",
        )


class TestPreflightIntegrityAndNegativeRegressions(unittest.TestCase):
    """Negative regression tests proving skipped preflight cannot be reported as verified remote preflight."""

    def setUp(self):
        self.manifest_dir = Path(tempfile.mkdtemp(prefix="efso_staging_preflight_neg_test_"))

    def tearDown(self):
        if self.manifest_dir.exists():
            shutil.rmtree(self.manifest_dir, ignore_errors=True)

    def test_skip_live01_reports_false_preflight_and_explicit_mock_provenance(self):
        """Simulated skip_live_01=True reports preflight_passed=False and ASSUMED_OFFLINE_MOCK_ONLY provenance."""
        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            skip_live_01=True,
        )
        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertFalse(report.preflight_passed)
        self.assertTrue(report.preflight_assumed)
        self.assertEqual(report.preflight_provenance, "ASSUMED_OFFLINE_MOCK_ONLY")

    def test_nominal_full_session_reports_verified_in_session_provenance(self):
        """Nominal session with executed LIVE-01 reports preflight_passed=True and VERIFIED_IN_SESSION provenance."""
        report = execute_controlled_staging(
            manifest_dir=self.manifest_dir,
            skip_live_01=False,
        )
        self.assertEqual(report.status, SessionStatus.PASSED)
        self.assertTrue(report.preflight_passed)
        self.assertFalse(report.preflight_assumed)
        self.assertEqual(report.preflight_provenance, "VERIFIED_IN_SESSION")

    def test_wire_boundary_denies_preflight_completed_without_evidence(self):
        """Security boundary record_preflight_completed fails closed if wire queries are absent."""
        entities = create_default_manifest_entities("run_20261025T150000Z_aaaaaaaaaaaa")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261025T150000Z_aaaaaaaaaaaa",
            created_at="2026-10-25T15:00:00Z",
            registered_entities=entities,
        )
        manifest_path = manifest.save_atomic(self.manifest_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, expected_run_id="run_20261025T150000Z_aaaaaaaaaaaa")
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)
        raw = HermeticStagingTransport(conn)
        boundary = HranaOutboundSecurityBoundary(
            inner=raw,
            seal=seal,
            dynamic_validation=True,
            session_mode=True,
        )
        with self.assertRaises(PermissionError) as ctx:
            boundary.record_preflight_completed(seal.sha256)
        self.assertIn("PREFLIGHT_EVIDENCE_MISSING", str(ctx.exception))

    def test_wire_boundary_simulated_preflight_assumption_gate_checks(self):
        """Security boundary record_simulated_preflight_assumption enforces SHA, marker, and sequence."""
        entities = create_default_manifest_entities("run_20261025T150000Z_bbbbbbbbbbbb")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261025T150000Z_bbbbbbbbbbbb",
            created_at="2026-10-25T15:00:00Z",
            registered_entities=entities,
        )
        manifest_path = manifest.save_atomic(self.manifest_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, expected_run_id="run_20261025T150000Z_bbbbbbbbbbbb")
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)
        raw = HermeticStagingTransport(conn)
        boundary = HranaOutboundSecurityBoundary(
            inner=raw,
            seal=seal,
            dynamic_validation=True,
            session_mode=True,
        )

        # 1. Reject invalid manifest SHA
        with self.assertRaises(PermissionError) as ctx:
            boundary.record_simulated_preflight_assumption(
                manifest_sha256="0" * 64,
                marker="OFFLINE_SIMULATED_PREFLIGHT_ASSUMPTION",
            )
        self.assertIn("MANIFEST_MISMATCH", str(ctx.exception))

        # Re-create fresh boundary
        boundary = HranaOutboundSecurityBoundary(
            inner=raw,
            seal=seal,
            dynamic_validation=True,
            session_mode=True,
        )

        # 2. Reject invalid marker
        with self.assertRaises(PermissionError) as ctx:
            boundary.record_simulated_preflight_assumption(
                manifest_sha256=seal.sha256,
                marker="REMOTE_VERIFIED_AUTHENTICATED",
            )
        self.assertIn("INVALID_PREFLIGHT_ASSUMPTION_MARKER", str(ctx.exception))

        # Re-create fresh boundary
        boundary = HranaOutboundSecurityBoundary(
            inner=raw,
            seal=seal,
            dynamic_validation=True,
            session_mode=True,
        )

        # 3. Successful transition
        boundary.record_simulated_preflight_assumption(
            manifest_sha256=seal.sha256,
            marker="OFFLINE_SIMULATED_PREFLIGHT_ASSUMPTION",
        )
        self.assertFalse(boundary._preflight_passed)
        self.assertTrue(boundary._preflight_assumed_offline)
        self.assertEqual(boundary._stage_sequence_index, 1)

        # 4. Reject re-recording (sequence index != 0)
        with self.assertRaises(PermissionError) as ctx:
            boundary.record_simulated_preflight_assumption(
                manifest_sha256=seal.sha256,
                marker="OFFLINE_SIMULATED_PREFLIGHT_ASSUMPTION",
            )
        self.assertIn("OUT_OF_ORDER_STAGE", str(ctx.exception))

    def test_offline_session_gate_simulated_preflight_assumption_checks(self):
        """OfflineSessionGate record_simulated_preflight_assumption validates SHA, marker, and phase."""
        from efso_radar.staging.offline_session_gate import MockGateDenied, OfflineSessionGate
        digest = "a" * 64
        gate = OfflineSessionGate(expected_manifest_sha256=digest)

        # 1. Reject wrong digest
        with self.assertRaises(MockGateDenied) as ctx:
            gate.record_simulated_preflight_assumption(
                manifest_sha256="b" * 64,
                marker="OFFLINE_SIMULATED_PREFLIGHT_ASSUMPTION",
            )
        self.assertIn("MANIFEST_MISMATCH", str(ctx.exception))

        # Re-create fresh gate
        gate = OfflineSessionGate(expected_manifest_sha256=digest)

        # 2. Reject invalid marker
        with self.assertRaises(MockGateDenied) as ctx:
            gate.record_simulated_preflight_assumption(
                manifest_sha256=digest,
                marker="INVALID_MARKER",
            )
        self.assertIn("INVALID_PREFLIGHT_ASSUMPTION_MARKER", str(ctx.exception))

        # Re-create fresh gate
        gate = OfflineSessionGate(expected_manifest_sha256=digest)

        # 3. Successful assumption
        gate.record_simulated_preflight_assumption(
            manifest_sha256=digest,
            marker="OFFLINE_SIMULATED_PREFLIGHT_ASSUMPTION",
        )
        snap = gate.snapshot()
        self.assertEqual(snap.phase.value, "PREFLIGHT")
        self.assertEqual(snap.requests, 0)  # No phantom requests
        self.assertTrue(snap.preflight_assumed)
    def test_wire_boundary_rejects_mutation_without_preflight_or_assumption(self):
        """Wire boundary rejects any mutating SQL if preflight is neither verified nor assumed."""
        entities = create_default_manifest_entities("run_20261025T150000Z_cccccccccccc")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261025T150000Z_cccccccccccc",
            created_at="2026-10-25T15:00:00Z",
            registered_entities=entities,
        )
        manifest_path = manifest.save_atomic(self.manifest_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, expected_run_id="run_20261025T150000Z_cccccccccccc")
        conn = _create_isolated_v6_db()
        self.addCleanup(conn.close)
        raw = HermeticStagingTransport(conn)
        boundary = HranaOutboundSecurityBoundary(
            inner=raw,
            seal=seal,
            dynamic_validation=True,
            session_mode=True,
        )

        request = httpx.Request(
            "POST",
            "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline",
            json={
                "requests": [
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?, ?)",
                            "args": [
                                {"type": "text", "value": "synth_art_run_20261025T150000Z_cccccccccccc_01"},
                                {"type": "text", "value": "https://example.com/synth/01"},
                                {"type": "text", "value": "SYNTH_ARTICLE_run_20261025T150000Z_cccccccccccc_01"},
                                {"type": "text", "value": "staging_synth_test"},
                                {"type": "text", "value": "2026-10-25T15:00:00Z"},
                                {"type": "text", "value": "2026-10-25T15:00:00Z"},
                            ],
                        },
                    },
                    {"type": "close"},
                ]
            },
        )

        with self.assertRaises(PermissionError) as ctx:
            boundary.handle_request(request)
        self.assertIn("PREFLIGHT_REQUIRED", str(ctx.exception))
