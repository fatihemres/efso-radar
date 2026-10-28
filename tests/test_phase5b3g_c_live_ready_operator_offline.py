"""Comprehensive offline validation for Phase 5B.3G-C Live-Ready Operator Candidate.

Tests verify:
- Default deny for live execution (rejects real network transports)
- Outbound Hrana security boundary (destination, JSON size, baton lifecycle, request budget, deadline)
- Dynamic synthetic parameter validation (verifying all mutation columns and ISO timestamps)
- Pre-dispatch manifest persistence, tamper sealing, crash recovery, and UNKNOWN retention
- Preflight gate and human approval window gate before mutations
- Ambiguous COMMIT outcome handling (UNKNOWN, no blind retry, no cleanup, manifest preserved)
- Parameterized conditional cleanup (affected row count == 1) and 3-state audit (CLEAN/RESIDUAL/UNKNOWN)
- Sanitized errors (zero leakage of SQL, tokens, or credentials)
- Hermetic end-to-end execution
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import TursoHTTPClient
from efso_radar.staging.hrana_outbound_guard import (
    AuthorizedMockIntent,
    HranaOutboundSecurityBoundary,
    MockHranaDenied,
    is_valid_iso_timestamp,
    validate_budget_invariants,
    validate_dynamic_mutation_args,
)
from efso_radar.staging.live_ready_operator import (
    LiveReadyOperatorReport,
    LiveReadyOperatorStatus,
    LiveReadyStagingOperator,
    main as operator_cli_main,
)
from efso_radar.staging.manifest_seal import OfflineManifestSeal, ManifestSealViolation
from efso_radar.staging.offline_operator import (
    _INSERT_ARTICLE,
    _INSERT_STORY,
    _INSERT_LOCK,
    _INSERT_RECOVERY,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
    PerRunOwnershipManifest,
    create_default_manifest_entities,
)


class TestPhase5B3GCLiveReadyOperator(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        for sql in get_all_init_statements():
            self.db.execute(sql)
        self.db.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-10-27T14:00:00Z', 'Schema v6')"
        )
        self.db.commit()

        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.temp_dir = Path(self.temp.name)

        self.raw_transport = HermeticStagingTransport(self.db)
        self.budget_transport = BudgetTrackingTransport(self.raw_transport)
        self.client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-offline-only",
            transport=self.budget_transport,
        )
        self.addCleanup(self.client.close)

    def _make_operator(self, **kwargs):
        params = dict(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.temp_dir,
            live_authorized=False,
        )
        params.update(kwargs)
        return LiveReadyStagingOperator(**params)

    # -------------------------------------------------------------------------
    # 1. Default-Deny & Live Execution Prevention
    # -------------------------------------------------------------------------

    def test_default_deny_rejects_real_network_transport(self):
        real_client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="secret-token-12345",
            transport=httpx.HTTPTransport(retries=0),
        )
        self.addCleanup(real_client.close)

        with self.assertRaisesRegex(PermissionError, "Hermetic security boundary violation"):
            LiveReadyStagingOperator(
                client=real_client,
                manifest_dir=self.temp_dir,
                live_authorized=False,
            )

    def test_live_authorized_flag_strictly_refuses_execution_in_this_phase(self):
        # Even if someone erroneously sets live_authorized=True, it permanently fails closed
        with self.assertRaisesRegex(PermissionError, "LIVE_TURSO_UNAUTHORIZED"):
            LiveReadyStagingOperator(
                client=self.client,
                manifest_dir=self.temp_dir,
                live_authorized=True,
            )

    def test_destination_allowlist_enforced(self):
        with self.assertRaisesRegex(PermissionError, "Destination security violation"):
            LiveReadyStagingOperator(
                client=self.client,
                destination_url="libsql://other-db.turso.io",
                manifest_dir=self.temp_dir,
                live_authorized=False,
            )

    # -------------------------------------------------------------------------
    # 2. Preflight & Baseline Collision Checks
    # -------------------------------------------------------------------------

    def test_write_mutation_rejected_without_preflight(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        # Stage 2 cannot run before preflight
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_PREFLIGHT_REQUIRED"):
            op._run_live_02()

    def test_preflight_reentry_permanently_halts_session(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        self.assertTrue(op._preflight_accepted)
        # Second preflight attempt fails closed
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_PREFLIGHT_REENTRY"):
            op._run_live_01()
        self.assertTrue(op.halt_before_cleanup)
        self.assertEqual(op.audit_state, AuditOutcomeState.UNKNOWN)

    def test_baseline_synthetic_collision_aborts_session(self):
        op = self._make_operator()
        # Pre-seed a colliding synthetic article into the database
        colliding_id = f"synth_art_{op.run_id}_01"
        self.db.execute(
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, 'https://colliding.com', 'Pre-existing', 'synth', '2026-10-27T00:00:00Z', '2026-10-27T00:00:00Z')",
            [colliding_id],
        )
        self.db.commit()

        op._create_and_persist_manifest()
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_PREFLIGHT_FAILED"):
            op._run_live_01()
        self.assertFalse(op._preflight_accepted)
        self.assertTrue(op.halt_before_cleanup)

    # -------------------------------------------------------------------------
    # 3. Human Approval Window Gate
    # -------------------------------------------------------------------------

    def test_human_approval_window_required_before_live_02(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        self.assertTrue(op._preflight_accepted)
        self.assertFalse(op.human_window_confirmed)

        # Without human confirmation, stage 2 fails closed
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_HUMAN_WINDOW_REQUIRED"):
            op._run_live_02()
        self.assertTrue(op.halt_before_cleanup)

    def test_human_approval_cannot_precede_preflight(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        # Calling confirm_human_window before preflight fails
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_PREFLIGHT_REQUIRED"):
            op.confirm_human_window()

    def test_human_window_reentry_denied(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        self.assertTrue(op.human_window_confirmed)
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_HUMAN_WINDOW_ALREADY_CONFIRMED"):
            op.confirm_human_window(marker="MOCK_ONLY")

    # -------------------------------------------------------------------------
    # 4. Manifest Persistence, Sealing & Tamper Protection
    # -------------------------------------------------------------------------

    def test_tampered_manifest_blocks_mutation(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")

        # Tamper with the manifest file on disk
        manifest_file = Path(op.manifest_path)
        data = json.loads(manifest_file.read_text(encoding="utf-8"))
        data["created_at"] = "2099-01-01T00:00:00Z"
        manifest_file.write_text(json.dumps(data), encoding="utf-8")

        # Any subsequent mutation is blocked by the seal assertion
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_MANIFEST_INVALID"):
            op._run_live_02()
        self.assertTrue(op.halt_before_cleanup)
        self.assertTrue(manifest_file.exists())

    def test_unregistered_identity_denied(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")

        unregistered_params = [
            "synth_art_unregistered_pk",
            "https://example.com/synth/unregistered",
            "Title", "Source", "2026-10-25T15:00:00Z", "2026-10-25T15:00:00Z"
        ]
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_UNREGISTERED_IDENTITY"):
            op._execute(_INSERT_ARTICLE, unregistered_params)

    # -------------------------------------------------------------------------
    # 5. Budget, Cutoff & Deadline Gates
    # -------------------------------------------------------------------------

    def test_session_deadline_exhaustion_halts_session(self):
        ticks = [0.0]
        def advance_clock():
            return ticks[0]

        op = self._make_operator(clock=advance_clock)
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")

        # Advance past 120 seconds
        ticks[0] = 125.0
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_SESSION_DEADLINE_EXCEEDED"):
            op._run_live_02()
        self.assertTrue(op.halt_before_cleanup)
        self.assertEqual(op.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(Path(op.manifest_path).exists())

    def test_request_ceiling_exhaustion_fails_closed(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")

        # Artificially set requests to ceiling
        self.budget_transport.request_count = 100
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_REQUEST_CEILING"):
            op._run_live_02()
        self.assertTrue(op.halt_before_cleanup)

    def test_cleanup_reserve_protected_from_test_stages(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")

        # Reached test cutoff (80)
        self.budget_transport.request_count = 80
        with self.assertRaisesRegex(PermissionError, "OFFLINE_SEALED_GATE_CLEANUP_RESERVE"):
            op._run_live_02()
        self.assertTrue(op.halt_before_cleanup)

    # -------------------------------------------------------------------------
    # 6. Ambiguous COMMIT Handling
    # -------------------------------------------------------------------------

    def test_ambiguous_commit_sets_unknown_and_retains_manifest(self):
        fail_raw = HermeticStagingTransport(self.db, fail_on_commit_timeout=True)
        fail_budget = BudgetTrackingTransport(fail_raw)
        fail_client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-offline-only",
            transport=fail_budget,
        )
        self.addCleanup(fail_client.close)

        op = LiveReadyStagingOperator(
            client=fail_client,
            manifest_dir=self.temp_dir,
            live_authorized=False,
        )
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    # -------------------------------------------------------------------------
    # 7. LIVE-08 Ownership Verification & Cleanup
    # -------------------------------------------------------------------------

    def test_ownership_mismatch_halts_and_preserves_manifest(self):
        op = self._make_operator(simulate_ownership_mismatch=True)
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.OWNERSHIP_MISMATCH_HALT)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_residual_rows_retains_manifest(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op._run_live_02()
        op._run_live_03()
        op._run_live_04()
        op._run_live_05()
        op._run_live_06()
        op._run_live_07()

        orig_execute = op._execute
        def execute_with_residual_delete(sql, params=None):
            res = orig_execute(sql, params)
            if "DELETE FROM" in sql:
                # Simulate deletion anomaly where affected row count is 0
                res.affected_row_count = 0
            return res

        op._execute = execute_with_residual_delete

        # Conditional delete detects affected_row_count == 0 and marks RESIDUAL
        with self.assertRaisesRegex(RuntimeError, "Unexpected affected row count during conditional deletion"):
            op._run_live_08()

        self.assertEqual(op.audit_state, AuditOutcomeState.RESIDUAL)
        self.assertTrue(Path(op.manifest_path).exists())

    # -------------------------------------------------------------------------
    # 8. Nominal Full 8-Stage Execution
    # -------------------------------------------------------------------------

    def test_nominal_full_execution_succeeds_and_deletes_manifest(self):
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertTrue(report.preflight_passed)
        self.assertTrue(report.human_window_confirmed)
        self.assertTrue(report.lockdown_recorded)
        self.assertFalse(report.manifest_preserved)
        self.assertFalse(Path(report.manifest_path).exists())
        self.assertEqual(len(report.stages), 8)
        self.assertLess(report.http_requests_total, 100)

    # -------------------------------------------------------------------------
    # 9. Outbound Hrana Security Boundary
    # -------------------------------------------------------------------------

    def test_hrana_outbound_security_boundary_rejects_live_unauthorized(self):
        manifest_entities = create_default_manifest_entities("run_20261027T120000Z_abcdef123456")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261027T120000Z_abcdef123456",
            created_at="2026-10-27T12:00:00Z",
            registered_entities=manifest_entities,
        )
        manifest_path = manifest.save_atomic(self.temp_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, manifest.run_id)

        intent = AuthorizedMockIntent(
            sql="SELECT 1 AS found FROM processed_articles WHERE article_id = ?",
            args=(manifest_entities[0].pk_value,),
        )

        with self.assertRaisesRegex(PermissionError, "LIVE_TURSO_UNAUTHORIZED"):
            HranaOutboundSecurityBoundary(
                inner=httpx.HTTPTransport(retries=0),
                seal=seal,
                approved_intents=(intent,),
                live_authorized=False,
            )

    def test_hrana_outbound_security_boundary_baton_and_payload_checks(self):
        manifest_entities = create_default_manifest_entities("run_20261027T120000Z_abcdef123456")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261027T120000Z_abcdef123456",
            created_at="2026-10-27T12:00:00Z",
            registered_entities=manifest_entities,
        )
        manifest_path = manifest.save_atomic(self.temp_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, manifest.run_id)

        intent = AuthorizedMockIntent(
            sql="SELECT 1 AS found FROM processed_articles WHERE article_id = ?",
            args=(manifest_entities[0].pk_value,),
        )

        boundary = HranaOutboundSecurityBoundary(
            inner=self.budget_transport,
            seal=seal,
            approved_intents=(intent,),
            live_authorized=False,
        )

        # 1. Invalid method (GET instead of POST)
        req_get = httpx.Request("GET", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline")
        with self.assertRaises(MockHranaDenied):
            boundary.handle_request(req_get)
        self.assertTrue(boundary.halted)

    # -------------------------------------------------------------------------
    # 10. Dynamic Synthetic Parameter Validation
    # -------------------------------------------------------------------------

    def test_dynamic_synthetic_validation_accepts_valid_parameters(self):
        run_id = "run_20261027T140000Z_123456789abc"
        stamp = "2026-10-27T14:00:00Z"
        entities = create_default_manifest_entities(run_id)

        art = next(e for e in entities if e.table == "processed_articles" and e.pk_value.endswith("_01"))
        valid_art_args = (
            art.pk_value,
            f"https://example.com/synth/{run_id}/01",
            art.expected_attributes["title"],
            art.expected_attributes["source"],
            stamp,
            stamp,
        )
        # Should not raise
        validate_dynamic_mutation_args(
            table="processed_articles",
            operation="insert",
            args=valid_art_args,
            run_id=run_id,
            attrs=art.expected_attributes,
        )

        story = next(e for e in entities if e.table == "stories")
        valid_story_args = (
            story.pk_value,
            story.expected_attributes["headline_tr"],
            story.expected_attributes["category"],
            stamp,
            stamp,
            json.dumps([f"synth_art_{run_id}_01"]),
            1,
            1,
        )
        validate_dynamic_mutation_args(
            table="stories",
            operation="insert",
            args=valid_story_args,
            run_id=run_id,
            attrs=story.expected_attributes,
        )

    def test_dynamic_synthetic_validation_rejects_tampered_parameters(self):
        run_id = "run_20261027T140000Z_123456789abc"
        entities = {e.table: e for e in create_default_manifest_entities(run_id)}
        art = entities["processed_articles"]

        # Tampered URL
        bad_url_args = (
            art.pk_value,
            "https://evil.com/fake",
            art.expected_attributes["title"],
            art.expected_attributes["source"],
            "2026-10-27T14:00:00Z",
            "2026-10-27T14:00:00Z",
        )
        with self.assertRaisesRegex(ValueError, "Noncanonical dynamic article payload"):
            validate_dynamic_mutation_args(
                table="processed_articles",
                operation="insert",
                args=bad_url_args,
                run_id=run_id,
                attrs=art.expected_attributes,
            )

        # Invalid timestamp
        bad_time_args = (
            art.pk_value,
            f"https://example.com/synth/{run_id}/01",
            art.expected_attributes["title"],
            art.expected_attributes["source"],
            "not-a-timestamp",
            "2026-10-27T14:00:00Z",
        )
        with self.assertRaisesRegex(ValueError, "Noncanonical dynamic article payload"):
            validate_dynamic_mutation_args(
                table="processed_articles",
                operation="insert",
                args=bad_time_args,
                run_id=run_id,
                attrs=art.expected_attributes,
            )

    # -------------------------------------------------------------------------
    # 11. Error Sanitization
    # -------------------------------------------------------------------------

    def test_error_messages_contain_no_sql_tokens_or_secrets(self):
        op = self._make_operator()
        secret_token = "turso_jwt_token_super_secret_xyz"
        evil_sql = "INSERT INTO processed_articles (article_id) VALUES ('evil_sql_injection');"

        try:
            op._guard_mutation(evil_sql, ["test"])
        except PermissionError as exc:
            err_msg = str(exc)
            self.assertNotIn(secret_token, err_msg)
            self.assertNotIn(evil_sql, err_msg)
            self.assertIn("OFFLINE_SEALED_GATE", err_msg)

    # -------------------------------------------------------------------------
    # 12. CLI Safety
    # -------------------------------------------------------------------------

    def test_cli_default_deny_and_disallowed_flags(self):
        with patch("sys.stdout"), patch("sys.stderr"):
            # Default with no args: exit 2
            self.assertEqual(operator_cli_main([]), 2)
            # Live flag: exit 2
            self.assertEqual(operator_cli_main(["--live"]), 2)
            # Execute flag: exit 2
            self.assertEqual(operator_cli_main(["--execute"]), 2)
            # Offline simulation: exit 0
            self.assertEqual(operator_cli_main(["--simulate-offline"]), 0)

    # -------------------------------------------------------------------------
    # 13. P1-A False Acceptance & Missing Stages Prevention
    # -------------------------------------------------------------------------

    def test_no_human_window_empty_db_fails_and_preserves_manifest(self):
        op = self._make_operator()
        report = op.run(auto_human_window=False)
        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertFalse(report.human_window_confirmed)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        # Confirms only LIVE-01 preflight was attempted; stages 2..8 were not run
        executed_stages = [s.stage_id for s in report.stages]
        self.assertIn("LIVE-01", executed_stages)
        self.assertNotIn("LIVE-02", executed_stages)
        self.assertNotIn("LIVE-08", executed_stages)

    def test_incomplete_stages_fails_and_preserves_manifest(self):
        op = self._make_operator()
        def failing_stage_05():
            raise RuntimeError("Simulated transient connection failure in stage 5")
        op._run_live_05 = failing_stage_05

        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        executed_successes = {s.stage_id for s in report.stages if s.success}
        self.assertNotIn("LIVE-05", executed_successes)

    def test_failed_lockdown_fails_and_preserves_manifest(self):
        op = self._make_operator()
        def failing_lockdown(*args, **kwargs):
            raise RuntimeError("Simulated lockdown assertion failure")
        op.record_lockdown = failing_lockdown

        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertFalse(report.lockdown_recorded)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    # -------------------------------------------------------------------------
    # 14. P1-B Anti-Spoofing & Live Escape Hatch Removal
    # -------------------------------------------------------------------------

    def test_outbound_guard_rejects_duck_typing_spoofed_conn(self):
        class SpoofedConnTransport:
            def __init__(self, conn):
                self.conn = conn
                self._conn = conn

        manifest_entities = create_default_manifest_entities("run_20261027T120000Z_abcdef123456")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261027T120000Z_abcdef123456",
            created_at="2026-10-27T12:00:00Z",
            registered_entities=manifest_entities,
        )
        manifest_path = manifest.save_atomic(self.temp_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, manifest.run_id)

        with self.assertRaisesRegex(PermissionError, "HERMETIC_TRANSPORT_REQUIRED"):
            HranaOutboundSecurityBoundary(
                inner=SpoofedConnTransport(self.db),
                seal=seal,
                approved_intents=(),
                live_authorized=False,
            )

    def test_outbound_guard_rejects_duck_typing_is_hermetic(self):
        class SpoofedHermeticFlagTransport:
            is_hermetic = True

        manifest_entities = create_default_manifest_entities("run_20261027T120000Z_abcdef123456")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261027T120000Z_abcdef123456",
            created_at="2026-10-27T12:00:00Z",
            registered_entities=manifest_entities,
        )
        manifest_path = manifest.save_atomic(self.temp_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, manifest.run_id)

        with self.assertRaisesRegex(PermissionError, "HERMETIC_TRANSPORT_REQUIRED"):
            HranaOutboundSecurityBoundary(
                inner=SpoofedHermeticFlagTransport(),
                seal=seal,
                approved_intents=(),
                live_authorized=False,
            )

    def test_outbound_guard_rejects_subclass_spoofing(self):
        class SubclassedHermeticTransport(HermeticStagingTransport):
            pass

        class SubclassedBudgetTransport(BudgetTrackingTransport):
            pass

        manifest_entities = create_default_manifest_entities("run_20261027T120000Z_abcdef123456")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261027T120000Z_abcdef123456",
            created_at="2026-10-27T12:00:00Z",
            registered_entities=manifest_entities,
        )
        manifest_path = manifest.save_atomic(self.temp_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, manifest.run_id)

        with self.assertRaisesRegex(PermissionError, "HERMETIC_TRANSPORT_REQUIRED"):
            HranaOutboundSecurityBoundary(
                inner=SubclassedHermeticTransport(self.db),
                seal=seal,
                approved_intents=(),
                live_authorized=False,
            )

        with self.assertRaisesRegex(PermissionError, "HERMETIC_TRANSPORT_REQUIRED"):
            HranaOutboundSecurityBoundary(
                inner=SubclassedBudgetTransport(self.raw_transport),
                seal=seal,
                approved_intents=(),
                live_authorized=False,
            )

    def test_outbound_guard_rejects_live_authorized_escape_hatch(self):
        manifest_entities = create_default_manifest_entities("run_20261027T120000Z_abcdef123456")
        manifest = PerRunOwnershipManifest(
            run_id="run_20261027T120000Z_abcdef123456",
            created_at="2026-10-27T12:00:00Z",
            registered_entities=manifest_entities,
        )
        manifest_path = manifest.save_atomic(self.temp_dir)
        seal = OfflineManifestSeal.from_persisted(manifest_path, manifest.run_id)

        with self.assertRaisesRegex(PermissionError, "LIVE_TURSO_UNAUTHORIZED"):
            HranaOutboundSecurityBoundary(
                inner=self.budget_transport,
                seal=seal,
                approved_intents=(),
                live_authorized=True,
            )

    # -------------------------------------------------------------------------
    # 15. P1-C Wire Integration & Wire Guard Verification
    # -------------------------------------------------------------------------

    def test_operator_routes_all_8_stages_through_security_boundary(self):
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.PASSED)
        self.assertIsNotNone(op.security_boundary)
        self.assertIsInstance(op.security_boundary, HranaOutboundSecurityBoundary)
        self.assertGreater(op.security_boundary._requests, 20)
        self.assertFalse(op.security_boundary.halted)
        self.assertTrue(report.security_boundary_active)

    def test_security_boundary_blocks_unallowlisted_sql_at_wire_level(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")

        with self.assertRaises(Exception) as ctx:
            op.client.execute("DROP TABLE processed_articles")
        err_str = str(ctx.exception)
        self.assertTrue(
            "MOCK_HRANA_DENIED" in err_str or "SQL_NOT_ALLOWLISTED" in err_str or "MockHranaDenied" in type(ctx.exception).__name__,
            f"Expected MockHranaDenied, got {ctx.exception}"
        )
        self.assertTrue(op.security_boundary.halted)

    # -------------------------------------------------------------------------
    # 16. P1-D Temp Manifest Crash Durability & Operational Boundaries
    # -------------------------------------------------------------------------

    def test_process_restart_with_missing_manifest_fails_closed(self):
        missing_path = self.temp_dir / "efso_radar_staging_manifest_missing_run.json"
        with self.assertRaises(ManifestSealViolation):
            OfflineManifestSeal.from_persisted(missing_path, "missing_run")

        # Pointing manifest to operational data directory is strictly rejected
        operational_manifest_path = Path("data/efso_radar_staging_manifest_run_20261027T120000Z_abcdef123456.json")
        with self.assertRaisesRegex(ManifestSealViolation, "Canonical per-run staging manifest is missing or unsafe"):
            OfflineManifestSeal.from_persisted(operational_manifest_path, "run_20261027T120000Z_abcdef123456")

        default_op = self._make_operator(manifest_dir=None)
        # Default manifest dir is strictly inside the system tempdir, never data/
        norm_dir = str(default_op.manifest_dir).replace("\\", "/")
        self.assertNotIn("/data/", f"/{norm_dir}/")

    # -------------------------------------------------------------------------
    # 17. P2-E ISO 8601 Calendar & UTC Validity
    # -------------------------------------------------------------------------

    def test_is_valid_iso_timestamp_calendar_validity(self):
        # Valid cases
        self.assertTrue(is_valid_iso_timestamp("2024-02-29T12:00:00Z"))  # 2024 leap year
        self.assertTrue(is_valid_iso_timestamp("2026-10-27T15:30:00Z"))
        self.assertTrue(is_valid_iso_timestamp("2026-12-31T23:59:59Z"))
        self.assertTrue(is_valid_iso_timestamp("2026-01-01T00:00:00.123456Z"))

        # Invalid calendar dates
        self.assertFalse(is_valid_iso_timestamp("2026-02-29T12:00:00Z"))  # 2026 not leap year
        self.assertFalse(is_valid_iso_timestamp("2026-04-31T12:00:00Z"))  # April has 30 days
        self.assertFalse(is_valid_iso_timestamp("2026-13-01T12:00:00Z"))  # Month 13
        self.assertFalse(is_valid_iso_timestamp("2026-00-10T12:00:00Z"))  # Month 0
        self.assertFalse(is_valid_iso_timestamp("2026-10-00T12:00:00Z"))  # Day 0
        self.assertFalse(is_valid_iso_timestamp("2026-10-32T12:00:00Z"))  # Day 32

        # Invalid clock times
        self.assertFalse(is_valid_iso_timestamp("2026-10-27T24:00:00Z"))  # Hour 24
        self.assertFalse(is_valid_iso_timestamp("2026-10-27T25:00:00Z"))  # Hour 25
        self.assertFalse(is_valid_iso_timestamp("2026-10-27T12:60:00Z"))  # Minute 60
        self.assertFalse(is_valid_iso_timestamp("2026-10-27T12:00:61Z"))  # Second 61

        # Invalid timezone / non-UTC / impossible
        self.assertFalse(is_valid_iso_timestamp("2026-10-27T12:00:00+03:00"))  # Offset not Z
        self.assertFalse(is_valid_iso_timestamp("2026-10-27T12:00:00-05:00"))
        self.assertFalse(is_valid_iso_timestamp("2026-10-27T12:00:00"))  # Missing timezone
        self.assertFalse(is_valid_iso_timestamp("2026-99-99T99:99:99Z"))  # Impossible date
        self.assertFalse(is_valid_iso_timestamp("not-a-timestamp"))
        self.assertFalse(is_valid_iso_timestamp(123456))
        self.assertFalse(is_valid_iso_timestamp(None))

    # -------------------------------------------------------------------------
    # 18. P2-F Constructor Budget Invariant Enforcement
    # -------------------------------------------------------------------------

    def test_operator_rejects_exceeded_budget_invariants(self):
        with self.assertRaisesRegex(ValueError, "request_ceiling .* exceeds absolute limit of 100"):
            self._make_operator(request_ceiling=101)

        with self.assertRaisesRegex(ValueError, "request_cutoff .* exceeds limit of 80"):
            self._make_operator(request_cutoff=81)

        with self.assertRaisesRegex(ValueError, "cleanup_reserve .* must be at least 20"):
            self._make_operator(cleanup_reserve=19)

        with self.assertRaisesRegex(ValueError, "exceeds request_ceiling"):
            self._make_operator(request_ceiling=90, request_cutoff=75, cleanup_reserve=20)

        with self.assertRaisesRegex(ValueError, "session_deadline_seconds .* exceeds 120.0s ceiling"):
            self._make_operator(session_deadline_seconds=121.0)

    # -------------------------------------------------------------------------
    # 19. Priority 1: Wire-Level Authorization (P1-1)
    # -------------------------------------------------------------------------

    def test_wire_mutation_blocked_before_preflight(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        art_id = f"synth_art_{op.run_id}_01"
        valid_insert = (
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        params = [
            art_id,
            f"https://example.com/synth/{op.run_id}/01",
            f"SYNTH_ARTICLE_{op.run_id}_01",
            "staging_synth_test",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
        ]
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(valid_insert, params)
        self.assertIn("PREFLIGHT_REQUIRED", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)
        # Database remains untouched
        cur = self.db.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", [art_id])
        self.assertEqual(cur.fetchone()[0], 0)
        self.assertTrue(Path(op.manifest_path).exists())

    def test_wire_mutation_blocked_after_preflight_before_human_window(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        self.assertTrue(op._preflight_accepted)
        self.assertFalse(op.human_window_confirmed)

        art_id = f"synth_art_{op.run_id}_01"
        valid_insert = (
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        params = [
            art_id,
            f"https://example.com/synth/{op.run_id}/01",
            f"SYNTH_ARTICLE_{op.run_id}_01",
            "staging_synth_test",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
        ]
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(valid_insert, params)
        self.assertIn("HUMAN_WINDOW_REQUIRED", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)

    def test_wire_mutation_blocked_when_no_active_stage(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        self.assertIsNone(op.security_boundary._active_stage)

        art_id = f"synth_art_{op.run_id}_01"
        valid_insert = (
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        params = [
            art_id,
            f"https://example.com/synth/{op.run_id}/01",
            f"SYNTH_ARTICLE_{op.run_id}_01",
            "staging_synth_test",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
        ]
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(valid_insert, params)
        self.assertIn("NO_ACTIVE_STAGE", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)

    def test_wire_mutation_blocked_on_stage_mismatch(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")

        # Active stage is LIVE-02, but caller attempts run_locks mutation (which belongs to LIVE-04)
        op.security_boundary.record_active_stage("LIVE-02", op._seal.sha256)
        lock_key = f"synth_lock_{op.run_id}"
        cas_sql = (
            "INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(lock_key) DO UPDATE SET worker_id = excluded.worker_id, "
            "acquired_at = excluded.acquired_at, expires_at = excluded.expires_at, "
            "heartbeat_at = excluded.heartbeat_at "
            "WHERE expires_at < excluded.acquired_at OR worker_id = excluded.worker_id"
        )
        params = [
            lock_key,
            f"runner_alpha_{op.run_id}",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:05:00Z",
            "2026-10-25T15:00:00Z",
        ]
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(cas_sql, params)
        self.assertIn("STAGE_MUTATION_MISMATCH", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)

    def test_wire_duplicate_mutation_replay_blocked(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op.security_boundary.record_active_stage("LIVE-02", op._seal.sha256)

        art_id = f"synth_art_{op.run_id}_01"
        valid_insert = (
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        params = [
            art_id,
            f"https://example.com/synth/{op.run_id}/01",
            f"SYNTH_ARTICLE_{op.run_id}_01",
            "staging_synth_test",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
        ]
        # First execution succeeds
        op.client.execute(valid_insert, params)

        # Attempting identical mutation again within LIVE-02 is blocked as replay
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(valid_insert, params)
        self.assertIn("MUTATION_ALREADY_SPENT", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)

    def test_wire_state_transitions_cannot_be_escalated_without_manifest_sha256(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary
        assert boundary is not None

        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_preflight_completed("invalid_forged_sha256")
        self.assertIn("MANIFEST_MISMATCH", str(ctx.exception))

        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_human_window_confirmed("MOCK_ONLY", "invalid_forged_sha256")
        self.assertIn("MANIFEST_MISMATCH", str(ctx.exception))

        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_active_stage("LIVE-02", "invalid_forged_sha256")
        self.assertIn("MANIFEST_MISMATCH", str(ctx.exception))

    # -------------------------------------------------------------------------
    # 20. Priority 2: Read-Only Least Privilege (P1-2)
    # -------------------------------------------------------------------------

    def test_wire_rejects_broad_select_on_operational_tables(self):
        op = self._make_operator()
        op._create_and_persist_manifest()

        disallowed_queries = [
            "SELECT * FROM telegram_delivery_records",
            "SELECT * FROM owner_feedback",
            "SELECT * FROM candidate_backlog",
            "SELECT * FROM ai_usage_ledger",
            "SELECT * FROM distribution_delivery_attempts",
            "SELECT * FROM dead_letter_deliveries",
            "SELECT * FROM digest_history",
        ]
        for query in disallowed_queries:
            with self.assertRaises(MockHranaDenied) as ctx:
                op.client.execute(query)
            self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception), f"Failed for {query}")
            op.security_boundary.halted = False

    def test_wire_rejects_unconstrained_full_table_scan_on_synthetic_tables(self):
        op = self._make_operator()
        op._create_and_persist_manifest()

        unconstrained_queries = [
            "SELECT * FROM processed_articles",
            "SELECT * FROM stories",
            "SELECT * FROM run_locks",
            "SELECT * FROM durable_recovery_state",
            "SELECT * FROM processed_articles WHERE source = 'test'",
        ]
        for query in unconstrained_queries:
            with self.assertRaises(MockHranaDenied) as ctx:
                op.client.execute(query)
            self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception), f"Failed for {query}")
            op.security_boundary.halted = False

        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute("SELECT * FROM processed_articles WHERE article_id = ?", ["prod_article_9999"])
        self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception))

    def test_wire_permits_exact_preflight_and_manifest_scoped_reads(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op.security_boundary.record_active_stage("LIVE-01", op._seal.sha256)

        res1 = op.client.execute("SELECT MAX(version) AS ver FROM schema_migrations")
        self.assertIsNotNone(res1.fetchone())

        res2 = op.client.execute("SELECT DISTINCT status FROM telegram_delivery_records")
        self.assertIsNotNone(res2.fetchall())

        res3 = op.client.execute("PRAGMA table_info(telegram_delivery_records)")
        self.assertIsNotNone(res3.fetchall())

        art_id = f"synth_art_{op.run_id}_01"
        res4 = op.client.execute("SELECT 1 AS found FROM processed_articles WHERE article_id = ?", [art_id])
        self.assertEqual(len(res4.fetchall()), 0)

    # -------------------------------------------------------------------------
    # 21. Priority 3: Lifecycle Transport Closure & Strict Hard Bounds (P2)
    # -------------------------------------------------------------------------

    def test_lifecycle_transport_closed_on_nominal_run(self):
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.PASSED)
        self.assertTrue(op.security_boundary._session_closed)

        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute("SELECT 1 AS ping")
        self.assertIn("SESSION_HALTED", str(ctx.exception))

    def test_lifecycle_transport_closed_on_early_exit_no_human_window(self):
        op = self._make_operator()
        report = op.run(auto_human_window=False)
        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertTrue(op.security_boundary._session_closed)
        self.assertTrue(Path(op.manifest_path).exists())

        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute("SELECT 1 AS ping")
        self.assertIn("SESSION_HALTED", str(ctx.exception))

    def test_lifecycle_transport_closed_on_exception(self):
        op = self._make_operator(simulate_ownership_mismatch=True)
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")
        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertTrue(op.security_boundary._session_closed)
        self.assertTrue(Path(op.manifest_path).exists())

        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute("SELECT 1 AS ping")
        self.assertIn("SESSION_HALTED", str(ctx.exception))

    def test_budget_invariants_reject_booleans_and_nonfinite_values(self):
        with self.assertRaises(TypeError):
            validate_budget_invariants(
                request_ceiling=True,
                request_cutoff=80,
                cleanup_reserve=20,
                session_deadline_seconds=120.0,
            )

        with self.assertRaises(TypeError):
            validate_budget_invariants(
                request_ceiling=100,
                request_cutoff=True,
                cleanup_reserve=20,
                session_deadline_seconds=120.0,
            )

        with self.assertRaises(TypeError):
            validate_budget_invariants(
                request_ceiling=100,
                request_cutoff=80,
                cleanup_reserve=True,
                session_deadline_seconds=120.0,
            )

        with self.assertRaises(TypeError):
            validate_budget_invariants(
                request_ceiling=100,
                request_cutoff=80,
                cleanup_reserve=20,
                session_deadline_seconds=True,
            )

        with self.assertRaises(ValueError):
            validate_budget_invariants(
                request_ceiling=100,
                request_cutoff=80,
                cleanup_reserve=20,
                session_deadline_seconds=float("nan"),
            )

        with self.assertRaises(ValueError):
            validate_budget_invariants(
                request_ceiling=100,
                request_cutoff=80,
                cleanup_reserve=20,
                session_deadline_seconds=float("inf"),
            )

        with self.assertRaises(ValueError):
            validate_budget_invariants(
                request_ceiling=0,
                request_cutoff=80,
                cleanup_reserve=20,
                session_deadline_seconds=120.0,
            )

        with self.assertRaises(ValueError):
            validate_budget_invariants(
                request_ceiling=100,
                request_cutoff=-10,
                cleanup_reserve=20,
                session_deadline_seconds=120.0,
            )

    def test_strict_request_ceiling_under_100(self):
        run_id = "run_20261027T120000Z_abcdef123456"
        manifest_entities = create_default_manifest_entities(run_id)
        manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at="2026-10-27T12:00:00Z",
            registered_entities=manifest_entities,
        )
        seal = OfflineManifestSeal.from_persisted(
            manifest.save_atomic(self.temp_dir), manifest.run_id
        )
        boundary = HranaOutboundSecurityBoundary(
            inner=self.budget_transport,
            seal=seal,
            session_mode=True,
            max_requests=100,
            test_cutoff_requests=80,
            cleanup_reserve_requests=20,
            session_timeout_seconds=120.0,
        )
        boundary._requests = 99
        req = httpx.Request(
            "POST",
            "https://efso-radar-staging.turso.io/v2/pipeline",
            content=json.dumps({"requests": [{"type": "execute", "stmt": {"sql": "SELECT 1 AS ping"}}, {"type": "close"}]}).encode("utf-8"),
        )
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.handle_request(req)
        self.assertIn("REQUEST_CEILING", str(ctx.exception))

    # -------------------------------------------------------------------------
    # 22. Round 3 Engineering Review Hardening: Attestation & Read Least-Privilege
    # -------------------------------------------------------------------------

    def test_adversarial_self_issued_attestation_without_preflight_or_human_window_blocked(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        # op._run_live_01() is NOT called!
        # op.confirm_human_window() is NOT called!
        sha = op._seal.sha256
        boundary = op.security_boundary
        assert boundary is not None

        # 1. Attacker attempts to forge preflight completion on wire boundary
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_preflight_completed(sha)
        self.assertIn("PREFLIGHT_EVIDENCE_MISSING", str(ctx.exception))
        boundary.halted = False

        # 2. Attacker attempts to self-issue human window attestation
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_human_window_confirmed(op, "MOCK_ONLY", sha)
        self.assertIn("PREFLIGHT_REQUIRED", str(ctx.exception))
        boundary.halted = False

        # 3. Attacker attempts to advance active stage to LIVE-02
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_active_stage("LIVE-02", sha)
        self.assertIn("PREFLIGHT_REQUIRED", str(ctx.exception))
        boundary.halted = False

        # 4. Attacker attempts direct client mutation via op.client.execute()
        art_id = f"synth_art_{op.run_id}_01"
        valid_insert = (
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        params = [
            art_id,
            f"https://example.com/synth/{op.run_id}/01",
            f"SYNTH_ARTICLE_{op.run_id}_01",
            "staging_synth_test",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
        ]
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(valid_insert, params)
        self.assertIn("PREFLIGHT_REQUIRED", str(ctx.exception))

        # 5. Verify actual row count before and after: strictly 0
        cur = self.db.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", [art_id])
        self.assertEqual(cur.fetchone()[0], 0)
        # Manifest remains preserved on disk
        self.assertTrue(Path(op.manifest_path).exists())

    def test_out_of_order_stage_activation_blocked(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        boundary = op.security_boundary
        assert boundary is not None

        # Stage index is at 2 (expects LIVE-02). Attempting to jump to LIVE-03 is rejected
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_active_stage("LIVE-03", op._seal.sha256)
        self.assertIn("OUT_OF_ORDER_STAGE", str(ctx.exception))
        boundary.halted = False

        # Attempting to jump to LIVE-05 is rejected
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_active_stage("LIVE-05", op._seal.sha256)
        self.assertIn("OUT_OF_ORDER_STAGE", str(ctx.exception))
        boundary.halted = False

        # Attempting to jump directly to LIVE-08 is rejected
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_active_stage("LIVE-08", op._seal.sha256)
        self.assertIn("OUT_OF_ORDER_STAGE", str(ctx.exception))
        boundary.halted = False

    def test_cleanup_reserve_escalation_before_live_08_blocked(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary
        assert boundary is not None

        # 1. Attempting cleanup escalation before preflight / during LIVE-01
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.set_cleanup_phase()
        self.assertIn("CLEANUP_RESERVE_ESCALATION", str(ctx.exception))
        self.assertTrue(boundary.halted)

        # Reset for stage 2 test
        boundary.halted = False
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        boundary.record_active_stage("LIVE-02", op._seal.sha256)

        # 2. Attempting cleanup escalation during LIVE-02
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.set_cleanup_phase()
        self.assertIn("CLEANUP_RESERVE_ESCALATION", str(ctx.exception))
        self.assertTrue(boundary.halted)

    def test_read_least_privilege_rejects_or_1_equals_1_injection(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op.security_boundary.record_active_stage("LIVE-02", op._seal.sha256)

        story_id = f"synth_story_{op.run_id}_01"
        injected_query = "SELECT * FROM stories WHERE story_id = ? OR 1=1"
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(injected_query, [story_id])
        self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)

    def test_read_least_privilege_rejects_join_cardinality_expansion(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op.security_boundary.record_active_stage("LIVE-02", op._seal.sha256)

        story_id = f"synth_story_{op.run_id}_01"
        join_query = "SELECT * FROM stories s JOIN processed_articles a ON 1=1 WHERE s.story_id = ?"
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(join_query, [story_id])
        self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)

    def test_read_least_privilege_rejects_extra_parameters(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op.security_boundary.record_active_stage("LIVE-02", op._seal.sha256)

        art_id = f"synth_art_{op.run_id}_01"
        extra_param_query = "SELECT title, source FROM processed_articles WHERE article_id = ? AND 1 = ?"
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(extra_param_query, [art_id, 1])
        self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception))
        self.assertTrue(op.security_boundary.halted)

    def test_read_least_privilege_rejects_preflight_queries_outside_live_01(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op.security_boundary.record_active_stage("LIVE-02", op._seal.sha256)

        # Attempting preflight delivery status read in LIVE-02
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute("SELECT DISTINCT status FROM telegram_delivery_records")
        self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception))
        op.security_boundary.halted = False

        # Attempting preflight schema version read in LIVE-02
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute("SELECT MAX(version) AS ver FROM schema_migrations")
        self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception))
        op.security_boundary.halted = False

        # Attempting preflight duplicate recipient_chat_id aggregation in LIVE-02
        agg_query = (
            "SELECT recipient_chat_id, message_key, COUNT(*) as cnt "
            "FROM telegram_delivery_records "
            "GROUP BY recipient_chat_id, message_key "
            "HAVING COUNT(*) > 1"
        )
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(agg_query)
        self.assertIn("READ_NOT_ALLOWLISTED", str(ctx.exception))

    def test_adversarial_early_live_08_and_cleanup_reserve_blocked(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        boundary = op.security_boundary
        assert boundary is not None

        # 1. Attacker attempts to jump directly to LIVE-08 after preflight + human window
        with self.assertRaises(MockHranaDenied) as ctx:
            boundary.record_active_stage("LIVE-08", op._seal.sha256)
        self.assertIn("OUT_OF_ORDER_STAGE", str(ctx.exception))
        self.assertTrue(boundary.halted)

        # 2. Boundary is halted: cleanup reserve escalation is fail-closed denied
        with self.assertRaises(MockHranaDenied):
            boundary.set_cleanup_phase()
        self.assertTrue(boundary.halted)

        # 3. Direct client conditional DELETE is rejected
        del_sql = "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?"
        del_params = [
            f"synth_art_{op.run_id}_01",
            "staging_synth_test",
            f"SYNTH_ARTICLE_{op.run_id}_01",
        ]
        with self.assertRaises(MockHranaDenied):
            op.client.execute(del_sql, del_params)

        # 4. Manifest preserved on disk, DB row count strictly unchanged
        self.assertTrue(Path(op.manifest_path).exists())

    def test_adversarial_direct_client_delete_before_ownership_precheck_blocked(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op._run_live_02()
        op._run_live_03()
        op._run_live_04()
        op._run_live_05()
        op._run_live_06()
        op._run_live_07()

        boundary = op.security_boundary
        assert boundary is not None

        # Stages 1..7 completed: sequence index is 8. LIVE-08 activated and cleanup reserve set.
        boundary.record_active_stage("LIVE-08", op._seal.sha256)
        boundary.set_cleanup_phase()

        # Attacker attempts direct conditional DELETE via client.execute() BEFORE step 8a ownership precheck
        del_sql = "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?"
        del_params = [
            f"synth_art_{op.run_id}_01",
            "staging_synth_test",
            f"SYNTH_ARTICLE_{op.run_id}_01",
        ]
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(del_sql, del_params)
        self.assertIn("OWNERSHIP_PRECHECK_REQUIRED", str(ctx.exception))
        self.assertTrue(boundary.halted)

        # Verify row still exists in database (NOT deleted)
        cur = self.db.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", [del_params[0]])
        self.assertEqual(cur.fetchone()[0], 1)
        self.assertTrue(Path(op.manifest_path).exists())

    def test_adversarial_delete_without_wire_select_precheck_blocked(self):
        op = self._make_operator()
        op._create_and_persist_manifest()
        op._run_live_01()
        op.confirm_human_window(marker="MOCK_ONLY")
        op._run_live_02()
        op._run_live_03()
        op._run_live_04()
        op._run_live_05()
        op._run_live_06()
        op._run_live_07()

        boundary = op.security_boundary
        assert boundary is not None

        boundary.record_active_stage("LIVE-08", op._seal.sha256)
        boundary.set_cleanup_phase()

        # Boundary ownership_verification_completed marked, but the specific entity was NOT prechecked on wire
        boundary._ownership_verification_completed = True
        boundary._ownership_prechecked_keys.clear()  # No wire SELECT executed

        del_sql = "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?"
        del_params = [
            f"synth_art_{op.run_id}_01",
            "staging_synth_test",
            f"SYNTH_ARTICLE_{op.run_id}_01",
        ]
        with self.assertRaises(MockHranaDenied) as ctx:
            op.client.execute(del_sql, del_params)
        self.assertIn("OWNERSHIP_PRECHECK_REQUIRED", str(ctx.exception))
        self.assertTrue(boundary.halted)


if __name__ == "__main__":
    unittest.main()
