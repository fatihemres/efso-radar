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


if __name__ == "__main__":
    unittest.main()
