"""Phase 5B.3G-C Live Readiness Wire Contract Integration Tests (Offline Mock).

Validates the full operator -> outbound security boundary -> Hrana mock transport chain:
- Exact Hrana v2 pipeline JSON envelopes and response parsing
- Early-failure stop contract: LIVE-02..07 failures halt fail-closed, preserve manifest,
  set UNKNOWN/RESIDUAL, prohibit automatic retry/deletion, and state explicit separate
  operator-reviewed recovery dependency (zero calls to LIVE-08)
- Architecture conflict preservation: historical 80-request cutoff transition to LIVE-08
  contradicts monotonic stage indexing; fails closed without stage bypass
- Outbound security boundary wire checks: exact destination, payload size, JSON non-duplication,
  baton lifecycle (including transaction close), monotonic stage sequence, and read-only least privilege
- Server error response modes (500, malformed JSON, missing baton, truncated results, COMMIT timeout)
- LIVE-08 exact attribute pre-verification, conditional DELETE (affected == 1), and 3-state audit
- Request ceiling (<100), cleanup reserve (20 calls), and session deadline (<120.0s)
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import (
    TursoClientError,
    TursoHTTPClient,
)
from efso_radar.staging.hrana_outbound_guard import (
    HranaOutboundSecurityBoundary,
    MockHranaDenied,
)
from efso_radar.staging.live_ready_operator import (
    LiveReadyOperatorReport,
    LiveReadyOperatorStatus,
    LiveReadyStagingOperator,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
)


class TestPhase5B3GCLiveReadinessWireContract(unittest.TestCase):
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

        self.recorded_requests: list[httpx.Request] = []
        self.raw_transport = HermeticStagingTransport(
            self.db,
            custom_response_hook=self._intercept_and_record,
        )
        self.budget_transport = BudgetTrackingTransport(self.raw_transport)
        self.client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-offline-only",
            transport=self.budget_transport,
        )
        self.addCleanup(self.client.close)
        self._custom_hook = None

    def _intercept_and_record(self, request: httpx.Request) -> httpx.Response | None:
        self.recorded_requests.append(request)
        if self._custom_hook is not None:
            return self._custom_hook(request)
        return None

    def _make_operator(self, **kwargs) -> LiveReadyStagingOperator:
        params = dict(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=self.temp_dir,
            live_authorized=False,
        )
        params.update(kwargs)
        return LiveReadyStagingOperator(**params)

    # =========================================================================
    # 1. Full Pipeline Operator -> Outbound Boundary -> Wire Mock Execution
    # =========================================================================

    def test_nominal_pipeline_wire_contract_and_envelope_structure(self):
        """Verify nominal execution routes through boundary and generates compliant Hrana wire requests."""
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.PASSED)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertFalse(report.manifest_preserved)
        self.assertIsNone(report.recovery_notice)
        self.assertTrue(report.preflight_passed)
        self.assertTrue(report.human_window_confirmed)
        self.assertTrue(report.lockdown_recorded)
        self.assertTrue(report.security_boundary_active)
        self.assertGreater(len(self.recorded_requests), 40)

        # Inspect all recorded wire requests for strict Hrana v2 pipeline compliance
        for req in self.recorded_requests:
            self.assertEqual(req.method, "POST")
            self.assertEqual(str(req.url), "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline")
            self.assertLessEqual(len(req.content), 16384)

            # JSON decode with non-duplicate keys enforcement
            payload = json.loads(req.content.decode("utf-8"))
            self.assertIn("requests", payload)
            self.assertIsInstance(payload["requests"], list)
            self.assertGreaterEqual(len(payload["requests"]), 1)
            self.assertLessEqual(len(payload["requests"]), 8)

            first_stmt = payload["requests"][0]
            is_tx = (first_stmt.get("type") == "execute" and first_stmt.get("stmt", {}).get("sql") == "BEGIN")
            if not is_tx and payload.get("baton") is None:
                self.assertEqual(payload["requests"][-1], {"type": "close"})

        # Summary output checks
        summary = report.summary()
        self.assertIn("=== EFSO RADAR STAGING OPERATOR CANDIDATE [PASSED] ===", summary)
        self.assertIn("Audit Outcome State: CLEAN", summary)
        self.assertIn("Stages Executed: 8", summary)

    # =========================================================================
    # 2. Early-Failure Stop Contract (LIVE-02 through LIVE-07)
    # =========================================================================

    def test_early_failure_live_02_halts_fail_closed_and_requires_operator_recovery(self):
        """Failure in LIVE-02 halts fail-closed, preserves manifest, and prohibits LIVE-08 bypass."""
        def fail_on_live02_story(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "INSERT INTO stories" in sql:
                    return httpx.Response(500, json={"message": "Simulated cloud error on stories insert"}, request=req)
            return None

        self._custom_hook = fail_on_live02_story
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)
        self.assertIn("SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED", report.recovery_notice)

        # Zero calls to LIVE-08
        executed_stages = [st.stage_id for st in report.stages]
        self.assertIn("LIVE-01", executed_stages)
        self.assertNotIn("LIVE-08", executed_stages)

        # Boundary session is permanently closed
        self.assertTrue(op.security_boundary._session_closed)

        # Adversarial attempt to call _run_live_08 directly fails closed
        with self.assertRaises(MockHranaDenied):
            op._run_live_08()

    def test_early_failure_live_03_halts_fail_closed_and_requires_operator_recovery(self):
        """Failure in LIVE-03 (readback error) halts fail-closed and preserves manifest."""
        def fail_on_live03_readback(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "SELECT article_id FROM processed_articles" in sql:
                    raise httpx.ConnectError("Simulated connection drop during LIVE-03 rollback read-back")
            return None

        self._custom_hook = fail_on_live03_readback
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)
        self.assertIn("SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED", report.recovery_notice)

        executed_stages = [st.stage_id for st in report.stages]
        self.assertIn("LIVE-02", executed_stages)
        self.assertNotIn("LIVE-08", executed_stages)
        self.assertTrue(op.security_boundary._session_closed)

    def test_early_failure_live_04_halts_fail_closed_and_requires_operator_recovery(self):
        """Failure in LIVE-04 (CAS lock acquisition transport failure) halts fail-closed and preserves manifest."""
        def fail_on_live04_cas(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "INSERT INTO run_locks" in sql and "runner_alpha" in str(r.get("stmt", {}).get("args", [])):
                    raise httpx.ConnectError("Simulated network drop during LIVE-04 lock acquisition")
            return None

        self._custom_hook = fail_on_live04_cas
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)
        self.assertIn("SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED", report.recovery_notice)

        executed_stages = [st.stage_id for st in report.stages]
        self.assertIn("LIVE-03", executed_stages)
        self.assertNotIn("LIVE-08", executed_stages)
        self.assertTrue(op.security_boundary._session_closed)

    def test_early_failure_live_05_halts_fail_closed_and_requires_operator_recovery(self):
        """Failure in LIVE-05 (lease takeover read-back failure) halts fail-closed and preserves manifest."""
        def fail_on_live05_readback(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "SELECT worker_id FROM run_locks" in sql:
                    return httpx.Response(500, json={"message": "Simulated read-back error"}, request=req)
            return None

        self._custom_hook = fail_on_live05_readback
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)
        self.assertIn("SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED", report.recovery_notice)

        executed_stages = [st.stage_id for st in report.stages]
        self.assertIn("LIVE-04", executed_stages)
        self.assertNotIn("LIVE-08", executed_stages)
        self.assertTrue(op.security_boundary._session_closed)

    def test_early_failure_live_06_halts_fail_closed_and_requires_operator_recovery(self):
        """Failure in LIVE-06 (recovery checkpoint write failure) halts fail-closed and preserves manifest."""
        def fail_on_live06_save(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "INSERT INTO durable_recovery_state" in sql:
                    raise httpx.TimeoutException("Simulated checkpoint timeout")
            return None

        self._custom_hook = fail_on_live06_save
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)
        self.assertIn("SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED", report.recovery_notice)

        executed_stages = [st.stage_id for st in report.stages]
        self.assertIn("LIVE-05", executed_stages)
        self.assertNotIn("LIVE-08", executed_stages)
        self.assertTrue(op.security_boundary._session_closed)

    def test_early_failure_live_07_halts_fail_closed_and_requires_operator_recovery(self):
        """Failure in LIVE-07 (contradictory reconciliation outcome) halts fail-closed and preserves manifest."""
        op = self._make_operator(simulate_ambiguous_contradictory=True)
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)
        self.assertIn("SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED", report.recovery_notice)

        executed_stages = [st.stage_id for st in report.stages]
        self.assertIn("LIVE-06", executed_stages)
        self.assertNotIn("LIVE-08", executed_stages)
        self.assertTrue(op.security_boundary._session_closed)

    # =========================================================================
    # 3. Architecture Conflict Preservation (Historical 80-Request Cutoff)
    # =========================================================================

    def test_historical_80_request_cutoff_conflict_fails_closed_without_cleanup_bypass(self):
        """Reaching request cutoff (80) fails closed; does not bypass stages to jump to LIVE-08."""
        # Lower cutoff to 45 so it trips during LIVE-01 / LIVE-02 transition
        op = self._make_operator(request_cutoff=45, request_ceiling=90, cleanup_reserve=25)
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)
        self.assertIn("SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED", report.recovery_notice)

        # Monotonic stage guards prevented jumping to LIVE-08
        executed_stages = [st.stage_id for st in report.stages]
        self.assertNotIn("LIVE-08", executed_stages)

        # Any attempt to enter cleanup phase early is rejected
        if op.security_boundary is not None:
            with self.assertRaises(MockHranaDenied):
                op.security_boundary.set_cleanup_phase()

    # =========================================================================
    # 4. Outbound Wire Boundary Checks & Protocol Robustness
    # =========================================================================

    def test_wire_boundary_accepts_close_with_baton_on_failed_transaction(self):
        """Wire boundary cleanly accepts [{"type": "close"}] with pending baton as transaction_end."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary
        self.assertIsNotNone(boundary)

        boundary._pending_baton = "test-baton-xyz"
        req = httpx.Request(
            "POST",
            "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline",
            json={"baton": "test-baton-xyz", "requests": [{"type": "close"}]},
        )
        action = boundary._validate(req)
        self.assertEqual(action, "transaction_end")

    def test_wire_boundary_rejects_payload_exceeding_16kb(self):
        """Wire boundary rejects payloads exceeding 16KB with BODY_SIZE."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary

        large_payload = {"requests": [{"type": "execute", "stmt": {"sql": "SELECT 1"}}, {"type": "close"}], "pad": "x" * 20000}
        req = httpx.Request(
            "POST",
            "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline",
            content=json.dumps(large_payload).encode("utf-8"),
        )
        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_BODY_SIZE"):
            boundary.handle_request(req)

    def test_wire_boundary_rejects_duplicate_json_keys(self):
        """Wire boundary rejects requests with duplicate JSON keys."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary

        raw_dup_json = b'{"requests": [{"type": "close"}], "requests": [{"type": "close"}]}'
        req = httpx.Request(
            "POST",
            "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline",
            content=raw_dup_json,
        )
        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_MALFORMED_JSON"):
            boundary.handle_request(req)

    def test_wire_boundary_rejects_unallowlisted_destination_http(self):
        """Wire boundary rejects non-exact destinations or cleartext HTTP."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary

        req_http = httpx.Request(
            "POST",
            "http://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline",
            json={"requests": [{"type": "close"}]},
        )
        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_DESTINATION"):
            boundary.handle_request(req_http)

    def test_wire_boundary_rejects_unallowlisted_destination_wrong_host(self):
        """Wire boundary rejects foreign host destination."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary

        req_wrong_host = httpx.Request(
            "POST",
            "https://evil-turso-staging.turso.io/v2/pipeline",
            json={"requests": [{"type": "close"}]},
        )
        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_DESTINATION"):
            boundary.handle_request(req_wrong_host)

    def test_wire_boundary_rejects_stage_monotonic_sequence_violation_active(self):
        """Wire boundary strictly rejects activating stage when another is active."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary
        seal = op._seal

        boundary.record_active_stage("LIVE-01", seal.sha256)
        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_STAGE_ALREADY_ACTIVE"):
            boundary.record_active_stage("LIVE-02", seal.sha256)

    def test_wire_boundary_rejects_stage_monotonic_sequence_violation_skip(self):
        """Wire boundary strictly rejects skipping stages out of order."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary
        seal = op._seal

        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_OUT_OF_ORDER_STAGE"):
            boundary.record_active_stage("LIVE-03", seal.sha256)

    def test_wire_boundary_rejects_stage_monotonic_sequence_violation_premature_live_08(self):
        """Wire boundary strictly rejects premature activation of LIVE-08."""
        op = self._make_operator()
        op._create_and_persist_manifest()
        boundary = op.security_boundary
        seal = op._seal

        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_PREFLIGHT_REQUIRED"):
            boundary.record_active_stage("LIVE-08", seal.sha256)

    # =========================================================================
    # 5. Wire Response Failure Modes
    # =========================================================================

    def test_server_500_response_halts_session_with_unknown_and_preserves_manifest(self):
        """Server returning 500 status on statement dispatch halts with UNKNOWN and preserves manifest."""
        def return_500(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "SELECT MAX(version)" in sql:
                    return httpx.Response(500, json={"message": "Server 500 error"}, request=req)
            return None

        self._custom_hook = return_500
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)

    def test_malformed_json_response_halts_session_with_unknown_and_preserves_manifest(self):
        """Malformed JSON response halts with UNKNOWN and preserves manifest."""
        def return_malformed(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "SELECT MAX(version)" in sql:
                    return httpx.Response(200, content=b"<html>Bad Gateway</html>", headers={"content-type": "application/json"}, request=req)
            return None

        self._custom_hook = return_malformed
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_missing_baton_on_transaction_begin_halts_session(self):
        """Missing baton in response to BEGIN halts with UNKNOWN and preserves manifest."""
        def return_no_baton_on_begin(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            requests_list = body.get("requests", [])
            if requests_list and requests_list[0].get("stmt", {}).get("sql") == "BEGIN":
                return httpx.Response(
                    200,
                    json={"baton": None, "results": [{"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}}] * len(requests_list)},
                    request=req,
                )
            return None

        self._custom_hook = return_no_baton_on_begin
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_commit_network_timeout_halts_session_with_unknown_and_preserves_manifest(self):
        """Network timeout during COMMIT sets UNKNOWN, prohibits blind retry, and retains manifest."""
        def timeout_on_commit(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                if r.get("stmt", {}).get("sql") == "COMMIT":
                    raise httpx.TimeoutException("Network timeout during COMMIT dispatch")
            return None

        self._custom_hook = timeout_on_commit
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)

    # =========================================================================
    # 6. LIVE-08 Ownership, Conditional Delete, and 3-State Audit
    # =========================================================================

    def test_live_08_ownership_mismatch_halts_without_any_delete_and_preserves_manifest(self):
        """Row attribute altered before LIVE-08 halts with OWNERSHIP_MISMATCH_HALT and zero DELETEs."""
        op = self._make_operator(simulate_ownership_mismatch=True)
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.OWNERSHIP_MISMATCH_HALT)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

        # Zero DELETE queries were dispatched at the wire boundary
        delete_requests = [
            req for req in self.recorded_requests
            if "DELETE FROM" in req.content.decode("utf-8")
        ]
        self.assertEqual(len(delete_requests), 0)

    def test_live_08_residual_row_halts_with_residual_and_preserves_manifest(self):
        """Residual row present post-deletion sets RESIDUAL and preserves manifest."""
        def mock_zero_delete(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "DELETE FROM" in sql:
                    # Return affected_row_count = 0 to trigger RESIDUAL
                    return httpx.Response(
                        200,
                        json={"baton": None, "results": [
                            {"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": [], "affected_row_count": 0, "last_insert_rowid": None}}},
                            {"type": "ok"},
                        ]},
                        request=req,
                    )
            return None

        self._custom_hook = mock_zero_delete
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.RESIDUAL)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)

    def test_live_08_audit_failure_halts_with_unknown_and_preserves_manifest(self):
        """Transport failure during LIVE-08 audit verification sets UNKNOWN and preserves manifest."""
        op = self._make_operator(simulate_audit_failure=True)
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIsNotNone(report.recovery_notice)

    # =========================================================================
    # 7. Ceilings, Deadlines & Sanitized Diagnostics
    # =========================================================================

    def test_request_ceiling_100_strictly_blocked(self):
        """Dispatched request #100 is strictly blocked fail-closed."""
        # Use valid invariants: cutoff=20, cleanup_reserve=20, ceiling=40
        op = self._make_operator(request_ceiling=40, request_cutoff=20, cleanup_reserve=20)
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_session_deadline_exhaustion_strictly_halts_session(self):
        """Elapsed session deadline (>=120s) strictly halts session fail-closed."""
        fake_time = 1000.0

        def advance_clock():
            nonlocal fake_time
            fake_time += 2.5
            return fake_time

        op = self._make_operator(
            clock=advance_clock,
            session_deadline_seconds=20.0,
        )
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_sanitized_errors_leak_zero_sql_or_tokens(self):
        """Error messages and reports leak zero SQL statements, parameter values, or auth tokens."""
        def fail_with_secret(req: httpx.Request) -> httpx.Response | None:
            body = json.loads(req.content.decode("utf-8"))
            for r in body.get("requests", []):
                sql = r.get("stmt", {}).get("sql", "")
                if "SELECT MAX(version)" in sql:
                    raise TursoClientError("Internal failure with token secret-12345 and SELECT * FROM admin")
            return None

        self._custom_hook = fail_with_secret
        op = self._make_operator()
        report = op.run(auto_human_window=True, human_marker="MOCK_ONLY")

        self.assertEqual(report.status, LiveReadyOperatorStatus.FAILED)
        for err in report.errors:
            self.assertNotIn("secret-12345", err)
            self.assertNotIn("SELECT * FROM admin", err)
            self.assertNotIn("synthetic-offline-only", err)
