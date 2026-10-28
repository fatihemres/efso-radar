"""Phase 5B.3G-C Live-Ready Staging Operator Candidate (Default Deny).

This operator candidate reuses existing offline components to establish a
security-reviewed, deny-by-default execution pipeline for Turso staging.

SAFETY NOTICE:
- Live Turso cloud execution is PERMANENTLY DISABLED BY DEFAULT.
- This module does NOT authorize connecting to real Turso, creating tokens,
  or unblocking Block Writes on Turso staging.
- Any live execution requires future, explicit, separate Product Owner authorization.
- All testing and validation is strictly hermetic (in-memory SQLite + mock HTTP).
- MANIFEST DURABILITY NOTICE: Ephemeral ownership manifests are stored in OS tempdir
  for process-crash durability. They are NOT machine-reboot durable. True reboot durability
  remains an unresolved live prerequisite requiring a separately reviewed storage design.
- LOCKDOWN RECORDING NOTICE: Post-run lockdown recording proves an offline simulated assertion
  only, NOT real cloud dashboard state or token revocation.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from enum import Enum
import json
import logging
from pathlib import Path
import re
import secrets
import sys
import tempfile
import time
from typing import Any, Callable, Optional

from pydantic import BaseModel, Field

from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.turso_client import (
    TursoAmbiguousTransactionError,
    TursoClientError,
    TursoHTTPClient,
    mask_turso_url,
)
from efso_radar.staging.hrana_outbound_guard import (
    HranaOutboundSecurityBoundary,
    is_valid_iso_timestamp,
    validate_budget_invariants,
)
from efso_radar.staging.manifest_seal import OfflineManifestSeal, ManifestSealViolation
from efso_radar.staging.offline_operator import (
    SealedHermeticStagingRunner,
    _MUTATING_TEMPLATES,
    _norm,
)
from efso_radar.staging.offline_session_gate import (
    MockAudit,
    MockPhase,
    OfflineSessionGate,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    EXACT_STAGING_DESTINATION,
    EXACT_STAGING_HTTPS_URL,
    MAX_REQUEST_CEILING,
    TEST_CUTOFF_REQUESTS,
    CLEANUP_RESERVE_REQUESTS,
    SESSION_TIMEOUT_SECONDS,
    PerRunOwnershipManifest,
    RegisteredEntity,
    StageResult,
    create_default_manifest_entities,
    is_ambiguous_transaction_error,
    validate_staging_destination,
    verify_hermetic_session_transport,
)

logger = logging.getLogger("efso-radar.staging.live_ready_operator")

_CANONICAL_STAMP = "2026-10-25T15:00:00Z"


def _is_valid_timestamp(val: Any) -> bool:
    return is_valid_iso_timestamp(val)


class LiveReadyOperatorStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class LiveReadyOperatorReport(BaseModel):
    """Sanitized report for the Phase 5B.3G-C operator candidate."""
    model_config = {"extra": "forbid"}

    run_id: str
    status: LiveReadyOperatorStatus
    audit_state: AuditOutcomeState
    preflight_passed: bool
    human_window_confirmed: bool
    destination_url: str
    manifest_path: str
    manifest_preserved: bool
    http_requests_total: int
    http_requests_ceiling: int = MAX_REQUEST_CEILING
    cleanup_reserve_used: int
    duration_seconds: float
    stages: list[StageResult] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    lockdown_recorded: bool = False
    live_execution_refused: bool = True
    security_boundary_active: bool = True
    recovery_notice: Optional[str] = None

    def summary(self) -> str:
        lines = [
            f"=== EFSO RADAR STAGING OPERATOR CANDIDATE [{self.status.value}] ===",
            f"Run ID: {self.run_id}",
            f"Destination: {mask_turso_url(self.destination_url)}",
            f"Preflight Passed: {self.preflight_passed}",
            f"Human Window Confirmed: {self.human_window_confirmed}",
            f"Live Execution Refused (Default-Deny): {self.live_execution_refused}",
            f"Security Boundary Active: {self.security_boundary_active}",
            f"Audit Outcome State: {self.audit_state.value}",
            f"Manifest Path: {self.manifest_path}",
            f"Manifest Preserved: {self.manifest_preserved}",
            f"HTTP Requests: {self.http_requests_total}/{self.http_requests_ceiling} "
            f"(cleanup reserve used: {self.cleanup_reserve_used}/{CLEANUP_RESERVE_REQUESTS})",
            f"Duration: {self.duration_seconds:.2f}s (ceiling: {SESSION_TIMEOUT_SECONDS}s)",
            f"Post-Run Lockdown Recorded: {self.lockdown_recorded} (offline simulated assertion only)",
            f"Stages Executed: {len(self.stages)}",
        ]
        for st in self.stages:
            st_res = "PASS" if st.success else "FAIL"
            lines.append(f"  [{st_res}] {st.stage_id}: {st.name} ({st.http_requests_used} calls, {st.duration_seconds:.3f}s)")
        if self.recovery_notice:
            lines.append(f"Recovery Notice: {self.recovery_notice}")
        if self.errors:
            lines.append("Errors:")
            for err in self.errors:
                lines.append(f"  - {err}")
        return "\n".join(lines)


class LiveReadyStagingOperator(SealedHermeticStagingRunner):
    """Phase 5B.3G-C Staging Operator Candidate.

    Enforces all safety gates:
    1. Default-deny live Turso execution (rejects non-hermetic transports).
    2. Outbound HTTP security boundary wiring (inspects every wire request).
    3. Crash-recoverable ownership manifest & seal before any mutation.
    4. LIVE-01 preflight & baseline collision check under Block Writes ON.
    5. Human approval window gate before any write mutation.
    6. Session deadline (<120.0s) and request budget (<100 requests, 20 cleanup reserve).
    7. Ambiguous COMMIT => UNKNOWN, no blind retry, no cleanup, manifest retained.
    8. LIVE-08 pre-deletion ownership check, conditional DELETE (affected == 1), 3-state audit.
    9. Post-run lockdown recording (offline assertion only).
    10. Sanitized error messages with zero credential / SQL leakage.
    """

    def __init__(
        self,
        client: TursoHTTPClient,
        destination_url: str = EXACT_STAGING_DESTINATION,
        manifest_dir: Optional[Path] = None,
        *,
        live_authorized: bool = False,
        clock: Callable[[], float] = time.monotonic,
        request_ceiling: int = MAX_REQUEST_CEILING,
        request_cutoff: int = TEST_CUTOFF_REQUESTS,
        cleanup_reserve: int = CLEANUP_RESERVE_REQUESTS,
        session_deadline_seconds: float = SESSION_TIMEOUT_SECONDS,
        simulate_ownership_mismatch: bool = False,
        simulate_rollback_persisted: bool = False,
        simulate_audit_failure: bool = False,
        simulate_ambiguous_contradictory: bool = False,
        simulate_ambiguous_inspection_failure: bool = False,
    ) -> None:
        if live_authorized:
            raise PermissionError(
                "LIVE_TURSO_UNAUTHORIZED: Live Turso staging execution is permanently "
                "disabled by default pending PO authorization."
            )

        # Budget invariant validation (fail-closed at instantiation)
        validate_budget_invariants(
            request_ceiling=request_ceiling,
            request_cutoff=request_cutoff,
            cleanup_reserve=cleanup_reserve,
            session_deadline_seconds=session_deadline_seconds,
            is_session=True,
        )

        # Default-deny: requires concrete hermetic transport
        transport = getattr(client, "transport", None)
        verify_hermetic_session_transport(transport)

        super().__init__(
            client=client,
            destination_url=destination_url,
            manifest_dir=manifest_dir,
            simulate_ownership_mismatch=simulate_ownership_mismatch,
            simulate_rollback_persisted=simulate_rollback_persisted,
            simulate_audit_failure=simulate_audit_failure,
            simulate_ambiguous_contradictory=simulate_ambiguous_contradictory,
            simulate_ambiguous_inspection_failure=simulate_ambiguous_inspection_failure,
        )

        self._clock = clock
        self._operator_started_at = clock()
        self.request_ceiling = request_ceiling
        self.request_cutoff = request_cutoff
        self.cleanup_reserve = cleanup_reserve
        self.session_deadline_seconds = session_deadline_seconds
        self.human_window_confirmed = False
        self._gate: Optional[OfflineSessionGate] = None
        self.lockdown_recorded = False
        self.live_execution_refused = True
        self._in_cleanup = False
        self._ownership_verified = False
        self.security_boundary: Optional[HranaOutboundSecurityBoundary] = None

    def _create_and_persist_manifest(self) -> None:
        super()._create_and_persist_manifest()
        if self._seal is not None:
            self._gate = OfflineSessionGate(
                expected_manifest_sha256=self._seal.sha256,
                clock=self._clock,
            )
            # Wire outbound security boundary directly into transport chain
            bt = self.budget_transport
            if bt is not None:
                raw_transport = bt.inner
                if isinstance(raw_transport, HranaOutboundSecurityBoundary):
                    raw_transport = raw_transport._inner
                self.security_boundary = HranaOutboundSecurityBoundary(
                    inner=raw_transport,
                    seal=self._seal,
                    dynamic_validation=True,
                    session_mode=True,
                    clock=self._clock,
                    max_requests=self.request_ceiling,
                    test_cutoff_requests=self.request_cutoff,
                    cleanup_reserve_requests=self.cleanup_reserve,
                    session_timeout_seconds=self.session_deadline_seconds,
                )
                bt.inner = self.security_boundary
                if hasattr(self.client, "_client") and self.client._client is not None:
                    self.client._client.close()
                    self.client._client = None

    def _check_budget_and_deadline(self, cleanup: bool = False) -> None:
        now = self._clock()
        if now - self._operator_started_at >= self.session_deadline_seconds:
            self._deny("SESSION_DEADLINE_EXCEEDED")
        if self.http_requests_total >= self.request_ceiling:
            self._deny("REQUEST_CEILING")
        if not cleanup and self.http_requests_total >= self.request_cutoff:
            self._deny("CLEANUP_RESERVE")

    def _run_live_01(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        if (
            self.halt_before_cleanup
            or self._preflight_attempted
            or self._preflight_accepted
        ):
            self._deny("PREFLIGHT_REENTRY")
        req_start = self.http_requests_total
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_active_stage("LIVE-01", self._seal.sha256)
        try:
            super()._run_live_01()
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-01", self._seal.sha256)
        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_preflight(
                manifest_sha256=self._seal.sha256,
                schema_version=CURRENT_SCHEMA_VERSION,
                tables_checked=15,
                columns_checked=129,
                collision_checks=5,
                simulated_block_writes_on=True,
                request_count=req_used,
            )

    def confirm_human_window(self, marker: str = "MOCK_ONLY") -> None:
        """Explicit human approval window gate; required before any write mutation."""
        if not self._preflight_accepted or self.halt_before_cleanup:
            self._deny("PREFLIGHT_REQUIRED")
        if self.human_window_confirmed:
            self._deny("HUMAN_WINDOW_ALREADY_CONFIRMED")
        self._check_budget_and_deadline(cleanup=False)
        if self._gate is not None:
            self._gate.record_simulated_human_window(marker=marker)
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_human_window_confirmed(
                self, marker, self._seal.sha256
            )
        self.human_window_confirmed = True

    def _guard_mutation(self, sql: str, params: list[Any] | None) -> None:
        if not self._preflight_accepted or self._seal is None:
            self._deny("PREFLIGHT_REQUIRED")
        if not self.human_window_confirmed:
            self._deny("HUMAN_WINDOW_REQUIRED")
        self._check_budget_and_deadline(cleanup=self._in_cleanup)

        spec = _MUTATING_TEMPLATES.get(_norm(sql))
        if spec is None:
            self._deny("SQL_NOT_ALLOWLISTED")
        table, operation, count = spec
        if type(params) is not list or len(params) != count:
            self._deny("PARAMETERS_INVALID")

        if any(
            type(value) not in (str, int, float, bool)
            or (type(value) is str and len(value) > 512)
            for value in params
        ):
            self._deny("PARAMETER_SCALAR_UNSAFE")

        pk_value = str(params[0])
        try:
            self._seal.assert_unchanged()
        except ManifestSealViolation:
            self._deny("MANIFEST_INVALID")
        try:
            self._seal.require_registered_identity(table=table, pk_column=self._get_pk_column(table), pk_value=pk_value)
        except ManifestSealViolation:
            self._deny("UNREGISTERED_IDENTITY")

        # Dynamic parameter verification
        if table == "processed_articles":
            if operation == "insert":
                if not (
                    (pk_value.endswith("_01") and str(params[1]).startswith("https://example.com/synth/"))
                    or (pk_value.endswith("_02_rollback") and str(params[1]).startswith("https://example.com/synth/"))
                ):
                    self._deny("DYNAMIC_ARTICLE_URL_INVALID")
                if not _is_valid_timestamp(params[4]) or not _is_valid_timestamp(params[5]):
                    self._deny("TIMESTAMP_INVALID")
            elif operation == "delete":
                if len(params) != 3:
                    self._deny("DELETE_PARAMS_INVALID")
        elif table == "stories":
            if operation == "insert":
                if not _is_valid_timestamp(params[3]) or not _is_valid_timestamp(params[4]):
                    self._deny("TIMESTAMP_INVALID")
                try:
                    art_ids = json.loads(params[5])
                    if not isinstance(art_ids, list) or not all(str(a).startswith("synth_art_") for a in art_ids):
                        self._deny("STORY_ARTICLE_REF_INVALID")
                except Exception:
                    self._deny("STORY_ARTICLE_REF_INVALID")
            elif operation == "delete":
                if len(params) != 3:
                    self._deny("DELETE_PARAMS_INVALID")
        elif table == "run_locks":
            if operation == "insert":
                if not _is_valid_timestamp(params[2]) or not _is_valid_timestamp(params[3]) or not _is_valid_timestamp(params[4]):
                    self._deny("TIMESTAMP_INVALID")
            elif operation == "delete":
                if len(params) != 3:
                    self._deny("DELETE_PARAMS_INVALID")
        elif table == "durable_recovery_state":
            if operation == "insert":
                if not _is_valid_timestamp(params[4]) or not _is_valid_timestamp(params[5]):
                    self._deny("TIMESTAMP_INVALID")
            elif operation == "delete":
                if len(params) != 3:
                    self._deny("DELETE_PARAMS_INVALID")

    def _get_pk_column(self, table: str) -> str:
        for ent in self.manifest.registered_entities:
            if ent["table"] == table:
                return ent["pk_column"]
        raise KeyError(f"Unknown synthetic table {table}")

    def _run_live_02(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        if not self._preflight_accepted:
            self._deny("PREFLIGHT_REQUIRED")
        if not self.human_window_confirmed:
            self._deny("HUMAN_WINDOW_REQUIRED")
        if self._seal is not None:
            try:
                self._seal.assert_unchanged()
            except Exception:
                self._deny("MANIFEST_INVALID")
        req_start = self.http_requests_total
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_active_stage("LIVE-02", self._seal.sha256)
        try:
            super()._run_live_02()
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-02", self._seal.sha256)
        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-02",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_03(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        req_start = self.http_requests_total
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_active_stage("LIVE-03", self._seal.sha256)
        try:
            super()._run_live_03()
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-03", self._seal.sha256)
        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-03",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_04(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        req_start = self.http_requests_total
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_active_stage("LIVE-04", self._seal.sha256)
        try:
            super()._run_live_04()
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-04", self._seal.sha256)
        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-04",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_05(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        req_start = self.http_requests_total
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_active_stage("LIVE-05", self._seal.sha256)
        try:
            super()._run_live_05()
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-05", self._seal.sha256)
        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-05",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_06(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        req_start = self.http_requests_total
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_active_stage("LIVE-06", self._seal.sha256)
        try:
            super()._run_live_06()
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-06", self._seal.sha256)
        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-06",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_07(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        req_start = self.http_requests_total
        if self.security_boundary is not None and self._seal is not None:
            self.security_boundary.record_active_stage("LIVE-07", self._seal.sha256)
        try:
            super()._run_live_07()
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-07", self._seal.sha256)
        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-07",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_08(self) -> None:
        """LIVE-08 Verified Cleanup & 3-State Audit fail-closed."""
        self._in_cleanup = True
        bt = self.budget_transport
        if bt:
            bt.in_cleanup = True
        if self.security_boundary is not None:
            if self._seal is not None:
                self.security_boundary.record_active_stage("LIVE-08", self._seal.sha256)
            self.security_boundary.set_cleanup_phase()

        self._check_budget_and_deadline(cleanup=True)
        req_start = self.http_requests_total
        start_8 = time.perf_counter()

        try:
            # 8a: Pre-deletion full attribute verification
            assert self.manifest is not None
            verified_entities: list[dict[str, Any]] = []
            for ent in self.manifest.registered_entities:
                tbl = ent["table"]
                pk_col = ent["pk_column"]
                pk_val = ent["pk_value"]
                expected_attrs = ent["expected_attributes"]

                res = self._execute(f"SELECT * FROM {tbl} WHERE {pk_col} = ?", [pk_val])
                row = res.fetchone()
                if not row:
                    continue  # Rolled back entity in Stage 3 does not exist in DB

                if self.simulate_ownership_mismatch and tbl == "processed_articles":
                    self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                    self.halt_before_cleanup = True
                    raise PermissionError("OWNERSHIP_MISMATCH_HALT: Row attributes do not match expected synthetic markers.")

                # Ownership verification: every attribute must match exactly
                if tbl == "run_locks":
                    act_worker = row.get("worker_id")
                    if act_worker not in expected_attrs.get("worker_ids", []):
                        self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                        self.halt_before_cleanup = True
                        raise PermissionError(
                            f"OWNERSHIP_MISMATCH_HALT: worker_id '{act_worker}' on {tbl}.{pk_val} "
                            "is not one of the registered worker_ids in ownership manifest."
                        )
                else:
                    for attr_key, exp_val in expected_attrs.items():
                        act_val = row.get(attr_key)
                        if act_val != exp_val:
                            self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                            self.halt_before_cleanup = True
                            raise PermissionError(
                                f"OWNERSHIP_MISMATCH_HALT: Attribute '{attr_key}' on {tbl}.{pk_val} "
                                "does not match registered ownership manifest."
                            )
                verified_entities.append(ent)

            # Record verified ownership precheck at wire boundary before initiating conditional DELETE
            self._ownership_verified = True
            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_ownership_verification_completed(self, self._seal.sha256)

            # 8b: Parameterized conditional DELETE checking affected_row_count == 1
            for ent in sorted(
                verified_entities,
                key=lambda x: x["cleanup_dependency_order"],
            ):
                tbl = ent["table"]
                pk_col = ent["pk_column"]
                pk_val = ent["pk_value"]
                attrs = ent["expected_attributes"]

                if tbl == "processed_articles":
                    del_sql = "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?"
                    del_params = [pk_val, attrs["source"], attrs["title"]]
                elif tbl == "stories":
                    del_sql = "DELETE FROM stories WHERE story_id = ? AND category = ? AND headline_tr = ?"
                    del_params = [pk_val, attrs["category"], attrs["headline_tr"]]
                elif tbl == "run_locks":
                    del_sql = "DELETE FROM run_locks WHERE lock_key = ? AND (worker_id = ? OR worker_id = ?)"
                    del_params = [pk_val, attrs["worker_ids"][0], attrs["worker_ids"][1]]
                elif tbl == "durable_recovery_state":
                    del_sql = "DELETE FROM durable_recovery_state WHERE state_key = ? AND stage = ? AND worker_id = ?"
                    del_params = [pk_val, attrs["stage"], attrs["worker_id"]]
                else:
                    self._deny("UNEXPECTED_CLEANUP_TABLE")

                self._guard_mutation(del_sql, del_params)
                del_res = self._execute(del_sql, del_params)

                # Check affected row count (must be exactly 1)
                if del_res.affected_row_count != 1:
                    self.audit_state = AuditOutcomeState.RESIDUAL
                    self.halt_before_cleanup = True
                    raise RuntimeError(
                        f"Unexpected affected row count during conditional deletion of {tbl}.{pk_val}: "
                        f"expected 1, got {del_res.affected_row_count}"
                    )

            # 8c: Independent 3-State Audit
            if self.simulate_audit_failure:
                self.audit_state = AuditOutcomeState.UNKNOWN
                raise TursoClientError("Simulated transport failure during audit verification query")

            residual_found = False
            for ent in self.manifest.registered_entities:
                tbl = ent["table"]
                pk_col = ent["pk_column"]
                pk_val = ent["pk_value"]
                audit_res = self._execute(f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE {pk_col} = ?", [pk_val])
                cnt = audit_res.fetchone()["cnt"]
                if cnt > 0:
                    residual_found = True
                    break

            if residual_found:
                self.audit_state = AuditOutcomeState.RESIDUAL
            else:
                self.audit_state = AuditOutcomeState.CLEAN

            if self.security_boundary is not None and self._seal is not None:
                self.security_boundary.record_stage_completed("LIVE-08", self._seal.sha256)

        except Exception:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
            raise
        finally:
            req_used = self.http_requests_total - req_start
            duration = time.perf_counter() - start_8
            self.stages.append(
                StageResult(
                    stage_id="LIVE-08",
                    name="Pre-Deletion Ownership Verification, Conditional Deletions & Three-State Audit",
                    success=(self.audit_state == AuditOutcomeState.CLEAN),
                    http_requests_used=req_used,
                    duration_seconds=duration,
                    details={"audit_state": self.audit_state.value},
                )
            )
            if self._gate is not None and self._seal is not None:
                mock_outcome = MockAudit.CLEAN if self.audit_state == AuditOutcomeState.CLEAN else (
                    MockAudit.RESIDUAL if self.audit_state == AuditOutcomeState.RESIDUAL else MockAudit.UNKNOWN
                )
                try:
                    self._gate.record_simulated_audit(
                        manifest_sha256=self._seal.sha256,
                        ownership_verified=(self.audit_state != AuditOutcomeState.OWNERSHIP_MISMATCH_HALT),
                        outcome=mock_outcome,
                        request_count=max(req_used, 1),
                    )
                except Exception:
                    pass

    def record_lockdown(
        self,
        marker: str = "MOCK_ONLY",
        block_writes_on: bool = True,
        token_revoked: bool = True,
    ) -> None:
        """Confirm post-flight lockdown state (Block Writes ON and token revocation).

        SAFETY NOTICE: Proves an offline simulated assertion only, NOT real cloud
        dashboard state or token revocation.
        """
        if self.lockdown_recorded:
            self._deny("LOCKDOWN_REENTRY")
        if self._gate is not None:
            self._gate.record_simulated_lockdown(
                marker=marker,
                simulated_block_writes_on=block_writes_on,
                simulated_token_revoked=token_revoked,
            )
        self.lockdown_recorded = True

    def run(
        self,
        *,
        auto_human_window: bool = False,
        human_marker: str = "MOCK_ONLY",
    ) -> LiveReadyOperatorReport:
        """Execute full staging validation pipeline fail-closed."""
        start_time = time.perf_counter()
        session_status = LiveReadyOperatorStatus.FAILED
        errors: list[str] = []

        try:
            # 1. Manifest creation & sealing
            self._create_and_persist_manifest()

            # 2. LIVE-01 Preflight & Baseline Collision Check
            self._run_live_01()

            # 3. Human Approval Window Gate
            if auto_human_window:
                self.confirm_human_window(marker=human_marker)
            if not self.human_window_confirmed:
                errors.append("OPERATOR_GATE_FAILURE: HUMAN_WINDOW_NOT_CONFIRMED")
                session_status = LiveReadyOperatorStatus.FAILED
                self.halt_before_cleanup = True
            else:
                # 4. LIVE-02 to LIVE-07 Controlled Write Stages
                self._run_live_02()
                self._run_live_03()
                self._run_live_04()
                self._run_live_05()
                self._run_live_06()
                self._run_live_07()

                # 5. LIVE-08 Pre-Deletion Ownership Check, Conditional DELETE, 3-State Audit
                self._run_live_08()

                # 6. Post-Run Lockdown Recording (does not swallow errors)
                self.record_lockdown(marker=human_marker, block_writes_on=True, token_revoked=True)

                # 7. Comprehensive Completion Gate
                expected_stages = {
                    "LIVE-01", "LIVE-02", "LIVE-03", "LIVE-04",
                    "LIVE-05", "LIVE-06", "LIVE-07", "LIVE-08",
                }
                executed_stage_ids = {st.stage_id for st in self.stages if st.success}
                all_stages_succeeded = expected_stages.issubset(executed_stage_ids)

                if (
                    self.human_window_confirmed
                    and self._preflight_accepted
                    and all_stages_succeeded
                    and self.lockdown_recorded
                    and self.audit_state == AuditOutcomeState.CLEAN
                    and len(errors) == 0
                ):
                    session_status = LiveReadyOperatorStatus.PASSED
                else:
                    session_status = LiveReadyOperatorStatus.FAILED

        except Exception as exc:
            # Sanitized error capturing: never echo SQL or tokens
            err_code = f"OPERATOR_GATE_FAILURE: {type(exc).__name__}"
            if "DEADLINE" in str(exc) or "SESSION_DEADLINE" in str(exc):
                err_code = "OPERATOR_GATE_FAILURE: SESSION_DEADLINE_EXCEEDED"
            elif "REQUEST_CEILING" in str(exc):
                err_code = "OPERATOR_GATE_FAILURE: REQUEST_CEILING_EXCEEDED"
            elif "CLEANUP_RESERVE" in str(exc):
                err_code = "OPERATOR_GATE_FAILURE: CLEANUP_RESERVE_EXHAUSTED"
            elif "OWNERSHIP_MISMATCH" in str(exc):
                err_code = "OPERATOR_GATE_FAILURE: OWNERSHIP_MISMATCH_HALT"
            elif "HUMAN_WINDOW" in str(exc):
                err_code = "OPERATOR_GATE_FAILURE: HUMAN_WINDOW_REQUIRED"
            errors.append(err_code)
            session_status = LiveReadyOperatorStatus.FAILED

            if self.audit_state not in (
                AuditOutcomeState.CLEAN,
                AuditOutcomeState.RESIDUAL,
                AuditOutcomeState.OWNERSHIP_MISMATCH_HALT,
            ):
                self.audit_state = AuditOutcomeState.UNKNOWN
            self.halt_before_cleanup = True

        finally:
            if self.security_boundary is not None:
                self.security_boundary.close_session()
                self.security_boundary.close()
            if hasattr(self.client, "close"):
                self.client.close()

            # Unlink manifest if and only if strictly CLEAN and session PASSED
            manifest_preserved = True
            if self.manifest_path and self.manifest:
                if (
                    session_status == LiveReadyOperatorStatus.PASSED
                    and self.audit_state == AuditOutcomeState.CLEAN
                ):
                    manifest_preserved = not self.manifest.delete_if_clean(
                        self.manifest_path, self.audit_state
                    )
                else:
                    manifest_preserved = True

            duration = time.perf_counter() - start_time

        cleanup_reserve_used = max(0, self.http_requests_total - self.request_cutoff)

        return LiveReadyOperatorReport(
            run_id=self.run_id,
            status=session_status,
            audit_state=self.audit_state,
            preflight_passed=self._preflight_accepted,
            human_window_confirmed=self.human_window_confirmed,
            destination_url=self.destination_url,
            manifest_path=str(self.manifest_path or ""),
            manifest_preserved=manifest_preserved,
            http_requests_total=self.http_requests_total,
            http_requests_ceiling=self.request_ceiling,
            cleanup_reserve_used=cleanup_reserve_used,
            duration_seconds=duration,
            stages=list(self.stages),
            errors=errors,
            lockdown_recorded=self.lockdown_recorded,
            live_execution_refused=self.live_execution_refused,
            security_boundary_active=self.security_boundary is not None,
            recovery_notice=(
                "SEPARATE_OPERATOR_REVIEWED_RECOVERY_REQUIRED: Automatic retry or deletion is prohibited; "
                "preserved manifest on disk requires a separate, independently reviewed recovery procedure."
                if session_status != LiveReadyOperatorStatus.PASSED or self.audit_state != AuditOutcomeState.CLEAN
                else None
            ),
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="efso-radar-staging-live-ready-operator",
        description="Phase 5B.3G-C Staging Operator Candidate (Default Deny).",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Attempt live execution (PERMANENTLY DENIED).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Execute remote commands (PERMANENTLY DENIED).",
    )
    parser.add_argument(
        "--enable-writes",
        action="store_true",
        default=False,
        help="Enable mutating writes (PERMANENTLY DENIED).",
    )
    parser.add_argument(
        "--simulate-offline",
        action="store_true",
        default=False,
        help="Run hermetic offline mock simulation.",
    )
    parser.add_argument(
        "--auto-human-window",
        action="store_true",
        default=False,
        help="Simulate human approval window confirmation for hermetic automated tests.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Output JSON serialized report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.live or args.execute or args.enable_writes:
        print(
            "STOP: Live execution is strictly prohibited. Phase 5B.3G-C candidate "
            "operator is review-only. Live execution requires future explicit Product "
            "Owner authorization and security review.",
            file=sys.stderr,
        )
        return 2

    if not args.simulate_offline:
        print(
            "STOP: Live Turso connection is disabled by default. Pass --simulate-offline "
            "for offline verification.",
            file=sys.stderr,
        )
        return 2

    print("Running Phase 5B.3G-C hermetic offline simulation...")
    import sqlite3
    from efso_radar.repositories.schema import get_all_init_statements
    from efso_radar.staging.runner import BudgetTrackingTransport, HermeticStagingTransport

    conn = sqlite3.connect(":memory:")
    for stmt in get_all_init_statements():
        conn.execute(stmt)
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
        "VALUES (6, '2026-10-27T14:00:00Z', 'Schema v6')"
    )
    conn.commit()

    raw = HermeticStagingTransport(conn)
    budget = BudgetTrackingTransport(raw)
    client = TursoHTTPClient(
        database_url=EXACT_STAGING_DESTINATION,
        auth_token="synthetic-offline-only",
        transport=budget,
    )
    operator = LiveReadyStagingOperator(
        client=client,
        live_authorized=False,
    )
    report = operator.run(auto_human_window=True, human_marker="MOCK_ONLY")
    print(report.summary())
    return 0 if report.status == LiveReadyOperatorStatus.PASSED else 1


if __name__ == "__main__":
    sys.exit(main())
