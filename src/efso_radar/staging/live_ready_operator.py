"""Phase 5B.3G-C Live-Ready Staging Operator Candidate (Default Deny).

This operator candidate reuses existing offline components to establish a
security-reviewed, deny-by-default execution pipeline for Turso staging.

SAFETY NOTICE:
- Live Turso cloud execution is PERMANENTLY DISABLED BY DEFAULT.
- This module does NOT authorize connecting to real Turso, creating tokens,
  or unblocking Block Writes on Turso staging.
- Any live execution requires future, explicit, separate Product Owner authorization.
- All testing and validation is strictly hermetic (in-memory SQLite + mock HTTP).
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

_ISO_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
_CANONICAL_STAMP = "2026-10-25T15:00:00Z"


def _is_valid_timestamp(val: Any) -> bool:
    return isinstance(val, str) and (val == _CANONICAL_STAMP or bool(_ISO_TIMESTAMP_RE.match(val)))


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

    def summary(self) -> str:
        lines = [
            f"=== EFSO RADAR STAGING OPERATOR CANDIDATE [{self.status.value}] ===",
            f"Run ID: {self.run_id}",
            f"Destination: {mask_turso_url(self.destination_url)}",
            f"Preflight Passed: {self.preflight_passed}",
            f"Human Window Confirmed: {self.human_window_confirmed}",
            f"Live Execution Refused (Default-Deny): {self.live_execution_refused}",
            f"Audit Outcome State: {self.audit_state.value}",
            f"Manifest Path: {self.manifest_path}",
            f"Manifest Preserved: {self.manifest_preserved}",
            f"HTTP Requests: {self.http_requests_total}/{self.http_requests_ceiling} "
            f"(cleanup reserve used: {self.cleanup_reserve_used}/{CLEANUP_RESERVE_REQUESTS})",
            f"Duration: {self.duration_seconds:.2f}s (ceiling: {SESSION_TIMEOUT_SECONDS}s)",
            f"Post-Run Lockdown Recorded: {self.lockdown_recorded}",
            f"Stages Executed: {len(self.stages)}",
        ]
        for st in self.stages:
            st_res = "PASS" if st.success else "FAIL"
            lines.append(f"  [{st_res}] {st.stage_id}: {st.name} ({st.http_requests_used} calls, {st.duration_seconds:.3f}s)")
        if self.errors:
            lines.append("Errors:")
            for err in self.errors:
                lines.append(f"  - {err}")
        return "\n".join(lines)


class LiveReadyStagingOperator(SealedHermeticStagingRunner):
    """Phase 5B.3G-C Staging Operator Candidate.

    Enforces all safety gates:
    1. Default-deny live Turso execution (rejects non-hermetic transports).
    2. Crash-recoverable ownership manifest & seal before any mutation.
    3. LIVE-01 preflight & baseline collision check under Block Writes ON.
    4. Human approval window gate before any write mutation.
    5. Session deadline (<120.0s) and request budget (<100 requests, 20 cleanup reserve).
    6. Ambiguous COMMIT => UNKNOWN, no blind retry, no cleanup, manifest retained.
    7. LIVE-08 pre-deletion ownership check, conditional DELETE (affected == 1), 3-state audit.
    8. Post-run lockdown recording.
    9. Sanitized error messages with zero credential / SQL leakage.
    """

    def __init__(
        self,
        client: TursoHTTPClient,
        destination_url: str = EXACT_STAGING_DESTINATION,
        manifest_dir: Optional[Path] = None,
        *,
        live_authorized: bool = False,
        clock: Callable[[], float] = time.monotonic,
        simulate_ownership_mismatch: bool = False,
        simulate_rollback_persisted: bool = False,
        simulate_audit_failure: bool = False,
        simulate_ambiguous_contradictory: bool = False,
        simulate_ambiguous_inspection_failure: bool = False,
    ) -> None:
        if not live_authorized:
            # Default-deny: requires concrete hermetic transport
            transport = getattr(client, "transport", None)
            verify_hermetic_session_transport(transport)
        else:
            # Live Turso is strictly disabled by default pending future PO authorization
            raise PermissionError(
                "LIVE_TURSO_UNAUTHORIZED: Live Turso staging execution is permanently "
                "disabled by default pending PO authorization."
            )

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
        self.human_window_confirmed = False
        self._gate: Optional[OfflineSessionGate] = None
        self.lockdown_recorded = False
        self.live_execution_refused = not live_authorized
        self._in_cleanup = False

    def _create_and_persist_manifest(self) -> None:
        super()._create_and_persist_manifest()
        if self._seal is not None:
            self._gate = OfflineSessionGate(
                expected_manifest_sha256=self._seal.sha256,
                clock=self._clock,
            )

    def _check_budget_and_deadline(self, cleanup: bool = False) -> None:
        now = self._clock()
        if now - self._operator_started_at >= SESSION_TIMEOUT_SECONDS:
            self._deny("SESSION_DEADLINE_EXCEEDED")
        if self.http_requests_total >= MAX_REQUEST_CEILING:
            self._deny("REQUEST_CEILING")
        if not cleanup and self.http_requests_total >= TEST_CUTOFF_REQUESTS:
            self._deny("CLEANUP_RESERVE")

    def _run_live_01(self) -> None:
        self._check_budget_and_deadline(cleanup=False)
        req_start = self.http_requests_total
        super()._run_live_01()
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
            self._deny("PARAMETERS_INVALID")
        if type(params[0]) is not str:
            self._deny("PARAMETERS_INVALID")

        assert params is not None
        target = params[0]
        entity = next(
            (e for e in create_default_manifest_entities(self.run_id)
             if e.table == table and e.pk_value == target),
            None,
        )
        if entity is None:
            self._deny("UNREGISTERED_IDENTITY")

        try:
            self._seal.require_registered_identity(
                entity.table, entity.pk_column, entity.pk_value
            )
        except Exception:
            self._deny("MANIFEST_INVALID")

        expected = entity.expected_attributes
        stamp = _CANONICAL_STAMP
        if table == "processed_articles":
            if operation == "insert":
                suffix = "/01" if target == f"synth_art_{self.run_id}_01" else "/02"
                allowed_urls = {f"https://example.com/synth/{self.run_id}{suffix}"}
                if suffix == "/02":
                    allowed_urls.add(f"https://example.com/synth/{self.run_id}/02_duplicate")
                if (
                    params[1] not in allowed_urls
                    or params[2:4] != [expected["title"], expected["source"]]
                    or (params[4:] != [stamp, stamp] and not (_is_valid_timestamp(params[4]) and _is_valid_timestamp(params[5])))
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["source"], expected["title"]]:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "stories":
            if operation == "insert":
                try:
                    article_ids = json.loads(params[5])
                except (ValueError, TypeError):
                    self._deny("OWNERSHIP_MISMATCH")
                if (
                    params[1:3] != [expected["headline_tr"], expected["category"]]
                    or (params[3:5] != [stamp, stamp] and not (_is_valid_timestamp(params[3]) and _is_valid_timestamp(params[4])))
                    or article_ids != [f"synth_art_{self.run_id}_01"]
                    or type(params[6]) is not int
                    or type(params[7]) is not int
                    or params[6:] != [1, 1]
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["category"], expected["headline_tr"]]:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "run_locks":
            workers = expected["worker_ids"]
            if operation == "insert":
                accepted = (
                    [workers[0], stamp, "2026-10-25T15:05:00Z", stamp],
                    [workers[1], stamp, "2026-10-25T15:05:00Z", stamp],
                    [
                        workers[1],
                        "2026-10-25T15:06:00Z",
                        "2026-10-25T15:11:00Z",
                        "2026-10-25T15:06:00Z",
                    ],
                )
                if params[1:] not in accepted and not (
                    params[1] in workers
                    and _is_valid_timestamp(params[2])
                    and _is_valid_timestamp(params[3])
                    and _is_valid_timestamp(params[4])
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != workers:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "durable_recovery_state":
            if operation == "insert":
                if (
                    params[1:3] != [expected["stage"], expected["worker_id"]]
                    or params[3] != json.dumps({"cursor": 100})
                    or (params[4:] != [stamp, stamp] and not (_is_valid_timestamp(params[4]) and _is_valid_timestamp(params[5])))
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["stage"], expected["worker_id"]]:
                self._deny("OWNERSHIP_MISMATCH")

    def _guard_read(self, sql: str, params: list[Any] | None) -> None:
        self._check_budget_and_deadline(cleanup=self._in_cleanup)
        super()._guard_read(sql, params)

    def _execute(self, sql: str, params: list[Any] | None = None) -> Any:
        self._check_budget_and_deadline(cleanup=self._in_cleanup)
        return super()._execute(sql, params)

    def _execute_transaction(self, statements: list[tuple[str, list[Any]]]) -> list[Any]:
        self._check_budget_and_deadline(cleanup=self._in_cleanup)
        try:
            return super()._execute_transaction(statements)
        except Exception as exc:
            if is_ambiguous_transaction_error(exc) or "timeout" in str(exc).lower():
                self.halt_before_cleanup = True
                self.audit_state = AuditOutcomeState.UNKNOWN
                self._deny("AMBIGUOUS_COMMIT_UNKNOWN")
            raise

    def _run_live_02(self) -> None:
        if not self._preflight_accepted:
            self._deny("PREFLIGHT_REQUIRED")
        if not self.human_window_confirmed:
            self._deny("HUMAN_WINDOW_REQUIRED")
        req_start = self.http_requests_total
        super()._run_live_02()
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-02",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_03(self) -> None:
        req_start = self.http_requests_total
        super()._run_live_03()
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-03",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_04(self) -> None:
        req_start = self.http_requests_total
        super()._run_live_04()
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-04",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_05(self) -> None:
        req_start = self.http_requests_total
        super()._run_live_05()
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-05",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_06(self) -> None:
        req_start = self.http_requests_total
        super()._run_live_06()
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-06",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_07(self) -> None:
        req_start = self.http_requests_total
        super()._run_live_07()
        req_used = self.http_requests_total - req_start
        if self._gate is not None and self._seal is not None:
            self._gate.record_simulated_stage(
                stage="LIVE-07",
                manifest_sha256=self._seal.sha256,
                request_count=req_used,
            )

    def _run_live_08(self) -> None:
        self._in_cleanup = True
        bt = getattr(self.client, "transport", None)
        if bt and hasattr(bt, "in_cleanup"):
            bt.in_cleanup = True
        req_start = self.http_requests_total
        try:
            super()._run_live_08()
        except PermissionError as exc:
            if "OWNERSHIP_MISMATCH_HALT" in str(exc):
                self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                self.halt_before_cleanup = True
            raise
        except Exception:
            raise
        finally:
            req_used = self.http_requests_total - req_start
            if self._gate is not None and self._seal is not None:
                mock_outcome = {
                    AuditOutcomeState.CLEAN: MockAudit.CLEAN,
                    AuditOutcomeState.RESIDUAL: MockAudit.RESIDUAL,
                }.get(self.audit_state, MockAudit.UNKNOWN)
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
        """Confirm post-flight lockdown state (Block Writes ON and token revocation)."""
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

            # 4. LIVE-02 to LIVE-07 Controlled Write Stages
            if self.human_window_confirmed:
                self._run_live_02()
                self._run_live_03()
                self._run_live_04()
                self._run_live_05()
                self._run_live_06()
                self._run_live_07()

            # 5. LIVE-08 Pre-Deletion Ownership Check, Conditional DELETE, 3-State Audit
            self._run_live_08()

            # 6. Post-Run Lockdown Recording
            try:
                self.record_lockdown(marker=human_marker, block_writes_on=True, token_revoked=True)
            except Exception:
                pass

            if self.audit_state == AuditOutcomeState.CLEAN:
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
            # Unlink manifest if and only if strictly CLEAN
            manifest_preserved = True
            if self.manifest_path and self.manifest:
                if self.audit_state == AuditOutcomeState.CLEAN:
                    manifest_preserved = not self.manifest.delete_if_clean(
                        self.manifest_path, self.audit_state
                    )
                else:
                    manifest_preserved = True

            duration = time.perf_counter() - start_time

        cleanup_reserve_used = max(0, self.http_requests_total - TEST_CUTOFF_REQUESTS)

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
            http_requests_ceiling=MAX_REQUEST_CEILING,
            cleanup_reserve_used=cleanup_reserve_used,
            duration_seconds=duration,
            stages=self.stages,
            errors=errors,
            lockdown_recorded=self.lockdown_recorded,
            live_execution_refused=self.live_execution_refused,
        )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Phase 5B.3G-C Live-Ready Staging Operator Candidate (Default Deny / Live Execution Disabled)"
    )
    parser.add_argument(
        "--simulate-offline",
        action="store_true",
        help="Run sealed hermetic offline simulation.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="DISALLOWED: Live staging execution flag (strictly rejected).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="DISALLOWED: Live execution flag (strictly rejected).",
    )
    parser.add_argument(
        "--enable-writes",
        action="store_true",
        help="DISALLOWED: Live write enablement flag (strictly rejected).",
    )

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
    raise SystemExit(main())
