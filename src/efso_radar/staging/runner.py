"""Hermetic staging runner foundation for Efso Radar Phase 5B.3G-B (Offline Only).

Provides:
- Exact staging destination allowlisting and fail-closed validation.
- Collision-resistant unique run ID and synthetic entity generation.
- Allowlisted, typed per-run ownership manifest with atomic disk persistence.
- Offline crash-recovery validation and simulation (--recover-manifest).
- Hermetic simulation of LIVE-01 through LIVE-08 test stages over mock Hrana v2 transport.
- Strict request-budget instrumentation with a single hard ceiling (<100 requests)
  and a dedicated 20-request cleanup planning reserve.
- Parameterized conditional deletion enforcing both exact PK and synthetic ownership attributes.
- Three-state audit verification (CLEAN / RESIDUAL / UNKNOWN / OWNERSHIP_MISMATCH_HALT).
- Dry-run-only CLI strictly rejecting any real-network or live execution flags.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time
from typing import Any, Optional
from urllib.parse import urlparse
import uuid

import httpx
from pydantic import BaseModel, Field

from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION, get_all_init_statements
from efso_radar.repositories.turso import TursoReadOnlyStateRepository
from efso_radar.repositories.turso_client import (
    TursoClientError,
    TursoHTTPClient,
    deserialize_hrana_value,
    mask_turso_url,
    normalize_turso_url,
    serialize_hrana_arg,
)
from efso_radar.staging.preflight import CloudPreflightValidator

logger = logging.getLogger("efso-radar.staging.runner")

# =============================================================================
# Constants & Allowlisted Configuration
# =============================================================================

EXACT_STAGING_HOSTNAME = "efso-radar-staging-fatihemres.aws-eu-north-1.turso.io"
EXACT_STAGING_DESTINATION = f"libsql://{EXACT_STAGING_HOSTNAME}"
EXACT_STAGING_HTTPS_URL = f"https://{EXACT_STAGING_HOSTNAME}"

# Exactly four business tables may receive controlled synthetic mutations
ALLOWED_TABLE_PK_MAP: dict[str, str] = {
    "processed_articles": "article_id",
    "stories": "story_id",
    "run_locks": "lock_key",
    "durable_recovery_state": "state_key",
}

# The remaining 11 Schema v6 tables are strictly read-only
READ_ONLY_SCHEMA_V6_TABLES: frozenset[str] = frozenset({
    "schema_migrations",
    "story_updates",
    "notifications",
    "editorial_assessments",
    "owner_feedback",
    "candidate_backlog",
    "ai_usage_ledger",
    "telegram_delivery_records",
    "admin_operational_reports",
    "experiment_budget_history",
    "scheduled_execution_history",
})

# Operational Request Budget Constraints
MAX_REQUEST_CEILING = 100
TEST_CUTOFF_REQUESTS = 80
CLEANUP_RESERVE_REQUESTS = 20
SESSION_TIMEOUT_SECONDS = 120.0
PER_OPERATION_TIMEOUT_SECONDS = 15.0

RUN_ID_REGEX = re.compile(r"^run_\d{8}T\d{6}Z_[a-f0-9]{12}$")

DISALLOWED_LIVE_FLAGS = frozenset({
    "--live",
    "--execute",
    "--enable-writes",
    "--run-live",
    "--prod",
    "--production",
    "--staging-live",
    "--unblock-writes",
})


# =============================================================================
# Destination Validation
# =============================================================================

def validate_staging_destination(url: str) -> str:
    """Validate that the target URL matches the exact allowlisted staging destination.

    Fails closed on any discrepancy, wildcard, alternative subdomain, IP address, or port.
    Returns the canonicalized https:// pipeline URL on success.
    """
    if not isinstance(url, str) or not url.strip():
        raise ValueError("Staging destination URL cannot be empty.")

    raw = url.strip()
    parsed = urlparse(raw)

    if parsed.scheme not in ("libsql", "https"):
        raise PermissionError(
            f"Staging destination scheme '{parsed.scheme}' disallowed: must be 'libsql' or 'https'."
        )

    hostname = (parsed.hostname or "").lower()
    if hostname != EXACT_STAGING_HOSTNAME:
        raise PermissionError(
            f"Destination security violation: target hostname '{hostname}' does not match "
            f"exact allowlisted staging host '{EXACT_STAGING_HOSTNAME}'."
        )

    if parsed.username or parsed.password:
        raise PermissionError(
            "Destination security violation: credentials embedded in destination URL are strictly prohibited."
        )

    if parsed.port is not None and parsed.port not in (443, 80):
        raise PermissionError(
            f"Destination security violation: non-standard port '{parsed.port}' disallowed."
        )

    return EXACT_STAGING_HTTPS_URL


# =============================================================================
# Manifest & Entity Models
# =============================================================================

@dataclass(frozen=True)
class RegisteredEntity:
    """An individual synthetic entity registered in the per-run ownership manifest."""
    table: str
    pk_column: str
    pk_value: str
    cleanup_dependency_order: int
    expected_attributes: dict[str, Any]


class AuditOutcomeState(str, Enum):
    CLEAN = "CLEAN"
    RESIDUAL = "RESIDUAL"
    UNKNOWN = "UNKNOWN"
    OWNERSHIP_MISMATCH_HALT = "OWNERSHIP_MISMATCH_HALT"


class SessionStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


class PerRunOwnershipManifest(BaseModel):
    """Crash-recoverable, allowlisted on-disk manifest tracking registered synthetic entities.

    Stores ONLY structural metadata and expected synthetic attributes.
    Zero tokens, credentials, database URLs, or private identifiers are permitted.
    """
    run_id: str
    created_at: str
    version: int = 1
    registered_entities: list[dict[str, Any]]

    def save_atomic(self, directory: Optional[Path] = None) -> Path:
        """Atomically persist manifest to an ephemeral temporary directory with safe permissions."""
        if not RUN_ID_REGEX.match(self.run_id):
            raise ValueError(f"Invalid run_id format for manifest: {self.run_id}")

        target_dir = directory or Path(tempfile.gettempdir())
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"efso_radar_staging_manifest_{self.run_id}.json"
        target_path = target_dir / filename

        # Ensure no path traversal
        try:
            target_path.resolve().relative_to(target_dir.resolve())
        except ValueError:
            raise PermissionError("Path traversal violation in manifest persistence.")

        content = self.model_dump_json(indent=2)

        # Atomic write via temporary file
        temp_fd, temp_path_str = tempfile.mkstemp(
            prefix=f"manifest_tmp_{self.run_id}_",
            suffix=".tmp",
            dir=str(target_dir),
        )
        temp_path = Path(temp_path_str)
        try:
            with open(temp_fd, "w", encoding="utf-8") as f:
                f.write(content)
            # Apply 0600 permissions on POSIX
            if hasattr(os, "chmod") and sys.platform != "win32":
                try:
                    os.chmod(temp_path, 0o600)
                except OSError:
                    pass
            temp_path.replace(target_path)
        except Exception:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            raise

        return target_path

    @classmethod
    def load_and_validate(cls, path: Path) -> PerRunOwnershipManifest:
        """Load manifest from disk and strictly validate schema, allowlists, and key derivation."""
        if not path.is_file():
            raise FileNotFoundError(f"Manifest file not found or not a regular file: {path}")

        # Check for symlink abuse
        if path.is_symlink():
            raise PermissionError(f"Manifest security violation: symlinks are not permitted: {path}")

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            raise ValueError(f"Manifest corrupt or invalid JSON ({type(exc).__name__})") from None

        if not isinstance(data, dict):
            raise ValueError("Manifest root must be a JSON object.")

        run_id = data.get("run_id")
        if not isinstance(run_id, str) or not RUN_ID_REGEX.match(run_id):
            raise ValueError(f"Manifest missing or invalid run_id: {run_id}")

        if data.get("version") != 1:
            raise ValueError(f"Unsupported manifest version: {data.get('version')}")

        raw_entities = data.get("registered_entities")
        if not isinstance(raw_entities, list) or len(raw_entities) == 0:
            raise ValueError("Manifest must contain a non-empty registered_entities list.")

        # Validate each entity against strict allowlists and run-id derivation
        for idx, ent in enumerate(raw_entities):
            if not isinstance(ent, dict):
                raise ValueError(f"Entity at index {idx} is not an object.")

            table = ent.get("table")
            if table not in ALLOWED_TABLE_PK_MAP:
                raise PermissionError(
                    f"Manifest contains unauthorized table '{table}'. Allowed: {sorted(ALLOWED_TABLE_PK_MAP.keys())}"
                )

            expected_pk_col = ALLOWED_TABLE_PK_MAP[table]
            actual_pk_col = ent.get("pk_column")
            if actual_pk_col != expected_pk_col:
                raise ValueError(
                    f"Entity table '{table}' has unexpected pk_column '{actual_pk_col}', expected '{expected_pk_col}'."
                )

            pk_value = ent.get("pk_value")
            if not isinstance(pk_value, str) or not pk_value.strip():
                raise ValueError(f"Entity table '{table}' missing valid pk_value.")

            # Validate synthetic key derivation from run_id
            if table == "processed_articles":
                if not pk_value.startswith(f"synth_art_{run_id}_"):
                    raise ValueError(f"processed_articles PK '{pk_value}' not derived from run_id '{run_id}'.")
            elif table == "stories":
                if not pk_value.startswith(f"synth_story_{run_id}_"):
                    raise ValueError(f"stories PK '{pk_value}' not derived from run_id '{run_id}'.")
            elif table == "run_locks":
                if pk_value != f"synth_lock_{run_id}":
                    raise ValueError(f"run_locks PK '{pk_value}' does not match expected 'synth_lock_{run_id}'.")
                if "radar_cron" in pk_value:
                    raise PermissionError("Critical safety violation: manifest cannot target 'radar_cron'.")
            elif table == "durable_recovery_state":
                expected_state_key = f"synth_stage_{run_id}::current"
                if pk_value != expected_state_key:
                    raise ValueError(f"durable_recovery_state PK '{pk_value}' does not match '{expected_state_key}'.")

            expected_attrs = ent.get("expected_attributes")
            if not isinstance(expected_attrs, dict):
                raise ValueError(f"Entity table '{table}' expected_attributes must be a dictionary.")

        return cls(**data)

    def delete_if_clean(self, path: Path, audit_state: AuditOutcomeState) -> bool:
        """Unlink manifest file if and only if audit state is CLEAN. Retain otherwise."""
        if audit_state == AuditOutcomeState.CLEAN:
            if path.exists():
                try:
                    path.unlink()
                    return True
                except OSError as exc:
                    logger.warning("Could not unlink clean manifest file %s: %s", path, exc)
            return False
        else:
            logger.info("Manifest preserved on disk due to audit state '%s': %s", audit_state.value, path)
            return False


# =============================================================================
# Stage & Session Reporting Models
# =============================================================================

class StageResult(BaseModel):
    stage_id: str
    name: str
    success: bool
    http_requests_used: int
    duration_seconds: float
    details: dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class StagingRunnerReport(BaseModel):
    run_id: str
    status: SessionStatus
    audit_state: AuditOutcomeState
    destination_url: str
    manifest_path: str
    manifest_preserved: bool
    http_requests_total: int
    http_requests_ceiling: int = MAX_REQUEST_CEILING
    cleanup_reserve_used: int
    synthetic_rows_written_peak: int
    total_mutating_operations: int
    stages: list[StageResult] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"=== EFSO RADAR STAGING RUNNER REPORT [{self.status.value}] ===",
            f"Run ID: {self.run_id}",
            f"Destination: {mask_turso_url(self.destination_url)}",
            f"Audit Outcome State: {self.audit_state.value}",
            f"Manifest Path: {self.manifest_path} (Preserved: {'YES' if self.manifest_preserved else 'NO'})",
            f"HTTP Requests: {self.http_requests_total}/{self.http_requests_ceiling} (Cleanup Reserve Used: {self.cleanup_reserve_used}/{CLEANUP_RESERVE_REQUESTS})",
            f"Synthetic Rows Peak: {self.synthetic_rows_written_peak} | Mutating SQL Operations: {self.total_mutating_operations}",
            "Stages Executed:",
        ]
        for s in self.stages:
            st = "PASSED" if s.success else "FAILED"
            lines.append(f"  [{st}] {s.stage_id}: {s.name} ({s.http_requests_used} calls, {s.duration_seconds:.2f}s)")
        if self.errors:
            lines.append("Errors Encountered:")
            for err in self.errors:
                lines.append(f"  - {err}")
        return "\n".join(lines)


# =============================================================================
# Stateful Hermetic Hrana v2 Mock Transport with Budget Instrumentation
# =============================================================================

class BudgetTrackingTransport(httpx.BaseTransport):
    """Wraps an underlying transport to enforce hard request limits and cleanup reserves."""

    def __init__(
        self,
        inner: httpx.BaseTransport,
        ceiling: int = MAX_REQUEST_CEILING,
        cutoff: int = TEST_CUTOFF_REQUESTS,
    ):
        self.inner = inner
        self.request_count = 0
        self.cleanup_request_count = 0
        self.in_cleanup = False
        self.ceiling = ceiling
        self.cutoff = cutoff

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.request_count += 1
        if self.in_cleanup:
            self.cleanup_request_count += 1

        if self.request_count >= self.ceiling:
            raise TursoClientError(
                f"BUDGET_EXHAUSTED_AUDIT_INCOMPLETE: Reached hard request ceiling ({self.ceiling})."
            )

        if not self.in_cleanup and self.request_count >= self.cutoff:
            raise TursoClientError(
                f"TEST_CUTOFF_REACHED: Reached test step cutoff ({self.cutoff}); entering cleanup."
            )

        return self.inner.handle_request(request)


class HermeticStagingTransport(httpx.BaseTransport):
    """Deterministic, in-memory stateful mock transport for Hrana v2 pipeline emulation.

    Translates pipeline requests into SQLite transactions against an isolated temporary fixture.
    Supports failure injections (BEGIN errors, statement errors, COMMIT timeouts, network drops).
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        fail_on_commit_timeout: bool = False,
        fail_on_commit_network: bool = False,
        fail_on_commit_status: Optional[int] = None,
        fail_on_stmt_idx: Optional[int] = None,
        fail_stmt_code: str = "SQLITE_CONSTRAINT",
    ):
        self.conn = conn
        self.fail_on_commit_timeout = fail_on_commit_timeout
        self.fail_on_commit_network = fail_on_commit_network
        self.fail_on_commit_status = fail_on_commit_status
        self.fail_on_stmt_idx = fail_on_stmt_idx
        self.fail_stmt_code = fail_stmt_code

        self.session_counter = 0
        self.active_batons: set[str] = set()
        self.in_transaction: dict[str, bool] = {}

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        try:
            body = json.loads(request.content.decode("utf-8"))
        except Exception:
            body = {}

        in_baton = body.get("baton")
        requests_list = body.get("requests", [])

        if in_baton is not None and in_baton not in self.active_batons:
            return httpx.Response(
                400,
                json={"error": {"code": "HRANA_PROTO_ERROR", "message": "Invalid or expired session baton"}},
                request=request,
            )

        # Check for COMMIT injections
        has_commit = any(
            req.get("type") == "execute" and req.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
            for req in requests_list
        )
        if has_commit:
            if self.fail_on_commit_timeout:
                raise httpx.TimeoutException("Simulated connection timeout during COMMIT")
            if self.fail_on_commit_network:
                raise httpx.ConnectError("Simulated connection reset during COMMIT")
            if self.fail_on_commit_status is not None:
                return httpx.Response(self.fail_on_commit_status, json={"message": "Gateway error"}, request=request)

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

                if upper_sql == "BEGIN":
                    self.session_counter += 1
                    out_baton = f"mock-baton-{self.session_counter}"
                    self.active_batons.add(out_baton)
                    self.in_transaction[out_baton] = True
                    try:
                        self.conn.execute("BEGIN")
                    except sqlite3.OperationalError:
                        pass
                    results.append({"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}})
                    continue

                elif upper_sql == "COMMIT":
                    try:
                        self.conn.commit()
                        if in_baton in self.in_transaction:
                            self.in_transaction[in_baton] = False
                        results.append({"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}})
                    except sqlite3.Error as exc:
                        results.append({"type": "error", "error": {"code": "SQLITE_ERROR", "message": str(exc)}})
                    continue

                elif upper_sql == "ROLLBACK":
                    try:
                        self.conn.rollback()
                        if in_baton in self.in_transaction:
                            self.in_transaction[in_baton] = False
                        results.append({"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}})
                    except sqlite3.Error as exc:
                        results.append({"type": "error", "error": {"code": "SQLITE_ERROR", "message": str(exc)}})
                    continue

                # User SQL Statement
                stmt_exec_idx += 1
                if self.fail_on_stmt_idx is not None and stmt_exec_idx == self.fail_on_stmt_idx:
                    results.append({
                        "type": "error",
                        "error": {
                            "code": self.fail_stmt_code,
                            "message": "Simulated statement error in mock transport",
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
                    code = "SQLITE_CONSTRAINT" if "constraint" in str(exc).lower() else "SQLITE_ERROR"
                    results.append({
                        "type": "error",
                        "error": {
                            "code": code,
                            "message": "Execution error",
                        },
                    })
            elif req_type == "close":
                if in_baton in self.active_batons:
                    self.active_batons.discard(in_baton)
                results.append({"type": "ok"})

        return httpx.Response(200, json={"baton": out_baton, "results": results}, request=request)


# =============================================================================
# Hermetic Session Runner Engine
# =============================================================================

class StagingSessionRunner:
    """Orchestrates hermetic simulation of the eight Phase 5B.3G live staging test stages.

    Instrumented with single hard ceiling (<100 calls) and 20-request cleanup planning reserve.
    """

    def __init__(
        self,
        client: TursoHTTPClient,
        destination_url: str = EXACT_STAGING_DESTINATION,
        manifest_dir: Optional[Path] = None,
        *,
        simulate_ownership_mismatch: bool = False,
        simulate_rollback_persisted: bool = False,
        simulate_audit_failure: bool = False,
    ):
        self.destination_url = validate_staging_destination(destination_url)
        self.client = client
        self.manifest_dir = manifest_dir
        self.simulate_ownership_mismatch = simulate_ownership_mismatch
        self.simulate_rollback_persisted = simulate_rollback_persisted
        self.simulate_audit_failure = simulate_audit_failure

        self.run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid_hex(12)}"
        self.manifest: Optional[PerRunOwnershipManifest] = None
        self.manifest_path: Optional[Path] = None

        self.synthetic_rows_peak = 0
        self.total_mutating_operations = 0
        self.stages: list[StageResult] = []
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.audit_state = AuditOutcomeState.UNKNOWN
        self.session_status = SessionStatus.FAILED

    @property
    def budget_transport(self) -> Optional[BudgetTrackingTransport]:
        if isinstance(self.client.transport, BudgetTrackingTransport):
            return self.client.transport
        return None

    @property
    def http_requests_total(self) -> int:
        bt = self.budget_transport
        return bt.request_count if bt else 0

    @property
    def cleanup_reserve_used(self) -> int:
        bt = self.budget_transport
        return bt.cleanup_request_count if bt else 0

    def _execute(self, sql: str, params: Optional[list[Any]] = None) -> Any:
        upper = sql.strip().upper()
        if any(upper.startswith(v) for v in ("INSERT", "UPDATE", "DELETE")):
            self.total_mutating_operations += 1
        return self.client.execute(sql, params)

    def _execute_transaction(self, statements: list[tuple[str, list[Any]]]) -> list[Any]:
        upper_stmts = [s[0].strip().upper() for s in statements]
        for us in upper_stmts:
            if any(us.startswith(v) for v in ("INSERT", "UPDATE", "DELETE")):
                self.total_mutating_operations += 1
        return self.client.execute_transaction(statements)

    def _create_and_persist_manifest(self) -> None:
        """Register the 5 synthetic entities ahead of dispatch and persist to disk."""
        entities = [
            {
                "table": "processed_articles",
                "pk_column": "article_id",
                "pk_value": f"synth_art_{self.run_id}_01",
                "cleanup_dependency_order": 2,
                "expected_attributes": {
                    "source": "staging_synth_test",
                    "title_prefix": f"SYNTH_ARTICLE_{self.run_id}_01",
                },
            },
            {
                "table": "stories",
                "pk_column": "story_id",
                "pk_value": f"synth_story_{self.run_id}_01",
                "cleanup_dependency_order": 1,
                "expected_attributes": {
                    "category": "synth_test",
                    "headline_prefix": f"SYNTH_STORY_{self.run_id}_01",
                },
            },
            {
                "table": "processed_articles",
                "pk_column": "article_id",
                "pk_value": f"synth_art_{self.run_id}_02_rollback",
                "cleanup_dependency_order": 2,
                "expected_attributes": {
                    "source": "staging_synth_test",
                    "title_prefix": f"SYNTH_ARTICLE_{self.run_id}_02_ROLLBACK",
                },
            },
            {
                "table": "run_locks",
                "pk_column": "lock_key",
                "pk_value": f"synth_lock_{self.run_id}",
                "cleanup_dependency_order": 1,
                "expected_attributes": {
                    "worker_prefix": f"runner_alpha_{self.run_id}",
                },
            },
            {
                "table": "durable_recovery_state",
                "pk_column": "state_key",
                "pk_value": f"synth_stage_{self.run_id}::current",
                "cleanup_dependency_order": 1,
                "expected_attributes": {
                    "worker_prefix": f"synth_worker_{self.run_id}",
                },
            },
        ]

        self.manifest = PerRunOwnershipManifest(
            run_id=self.run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=entities,
        )
        self.manifest_path = self.manifest.save_atomic(self.manifest_dir)

    def run(self) -> StagingRunnerReport:
        """Execute full hermetic dry-run session across stages LIVE-01 through LIVE-08."""
        manifest_unlinked = False

        try:
            # Phase 1: Pre-Execution Safety Gate (LIVE-01)
            self._create_and_persist_manifest()
            self._run_live_01()

            # Phase 2: Controlled Live Execution (LIVE-02 through LIVE-07)
            self._run_live_02()
            self._run_live_03()
            self._run_live_04()
            self._run_live_05()
            self._run_live_06()
            self._run_live_07()

        except Exception as exc:
            self.errors.append(f"Execution suspended in testing phase: {type(exc).__name__}: {exc}")
        finally:
            # Phase 3: Exact-Match Cleanup & Three-State Audit (LIVE-08)
            bt = self.budget_transport
            if bt:
                bt.in_cleanup = True

            try:
                self._run_live_08()
                if self.manifest and self.manifest_path:
                    manifest_unlinked = self.manifest.delete_if_clean(self.manifest_path, self.audit_state)
            except Exception as cleanup_exc:
                self.errors.append(f"Cleanup failure in LIVE-08: {type(cleanup_exc).__name__}: {cleanup_exc}")
                if self.audit_state != AuditOutcomeState.OWNERSHIP_MISMATCH_HALT:
                    self.audit_state = AuditOutcomeState.UNKNOWN
            finally:
                # Teardown: close HTTP transports
                self.client.close()

        manifest_preserved = not manifest_unlinked
        self.session_status = (
            SessionStatus.PASSED
            if (len(self.errors) == 0 and self.audit_state == AuditOutcomeState.CLEAN)
            else SessionStatus.FAILED
        )

        return StagingRunnerReport(
            run_id=self.run_id,
            status=self.session_status,
            audit_state=self.audit_state,
            destination_url=self.destination_url,
            manifest_path=str(self.manifest_path or ""),
            manifest_preserved=manifest_preserved,
            http_requests_total=self.http_requests_total,
            cleanup_reserve_used=self.cleanup_reserve_used,
            synthetic_rows_written_peak=self.synthetic_rows_peak,
            total_mutating_operations=self.total_mutating_operations,
            stages=self.stages,
            errors=self.errors,
            warnings=self.warnings,
        )

    # -------------------------------------------------------------------------
    # Stage Implementations
    # -------------------------------------------------------------------------

    def _run_live_01(self) -> None:
        """LIVE-01: Full Preflight (41 calls) & Baseline Pre-Existence Verification (5 calls)."""
        start = time.perf_counter()
        req_start = self.http_requests_total

        # 1. Full non-mutating preflight validation using read-only repo adapter
        ro_client = TursoHTTPClient(
            database_url=self.destination_url,
            transport=self.client.transport,
            read_only=True,
        )
        ro_repo = TursoReadOnlyStateRepository(
            database_url=self.destination_url,
            client=ro_client,
        )
        validator = CloudPreflightValidator(repo=ro_repo)
        report = validator.validate()

        if not report.success:
            raise RuntimeError(f"Preflight validation failed: {report.errors}")

        # 2. Baseline pre-existence checks for all 5 registered manifest entities (5 calls)
        assert self.manifest is not None
        for ent in self.manifest.registered_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]
            res = self._execute(f"SELECT 1 AS found FROM {tbl} WHERE {pk_col} = ?", [pk_val])
            rows = res.fetchall()
            if rows:
                raise RuntimeError(
                    f"Baseline safety violation: synthetic entity {tbl}.{pk_val} pre-exists in staging database."
                )

        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-01",
            name="Read-Only Preflight & Baseline Pre-Existence Verification",
            success=True,
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
            details={"schema_version": report.schema_version, "tables_checked": report.tables_checked},
        ))

    def _run_live_02(self) -> None:
        """LIVE-02: Hrana v2 Multi-Statement Transaction Atomicity & Baton Read-Back (4 calls)."""
        start = time.perf_counter()
        req_start = self.http_requests_total

        art_id = f"synth_art_{self.run_id}_01"
        story_id = f"synth_story_{self.run_id}_01"

        stmts = [
            (
                "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    art_id,
                    f"https://example.com/synth/{self.run_id}/01",
                    f"SYNTH_ARTICLE_{self.run_id}_01",
                    "staging_synth_test",
                    "2026-10-25T15:00:00Z",
                    "2026-10-25T15:00:00Z",
                ],
            ),
            (
                "INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json, update_count, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    story_id,
                    f"SYNTH_STORY_{self.run_id}_01",
                    "synth_test",
                    "2026-10-25T15:00:00Z",
                    "2026-10-25T15:00:00Z",
                    json.dumps([art_id]),
                    1,
                    1,
                ],
            ),
        ]

        # Multi-statement transaction (2 calls: BEGIN+stmts, COMMIT+close)
        results = self._execute_transaction(stmts)
        if len(results) != 2:
            raise RuntimeError("Transaction atomicity error: expected 2 statement results.")

        # Read-back verification (2 calls)
        r_art = self._execute("SELECT title, source FROM processed_articles WHERE article_id = ?", [art_id])
        art_rows = r_art.fetchall()
        r_story = self._execute("SELECT headline_tr, category FROM stories WHERE story_id = ?", [story_id])
        story_rows = r_story.fetchall()

        if not (art_rows and story_rows):
            raise RuntimeError("Transaction atomicity read-back failed: records not found post-commit.")

        self.synthetic_rows_peak = max(self.synthetic_rows_peak, 2)
        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-02",
            name="Hrana v2 Multi-Statement Transaction Atomicity & Baton Read-Back",
            success=True,
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
        ))

    def _run_live_03(self) -> None:
        """LIVE-03: Real Remote Transaction Rollback on Statement Error (3 calls)."""
        start = time.perf_counter()
        req_start = self.http_requests_total

        art_id_rollback = f"synth_art_{self.run_id}_02_rollback"

        # Stmts: 1 valid INSERT, 1 invalid statement violating NOT NULL on title
        stmts = [
            (
                "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    art_id_rollback,
                    f"https://example.com/synth/{self.run_id}/02",
                    f"SYNTH_ARTICLE_{self.run_id}_02_ROLLBACK",
                    "staging_synth_test",
                    "2026-10-25T15:00:00Z",
                    "2026-10-25T15:00:00Z",
                ],
            ),
            (
                "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    f"synth_art_invalid_{self.run_id}",
                    "https://example.com/invalid",
                    None,  # Violates NOT NULL title
                    "staging_synth_test",
                    "2026-10-25T15:00:00Z",
                    "2026-10-25T15:00:00Z",
                ],
            ),
        ]

        rollback_succeeded = False
        try:
            self._execute_transaction(stmts)
        except TursoClientError:
            rollback_succeeded = True

        if not rollback_succeeded and not self.simulate_rollback_persisted:
            raise RuntimeError("LIVE-03 failed: transaction with statement error did not trigger rollback.")

        # Read-back verification (1 call)
        res = self._execute("SELECT article_id FROM processed_articles WHERE article_id = ?", [art_id_rollback])
        rows = res.fetchall()

        if self.simulate_rollback_persisted:
            # Simulate unexpected failure of rollback where tentative record persisted
            self._execute(
                "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    art_id_rollback,
                    f"https://example.com/synth/{self.run_id}/02",
                    f"SYNTH_ARTICLE_{self.run_id}_02_ROLLBACK",
                    "staging_synth_test",
                    "2026-10-25T15:00:00Z",
                    "2026-10-25T15:00:00Z",
                ],
            )
            self.synthetic_rows_peak = max(self.synthetic_rows_peak, 3)
        elif rows:
            raise RuntimeError("LIVE-03 rollback failure: tentative article persisted in database.")

        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-03",
            name="Real Remote Transaction Rollback on Statement Error",
            success=True,
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
        ))

    def _run_live_04(self) -> None:
        """LIVE-04: Sequential Remote CAS Query Semantics (Isolated Key) (3 calls)."""
        start = time.perf_counter()
        req_start = self.http_requests_total

        lock_key = f"synth_lock_{self.run_id}"
        if lock_key == "radar_cron":
            raise PermissionError("Critical safety violation: cannot target production lock 'radar_cron'.")

        worker_alpha = f"runner_alpha_{self.run_id}"
        worker_beta = f"runner_beta_{self.run_id}"
        t0 = "2026-10-25T15:00:00Z"
        expires = "2026-10-25T15:05:00Z"

        cas_sql = (
            "INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(lock_key) DO UPDATE SET "
            "worker_id = excluded.worker_id, "
            "acquired_at = excluded.acquired_at, "
            "expires_at = excluded.expires_at, "
            "heartbeat_at = excluded.heartbeat_at "
            "WHERE expires_at < excluded.acquired_at OR worker_id = excluded.worker_id"
        )

        # 1. Alpha acquires lock (affected = 1)
        res1 = self._execute(cas_sql, [lock_key, worker_alpha, t0, expires, t0])
        if res1.affected_row_count != 1:
            raise RuntimeError("LIVE-04 CAS failed: Alpha could not acquire lease.")

        # 2. Beta attempts conflict at same t0 (affected = 0)
        res2 = self._execute(cas_sql, [lock_key, worker_beta, t0, expires, t0])
        if res2.affected_row_count != 0:
            raise RuntimeError("LIVE-04 CAS failed: Beta unexpectedly acquired conflicting lease.")

        # 3. Alpha renews lock at same t0 (affected = 1)
        res3 = self._execute(cas_sql, [lock_key, worker_alpha, t0, expires, t0])
        if res3.affected_row_count != 1:
            raise RuntimeError("LIVE-04 CAS failed: Alpha could not renew own lease.")

        self.synthetic_rows_peak = max(self.synthetic_rows_peak, 3)
        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-04",
            name="Sequential Remote CAS Semantics & Ownership Enforcement",
            success=True,
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
        ))

    def _run_live_05(self) -> None:
        """LIVE-05: Expired Lease Takeover with Simulated Clock Progression (2 calls)."""
        start = time.perf_counter()
        req_start = self.http_requests_total

        lock_key = f"synth_lock_{self.run_id}"
        worker_beta = f"runner_beta_{self.run_id}"
        # Simulating time 6 minutes past expiration
        t_expired_takeover = "2026-10-25T15:06:00Z"
        expires_new = "2026-10-25T15:11:00Z"

        cas_sql = (
            "INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(lock_key) DO UPDATE SET "
            "worker_id = excluded.worker_id, "
            "acquired_at = excluded.acquired_at, "
            "expires_at = excluded.expires_at, "
            "heartbeat_at = excluded.heartbeat_at "
            "WHERE expires_at < excluded.acquired_at OR worker_id = excluded.worker_id"
        )

        # Beta takes over expired lease (1 call)
        res = self._execute(cas_sql, [lock_key, worker_beta, t_expired_takeover, expires_new, t_expired_takeover])
        if res.affected_row_count != 1:
            raise RuntimeError("LIVE-05 takeover failed: Beta could not claim expired lease.")

        # Read-back verification (1 call)
        res_read = self._execute("SELECT worker_id FROM run_locks WHERE lock_key = ?", [lock_key])
        rows = res_read.fetchall()
        if not (rows and rows[0].get("worker_id") == worker_beta):
            raise RuntimeError("LIVE-05 verification failed: worker_id is not Beta post-takeover.")

        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-05",
            name="Expired Lease Takeover with Simulated Clock Progression",
            success=True,
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
        ))

    def _run_live_06(self) -> None:
        """LIVE-06: Recovery Checkpoint Persistence & Idempotency (4 calls)."""
        start = time.perf_counter()
        req_start = self.http_requests_total

        stage = f"synth_stage_{self.run_id}"
        state_key = f"{stage}::current"
        worker_id = f"synth_worker_{self.run_id}"
        now_iso = "2026-10-25T15:00:00Z"
        payload = json.dumps({"cursor": 100})

        # 1. Initial read (returns None) (1 call)
        r0 = self._execute("SELECT state_json FROM durable_recovery_state WHERE state_key = ?", [state_key])
        if r0.fetchall():
            raise RuntimeError("LIVE-06 failed: recovery state pre-exists.")

        upsert_sql = (
            "INSERT INTO durable_recovery_state (state_key, stage, worker_id, state_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(state_key) DO UPDATE SET "
            "worker_id = excluded.worker_id, "
            "state_json = excluded.state_json, "
            "updated_at = excluded.updated_at"
        )

        # 2. Save recovery checkpoint (1 call)
        res1 = self._execute(upsert_sql, [state_key, stage, worker_id, payload, now_iso, now_iso])
        if res1.affected_row_count != 1:
            raise RuntimeError("LIVE-06 failed: checkpoint could not be saved.")

        # 3. Idempotent re-save (1 call)
        res2 = self._execute(upsert_sql, [state_key, stage, worker_id, payload, now_iso, now_iso])
        if res2.affected_row_count != 1:
            raise RuntimeError("LIVE-06 failed: idempotent re-save failed.")

        # 4. Read-back verification (1 call)
        r_read = self._execute("SELECT state_json FROM durable_recovery_state WHERE state_key = ?", [state_key])
        rows = r_read.fetchall()
        if not (rows and json.loads(rows[0].get("state_json", "{}")).get("cursor") == 100):
            raise RuntimeError("LIVE-06 read-back verification failed.")

        self.synthetic_rows_peak = max(self.synthetic_rows_peak, 4)
        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-06",
            name="Recovery Checkpoint Persistence & Idempotency",
            success=True,
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
        ))

    def _run_live_07(self) -> None:
        """LIVE-07: Ambiguous Outcome Comprehensive Protocol (2 calls)."""
        start = time.perf_counter()
        req_start = self.http_requests_total

        art_id = f"synth_art_{self.run_id}_01"
        story_id = f"synth_story_{self.run_id}_01"

        # Protocol verification: queries both processed_articles and stories (2 calls)
        r_art = self._execute("SELECT 1 AS f FROM processed_articles WHERE article_id = ?", [art_id])
        r_story = self._execute("SELECT 1 AS f FROM stories WHERE story_id = ?", [story_id])

        art_present = bool(r_art.fetchall())
        story_present = bool(r_story.fetchall())

        if not (art_present and story_present):
            raise RuntimeError("LIVE-07 protocol error: Expected records not found during reconciliation.")

        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-07",
            name="Ambiguous Outcome Comprehensive Protocol",
            success=True,
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
        ))

    def _run_live_08(self) -> None:
        """LIVE-08: Pre-Deletion Ownership Verification, Parameterized Conditional Deletion & Three-State Audit."""
        start = time.perf_counter()
        req_start = self.http_requests_total
        assert self.manifest is not None

        # ---------------------------------------------------------------------
        # Step 8a: Pre-Deletion Ownership Verification (5 calls)
        # ---------------------------------------------------------------------
        verified_entities: list[dict[str, Any]] = []

        for ent in self.manifest.registered_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]
            expected = ent["expected_attributes"]

            res = self._execute(f"SELECT * FROM {tbl} WHERE {pk_col} = ?", [pk_val])
            rows = res.fetchall()

            if not rows:
                # Entity absent (e.g. rolled back article) - safe
                continue

            row = rows[0]

            # Ownership mismatch simulation
            if self.simulate_ownership_mismatch and tbl == "processed_articles":
                self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                raise PermissionError("OWNERSHIP_MISMATCH_HALT: Row attributes do not match expected synthetic markers.")

            # Verify synthetic markers
            if tbl == "processed_articles":
                if row.get("source") != expected.get("source") or not str(row.get("title", "")).startswith(expected.get("title_prefix", "")):
                    self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                    raise PermissionError(f"OWNERSHIP_MISMATCH_HALT on {tbl}.{pk_val}")
            elif tbl == "stories":
                if row.get("category") != expected.get("category") or not str(row.get("headline_tr", "")).startswith(expected.get("headline_prefix", "")):
                    self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                    raise PermissionError(f"OWNERSHIP_MISMATCH_HALT on {tbl}.{pk_val}")
            elif tbl == "run_locks":
                wid = str(row.get("worker_id", ""))
                if not (wid.startswith(f"runner_alpha_{self.run_id}") or wid.startswith(f"runner_beta_{self.run_id}")):
                    self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                    raise PermissionError(f"OWNERSHIP_MISMATCH_HALT on {tbl}.{pk_val}")
            elif tbl == "durable_recovery_state":
                if not str(row.get("worker_id", "")).startswith(f"synth_worker_{self.run_id}"):
                    self.audit_state = AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                    raise PermissionError(f"OWNERSHIP_MISMATCH_HALT on {tbl}.{pk_val}")

            verified_entities.append(ent)

        # ---------------------------------------------------------------------
        # Step 8b: Parameterized Conditional Deletion (PK + Attributes) (4-5 calls)
        # ---------------------------------------------------------------------
        # Sort in cleanup dependency order (higher order first)
        verified_entities.sort(key=lambda e: e.get("cleanup_dependency_order", 1), reverse=True)

        for ent in verified_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]
            expected = ent["expected_attributes"]

            if tbl == "processed_articles":
                del_sql = "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title LIKE ?"
                del_params = [pk_val, expected["source"], f"{expected['title_prefix']}%"]
            elif tbl == "stories":
                del_sql = "DELETE FROM stories WHERE story_id = ? AND category = ? AND headline_tr LIKE ?"
                del_params = [pk_val, expected["category"], f"{expected['headline_prefix']}%"]
            elif tbl == "run_locks":
                del_sql = "DELETE FROM run_locks WHERE lock_key = ? AND (worker_id = ? OR worker_id = ?)"
                del_params = [pk_val, f"runner_alpha_{self.run_id}", f"runner_beta_{self.run_id}"]
            elif tbl == "durable_recovery_state":
                del_sql = "DELETE FROM durable_recovery_state WHERE state_key = ? AND stage = ? AND worker_id = ?"
                del_params = [pk_val, f"synth_stage_{self.run_id}", f"synth_worker_{self.run_id}"]
            else:
                continue

            del_res = self._execute(del_sql, del_params)
            if del_res.affected_row_count != 1:
                self.audit_state = AuditOutcomeState.RESIDUAL
                raise RuntimeError(
                    f"Unexpected affected row count during conditional deletion of {tbl}.{pk_val}: "
                    f"expected 1, got {del_res.affected_row_count}"
                )

        # ---------------------------------------------------------------------
        # Step 8c: Independent Three-State Verification Audit (5 calls)
        # ---------------------------------------------------------------------
        if self.simulate_audit_failure:
            self.audit_state = AuditOutcomeState.UNKNOWN
            raise TursoClientError("Simulated transport failure during audit verification query")

        any_residual = False
        for ent in self.manifest.registered_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]
            audit_res = self._execute(f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE {pk_col} = ?", [pk_val])
            rows = audit_res.fetchall()
            count = int(rows[0].get("cnt", 0)) if rows else 0
            if count > 0:
                any_residual = True

        if any_residual:
            self.audit_state = AuditOutcomeState.RESIDUAL
        else:
            self.audit_state = AuditOutcomeState.CLEAN

        duration = time.perf_counter() - start
        self.stages.append(StageResult(
            stage_id="LIVE-08",
            name="Pre-Deletion Ownership Verification, Conditional Deletions & Three-State Audit",
            success=(self.audit_state == AuditOutcomeState.CLEAN),
            http_requests_used=self.http_requests_total - req_start,
            duration_seconds=duration,
            details={"audit_state": self.audit_state.value, "entities_deleted": len(verified_entities)},
        ))


# =============================================================================
# Offline Recover-Manifest Simulation
# =============================================================================

class ManifestRecoveryReport(BaseModel):
    manifest_path: str
    run_id: str
    valid_schema: bool
    entities_count: int
    audit_state: AuditOutcomeState
    manifest_preserved: bool
    summary_message: str


def recover_manifest_offline(manifest_path: Path) -> ManifestRecoveryReport:
    """Offline validation and simulated cleanup audit for a persisted manifest file.

    Strictly validates schema, allowlisted tables, and synthetic key derivation.
    Demonstrates offline cleanup simulation and manifest preservation on failure.
    Does NOT connect to cloud or perform unauthorized deletion.
    """
    manifest = PerRunOwnershipManifest.load_and_validate(manifest_path)

    # In offline simulation, verify against an isolated SQLite schema v6 fixture
    conn = sqlite3.connect(":memory:")
    for stmt in get_all_init_statements():
        conn.execute(stmt)
    conn.commit()

    # Simulate checking records
    any_residual = False
    for ent in manifest.registered_entities:
        tbl = ent["table"]
        pk_col = ent["pk_column"]
        pk_val = ent["pk_value"]
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {pk_col} = ?", [pk_val])
        row = cur.fetchone()
        if row and row[0] > 0:
            any_residual = True

    audit_state = AuditOutcomeState.RESIDUAL if any_residual else AuditOutcomeState.CLEAN
    manifest_unlinked = manifest.delete_if_clean(manifest_path, audit_state)

    return ManifestRecoveryReport(
        manifest_path=str(manifest_path),
        run_id=manifest.run_id,
        valid_schema=True,
        entities_count=len(manifest.registered_entities),
        audit_state=audit_state,
        manifest_preserved=not manifest_unlinked,
        summary_message=f"Offline manifest validation succeeded for run {manifest.run_id} ({audit_state.value}).",
    )


# =============================================================================
# CLI Entrypoint (Dry-Run Only)
# =============================================================================

def uuid_hex(length: int = 12) -> str:
    """Generate a clean lowercase hexadecimal random string."""
    return uuid.uuid4().hex[:length]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m efso_radar.staging.runner",
        description="Efso Radar Hermetic Staging Runner Foundation (Offline Simulation Only).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Execute hermetic simulation against isolated SQLite fixture and mock transport (default).",
    )
    parser.add_argument(
        "--recover-manifest",
        type=str,
        default=None,
        metavar="PATH",
        help="Offline validation and simulated audit of a persisted manifest file.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args_list = argv if argv is not None else sys.argv[1:]

    # Fail closed immediately if any live execution flag is provided
    for arg in args_list:
        clean_arg = arg.strip().lower()
        if clean_arg in DISALLOWED_LIVE_FLAGS or clean_arg.startswith("--live") or clean_arg.startswith("--prod"):
            sys.stderr.write(
                "CRITICAL SECURITY VIOLATION: Live execution is strictly prohibited in Phase 5B.3G-B.\n"
                "This milestone supports hermetic offline simulation only (--dry-run).\n"
            )
            return 2

    parser = build_parser()
    args, unknown = parser.parse_known_args(args_list)

    if unknown:
        sys.stderr.write(f"Unknown arguments rejected: {unknown}\n")
        return 2

    # Mode 1: Offline manifest recovery
    if args.recover_manifest:
        try:
            m_path = Path(args.recover_manifest)
            rep = recover_manifest_offline(m_path)
            sys.stdout.write(f"{rep.summary_message}\n")
            return 0 if rep.audit_state == AuditOutcomeState.CLEAN else 1
        except Exception as exc:
            sys.stderr.write(f"Manifest recovery validation failed: {type(exc).__name__}: {exc}\n")
            return 1

    # Mode 2: Hermetic Dry-Run Simulation
    try:
        # Create an isolated temporary SQLite database initialized with Schema v6
        conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            conn.execute(stmt)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-10-25T15:00:00Z', 'Schema v6')"
        )
        conn.commit()

        raw_transport = HermeticStagingTransport(conn)
        budget_transport = BudgetTrackingTransport(raw_transport)
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock-hermetic-staging-token",
            transport=budget_transport,
            timeout_seconds=PER_OPERATION_TIMEOUT_SECONDS,
        )

        runner = StagingSessionRunner(
            client=client,
            destination_url=EXACT_STAGING_DESTINATION,
        )

        report = runner.run()
        sys.stdout.write(f"{report.summary()}\n")
        return 0 if report.status == SessionStatus.PASSED else 1

    except Exception as exc:
        sys.stderr.write(f"Hermetic simulation aborted: {type(exc).__name__}: {exc}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
