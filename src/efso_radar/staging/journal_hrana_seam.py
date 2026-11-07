"""Offline staging ownership journal ↔ mock Hrana transport integration seam for ERD-017 (Option B + Option C).

STRICTLY OFFLINE INTEGRATION SEAM FOR HERMETIC STAGING TESTS.
- Bridges StagingOwnershipJournal ownership invariants and HermeticStagingTransport mock wire protocol.
- Enforces atomic single-transaction coupling (Option B+C) over Hrana interactive pipeline batons.
- Guarantees fail-closed cleanup with in-transaction audit verification of registered entities.
- Enforces strict hermetic boundaries (uninitialized fresh TursoHTTPClient, transport identity verification).
- Enforces authoritative unresolved-run admission within the registration transaction.
- Strictly in-memory (:memory:) or temporary file SQLite fixtures.
- Zero network I/O, zero Turso/cloud connections, zero credentials or tokens.
- Absolute $0.00 / 0 TRY spending ceiling.
- Real provider durability and cross-runner recovery across ephemeral VM teardowns remain explicitly UNPROVEN / BLOCKED.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Optional, Sequence

import httpx

from efso_radar.repositories.turso_client import (
    TursoAmbiguousTransactionError,
    TursoClientError,
    TursoHTTPClient,
)
from efso_radar.staging.journal import (
    HEX_SHA256_REGEX,
    SCHEMA_V7_STAGING_JOURNAL_DDL,
    _FORBIDDEN_SQL_KEYWORDS,
    _SAFE_INSERT_PATTERN,
    ACTIVE_JOURNAL_STATUSES,
    ActiveWorkloadCollisionError,
    AmbiguousCommitError,
    AmbiguousCommitOutcome,
    FencingConflictError,
    HaltMarkerPersistenceError,
    InvalidJournalTransitionError,
    InvalidRunIdError,
    JournalError,
    JournalExecutionPattern,
    JournalHaltedError,
    JournalStatus,
    ManifestTamperedError,
    OperatorReconciliationProposal,
    ReadProvenance,
    StageMutationCommand,
    StagingJournalEntry,
    StaleOwnerError,
    TargetInventoryViolationError,
    TERMINAL_JOURNAL_STATUSES,
    UNRESOLVED_JOURNAL_STATUSES,
    UnauthorizedTakeoverError,
    _halt_marker_path,
    apply_candidate_schema_v7,
    compute_canonical_inventory_fingerprint,
    validate_and_normalize_mutation,
    validate_status_transition,
)
from efso_radar.staging.offline_recovery_evidence import (
    CommitState,
    CrossRunnerJournalEntry,
    CrossRunnerJournalStatus,
    CrossRunnerReview,
    EntityObservation,
    HermeticSnapshot,
    QueryState,
    ReviewDecision,
    review_cross_runner_recovery_evidence,
)
from efso_radar.staging.runner import (
    ALLOWED_TABLE_PK_MAP,
    RUN_ID_REGEX,
    AuditOutcomeState,
    BudgetTrackingTransport,
    HermeticStagingTransport,
    PerRunOwnershipManifest,
    RegisteredEntity,
    create_default_manifest_entities,
)


class CleanupVerificationError(JournalError, RuntimeError):
    """In-transaction cleanup verification failed; partial cleanup, foreign entity, or residual row detected."""


class OwnershipMismatchError(JournalError, PermissionError):
    """Entity attributes do not match registered ownership markers."""


def _assert_fixture_isolation(conn: sqlite3.Connection) -> None:
    """Enforce that the connection is strictly in-memory or points to an OS temp file."""
    cursor = conn.execute("PRAGMA database_list")
    rows = cursor.fetchall()
    temp_dir = Path(tempfile.gettempdir()).resolve()
    for row in rows:
        db_file = row[2] if len(row) > 2 else ""
        if db_file and db_file != "" and not str(db_file).startswith(":memory:"):
            resolved_file = Path(db_file).resolve()
            try:
                resolved_file.relative_to(temp_dir)
            except ValueError:
                raise PermissionError(
                    f"Hermetic fixture security violation: database connection points to '{resolved_file}', "
                    f"which is outside the OS temporary directory '{temp_dir}'."
                )


def _serialize_hrana_arg(val: Any) -> dict[str, Any]:
    """Serialize a Python value into Hrana v2 Value format."""
    if val is None:
        return {"type": "null"}
    elif isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    elif isinstance(val, float):
        return {"type": "float", "value": val}
    elif isinstance(val, str):
        return {"type": "text", "value": val}
    elif isinstance(val, (bytes, bytearray)):
        import base64
        return {"type": "blob", "base64": base64.b64encode(val).decode("ascii")}
    raise TypeError(f"Unsupported Hrana argument type: {type(val)}")


def _deserialize_hrana_value(val: dict[str, Any]) -> Any:
    """Deserialize a Hrana v2 Value object into a Python primitive."""
    vtype = val.get("type")
    if vtype == "null":
        return None
    elif vtype == "integer":
        return int(val.get("value", 0))
    elif vtype == "float":
        return float(val.get("value", 0.0))
    elif vtype == "text":
        return str(val.get("value", ""))
    elif vtype == "blob":
        import base64
        return base64.b64decode(val.get("base64", ""))
    return val.get("value")


def build_live02_synthetic_mutations(run_id: str) -> list[StageMutationCommand]:
    """Construct canonical LIVE-02 synthetic mutation commands for processed_articles and stories."""
    entities = create_default_manifest_entities(run_id)
    art_ent = next(e for e in entities if e.table == "processed_articles" and e.pk_value == f"synth_art_{run_id}_01")
    story_ent = next(e for e in entities if e.table == "stories" and e.pk_value == f"synth_story_{run_id}_01")

    art_cmd = StageMutationCommand(
        table="processed_articles",
        pk_column="article_id",
        pk_value=art_ent.pk_value,
        columns=["article_id", "canonical_url", "title", "source", "published_at", "processed_at"],
        values=[
            art_ent.pk_value,
            f"https://example.com/synth/{run_id}/01",
            art_ent.expected_attributes["title"],
            art_ent.expected_attributes["source"],
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
        ],
    )
    story_cmd = StageMutationCommand(
        table="stories",
        pk_column="story_id",
        pk_value=story_ent.pk_value,
        columns=[
            "story_id",
            "headline_tr",
            "category",
            "first_seen_at",
            "last_updated_at",
            "article_ids_json",
            "update_count",
            "is_active",
        ],
        values=[
            story_ent.pk_value,
            story_ent.expected_attributes["headline_tr"],
            story_ent.expected_attributes["category"],
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
            json.dumps([art_ent.pk_value]),
            1,
            1,
        ],
    )
    return [art_cmd, story_cmd]


class StagingJournalHranaSeam:
    """Strictly offline integration seam between StagingOwnershipJournal and HermeticStagingTransport under $0.00 / 0 TRY spending ceiling."""

    def __init__(
        self,
        *,
        client: TursoHTTPClient,
        owner_id: str,
        marker_dir: Optional[Path] = None,
        workload_key: str = "staging_controlled_run",
    ) -> None:
        if type(client) is not TursoHTTPClient:
            raise PermissionError(
                f"Hermetic security boundary violation: client must be TursoHTTPClient, got {type(client).__name__}."
            )

        if getattr(client, "read_only", False):
            raise PermissionError(
                "Hermetic security boundary violation: StagingJournalHranaSeam executes mutating staging operations "
                "and strictly rejects read_only=True clients."
            )

        # Require a fresh TursoHTTPClient instance with no previously initialized or cached internal HTTP client
        if getattr(client, "_client", None) is not None:
            raise PermissionError(
                "Hermetic security boundary violation: client has an already initialized internal HTTP client (_client is not None). "
                "StagingJournalHranaSeam requires a fresh, uninitialized TursoHTTPClient to prevent stale cached transports."
            )

        transport = getattr(client, "transport", None)
        if transport is None:
            raise PermissionError("Hermetic security boundary violation: transport cannot be None.")

        inner = transport
        if type(inner) is BudgetTrackingTransport:
            inner = inner.inner
        if type(inner).__name__ == "HranaOutboundSecurityBoundary":
            inner = getattr(inner, "_inner", None)
        if type(inner) is not HermeticStagingTransport:
            raise PermissionError(
                f"Hermetic security boundary violation: expected HermeticStagingTransport, got {type(inner).__name__}."
            )

        conn = getattr(inner, "conn", None)
        if not isinstance(conn, sqlite3.Connection):
            raise PermissionError(
                "Hermetic security boundary violation: HermeticStagingTransport must be backed by sqlite3.Connection."
            )

        _assert_fixture_isolation(conn)

        if not owner_id or not isinstance(owner_id, str) or not owner_id.strip():
            raise ValueError("owner_id must be a non-empty string.")

        if marker_dir is not None and not marker_dir.is_dir():
            raise ValueError(f"marker_dir '{marker_dir}' is not an existing directory.")

        self._client = client
        self._pinned_transport = transport
        self._pinned_inner_transport = inner
        self._transport = inner
        self._conn = conn
        self._owner_id = owner_id.strip()
        self._marker_dir = marker_dir
        self._workload_key = workload_key.strip()
        self._halted_runs: set[str] = set()

        self._verify_journal_table_exists()

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def marker_dir(self) -> Optional[Path]:
        return self._marker_dir

    def is_halted(self, run_id: str) -> bool:
        """Check if run is locally halted (LOCAL_OUTCOME_UNKNOWN)."""
        if run_id in self._halted_runs:
            return True
        if self._marker_dir is None:
            return False
        try:
            marker = _halt_marker_path(run_id, self._marker_dir)
        except HaltMarkerPersistenceError:
            return False
        if marker.is_file():
            self._halted_runs.add(run_id)
            return True
        return False

    def _verify_journal_table_exists(self) -> None:
        """Verify candidate schema v7 staging_run_journal exists."""
        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='staging_run_journal'"
        )
        if not cursor.fetchone():
            raise RuntimeError(
                "Candidate Schema v7 table 'staging_run_journal' does not exist in the test fixture. "
                "Invoke apply_candidate_schema_v7(conn) on the hermetic fixture before initializing the seam."
            )

    def _latch_local_halt(self, run_id: str, reason: str) -> None:
        """Latch local in-memory and on-disk halt marker."""
        self._halted_runs.add(run_id)
        if self._marker_dir is None:
            raise HaltMarkerPersistenceError(
                f"Cannot persist halt marker for run '{run_id}': marker_dir is not configured."
            )
        marker_path = _halt_marker_path(run_id, self._marker_dir)
        payload = {
            "run_id": run_id,
            "owner_id": self._owner_id,
            "halted_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
            "local_status": "LOCAL_OUTCOME_UNKNOWN",
            "durability_boundary": "UNPROVEN_OFFLINE_LOCAL_MARKER",
        }
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(str(marker_path), flags, 0o600)
        except FileExistsError:
            raise HaltMarkerPersistenceError(
                f"Halt marker '{marker_path}' already exists for run '{run_id}'."
            )
        except OSError as exc:
            raise HaltMarkerPersistenceError(
                f"Failed to open halt marker '{marker_path}': {exc}"
            ) from exc

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(json.dumps(payload, indent=2))
                f.flush()
                os.fsync(f.fileno())
        except OSError as exc:
            raise HaltMarkerPersistenceError(
                f"Failed to write halt marker '{marker_path}': {exc}"
            ) from exc

    def inspect_run(self, run_id: str) -> Optional[StagingJournalEntry]:
        """Fetch current journal record for run_id."""
        if not RUN_ID_REGEX.match(run_id):
            raise InvalidRunIdError(f"run_id '{run_id}' is invalid.")

        cursor = self._conn.execute(
            """
            SELECT run_id, owner_id, fencing_token, stage, status,
                   manifest_seal_hex, target_inventory_json, created_at, updated_at, last_error
            FROM staging_run_journal
            WHERE run_id = ?
            """,
            [run_id],
        )
        row = cursor.fetchone()
        if not row:
            return None

        cols = [desc[0] for desc in cursor.description]
        row_dict = dict(zip(cols, row))
        return StagingJournalEntry(
            run_id=row_dict["run_id"],
            owner_id=row_dict["owner_id"],
            fencing_token=row_dict["fencing_token"],
            stage=row_dict["stage"],
            status=JournalStatus(row_dict["status"]),
            manifest_seal_hex=row_dict["manifest_seal_hex"],
            target_inventory_json=row_dict["target_inventory_json"],
            created_at=row_dict["created_at"],
            updated_at=row_dict["updated_at"],
            last_error=row_dict.get("last_error"),
            read_provenance=ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
        )

    def list_unresolved_runs(self) -> list[StagingJournalEntry]:
        """Return all runs in active, mutating, or UNKNOWN statuses."""
        cursor = self._conn.execute(
            """
            SELECT run_id, owner_id, fencing_token, stage, status,
                   manifest_seal_hex, target_inventory_json, created_at, updated_at, last_error
            FROM staging_run_journal
            WHERE status NOT IN ('CLEANED_UP', 'ABORTED')
            ORDER BY created_at ASC
            """
        )
        runs = []
        cols = [desc[0] for desc in cursor.description]
        for row in cursor.fetchall():
            row_dict = dict(zip(cols, row))
            runs.append(
                StagingJournalEntry(
                    run_id=row_dict["run_id"],
                    owner_id=row_dict["owner_id"],
                    fencing_token=row_dict["fencing_token"],
                    stage=row_dict["stage"],
                    status=JournalStatus(row_dict["status"]),
                    manifest_seal_hex=row_dict["manifest_seal_hex"],
                    target_inventory_json=row_dict["target_inventory_json"],
                    created_at=row_dict["created_at"],
                    updated_at=row_dict["updated_at"],
                    last_error=row_dict.get("last_error"),
                    read_provenance=ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
                )
            )
        return runs

    def propose_operator_reconciliation(
        self,
        *,
        run_id: str,
        reason: str,
        proposed_action: str = "MANUAL_CLEANUP_AFTER_PO_GO",
    ) -> OperatorReconciliationProposal:
        """Generate a strictly read-only reconciliation proposal for offline inspection."""
        entry = self.inspect_run(run_id)
        if not entry:
            raise ValueError(f"Run '{run_id}' not found.")
        return OperatorReconciliationProposal(
            run_id=run_id,
            operator_id=self._owner_id,
            proposed_action=proposed_action,
            reason=reason,
        )

    def _validate_hermetic_transport_identity(self) -> None:
        """Enforce strict identity of pinned hermetic transport before any client initialization or dispatch."""
        if getattr(self._client, "read_only", False):
            raise PermissionError("Hermetic security boundary violation: mutating pipeline on read_only client.")

        current_transport = getattr(self._client, "transport", None)
        if current_transport is None:
            raise PermissionError("Hermetic security boundary violation: client transport is None.")

        # 1. Outer transport must be the exact pinned object
        if current_transport is not self._pinned_transport:
            raise PermissionError(
                "Hermetic security boundary violation: client transport identity mismatch. "
                "client.transport was modified or swapped after StagingJournalHranaSeam initialization."
            )

        # 2. Inner transport must be the exact pinned HermeticStagingTransport
        inner = current_transport
        if type(inner) is BudgetTrackingTransport:
            inner = inner.inner
        if type(inner).__name__ == "HranaOutboundSecurityBoundary":
            inner = getattr(inner, "_inner", None)
        if inner is not self._pinned_inner_transport or type(inner) is not HermeticStagingTransport:
            raise PermissionError(
                "Hermetic security boundary violation: inner transport identity mismatch. "
                "Expected pinned HermeticStagingTransport."
            )

        # 3. Connection isolation
        conn = getattr(inner, "conn", None)
        if not isinstance(conn, sqlite3.Connection):
            raise PermissionError(
                "Hermetic security boundary violation: HermeticStagingTransport must be backed by sqlite3.Connection."
            )
        _assert_fixture_isolation(conn)

        # 4. If internal client was already initialized, cached transport must match pinned transport
        cached_client = getattr(self._client, "_client", None)
        if cached_client is not None:
            cached_transport = getattr(cached_client, "_transport", None)
            if cached_transport is not None and cached_transport is not self._pinned_transport:
                raise PermissionError("Hermetic security boundary violation: cached transport identity mismatch.")

    def _get_guarded_http_client(self) -> httpx.Client:
        """Verify transport identity before initializing or using the internal httpx client."""
        self._validate_hermetic_transport_identity()
        http_client = self._client._get_client()
        actual_transport = getattr(http_client, "_transport", None)
        if actual_transport is not None and actual_transport is not self._pinned_transport:
            raise PermissionError("Hermetic security boundary violation: underlying HTTP client transport mismatch.")
        return http_client

    def _execute_guarded_client_query(self, sql: str, params: Optional[list[Any]] = None) -> Any:
        """Execute a single query via client while enforcing transport identity guards beforehand."""
        self._validate_hermetic_transport_identity()
        _ = self._get_guarded_http_client()
        return self._client.execute(sql, params)

    def _post_pipeline(self, requests_payload: list[dict[str, Any]], baton: Optional[str] = None) -> dict[str, Any]:
        """Internal helper to dispatch Hrana /v2/pipeline requests enforcing strict transport identity."""
        http_client = self._get_guarded_http_client()

        body: dict[str, Any] = {"requests": requests_payload}
        if baton is not None:
            body["baton"] = baton
        resp = http_client.post(self._client.pipeline_url, json=body)
        if resp.status_code != 200:
            if resp.status_code in (500, 502, 503, 504):
                raise TursoAmbiguousTransactionError(f"HTTP {resp.status_code} during pipeline request.")
            raise TursoClientError(f"HTTP {resp.status_code} error during pipeline request.")
        try:
            data = resp.json()
        except Exception as exc:
            raise TursoClientError(f"Invalid JSON in pipeline response: {exc}") from exc
        if not isinstance(data, dict):
            raise TursoClientError(f"Malformed JSON pipeline response: expected dict, got {type(data).__name__}")
        return data

    def _execute_hrana_transaction(
        self,
        statements: list[tuple[str, list[Any]]],
        *,
        required_affected_counts: list[Optional[int]],
        run_id: str,
    ) -> list[dict[str, Any]]:
        """Execute a multi-statement transaction with baton over Hrana v2 pipeline."""
        requests: list[dict[str, Any]] = [
            {"type": "execute", "stmt": {"sql": "BEGIN"}}
        ]
        for sql, params in statements:
            raw_args = [_serialize_hrana_arg(p) for p in params]
            requests.append({"type": "execute", "stmt": {"sql": sql, "args": raw_args}})

        try:
            pipeline_res = self._post_pipeline(requests)
        except PermissionError:
            raise
        except (TursoAmbiguousTransactionError, TursoClientError, httpx.HTTPError, Exception) as exc:
            self._latch_local_halt(run_id, f"Initial pipeline request failed in Hrana transaction: {exc}")
            if isinstance(exc, TursoAmbiguousTransactionError):
                raise
            raise TursoClientError(f"Initial pipeline request failed in Hrana transaction: {exc}") from exc

        if not isinstance(pipeline_res, dict):
            self._latch_local_halt(run_id, f"Malformed Hrana pipeline response: expected dict, got {type(pipeline_res).__name__}")
            raise TursoClientError(f"Malformed Hrana pipeline response: expected dict, got {type(pipeline_res).__name__}")

        baton = pipeline_res.get("baton")
        results = pipeline_res.get("results")

        # 1. Validate session baton
        if not baton or not isinstance(baton, str) or not baton.strip():
            self._latch_local_halt(run_id, "Hrana transaction response missing valid session baton")
            raise TursoClientError("Hrana transaction response missing valid session baton.")

        # 2. Validate exact result cardinality
        expected_results_count = 1 + len(statements)
        if not isinstance(results, list) or len(results) != expected_results_count:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(
                run_id,
                f"Malformed or truncated Hrana results: expected {expected_results_count}, "
                f"got {len(results) if isinstance(results, list) else type(results)}",
            )
            raise TursoClientError(
                f"Malformed or truncated Hrana transaction results: expected {expected_results_count}, "
                f"got {len(results) if isinstance(results, list) else type(results)}. "
                "Transaction rolled back; run locally HALTED."
            )

        # 3. Check BEGIN outcome
        begin_res = results[0]
        if not isinstance(begin_res, dict) or begin_res.get("type") != "ok":
            err_msg = begin_res.get("error", {}).get("message", "BEGIN not ok") if isinstance(begin_res, dict) else "malformed begin result"
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(run_id, f"Failed to start Hrana transaction: {err_msg}")
            raise TursoClientError(f"Failed to start Hrana transaction: {err_msg}")

        # 4. Check each statement outcome and row count
        stmt_results = results[1:]
        for idx, s_res in enumerate(stmt_results):
            if not isinstance(s_res, dict) or s_res.get("type") != "ok":
                err_dict = s_res.get("error", {}) if isinstance(s_res, dict) and isinstance(s_res.get("error"), dict) else {}
                err_msg = err_dict.get("message", f"Statement {idx} result type '{s_res.get('type') if isinstance(s_res, dict) else type(s_res)}'")
                if baton:
                    try:
                        self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
                    except Exception:
                        pass
                self._latch_local_halt(run_id, f"Statement {idx} failed in Hrana transaction: {err_msg}")
                raise TursoClientError(f"Statement {idx} failed in Hrana transaction: {err_msg}")

            resp_obj = s_res.get("response") if isinstance(s_res, dict) else None
            exec_res = resp_obj.get("result") if isinstance(resp_obj, dict) else None
            actual_affected = exec_res.get("affected_row_count", 0) if isinstance(exec_res, dict) else None
            req_affected = required_affected_counts[idx]

            if req_affected is not None and (actual_affected is None or actual_affected != req_affected):
                if baton:
                    try:
                        self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
                    except Exception:
                        pass
                raise FencingConflictError(
                    f"Hrana statement {idx} affected {actual_affected} rows (expected {req_affected}). "
                    "Transaction rolled back fail-closed."
                )

        # 5. Commit transaction
        try:
            commit_res = self._post_pipeline(
                [{"type": "execute", "stmt": {"sql": "COMMIT"}}],
                baton=baton,
            )
        except PermissionError:
            raise
        except (TursoAmbiguousTransactionError, TursoClientError, httpx.HTTPError, Exception) as exc:
            self._latch_local_halt(run_id, f"Ambiguous COMMIT on Hrana transaction: {exc}")
            raise AmbiguousCommitError(
                f"Ambiguous COMMIT on Hrana transaction for run '{run_id}': {exc}. "
                "Local status latched to LOCAL_OUTCOME_UNKNOWN / HALT."
            ) from exc

        # 6. Validate COMMIT acknowledgment
        if not isinstance(commit_res, dict):
            self._latch_local_halt(run_id, f"Malformed COMMIT response: expected dict, got {type(commit_res).__name__}")
            raise AmbiguousCommitError(f"Malformed COMMIT response: expected dict, got {type(commit_res).__name__}. Local status latched to LOCAL_OUTCOME_UNKNOWN / HALT.")

        c_results = commit_res.get("results")
        if (
            not isinstance(c_results, list)
            or len(c_results) != 1
            or not isinstance(c_results[0], dict)
            or c_results[0].get("type") != "ok"
        ):
            err_msg = (
                c_results[0].get("error", {}).get("message", "COMMIT not ok")
                if (isinstance(c_results, list) and c_results and isinstance(c_results[0], dict) and isinstance(c_results[0].get("error"), dict))
                else f"Malformed COMMIT acknowledgment: {commit_res}"
            )
            self._latch_local_halt(run_id, f"Hrana COMMIT outcome uncertain: {err_msg}")
            raise AmbiguousCommitError(f"Hrana COMMIT failed or unverified: {err_msg}. Local status latched to LOCAL_OUTCOME_UNKNOWN / HALT.")

        return stmt_results

    def register_intent(
        self,
        *,
        run_id: str,
        stage: str = "LIVE_02",
        target_inventory: Sequence[RegisteredEntity],
        manifest_seal_hex: Optional[str] = None,
        pattern: JournalExecutionPattern = JournalExecutionPattern.OPTION_B_C_ATOMIC,
        simulate_ambiguous_commit: Optional[AmbiguousCommitOutcome] = None,
        lease_duration_seconds: int = 900,
    ) -> StagingJournalEntry:
        """Register staging run intent atomically in staging_run_journal with authoritative admission checks."""
        if not RUN_ID_REGEX.match(run_id):
            raise InvalidRunIdError(f"run_id '{run_id}' does not match canonical profile.")

        if self.is_halted(run_id):
            raise JournalHaltedError(f"Run '{run_id}' is locally HALTED. Registration rejected fail-closed.")

        now_iso = datetime.now(timezone.utc).isoformat()
        lease_expires_iso = (datetime.now(timezone.utc) + timedelta(seconds=lease_duration_seconds)).isoformat()

        manifest_payload = {
            "version": 1,
            "run_id": run_id,
            "created_at": now_iso,
            "registered_entities": [e.model_dump() if hasattr(e, "model_dump") else e for e in target_inventory],
        }
        manifest = PerRunOwnershipManifest.validate_payload(manifest_payload)
        expected_seal_hex = compute_canonical_inventory_fingerprint(
            run_id=run_id,
            entities=manifest.registered_entities,
        )
        if manifest_seal_hex is not None:
            if not HEX_SHA256_REGEX.match(manifest_seal_hex):
                raise ManifestTamperedError(f"manifest_seal_hex '{manifest_seal_hex}' is not a valid 64-char lowercase hex SHA-256.")
            if manifest_seal_hex != expected_seal_hex:
                raise ManifestTamperedError(
                    f"Provided manifest_seal_hex '{manifest_seal_hex}' does not match authenticated "
                    f"canonical inventory fingerprint '{expected_seal_hex}'."
                )
        seal_to_store = expected_seal_hex

        inventory_json = json.dumps(
            [e.model_dump() for e in manifest.registered_entities],
            sort_keys=True,
        )

        initial_fencing_token = 1
        initial_status = (
            JournalStatus.INTENT_PENDING
            if pattern == JournalExecutionPattern.OPTION_B_SEPARATED
            else JournalStatus.ACKED
        )

        cur_tbl = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='run_locks'"
        )
        has_run_locks = bool(cur_tbl.fetchone())

        statements: list[tuple[str, list[Any]]] = []
        required_counts: list[Optional[int]] = []

        if has_run_locks:
            statements.append((
                """
                INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at)
                SELECT ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM staging_run_journal
                    WHERE status NOT IN ('CLEANED_UP', 'ABORTED')
                )
                ON CONFLICT(lock_key) DO UPDATE SET
                    worker_id = excluded.worker_id,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at,
                    heartbeat_at = excluded.heartbeat_at
                WHERE (run_locks.expires_at < ? OR run_locks.worker_id = ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM staging_run_journal
                      WHERE status NOT IN ('CLEANED_UP', 'ABORTED')
                  )
                """,
                [self._workload_key, self._owner_id, now_iso, lease_expires_iso, now_iso, now_iso, self._owner_id],
            ))
            required_counts.append(1)

        statements.append((
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM staging_run_journal
                WHERE status NOT IN ('CLEANED_UP', 'ABORTED')
            )
            AND NOT EXISTS (
                SELECT 1 FROM staging_run_journal
                WHERE run_id = ?
            )
            """,
            [
                run_id,
                self._owner_id,
                initial_fencing_token,
                stage,
                initial_status.value,
                seal_to_store,
                inventory_json,
                now_iso,
                now_iso,
                run_id,
            ],
        ))
        required_counts.append(1)

        if pattern == JournalExecutionPattern.OPTION_B_SEPARATED:
            statements.append((
                """
                UPDATE staging_run_journal
                SET status = ?, updated_at = ?
                WHERE run_id = ? AND status = ?
                """,
                [
                    JournalStatus.ACKED.value,
                    now_iso,
                    run_id,
                    JournalStatus.INTENT_PENDING.value,
                ],
            ))
            required_counts.append(1)
            final_status = JournalStatus.ACKED
        else:
            final_status = initial_status

        if simulate_ambiguous_commit == AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST:
            try:
                self._execute_hrana_transaction(
                    statements,
                    required_affected_counts=required_counts,
                    run_id=run_id,
                )
            except FencingConflictError:
                self._diagnose_registration_failure(run_id)
            try:
                self._latch_local_halt(run_id, "Ambiguous intent COMMIT (COMMITTED_BUT_ACK_LOST)")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Simulated network drop during intent COMMIT for run '{run_id}'. "
                    f"Transaction committed on provider (COMMITTED_BUT_ACK_LOST), but local halt marker persistence failed: {marker_exc}. "
                    "Client outcome is LOCAL_OUTCOME_UNKNOWN. Run is in-memory HALTED."
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Simulated network drop during intent COMMIT for run '{run_id}'. "
                "Transaction committed on provider (COMMITTED_BUT_ACK_LOST), but client outcome is LOCAL_OUTCOME_UNKNOWN. "
                "Run is locally HALTED. Automated retries or mutations are prohibited."
            )
        elif simulate_ambiguous_commit == AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST:
            try:
                self._latch_local_halt(run_id, "Ambiguous intent COMMIT (ROLLED_BACK_AND_ACK_LOST)")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Simulated network drop during intent COMMIT for run '{run_id}'. "
                    f"Transaction rolled back on provider (ROLLED_BACK_AND_ACK_LOST), but local halt marker persistence failed: {marker_exc}."
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Simulated network drop during intent COMMIT for run '{run_id}'. "
                "Transaction rolled back on provider (ROLLED_BACK_AND_ACK_LOST), client outcome is LOCAL_OUTCOME_UNKNOWN. "
                "Run is locally HALTED. Zero rows exist on provider. Automated retries or mutations are prohibited."
            )

        try:
            self._execute_hrana_transaction(
                statements,
                required_affected_counts=required_counts,
                run_id=run_id,
            )
        except FencingConflictError:
            self._diagnose_registration_failure(run_id)

        return StagingJournalEntry(
            run_id=run_id,
            owner_id=self._owner_id,
            fencing_token=initial_fencing_token,
            stage=stage,
            status=final_status,
            manifest_seal_hex=seal_to_store,
            target_inventory_json=inventory_json,
            created_at=now_iso,
            updated_at=now_iso,
            read_provenance=ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
        )

    def _diagnose_registration_failure(self, run_id: str) -> None:
        """Inspect fixture state to raise specific collision error when atomic admission fails."""
        cur_dup = self._conn.execute(
            "SELECT run_id FROM staging_run_journal WHERE run_id = ?",
            [run_id],
        )
        if cur_dup.fetchone():
            raise ActiveWorkloadCollisionError(
                f"Duplicate identical run_id '{run_id}' already registered in journal."
            )

        cur = self._conn.execute(
            "SELECT run_id, stage, status FROM staging_run_journal WHERE status NOT IN ('CLEANED_UP', 'ABORTED')"
        )
        row = cur.fetchone()
        if row:
            raise ActiveWorkloadCollisionError(
                f"Cannot register intent: an unresolved staging run '{row[0]}' exists "
                f"with status '{row[2]}' in stage '{row[1]}'. "
                "Workload is locked until all prior runs reach CLEANED_UP or ABORTED."
            )

        cur_lock = self._conn.execute(
            "SELECT worker_id, expires_at FROM run_locks WHERE lock_key = ?",
            [self._workload_key],
        )
        lock_row = cur_lock.fetchone()
        now_iso = datetime.now(timezone.utc).isoformat()
        if lock_row and lock_row[1] >= now_iso and lock_row[0] != self._owner_id:
            raise ActiveWorkloadCollisionError(
                f"Workload lock '{self._workload_key}' is actively leased to worker '{lock_row[0]}' until '{lock_row[1]}'."
            )

        raise ActiveWorkloadCollisionError(
            f"Registration admission failed inside atomic transaction for run '{run_id}'."
        )

    def execute_atomic_stage_mutation(
        self,
        *,
        run_id: str,
        expected_fencing_token: int,
        stage: str,
        mutations: Optional[list[tuple[str, list[Any]]]] = None,
        mutation_statements: Optional[list[StageMutationCommand | tuple[str, Any]]] = None,
        manifest_seal_hex: Optional[str] = None,
        expected_status: JournalStatus = JournalStatus.ACKED,
        simulate_ambiguous_commit: Optional[AmbiguousCommitOutcome] = None,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute Option B+C single-transaction atomic batching."""
        if self.is_halted(run_id):
            raise JournalHaltedError(f"Run '{run_id}' is locally HALTED. Mutations rejected fail-closed.")

        now_iso = (now or datetime.now(timezone.utc)).isoformat()

        entry = self.inspect_run(run_id)
        if not entry:
            raise FencingConflictError(f"Run '{run_id}' not found in staging_run_journal.")
        if entry.owner_id != self._owner_id:
            raise StaleOwnerError(
                f"Mutation ownership violation: runner '{self._owner_id}' does not match registered owner '{entry.owner_id}' for run '{run_id}'."
            )
        if entry.fencing_token != expected_fencing_token:
            raise FencingConflictError(
                f"Fencing token mismatch for run '{run_id}': expected {expected_fencing_token}, current {entry.fencing_token}."
            )
        if manifest_seal_hex is not None and manifest_seal_hex != entry.manifest_seal_hex:
            raise ManifestTamperedError(
                f"Provided manifest_seal_hex '{manifest_seal_hex}' does not match registered seal '{entry.manifest_seal_hex}'."
            )

        validate_status_transition(entry.status, JournalStatus.MUTATING)

        raw_mutations = mutation_statements if mutation_statements is not None else mutations
        if raw_mutations is None:
            raw_mutations = []

        registered_entities = entry.get_registered_entities()
        validated_sql_list: list[tuple[str, list[Any]]] = []
        for m in raw_mutations:
            sql, params = validate_and_normalize_mutation(m, registered_entities)
            validated_sql_list.append((sql, list(params)))

        new_fencing_token = expected_fencing_token + 1

        statements: list[tuple[str, list[Any]]] = []
        required_counts: list[Optional[int]] = []

        # 1. CAS fence increment with status & active lease predicate
        statements.append((
            """
            UPDATE staging_run_journal
            SET fencing_token = ?,
                stage = ?,
                status = ?,
                updated_at = ?
            WHERE run_id = ?
              AND fencing_token = ?
              AND owner_id = ?
              AND status IN ('ACKED', 'INTENT_RECORDED', 'MUTATING')
              AND (
                  NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='run_locks')
                  OR EXISTS (
                      SELECT 1 FROM run_locks
                      WHERE lock_key = ? AND worker_id = ? AND expires_at > ?
                  )
              )
            """,
            [
                new_fencing_token,
                stage,
                JournalStatus.MUTATING.value,
                now_iso,
                run_id,
                expected_fencing_token,
                self._owner_id,
                self._workload_key,
                self._owner_id,
                now_iso,
            ],
        ))
        required_counts.append(1)

        # 2. Entity mutations
        for sql, params in validated_sql_list:
            statements.append((sql, params))
            required_counts.append(1)

        # 3. Effect confirmation
        statements.append((
            """
            UPDATE staging_run_journal
            SET status = ?,
                updated_at = ?
            WHERE run_id = ?
              AND fencing_token = ?
            """,
            [
                JournalStatus.EFFECT_COMMITTED.value,
                now_iso,
                run_id,
                new_fencing_token,
            ],
        ))
        required_counts.append(1)

        if simulate_ambiguous_commit == AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST:
            self._execute_hrana_transaction(
                statements,
                required_affected_counts=required_counts,
                run_id=run_id,
            )
            try:
                self._latch_local_halt(run_id, "Ambiguous effect COMMIT (COMMITTED_BUT_ACK_LOST)")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Simulated network drop during effect COMMIT for run '{run_id}'. "
                    f"Transaction committed on provider (COMMITTED_BUT_ACK_LOST), but local halt marker persistence failed: {marker_exc}. "
                    "Client outcome is LOCAL_OUTCOME_UNKNOWN. Run is in-memory HALTED."
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Simulated network drop during effect COMMIT for run '{run_id}'. "
                "Transaction committed on provider (COMMITTED_BUT_ACK_LOST), but client outcome is LOCAL_OUTCOME_UNKNOWN. "
                "Run is locally HALTED. Subsequent mutations, retries, or cleanup are prohibited."
            )
        elif simulate_ambiguous_commit == AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST:
            try:
                self._latch_local_halt(run_id, "Ambiguous effect COMMIT (ROLLED_BACK_AND_ACK_LOST)")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Simulated network drop during effect COMMIT for run '{run_id}'. "
                    f"Transaction rolled back on provider (ROLLED_BACK_AND_ACK_LOST), but local halt marker persistence failed: {marker_exc}."
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Simulated network drop during effect COMMIT for run '{run_id}'. "
                "Transaction rolled back on provider (ROLLED_BACK_AND_ACK_LOST), client outcome is LOCAL_OUTCOME_UNKNOWN. "
                "Run is locally HALTED. Provider state is rolled back. Subsequent mutations, retries, or cleanup are prohibited."
            )

        try:
            self._execute_hrana_transaction(
                statements,
                required_affected_counts=required_counts,
                run_id=run_id,
            )
        except TursoClientError as exc:
            if "Statement 0" in str(exc):
                cur = self.inspect_run(run_id)
                if cur and cur.fencing_token != expected_fencing_token:
                    raise FencingConflictError(
                        f"Fencing token mismatch during CAS for run '{run_id}': expected {expected_fencing_token}, db has {cur.fencing_token}."
                    ) from exc
                raise FencingConflictError(f"Stage mutation CAS failed for run '{run_id}': {exc}") from exc
            raise JournalError(f"Atomic stage mutation failed for run '{run_id}': {exc}") from exc

        return new_fencing_token

    def execute_conditional_cleanup(
        self,
        *,
        run_id: str,
        expected_fencing_token: int,
        manifest_seal_hex: Optional[str] = None,
        target_entities_subset: Optional[list[str]] = None,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute fail-closed conditional cleanup with in-transaction audit verification."""
        if self.is_halted(run_id):
            raise JournalHaltedError(f"Run '{run_id}' is locally HALTED. Cleanup rejected fail-closed.")

        now_iso = (now or datetime.now(timezone.utc)).isoformat()

        entry = self.inspect_run(run_id)
        if not entry:
            raise FencingConflictError(f"Run '{run_id}' not found in staging_run_journal.")
        if entry.owner_id != self._owner_id:
            raise StaleOwnerError(
                f"Cleanup ownership violation: runner '{self._owner_id}' is not registered owner '{entry.owner_id}' for run '{run_id}'."
            )
        if entry.fencing_token != expected_fencing_token:
            raise FencingConflictError(
                f"Fencing token mismatch for run '{run_id}': expected {expected_fencing_token}, current {entry.fencing_token}."
            )
        if manifest_seal_hex is not None and manifest_seal_hex != entry.manifest_seal_hex:
            raise ManifestTamperedError(
                f"Provided manifest_seal_hex '{manifest_seal_hex}' does not match registered seal '{entry.manifest_seal_hex}'."
            )

        validate_status_transition(entry.status, JournalStatus.CLEANUP_PENDING)

        registered_entities = entry.get_registered_entities()
        sorted_entities = sorted(
            registered_entities,
            key=lambda e: int(e.get("cleanup_dependency_order", 1)),
            reverse=True,
        )
        all_pks = {ent["pk_value"] for ent in sorted_entities}

        if target_entities_subset is not None:
            if set(target_entities_subset) != all_pks:
                raise CleanupVerificationError(
                    f"Partial cleanup subset requested: registered entities count is {len(all_pks)}, "
                    f"subset count is {len(target_entities_subset)}. Partial cleanup cannot transition to CLEANED_UP "
                    "and is rejected fail-closed."
                )

        new_fencing_token = expected_fencing_token + 1

        # Step 1: BEGIN + CAS fence update to CLEANUP_PENDING + Deletions
        req1: list[dict[str, Any]] = [
            {"type": "execute", "stmt": {"sql": "BEGIN"}}
        ]

        cas_sql = """
        UPDATE staging_run_journal
        SET fencing_token = ?,
            stage = 'LIVE_08_CLEANUP',
            status = ?,
            updated_at = ?
        WHERE run_id = ?
          AND fencing_token = ?
          AND owner_id = ?
          AND status IN ('MUTATING', 'EFFECT_COMMITTED')
          AND (
              NOT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='run_locks')
              OR EXISTS (
                  SELECT 1 FROM run_locks
                  WHERE lock_key = ? AND worker_id = ? AND expires_at > ?
              )
          )
        """
        cas_params = [
            new_fencing_token,
            JournalStatus.CLEANUP_PENDING.value,
            now_iso,
            run_id,
            expected_fencing_token,
            self._owner_id,
            self._workload_key,
            self._owner_id,
            now_iso,
        ]
        req1.append({
            "type": "execute",
            "stmt": {"sql": cas_sql, "args": [_serialize_hrana_arg(p) for p in cas_params]},
        })

        for ent in sorted_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]

            clauses = [f"{pk_col} = ?"]
            params: list[Any] = [pk_val]

            for attr_k, attr_v in ent.get("expected_attributes", {}).items():
                col_name = "worker_id" if (tbl == "run_locks" and attr_k == "worker_ids") else attr_k
                if isinstance(attr_v, list):
                    placeholders = ", ".join("?" for _ in attr_v)
                    clauses.append(f"{col_name} IN ({placeholders})")
                    params.extend(attr_v)
                else:
                    clauses.append(f"{col_name} = ?")
                    params.append(attr_v)

            del_sql = f"DELETE FROM {tbl} WHERE " + " AND ".join(clauses)
            req1.append({
                "type": "execute",
                "stmt": {"sql": del_sql, "args": [_serialize_hrana_arg(p) for p in params]},
            })

        try:
            res1 = self._post_pipeline(req1)
        except PermissionError:
            raise
        except (TursoAmbiguousTransactionError, TursoClientError, httpx.HTTPError, Exception) as exc:
            self._latch_local_halt(run_id, f"Phase 1 pipeline dispatch failed in cleanup: {exc}")
            raise CleanupVerificationError(f"Phase 1 pipeline dispatch failed in cleanup: {exc}. Run locally HALTED.") from exc

        if not isinstance(res1, dict):
            self._latch_local_halt(run_id, f"Malformed Phase 1 cleanup response: expected dict, got {type(res1).__name__}")
            raise CleanupVerificationError(f"Malformed Phase 1 cleanup response: expected dict, got {type(res1).__name__}. Run locally HALTED.")

        baton = res1.get("baton")
        results1 = res1.get("results")

        if not baton or not isinstance(baton, str) or not baton.strip():
            self._latch_local_halt(run_id, "Missing session baton in cleanup Phase 1")
            raise TursoClientError("Missing session baton in cleanup Phase 1.")

        expected_results1_count = 2 + len(sorted_entities)
        if not isinstance(results1, list) or len(results1) != expected_results1_count:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(
                run_id,
                f"Truncated Phase 1 cleanup response: expected {expected_results1_count}, "
                f"got {len(results1) if isinstance(results1, list) else type(results1)}",
            )
            raise CleanupVerificationError(
                f"Truncated Phase 1 cleanup response: expected {expected_results1_count}, "
                f"got {len(results1) if isinstance(results1, list) else type(results1)}. Transaction rolled back."
            )

        # Check BEGIN
        if not isinstance(results1[0], dict) or results1[0].get("type") != "ok":
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(run_id, "Failed to begin cleanup transaction")
            raise TursoClientError("Failed to begin cleanup transaction.")

        # Check CAS
        cas_res = results1[1]
        if not isinstance(cas_res, dict) or cas_res.get("type") != "ok":
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            raise FencingConflictError(f"Cleanup CAS failed: {cas_res.get('error') if isinstance(cas_res, dict) else 'malformed CAS result'}")

        cas_resp_obj = cas_res.get("response") if isinstance(cas_res, dict) else None
        cas_res_dict = cas_resp_obj.get("result") if isinstance(cas_resp_obj, dict) else None
        affected_cas = cas_res_dict.get("affected_row_count", 0) if isinstance(cas_res_dict, dict) else 0
        if affected_cas != 1:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            raise FencingConflictError(
                f"Cleanup CAS affected {affected_cas} rows (expected 1). Fencing check failed; transaction rolled back."
            )

        # Check all DELETE statement outcomes
        for del_idx, del_res in enumerate(results1[2:]):
            if not isinstance(del_res, dict) or del_res.get("type") != "ok":
                try:
                    self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
                except Exception:
                    pass
                err_dict = del_res.get("error", {}) if isinstance(del_res, dict) and isinstance(del_res.get("error"), dict) else {}
                err_msg = err_dict.get("message", f"DELETE statement {del_idx} failed")
                self._latch_local_halt(run_id, f"Cleanup DELETE statement {del_idx} failed: {err_msg}")
                raise CleanupVerificationError(f"Cleanup DELETE statement {del_idx} failed: {err_msg}. Transaction rolled back.")

        # Phase 2: In-transaction audit of all registered PKs inside the active baton
        req2: list[dict[str, Any]] = []
        for ent in sorted_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]
            audit_sql = f"SELECT * FROM {tbl} WHERE {pk_col} = ?"
            req2.append({
                "type": "execute",
                "stmt": {"sql": audit_sql, "args": [_serialize_hrana_arg(pk_val)]},
            })

        try:
            res2 = self._post_pipeline(req2, baton=baton)
        except PermissionError:
            raise
        except (TursoAmbiguousTransactionError, TursoClientError, httpx.HTTPError, Exception) as exc:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(run_id, f"Phase 2 audit pipeline dispatch failed in cleanup: {exc}")
            raise CleanupVerificationError(f"Phase 2 audit pipeline dispatch failed in cleanup: {exc}. Run locally HALTED.") from exc

        if not isinstance(res2, dict):
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(run_id, f"Malformed Phase 2 audit response: expected dict, got {type(res2).__name__}")
            raise CleanupVerificationError(f"Malformed Phase 2 audit response: expected dict, got {type(res2).__name__}. Run locally HALTED.")

        baton = res2.get("baton", baton)
        results2 = res2.get("results")

        expected_audit_count = len(sorted_entities)
        if not isinstance(results2, list) or len(results2) != expected_audit_count:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(
                run_id,
                f"Truncated in-transaction cleanup audit response: expected {expected_audit_count}, "
                f"got {len(results2) if isinstance(results2, list) else type(results2)}",
            )
            raise CleanupVerificationError(
                f"Truncated in-transaction cleanup audit response: expected {expected_audit_count}, "
                f"got {len(results2) if isinstance(results2, list) else type(results2)}. Transaction rolled back."
            )

        residual_detected: Optional[str] = None
        for idx, audit_res in enumerate(results2):
            if not isinstance(audit_res, dict) or audit_res.get("type") != "ok":
                err = audit_res.get("error") if isinstance(audit_res, dict) else "malformed result"
                residual_detected = f"Audit query failed for entity index {idx}: {err}"
                break

            resp_obj = audit_res.get("response")
            if not isinstance(resp_obj, dict):
                residual_detected = f"Malformed audit response object for entity index {idx}: {type(resp_obj).__name__}"
                break

            result_dict = resp_obj.get("result")
            if not isinstance(result_dict, dict):
                residual_detected = f"Malformed audit result dict for entity index {idx}: {type(result_dict).__name__}"
                break

            if "rows" not in result_dict or not isinstance(result_dict["rows"], list):
                residual_detected = (
                    f"Audit result missing explicit 'rows' array for entity index {idx} "
                    f"table '{sorted_entities[idx]['table']}', pk '{sorted_entities[idx]['pk_value']}'"
                )
                break

            rows = result_dict["rows"]
            if len(rows) > 0:
                ent = sorted_entities[idx]
                residual_detected = (
                    f"Residual or foreign row detected for table '{ent['table']}', "
                    f"pk '{ent['pk_value']}' (found {len(rows)} rows after cleanup DELETE)."
                )
                break

        if residual_detected:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(run_id, f"In-transaction cleanup audit failed: {residual_detected}")
            raise CleanupVerificationError(
                f"In-transaction cleanup audit failed: {residual_detected}. "
                "Transaction rolled back; run remains unresolved and is locally HALTED."
            )

        # Phase 3: Release lease in run_locks + update status to CLEANED_UP (WITHOUT COMMIT)
        req3: list[dict[str, Any]] = []

        cur_tbl = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='run_locks'"
        )
        has_run_locks = bool(cur_tbl.fetchone())
        if has_run_locks:
            req3.append({
                "type": "execute",
                "stmt": {
                    "sql": "DELETE FROM run_locks WHERE lock_key = ? AND worker_id = ?",
                    "args": [_serialize_hrana_arg(self._workload_key), _serialize_hrana_arg(self._owner_id)],
                },
            })

        req3.append({
            "type": "execute",
            "stmt": {
                "sql": "UPDATE staging_run_journal SET status = ?, updated_at = ? WHERE run_id = ? AND fencing_token = ?",
                "args": [
                    _serialize_hrana_arg(JournalStatus.CLEANED_UP.value),
                    _serialize_hrana_arg(now_iso),
                    _serialize_hrana_arg(run_id),
                    _serialize_hrana_arg(new_fencing_token),
                ],
            },
        })

        try:
            res3 = self._post_pipeline(req3, baton=baton)
        except PermissionError:
            raise
        except (TursoAmbiguousTransactionError, TursoClientError, httpx.HTTPError, Exception) as exc:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(run_id, f"Phase 3 finalization pipeline dispatch failed in cleanup: {exc}")
            raise CleanupVerificationError(f"Phase 3 finalization pipeline dispatch failed in cleanup: {exc}. Run locally HALTED.") from exc

        if not isinstance(res3, dict):
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(run_id, f"Malformed Phase 3 finalization response: expected dict, got {type(res3).__name__}")
            raise CleanupVerificationError(f"Malformed Phase 3 finalization response: expected dict, got {type(res3).__name__}. Run locally HALTED.")

        baton = res3.get("baton", baton)
        results3 = res3.get("results")

        expected_prep_count = len(req3)
        if not isinstance(results3, list) or len(results3) != expected_prep_count:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(
                run_id,
                f"Truncated Phase 3 cleanup preparation response: expected {expected_prep_count}, "
                f"got {len(results3) if isinstance(results3, list) else type(results3)}",
            )
            raise CleanupVerificationError(
                f"Truncated Phase 3 cleanup preparation response: expected {expected_prep_count}, "
                f"got {len(results3) if isinstance(results3, list) else type(results3)}. Transaction rolled back."
            )

        for p_idx, p_res in enumerate(results3):
            if not isinstance(p_res, dict) or p_res.get("type") != "ok":
                err = p_res.get("error") if isinstance(p_res, dict) else "malformed result"
                try:
                    self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
                except Exception:
                    pass
                self._latch_local_halt(run_id, f"Phase 3 statement {p_idx} failed: {err}")
                raise CleanupVerificationError(f"Cleanup finalization statement {p_idx} failed: {err}. Transaction rolled back.")

        # Crucial check: verify journal UPDATE affected exactly 1 row
        last_res = results3[-1] if isinstance(results3, list) and results3 else None
        last_resp_obj = last_res.get("response") if isinstance(last_res, dict) else None
        last_res_dict = last_resp_obj.get("result") if isinstance(last_resp_obj, dict) else None
        affected_journal = last_res_dict.get("affected_row_count", 0) if isinstance(last_res_dict, dict) else None
        if affected_journal != 1:
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)
            except Exception:
                pass
            self._latch_local_halt(
                run_id,
                f"Journal UPDATE to CLEANED_UP affected {affected_journal} rows (expected 1)",
            )
            raise CleanupVerificationError(
                f"Failed to update journal status to CLEANED_UP: affected {affected_journal} rows (expected 1). "
                "Transaction rolled back; run locally HALTED."
            )

        # Phase 4: Explicit COMMIT only after Phase 3 verification succeeded
        try:
            commit_res = self._post_pipeline(
                [{"type": "execute", "stmt": {"sql": "COMMIT"}}],
                baton=baton,
            )
        except PermissionError:
            raise
        except (TursoAmbiguousTransactionError, TursoClientError, httpx.HTTPError, Exception) as exc:
            self._latch_local_halt(run_id, f"Ambiguous COMMIT on cleanup transaction: {exc}")
            raise AmbiguousCommitError(
                f"Ambiguous COMMIT on cleanup transaction for run '{run_id}': {exc}. "
                "Local status latched to LOCAL_OUTCOME_UNKNOWN / HALT."
            ) from exc

        if not isinstance(commit_res, dict):
            self._latch_local_halt(run_id, f"Malformed cleanup COMMIT response: expected dict, got {type(commit_res).__name__}")
            raise AmbiguousCommitError(f"Malformed cleanup COMMIT response: expected dict, got {type(commit_res).__name__}. Local status latched to LOCAL_OUTCOME_UNKNOWN / HALT.")

        c_results = commit_res.get("results")
        if (
            not isinstance(c_results, list)
            or len(c_results) != 1
            or not isinstance(c_results[0], dict)
            or c_results[0].get("type") != "ok"
        ):
            err_msg = (
                c_results[0].get("error", {}).get("message", "COMMIT not ok")
                if (isinstance(c_results, list) and c_results and isinstance(c_results[0], dict) and isinstance(c_results[0].get("error"), dict))
                else f"Malformed COMMIT acknowledgment: {commit_res}"
            )
            self._latch_local_halt(run_id, f"Cleanup COMMIT outcome uncertain: {err_msg}")
            raise AmbiguousCommitError(f"Cleanup COMMIT failed or unverified: {err_msg}. Local status latched to LOCAL_OUTCOME_UNKNOWN / HALT.")

        return new_fencing_token

    def audit_workload(self, run_id: str) -> AuditOutcomeState:
        """Perform an independent 3-state audit on the registered entity inventory."""
        self._validate_hermetic_transport_identity()
        entry = self.inspect_run(run_id)
        if not entry:
            return AuditOutcomeState.UNKNOWN

        registered = entry.get_registered_entities()
        found_any = False

        for ent in registered:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]

            try:
                res = self._execute_guarded_client_query(
                    f"SELECT * FROM {tbl} WHERE {pk_col} = ?",
                    [pk_val],
                )
                rows = res.fetchall()
            except PermissionError:
                raise
            except Exception:
                return AuditOutcomeState.UNKNOWN

            if rows:
                found_any = True
                row = rows[0]
                for exp_k, exp_v in ent.get("expected_attributes", {}).items():
                    col_name = "worker_id" if (tbl == "run_locks" and exp_k == "worker_ids") else exp_k
                    actual_val = row.get(col_name)
                    if isinstance(exp_v, list):
                        if actual_val not in exp_v:
                            return AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                    elif actual_val != exp_v:
                        return AuditOutcomeState.OWNERSHIP_MISMATCH_HALT

        if found_any:
            return AuditOutcomeState.RESIDUAL
        return AuditOutcomeState.CLEAN

    def review_recovery_evidence(
        self,
        *,
        run_id: str,
        manifest_bytes: Optional[bytes] = None,
    ) -> CrossRunnerReview:
        """Inspect provider DB via Hrana and generate a typed CrossRunnerReview."""
        self._validate_hermetic_transport_identity()
        entry = self.inspect_run(run_id)

        j_entry: Optional[CrossRunnerJournalEntry] = None
        observations: list[EntityObservation] = []
        commit_state = CommitState.NOT_REPORTED

        if entry:
            j_status = CrossRunnerJournalStatus.INTENT_RECORDED
            if entry.status == JournalStatus.EFFECT_COMMITTED:
                j_status = CrossRunnerJournalStatus.EFFECT_COMMITTED
            elif entry.status == JournalStatus.ABORTED:
                j_status = CrossRunnerJournalStatus.ABORTED
            elif entry.status == JournalStatus.UNKNOWN:
                commit_state = CommitState.AMBIGUOUS

            registered_pks = tuple(f"{e['table']}:{e['pk_value']}" for e in entry.get_registered_entities())
            stage_str = entry.stage if entry.stage.startswith("LIVE") else "LIVE_02"
            if "-" in stage_str:
                stage_str = stage_str.replace("-", "_")

            j_entry = CrossRunnerJournalEntry(
                run_id=entry.run_id,
                runner_id=entry.owner_id,
                status=j_status,
                intent_stage=stage_str,
                target_entities=registered_pks[:10],
                journal_commit_state=commit_state,
                created_at=entry.created_at,
            )

            # Query observations for registered entities
            for ent in entry.get_registered_entities():
                tbl = ent["table"]
                pk_col = ent["pk_column"]
                pk_val = ent["pk_value"]
                try:
                    res = self._execute_guarded_client_query(
                        f"SELECT * FROM {tbl} WHERE {pk_col} = ?",
                        [pk_val],
                    )
                    rows = res.fetchall()
                    expected_keys = {pk_col} | {
                        ("worker_id" if k == "worker_ids" else k)
                        for k in ent.get("expected_attributes", {}).keys()
                    }
                    safe_rows = []
                    for r in rows[:2]:
                        safe_r = {str(k): str(v) for k, v in r.items() if str(k) in expected_keys and len(str(v)) <= 512}
                        safe_rows.append(safe_r)
                    observations.append(
                        EntityObservation(
                            table=tbl,
                            pk_column=pk_col,
                            pk_value=pk_val,
                            query_state=QueryState.OBSERVED,
                            rows=tuple(safe_rows),
                        )
                    )
                except PermissionError:
                    raise
                except Exception:
                    observations.append(
                        EntityObservation(
                            table=tbl,
                            pk_column=pk_col,
                            pk_value=pk_val,
                            query_state=QueryState.FAILED,
                            rows=(),
                        )
                    )

        seal_hex = compute_canonical_inventory_fingerprint(run_id)
        snapshot = HermeticSnapshot(
            manifest_sha256=seal_hex,
            observations=tuple(observations[:5]),
            commit_state=commit_state,
        ) if observations else None

        return review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id=self._owner_id,
            journal_entry=j_entry,
            snapshot=snapshot,
        )
