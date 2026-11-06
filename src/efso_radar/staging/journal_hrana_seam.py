"""Offline staging journal <-> mock Hrana transaction integration seam for ERD-017 (Option B + Option C).

STRICTLY OFFLINE INTEGRATION SEAM FOR STAGING RUN OWNERSHIP AND RECOVERY.
- Operates only against hermetic in-memory (:memory:) or temporary file SQLite fixtures
  within tempfile.gettempdir().
- Requires concrete HermeticStagingTransport (or wrapped in BudgetTrackingTransport).
- Zero network I/O, zero Turso/cloud connections, zero credentials or tokens.
- Absolute $0.00 / 0 TRY spending ceiling.
- Candidate Schema v7 DDL is prototyped here ONLY and must NEVER be added to
  automatic startup migrations or bump CURRENT_SCHEMA_VERSION in schema.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Optional

import httpx

from efso_radar.repositories.turso_client import (
    TursoAmbiguousTransactionError,
    TursoClientError,
    TursoHTTPClient,
    serialize_hrana_arg,
)
from efso_radar.staging.journal import (
    HEX_SHA256_REGEX,
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
    _halt_marker_path,
    canonical_inventory_json,
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
    review_cross_runner_recovery_evidence,
)
from efso_radar.staging.runner import (
    RUN_ID_REGEX,
    AuditOutcomeState,
    BudgetTrackingTransport,
    HermeticStagingTransport,
    PerRunOwnershipManifest,
    RegisteredEntity,
    create_default_manifest_entities,
)


def _assert_fixture_isolation(conn: sqlite3.Connection) -> None:
    """Enforce runtime isolation: DB must be in-memory or in temp directory."""
    cur = conn.cursor()
    cur.execute("PRAGMA database_list;")
    db_list = cur.fetchall()
    for row in db_list:
        db_file = row[2] if len(row) > 2 else ""
        if db_file and db_file != "" and not str(db_file).startswith(":memory:"):
            resolved_file = Path(db_file).resolve()
            temp_dir = Path(tempfile.gettempdir()).resolve()
            try:
                resolved_file.relative_to(temp_dir)
            except ValueError:
                raise PermissionError(
                    f"Security violation: backing SQLite file '{resolved_file}' is outside the OS temporary directory. "
                    "Integration seam is restricted strictly to :memory: or OS temp directory fixtures."
                )


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
    """Minimal offline integration seam connecting StagingOwnershipJournal and mock Hrana transport.

    Proves that journal ownership, authoritative fencing, allowlisted entity mutations,
    and effect confirmation operate atomically when the mock backend supports a shared transaction.

    Safety Boundary:
    - Strictly offline: Operates ONLY against hermetic in-memory or temporary file SQLite fixtures
      within tempfile.gettempdir(). Operational databases are rejected fail-closed.
    - Requires concrete HermeticStagingTransport (or wrapped in BudgetTrackingTransport).
      Standard HTTP/network transports are rejected fail-closed.
    - Absolute $0.00 / 0 TRY spending ceiling.
    - Zero remote network I/O, zero Turso credentials.
    """

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

        self._client = client
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

    def _latch_local_halt(self, run_id: str, reason: str) -> Path:
        """Latch run into LOCAL_OUTCOME_UNKNOWN / HALT state in-memory and on local disk."""
        self._halted_runs.add(run_id)
        if self._marker_dir is None:
            raise HaltMarkerPersistenceError(
                f"Cannot persist halt marker for run '{run_id}': no scoped marker_dir configured. "
                "Implicit global temp marker persistence is prohibited; run is halted in-memory only."
            )
        marker = _halt_marker_path(run_id, self._marker_dir)

        payload = {
            "run_id": run_id,
            "halted_at": self._now_iso(),
            "reason": reason,
        }
        content = json.dumps(payload, indent=2)

        if marker.exists():
            try:
                existing = json.loads(marker.read_text(encoding="utf-8"))
                if isinstance(existing, dict) and existing.get("run_id") == run_id:
                    return marker
            except Exception:
                pass
            raise HaltMarkerPersistenceError(
                f"Cannot persist halt marker for run '{run_id}': pre-existing file at '{marker}' "
                "cannot be overwritten without proven file ownership."
            )

        try:
            with open(marker, "x", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
        except OSError as exc:
            raise HaltMarkerPersistenceError(
                f"Failed to persist durable local halt marker for run '{run_id}' at '{marker}': {exc}. "
                "Local persistence failed; automated continuation cannot be safely prevented."
            ) from exc

        return marker

    def _now_iso(self, now: Optional[datetime] = None) -> str:
        ref_now = now or datetime.now(timezone.utc)
        if ref_now.tzinfo is None:
            ref_now = ref_now.replace(tzinfo=timezone.utc)
        else:
            ref_now = ref_now.astimezone(timezone.utc)
        return ref_now.isoformat()

    def _verify_journal_table_exists(self) -> None:
        """Verify candidate Schema v7 table exists via Hrana query."""
        res = self._client.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='staging_run_journal'"
        )
        if not res.fetchone():
            raise JournalError(
                "Candidate Schema v7 table 'staging_run_journal' does not exist in this database. "
                "Invoke apply_candidate_schema_v7(conn) on this hermetic test connection first."
            )

    def inspect_run(self, run_id: str) -> Optional[StagingJournalEntry]:
        """Inspect a staging run entry in the journal over Hrana."""
        res = self._client.execute(
            """
            SELECT run_id, owner_id, fencing_token, stage, status,
                   manifest_seal_hex, target_inventory_json, created_at, updated_at, last_error
            FROM staging_run_journal
            WHERE run_id = ?
            """,
            [run_id],
        )
        row = res.fetchone()
        if not row:
            return None
        return StagingJournalEntry(
            run_id=row["run_id"],
            owner_id=row["owner_id"],
            fencing_token=int(row["fencing_token"]),
            stage=row["stage"],
            status=JournalStatus(row["status"]),
            manifest_seal_hex=row["manifest_seal_hex"],
            target_inventory_json=row["target_inventory_json"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_error=row.get("last_error"),
            read_provenance=ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
        )

    def _post_pipeline(self, requests_payload: list[dict[str, Any]], baton: Optional[str] = None) -> dict[str, Any]:
        """Internal helper to dispatch Hrana /v2/pipeline requests using configured client transport."""
        http_client = self._client._get_client()
        body: dict[str, Any] = {"requests": requests_payload}
        if baton is not None:
            body["baton"] = baton
        resp = http_client.post(self._client.pipeline_url, json=body)
        if resp.status_code != 200:
            if resp.status_code in (500, 502, 503, 504):
                raise TursoAmbiguousTransactionError(f"HTTP {resp.status_code} during pipeline request.")
            raise TursoClientError(f"HTTP {resp.status_code} error during pipeline request.")
        return resp.json()

    def _execute_hrana_transaction(
        self,
        statements: list[tuple[str, list[Any]]],
        *,
        required_affected_counts: list[Optional[int]],
        run_id: str,
    ) -> list[dict[str, Any]]:
        """Execute a multi-statement transaction with baton over Hrana v2 pipeline.

        Enforces Fencing on the Effect:
        - Inspects individual statement results for errors and rowcount requirements before COMMIT.
        - If ANY statement returns an error, or if required_affected_counts check fails:
          sends explicit ROLLBACK and aborts, ensuring mutations are NEVER committed.
        - If COMMIT times out or drops network: latches local halt and raises AmbiguousCommitError.
        """
        requests: list[dict[str, Any]] = [
            {"type": "execute", "stmt": {"sql": "BEGIN"}}
        ]
        for sql, args in statements:
            requests.append({
                "type": "execute",
                "stmt": {
                    "sql": sql.strip(),
                    "args": [serialize_hrana_arg(a) for a in args],
                },
            })

        # Step 1: Send BEGIN + statements
        http_client = self._client._get_client()
        try:
            data = self._post_pipeline(requests, baton=None)
        except (httpx.HTTPError, TursoClientError) as exc:
            raise JournalError(f"Transaction initialization failed: {exc}") from exc

        baton = data.get("baton")
        raw_results = data.get("results", [])

        if not baton:
            raise JournalError("Server protocol violation: missing session baton for transaction.")

        # Validate BEGIN
        if not raw_results or raw_results[0].get("type") != "ok":
            if baton:
                try:
                    self._post_pipeline([{"type": "close"}], baton=baton)
                except Exception:
                    pass
            raise JournalError("Transaction BEGIN failed.")

        # Validate user statement results
        stmt_results = raw_results[1:]
        if len(stmt_results) < len(statements):
            try:
                self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}], baton=baton)
            except Exception:
                pass
            raise JournalError("Transaction response malformed: truncated statement result list.")

        for i, res in enumerate(stmt_results):
            req_cnt = required_affected_counts[i] if i < len(required_affected_counts) else None
            res_type = res.get("type")
            if res_type == "error":
                err_msg = res.get("error", {}).get("message", "Unknown statement error")
                try:
                    self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}], baton=baton)
                except Exception:
                    pass
                raise JournalError(f"Statement execution error in transaction at index {i}: {err_msg}")

            if res_type != "ok":
                try:
                    self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}], baton=baton)
                except Exception:
                    pass
                raise JournalError(f"Unexpected statement response type at index {i}: {res_type}")

            if req_cnt is not None:
                actual_cnt = res.get("response", {}).get("result", {}).get("affected_row_count", 0)
                if actual_cnt != req_cnt:
                    try:
                        self._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}], baton=baton)
                    except Exception:
                        pass
                    raise FencingConflictError(
                        f"Authoritative fence/rowcount condition failed at index {i}: expected {req_cnt}, got {actual_cnt}. "
                        "Transaction rolled back; zero mutations applied."
                    )

        # Step 2: Send COMMIT
        commit_payload = [
            {"type": "execute", "stmt": {"sql": "COMMIT"}},
            {"type": "close"},
        ]
        try:
            commit_data = self._post_pipeline(commit_payload, baton=baton)
        except (httpx.HTTPError, TursoAmbiguousTransactionError) as exc:
            try:
                self._latch_local_halt(run_id, f"Ambiguous COMMIT outcome: {exc}")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Ambiguous COMMIT encountered for run '{run_id}': transaction outcome uncertain, "
                    f"and local halt marker persistence failed: {marker_exc}. Run is in-memory HALTED."
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Ambiguous COMMIT encountered for run '{run_id}': network error or timeout during COMMIT ({exc}). "
                "Client outcome is LOCAL_OUTCOME_UNKNOWN / HALT. Automated retries or mutations are prohibited."
            ) from exc

        commit_results = commit_data.get("results", [])
        if not commit_results or commit_results[0].get("type") != "ok":
            try:
                self._latch_local_halt(run_id, "Ambiguous COMMIT response error")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Ambiguous COMMIT outcome for run '{run_id}'; halt marker persistence failed: {marker_exc}"
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Ambiguous COMMIT outcome for run '{run_id}': commit result was not 'ok'. Client outcome is UNKNOWN."
            )

        return stmt_results

    def register_intent(
        self,
        *,
        run_id: str,
        manifest_seal_hex: str,
        target_inventory: list[RegisteredEntity] | list[dict[str, Any]],
        stage: str = "INTENT_RECORDING",
        initial_fencing_token: int = 1,
        pattern: JournalExecutionPattern = JournalExecutionPattern.OPTION_B_SEPARATED,
        simulate_ambiguous_commit: Optional[AmbiguousCommitOutcome] = None,
        now: Optional[datetime] = None,
    ) -> StagingJournalEntry:
        """Register ahead-of-time staging run ownership and target inventory over Hrana."""
        if self.is_halted(run_id):
            raise JournalHaltedError(
                f"Run '{run_id}' is permanently halted in LOCAL_OUTCOME_UNKNOWN. "
                "Subsequent registration, automated mutations, retries, cleanups, or aborts are prohibited."
            )

        if not isinstance(run_id, str) or not RUN_ID_REGEX.match(run_id):
            raise InvalidRunIdError(
                f"Invalid run_id '{run_id}'. Must match canonical profile {RUN_ID_REGEX.pattern}."
            )

        if not isinstance(manifest_seal_hex, str) or not HEX_SHA256_REGEX.match(manifest_seal_hex):
            raise ManifestTamperedError(
                f"Invalid manifest seal '{manifest_seal_hex}'. Must be a 64-character lowercase SHA-256 hex string."
            )

        # Canonical manifest payload validation & fingerprint check
        raw_entities = []
        for ent in target_inventory:
            if hasattr(ent, "model_dump"):
                raw_entities.append(ent.model_dump())
            elif isinstance(ent, dict):
                raw_entities.append(ent)
            else:
                raise TargetInventoryViolationError(f"Unsupported entity type in target_inventory: {type(ent)}")

        manifest_payload = {
            "run_id": run_id,
            "created_at": "2026-11-06T20:00:00Z",
            "version": 1,
            "registered_entities": raw_entities,
        }
        try:
            validated_manifest = PerRunOwnershipManifest.validate_payload(manifest_payload)
        except Exception as exc:
            raise TargetInventoryViolationError(
                f"Target inventory fails canonical manifest validation for run '{run_id}': {exc}"
            ) from exc

        expected_seal = compute_canonical_inventory_fingerprint(run_id, validated_manifest.registered_entities)
        if manifest_seal_hex != expected_seal:
            raise ManifestTamperedError(
                f"Manifest seal mismatch for run '{run_id}': expected authentic seal '{expected_seal}', "
                f"received '{manifest_seal_hex}'. Caller-supplied digest is not trusted as authentication proof."
            )

        inventory_json = canonical_inventory_json(target_inventory)
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        else:
            now_dt = now_dt.astimezone(timezone.utc)
        now_iso = now_dt.isoformat()
        lease_expires_iso = (now_dt + timedelta(minutes=15)).isoformat()

        initial_status = (
            JournalStatus.INTENT_PENDING
            if pattern == JournalExecutionPattern.OPTION_B_SEPARATED
            else JournalStatus.ACKED
        )

        # 1. Check for ANY unresolved runs in staging_run_journal
        res_unresolved = self._client.execute(
            """
            SELECT run_id, stage, status
            FROM staging_run_journal
            WHERE status NOT IN ('CLEANED_UP', 'ABORTED')
            """
        )
        for u_row in res_unresolved.fetchall():
            u_run = u_row["run_id"]
            if u_run != run_id:
                raise ActiveWorkloadCollisionError(
                    f"Cannot register intent: an unresolved staging run '{u_run}' exists "
                    f"with status '{u_row['status']}' in stage '{u_row['stage']}'. "
                    "Workload is locked until all prior runs reach CLEANED_UP or ABORTED."
                )

        # 2. Check duplicate identical run_id
        res_dup = self._client.execute(
            "SELECT run_id FROM staging_run_journal WHERE run_id = ?",
            [run_id],
        )
        if res_dup.fetchone():
            raise ActiveWorkloadCollisionError(
                f"Duplicate identical run_id '{run_id}' already registered in journal."
            )

        # 3. Check and acquire lease in run_locks if table exists
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
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(lock_key) DO UPDATE SET
                    worker_id = excluded.worker_id,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at,
                    heartbeat_at = excluded.heartbeat_at
                WHERE run_locks.expires_at < ? OR run_locks.worker_id = ?
                """,
                [self._workload_key, self._owner_id, now_iso, lease_expires_iso, now_iso, now_iso, self._owner_id],
            ))
            required_counts.append(1)

        # Insert journal entry
        statements.append((
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                self._owner_id,
                initial_fencing_token,
                stage,
                initial_status.value,
                manifest_seal_hex,
                inventory_json,
                now_iso,
                now_iso,
            ],
        ))
        required_counts.append(1)

        # In Option B separated pattern: update INTENT_PENDING -> ACKED
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
            # Execute commit on provider, then simulate lost ACK
            self._execute_hrana_transaction(
                statements,
                required_affected_counts=required_counts,
                run_id=run_id,
            )
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
            # Transaction is rolled back without persisting on provider
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

        self._execute_hrana_transaction(
            statements,
            required_affected_counts=required_counts,
            run_id=run_id,
        )

        return StagingJournalEntry(
            run_id=run_id,
            owner_id=self._owner_id,
            fencing_token=initial_fencing_token,
            stage=stage,
            status=final_status,
            manifest_seal_hex=manifest_seal_hex,
            target_inventory_json=inventory_json,
            created_at=now_iso,
            updated_at=now_iso,
            last_error=None,
            read_provenance=ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
        )

    def execute_atomic_stage_mutation(
        self,
        *,
        run_id: str,
        expected_fencing_token: int,
        stage: str,
        mutation_statements: list[StageMutationCommand | tuple[str, tuple[Any, ...]]],
        manifest_seal_hex: str,
        simulate_ambiguous_commit: Optional[AmbiguousCommitOutcome] = None,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute Option B+C atomic batch over Hrana v2 pipeline transaction.

        Atomically couples:
        1. Journal CAS fence increment & transition to MUTATING.
        2. Strictly allowlisted entity mutations.
        3. Effect confirmation & transition to EFFECT_COMMITTED.

        Fencing on the Effect:
        - If token CAS condition matches 0 rows (stale owner or fencing race),
          an explicit ROLLBACK is issued and mutations are NEVER committed.
        - Returns incremented fencing token upon successful commit.
        """
        if self.is_halted(run_id):
            raise JournalHaltedError(
                f"Run '{run_id}' is permanently halted in LOCAL_OUTCOME_UNKNOWN. "
                "Subsequent automated mutations, retries, cleanups, or aborts are prohibited."
            )

        now_iso = self._now_iso(now)

        entry = self.inspect_run(run_id)
        if not entry:
            raise JournalError(f"No journal entry found for run_id '{run_id}'.")

        if entry.owner_id != self._owner_id:
            raise StaleOwnerError(
                f"Ownership violation: acting runner '{self._owner_id}' does not match registered owner '{entry.owner_id}'."
            )

        if entry.fencing_token != expected_fencing_token:
            raise FencingConflictError(
                f"Fencing token mismatch for run_id '{run_id}': expected {expected_fencing_token}, "
                f"current database token is {entry.fencing_token}."
            )

        if entry.manifest_seal_hex != manifest_seal_hex:
            raise ManifestTamperedError(
                f"Manifest seal mismatch for run_id '{run_id}': manifest has been altered or tampered."
            )

        validate_status_transition(entry.status, JournalStatus.MUTATING)

        registered_entities = entry.get_registered_entities()
        validated_sql_list: list[tuple[str, list[Any]]] = []
        for m in mutation_statements:
            sql, params = validate_and_normalize_mutation(m, registered_entities)
            validated_sql_list.append((sql, list(params)))

        new_fencing_token = expected_fencing_token + 1

        # Build atomic statement batch
        statements: list[tuple[str, list[Any]]] = []
        required_counts: list[Optional[int]] = []

        # Stmt 0: CAS fence increment & MUTATING status
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
            """,
            [
                new_fencing_token,
                stage,
                JournalStatus.MUTATING.value,
                now_iso,
                run_id,
                expected_fencing_token,
                self._owner_id,
            ],
        ))
        required_counts.append(1)

        # Stmts 1..N: Entity mutations
        for sql, params in validated_sql_list:
            statements.append((sql, params))
            required_counts.append(1)

        # Stmt N+1: Effect confirmation
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
                self._latch_local_halt(run_id, "Ambiguous mutation COMMIT (COMMITTED_BUT_ACK_LOST)")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Ambiguous commit encountered for run '{run_id}': transaction committed on provider, "
                    f"but local halt marker persistence failed: {marker_exc}. "
                    "Client outcome is LOCAL_OUTCOME_UNKNOWN / HALT. Run is in-memory HALTED."
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Ambiguous commit encountered for run '{run_id}': transaction committed on provider, "
                "but network ACK was lost. Client outcome is LOCAL_OUTCOME_UNKNOWN / HALT."
            )
        elif simulate_ambiguous_commit == AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST:
            try:
                self._latch_local_halt(run_id, "Ambiguous mutation COMMIT (ROLLED_BACK_AND_ACK_LOST)")
            except HaltMarkerPersistenceError as marker_exc:
                raise AmbiguousCommitError(
                    f"Ambiguous commit encountered for run '{run_id}': transaction rolled back on provider, "
                    f"but local halt marker persistence failed: {marker_exc}."
                ) from marker_exc
            raise AmbiguousCommitError(
                f"Ambiguous commit encountered for run '{run_id}': transaction rolled back on provider, "
                "and network ACK was lost. Client outcome is LOCAL_OUTCOME_UNKNOWN / HALT."
            )

        self._execute_hrana_transaction(
            statements,
            required_affected_counts=required_counts,
            run_id=run_id,
        )

        return new_fencing_token

    def execute_conditional_cleanup(
        self,
        *,
        run_id: str,
        expected_fencing_token: int,
        manifest_seal_hex: str,
        target_entities_subset: Optional[list[str]] = None,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute LIVE-08 parameterized conditional cleanup & 3-state audit under authoritative fence."""
        if self.is_halted(run_id):
            raise JournalHaltedError(
                f"Run '{run_id}' is permanently halted in LOCAL_OUTCOME_UNKNOWN. "
                "Subsequent automated mutations, retries, cleanups, or aborts are prohibited."
            )

        now_iso = self._now_iso(now)

        entry = self.inspect_run(run_id)
        if not entry:
            raise JournalError(f"No journal entry found for run_id '{run_id}'.")

        if entry.owner_id != self._owner_id:
            raise StaleOwnerError(
                f"Cleanup ownership violation: runner '{self._owner_id}' is not registered owner '{entry.owner_id}'."
            )

        if entry.fencing_token != expected_fencing_token:
            raise FencingConflictError(
                f"Cleanup fencing violation: expected token {expected_fencing_token}, "
                f"current database token is {entry.fencing_token}."
            )

        if entry.manifest_seal_hex != manifest_seal_hex:
            raise ManifestTamperedError(
                f"Cleanup manifest seal mismatch for run_id '{run_id}'."
            )

        validate_status_transition(entry.status, JournalStatus.CLEANUP_PENDING)

        registered_entities = entry.get_registered_entities()
        sorted_entities = sorted(
            registered_entities,
            key=lambda e: int(e.get("cleanup_dependency_order", 1)),
            reverse=True,
        )

        new_fencing_token = expected_fencing_token + 1

        statements: list[tuple[str, list[Any]]] = []
        required_counts: list[Optional[int]] = []

        # 1. CAS fence update to CLEANUP_PENDING
        statements.append((
            """
            UPDATE staging_run_journal
            SET fencing_token = ?,
                stage = 'LIVE_08_CLEANUP',
                status = ?,
                updated_at = ?
            WHERE run_id = ?
              AND fencing_token = ?
              AND owner_id = ?
            """,
            [
                new_fencing_token,
                JournalStatus.CLEANUP_PENDING.value,
                now_iso,
                run_id,
                expected_fencing_token,
                self._owner_id,
            ],
        ))
        required_counts.append(1)

        # 2. Deletions
        for ent in sorted_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]

            if target_entities_subset is not None and pk_val not in target_entities_subset:
                continue

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
            statements.append((del_sql, params))
            required_counts.append(None)  # Can delete 0 or 1 rows depending on presence

        # 2b. Release workload lease in run_locks if present
        cur_tbl = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='run_locks'"
        )
        if cur_tbl.fetchone():
            statements.append((
                "DELETE FROM run_locks WHERE lock_key = ? AND worker_id = ?",
                [self._workload_key, self._owner_id],
            ))
            required_counts.append(None)

        # 3. Transition journal status to CLEANED_UP
        statements.append((
            """
            UPDATE staging_run_journal
            SET status = ?,
                updated_at = ?
            WHERE run_id = ?
              AND fencing_token = ?
            """,
            [
                JournalStatus.CLEANED_UP.value,
                now_iso,
                run_id,
                new_fencing_token,
            ],
        ))
        required_counts.append(1)

        self._execute_hrana_transaction(
            statements,
            required_affected_counts=required_counts,
            run_id=run_id,
        )

        return new_fencing_token

    def audit_workload(self, run_id: str) -> AuditOutcomeState:
        """Perform an independent 3-state audit on the registered entity inventory."""
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
                res = self._client.execute(
                    f"SELECT * FROM {tbl} WHERE {pk_col} = ?",
                    [pk_val],
                )
                rows = res.fetchall()
            except Exception:
                return AuditOutcomeState.UNKNOWN

            if rows:
                found_any = True
                expected_attrs = ent.get("expected_attributes", {})
                for row in rows:
                    for attr_k, attr_v in expected_attrs.items():
                        col_name = "worker_id" if (tbl == "run_locks" and attr_k == "worker_ids") else attr_k
                        if col_name in row:
                            actual = row[col_name]
                            if isinstance(attr_v, list):
                                if actual not in attr_v:
                                    return AuditOutcomeState.OWNERSHIP_MISMATCH_HALT
                            elif actual != attr_v:
                                return AuditOutcomeState.OWNERSHIP_MISMATCH_HALT

        if found_any:
            return AuditOutcomeState.RESIDUAL
        return AuditOutcomeState.CLEAN

    def propose_operator_reconciliation(
        self,
        run_id: str,
        proposed_action: str,
        reason: str,
    ) -> OperatorReconciliationProposal:
        """Emit a non-authorizing offline review proposal for an unresolved run."""
        return OperatorReconciliationProposal(
            run_id=run_id,
            operator_id=self._owner_id,
            proposed_action=proposed_action,
            reason=reason,
        )

    def review_recovery_evidence(
        self,
        *,
        run_id: str,
        manifest_bytes: bytes,
    ) -> CrossRunnerReview:
        """Inspect provider DB via Hrana and generate a typed CrossRunnerReview."""
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
                    res = self._client.execute(
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
