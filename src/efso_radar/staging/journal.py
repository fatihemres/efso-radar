"""Offline staging ownership journal implementation for ERD-017 (Option B + Option C).

STRICTLY OFFLINE ENGINE FOR STAGING RUN OWNERSHIP AND RECOVERY.
- Operates only against in-memory or temporary SQLite connections in hermetic tests.
- Zero network I/O, zero Turso/cloud connections, zero credentials or tokens.
- Absolute $0.00 / 0 TRY spending ceiling.
- Candidate Schema v7 DDL is prototyped here ONLY and must NEVER be added to
  automatic startup migrations or bump CURRENT_SCHEMA_VERSION in schema.py.
- Staging journal ONLY: Does NOT prove or implement Gemini project-wide quota
  reservations, G1 billing proof, Telegram at-most-once delivery, or G6 live readiness.
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import json
import re
import sqlite3
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from efso_radar.staging.runner import (
    RUN_ID_REGEX,
    RegisteredEntity,
)


# =============================================================================
# Candidate Schema v7 DDL (Prototype Only — In-Memory / Temporary Tests)
# =============================================================================

SCHEMA_V7_STAGING_JOURNAL_DDL = """
CREATE TABLE IF NOT EXISTS staging_run_journal (
    run_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL DEFAULT 1,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    manifest_seal_hex TEXT NOT NULL,
    target_inventory_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_run_journal_status
    ON staging_run_journal (status, updated_at);
"""

HEX_SHA256_REGEX = re.compile(r"\A[0-9a-f]{64}\Z")


def apply_candidate_schema_v7(conn: sqlite3.Connection) -> None:
    """Explicitly apply candidate Schema v7 DDL to a hermetic test database.

    CRITICAL SAFETY BOUNDARY:
    - This function must be invoked ONLY in explicitly designed hermetic tests
      operating on in-memory (:memory:) or temporary file SQLite fixtures.
    - NEVER call this in production startup, automatic migration pipelines,
      or against operational databases.
    - Default CURRENT_SCHEMA_VERSION in efso_radar.repositories.schema remains 6.
    """
    conn.executescript(SCHEMA_V7_STAGING_JOURNAL_DDL)


# =============================================================================
# Domain Enums & Status Machine
# =============================================================================

class JournalStatus(str, Enum):
    """Legal lifecycle states for a staging run journal entry (ERD-017)."""
    INTENT_PENDING = "INTENT_PENDING"
    ACKED = "ACKED"
    MUTATING = "MUTATING"
    EFFECT_COMMITTED = "EFFECT_COMMITTED"
    CLEANUP_PENDING = "CLEANUP_PENDING"
    CLEANED_UP = "CLEANED_UP"
    ABORTED = "ABORTED"
    UNKNOWN = "UNKNOWN"


class ReadProvenance(str, Enum):
    """Provenance tracking for journal inspection queries."""
    UNVERIFIED_READ = "UNVERIFIED_READ"
    PRIMARY_CONSISTENT_READ = "PRIMARY_CONSISTENT_READ"


class JournalExecutionPattern(str, Enum):
    """Execution pattern for journal coordination."""
    OPTION_B_SEPARATED = "OPTION_B_SEPARATED"  # Separated 2-phase intent & effect
    OPTION_B_C_ATOMIC = "OPTION_B_C_ATOMIC"    # Single-transaction atomic batching


ACTIVE_JOURNAL_STATUSES: frozenset[JournalStatus] = frozenset({
    JournalStatus.INTENT_PENDING,
    JournalStatus.ACKED,
    JournalStatus.MUTATING,
    JournalStatus.EFFECT_COMMITTED,
    JournalStatus.CLEANUP_PENDING,
})

TERMINAL_JOURNAL_STATUSES: frozenset[JournalStatus] = frozenset({
    JournalStatus.CLEANED_UP,
    JournalStatus.ABORTED,
    JournalStatus.UNKNOWN,
})

LEGAL_TRANSITIONS: dict[JournalStatus, frozenset[JournalStatus]] = {
    JournalStatus.INTENT_PENDING: frozenset({
        JournalStatus.ACKED,
        JournalStatus.ABORTED,
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.ACKED: frozenset({
        JournalStatus.MUTATING,
        JournalStatus.ABORTED,
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.MUTATING: frozenset({
        JournalStatus.EFFECT_COMMITTED,
        JournalStatus.ABORTED,
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.EFFECT_COMMITTED: frozenset({
        JournalStatus.MUTATING,          # subsequent stage (e.g. LIVE_02 -> LIVE_03)
        JournalStatus.CLEANUP_PENDING,   # moving to cleanup (LIVE_08)
        JournalStatus.ABORTED,
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.CLEANUP_PENDING: frozenset({
        JournalStatus.CLEANED_UP,
        JournalStatus.ABORTED,
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.CLEANED_UP: frozenset(),   # terminal
    JournalStatus.ABORTED: frozenset(),      # terminal
    JournalStatus.UNKNOWN: frozenset(),      # terminal (requires operator reconciliation)
}


# =============================================================================
# Domain Exceptions
# =============================================================================

class JournalError(Exception):
    """Base error for staging ownership journal operations."""


class InvalidRunIdError(JournalError, ValueError):
    """The provided run_id does not conform to the canonical regex profile."""


class InvalidJournalTransitionError(JournalError, ValueError):
    """Attempted status transition is illegal under the ERD-017 state machine."""


class FencingConflictError(JournalError, PermissionError):
    """Monotonic fencing token mismatch or CAS condition failed."""


class StaleOwnerError(JournalError, PermissionError):
    """Acting runner identity does not match the registered journal owner."""


class ManifestTamperedError(JournalError, PermissionError):
    """Manifest seal hex or target inventory does not match the registered intent."""


class AmbiguousCommitError(JournalError, RuntimeError):
    """Transaction commit outcome was ambiguous; marked UNKNOWN and HALTED fail-closed."""


class ActiveWorkloadCollisionError(JournalError, RuntimeError):
    """Workload is actively leased or duplicate run_id already exists."""


class TargetInventoryViolationError(JournalError, PermissionError):
    """Mutation or cleanup attempted on an entity not present in target inventory."""


class UnauthorizedTakeoverError(JournalError, PermissionError):
    """Automated takeover attempted; only manual PO-approved operator takeover is allowed."""


# =============================================================================
# Models
# =============================================================================

class StagingJournalEntry(BaseModel):
    """Immutable representation of a staging_run_journal row."""
    model_config = {"extra": "forbid", "frozen": True}

    run_id: str
    owner_id: str
    fencing_token: int
    stage: str
    status: JournalStatus
    manifest_seal_hex: str
    target_inventory_json: str
    created_at: str
    updated_at: str
    last_error: Optional[str] = None
    read_provenance: ReadProvenance = ReadProvenance.PRIMARY_CONSISTENT_READ

    def get_registered_entities(self) -> list[dict[str, Any]]:
        """Parse and return the registered entity inventory."""
        return json.loads(self.target_inventory_json)

    def is_active(self) -> bool:
        """Check if this run is actively in-flight."""
        return self.status in ACTIVE_JOURNAL_STATUSES

    def is_terminal(self) -> bool:
        """Check if this run has reached a terminal state."""
        return self.status in TERMINAL_JOURNAL_STATUSES


def validate_status_transition(
    current_status: JournalStatus,
    target_status: JournalStatus,
    *,
    is_operator_override: bool = False,
) -> None:
    """Validate that a transition between journal statuses is legal.

    Raises InvalidJournalTransitionError if the transition is prohibited.
    """
    if is_operator_override:
        # Operator reconciliation can transition UNKNOWN or ABORTED states under PO GO
        return

    allowed = LEGAL_TRANSITIONS.get(current_status, frozenset())
    if target_status not in allowed:
        raise InvalidJournalTransitionError(
            f"Illegal journal status transition: '{current_status.value}' -> '{target_status.value}'. "
            f"Allowed target states: {[s.value for s in allowed]}."
        )


def canonical_inventory_json(
    entities: list[RegisteredEntity] | list[dict[str, Any]],
) -> str:
    """Produce a deterministic, canonical JSON string for target entity inventory."""
    raw_list: list[dict[str, Any]] = []
    for ent in entities:
        if isinstance(ent, RegisteredEntity):
            raw_list.append({
                "table": ent.table,
                "pk_column": ent.pk_column,
                "pk_value": ent.pk_value,
                "cleanup_dependency_order": ent.cleanup_dependency_order,
                "expected_attributes": ent.expected_attributes,
            })
        elif isinstance(ent, dict):
            raw_list.append({
                "table": str(ent["table"]),
                "pk_column": str(ent["pk_column"]),
                "pk_value": str(ent["pk_value"]),
                "cleanup_dependency_order": int(ent.get("cleanup_dependency_order", 1)),
                "expected_attributes": dict(ent.get("expected_attributes", {})),
            })
        else:
            raise TypeError(f"Unsupported entity type: {type(ent)}")

    # Sort deterministically by table then pk_value
    sorted_list = sorted(raw_list, key=lambda x: (x["table"], x["pk_value"]))
    return json.dumps(sorted_list, sort_keys=True, separators=(",", ":"))


# =============================================================================
# StagingOwnershipJournal Class
# =============================================================================

class StagingOwnershipJournal:
    """Offline staging ownership journal manager following ERD-017 (Option B + Option C).

    Operates strictly against local SQLite connections (in-memory or temp file).
    Completely decoupled from live Turso, live cloud endpoints, and production migrations.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        allow_unverified_read: bool = False,
    ) -> None:
        self._conn = conn
        self._allow_unverified_read = allow_unverified_read
        self._verify_table_exists()

    def _verify_table_exists(self) -> None:
        """Verify that candidate Schema v7 table exists in the test connection."""
        cursor = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='staging_run_journal'"
        )
        if not cursor.fetchone():
            raise JournalError(
                "Candidate Schema v7 table 'staging_run_journal' does not exist in this database. "
                "Invoke apply_candidate_schema_v7(conn) on this hermetic test connection first."
            )

    def _now_iso(self, now: Optional[datetime] = None) -> str:
        ref_now = now or datetime.now(timezone.utc)
        if ref_now.tzinfo is None:
            ref_now = ref_now.replace(tzinfo=timezone.utc)
        else:
            ref_now = ref_now.astimezone(timezone.utc)
        return ref_now.isoformat()

    def register_intent(
        self,
        *,
        run_id: str,
        owner_id: str,
        stage: str = "INTENT_RECORDING",
        manifest_seal_hex: str,
        target_inventory: list[RegisteredEntity] | list[dict[str, Any]],
        initial_fencing_token: int = 1,
        pattern: JournalExecutionPattern = JournalExecutionPattern.OPTION_B_SEPARATED,
        workload_key: Optional[str] = "staging_controlled_run",
        enforce_workload_lease: bool = True,
        now: Optional[datetime] = None,
    ) -> StagingJournalEntry:
        """Register ahead-of-time staging run ownership and target inventory.

        Multi-Layered Invariants Enforced:
        1. Validates canonical run_id format profile.
        2. Validates 64-character SHA-256 manifest seal hex.
        3. Enforces coarse-grained workload lease in run_locks (prevents distinct
           run IDs from targeting the same schedule/workload concurrently).
        4. Detects active in-flight runs in staging_run_journal for same workload.
        5. Enforces PK(run_id) uniqueness.
        6. Implements Option B (separated INTENT_PENDING -> ACKED) or Option B+C (ACKED).
        """
        # 1. Validate run_id regex
        if not isinstance(run_id, str) or not RUN_ID_REGEX.match(run_id):
            raise InvalidRunIdError(
                f"Invalid run_id '{run_id}'. Must match canonical profile {RUN_ID_REGEX.pattern}."
            )

        # 2. Validate manifest seal hex
        if not isinstance(manifest_seal_hex, str) or not HEX_SHA256_REGEX.match(manifest_seal_hex):
            raise ManifestTamperedError(
                f"Invalid manifest seal '{manifest_seal_hex}'. Must be a 64-character lowercase SHA-256 hex string."
            )

        # 3. Canonical inventory serialization
        inventory_json = canonical_inventory_json(target_inventory)
        now_iso = self._now_iso(now)

        # 4. Check coarse-grained workload lease in run_locks (if table exists)
        if enforce_workload_lease and workload_key:
            cursor = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='run_locks'"
            )
            if cursor.fetchone():
                lock_cur = self._conn.execute(
                    "SELECT worker_id, expires_at FROM run_locks WHERE lock_key = ?",
                    (workload_key,),
                )
                lock_row = lock_cur.fetchone()
                if lock_row:
                    leased_worker, expires_at = lock_row
                    if expires_at >= now_iso and leased_worker != owner_id:
                        raise ActiveWorkloadCollisionError(
                            f"Workload '{workload_key}' is actively leased to '{leased_worker}' "
                            f"until '{expires_at}'. Distinct run '{run_id}' cannot execute concurrently."
                        )

            # Also check for active in-flight runs in staging_run_journal
            active_cur = self._conn.execute(
                "SELECT run_id, stage, status FROM staging_run_journal WHERE status IN ("
                "'INTENT_PENDING', 'ACKED', 'MUTATING', 'EFFECT_COMMITTED', 'CLEANUP_PENDING')"
            )
            active_rows = active_cur.fetchall()
            for a_run, a_stage, a_status in active_rows:
                if a_run != run_id:
                    raise ActiveWorkloadCollisionError(
                        f"Another staging run '{a_run}' is currently active in stage '{a_stage}' "
                        f"with status '{a_status}'. Concurrent runs for workload are rejected."
                    )

        # 5. Check duplicate identical run_id
        dup_cur = self._conn.execute(
            "SELECT run_id FROM staging_run_journal WHERE run_id = ?",
            (run_id,),
        )
        if dup_cur.fetchone():
            raise ActiveWorkloadCollisionError(
                f"Duplicate identical run_id '{run_id}' already registered in journal."
            )

        # 6. Insert journal entry
        initial_status = (
            JournalStatus.INTENT_PENDING
            if pattern == JournalExecutionPattern.OPTION_B_SEPARATED
            else JournalStatus.ACKED
        )

        with self._conn:
            self._conn.execute(
                """
                INSERT INTO staging_run_journal (
                    run_id, owner_id, fencing_token, stage, status,
                    manifest_seal_hex, target_inventory_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    owner_id,
                    initial_fencing_token,
                    stage,
                    initial_status.value,
                    manifest_seal_hex,
                    inventory_json,
                    now_iso,
                    now_iso,
                ),
            )

        # In Option B separated pattern: transition INTENT_PENDING -> ACKED upon successful commit
        if pattern == JournalExecutionPattern.OPTION_B_SEPARATED:
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE staging_run_journal
                    SET status = ?, updated_at = ?
                    WHERE run_id = ? AND status = ?
                    """,
                    (
                        JournalStatus.ACKED.value,
                        now_iso,
                        run_id,
                        JournalStatus.INTENT_PENDING.value,
                    ),
                )
            final_status = JournalStatus.ACKED
        else:
            final_status = initial_status

        return StagingJournalEntry(
            run_id=run_id,
            owner_id=owner_id,
            fencing_token=initial_fencing_token,
            stage=stage,
            status=final_status,
            manifest_seal_hex=manifest_seal_hex,
            target_inventory_json=inventory_json,
            created_at=now_iso,
            updated_at=now_iso,
            last_error=None,
            read_provenance=ReadProvenance.PRIMARY_CONSISTENT_READ,
        )

    def execute_atomic_stage_mutation(
        self,
        *,
        run_id: str,
        owner_id: str,
        expected_fencing_token: int,
        stage: str,
        mutation_statements: list[tuple[str, tuple[Any, ...]]],
        manifest_seal_hex: str,
        simulate_ambiguous_commit: bool = False,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute Option B+C atomic batch: CAS fence check + entity mutations + effect confirmation.

        Enclosed entirely within a single atomic SQLite transaction.
        Enforces Fencing on the Effect: If token CAS fails, 0 rows are updated,
        the transaction is immediately rolled back, and mutations are NEVER applied.

        Returns the new incremented fencing token upon successful commit.
        """
        now_iso = self._now_iso(now)

        # 1. Read existing entry to validate preconditions before mutation
        entry = self.inspect_run(run_id)
        if not entry:
            raise JournalError(f"No journal entry found for run_id '{run_id}'.")

        if entry.owner_id != owner_id:
            raise StaleOwnerError(
                f"Ownership violation: acting runner '{owner_id}' does not match registered owner '{entry.owner_id}'."
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

        # 2. Validate that all mutation statements target entities registered in inventory
        registered_entities = entry.get_registered_entities()
        registered_tables = {e["table"] for e in registered_entities}
        registered_keys = {(e["table"], str(e["pk_value"])) for e in registered_entities}

        for sql, params in mutation_statements:
            sql_upper = sql.strip().upper()
            matched_table = None
            for tbl in registered_tables:
                if tbl.upper() in sql_upper:
                    matched_table = tbl
                    break
            if not matched_table:
                raise TargetInventoryViolationError(
                    f"Mutation targets unregistered table in SQL: '{sql}'."
                )
            # Verify primary key parameter is registered
            param_strs = {str(p) for p in params}
            matching_key = False
            for (t, pk) in registered_keys:
                if t == matched_table and pk in param_strs:
                    matching_key = True
                    break
            if not matching_key:
                raise TargetInventoryViolationError(
                    f"Mutation parameters {params} do not contain registered primary key for table '{matched_table}'."
                )

        new_fencing_token = expected_fencing_token + 1

        # 3. Execute atomic transaction (Option B+C)
        cursor = self._conn.cursor()
        try:
            self._conn.execute("BEGIN IMMEDIATE")

            # 3a. Step 1: Update journal fence & set MUTATING status
            cur = self._conn.execute(
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
                (
                    new_fencing_token,
                    stage,
                    JournalStatus.MUTATING.value,
                    now_iso,
                    run_id,
                    expected_fencing_token,
                    owner_id,
                ),
            )
            if cur.rowcount != 1:
                self._conn.execute("ROLLBACK")
                raise FencingConflictError(
                    f"Authoritative fence update failed for run '{run_id}'. "
                    f"Expected token {expected_fencing_token}, affected rows was {cur.rowcount}. "
                    "Another runner may have bumped the fence."
                )

            # 3b. Step 2: Execute entity mutations
            for sql, params in mutation_statements:
                self._conn.execute(sql, params)

            # 3c. Step 3: Confirm effect commitment in journal
            cur_confirm = self._conn.execute(
                """
                UPDATE staging_run_journal
                SET status = ?,
                    updated_at = ?
                WHERE run_id = ?
                  AND fencing_token = ?
                """,
                (
                    JournalStatus.EFFECT_COMMITTED.value,
                    now_iso,
                    run_id,
                    new_fencing_token,
                ),
            )
            if cur_confirm.rowcount != 1:
                self._conn.execute("ROLLBACK")
                raise FencingConflictError(
                    f"Journal effect confirmation failed for run '{run_id}'. Affected rows was {cur_confirm.rowcount}."
                )

            # 3d. Step 4: Commit or handle simulated ambiguous drop
            if simulate_ambiguous_commit:
                self._conn.execute("ROLLBACK")
                # Mark local journal entry as UNKNOWN in a separate transaction to record HALT
                with self._conn:
                    self._conn.execute(
                        """
                        UPDATE staging_run_journal
                        SET status = ?,
                            last_error = ?,
                            updated_at = ?
                        WHERE run_id = ?
                        """,
                        (
                            JournalStatus.UNKNOWN.value,
                            "LOCAL_OUTCOME_UNKNOWN: Simulated ambiguous commit timeout/drop.",
                            now_iso,
                            run_id,
                        ),
                    )
                raise AmbiguousCommitError(
                    f"Ambiguous commit encountered for run '{run_id}'. "
                    "Outcome is UNKNOWN; remote provider state is UNVERIFIED / ENTIRE_TX_UNCERTAIN. "
                    "Fail-closed HALT initiated. Blind retry is forbidden."
                )

            self._conn.execute("COMMIT")
        except Exception:
            # Rollback if an active transaction is open
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass  # already rolled back
            raise
        finally:
            cursor.close()

        return new_fencing_token

    def execute_conditional_cleanup(
        self,
        *,
        run_id: str,
        owner_id: str,
        expected_fencing_token: int,
        cleanup_statements: list[tuple[str, tuple[Any, ...], str, str]],  # (sql, params, table, pk_value)
        manifest_seal_hex: str,
        simulate_ambiguous_commit: bool = False,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute LIVE-08 parameterized conditional cleanup & 3-state audit under authoritative fence.

        Invariants Enforced:
        1. Caller must hold active owner identity and expected fencing token.
        2. Journal is transitioned to CLEANUP_PENDING with incremented fencing token.
        3. Each conditional deletion MUST affect exactly 1 row (verifying ownership attributes).
        4. If affected_rows != 1, cleanup HALTS, status is marked UNKNOWN, and records are preserved.
        5. Independent 3-state audit: transitions to CLEANED_UP if and only if all registered
           entities are verified absent (0 rows).
        6. If residual rows exist, status remains CLEANUP_PENDING and manifest/journal are preserved.
        """
        now_iso = self._now_iso(now)

        entry = self.inspect_run(run_id)
        if not entry:
            raise JournalError(f"No journal entry found for run_id '{run_id}'.")

        if entry.owner_id != owner_id:
            raise StaleOwnerError(
                f"Cleanup ownership violation: runner '{owner_id}' is not registered owner '{entry.owner_id}'."
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

        new_fencing_token = expected_fencing_token + 1

        # Step 1: Transition journal to CLEANUP_PENDING with CAS fence update
        with self._conn:
            cur = self._conn.execute(
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
                (
                    new_fencing_token,
                    JournalStatus.CLEANUP_PENDING.value,
                    now_iso,
                    run_id,
                    expected_fencing_token,
                    owner_id,
                ),
            )
            if cur.rowcount != 1:
                raise FencingConflictError(
                    f"Cleanup CAS fence update failed for run '{run_id}'. Token mismatch."
                )

        # Step 2: Execute each conditional deletion statement
        for sql, params, table, pk_value in cleanup_statements:
            with self._conn:
                del_cur = self._conn.execute(sql, params)
                affected = del_cur.rowcount
            if affected != 1:
                # Parameterized deletion affected unexpected rows (e.g. 0 rows or >1)
                err_msg = (
                    f"Conditional deletion on table '{table}' for PK '{pk_value}' affected "
                    f"{affected} rows (expected exactly 1). HALT."
                )
                with self._conn:
                    self._conn.execute(
                        """
                        UPDATE staging_run_journal
                        SET status = ?, last_error = ?, updated_at = ?
                        WHERE run_id = ?
                        """,
                        (JournalStatus.UNKNOWN.value, err_msg, now_iso, run_id),
                    )
                raise JournalError(err_msg)

        if simulate_ambiguous_commit:
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE staging_run_journal
                    SET status = ?, last_error = ?, updated_at = ?
                    WHERE run_id = ?
                    """,
                    (
                        JournalStatus.UNKNOWN.value,
                        "LOCAL_OUTCOME_UNKNOWN: Simulated ambiguous commit during cleanup.",
                        now_iso,
                        run_id,
                    ),
                )
            raise AmbiguousCommitError(
                f"Ambiguous commit during cleanup for run '{run_id}'. Preserving manifest and journal."
            )

        # Step 3: Independent 3-State Audit (CLEAN / RESIDUAL / UNKNOWN)
        registered_entities = entry.get_registered_entities()
        residual_found = False

        for ent in registered_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]
            cur_audit = self._conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE {pk_col} = ?",
                (pk_val,),
            )
            row = cur_audit.fetchone()
            if row and row[0] > 0:
                residual_found = True
                break

        if residual_found:
            # Audit failed: entities remain residual. Journal row is PRESERVED, status remains CLEANUP_PENDING
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE staging_run_journal
                    SET last_error = ?, updated_at = ?
                    WHERE run_id = ?
                    """,
                    ("AUDIT_RESIDUAL: One or more synthetic entities remained after cleanup.", now_iso, run_id),
                )
        else:
            # Audit confirmed strictly CLEAN: transition to CLEANED_UP
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE staging_run_journal
                    SET status = ?, updated_at = ?
                    WHERE run_id = ? AND fencing_token = ?
                    """,
                    (
                        JournalStatus.CLEANED_UP.value,
                        now_iso,
                        run_id,
                        new_fencing_token,
                    ),
                )

        return new_fencing_token

    def inspect_run(
        self,
        run_id: str,
        provenance: ReadProvenance = ReadProvenance.PRIMARY_CONSISTENT_READ,
    ) -> Optional[StagingJournalEntry]:
        """Scoped query for a single run_id (least-privilege read inspection).

        Records read provenance to distinguish primary-consistent reads from replica reads.
        """
        cursor = self._conn.execute(
            """
            SELECT run_id, owner_id, fencing_token, stage, status,
                   manifest_seal_hex, target_inventory_json, created_at, updated_at, last_error
            FROM staging_run_journal
            WHERE run_id = ?
            """,
            (run_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        return StagingJournalEntry(
            run_id=row[0],
            owner_id=row[1],
            fencing_token=row[2],
            stage=row[3],
            status=JournalStatus(row[4]),
            manifest_seal_hex=row[5],
            target_inventory_json=row[6],
            created_at=row[7],
            updated_at=row[8],
            last_error=row[9],
            read_provenance=provenance,
        )

    def list_unresolved_runs(
        self,
        provenance: ReadProvenance = ReadProvenance.PRIMARY_CONSISTENT_READ,
    ) -> list[StagingJournalEntry]:
        """Operator query scanning strictly for unresolved staging runs.

        Returns entries with status in (INTENT_PENDING, ACKED, MUTATING, CLEANUP_PENDING, UNKNOWN).
        """
        cursor = self._conn.execute(
            """
            SELECT run_id, owner_id, fencing_token, stage, status,
                   manifest_seal_hex, target_inventory_json, created_at, updated_at, last_error
            FROM staging_run_journal
            WHERE status IN ('INTENT_PENDING', 'ACKED', 'MUTATING', 'EFFECT_COMMITTED', 'CLEANUP_PENDING', 'UNKNOWN')
            ORDER BY updated_at ASC
            """
        )
        results: list[StagingJournalEntry] = []
        for row in cursor.fetchall():
            results.append(
                StagingJournalEntry(
                    run_id=row[0],
                    owner_id=row[1],
                    fencing_token=row[2],
                    stage=row[3],
                    status=JournalStatus(row[4]),
                    manifest_seal_hex=row[5],
                    target_inventory_json=row[6],
                    created_at=row[7],
                    updated_at=row[8],
                    last_error=row[9],
                    read_provenance=provenance,
                )
            )
        return results

    def abort_run(
        self,
        *,
        run_id: str,
        owner_id: str,
        expected_fencing_token: int,
        reason: str,
        now: Optional[datetime] = None,
    ) -> StagingJournalEntry:
        """Safely abort an in-flight run before or after non-mutating steps."""
        now_iso = self._now_iso(now)
        entry = self.inspect_run(run_id)
        if not entry:
            raise JournalError(f"Run '{run_id}' not found.")

        if entry.owner_id != owner_id:
            raise StaleOwnerError(
                f"Runner '{owner_id}' cannot abort run owned by '{entry.owner_id}'."
            )

        if entry.fencing_token != expected_fencing_token:
            raise FencingConflictError(
                f"Fencing token mismatch for abort on run '{run_id}'."
            )

        validate_status_transition(entry.status, JournalStatus.ABORTED)

        new_fencing_token = expected_fencing_token + 1
        with self._conn:
            cur = self._conn.execute(
                """
                UPDATE staging_run_journal
                SET fencing_token = ?,
                    status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE run_id = ?
                  AND fencing_token = ?
                  AND owner_id = ?
                """,
                (
                    new_fencing_token,
                    JournalStatus.ABORTED.value,
                    f"ABORTED: {reason}",
                    now_iso,
                    run_id,
                    expected_fencing_token,
                    owner_id,
                ),
            )
            if cur.rowcount != 1:
                raise FencingConflictError(f"Abort CAS failed for run '{run_id}'.")

        return self.inspect_run(run_id)  # type: ignore[return-value]

    def operator_reconcile_run(
        self,
        *,
        run_id: str,
        operator_id: str,
        action: Literal["ABORT", "FORCE_CLEANUP", "RETIRE"],
        reason: str,
        is_po_approved: bool = False,
        now: Optional[datetime] = None,
    ) -> StagingJournalEntry:
        """Authorized operator reconciliation for unverified, orphaned, or UNKNOWN runs.

        HUMAN OPERATOR GO REQUIRED:
        - Automated runners must NEVER invoke this method.
        - Requires explicit is_po_approved=True.
        - Monotonically increments fencing token to preempt delayed runners.
        - Records complete operator audit trail in last_error.
        """
        if not is_po_approved:
            raise UnauthorizedTakeoverError(
                "Operator reconciliation requires explicit Product Owner approval (is_po_approved=True). "
                "Automated takeover is strictly forbidden."
            )

        if not operator_id or not reason:
            raise ValueError("operator_id and reason are mandatory for audit logging.")

        now_iso = self._now_iso(now)
        entry = self.inspect_run(run_id)
        if not entry:
            raise JournalError(f"Run '{run_id}' not found.")

        target_status = (
            JournalStatus.ABORTED if action in ("ABORT", "RETIRE")
            else JournalStatus.CLEANED_UP
        )

        new_fencing_token = entry.fencing_token + 1
        audit_note = f"OPERATOR_RECONCILED [{action}] by '{operator_id}': {reason}"

        with self._conn:
            self._conn.execute(
                """
                UPDATE staging_run_journal
                SET fencing_token = ?,
                    status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE run_id = ?
                """,
                (
                    new_fencing_token,
                    target_status.value,
                    audit_note,
                    now_iso,
                    run_id,
                ),
            )

        return self.inspect_run(run_id)  # type: ignore[return-value]
