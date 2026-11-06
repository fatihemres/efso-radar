"""Offline staging ownership journal implementation for ERD-017 (Option B + Option C).

STRICTLY OFFLINE ENGINE FOR STAGING RUN OWNERSHIP AND RECOVERY.
- Operates only against in-memory (:memory:) or temporary file SQLite fixtures in hermetic tests.
- Zero network I/O, zero Turso/cloud connections, zero credentials or tokens.
- Absolute $0.00 / 0 TRY spending ceiling.
- Candidate Schema v7 DDL is prototyped here ONLY and must NEVER be added to
  automatic startup migrations or bump CURRENT_SCHEMA_VERSION in schema.py.
- Staging journal ONLY: Does NOT prove or implement Gemini project-wide quota
  reservations, G1 billing proof, Telegram at-most-once delivery, or G6 live readiness.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from efso_radar.staging.runner import (
    ALLOWED_TABLE_PK_MAP,
    RUN_ID_REGEX,
    PerRunOwnershipManifest,
    RegisteredEntity,
    create_default_manifest_entities,
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

# Strict pattern for allowlisted INSERT statements:
# Only simple parameterized INSERT INTO table (col1, col2, ...) VALUES (?, ?, ...)
_SAFE_INSERT_PATTERN = re.compile(
    r"\A\s*INSERT\s+INTO\s+([a-zA-Z0-9_]+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)\s*\Z",
    re.IGNORECASE,
)
_FORBIDDEN_SQL_KEYWORDS = re.compile(
    r"\b(WHERE|UPDATE|DELETE|DROP|ALTER|SELECT|UNION|OR|AND|HAVING|GROUP|ORDER|EXEC|PRAGMA)\b",
    re.IGNORECASE,
)


def apply_candidate_schema_v7(conn: sqlite3.Connection) -> None:
    """Explicitly apply candidate Schema v7 DDL to a hermetic test database.

    CRITICAL SAFETY BOUNDARY:
    - This function must be invoked ONLY in explicitly designed hermetic tests
      operating on in-memory (:memory:) or temporary file SQLite fixtures.
    - Enforces runtime isolation: rejects any connection pointing to an
      operational database path or file outside the OS temporary directory.
    - Default CURRENT_SCHEMA_VERSION in efso_radar.repositories.schema remains 6.
    """
    cursor = conn.execute("PRAGMA database_list")
    rows = cursor.fetchall()
    for row in rows:
        db_file = row[2]
        if db_file and db_file != "":
            resolved_file = Path(db_file).resolve()
            temp_dir = Path(tempfile.gettempdir()).resolve()
            try:
                resolved_file.relative_to(temp_dir)
            except ValueError:
                raise PermissionError(
                    f"Security violation: apply_candidate_schema_v7 is restricted strictly "
                    f"to in-memory (:memory:) or temporary test directory fixtures. "
                    f"Attempted to apply to '{resolved_file}'."
                )
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
    OFFLINE_LOCAL_FIXTURE_READ = "OFFLINE_LOCAL_FIXTURE_READ"


class JournalExecutionPattern(str, Enum):
    """Execution pattern for journal coordination."""
    OPTION_B_SEPARATED = "OPTION_B_SEPARATED"  # Separated 2-phase intent & effect
    OPTION_B_C_ATOMIC = "OPTION_B_C_ATOMIC"    # Single-transaction atomic batching


class AmbiguousCommitOutcome(str, Enum):
    """Possible remote outcomes when a COMMIT ACK is lost or times out."""
    COMMITTED_BUT_ACK_LOST = "COMMITTED_BUT_ACK_LOST"
    ROLLED_BACK_AND_ACK_LOST = "ROLLED_BACK_AND_ACK_LOST"


ACTIVE_JOURNAL_STATUSES: frozenset[JournalStatus] = frozenset({
    JournalStatus.INTENT_PENDING,
    JournalStatus.ACKED,
    JournalStatus.MUTATING,
    JournalStatus.EFFECT_COMMITTED,
    JournalStatus.CLEANUP_PENDING,
})

UNRESOLVED_JOURNAL_STATUSES: frozenset[JournalStatus] = frozenset({
    JournalStatus.INTENT_PENDING,
    JournalStatus.ACKED,
    JournalStatus.MUTATING,
    JournalStatus.EFFECT_COMMITTED,
    JournalStatus.CLEANUP_PENDING,
    JournalStatus.UNKNOWN,
})

TERMINAL_JOURNAL_STATUSES: frozenset[JournalStatus] = frozenset({
    JournalStatus.CLEANED_UP,
    JournalStatus.ABORTED,
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
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.EFFECT_COMMITTED: frozenset({
        JournalStatus.MUTATING,          # subsequent stage (e.g. LIVE_02 -> LIVE_03)
        JournalStatus.CLEANUP_PENDING,   # moving to cleanup (LIVE_08)
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.CLEANUP_PENDING: frozenset({
        JournalStatus.CLEANED_UP,
        JournalStatus.UNKNOWN,
    }),
    JournalStatus.CLEANED_UP: frozenset(),   # terminal
    JournalStatus.ABORTED: frozenset(),      # terminal
    JournalStatus.UNKNOWN: frozenset(),      # terminal (requires operator forensic review)
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
    """Workload is actively leased or an unresolved run already exists."""


class TargetInventoryViolationError(JournalError, PermissionError):
    """Mutation or cleanup attempted on an entity not present in target inventory."""


class UnauthorizedTakeoverError(JournalError, PermissionError):
    """Automated takeover attempted; caller cannot self-certify PO authorization."""


class JournalHaltedError(JournalError, RuntimeError):
    """Operation attempted on a locally halted run in LOCAL_OUTCOME_UNKNOWN."""


def compute_authenticated_manifest_seal(
    run_id: str,
    entities: Optional[list[RegisteredEntity] | list[dict[str, Any]]] = None,
    created_at: str = "2026-11-06T20:00:00Z",
) -> str:
    """Compute the authentic SHA-256 seal for the canonical PerRunOwnershipManifest.

    Binds the run_id, canonical timestamp, version 1, and the exact canonical
    five synthetic entities to form the authentic cryptographic digest.
    """
    if entities is None:
        canonical_entities = create_default_manifest_entities(run_id)
    else:
        canonical_entities = []
        for e in entities:
            if isinstance(e, RegisteredEntity):
                canonical_entities.append(e)
            elif isinstance(e, dict):
                canonical_entities.append(RegisteredEntity.model_validate(e))
            else:
                raise ValueError(f"Unsupported entity type: {type(e)}")

    manifest = PerRunOwnershipManifest(
        run_id=run_id,
        created_at=created_at,
        version=1,
        registered_entities=canonical_entities,
    )
    content = manifest.model_dump_json(indent=2).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


# =============================================================================
# Models
# =============================================================================

class StageMutationCommand(BaseModel):
    """Structured mutation command strictly bounded to a registered target entity."""
    model_config = {"extra": "forbid", "frozen": True}

    table: str
    pk_column: str
    pk_value: str
    columns: list[str]
    values: list[Any]

    def build_parameterized_sql(self) -> tuple[str, tuple[Any, ...]]:
        col_names = ", ".join(self.columns)
        placeholders = ", ".join("?" for _ in self.columns)
        sql = f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders})"
        return sql, tuple(self.values)


class OperatorReconciliationProposal(BaseModel):
    """Non-authorizing offline review proposal for an unresolved staging run."""
    model_config = {"extra": "forbid", "frozen": True}

    run_id: str
    operator_id: str
    proposed_action: str
    reason: str
    status: str = "PROPOSAL_ONLY_NON_AUTHORIZING"
    is_authorized: bool = False
    notes: str = (
        "Non-authorizing offline review proposal only. Trusted independent "
        "Product Owner grant validation is unavailable in this offline slice. "
        "Database is NOT modified."
    )


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
    read_provenance: ReadProvenance = ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ

    def get_registered_entities(self) -> list[dict[str, Any]]:
        """Parse and return the registered entity inventory."""
        return json.loads(self.target_inventory_json)

    def is_active(self) -> bool:
        """Check if this run is actively in-flight."""
        return self.status in ACTIVE_JOURNAL_STATUSES

    def is_unresolved(self) -> bool:
        """Check if this run is unresolved (including UNKNOWN)."""
        return self.status in UNRESOLVED_JOURNAL_STATUSES

    def is_terminal(self) -> bool:
        """Check if this run has reached a terminal state."""
        return self.status in TERMINAL_JOURNAL_STATUSES


def validate_status_transition(
    current_status: JournalStatus,
    target_status: JournalStatus,
) -> None:
    """Validate that a transition between journal statuses is legal.

    Raises InvalidJournalTransitionError if the transition is prohibited.
    """
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


def validate_and_normalize_mutation(
    stmt: StageMutationCommand | tuple[str, tuple[Any, ...]],
    registered_entities: list[dict[str, Any]],
) -> tuple[str, tuple[Any, ...]]:
    """Strictly validate that a mutation targets an exact registered entity.

    Prevents arbitrary SQL injection, broad clauses (e.g. OR 1=1), foreign row mutations,
    and expression bypasses in VALUES clauses.
    """
    if isinstance(stmt, StageMutationCommand):
        # 1. Validate table identifier
        if not re.match(r"\A[a-zA-Z0-9_]+\Z", stmt.table):
            raise TargetInventoryViolationError(f"Invalid table identifier: '{stmt.table}'.")
        if stmt.table not in ALLOWED_TABLE_PK_MAP:
            raise TargetInventoryViolationError(
                f"Unauthorized mutation table '{stmt.table}'. Allowed: {sorted(ALLOWED_TABLE_PK_MAP.keys())}."
            )

        # 2. Validate columns and values
        if len(stmt.columns) != len(stmt.values) or not stmt.columns:
            raise TargetInventoryViolationError(
                f"Column and value length mismatch in StageMutationCommand: {len(stmt.columns)} cols vs {len(stmt.values)} vals."
            )
        for col in stmt.columns:
            if not re.match(r"\A[a-zA-Z0-9_]+\Z", col):
                raise TargetInventoryViolationError(f"Invalid column identifier: '{col}'.")

        # 3. Check PK column matches table definition
        expected_pk_col = ALLOWED_TABLE_PK_MAP[stmt.table]
        if stmt.pk_column != expected_pk_col:
            raise TargetInventoryViolationError(
                f"PK column mismatch for table '{stmt.table}': expected '{expected_pk_col}', got '{stmt.pk_column}'."
            )
        if stmt.pk_column not in stmt.columns:
            raise TargetInventoryViolationError(
                f"Mutation columns for '{stmt.table}' do not include primary key column '{stmt.pk_column}'."
            )

        col_val_map = dict(zip(stmt.columns, stmt.values))

        # 4. Enforce that bound value for PK column strictly equals declared pk_value
        bound_pk_val = str(col_val_map[stmt.pk_column])
        if bound_pk_val != str(stmt.pk_value):
            raise TargetInventoryViolationError(
                f"Bound value '{bound_pk_val}' for PK column '{stmt.pk_column}' does not match "
                f"declared pk_value '{stmt.pk_value}' in StageMutationCommand."
            )

        # 5. Find matching registered entity
        matching = [
            e for e in registered_entities
            if e["table"] == stmt.table and str(e["pk_value"]) == str(stmt.pk_value)
        ]
        if not matching:
            raise TargetInventoryViolationError(
                f"Mutation command targets unregistered entity: table '{stmt.table}', PK '{stmt.pk_value}'."
            )
        ent = matching[0]

        # 6. Mandatory expected-attributes check: ALL must be present and match
        expected_attrs = ent.get("expected_attributes", {})
        for attr_k, exp_val in expected_attrs.items():
            col_name = "worker_id" if (stmt.table == "run_locks" and attr_k == "worker_ids") else attr_k
            if col_name not in col_val_map:
                raise TargetInventoryViolationError(
                    f"Mandatory expected attribute '{col_name}' is missing from mutation columns for entity '{stmt.table}:{stmt.pk_value}'."
                )
            actual_val = col_val_map[col_name]
            if isinstance(exp_val, list):
                if actual_val not in exp_val:
                    raise TargetInventoryViolationError(
                        f"Attribute '{col_name}' value '{actual_val}' not in expected list {exp_val}."
                    )
            elif actual_val != exp_val:
                raise TargetInventoryViolationError(
                    f"Attribute '{col_name}' value '{actual_val}' does not match expected '{exp_val}'."
                )

        return stmt.build_parameterized_sql()

    elif isinstance(stmt, (tuple, list)) and len(stmt) == 2:
        sql, params = stmt
        if not isinstance(sql, str) or not isinstance(params, (tuple, list)):
            raise TargetInventoryViolationError("Invalid mutation tuple format.")

        # Disallow dangerous SQL keywords and multiple statements
        if ";" in sql or "--" in sql or "/*" in sql:
            raise TargetInventoryViolationError("Semicolons and comments are strictly disallowed in mutation SQL.")
        if _FORBIDDEN_SQL_KEYWORDS.search(sql):
            raise TargetInventoryViolationError(
                f"Disallowed SQL keyword detected in mutation SQL: '{sql}'."
            )

        match = _SAFE_INSERT_PATTERN.match(sql)
        if not match:
            raise TargetInventoryViolationError(
                f"Mutation SQL does not match allowlisted INSERT INTO pattern: '{sql}'."
            )

        tbl = match.group(1).strip()
        if not re.match(r"\A[a-zA-Z0-9_]+\Z", tbl):
            raise TargetInventoryViolationError(f"Invalid table identifier in SQL: '{tbl}'.")
        if tbl not in ALLOWED_TABLE_PK_MAP:
            raise TargetInventoryViolationError(f"Unauthorized mutation table '{tbl}'.")

        cols = [c.strip() for c in match.group(2).split(",")]
        placeholders = [p.strip() for p in match.group(3).split(",")]

        if len(cols) != len(params) or len(placeholders) != len(params):
            raise TargetInventoryViolationError(
                f"Column/placeholder/parameter length mismatch in SQL: '{sql}'."
            )

        for col in cols:
            if not re.match(r"\A[a-zA-Z0-9_]+\Z", col):
                raise TargetInventoryViolationError(f"Invalid column identifier in SQL: '{col}'.")

        # CRITICAL P1 FIX: Every single placeholder expression MUST literally be '?'
        for p in placeholders:
            if p != "?":
                raise TargetInventoryViolationError(
                    f"Disallowed SQL expression '{p}' in VALUES clause: only literal '?' parameter placeholders are permitted."
                )

        col_val_map = dict(zip(cols, params))
        expected_pk_col = ALLOWED_TABLE_PK_MAP[tbl]
        if expected_pk_col not in col_val_map:
            raise TargetInventoryViolationError(
                f"Mutation SQL for '{tbl}' does not include primary key column '{expected_pk_col}'."
            )

        bound_pk_val = str(col_val_map[expected_pk_col])

        # Find matching registered entity
        matching = [
            e for e in registered_entities
            if e["table"] == tbl and str(e["pk_value"]) == bound_pk_val
        ]
        if not matching:
            raise TargetInventoryViolationError(
                f"Mutation targets unregistered table '{tbl}' or unregistered PK value '{bound_pk_val}'."
            )
        ent = matching[0]

        # Mandatory expected-attributes check: ALL must be present and match
        expected_attrs = ent.get("expected_attributes", {})
        for attr_k, exp_val in expected_attrs.items():
            col_name = "worker_id" if (tbl == "run_locks" and attr_k == "worker_ids") else attr_k
            if col_name not in col_val_map:
                raise TargetInventoryViolationError(
                    f"Mandatory expected attribute '{col_name}' is missing from mutation columns for entity '{tbl}:{bound_pk_val}'."
                )
            actual_val = col_val_map[col_name]
            if isinstance(exp_val, list):
                if actual_val not in exp_val:
                    raise TargetInventoryViolationError(
                        f"Attribute '{col_name}' value '{actual_val}' not in expected list {exp_val}."
                    )
            elif actual_val != exp_val:
                raise TargetInventoryViolationError(
                    f"Attribute '{col_name}' value '{actual_val}' does not match expected '{exp_val}'."
                )

        canonical_sql = f"INSERT INTO {tbl} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
        return canonical_sql, tuple(params)

    else:
        raise TargetInventoryViolationError(f"Unsupported mutation statement type: {type(stmt)}")


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
        self._halted_runs: set[str] = set()
        self._verify_table_exists()

    def is_halted(self, run_id: str) -> bool:
        """Check if run is in a local halted state (LOCAL_OUTCOME_UNKNOWN)."""
        return run_id in self._halted_runs

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
        workload_key: str = "staging_controlled_run",
        simulate_ambiguous_commit: Optional[AmbiguousCommitOutcome] = None,
        now: Optional[datetime] = None,
    ) -> StagingJournalEntry:
        """Register ahead-of-time staging run ownership and target inventory.

        Multi-Layered Invariants Enforced in a Single Atomic Transaction:
        1. Checks that run_id is not locally halted in LOCAL_OUTCOME_UNKNOWN.
        2. Validates canonical run_id format profile.
        3. Validates canonical manifest payload against PerRunOwnershipManifest.
        4. Authenticates manifest_seal_hex against canonical 5-entity manifest digest.
        5. Mandatory workload_key cannot be bypassed or omitted.
        6. Atomically acquires coarse-grained task lease in run_locks (fails if leased).
        7. Atomically checks staging_run_journal for ANY unresolved runs (including UNKNOWN).
           If an unresolved run exists, new registration is blocked fail-closed.
        8. Enforces PK(run_id) uniqueness.
        9. Encloses registration and lease acquisition inside a single BEGIN IMMEDIATE transaction.
        10. Supports simulating ambiguous commits (COMMITTED_BUT_ACK_LOST / ROLLED_BACK_AND_ACK_LOST)
            triggering local UNKNOWN/HALT without automated retries.
        """
        if run_id in self._halted_runs:
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

        if not isinstance(workload_key, str) or not workload_key.strip():
            raise ValueError("workload_key is mandatory and cannot be empty.")

        # P1-2: Canonical Manifest Validation & Authentication
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

        expected_seal = compute_authenticated_manifest_seal(run_id, validated_manifest.registered_entities)
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

        cursor = self._conn.cursor()
        try:
            self._conn.execute("BEGIN IMMEDIATE")

            # A. Check for ANY unresolved runs in staging_run_journal (including UNKNOWN)
            cur_unresolved = self._conn.execute(
                """
                SELECT run_id, stage, status
                FROM staging_run_journal
                WHERE status NOT IN ('CLEANED_UP', 'ABORTED')
                """
            )
            unresolved_rows = cur_unresolved.fetchall()
            for u_run, u_stage, u_status in unresolved_rows:
                if u_run != run_id:
                    self._conn.execute("ROLLBACK")
                    raise ActiveWorkloadCollisionError(
                        f"Cannot register intent: an unresolved staging run '{u_run}' exists "
                        f"with status '{u_status}' in stage '{u_stage}'. "
                        "Workload is locked until all prior runs reach CLEANED_UP or ABORTED."
                    )

            # B. Check and acquire lease in run_locks if table exists
            cur_tbl = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='run_locks'"
            )
            if cur_tbl.fetchone():
                cur_lock = self._conn.execute(
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
                    (workload_key, owner_id, now_iso, lease_expires_iso, now_iso, now_iso, owner_id),
                )
                if cur_lock.rowcount == 0:
                    self._conn.execute("ROLLBACK")
                    raise ActiveWorkloadCollisionError(
                        f"Workload '{workload_key}' is actively leased to another worker. "
                        f"Distinct run '{run_id}' cannot execute concurrently."
                    )

            # C. Check duplicate identical run_id
            cur_dup = self._conn.execute(
                "SELECT run_id FROM staging_run_journal WHERE run_id = ?",
                (run_id,),
            )
            if cur_dup.fetchone():
                self._conn.execute("ROLLBACK")
                raise ActiveWorkloadCollisionError(
                    f"Duplicate identical run_id '{run_id}' already registered in journal."
                )

            # D. Insert journal entry
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

            # E. In Option B separated pattern: update INTENT_PENDING -> ACKED
            if pattern == JournalExecutionPattern.OPTION_B_SEPARATED:
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

            # Handle simulated ambiguous commit during intent registration
            if simulate_ambiguous_commit == AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST:
                self._conn.execute("COMMIT")
                self._halted_runs.add(run_id)
                raise AmbiguousCommitError(
                    f"Simulated network drop during intent COMMIT for run '{run_id}'. "
                    "Transaction committed on provider (COMMITTED_BUT_ACK_LOST), but client outcome is LOCAL_OUTCOME_UNKNOWN. "
                    "Run is locally HALTED. Automated retries or mutations are prohibited."
                )
            elif simulate_ambiguous_commit == AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST:
                self._conn.execute("ROLLBACK")
                self._halted_runs.add(run_id)
                raise AmbiguousCommitError(
                    f"Simulated network drop during intent COMMIT for run '{run_id}'. "
                    "Transaction rolled back on provider (ROLLED_BACK_AND_ACK_LOST), client outcome is LOCAL_OUTCOME_UNKNOWN. "
                    "Run is locally HALTED. Zero rows exist on provider. Automated retries or mutations are prohibited."
                )

            self._conn.execute("COMMIT")
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise
        finally:
            cursor.close()

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
            read_provenance=ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
        )

    def execute_atomic_stage_mutation(
        self,
        *,
        run_id: str,
        owner_id: str,
        expected_fencing_token: int,
        stage: str,
        mutation_statements: list[StageMutationCommand | tuple[str, tuple[Any, ...]]],
        manifest_seal_hex: str,
        simulate_ambiguous_commit: Optional[AmbiguousCommitOutcome] = None,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute Option B+C atomic batch: CAS fence check + entity mutations + effect confirmation.

        Enclosed entirely within a single atomic SQLite transaction.
        Enforces Fencing on the Effect: If token CAS fails, 0 rows are updated,
        the transaction is immediately rolled back, and mutations are NEVER applied.

        Returns the new incremented fencing token upon successful commit.
        """
        if run_id in self._halted_runs:
            raise JournalHaltedError(
                f"Run '{run_id}' is permanently halted in LOCAL_OUTCOME_UNKNOWN. "
                "Subsequent automated mutations, retries, cleanups, or aborts are prohibited."
            )

        now_iso = self._now_iso(now)

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

        registered_entities = entry.get_registered_entities()
        validated_sql_list: list[tuple[str, tuple[Any, ...]]] = []
        for m in mutation_statements:
            sql, params = validate_and_normalize_mutation(m, registered_entities)
            validated_sql_list.append((sql, params))

        new_fencing_token = expected_fencing_token + 1

        cursor = self._conn.cursor()
        try:
            self._conn.execute("BEGIN IMMEDIATE")

            # Step 1: Update journal fence & set MUTATING status
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

            # Step 2: Execute entity mutations
            for sql, params in validated_sql_list:
                self._conn.execute(sql, params)

            # Step 3: Confirm effect commitment in journal
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

            # Step 4: Handle simulated ambiguous commit
            if simulate_ambiguous_commit == AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST:
                self._conn.execute("COMMIT")
                self._halted_runs.add(run_id)
                raise AmbiguousCommitError(
                    f"Ambiguous commit encountered for run '{run_id}': transaction committed on provider, "
                    "but network ACK was lost. Client outcome is LOCAL_OUTCOME_UNKNOWN / HALT."
                )
            elif simulate_ambiguous_commit == AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST:
                self._conn.execute("ROLLBACK")
                self._halted_runs.add(run_id)
                raise AmbiguousCommitError(
                    f"Ambiguous commit encountered for run '{run_id}': transaction rolled back on provider, "
                    "and network ACK was lost. Client outcome is LOCAL_OUTCOME_UNKNOWN / HALT."
                )

            self._conn.execute("COMMIT")
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
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
        manifest_seal_hex: str,
        target_entities_subset: Optional[list[str]] = None,
        now: Optional[datetime] = None,
    ) -> int:
        """Execute LIVE-08 parameterized conditional cleanup & 3-state audit under authoritative fence.

        Strict Atomicity Enforced:
        1. Caller must hold active owner identity and expected fencing token.
        2. Deletions are NOT free-form SQL from caller: the journal constructs exact
           parameterized deletions from target_inventory_json using registered PK and expected attributes.
        3. Entire cleanup executes inside ONE atomic SQLite transaction:
           - CAS fence check & transition to CLEANUP_PENDING.
           - Exact conditional deletions for registered entities.
           - Complete 3-state audit confirming all registered entities are absent.
           - Transition journal status to CLEANED_UP.
        4. If ANY deletion affects unexpected rows, or if residual entities remain:
           the transaction is immediately rolled back! No partial destructive deletions persist.
        """
        if run_id in self._halted_runs:
            raise JournalHaltedError(
                f"Run '{run_id}' is permanently halted in LOCAL_OUTCOME_UNKNOWN. "
                "Subsequent automated mutations, retries, cleanups, or aborts are prohibited."
            )

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

        registered_entities = entry.get_registered_entities()
        sorted_entities = sorted(
            registered_entities,
            key=lambda e: int(e.get("cleanup_dependency_order", 1)),
            reverse=True,
        )

        new_fencing_token = expected_fencing_token + 1

        cursor = self._conn.cursor()
        try:
            self._conn.execute("BEGIN IMMEDIATE")

            # 1. CAS fence update to CLEANUP_PENDING
            cur_fence = self._conn.execute(
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
            if cur_fence.rowcount != 1:
                self._conn.execute("ROLLBACK")
                raise FencingConflictError(
                    f"Cleanup CAS fence update failed for run '{run_id}'. Token mismatch or foreign takeover."
                )

            # 2. Execute constructed, exact conditional deletions
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
                del_cur = self._conn.execute(del_sql, tuple(params))

                if del_cur.rowcount > 1:
                    self._conn.execute("ROLLBACK")
                    raise JournalError(
                        f"Conditional deletion on table '{tbl}' for PK '{pk_val}' affected "
                        f"{del_cur.rowcount} rows (>1). Aborting cleanup atomically."
                    )

            # 3. Independent 3-State Audit inside the transaction
            for ent in registered_entities:
                tbl = ent["table"]
                pk_col = ent["pk_column"]
                pk_val = ent["pk_value"]
                cur_audit = self._conn.execute(
                    f"SELECT COUNT(*) FROM {tbl} WHERE {pk_col} = ?",
                    (pk_val,),
                )
                cnt = cur_audit.fetchone()[0]
                if cnt > 0:
                    self._conn.execute("ROLLBACK")
                    raise JournalError(
                        f"Audit failed: residual entity '{tbl}:{pk_val}' remains present. "
                        "Aborting cleanup atomically; state preserved for investigation."
                    )

            # 4. Finalize journal status to CLEANED_UP
            self._conn.execute(
                """
                UPDATE staging_run_journal
                SET status = ?,
                    updated_at = ?
                WHERE run_id = ? AND fencing_token = ?
                """,
                (
                    JournalStatus.CLEANED_UP.value,
                    now_iso,
                    run_id,
                    new_fencing_token,
                ),
            )

            self._conn.execute("COMMIT")
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            raise
        finally:
            cursor.close()

        return new_fencing_token

    def inspect_run(
        self,
        run_id: str,
        provenance: ReadProvenance = ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
    ) -> Optional[StagingJournalEntry]:
        """Scoped query for a single run_id (least-privilege read inspection).

        Records read provenance to distinguish local fixture reads from remote replica reads.
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
        provenance: ReadProvenance = ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ,
    ) -> list[StagingJournalEntry]:
        """Operator query scanning strictly for unresolved staging runs.

        Returns entries with status in UNRESOLVED_JOURNAL_STATUSES.
        """
        cursor = self._conn.execute(
            """
            SELECT run_id, owner_id, fencing_token, stage, status,
                   manifest_seal_hex, target_inventory_json, created_at, updated_at, last_error
            FROM staging_run_journal
            WHERE status NOT IN ('CLEANED_UP', 'ABORTED')
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
        """Safely abort an in-flight run BEFORE mutations are dispatched.

        Strict Safety Guarantees:
        - Only permitted when status is INTENT_PENDING or ACKED.
        - Once mutations have been initiated (MUTATING, EFFECT_COMMITTED, CLEANUP_PENDING),
          abort is strictly prohibited to avoid stranding committed effects.
        - Verifies that zero registered entities exist in the database before aborting.
        """
        if run_id in self._halted_runs:
            raise JournalHaltedError(
                f"Run '{run_id}' is permanently halted in LOCAL_OUTCOME_UNKNOWN. "
                "Subsequent automated mutations, retries, cleanups, or aborts are prohibited."
            )

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

        # Prohibit aborting a run that has already initiated mutations
        if entry.status not in (JournalStatus.INTENT_PENDING, JournalStatus.ACKED):
            raise InvalidJournalTransitionError(
                f"Cannot abort run '{run_id}' in state '{entry.status.value}'. "
                "Mutations may have been dispatched. Must execute conditional cleanup "
                "and verify clean audit before retirement."
            )

        # Verify that zero registered entities exist in database
        registered_entities = entry.get_registered_entities()
        for ent in registered_entities:
            tbl = ent["table"]
            pk_col = ent["pk_column"]
            pk_val = ent["pk_value"]
            cur_check = self._conn.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE {pk_col} = ?",
                (pk_val,),
            )
            cnt = cur_check.fetchone()[0]
            if cnt > 0:
                raise JournalError(
                    f"Cannot abort run '{run_id}': registered entity '{tbl}:{pk_val}' "
                    "already exists in database. Abort is rejected to prevent stranding residual data."
                )

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

    def propose_operator_reconciliation(
        self,
        *,
        run_id: str,
        operator_id: str,
        proposed_action: Literal["ABORT", "FORCE_CLEANUP", "RETIRE"],
        reason: str,
    ) -> OperatorReconciliationProposal:
        """Construct a strictly non-authorizing offline review proposal for an unresolved run.

        SAFETY BARRIER:
        - This method is strictly read-only and NEVER modifies the database.
        - Does NOT transition the status to CLEANED_UP or self-certify PO approval.
        - Exposes a review proposal only, pending future trusted authenticated PO authority.
        """
        if not operator_id or not reason:
            raise ValueError("operator_id and reason are mandatory for review proposal.")

        entry = self.inspect_run(run_id)
        if not entry:
            raise JournalError(f"Run '{run_id}' not found.")

        return OperatorReconciliationProposal(
            run_id=run_id,
            operator_id=operator_id,
            proposed_action=proposed_action,
            reason=reason,
            status="PROPOSAL_ONLY_NON_AUTHORIZING",
            is_authorized=False,
        )

    def operator_reconcile_run(
        self,
        *,
        run_id: str,
        operator_id: str,
        action: str,
        reason: str,
        is_po_approved: bool = False,
    ) -> Any:
        """Deprecated / disabled write-capable override.

        Fail-closed barrier: caller-supplied boolean flags cannot self-certify PO authorization.
        Always raises UnauthorizedTakeoverError.
        """
        raise UnauthorizedTakeoverError(
            "Caller-supplied is_po_approved=True cannot certify runtime authority. "
            "Trusted independent Product Owner grant validation is unavailable in this offline slice. "
            "Use propose_operator_reconciliation() for a non-authorizing review proposal."
        )
