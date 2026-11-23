"""Strictly read-only offline schema phase gate and inert mock operator handoff (B1).

GOVERNANCE AND SAFETY BOUNDARY:
- Operates ONLY against in-memory (:memory:) or OS-temp sqlite3.Connection fixtures.
- Zero network imports, zero HTTP clients, zero Turso/cloud connections, zero credentials.
- Zero DDL/DML on the inspected connection, zero operational/provider DDL, and zero
  schema_migrations/CURRENT_SCHEMA_VERSION bump (only initializes a private ephemeral
  :memory: reference fixture once to derive canonical Schema v6 metadata).
- Positive mock eligibility is produced ONLY by inspecting an exact hermetic
  sqlite3.Connection; caller-constructed Pydantic classifications never authenticate provenance.
- Never applies candidate Schema v7 DDL or confers live authorization; remote_state is always UNKNOWN.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import tempfile
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ValidationError, model_validator

from efso_radar.repositories.schema import (
    CURRENT_SCHEMA_VERSION,
    MIGRATIONS,
    get_all_init_statements,
)
from efso_radar.staging.journal import (
    EXPECTED_JOURNAL_COLUMNS,
    EXPECTED_JOURNAL_INDEX_NAME,
    verify_staging_journal_schema,
)
from efso_radar.staging.preflight import (
    SCHEMA_V6_COLUMN_TYPES,
    SCHEMA_V6_CRITICAL_UNIQUE_CONSTRAINTS,
    SCHEMA_V6_INDEXES,
    SCHEMA_V6_TABLES,
)


_FORBIDDEN_V6_DDL_CLAUSES = re.compile(
    r"\b(ON\s+CONFLICT|CHECK|WITHOUT\s+ROWID|AUTOINCREMENT)\b",
    re.IGNORECASE,
)
_AUTOINDEX_NAME_RE = re.compile(r"\Asqlite_autoindex_[a-z0-9_]+_[0-9]+\Z")
_HEX_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_CANDIDATE_JOURNAL_TABLE = "staging_run_journal"

__all__ = (
    "ManualNextGate",
    "MockOperatorHandoff",
    "MockTestEligibility",
    "PhaseGateReasonCode",
    "SchemaPhase",
    "SchemaPhaseClassification",
    "build_mock_operator_handoff",
    "classify_offline_schema_phase",
    "evaluate_offline_schema_phase_gate",
)


class _StrictFrozenModel(BaseModel):
    model_config = {
        "extra": "forbid",
        "frozen": True,
        "strict": True,
        "revalidate_instances": "always",
    }


class SchemaPhase(str, Enum):
    V6_BASELINE_OFFLINE = "V6_BASELINE_OFFLINE"
    V6_WITH_CANDIDATE_JOURNAL_OFFLINE = "V6_WITH_CANDIDATE_JOURNAL_OFFLINE"
    DENY = "DENY"
    UNKNOWN = "UNKNOWN"


class MockTestEligibility(str, Enum):
    LEGACY_V6_MOCK_LIVE01_AND_RUNNER_ONLY = "LEGACY_V6_MOCK_LIVE01_AND_RUNNER_ONLY"
    CANDIDATE_JOURNAL_MOCK_SEAM_ONLY = "CANDIDATE_JOURNAL_MOCK_SEAM_ONLY"
    NONE_DENIED = "NONE_DENIED"
    NONE_UNKNOWN = "NONE_UNKNOWN"


class ManualNextGate(str, Enum):
    SEPARATE_PO_B2_COST_AND_B3_LIVE_READ_GO_REQUIRED = (
        "SEPARATE_PO_B2_COST_AND_B3_LIVE_READ_GO_REQUIRED"
    )
    SEPARATE_PO_REVIEW_AND_FUTURE_B4_MIGRATION_GO_REQUIRED = (
        "SEPARATE_PO_REVIEW_AND_FUTURE_B4_MIGRATION_GO_REQUIRED"
    )
    STOP_MANUAL_SCHEMA_INVESTIGATION_REQUIRED = "STOP_MANUAL_SCHEMA_INVESTIGATION_REQUIRED"
    STOP_MANUAL_EVIDENCE_VERIFICATION_REQUIRED = "STOP_MANUAL_EVIDENCE_VERIFICATION_REQUIRED"


class PhaseGateReasonCode(str, Enum):
    ATTACHED_DATABASE_DISALLOWED = "ATTACHED_DATABASE_DISALLOWED"
    INSPECTION_READ_ERROR = "INSPECTION_READ_ERROR"
    NON_MUTATING_FINGERPRINT_MISMATCH = "NON_MUTATING_FINGERPRINT_MISMATCH"
    UNOBSERVED_OR_MISSING_EVIDENCE = "UNOBSERVED_OR_MISSING_EVIDENCE"
    UNTRUSTED_OR_REMOTE_CLAIMED_EVIDENCE = "UNTRUSTED_OR_REMOTE_CLAIMED_EVIDENCE"
    SCHEMA_MIGRATIONS_TABLE_MISSING = "SCHEMA_MIGRATIONS_TABLE_MISSING"
    SCHEMA_VERSION_MISSING = "SCHEMA_VERSION_MISSING"
    SCHEMA_VERSION_OUTDATED = "SCHEMA_VERSION_OUTDATED"
    UNAPPROVED_FUTURE_SCHEMA_VERSION = "UNAPPROVED_FUTURE_SCHEMA_VERSION"
    UNEXPECTED_SCHEMA_MIGRATION_HISTORY = "UNEXPECTED_SCHEMA_MIGRATION_HISTORY"
    MALFORMED_SCHEMA_MIGRATION_ROW = "MALFORMED_SCHEMA_MIGRATION_ROW"
    MISSING_V6_TABLE = "MISSING_V6_TABLE"
    UNEXPECTED_TABLE_OBJECT = "UNEXPECTED_TABLE_OBJECT"
    UNEXPECTED_VIEW_OBJECT = "UNEXPECTED_VIEW_OBJECT"
    UNEXPECTED_TRIGGER_OBJECT = "UNEXPECTED_TRIGGER_OBJECT"
    UNEXPECTED_VIRTUAL_TABLE = "UNEXPECTED_VIRTUAL_TABLE"
    UNEXPECTED_INTERNAL_SCHEMA_OBJECT = "UNEXPECTED_INTERNAL_SCHEMA_OBJECT"
    V6_TABLE_DDL_MISMATCH = "V6_TABLE_DDL_MISMATCH"
    V6_COLUMN_COUNT_MISMATCH = "V6_COLUMN_COUNT_MISMATCH"
    V6_COLUMN_METADATA_MISMATCH = "V6_COLUMN_METADATA_MISMATCH"
    V6_FOREIGN_KEY_MISMATCH = "V6_FOREIGN_KEY_MISMATCH"
    MISSING_V6_INDEX = "MISSING_V6_INDEX"
    UNEXPECTED_INDEX_OBJECT = "UNEXPECTED_INDEX_OBJECT"
    V6_INDEX_DDL_MISMATCH = "V6_INDEX_DDL_MISMATCH"
    V6_INDEX_METADATA_MISMATCH = "V6_INDEX_METADATA_MISMATCH"
    V6_UNIQUE_CONSTRAINT_MISMATCH = "V6_UNIQUE_CONSTRAINT_MISMATCH"
    TEMP_TRIGGER_DISALLOWED = "TEMP_TRIGGER_DISALLOWED"
    TEMP_SHADOW_OBJECT_DISALLOWED = "TEMP_SHADOW_OBJECT_DISALLOWED"
    UNEXPECTED_TEMP_OBJECT = "UNEXPECTED_TEMP_OBJECT"
    UNEXPECTED_JOURNAL_PRESENT = "UNEXPECTED_JOURNAL_PRESENT"
    PARTIAL_OR_ORPHAN_JOURNAL_INDEX = "PARTIAL_OR_ORPHAN_JOURNAL_INDEX"
    CANDIDATE_JOURNAL_SCHEMA_INVALID = "CANDIDATE_JOURNAL_SCHEMA_INVALID"


_NOTES_V6_BASELINE: tuple[str, ...] = (
    "Offline fixture only: matches exact canonical Schema v6 baseline without staging_run_journal.",
    "Valid only for legacy/mock LIVE-01 and v6 mock runner fixtures; NOT live authorization.",
    "Remote Turso state remains UNKNOWN; separate B2/B3 PO approvals are required before any remote read.",
)

_NOTES_V6_WITH_CANDIDATE_JOURNAL: tuple[str, ...] = (
    "Offline fixture only: matches Schema v6 baseline plus test-only candidate staging_run_journal.",
    "Candidate journal is NOT an approved or deployed operational Schema v7 migration.",
    "Valid only for offline mock journal seam tests; NO automated takeover, retry, cleanup, or live GO.",
)

_NOTES_DENY: tuple[str, ...] = (
    "Schema phase gate DENIED due to structural drift, unexpected objects, or untrusted evidence.",
    "No automatic repair, CREATE IF NOT EXISTS, cleanup, retry, or takeover is permitted.",
)

_NOTES_UNKNOWN: tuple[str, ...] = (
    "Schema phase classification is UNKNOWN due to missing/unobserved evidence or inspection read error.",
    "STOP: manual offline schema evidence verification is required; no automated action permitted.",
)


_HANDOFF_PHASE_SPEC: dict[
    SchemaPhase,
    tuple[MockTestEligibility, bool, bool, ManualNextGate, tuple[str, ...]],
] = {
    SchemaPhase.V6_BASELINE_OFFLINE: (
        MockTestEligibility.LEGACY_V6_MOCK_LIVE01_AND_RUNNER_ONLY,
        True,
        False,
        ManualNextGate.SEPARATE_PO_B2_COST_AND_B3_LIVE_READ_GO_REQUIRED,
        _NOTES_V6_BASELINE,
    ),
    SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE: (
        MockTestEligibility.CANDIDATE_JOURNAL_MOCK_SEAM_ONLY,
        False,
        True,
        ManualNextGate.SEPARATE_PO_REVIEW_AND_FUTURE_B4_MIGRATION_GO_REQUIRED,
        _NOTES_V6_WITH_CANDIDATE_JOURNAL,
    ),
    SchemaPhase.DENY: (
        MockTestEligibility.NONE_DENIED,
        False,
        False,
        ManualNextGate.STOP_MANUAL_SCHEMA_INVESTIGATION_REQUIRED,
        _NOTES_DENY,
    ),
    SchemaPhase.UNKNOWN: (
        MockTestEligibility.NONE_UNKNOWN,
        False,
        False,
        ManualNextGate.STOP_MANUAL_EVIDENCE_VERIFICATION_REQUIRED,
        _NOTES_UNKNOWN,
    ),
}


class SchemaPhaseClassification(_StrictFrozenModel):
    """Structured offline fixture classification (field coherence only; not provenance authentication)."""

    phase: SchemaPhase
    evidence_provenance: Literal["OFFLINE_FIXTURE_ONLY"] = "OFFLINE_FIXTURE_ONLY"
    operational_schema_version_contract: Literal[6] = 6
    observed_schema_version: Optional[int] = None
    expected_v6_table_count: Literal[15] = 15
    observed_v6_tables_matched: int = Field(ge=0, le=15)
    expected_v6_column_count: Literal[129] = 129
    observed_v6_columns_matched: int = Field(ge=0, le=129)
    expected_v6_explicit_index_count: Literal[15] = 15
    observed_v6_explicit_indexes_matched: int = Field(ge=0, le=15)
    candidate_journal_present: bool
    candidate_journal_valid: bool
    candidate_journal_is_approved_v7_migration: Literal[False] = False
    schema_fingerprint_before: str
    schema_fingerprint_after: str
    content_fingerprint_before: str
    content_fingerprint_after: str
    non_mutating_verified: bool
    reason_codes: tuple[PhaseGateReasonCode, ...] = ()

    @model_validator(mode="after")
    def _validate_coherence(self) -> SchemaPhaseClassification:
        for fp in (
            self.schema_fingerprint_before,
            self.schema_fingerprint_after,
            self.content_fingerprint_before,
            self.content_fingerprint_after,
        ):
            if fp != "" and not _HEX_SHA256_RE.match(fp):
                raise ValueError("Fingerprints must be empty or lowercase 64-char hex SHA-256 digests.")

        expected_non_mutating = (
            bool(_HEX_SHA256_RE.match(self.schema_fingerprint_before))
            and self.schema_fingerprint_before == self.schema_fingerprint_after
            and bool(_HEX_SHA256_RE.match(self.content_fingerprint_before))
            and self.content_fingerprint_before == self.content_fingerprint_after
        )
        if self.non_mutating_verified != expected_non_mutating:
            raise ValueError("non_mutating_verified does not match fingerprint equality.")
        if self.candidate_journal_valid and not self.candidate_journal_present:
            raise ValueError("candidate_journal_valid cannot be True without candidate_journal_present.")

        if self.phase in (
            SchemaPhase.V6_BASELINE_OFFLINE,
            SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE,
        ):
            want_journal = self.phase == SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE
            if (
                self.observed_schema_version != CURRENT_SCHEMA_VERSION
                or self.observed_v6_tables_matched != self.expected_v6_table_count
                or self.observed_v6_columns_matched != self.expected_v6_column_count
                or self.observed_v6_explicit_indexes_matched != self.expected_v6_explicit_index_count
                or self.candidate_journal_present is not want_journal
                or self.candidate_journal_valid is not want_journal
                or not self.non_mutating_verified
                or len(self.reason_codes) != 0
            ):
                raise ValueError(f"Incoherent {self.phase.value} classification.")
        elif len(self.reason_codes) < 1:
            raise ValueError("DENY and UNKNOWN classifications require at least one reason code.")
        return self


class MockOperatorHandoff(_StrictFrozenModel):
    """Inert, non-authorizing mock operator handoff (remote_state is always UNKNOWN)."""

    phase: SchemaPhase
    evidence_provenance: Literal["OFFLINE_FIXTURE_ONLY"] = "OFFLINE_FIXTURE_ONLY"
    authorization_scope: Literal["NONE_OFFLINE_MOCK_ONLY"] = "NONE_OFFLINE_MOCK_ONLY"
    remote_state: Literal["UNKNOWN"] = "UNKNOWN"
    schema_v7_adoption_status: Literal["NOT_APPROVED_NOT_MIGRATED"] = "NOT_APPROVED_NOT_MIGRATED"
    automation_policy: Literal["NO_AUTO_RECOVERY_NO_AUTO_CLEANUP_NO_AUTO_RETRY"] = (
        "NO_AUTO_RECOVERY_NO_AUTO_CLEANUP_NO_AUTO_RETRY"
    )
    mock_test_eligibility: MockTestEligibility
    eligible_for_mock_v6_live01: bool
    eligible_for_mock_journal_seam: bool
    manual_next_gate: ManualNextGate
    observed_schema_version: Optional[int] = None
    schema_fingerprint_sha256: str = ""
    non_mutating_verified: bool = False
    reason_codes: tuple[PhaseGateReasonCode, ...] = ()
    notes: tuple[str, ...]

    @model_validator(mode="after")
    def _validate_handoff_coherence(self) -> MockOperatorHandoff:
        if self.schema_fingerprint_sha256 != "" and not _HEX_SHA256_RE.match(self.schema_fingerprint_sha256):
            raise ValueError("schema_fingerprint_sha256 must be empty or a lowercase 64-char hex SHA-256 digest.")

        exp_elig, exp_v6, exp_journal, exp_gate, exp_notes = _HANDOFF_PHASE_SPEC[self.phase]
        if (
            self.mock_test_eligibility != exp_elig
            or self.eligible_for_mock_v6_live01 is not exp_v6
            or self.eligible_for_mock_journal_seam is not exp_journal
            or self.manual_next_gate != exp_gate
            or self.notes != exp_notes
        ):
            raise ValueError(f"Incoherent {self.phase.value} handoff.")

        if self.phase in (
            SchemaPhase.V6_BASELINE_OFFLINE,
            SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE,
        ):
            if (
                self.observed_schema_version != CURRENT_SCHEMA_VERSION
                or not self.non_mutating_verified
                or not _HEX_SHA256_RE.match(self.schema_fingerprint_sha256)
                or len(self.reason_codes) != 0
            ):
                raise ValueError(f"Incoherent {self.phase.value} handoff.")
        elif len(self.reason_codes) < 1:
            raise ValueError(f"Incoherent {self.phase.value} handoff.")
        return self

    def summary(self) -> str:
        """Render a sanitized single-string operator handoff summary with fixed tokens only."""
        reasons = ",".join(code.value for code in self.reason_codes) if self.reason_codes else "NONE"
        ver = str(self.observed_schema_version) if self.observed_schema_version is not None else "UNKNOWN"
        fp = self.schema_fingerprint_sha256 if self.schema_fingerprint_sha256 else "UNAVAILABLE"
        return (
            f"SCHEMA_PHASE_GATE phase={self.phase.value} "
            f"| provenance={self.evidence_provenance} "
            f"| scope={self.authorization_scope} "
            f"| remote_state={self.remote_state} "
            f"| v7_status={self.schema_v7_adoption_status} "
            f"| automation={self.automation_policy} "
            f"| mock_eligibility={self.mock_test_eligibility.value} "
            f"| next_gate={self.manual_next_gate.value} "
            f"| schema_v={ver} "
            f"| non_mutating={self.non_mutating_verified} "
            f"| schema_fp={fp} "
            f"| reasons={reasons}"
        )


@dataclass(frozen=True)
class _CanonicalIndexSpec:
    ddl: str
    unique: int
    origin: str
    partial: int
    keys: tuple[tuple[int, int, str, int, str, int], ...]


@dataclass(frozen=True)
class _CanonicalV6Profile:
    tables: frozenset[str]
    table_ddls: dict[str, str]
    table_columns: dict[str, tuple[tuple[int, str, str, int, Optional[str], int, int], ...]]
    explicit_indexes: dict[str, dict[str, _CanonicalIndexSpec]]
    pk_index_counts: dict[str, int]
    pk_index_keys: dict[str, tuple[str, ...]]
    unique_constraints: dict[str, frozenset[frozenset[str]]]
    foreign_keys: dict[str, tuple[tuple[str, str, str, str, str, str], ...]]
    all_known_object_names_lower: frozenset[str]
    total_columns: int
    total_explicit_indexes: int


def _normalize_ddl(sql: str) -> str:
    """Normalize SQL DDL text deterministically for structural comparison."""
    s = re.sub(r"--[^\n]*", "", sql)
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    s = s.strip().rstrip(";")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bCREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+", "CREATE TABLE ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bCREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+", "CREATE INDEX ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*\(\s*", " (", s)
    s = re.sub(r"\s*\)\s*", ")", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    return s.strip().upper()


def _run_query(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[tuple[Any, ...]]:
    """Execute a read-only query using an isolated cursor without altering conn.row_factory."""
    with closing(conn.cursor()) as cur:
        cur.row_factory = None
        cur.execute(sql, params)
        return cur.fetchall()


def _extract_index_keys(
    conn: sqlite3.Connection,
    idx_name: str,
) -> tuple[tuple[int, int, str, int, str, int], ...] | None:
    """Extract canonical (seqno, cid, name, desc, coll, key) tuples for key columns."""
    info_rows = _run_query(conn, f'PRAGMA main.index_xinfo("{idx_name}")')
    key_rows: list[tuple[int, int, str, int, str, int]] = []
    for row in info_rows:
        seqno, cid, col_name, desc, coll, is_key = row[:6]
        if int(is_key) != 1:
            continue
        if cid is None or int(cid) < 0 or not isinstance(col_name, str) or not col_name:
            return None
        key_rows.append(
            (int(seqno), int(cid), col_name, int(desc), str(coll or "").upper(), int(is_key))
        )
    if not key_rows:
        return None
    return tuple(key_rows)


def _read_table_columns(
    conn: sqlite3.Connection,
    tbl: str,
) -> tuple[tuple[int, str, str, int, Optional[str], int, int], ...]:
    """Read normalized PRAGMA main.table_xinfo tuples for an allowlisted table."""
    return tuple(
        (
            int(r[0]),
            str(r[1]),
            str(r[2] or "").upper(),
            int(r[3]),
            None if r[4] is None else str(r[4]),
            int(r[5]),
            int(r[6]),
        )
        for r in _run_query(conn, f'PRAGMA main.table_xinfo("{tbl}")')
    )


def _read_foreign_keys(
    conn: sqlite3.Connection,
    tbl: str,
) -> tuple[tuple[str, str, str, str, str, str], ...]:
    """Read sorted normalized PRAGMA main.foreign_key_list tuples for an allowlisted table."""
    return tuple(
        sorted(
            (
                str(r[2]),
                str(r[3]),
                str(r[4]),
                str(r[5]).upper(),
                str(r[6]).upper(),
                str(r[7]).upper(),
            )
            for r in _run_query(conn, f'PRAGMA main.foreign_key_list("{tbl}")')
        )
    )


def _unknown_read_error_classification(
    schema_fp_before: str = "",
    content_fp_before: str = "",
) -> SchemaPhaseClassification:
    """Return a fail-closed UNKNOWN classification for SQLite read/inspection errors."""
    return SchemaPhaseClassification(
        phase=SchemaPhase.UNKNOWN,
        observed_schema_version=None,
        observed_v6_tables_matched=0,
        observed_v6_columns_matched=0,
        observed_v6_explicit_indexes_matched=0,
        candidate_journal_present=False,
        candidate_journal_valid=False,
        schema_fingerprint_before=schema_fp_before,
        schema_fingerprint_after="",
        content_fingerprint_before=content_fp_before,
        content_fingerprint_after="",
        non_mutating_verified=False,
        reason_codes=(PhaseGateReasonCode.INSPECTION_READ_ERROR,),
    )


@lru_cache(maxsize=1)
def _build_canonical_v6_profile() -> _CanonicalV6Profile:
    """Derive canonical Schema v6 structural metadata using a private ephemeral :memory: reference fixture."""
    if CURRENT_SCHEMA_VERSION != 6:
        raise RuntimeError(f"Expected operational CURRENT_SCHEMA_VERSION=6, got {CURRENT_SCHEMA_VERSION}.")
    if len(EXPECTED_JOURNAL_COLUMNS) != 10:
        raise RuntimeError("Canonical staging_run_journal contract must define exactly 10 columns.")

    with closing(sqlite3.connect(":memory:")) as conn:
        for stmt in get_all_init_statements():
            conn.execute(stmt)

        master_rows = _run_query(
            conn,
            "SELECT type, name, tbl_name, sql FROM main.sqlite_master ORDER BY type, name, tbl_name",
        )
        table_sqls: dict[str, str] = {}
        index_sqls: dict[tuple[str, str], str] = {}
        for obj_type, obj_name, tbl_name, obj_sql in master_rows:
            if str(obj_name).startswith("sqlite_"):
                continue
            if obj_type == "table" and isinstance(obj_sql, str):
                table_sqls[str(obj_name)] = obj_sql
            elif obj_type == "index" and isinstance(obj_sql, str):
                index_sqls[(str(tbl_name), str(obj_name))] = obj_sql

        tables = frozenset(table_sqls.keys())
        if tables != frozenset(SCHEMA_V6_TABLES.keys()) or len(tables) != 15:
            raise RuntimeError("Canonical schema tables do not match SCHEMA_V6_TABLES.")
        if _CANDIDATE_JOURNAL_TABLE in tables:
            raise RuntimeError("Operational Schema v6 must not include staging_run_journal.")

        table_ddls: dict[str, str] = {}
        table_columns: dict[str, tuple[tuple[int, str, str, int, Optional[str], int, int], ...]] = {}
        explicit_indexes: dict[str, dict[str, _CanonicalIndexSpec]] = {}
        pk_index_counts: dict[str, int] = {}
        pk_index_keys: dict[str, tuple[str, ...]] = {}
        unique_constraints: dict[str, frozenset[frozenset[str]]] = {}
        foreign_keys: dict[str, tuple[tuple[str, str, str, str, str, str], ...]] = {}

        total_cols = 0
        total_idxs = 0
        all_names_lower: set[str] = {
            _CANDIDATE_JOURNAL_TABLE.lower(),
            EXPECTED_JOURNAL_INDEX_NAME.lower(),
        }

        for tbl in sorted(tables):
            all_names_lower.add(tbl.lower())
            table_ddls[tbl] = _normalize_ddl(table_sqls[tbl])

            cols_norm = _read_table_columns(conn, tbl)
            if [c[1] for c in cols_norm] != SCHEMA_V6_TABLES[tbl]:
                raise RuntimeError(f"Column contract mismatch with preflight SCHEMA_V6_TABLES for {tbl}.")
            expected_types = SCHEMA_V6_COLUMN_TYPES.get(tbl, {})
            cols_type_map = {c[1]: c[2] for c in cols_norm}
            for col_name, exp_type in expected_types.items():
                if cols_type_map.get(col_name) != exp_type.upper():
                    raise RuntimeError(f"Column type mismatch with SCHEMA_V6_COLUMN_TYPES for {tbl}.{col_name}.")
            if any(c[6] != 0 for c in cols_norm):
                raise RuntimeError(f"Unexpected hidden column in canonical Schema v6 table {tbl}.")
            table_columns[tbl] = cols_norm
            total_cols += len(cols_norm)

            foreign_keys[tbl] = _read_foreign_keys(conn, tbl)

            idx_rows = _run_query(conn, f'PRAGMA main.index_list("{tbl}")')
            tbl_explicit: dict[str, _CanonicalIndexSpec] = {}
            pk_count = 0
            pk_cols: tuple[str, ...] = ()
            unique_sets: set[frozenset[str]] = set()
            for idx_row in idx_rows:
                idx_name = str(idx_row[1])
                is_unique = int(idx_row[2])
                origin = str(idx_row[3])
                is_partial = int(idx_row[4])
                keys = _extract_index_keys(conn, idx_name)
                if keys is None:
                    raise RuntimeError(f"Unexpected expression index in canonical v6 schema: {idx_name}")
                if origin == "c":
                    all_names_lower.add(idx_name.lower())
                    raw_sql = index_sqls[(tbl, idx_name)]
                    tbl_explicit[idx_name] = _CanonicalIndexSpec(
                        ddl=_normalize_ddl(raw_sql),
                        unique=is_unique,
                        origin=origin,
                        partial=is_partial,
                        keys=keys,
                    )
                    total_idxs += 1
                elif origin == "pk":
                    pk_count += 1
                    pk_cols = tuple(k[2] for k in keys)
                elif origin == "u" and is_unique == 1 and is_partial == 0:
                    unique_sets.add(frozenset(k[2] for k in keys))

            preflight_tbl_idxs = set(SCHEMA_V6_INDEXES.get(tbl, []))
            if not preflight_tbl_idxs.issubset(set(tbl_explicit.keys())):
                raise RuntimeError(f"Preflight SCHEMA_V6_INDEXES missing from schema DDL for {tbl}.")
            critical_uqs = {frozenset(s) for s in SCHEMA_V6_CRITICAL_UNIQUE_CONSTRAINTS.get(tbl, [])}
            if not critical_uqs.issubset(unique_sets):
                raise RuntimeError(f"Preflight unique constraints missing from schema DDL for {tbl}.")

            explicit_indexes[tbl] = tbl_explicit
            pk_index_counts[tbl] = pk_count
            pk_index_keys[tbl] = pk_cols
            unique_constraints[tbl] = frozenset(unique_sets)

    return _CanonicalV6Profile(
        tables=tables,
        table_ddls=table_ddls,
        table_columns=table_columns,
        explicit_indexes=explicit_indexes,
        pk_index_counts=pk_index_counts,
        pk_index_keys=pk_index_keys,
        unique_constraints=unique_constraints,
        foreign_keys=foreign_keys,
        all_known_object_names_lower=frozenset(all_names_lower),
        total_columns=total_cols,
        total_explicit_indexes=total_idxs,
    )


def _assert_hermetic_fixture_connection(conn: Any) -> tuple[bool, bool]:
    """Enforce exact sqlite3.Connection type and in-memory / OS-temp provenance.

    Never echoes filesystem paths or untrusted values into exceptions.
    Returns (db_list_ok, has_attached_extra).
    """
    if type(conn) is not sqlite3.Connection:
        raise PermissionError(
            "HERMETIC_FIXTURE_CONNECTION_REQUIRED: only local sqlite3.Connection fixtures are permitted."
        ) from None

    try:
        db_rows = _run_query(conn, "PRAGMA database_list")
    except sqlite3.Error:
        return False, False

    temp_dir = Path(tempfile.gettempdir()).resolve()
    repo_data_dir = (Path(__file__).resolve().parents[3] / "data").resolve()

    has_attached_extra = False
    for row in db_rows:
        db_name = str(row[1]) if len(row) > 1 else ""
        db_file = row[2] if len(row) > 2 else ""
        if db_name not in ("main", "temp"):
            has_attached_extra = True
        if db_file and db_file != "" and db_file != ":memory:":
            raw_path = Path(db_file)
            if raw_path.is_symlink():
                raise PermissionError(
                    "HERMETIC_FIXTURE_ISOLATION_VIOLATION: symlink SQLite databases are disallowed."
                ) from None
            resolved = raw_path.resolve()
            if (
                resolved == temp_dir
                or not resolved.is_relative_to(temp_dir)
                or resolved == repo_data_dir
                or resolved.is_relative_to(repo_data_dir)
                or (resolved.name.lower() == "radar.db" and any(part.lower() == "data" for part in resolved.parts))
            ):
                raise PermissionError(
                    "HERMETIC_FIXTURE_ISOLATION_VIOLATION: SQLite connection targets a non-temporary or operational database."
                ) from None

    return True, has_attached_extra


def _compute_fingerprints(
    conn: sqlite3.Connection,
    allowlisted_tables: frozenset[str],
) -> tuple[str, str]:
    """Compute read-only SHA-256 digests of schema objects and allowlisted content state."""
    main_master = _run_query(
        conn,
        "SELECT type, name, tbl_name, sql FROM main.sqlite_master ORDER BY type, name, tbl_name",
    )
    temp_master = _run_query(
        conn,
        "SELECT type, name, tbl_name, sql FROM temp.sqlite_master ORDER BY type, name, tbl_name",
    )
    schema_payload = json.dumps(
        {"main": main_master, "temp": temp_master},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    schema_fp = hashlib.sha256(schema_payload).hexdigest()

    data_ver_rows = _run_query(conn, "PRAGMA main.data_version")
    data_version = int(data_ver_rows[0][0]) if data_ver_rows else 0

    existing_real_tables: set[str] = set()
    for obj_type, obj_name, _, obj_sql in main_master:
        if obj_type != "table" or obj_name not in allowlisted_tables:
            continue
        if isinstance(obj_sql, str) and obj_sql.strip().upper().startswith("CREATE VIRTUAL TABLE"):
            continue
        existing_real_tables.add(str(obj_name))

    row_counts: dict[str, int | str] = {}
    for tbl in sorted(existing_real_tables):
        try:
            cnt_rows = _run_query(conn, f'SELECT COUNT(*) FROM main."{tbl}"')
            row_counts[tbl] = int(cnt_rows[0][0]) if cnt_rows else 0
        except sqlite3.Error:
            row_counts[tbl] = "READ_ERROR"

    sm_rows: Any = None
    if "schema_migrations" in existing_real_tables:
        sm_cols = [str(r[1]) for r in _run_query(conn, 'PRAGMA main.table_xinfo("schema_migrations")')]
        if set(("version", "applied_at", "description")).issubset(set(sm_cols)):
            try:
                sm_rows = _run_query(
                    conn,
                    "SELECT version, applied_at, description FROM main.schema_migrations ORDER BY version ASC",
                )
            except sqlite3.Error:
                sm_rows = "READ_ERROR"

    content_payload = json.dumps(
        {
            "data_version": data_version,
            "total_changes": int(conn.total_changes),
            "row_counts": row_counts,
            "schema_migrations": sm_rows,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    content_fp = hashlib.sha256(content_payload).hexdigest()
    return schema_fp, content_fp


def classify_offline_schema_phase(
    conn: sqlite3.Connection,
    *,
    allow_candidate_journal: bool = True,
) -> SchemaPhaseClassification:
    """Strictly read-only classification of an offline SQLite schema fixture."""
    profile = _build_canonical_v6_profile()
    db_list_ok, has_attached_extra = _assert_hermetic_fixture_connection(conn)
    allowlisted_for_fp = profile.tables | frozenset({_CANDIDATE_JOURNAL_TABLE})

    if not db_list_ok:
        return _unknown_read_error_classification()

    reason_codes: list[PhaseGateReasonCode] = []

    def add_reason(code: PhaseGateReasonCode) -> None:
        if code not in reason_codes:
            reason_codes.append(code)

    try:
        schema_fp_before, content_fp_before = _compute_fingerprints(conn, allowlisted_for_fp)
    except sqlite3.Error:
        return _unknown_read_error_classification()

    observed_schema_version: Optional[int] = None
    observed_v6_tables_matched = 0
    observed_v6_columns_matched = 0
    observed_v6_explicit_indexes_matched = 0
    candidate_journal_present = False
    candidate_journal_valid = False
    journal_has_invalid_artifact = False

    try:
        if has_attached_extra:
            add_reason(PhaseGateReasonCode.ATTACHED_DATABASE_DISALLOWED)

        # 1. Inspect temp schema case-insensitively without SQL LIKE
        temp_rows = _run_query(conn, "SELECT type, name, tbl_name FROM temp.sqlite_master ORDER BY type, name")
        for obj_type, obj_name, tbl_name in temp_rows:
            name_str = str(obj_name or "")
            tbl_str = str(tbl_name or "")
            if obj_type == "trigger":
                add_reason(PhaseGateReasonCode.TEMP_TRIGGER_DISALLOWED)
            elif (
                name_str.lower() in profile.all_known_object_names_lower
                or tbl_str.lower() in profile.all_known_object_names_lower
            ):
                add_reason(PhaseGateReasonCode.TEMP_SHADOW_OBJECT_DISALLOWED)
            elif not name_str.startswith("sqlite_"):
                add_reason(PhaseGateReasonCode.UNEXPECTED_TEMP_OBJECT)

        # 2. Inspect main.sqlite_master objects
        main_rows = _run_query(
            conn,
            "SELECT type, name, tbl_name, sql FROM main.sqlite_master ORDER BY type, name, tbl_name",
        )
        main_tables: dict[str, Optional[str]] = {}
        explicit_main_indexes: dict[tuple[str, str], Optional[str]] = {}

        for obj_type, obj_name, tbl_name, obj_sql in main_rows:
            t_str = str(obj_type or "")
            name_str = str(obj_name or "")
            tbl_str = str(tbl_name or "")
            sql_str = str(obj_sql) if obj_sql is not None else None

            if name_str.startswith("sqlite_"):
                if not (t_str == "index" and sql_str is None and _AUTOINDEX_NAME_RE.match(name_str)):
                    add_reason(PhaseGateReasonCode.UNEXPECTED_INTERNAL_SCHEMA_OBJECT)
                continue

            if t_str == "view":
                add_reason(PhaseGateReasonCode.UNEXPECTED_VIEW_OBJECT)
                if name_str.lower() == _CANDIDATE_JOURNAL_TABLE:
                    candidate_journal_present = True
                    journal_has_invalid_artifact = True
                    add_reason(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID)
            elif t_str == "trigger":
                add_reason(PhaseGateReasonCode.UNEXPECTED_TRIGGER_OBJECT)
                if tbl_str.lower() == _CANDIDATE_JOURNAL_TABLE or name_str.lower() == _CANDIDATE_JOURNAL_TABLE:
                    candidate_journal_present = True
                    journal_has_invalid_artifact = True
                    add_reason(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID)
            elif t_str == "table":
                main_tables[name_str] = sql_str
                is_virtual = sql_str is not None and sql_str.strip().upper().startswith("CREATE VIRTUAL TABLE")
                if is_virtual:
                    add_reason(PhaseGateReasonCode.UNEXPECTED_VIRTUAL_TABLE)
                if name_str == _CANDIDATE_JOURNAL_TABLE:
                    candidate_journal_present = True
                    if is_virtual:
                        journal_has_invalid_artifact = True
                        add_reason(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID)
                elif name_str not in profile.tables:
                    if not is_virtual:
                        add_reason(PhaseGateReasonCode.UNEXPECTED_TABLE_OBJECT)
                    if name_str.lower() == _CANDIDATE_JOURNAL_TABLE:
                        candidate_journal_present = True
                        journal_has_invalid_artifact = True
                        add_reason(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID)
            elif t_str == "index":
                explicit_main_indexes[(tbl_str, name_str)] = sql_str
            else:
                add_reason(PhaseGateReasonCode.UNEXPECTED_TABLE_OBJECT)

        # 3. Validate all 15 canonical Schema v6 tables
        sm_structurally_valid = False
        for tbl in sorted(profile.tables):
            if tbl not in main_tables:
                add_reason(PhaseGateReasonCode.MISSING_V6_TABLE)
                if tbl == "schema_migrations":
                    add_reason(PhaseGateReasonCode.SCHEMA_MIGRATIONS_TABLE_MISSING)
                continue

            tbl_sql = main_tables[tbl]
            if tbl_sql is None:
                add_reason(PhaseGateReasonCode.V6_TABLE_DDL_MISMATCH)
                continue
            if tbl_sql.strip().upper().startswith("CREATE VIRTUAL TABLE"):
                add_reason(PhaseGateReasonCode.UNEXPECTED_VIRTUAL_TABLE)
                continue

            table_valid = True
            if _FORBIDDEN_V6_DDL_CLAUSES.search(tbl_sql) or _normalize_ddl(tbl_sql) != profile.table_ddls[tbl]:
                add_reason(PhaseGateReasonCode.V6_TABLE_DDL_MISMATCH)
                table_valid = False

            # 3a. Column inspection via PRAGMA main.table_xinfo
            expected_cols = profile.table_columns[tbl]
            actual_norm_cols = _read_table_columns(conn, tbl)

            cols_exact = True
            if len(actual_norm_cols) != len(expected_cols):
                add_reason(PhaseGateReasonCode.V6_COLUMN_COUNT_MISMATCH)
                cols_exact = False
                actual_by_name = {c[1]: c for c in actual_norm_cols}
                for exp_col in expected_cols:
                    act_col = actual_by_name.get(exp_col[1])
                    if act_col == exp_col:
                        observed_v6_columns_matched += 1
                    elif act_col is not None:
                        add_reason(PhaseGateReasonCode.V6_COLUMN_METADATA_MISMATCH)
            else:
                for exp_col, act_col in zip(expected_cols, actual_norm_cols):
                    if act_col == exp_col:
                        observed_v6_columns_matched += 1
                    else:
                        add_reason(PhaseGateReasonCode.V6_COLUMN_METADATA_MISMATCH)
                        cols_exact = False

            if not cols_exact:
                table_valid = False
            elif tbl == "schema_migrations" and table_valid:
                sm_structurally_valid = True

            # 3b. Foreign key inspection
            if _read_foreign_keys(conn, tbl) != profile.foreign_keys[tbl]:
                add_reason(PhaseGateReasonCode.V6_FOREIGN_KEY_MISMATCH)
                table_valid = False

            # 3c. Explicit indexes, PK autoindexes, and unique constraints on this v6 table
            idx_rows = _run_query(conn, f'PRAGMA main.index_list("{tbl}")')
            exp_tbl_indexes = profile.explicit_indexes[tbl]
            actual_explicit_by_name = {str(r[1]): r for r in idx_rows if str(r[3]) == "c"}

            for exp_idx_name, exp_spec in exp_tbl_indexes.items():
                act_idx_row = actual_explicit_by_name.get(exp_idx_name)
                if act_idx_row is None:
                    add_reason(PhaseGateReasonCode.MISSING_V6_INDEX)
                    table_valid = False
                    continue

                idx_ok = True
                act_unique = int(act_idx_row[2])
                act_origin = str(act_idx_row[3])
                act_partial = int(act_idx_row[4])
                if act_unique != exp_spec.unique or act_origin != exp_spec.origin or act_partial != exp_spec.partial:
                    add_reason(PhaseGateReasonCode.V6_INDEX_METADATA_MISMATCH)
                    idx_ok = False

                idx_keys = _extract_index_keys(conn, exp_idx_name)
                if idx_keys != exp_spec.keys:
                    add_reason(PhaseGateReasonCode.V6_INDEX_METADATA_MISMATCH)
                    idx_ok = False

                raw_idx_sql = explicit_main_indexes.get((tbl, exp_idx_name))
                if raw_idx_sql is None or _normalize_ddl(raw_idx_sql) != exp_spec.ddl:
                    add_reason(PhaseGateReasonCode.V6_INDEX_DDL_MISMATCH)
                    idx_ok = False

                if idx_ok:
                    observed_v6_explicit_indexes_matched += 1
                else:
                    table_valid = False

            for act_idx_name in actual_explicit_by_name:
                if act_idx_name not in exp_tbl_indexes:
                    add_reason(PhaseGateReasonCode.UNEXPECTED_INDEX_OBJECT)
                    if act_idx_name.lower() == EXPECTED_JOURNAL_INDEX_NAME:
                        candidate_journal_present = True
                        journal_has_invalid_artifact = True
                        add_reason(PhaseGateReasonCode.PARTIAL_OR_ORPHAN_JOURNAL_INDEX)
                    table_valid = False

            pk_rows = [r for r in idx_rows if str(r[3]) == "pk"]
            if len(pk_rows) != profile.pk_index_counts[tbl]:
                add_reason(PhaseGateReasonCode.V6_UNIQUE_CONSTRAINT_MISMATCH)
                table_valid = False
            else:
                for pk_r in pk_rows:
                    pk_idx_name = str(pk_r[1])
                    if not _AUTOINDEX_NAME_RE.match(pk_idx_name):
                        add_reason(PhaseGateReasonCode.V6_UNIQUE_CONSTRAINT_MISMATCH)
                        table_valid = False
                        continue
                    pk_keys = _extract_index_keys(conn, pk_idx_name)
                    if pk_keys is None or tuple(k[2] for k in pk_keys) != profile.pk_index_keys[tbl]:
                        add_reason(PhaseGateReasonCode.V6_UNIQUE_CONSTRAINT_MISMATCH)
                        table_valid = False

            notnull_cols = {c[1] for c in actual_norm_cols if c[3] == 1}
            u_rows = [r for r in idx_rows if str(r[3]) == "u"]
            actual_unique_sets: set[frozenset[str]] = set()
            uq_ok = True
            for u_r in u_rows:
                u_name = str(u_r[1])
                if int(u_r[2]) != 1 or int(u_r[4]) != 0 or not _AUTOINDEX_NAME_RE.match(u_name):
                    uq_ok = False
                    continue
                u_keys = _extract_index_keys(conn, u_name)
                if u_keys is None:
                    uq_ok = False
                    continue
                u_col_names = frozenset(k[2] for k in u_keys)
                if not u_col_names.issubset(notnull_cols):
                    uq_ok = False
                actual_unique_sets.add(u_col_names)
            if not uq_ok or frozenset(actual_unique_sets) != profile.unique_constraints[tbl]:
                add_reason(PhaseGateReasonCode.V6_UNIQUE_CONSTRAINT_MISMATCH)
                table_valid = False

            if table_valid:
                observed_v6_tables_matched += 1

        # 4. Inspect remaining explicit indexes in main.sqlite_master
        for tbl_str, idx_name in explicit_main_indexes:
            if tbl_str in profile.tables and idx_name in profile.explicit_indexes[tbl_str]:
                continue
            if tbl_str == _CANDIDATE_JOURNAL_TABLE and idx_name == EXPECTED_JOURNAL_INDEX_NAME:
                continue
            if idx_name.lower() == EXPECTED_JOURNAL_INDEX_NAME:
                candidate_journal_present = True
                journal_has_invalid_artifact = True
                add_reason(PhaseGateReasonCode.PARTIAL_OR_ORPHAN_JOURNAL_INDEX)
            elif tbl_str.lower() == _CANDIDATE_JOURNAL_TABLE:
                candidate_journal_present = True
                journal_has_invalid_artifact = True
                add_reason(PhaseGateReasonCode.UNEXPECTED_INDEX_OBJECT)
                add_reason(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID)
            else:
                add_reason(PhaseGateReasonCode.UNEXPECTED_INDEX_OBJECT)

        # 5. Inspect schema_migrations version ledger when schema_migrations structure is intact
        if "schema_migrations" in main_tables and sm_structurally_valid:
            sm_rows = _run_query(
                conn,
                "SELECT version, applied_at, description FROM main.schema_migrations ORDER BY version ASC",
            )
            if not sm_rows:
                add_reason(PhaseGateReasonCode.SCHEMA_VERSION_MISSING)
            else:
                int_versions: list[int] = []
                malformed_row = False
                for ver, applied_at, description in sm_rows:
                    if (
                        type(ver) is not int
                        or not isinstance(applied_at, str)
                        or not applied_at.strip()
                        or not isinstance(description, str)
                        or not description.strip()
                    ):
                        malformed_row = True
                    if type(ver) is int:
                        int_versions.append(ver)
                if malformed_row:
                    add_reason(PhaseGateReasonCode.MALFORMED_SCHEMA_MIGRATION_ROW)
                if not int_versions:
                    add_reason(PhaseGateReasonCode.SCHEMA_VERSION_MISSING)
                else:
                    observed_schema_version = max(int_versions)
                    has_future = any(v > CURRENT_SCHEMA_VERSION for v in int_versions)
                    has_below_one = any(v < 1 for v in int_versions)
                    if has_future:
                        add_reason(PhaseGateReasonCode.UNAPPROVED_FUTURE_SCHEMA_VERSION)
                    if has_below_one:
                        add_reason(PhaseGateReasonCode.UNEXPECTED_SCHEMA_MIGRATION_HISTORY)
                    if observed_schema_version < CURRENT_SCHEMA_VERSION:
                        add_reason(PhaseGateReasonCode.SCHEMA_VERSION_OUTDATED)
                    canonical_histories = {
                        (CURRENT_SCHEMA_VERSION,),
                        tuple(step.version for step in MIGRATIONS),
                    }
                    if (
                        not has_future
                        and not has_below_one
                        and observed_schema_version == CURRENT_SCHEMA_VERSION
                        and tuple(int_versions) not in canonical_histories
                    ):
                        add_reason(PhaseGateReasonCode.UNEXPECTED_SCHEMA_MIGRATION_HISTORY)

        # 6. Validate candidate staging_run_journal if present
        if _CANDIDATE_JOURNAL_TABLE in main_tables:
            candidate_journal_present = True
            if not allow_candidate_journal:
                add_reason(PhaseGateReasonCode.UNEXPECTED_JOURNAL_PRESENT)
            try:
                journal_report = verify_staging_journal_schema(conn, raise_on_error=False)
            except PermissionError:
                raise PermissionError(
                    "HERMETIC_FIXTURE_ISOLATION_VIOLATION: SQLite connection failed hermetic fixture isolation."
                ) from None
            except (TypeError, ValueError, KeyError, IndexError):
                candidate_journal_valid = False
                add_reason(PhaseGateReasonCode.INSPECTION_READ_ERROR)
            else:
                if journal_report.is_valid and not journal_has_invalid_artifact:
                    candidate_journal_valid = True
                else:
                    candidate_journal_valid = False
                    add_reason(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID)
        elif candidate_journal_present:
            if not allow_candidate_journal:
                add_reason(PhaseGateReasonCode.UNEXPECTED_JOURNAL_PRESENT)
            candidate_journal_valid = False

        # 7. Post-inspection non-mutating fingerprint check
        schema_fp_after, content_fp_after = _compute_fingerprints(conn, allowlisted_for_fp)
        non_mutating_verified = (
            bool(schema_fp_before)
            and schema_fp_before == schema_fp_after
            and bool(content_fp_before)
            and content_fp_before == content_fp_after
        )
        if not non_mutating_verified:
            add_reason(PhaseGateReasonCode.NON_MUTATING_FINGERPRINT_MISMATCH)

        # 8. Determine final typed SchemaPhase
        if (
            PhaseGateReasonCode.INSPECTION_READ_ERROR in reason_codes
            or PhaseGateReasonCode.NON_MUTATING_FINGERPRINT_MISMATCH in reason_codes
        ):
            phase = SchemaPhase.UNKNOWN
        elif reason_codes:
            phase = SchemaPhase.DENY
        elif candidate_journal_present:
            if candidate_journal_valid and allow_candidate_journal:
                phase = SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE
            else:
                add_reason(PhaseGateReasonCode.UNEXPECTED_JOURNAL_PRESENT)
                phase = SchemaPhase.DENY
        else:
            phase = SchemaPhase.V6_BASELINE_OFFLINE

        return SchemaPhaseClassification(
            phase=phase,
            observed_schema_version=observed_schema_version,
            observed_v6_tables_matched=observed_v6_tables_matched,
            observed_v6_columns_matched=observed_v6_columns_matched,
            observed_v6_explicit_indexes_matched=observed_v6_explicit_indexes_matched,
            candidate_journal_present=candidate_journal_present,
            candidate_journal_valid=candidate_journal_valid,
            schema_fingerprint_before=schema_fp_before,
            schema_fingerprint_after=schema_fp_after,
            content_fingerprint_before=content_fp_before,
            content_fingerprint_after=content_fp_after,
            non_mutating_verified=non_mutating_verified,
            reason_codes=tuple(reason_codes),
        )
    except sqlite3.Error:
        return _unknown_read_error_classification(
            schema_fp_before=schema_fp_before,
            content_fp_before=content_fp_before,
        )


def _format_mock_operator_handoff(
    classification: SchemaPhaseClassification,
) -> MockOperatorHandoff:
    """Internal pure formatter — NOT a trust or authorization boundary.

    Does NOT authenticate database inspection provenance. Callers must only invoke
    this immediately after classify_offline_schema_phase(conn, ...) on a verified
    hermetic SQLite connection within the same call.
    """
    elig, elig_v6, elig_journal, next_gate, notes = _HANDOFF_PHASE_SPEC[classification.phase]
    return MockOperatorHandoff(
        phase=classification.phase,
        mock_test_eligibility=elig,
        eligible_for_mock_v6_live01=elig_v6,
        eligible_for_mock_journal_seam=elig_journal,
        manual_next_gate=next_gate,
        observed_schema_version=classification.observed_schema_version,
        schema_fingerprint_sha256=classification.schema_fingerprint_after,
        non_mutating_verified=classification.non_mutating_verified,
        reason_codes=classification.reason_codes,
        notes=notes,
    )


def _fail_closed_handoff(
    phase: SchemaPhase,
    reason: PhaseGateReasonCode,
) -> MockOperatorHandoff:
    elig, elig_v6, elig_journal, next_gate, notes = _HANDOFF_PHASE_SPEC[phase]
    return MockOperatorHandoff(
        phase=phase,
        mock_test_eligibility=elig,
        eligible_for_mock_v6_live01=elig_v6,
        eligible_for_mock_journal_seam=elig_journal,
        manual_next_gate=next_gate,
        observed_schema_version=None,
        schema_fingerprint_sha256="",
        non_mutating_verified=False,
        reason_codes=(reason,),
        notes=notes,
    )


def build_mock_operator_handoff(
    classification: Optional[SchemaPhaseClassification] = None,
    *,
    conn: Optional[sqlite3.Connection] = None,
    allow_candidate_journal: bool = True,
) -> MockOperatorHandoff:
    """Construct an inert mock-only operator handoff bound to an actual fixture inspection.

    Positive mock eligibility is NEVER granted from caller-declared classification
    fields alone. A hermetically scoped sqlite3.Connection must be inspected via
    classify_offline_schema_phase(); any missing connection, forged classification,
    or discrepancy against live fixture inspection fails closed (UNKNOWN / DENY).
    """
    if conn is None:
        if classification is None:
            return _fail_closed_handoff(
                SchemaPhase.UNKNOWN,
                PhaseGateReasonCode.UNOBSERVED_OR_MISSING_EVIDENCE,
            )
        return _fail_closed_handoff(
            SchemaPhase.DENY,
            PhaseGateReasonCode.UNTRUSTED_OR_REMOTE_CLAIMED_EVIDENCE,
        )

    inspected = classify_offline_schema_phase(
        conn,
        allow_candidate_journal=allow_candidate_journal,
    )
    if classification is not None:
        if type(classification) is not SchemaPhaseClassification:
            return _fail_closed_handoff(
                SchemaPhase.DENY,
                PhaseGateReasonCode.UNTRUSTED_OR_REMOTE_CLAIMED_EVIDENCE,
            )
        try:
            validated = SchemaPhaseClassification.model_validate(classification.model_dump())
        except (ValidationError, TypeError, ValueError):
            return _fail_closed_handoff(
                SchemaPhase.DENY,
                PhaseGateReasonCode.UNTRUSTED_OR_REMOTE_CLAIMED_EVIDENCE,
            )
        if validated != inspected:
            return _fail_closed_handoff(
                SchemaPhase.DENY,
                PhaseGateReasonCode.UNTRUSTED_OR_REMOTE_CLAIMED_EVIDENCE,
            )

    return _format_mock_operator_handoff(inspected)


def evaluate_offline_schema_phase_gate(
    conn: sqlite3.Connection,
    *,
    allow_candidate_journal: bool = True,
) -> MockOperatorHandoff:
    """Primary trusted entrypoint: inspect a hermetic SQLite fixture and return an inert handoff."""
    classification = classify_offline_schema_phase(
        conn,
        allow_candidate_journal=allow_candidate_journal,
    )
    return _format_mock_operator_handoff(classification)
