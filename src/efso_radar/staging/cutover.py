"""Staging cutover and rollback readiness protocol for Efso Radar.

Provides:
- Source-to-target schema and record count reconciliation.
- Identity and primary-key reconciliation across all 16 tables.
- Strict migration abort condition enforcement.
- Rollback prerequisite validation (verified backups and restore points).
- Product Owner Go/No-Go gate evaluation.

Safety Invariants:
- Rejects operational database data/radar.db.
- Non-mutating during inspection and reconciliation.
- Fails closed if any prerequisite is unfulfilled.
"""
from __future__ import annotations

import hashlib
import json
import logging
import struct
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.domain import utc_now
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.migration_ledger import mask_text_chat_ids
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.staging.preflight import (
    SCHEMA_V6_TABLES,
    CloudPreflightValidator,
    PreflightReport,
    execute_read_query,
)

logger = logging.getLogger("efso-radar.staging.cutover")


class ContentHashError(Exception):
    """Raised when table content cannot be losslessly serialized. Never carries record contents."""


def _frame(tag: bytes, payload: bytes) -> bytes:
    """Length-prefixed, type-tagged field so values can never bleed into each other."""
    return tag + len(payload).to_bytes(8, "big") + payload


def _serialize_value(val: Any) -> bytes:
    """Type-preserving lossless encoding of a SQLite/libSQL value (NULL/INTEGER/REAL/TEXT/BLOB)."""
    if val is None:
        return _frame(b"N", b"")
    if isinstance(val, bool):
        return _frame(b"I", str(int(val)).encode("ascii"))
    if isinstance(val, int):
        return _frame(b"I", str(val).encode("ascii"))
    if isinstance(val, float):
        # IEEE-754 big-endian bits: exact, no rounding
        return _frame(b"R", struct.pack(">d", val))
    if isinstance(val, str):
        try:
            return _frame(b"T", val.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise ContentHashError("TEXT value is not valid UTF-8") from exc
    if isinstance(val, (bytes, bytearray, memoryview)):
        return _frame(b"B", bytes(val))
    raise ContentHashError(f"Unsupported value type: {type(val).__name__}")


def compute_table_content_hash(rows: list[dict[str, Any]], expected_cols: list[str]) -> str:
    """Compute a deterministic, lossless SHA-256 hash of table records.

    Guarantees:
    - Type-preserving: NULL, INTEGER, REAL, TEXT and BLOB are all distinct (None != "__NULL__",
      6 != "6", b"a" != "a").
    - REAL values keep full IEEE-754 precision (no rounding).
    - TEXT is hashed exactly as stored (no trimming, no JSON interpretation).
    - Columns follow ``expected_cols`` order; every field and row is length-framed, so row and
      column boundaries are unambiguous. Row order is the caller's ORDER BY primary key.
    - Fails closed: a missing column or unsupported value raises ContentHashError (message never
      contains record contents).

    Limitation: ``rows`` is a fully materialized list (``execute_read_query`` does not stream), so
    memory use is proportional to table size. Bounded-memory reconciliation is NOT implemented.
    """
    hasher = hashlib.sha256()
    hasher.update(b"EFSO-CONTENT-HASH-V1")
    hasher.update(len(expected_cols).to_bytes(4, "big"))
    for col in expected_cols:
        hasher.update(_frame(b"C", col.encode("utf-8")))
    hasher.update(len(rows).to_bytes(8, "big"))
    for row in rows:
        hasher.update(b"R")
        for col in expected_cols:
            try:
                val = row[col]
            except (KeyError, IndexError, TypeError) as exc:
                raise ContentHashError(f"Result row is missing expected column '{col}'") from exc
            hasher.update(_serialize_value(val))
    return hasher.hexdigest()


class TableReconciliation(BaseModel):
    table_name: str
    source_count: int = 0
    target_count: int = 0
    count_matches: bool = False
    source_sample_keys: list[str] = Field(default_factory=list)
    target_sample_keys: list[str] = Field(default_factory=list)
    keys_match: bool = False
    content_matches: bool = True
    source_content_hash: str = ""
    target_content_hash: str = ""
    mismatched_keys_sample: list[str] = Field(default_factory=list)


class AbortReason(BaseModel):
    code: str
    message: str


class GoNoGoDecision(BaseModel):
    can_proceed: bool = False
    all_preflights_passed: bool = False
    schema_compatible: bool = False
    data_reconciliation_passed: bool = False
    backup_verified: bool = False
    rollback_ready: bool = False
    zero_cost_verified: bool = False
    product_owner_signed_off: bool = False
    abort_reasons: list[AbortReason] = Field(default_factory=list)
    decided_at: datetime = Field(default_factory=utc_now)

    def summary(self) -> str:
        verdict = "GO (APPROVED FOR STAGING CUTOVER)" if self.can_proceed else "NO-GO (CUTOVER BLOCKED)"
        lines = [
            f"=== STAGING CUTOVER GO / NO-GO DECISION: [{verdict}] ===",
            "NOTE: This evaluation represents offline planning evidence only and does NOT authorize live cutover.",
            f"Decided At: {self.decided_at.isoformat()}",
            f"1. Preflights Passed: {'YES' if self.all_preflights_passed else 'NO'}",
            f"2. Schema Compatible: {'YES' if self.schema_compatible else 'NO'}",
            f"3. Data Reconciled: {'YES' if self.data_reconciliation_passed else 'NO'}",
            f"4. Backup Verified: {'YES' if self.backup_verified else 'NO'}",
            f"5. Rollback Ready: {'YES' if self.rollback_ready else 'NO'}",
            f"6. Zero Cost Verified ($0.00 / 0 TRY): {'YES' if self.zero_cost_verified else 'NO'}",
            f"7. Product Owner Sign-off: {'YES' if self.product_owner_signed_off else 'NO'}",
        ]
        if self.abort_reasons:
            lines.append("\nAbort Conditions Triggered:")
            for ar in self.abort_reasons:
                lines.append(f"  - [{ar.code}] {ar.message}")
        return "\n".join(lines)


class StagingCutoverReport(BaseModel):
    source_type: str = ""
    target_type: str = ""
    source_schema_version: int = 0
    target_schema_version: int = 0
    table_reconciliations: list[TableReconciliation] = Field(default_factory=list)
    abort_reasons: list[AbortReason] = Field(default_factory=list)
    idempotent_dry_run_verified: bool = False

    @property
    def all_counts_match(self) -> bool:
        return bool(self.table_reconciliations) and all(t.count_matches for t in self.table_reconciliations)

    @property
    def all_keys_match(self) -> bool:
        return bool(self.table_reconciliations) and all(t.keys_match for t in self.table_reconciliations)

    @property
    def all_contents_match(self) -> bool:
        return bool(self.table_reconciliations) and all(t.content_matches for t in self.table_reconciliations)

    @property
    def is_safe_to_cutover(self) -> bool:
        return (
            self.all_counts_match
            and self.all_keys_match
            and self.all_contents_match
            and len(self.abort_reasons) == 0
        )


class GoNoGoGate:
    """Evaluates strict multi-factor prerequisites before authorizing staging transition.

    CRITICAL SAFETY CONTRACT:
    - Any reconciliation failure (count, full key set, content hash, or inspection error) MUST produce NO-GO.
    - All abort reasons from the cutover report must be propagated into the decision.
    - Never issue GO when the reconciler reports is_safe_to_cutover=False.
    - Product Owner sign-off is mandatory and non-negotiable.
    - An evaluation emitting can_proceed=True serves strictly as offline planning evidence,
      never as automatic permission for live service transition.
    """

    @staticmethod
    def evaluate(
        cutover_report: StagingCutoverReport,
        target_preflight: PreflightReport,
        backup_verified: bool,
        rollback_ready: bool,
        zero_cost_verified: bool,
        product_owner_signed_off: bool,
    ) -> GoNoGoDecision:
        decision = GoNoGoDecision(
            all_preflights_passed=target_preflight.success,
            schema_compatible=(
                cutover_report.target_schema_version >= CURRENT_SCHEMA_VERSION
                and cutover_report.source_schema_version >= CURRENT_SCHEMA_VERSION
            ),
            data_reconciliation_passed=cutover_report.is_safe_to_cutover,
            backup_verified=backup_verified,
            rollback_ready=rollback_ready,
            zero_cost_verified=zero_cost_verified,
            product_owner_signed_off=product_owner_signed_off,
        )

        # 1. Target preflight validation check
        if not target_preflight.success:
            decision.abort_reasons.append(
                AbortReason(code="ERR_PREFLIGHT_FAILED", message="Target preflight validation failed.")
            )
            for err in target_preflight.errors:
                decision.abort_reasons.append(
                    AbortReason(code="ERR_PREFLIGHT_ERROR", message=mask_text_chat_ids(str(err)))
                )
            for mc in target_preflight.missing_constraints:
                decision.abort_reasons.append(
                    AbortReason(code="ERR_PREFLIGHT_CONSTRAINT", message=f"Missing constraint: {mc}")
                )
            for table_name, mismatches in target_preflight.type_mismatches.items():
                for col, detail in mismatches.items():
                    decision.abort_reasons.append(
                        AbortReason(code="ERR_PREFLIGHT_TYPE", message=f"Type mismatch on {table_name}.{col}: {detail}")
                    )

        # 2. Schema compatibility check
        if not decision.schema_compatible:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_SCHEMA_INCOMPATIBLE",
                    message=f"Schema version mismatch: source={cutover_report.source_schema_version}, "
                            f"target={cutover_report.target_schema_version}, expected>={CURRENT_SCHEMA_VERSION}.",
                )
            )

        # 3. Propagate ALL reconciler abort reasons (counts, full key set, content hashes, inspection errors)
        for ar in cutover_report.abort_reasons:
            if not any(existing.code == ar.code for existing in decision.abort_reasons):
                decision.abort_reasons.append(ar)

        if not cutover_report.all_counts_match:
            if not any(ar.code.startswith("ERR_COUNT_MISMATCH") or ar.code == "ERR_DATA_MISMATCH" for ar in decision.abort_reasons):
                decision.abort_reasons.append(
                    AbortReason(
                        code="ERR_DATA_MISMATCH",
                        message="Source and target record count reconciliation failed.",
                    )
                )

        if not cutover_report.is_safe_to_cutover and not decision.abort_reasons:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_RECONCILIATION_UNSAFE",
                    message="Staging cutover reconciler determined target repository is unsafe for cutover.",
                )
            )

        # 4. Verified backup prerequisite
        if not backup_verified:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_NO_VERIFIED_BACKUP",
                    message="Prerequisite verified backup not present before cutover.",
                )
            )

        # 5. Rollback plan prerequisite
        if not rollback_ready:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_NO_ROLLBACK_PLAN",
                    message="Rollback plan or restore point is not ready.",
                )
            )

        # 6. Zero financial and token cost invariant
        if not zero_cost_verified:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_COST_INVARIANT_VIOLATION",
                    message="Non-zero financial or token cost detected ($0.00 / 0 TRY invariant violated).",
                )
            )

        # 7. Explicit Product Owner sign-off
        if not product_owner_signed_off:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_NO_PO_SIGNOFF",
                    message="Explicit Product Owner sign-off is required before staging cutover.",
                )
            )

        decision.can_proceed = (
            decision.all_preflights_passed
            and decision.schema_compatible
            and decision.data_reconciliation_passed
            and cutover_report.is_safe_to_cutover
            and decision.backup_verified
            and decision.rollback_ready
            and decision.zero_cost_verified
            and decision.product_owner_signed_off
            and len(decision.abort_reasons) == 0
        )

        return decision


class StagingCutoverReconciler:
    """Reconciles source and target staging repositories for cutover readiness."""

    def __init__(
        self,
        source_repo: StateRepository,
        target_repo: StateRepository,
        allow_operational_db: bool = False,
    ):
        self.source_repo = source_repo
        self.target_repo = target_repo
        self._assert_isolation(allow_operational_db)

    def _assert_isolation(self, allow_operational_db: bool) -> None:
        """Enforce strict isolation against operational data/radar.db."""
        if not allow_operational_db:
            for r in (self.source_repo, self.target_repo):
                if isinstance(r, SqliteStateRepository):
                    db_path = getattr(r, "db_path", None)
                    if db_path is not None:
                        resolved = Path(db_path).resolve()
                        prod_path = Path("data/radar.db").resolve()
                        if resolved == prod_path or (resolved.name == "radar.db" and "data" in resolved.parts):
                            raise PermissionError(
                                "Safety violation: StagingCutoverReconciler cannot operate on operational data/radar.db."
                            )

    def reconcile(self) -> StagingCutoverReport:
        """Execute non-mutating source/target reconciliation across schema v6 tables."""
        report = StagingCutoverReport(
            source_type=type(self.source_repo).__name__,
            target_type=type(self.target_repo).__name__,
        )

        # 1. Version checks (strictly via read-only execute_read_query to prevent creating missing SQLite files)
        try:
            src_ver_rows = execute_read_query(self.source_repo, "SELECT MAX(version) AS ver FROM schema_migrations")
            src_raw = src_ver_rows[0]["ver"] if src_ver_rows else None
            report.source_schema_version = int(src_raw) if src_raw is not None else 0

            tgt_ver_rows = execute_read_query(self.target_repo, "SELECT MAX(version) AS ver FROM schema_migrations")
            tgt_raw = tgt_ver_rows[0]["ver"] if tgt_ver_rows else None
            report.target_schema_version = int(tgt_raw) if tgt_raw is not None else 0

            if report.source_schema_version < CURRENT_SCHEMA_VERSION:
                report.abort_reasons.append(
                    AbortReason(
                        code="ERR_SOURCE_VERSION",
                        message=f"Source schema version {report.source_schema_version} is below required v{CURRENT_SCHEMA_VERSION}",
                    )
                )
            if report.target_schema_version < CURRENT_SCHEMA_VERSION:
                report.abort_reasons.append(
                    AbortReason(
                        code="ERR_TARGET_VERSION",
                        message=f"Target schema version {report.target_schema_version} is below required v{CURRENT_SCHEMA_VERSION}",
                    )
                )
        except Exception as exc:
            report.abort_reasons.append(
                AbortReason(code="ERR_VERSION_CHECK_FAILED", message=f"Version check failed: {type(exc).__name__}")
            )

        # 2. Table-by-table record count, complete primary key set, and content parity reconciliation
        for table_name, expected_cols in SCHEMA_V6_TABLES.items():
            pk_col = expected_cols[0]
            rec = TableReconciliation(table_name=table_name)
            try:
                # Count queries
                s_count_rows = execute_read_query(self.source_repo, f"SELECT COUNT(*) as cnt FROM {table_name}")
                t_count_rows = execute_read_query(self.target_repo, f"SELECT COUNT(*) as cnt FROM {table_name}")
                rec.source_count = int(s_count_rows[0].get("cnt", 0)) if s_count_rows else 0
                rec.target_count = int(t_count_rows[0].get("cnt", 0)) if t_count_rows else 0
                rec.count_matches = rec.source_count == rec.target_count

                if not rec.count_matches:
                    report.abort_reasons.append(
                        AbortReason(
                            code=f"ERR_COUNT_MISMATCH_{table_name.upper()}",
                            message=f"Table '{table_name}' count mismatch: source={rec.source_count}, target={rec.target_count}",
                        )
                    )

                # Complete primary key set parity check (all keys, not only samples)
                s_pk_rows = execute_read_query(
                    self.source_repo, f"SELECT {pk_col} FROM {table_name} ORDER BY {pk_col} ASC"
                )
                t_pk_rows = execute_read_query(
                    self.target_repo, f"SELECT {pk_col} FROM {table_name} ORDER BY {pk_col} ASC"
                )
                s_all_keys = [str(r.get(pk_col)) for r in s_pk_rows if r.get(pk_col) is not None]
                t_all_keys = [str(r.get(pk_col)) for r in t_pk_rows if r.get(pk_col) is not None]
                rec.source_sample_keys = [mask_text_chat_ids(k) for k in s_all_keys[:10]]
                rec.target_sample_keys = [mask_text_chat_ids(k) for k in t_all_keys[:10]]
                rec.keys_match = s_all_keys == t_all_keys

                if not rec.keys_match and (s_all_keys or t_all_keys):
                    diff_keys = list(set(s_all_keys) ^ set(t_all_keys))
                    if not diff_keys and len(s_all_keys) == len(t_all_keys):
                        for i, (sk, tk) in enumerate(zip(s_all_keys, t_all_keys)):
                            if sk != tk:
                                diff_keys.append(f"idx {i}: source={sk} vs target={tk}")
                                break
                    rec.mismatched_keys_sample = [mask_text_chat_ids(str(k)) for k in diff_keys[:5]]
                    report.abort_reasons.append(
                        AbortReason(
                            code=f"ERR_KEY_MISMATCH_{table_name.upper()}",
                            message=f"Table '{table_name}' complete primary key set differs between source and target "
                                    f"(mismatches sample: {rec.mismatched_keys_sample})",
                        )
                    )

                # Content parity verification via deterministic SHA-256 hash.
                # For schema_migrations, applied_at and description differ by backend/time.
                # Reconcile applied version set, preserving strict full-column content hashes for all business tables.
                hash_cols = ["version"] if table_name == "schema_migrations" else expected_cols
                cols_sql = ", ".join(hash_cols)
                s_rows = execute_read_query(
                    self.source_repo, f"SELECT {cols_sql} FROM {table_name} ORDER BY {pk_col} ASC"
                )
                t_rows = execute_read_query(
                    self.target_repo, f"SELECT {cols_sql} FROM {table_name} ORDER BY {pk_col} ASC"
                )
                rec.source_content_hash = compute_table_content_hash(s_rows, hash_cols)
                rec.target_content_hash = compute_table_content_hash(t_rows, hash_cols)
                rec.content_matches = rec.source_content_hash == rec.target_content_hash

                if not rec.content_matches:
                    report.abort_reasons.append(
                        AbortReason(
                            code=f"ERR_CONTENT_MISMATCH_{table_name.upper()}",
                            message=f"Table '{table_name}' content hash mismatch: "
                                    f"source={rec.source_content_hash[:8]}..., target={rec.target_content_hash[:8]}...",
                        )
                    )

            except Exception as exc:
                rec.count_matches = False
                rec.keys_match = False
                rec.content_matches = False
                # Only the exception class is reported: messages may embed record contents.
                report.abort_reasons.append(
                    AbortReason(
                        code=f"ERR_TABLE_INSPECT_{table_name.upper()}",
                        message=f"Failed reconciling table '{table_name}': {type(exc).__name__}",
                    )
                )

            report.table_reconciliations.append(rec)

        report.idempotent_dry_run_verified = len(report.abort_reasons) == 0
        return report
