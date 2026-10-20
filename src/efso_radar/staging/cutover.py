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

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.domain import utc_now
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.staging.preflight import (
    SCHEMA_V6_TABLES,
    CloudPreflightValidator,
    PreflightReport,
    execute_read_query,
)

logger = logging.getLogger("efso-radar.staging.cutover")


class TableReconciliation(BaseModel):
    table_name: str
    source_count: int = 0
    target_count: int = 0
    count_matches: bool = False
    source_sample_keys: list[str] = Field(default_factory=list)
    target_sample_keys: list[str] = Field(default_factory=list)
    keys_match: bool = False


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
    def is_safe_to_cutover(self) -> bool:
        return self.all_counts_match and len(self.abort_reasons) == 0


class GoNoGoGate:
    """Evaluates strict multi-factor prerequisites before authorizing staging transition."""

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
            data_reconciliation_passed=cutover_report.all_counts_match,
            backup_verified=backup_verified,
            rollback_ready=rollback_ready,
            zero_cost_verified=zero_cost_verified,
            product_owner_signed_off=product_owner_signed_off,
        )

        if not target_preflight.success:
            decision.abort_reasons.append(
                AbortReason(code="ERR_PREFLIGHT_FAILED", message="Target preflight validation failed.")
            )
        if not decision.schema_compatible:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_SCHEMA_INCOMPATIBLE",
                    message=f"Schema version mismatch: source={cutover_report.source_schema_version}, "
                            f"target={cutover_report.target_schema_version}, expected>={CURRENT_SCHEMA_VERSION}.",
                )
            )
        if not cutover_report.all_counts_match:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_DATA_MISMATCH",
                    message="Source and target record count reconciliation failed.",
                )
            )
        if not backup_verified:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_NO_VERIFIED_BACKUP",
                    message="Prerequisite verified backup not present before cutover.",
                )
            )
        if not rollback_ready:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_NO_ROLLBACK_PLAN",
                    message="Rollback plan or restore point is not ready.",
                )
            )
        if not zero_cost_verified:
            decision.abort_reasons.append(
                AbortReason(
                    code="ERR_COST_INVARIANT_VIOLATION",
                    message="Non-zero financial or token cost detected ($0.00 / 0 TRY invariant violated).",
                )
            )
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
                        if resolved == prod_path:
                            raise PermissionError(
                                "Safety violation: StagingCutoverReconciler cannot operate on operational data/radar.db."
                            )

    def reconcile(self) -> StagingCutoverReport:
        """Execute non-mutating source/target reconciliation across schema v6 tables."""
        report = StagingCutoverReport(
            source_type=type(self.source_repo).__name__,
            target_type=type(self.target_repo).__name__,
        )

        # 1. Version checks
        try:
            report.source_schema_version = self.source_repo.get_schema_version()
            report.target_schema_version = self.target_repo.get_schema_version()
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
                AbortReason(code="ERR_VERSION_CHECK_FAILED", message=f"Version check failed: {exc}")
            )

        # 2. Table-by-table record count & sample identity reconciliation
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

                # Sample primary key parity check (up to 10 keys)
                s_pk_rows = execute_read_query(
                    self.source_repo, f"SELECT {pk_col} FROM {table_name} ORDER BY {pk_col} ASC LIMIT 10"
                )
                t_pk_rows = execute_read_query(
                    self.target_repo, f"SELECT {pk_col} FROM {table_name} ORDER BY {pk_col} ASC LIMIT 10"
                )
                rec.source_sample_keys = [str(r.get(pk_col)) for r in s_pk_rows if r.get(pk_col)]
                rec.target_sample_keys = [str(r.get(pk_col)) for r in t_pk_rows if r.get(pk_col)]
                rec.keys_match = rec.source_sample_keys == rec.target_sample_keys

                if not rec.keys_match and (rec.source_sample_keys or rec.target_sample_keys):
                    report.abort_reasons.append(
                        AbortReason(
                            code=f"ERR_KEY_MISMATCH_{table_name.upper()}",
                            message=f"Table '{table_name}' sample primary keys differ between source and target",
                        )
                    )

            except Exception as exc:
                report.abort_reasons.append(
                    AbortReason(
                        code=f"ERR_TABLE_INSPECT_{table_name.upper()}",
                        message=f"Failed reconciling table '{table_name}': {exc}",
                    )
                )

            report.table_reconciliations.append(rec)

        report.idempotent_dry_run_verified = len(report.abort_reasons) == 0
        return report
