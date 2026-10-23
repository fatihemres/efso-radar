"""Read-only delivery recovery inspection workflow for Efso Radar staging environments.

Provides non-mutating operator diagnostic inspection for:
- DISPATCHING deliveries (stuck claims from interrupted runners)
- DELIVERY_UNCERTAIN deliveries (network timeouts requiring manual inspection)
- Conflicting or duplicate delivery identities
- Expired worker leases in run_locks or notifications
- Incomplete scheduled executions and recovery checkpoints

Safety Invariants:
- Strictly READ-ONLY: Zero database writes or status transitions.
- Never automatically mutates DELIVERY_UNCERTAIN to SENT, FAILED, or PENDING.
- Never automatically resends ambiguous Telegram messages.
- Sanitizes all recipient chat IDs and tokens in diagnostics and summaries.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.domain import DeliveryStatus, mask_chat_id, utc_now
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.migration_ledger import mask_text_chat_ids
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.staging.preflight import execute_read_query

logger = logging.getLogger("efso-radar.staging.recovery_inspection")


def sanitize_diagnostic_text(text: Optional[str]) -> str:
    """Sanitize strings for logs, errors, and reports by redacting sensitive tokens and chat IDs."""
    if not text:
        return ""
    val = str(text)
    # Mask chat IDs
    val = mask_text_chat_ids(val)
    # Redact Telegram bot tokens (e.g. 123456789:ABCdefGhIjkLmNoPqRsTuVwXyZ)
    val = re.sub(r"\b\d{8,12}:[A-Za-z0-9_-]{30,50}\b", "[REDACTED_BOT_TOKEN]", val)
    val = re.sub(r"bot\d+:[A-Za-z0-9_-]+", "bot[REDACTED_BOT_TOKEN]", val)
    # Redact bearer tokens
    val = re.sub(r"Bearer\s+[A-Za-z0-9._-]+", "Bearer [REDACTED_TOKEN]", val, flags=re.IGNORECASE)
    # Redact tokens/passwords in query params or connection strings
    val = re.sub(r"(authToken|token|password|key|auth)=([^\s&]+)", r"\1=[REDACTED]", val, flags=re.IGNORECASE)
    return val


class UncertainDeliveryRecord(BaseModel):
    delivery_id: str
    message_key: str
    recipient_role: str
    masked_chat_id: str
    category: str
    attempt_count: int
    created_at: str
    updated_at: str
    error_message: Optional[str] = None


class StuckDispatchingRecord(BaseModel):
    delivery_id: str
    message_key: str
    recipient_role: str
    masked_chat_id: str
    category: str
    created_at: str
    updated_at: str
    stuck_minutes: float


class DuplicateIdentityRecord(BaseModel):
    masked_chat_id: str
    message_key: str
    duplicate_count: int


class ExpiredLockRecord(BaseModel):
    lock_key: str
    worker_id: str
    acquired_at: str
    expires_at: str
    expired_minutes_ago: float


class IncompleteExecutionRecord(BaseModel):
    run_id: str
    run_type: str
    worker_id: str
    started_at: str
    status: str
    error_message: Optional[str] = None


class RecoveryCheckpointRecord(BaseModel):
    state_key: str
    stage: str
    worker_id: str
    batch_id: Optional[str] = None
    created_at: str
    updated_at: str


REQUIRED_RECOVERY_INSPECTIONS = 6


class RecoveryInspectionReport(BaseModel):
    """Sanitized diagnostic report of recovery and delivery state inspection."""
    inspected_at: datetime = Field(default_factory=utc_now)
    repository_type: str = ""
    is_inspection_complete: bool = False
    inspections_attempted: int = 0
    inspections_succeeded: int = 0
    inspection_errors: list[str] = Field(default_factory=list)
    uncertain_deliveries: list[UncertainDeliveryRecord] = Field(default_factory=list)
    stuck_dispatching: list[StuckDispatchingRecord] = Field(default_factory=list)
    duplicate_identities: list[DuplicateIdentityRecord] = Field(default_factory=list)
    expired_locks: list[ExpiredLockRecord] = Field(default_factory=list)
    incomplete_executions: list[IncompleteExecutionRecord] = Field(default_factory=list)
    recovery_checkpoints: list[RecoveryCheckpointRecord] = Field(default_factory=list)
    actionable_recommendations: list[str] = Field(default_factory=list)

    @property
    def _all_required_inspections_succeeded(self) -> bool:
        return (
            self.is_inspection_complete
            and self.inspections_attempted >= REQUIRED_RECOVERY_INSPECTIONS
            and self.inspections_succeeded == self.inspections_attempted
            and len(self.inspection_errors) == 0
        )

    @property
    def has_recovery_issues(self) -> bool:
        """Returns True if any recovery anomalies exist OR if inspection failed / is incomplete / never executed."""
        return (
            not self._all_required_inspections_succeeded
            or bool(self.uncertain_deliveries)
            or bool(self.stuck_dispatching)
            or bool(self.duplicate_identities)
            or bool(self.expired_locks)
            or bool(self.incomplete_executions)
            or bool(self.recovery_checkpoints)
        )

    @property
    def is_all_clear(self) -> bool:
        """Strictly True ONLY if all required inspections succeeded with zero errors and zero issues."""
        return self._all_required_inspections_succeeded and not (
            bool(self.uncertain_deliveries)
            or bool(self.stuck_dispatching)
            or bool(self.duplicate_identities)
            or bool(self.expired_locks)
            or bool(self.incomplete_executions)
            or bool(self.recovery_checkpoints)
        )

    def summary(self) -> str:
        if not self._all_required_inspections_succeeded:
            status_text = "INSPECTION FAILED / INCOMPLETE"
        elif self.has_recovery_issues:
            status_text = "ISSUES DETECTED"
        else:
            status_text = "ALL CLEAR"

        lines = [
            "=== DELIVERY RECOVERY & OPERATIONAL INSPECTION REPORT ===",
            f"Timestamp: {self.inspected_at.isoformat()}",
            f"Repository: {self.repository_type}",
            f"Status: {status_text}",
            f"Inspection Completion: {self.inspections_succeeded}/{self.inspections_attempted} succeeded",
            f"DELIVERY_UNCERTAIN Records: {len(self.uncertain_deliveries)}",
            f"Stuck DISPATCHING Claims: {len(self.stuck_dispatching)}",
            f"Duplicate Identity Keys: {len(self.duplicate_identities)}",
            f"Expired Worker Locks: {len(self.expired_locks)}",
            f"Incomplete Executions: {len(self.incomplete_executions)}",
            f"Active Recovery Checkpoints: {len(self.recovery_checkpoints)}",
        ]

        if self.inspection_errors:
            lines.append("\n[!] Inspection Failures & Query Errors (Failed Closed):")
            for err in self.inspection_errors:
                lines.append(f"  - {err}")

        if self.uncertain_deliveries:
            lines.append("\n[!] DELIVERY_UNCERTAIN Deliveries (Requires Operator Investigation):")
            for u in self.uncertain_deliveries:
                lines.append(
                    f"  - Delivery ID: {u.delivery_id} | Role: {u.recipient_role} | Chat: {u.masked_chat_id} | "
                    f"Key: {u.message_key} | Attempts: {u.attempt_count} | Error: {u.error_message or 'None'}"
                )

        if self.stuck_dispatching:
            lines.append("\n[!] Stuck DISPATCHING Claims (Potential Runner Crash):")
            for s in self.stuck_dispatching:
                lines.append(
                    f"  - Delivery ID: {s.delivery_id} | Role: {s.recipient_role} | Chat: {s.masked_chat_id} | "
                    f"Stuck: {s.stuck_minutes:.1f} min ago"
                )

        if self.duplicate_identities:
            lines.append("\n[!] Conflicting Duplicate Identity Keys:")
            for d in self.duplicate_identities:
                lines.append(
                    f"  - Chat: {d.masked_chat_id} | Key: {d.message_key} | Count: {d.duplicate_count}"
                )

        if self.expired_locks:
            lines.append("\n[!] Expired Worker Locks:")
            for l in self.expired_locks:
                lines.append(
                    f"  - Lock: {l.lock_key} | Worker: {l.worker_id} | Expired: {l.expired_minutes_ago:.1f} min ago"
                )

        if self.incomplete_executions:
            lines.append("\n[!] Incomplete / Failed Executions:")
            for e in self.incomplete_executions:
                lines.append(
                    f"  - Run ID: {e.run_id} | Type: {e.run_type} | Status: {e.status} | Worker: {e.worker_id} | "
                    f"Error: {e.error_message or 'None'}"
                )

        if self.recovery_checkpoints:
            lines.append("\n[!] Active Recovery Checkpoints:")
            for c in self.recovery_checkpoints:
                lines.append(
                    f"  - Stage: {c.stage} | Key: {c.state_key} | Worker: {c.worker_id} | Batch: {c.batch_id}"
                )

        if self.actionable_recommendations:
            lines.append("\nRecommended Operator Actions:")
            for rec in self.actionable_recommendations:
                lines.append(f"  * {rec}")

        return "\n".join(lines)


class DeliveryRecoveryInspector:
    """Read-only operator inspection tool for outbox and recovery state."""

    def __init__(self, repo: StateRepository, allow_operational_db: bool = False):
        self.repo = repo
        self._assert_isolation(allow_operational_db)

    def _assert_isolation(self, allow_operational_db: bool) -> None:
        """Ensure inspector fails closed against operational production database."""
        if not allow_operational_db and isinstance(self.repo, SqliteStateRepository):
            db_path = getattr(self.repo, "db_path", None)
            if db_path is not None:
                resolved = Path(db_path).resolve()
                prod_path = Path("data/radar.db").resolve()
                if resolved == prod_path or (resolved.name == "radar.db" and "data" in resolved.parts):
                    raise PermissionError(
                        "Safety violation: DeliveryRecoveryInspector cannot be invoked directly on "
                        "operational database data/radar.db without explicit authorization."
                    )

    def inspect(self, now: Optional[datetime] = None, stuck_claim_threshold_minutes: float = 15.0) -> RecoveryInspectionReport:
        """Perform non-mutating inspection of all delivery outbox and recovery state tables."""
        ref_now = now or utc_now()
        report = RecoveryInspectionReport(
            inspected_at=ref_now,
            repository_type=type(self.repo).__name__,
            is_inspection_complete=False,
        )

        self._inspect_uncertain_deliveries(report)
        self._inspect_stuck_dispatching(report, ref_now, stuck_claim_threshold_minutes)
        self._inspect_duplicate_identities(report)
        self._inspect_expired_locks(report, ref_now)
        self._inspect_incomplete_executions(report)
        self._inspect_recovery_checkpoints(report)

        report.is_inspection_complete = (
            report.inspections_attempted >= REQUIRED_RECOVERY_INSPECTIONS
            and report.inspections_succeeded == report.inspections_attempted
            and len(report.inspection_errors) == 0
        )
        self._build_recommendations(report)

        return report

    def _inspect_uncertain_deliveries(self, report: RecoveryInspectionReport) -> None:
        report.inspections_attempted += 1
        try:
            rows = execute_read_query(
                self.repo,
                """
                SELECT delivery_id, message_key, recipient_role, recipient_chat_id,
                       category, attempt_count, created_at, updated_at, error_message
                FROM telegram_delivery_records
                WHERE status = ?
                ORDER BY updated_at ASC
                """,
                [DeliveryStatus.DELIVERY_UNCERTAIN.value],
            )
            for r in rows:
                report.uncertain_deliveries.append(
                    UncertainDeliveryRecord(
                        delivery_id=sanitize_diagnostic_text(str(r.get("delivery_id", ""))),
                        message_key=sanitize_diagnostic_text(str(r.get("message_key", ""))),
                        recipient_role=sanitize_diagnostic_text(str(r.get("recipient_role", ""))),
                        masked_chat_id=mask_chat_id(str(r.get("recipient_chat_id", ""))),
                        category=sanitize_diagnostic_text(str(r.get("category", ""))),
                        attempt_count=int(r.get("attempt_count", 0)),
                        created_at=str(r.get("created_at", "")),
                        updated_at=str(r.get("updated_at", "")),
                        error_message=sanitize_diagnostic_text(r.get("error_message")),
                    )
                )
            report.inspections_succeeded += 1
        except Exception as exc:
            report.is_inspection_complete = False
            # Report only the safe exception category; raw exception text may embed database values or secrets.
            err_category = type(exc).__name__
            report.inspection_errors.append(f"Uncertain deliveries inspection failed: {err_category}")
            logger.warning("Could not inspect uncertain deliveries: %s", err_category)

    def _inspect_stuck_dispatching(
        self, report: RecoveryInspectionReport, now: datetime, threshold_minutes: float
    ) -> None:
        report.inspections_attempted += 1
        try:
            rows = execute_read_query(
                self.repo,
                """
                SELECT delivery_id, message_key, recipient_role, recipient_chat_id,
                       category, created_at, updated_at
                FROM telegram_delivery_records
                WHERE status = ?
                ORDER BY updated_at ASC
                """,
                [DeliveryStatus.DISPATCHING.value],
            )
            for r in rows:
                updated_str = str(r.get("updated_at", ""))
                try:
                    updated_dt = datetime.fromisoformat(updated_str)
                    if updated_dt.tzinfo is None:
                        updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                    stuck_minutes = max(0.0, (now - updated_dt).total_seconds() / 60.0)
                except Exception:
                    stuck_minutes = threshold_minutes + 1.0

                if stuck_minutes >= threshold_minutes:
                    report.stuck_dispatching.append(
                        StuckDispatchingRecord(
                            delivery_id=sanitize_diagnostic_text(str(r.get("delivery_id", ""))),
                            message_key=sanitize_diagnostic_text(str(r.get("message_key", ""))),
                            recipient_role=sanitize_diagnostic_text(str(r.get("recipient_role", ""))),
                            masked_chat_id=mask_chat_id(str(r.get("recipient_chat_id", ""))),
                            category=sanitize_diagnostic_text(str(r.get("category", ""))),
                            created_at=str(r.get("created_at", "")),
                            updated_at=updated_str,
                            stuck_minutes=stuck_minutes,
                        )
                    )
            report.inspections_succeeded += 1
        except Exception as exc:
            report.is_inspection_complete = False
            err_category = type(exc).__name__
            report.inspection_errors.append(f"Stuck dispatching inspection failed: {err_category}")
            logger.warning("Could not inspect stuck dispatching: %s", err_category)

    def _inspect_duplicate_identities(self, report: RecoveryInspectionReport) -> None:
        report.inspections_attempted += 1
        try:
            rows = execute_read_query(
                self.repo,
                """
                SELECT recipient_chat_id, message_key, COUNT(*) as cnt
                FROM telegram_delivery_records
                GROUP BY recipient_chat_id, message_key
                HAVING COUNT(*) > 1
                """,
            )
            for r in rows:
                report.duplicate_identities.append(
                    DuplicateIdentityRecord(
                        masked_chat_id=mask_chat_id(str(r.get("recipient_chat_id", ""))),
                        message_key=sanitize_diagnostic_text(str(r.get("message_key", ""))),
                        duplicate_count=int(r.get("cnt", 0)),
                    )
                )
            report.inspections_succeeded += 1
        except Exception as exc:
            report.is_inspection_complete = False
            err_category = type(exc).__name__
            report.inspection_errors.append(f"Duplicate identities inspection failed: {err_category}")
            logger.warning("Could not inspect duplicate identities: %s", err_category)

    def _inspect_expired_locks(self, report: RecoveryInspectionReport, now: datetime) -> None:
        report.inspections_attempted += 1
        try:
            rows = execute_read_query(
                self.repo,
                "SELECT lock_key, worker_id, acquired_at, expires_at FROM run_locks",
            )
            parse_errors: list[str] = []
            for r in rows:
                lock_key_clean = sanitize_diagnostic_text(str(r.get("lock_key", "")))
                worker_id_clean = sanitize_diagnostic_text(str(r.get("worker_id", "")))
                acquired_clean = sanitize_diagnostic_text(str(r.get("acquired_at", "")))
                raw_expires = r.get("expires_at")
                expires_str = str(raw_expires).strip() if raw_expires is not None else ""
                try:
                    if not expires_str:
                        raise ValueError("empty_lock_expiration_timestamp")
                    expires_dt = datetime.fromisoformat(expires_str)
                    if expires_dt.tzinfo is None:
                        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
                    if expires_dt < now:
                        expired_min = (now - expires_dt).total_seconds() / 60.0
                        report.expired_locks.append(
                            ExpiredLockRecord(
                                lock_key=lock_key_clean,
                                worker_id=worker_id_clean,
                                acquired_at=acquired_clean,
                                expires_at=sanitize_diagnostic_text(expires_str),
                                expired_minutes_ago=expired_min,
                            )
                        )
                except Exception as ts_exc:
                    # Report only the safe error category (never echo the raw unparseable database value).
                    reason_cat = "empty_timestamp" if not expires_str else f"invalid_iso_timestamp ({type(ts_exc).__name__})"
                    sanitized_ts_err = sanitize_diagnostic_text(
                        f"Invalid lock expiration timestamp for lock '{lock_key_clean}': {reason_cat}"
                    )
                    parse_errors.append(sanitized_ts_err)

            if parse_errors:
                report.is_inspection_complete = False
                for err in parse_errors:
                    report.inspection_errors.append(f"Expired locks inspection failed: {err}")
                    logger.warning("Could not parse run_locks expiration timestamp: %s", err)
            else:
                report.inspections_succeeded += 1
        except Exception as exc:
            report.is_inspection_complete = False
            err_category = type(exc).__name__
            report.inspection_errors.append(f"Expired locks inspection failed: {err_category}")
            logger.warning("Could not inspect run locks: %s", err_category)

    def _inspect_incomplete_executions(self, report: RecoveryInspectionReport) -> None:
        report.inspections_attempted += 1
        try:
            rows = execute_read_query(
                self.repo,
                """
                SELECT run_id, run_type, worker_id, started_at, status, error_message
                FROM scheduled_execution_history
                WHERE status IN ('STARTED', 'RUNNING', 'FAILED')
                ORDER BY started_at DESC
                LIMIT 20
                """,
            )
            for r in rows:
                report.incomplete_executions.append(
                    IncompleteExecutionRecord(
                        run_id=sanitize_diagnostic_text(str(r.get("run_id", ""))),
                        run_type=sanitize_diagnostic_text(str(r.get("run_type", ""))),
                        worker_id=sanitize_diagnostic_text(str(r.get("worker_id", ""))),
                        started_at=str(r.get("started_at", "")),
                        status=sanitize_diagnostic_text(str(r.get("status", ""))),
                        error_message=sanitize_diagnostic_text(r.get("error_message")),
                    )
                )
            report.inspections_succeeded += 1
        except Exception as exc:
            report.is_inspection_complete = False
            err_category = type(exc).__name__
            report.inspection_errors.append(f"Incomplete executions inspection failed: {err_category}")
            logger.warning("Could not inspect execution history: %s", err_category)

    def _inspect_recovery_checkpoints(self, report: RecoveryInspectionReport) -> None:
        report.inspections_attempted += 1
        try:
            rows = execute_read_query(
                self.repo,
                """
                SELECT state_key, stage, worker_id, batch_id, created_at, updated_at
                FROM durable_recovery_state
                ORDER BY updated_at DESC
                LIMIT 20
                """,
            )
            for r in rows:
                report.recovery_checkpoints.append(
                    RecoveryCheckpointRecord(
                        state_key=sanitize_diagnostic_text(str(r.get("state_key", ""))),
                        stage=sanitize_diagnostic_text(str(r.get("stage", ""))),
                        worker_id=sanitize_diagnostic_text(str(r.get("worker_id", ""))),
                        batch_id=sanitize_diagnostic_text(r.get("batch_id")),
                        created_at=str(r.get("created_at", "")),
                        updated_at=str(r.get("updated_at", "")),
                    )
                )
            report.inspections_succeeded += 1
        except Exception as exc:
            report.is_inspection_complete = False
            err_category = type(exc).__name__
            report.inspection_errors.append(f"Recovery checkpoints inspection failed: {err_category}")
            logger.warning("Could not inspect recovery checkpoints: %s", err_category)

    def _build_recommendations(self, report: RecoveryInspectionReport) -> None:
        if report.inspection_errors or not report.is_inspection_complete:
            report.actionable_recommendations.append(
                f"Inspection encountered {len(report.inspection_errors)} error(s) and failed closed. "
                "Resolve repository query/availability failures before relying on diagnostic status."
            )
        if report.uncertain_deliveries:
            report.actionable_recommendations.append(
                f"Verify external Telegram chat for {len(report.uncertain_deliveries)} DELIVERY_UNCERTAIN alerts. "
                "Do NOT auto-resend. Manually reconcile in outbox table once delivery status is verified."
            )
        if report.stuck_dispatching:
            report.actionable_recommendations.append(
                f"{len(report.stuck_dispatching)} DISPATCHING claims have exceeded lease timeout. "
                "Investigate runner crashes before releasing claims."
            )
        if report.duplicate_identities:
            report.actionable_recommendations.append(
                f"Detected {len(report.duplicate_identities)} duplicate message keys. "
                "Ensure unique index uq_recipient_message is enforced on target staging database."
            )
        if report.expired_locks:
            report.actionable_recommendations.append(
                f"{len(report.expired_locks)} stale run locks detected. "
                "Locks can be safely re-acquired by new scheduled workers under lease expiry policy."
            )
        if report.incomplete_executions or report.recovery_checkpoints:
            report.actionable_recommendations.append(
                "Incomplete executions or active recovery checkpoints found. "
                "Review scheduled execution logs before initiating staging cutover."
            )
