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


class RecoveryInspectionReport(BaseModel):
    """Sanitized diagnostic report of recovery and delivery state inspection."""
    inspected_at: datetime = Field(default_factory=utc_now)
    repository_type: str = ""
    uncertain_deliveries: list[UncertainDeliveryRecord] = Field(default_factory=list)
    stuck_dispatching: list[StuckDispatchingRecord] = Field(default_factory=list)
    duplicate_identities: list[DuplicateIdentityRecord] = Field(default_factory=list)
    expired_locks: list[ExpiredLockRecord] = Field(default_factory=list)
    incomplete_executions: list[IncompleteExecutionRecord] = Field(default_factory=list)
    recovery_checkpoints: list[RecoveryCheckpointRecord] = Field(default_factory=list)
    actionable_recommendations: list[str] = Field(default_factory=list)

    @property
    def has_recovery_issues(self) -> bool:
        return (
            bool(self.uncertain_deliveries)
            or bool(self.stuck_dispatching)
            or bool(self.duplicate_identities)
            or bool(self.expired_locks)
            or bool(self.incomplete_executions)
            or bool(self.recovery_checkpoints)
        )

    def summary(self) -> str:
        lines = [
            "=== DELIVERY RECOVERY & OPERATIONAL INSPECTION REPORT ===",
            f"Timestamp: {self.inspected_at.isoformat()}",
            f"Repository: {self.repository_type}",
            f"Status: {'ISSUES DETECTED' if self.has_recovery_issues else 'ALL CLEAR'}",
            f"DELIVERY_UNCERTAIN Records: {len(self.uncertain_deliveries)}",
            f"Stuck DISPATCHING Claims: {len(self.stuck_dispatching)}",
            f"Duplicate Identity Keys: {len(self.duplicate_identities)}",
            f"Expired Worker Locks: {len(self.expired_locks)}",
            f"Incomplete Executions: {len(self.incomplete_executions)}",
            f"Active Recovery Checkpoints: {len(self.recovery_checkpoints)}",
        ]

        if self.uncertain_deliveries:
            lines.append("\n[!] DELIVERY_UNCERTAIN Deliveries (Requires Operator Investigation):")
            for u in self.uncertain_deliveries:
                lines.append(
                    f"  - Delivery ID: {u.delivery_id} | Role: {u.recipient_role} | Chat: {u.masked_chat_id} | "
                    f"Key: {u.message_key} | Attempts: {u.attempt_count} | Error: {mask_text_chat_ids(u.error_message or 'None')}"
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
                    f"Error: {mask_text_chat_ids(e.error_message or 'None')}"
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
                if resolved == prod_path:
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
        )

        self._inspect_uncertain_deliveries(report)
        self._inspect_stuck_dispatching(report, ref_now, stuck_claim_threshold_minutes)
        self._inspect_duplicate_identities(report)
        self._inspect_expired_locks(report, ref_now)
        self._inspect_incomplete_executions(report)
        self._inspect_recovery_checkpoints(report)
        self._build_recommendations(report)

        return report

    def _inspect_uncertain_deliveries(self, report: RecoveryInspectionReport) -> None:
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
                        delivery_id=str(r.get("delivery_id", "")),
                        message_key=str(r.get("message_key", "")),
                        recipient_role=str(r.get("recipient_role", "")),
                        masked_chat_id=mask_chat_id(str(r.get("recipient_chat_id", ""))),
                        category=str(r.get("category", "")),
                        attempt_count=int(r.get("attempt_count", 0)),
                        created_at=str(r.get("created_at", "")),
                        updated_at=str(r.get("updated_at", "")),
                        error_message=mask_text_chat_ids(r.get("error_message")),
                    )
                )
        except Exception as exc:
            logger.debug("Could not inspect uncertain deliveries: %s", exc)

    def _inspect_stuck_dispatching(
        self, report: RecoveryInspectionReport, now: datetime, threshold_minutes: float
    ) -> None:
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
                            delivery_id=str(r.get("delivery_id", "")),
                            message_key=str(r.get("message_key", "")),
                            recipient_role=str(r.get("recipient_role", "")),
                            masked_chat_id=mask_chat_id(str(r.get("recipient_chat_id", ""))),
                            category=str(r.get("category", "")),
                            created_at=str(r.get("created_at", "")),
                            updated_at=updated_str,
                            stuck_minutes=stuck_minutes,
                        )
                    )
        except Exception as exc:
            logger.debug("Could not inspect stuck dispatching: %s", exc)

    def _inspect_duplicate_identities(self, report: RecoveryInspectionReport) -> None:
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
                        message_key=str(r.get("message_key", "")),
                        duplicate_count=int(r.get("cnt", 0)),
                    )
                )
        except Exception as exc:
            logger.debug("Could not inspect duplicate identities: %s", exc)

    def _inspect_expired_locks(self, report: RecoveryInspectionReport, now: datetime) -> None:
        try:
            rows = execute_read_query(
                self.repo,
                "SELECT lock_key, worker_id, acquired_at, expires_at FROM run_locks",
            )
            for r in rows:
                expires_str = str(r.get("expires_at", ""))
                try:
                    expires_dt = datetime.fromisoformat(expires_str)
                    if expires_dt.tzinfo is None:
                        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
                    if expires_dt < now:
                        expired_min = (now - expires_dt).total_seconds() / 60.0
                        report.expired_locks.append(
                            ExpiredLockRecord(
                                lock_key=str(r.get("lock_key", "")),
                                worker_id=str(r.get("worker_id", "")),
                                acquired_at=str(r.get("acquired_at", "")),
                                expires_at=expires_str,
                                expired_minutes_ago=expired_min,
                            )
                        )
                except Exception:
                    pass
        except Exception as exc:
            logger.debug("Could not inspect run locks: %s", exc)

    def _inspect_incomplete_executions(self, report: RecoveryInspectionReport) -> None:
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
                        run_id=str(r.get("run_id", "")),
                        run_type=str(r.get("run_type", "")),
                        worker_id=str(r.get("worker_id", "")),
                        started_at=str(r.get("started_at", "")),
                        status=str(r.get("status", "")),
                        error_message=mask_text_chat_ids(r.get("error_message")),
                    )
                )
        except Exception as exc:
            logger.debug("Could not inspect execution history: %s", exc)

    def _inspect_recovery_checkpoints(self, report: RecoveryInspectionReport) -> None:
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
                        state_key=str(r.get("state_key", "")),
                        stage=str(r.get("stage", "")),
                        worker_id=str(r.get("worker_id", "")),
                        batch_id=r.get("batch_id"),
                        created_at=str(r.get("created_at", "")),
                        updated_at=str(r.get("updated_at", "")),
                    )
                )
        except Exception as exc:
            logger.debug("Could not inspect recovery checkpoints: %s", exc)

    def _build_recommendations(self, report: RecoveryInspectionReport) -> None:
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
