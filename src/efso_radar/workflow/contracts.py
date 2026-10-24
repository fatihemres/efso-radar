"""Task interfaces and execution contracts for Efso Radar workflow operations.

Defines decoupled, independent contracts for the four future operational tasks:
1. SCAN_ONLY: Decoupled RSS discovery and candidate queueing (zero-AI, zero-Telegram).
2. EDITORIAL_DIGEST: Consolidated editorial digest dispatch to PRIMARY_EDITOR only.
3. ADMIN_HEALTH_REPORT: Daily operational health report to ADMIN only (maximum 1/day).
4. ENCRYPTED_BACKUP: Disaster recovery snapshot and AES-256-GCM packaging (DESIGN ONLY).

Safety Enforcement:
- OFFLINE_PREVIEW: Safely executable using synthetic fixtures, isolated repositories,
  mocked transports, and deterministic clocks (zero external network requests, zero spend).
- Ineligible schedule runs are strictly marked NO-GO and do NOT execute state-writing preview actions.
- LIVE_PRODUCTION: Strictly NOT IMPLEMENTED in Phase 5B.3D. Any attempt to invoke live
  production routines raises NotImplementedError and aborts fail-closed.
- Privacy Invariant: Output models expose only safe role labels and masked chat IDs.
  Raw chat IDs are strictly excluded from models, serialization, and logs.
"""
from __future__ import annotations

import abc
import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.config import Settings
from efso_radar.domain import utc_now
from efso_radar.repositories.base import StateRepository
from efso_radar.workflow.schedule import (
    DEFAULT_TOLERANCE_MINUTES,
    WorkflowTaskType,
    evaluate_task_eligibility,
    utc_to_istanbul,
    istanbul_to_utc,
    compute_window_identity,
)

logger = logging.getLogger("efso-radar.workflow.contracts")


class TaskExecutionMode(str, Enum):
    """Execution mode for workflow operations."""
    OFFLINE_PREVIEW = "OFFLINE_PREVIEW"
    LIVE_PRODUCTION = "LIVE_PRODUCTION"


class SafeRecipientInfo(BaseModel):
    """Sanitized recipient representation for preview models and logs with zero raw chat IDs."""
    role: str
    masked_chat_id: str
    label: str = ""

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class TaskExecutionResult(BaseModel):
    """Standardized result of a task contract execution."""
    task_type: WorkflowTaskType
    mode: TaskExecutionMode
    window_id: Optional[str] = None  # None when ineligible to avoid misleading window identities
    scheduled_slot_istanbul: str
    executed_at_utc: str
    status: str
    is_preview: bool = True
    recipients: list[SafeRecipientInfo] = Field(default_factory=list)
    network_calls_made: int = 0
    cost_usd: float = 0.0
    summary: dict[str, Any] = Field(default_factory=dict)
    message: str = ""


class BaseTaskContract(abc.ABC):
    """Abstract base contract for independent workflow operations."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()

    @property
    @abc.abstractmethod
    def task_type(self) -> WorkflowTaskType:
        """The workflow task type governed by this contract."""
        raise NotImplementedError

    def execute(
        self,
        mode: TaskExecutionMode,
        timestamp: datetime,
        repository: Optional[StateRepository] = None,
        tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    ) -> TaskExecutionResult:
        """Execute the contract in either preview or production mode.

        Validates mode strictly: unrecognized modes fail closed with ValueError.
        In Phase 5B.3D, LIVE_PRODUCTION is strictly prohibited and fails closed with NotImplementedError.
        """
        if not isinstance(mode, TaskExecutionMode):
            try:
                mode = TaskExecutionMode(str(mode))
            except (ValueError, TypeError):
                raise ValueError(
                    f"Unrecognized execution mode '{mode}'. Supported modes: {[m.value for m in TaskExecutionMode]}"
                )

        if mode == TaskExecutionMode.LIVE_PRODUCTION:
            raise NotImplementedError(
                f"Live production execution for {self.task_type.value} is strictly NOT IMPLEMENTED "
                "and unauthorized in Phase 5B.3D. All live automation, external dispatches, and cloud "
                "persistence remain disabled pending explicit Product Owner approval."
            )
        elif mode != TaskExecutionMode.OFFLINE_PREVIEW:
            raise ValueError(f"Unsupported execution mode: {mode}")

        return self.run_preview(timestamp=timestamp, repository=repository, tolerance_minutes=tolerance_minutes)

    @abc.abstractmethod
    def run_preview(
        self,
        timestamp: datetime,
        repository: Optional[StateRepository] = None,
        tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    ) -> TaskExecutionResult:
        """Execute hermetic offline preview with synthetic fixtures and zero network access."""
        raise NotImplementedError


class ScanOnlyTaskContract(BaseTaskContract):
    """Contract for decoupled RSS discovery and backlog queueing."""

    @property
    def task_type(self) -> WorkflowTaskType:
        return WorkflowTaskType.SCAN_ONLY

    def run_preview(
        self,
        timestamp: datetime,
        repository: Optional[StateRepository] = None,
        tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    ) -> TaskExecutionResult:
        from efso_radar.workflow.preview import run_offline_scan_preview

        eligibility = evaluate_task_eligibility(
            task_type=self.task_type,
            timestamp=timestamp,
            tolerance_minutes=tolerance_minutes,
            repository=repository,
            settings=self.settings,
        )

        # Do not execute state-writing preview operation when task is ineligible
        if not eligibility.is_eligible:
            return TaskExecutionResult(
                task_type=self.task_type,
                mode=TaskExecutionMode.OFFLINE_PREVIEW,
                window_id=None,  # No active window identity for ineligible runs
                scheduled_slot_istanbul=eligibility.slot_time_istanbul or "UNKNOWN",
                executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
                status=f"INELIGIBLE_{eligibility.status.value}",
                is_preview=True,
                recipients=[],
                network_calls_made=0,
                cost_usd=0.0,
                summary={
                    "mode": "OFFLINE_PREVIEW",
                    "task": "SCAN_ONLY",
                    "executed": False,
                    "reason": eligibility.reason,
                    "backlog_candidates_queued": 0,
                },
                message=f"Offline scan preview not executed: {eligibility.status.value}. {eligibility.reason}",
            )

        preview_summary = run_offline_scan_preview(
            timestamp=timestamp,
            settings=self.settings,
        )

        return TaskExecutionResult(
            task_type=self.task_type,
            mode=TaskExecutionMode.OFFLINE_PREVIEW,
            window_id=eligibility.window_id,
            scheduled_slot_istanbul=eligibility.slot_time_istanbul or "UNKNOWN",
            executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
            status="SUCCESS",
            is_preview=True,
            recipients=[],  # RSS discovery has zero external notification recipients
            network_calls_made=0,
            cost_usd=0.0,
            summary=preview_summary,
            message=f"Offline scan preview completed. Eligibility: {eligibility.status.value}. {eligibility.reason}",
        )


class EditorialDigestTaskContract(BaseTaskContract):
    """Contract for consolidated editorial digests dispatched exclusively to PRIMARY_EDITOR."""

    @property
    def task_type(self) -> WorkflowTaskType:
        return WorkflowTaskType.EDITORIAL_DIGEST

    def run_preview(
        self,
        timestamp: datetime,
        repository: Optional[StateRepository] = None,
        tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    ) -> TaskExecutionResult:
        from efso_radar.workflow.preview import run_offline_digest_preview

        eligibility = evaluate_task_eligibility(
            task_type=self.task_type,
            timestamp=timestamp,
            tolerance_minutes=tolerance_minutes,
            repository=repository,
            settings=self.settings,
        )

        # Enforce recipient routing invariant: PRIMARY_EDITOR receives editorial content; ADMIN does not.
        raw_editorial = self.settings.get_editorial_recipients()
        raw_admin = self.settings.get_administrative_recipients()
        admin_chat_ids = {r["chat_id"] for r in raw_admin}
        for rec in raw_editorial:
            if rec["chat_id"] in admin_chat_ids and rec.get("role") != "PRIMARY_EDITOR":
                raise AssertionError("CRITICAL SECURITY INVARIANT VIOLATION: ADMIN recipient found in editorial recipient list")

        # Sanitize recipients to exclude raw chat IDs
        safe_recipients = [
            SafeRecipientInfo(
                role=r["role"],
                masked_chat_id=r.get("masked_chat_id", "***"),
                label=r.get("label", ""),
            )
            for r in raw_editorial
        ]

        if not eligibility.is_eligible:
            return TaskExecutionResult(
                task_type=self.task_type,
                mode=TaskExecutionMode.OFFLINE_PREVIEW,
                window_id=None,
                scheduled_slot_istanbul=eligibility.slot_time_istanbul or "UNKNOWN",
                executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
                status=f"INELIGIBLE_{eligibility.status.value}",
                is_preview=True,
                recipients=safe_recipients,
                network_calls_made=0,
                cost_usd=0.0,
                summary={
                    "mode": "OFFLINE_PREVIEW",
                    "task": "EDITORIAL_DIGEST",
                    "executed": False,
                    "reason": eligibility.reason,
                },
                message=f"Offline editorial digest preview not executed: {eligibility.status.value}. {eligibility.reason}",
            )

        preview_summary = run_offline_digest_preview(
            timestamp=timestamp,
            settings=self.settings,
        )

        return TaskExecutionResult(
            task_type=self.task_type,
            mode=TaskExecutionMode.OFFLINE_PREVIEW,
            window_id=eligibility.window_id,
            scheduled_slot_istanbul=eligibility.slot_time_istanbul or "UNKNOWN",
            executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
            status="SUCCESS",
            is_preview=True,
            recipients=safe_recipients,
            network_calls_made=0,
            cost_usd=0.0,
            summary=preview_summary,
            message=f"Offline editorial digest preview completed. Eligibility: {eligibility.status.value}. {eligibility.reason}",
        )


class AdminHealthReportTaskContract(BaseTaskContract):
    """Contract for daily operational health report dispatched exclusively to ADMIN."""

    @property
    def task_type(self) -> WorkflowTaskType:
        return WorkflowTaskType.ADMIN_HEALTH_REPORT

    def run_preview(
        self,
        timestamp: datetime,
        repository: Optional[StateRepository] = None,
        tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    ) -> TaskExecutionResult:
        from efso_radar.workflow.preview import run_offline_admin_health_preview

        eligibility = evaluate_task_eligibility(
            task_type=self.task_type,
            timestamp=timestamp,
            tolerance_minutes=tolerance_minutes,
            repository=repository,
            settings=self.settings,
        )

        # Enforce recipient routing invariant: ADMIN receives health report; PRIMARY_EDITOR does not.
        raw_admin = self.settings.get_administrative_recipients()
        raw_editor = self.settings.get_editorial_recipients()
        editor_chat_ids = {r["chat_id"] for r in raw_editor}
        for rec in raw_admin:
            if rec["chat_id"] in editor_chat_ids and rec.get("role") != "ADMIN":
                raise AssertionError("CRITICAL SECURITY INVARIANT VIOLATION: PRIMARY_EDITOR recipient found in administrative recipient list")

        # Sanitize recipients to exclude raw chat IDs
        safe_recipients = [
            SafeRecipientInfo(
                role=r["role"],
                masked_chat_id=r.get("masked_chat_id", "***"),
                label=r.get("label", ""),
            )
            for r in raw_admin
        ]

        if not eligibility.is_eligible:
            return TaskExecutionResult(
                task_type=self.task_type,
                mode=TaskExecutionMode.OFFLINE_PREVIEW,
                window_id=None,
                scheduled_slot_istanbul=eligibility.slot_time_istanbul or "22:00",
                executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
                status=f"INELIGIBLE_{eligibility.status.value}",
                is_preview=True,
                recipients=safe_recipients,
                network_calls_made=0,
                cost_usd=0.0,
                summary={
                    "mode": "OFFLINE_PREVIEW",
                    "task": "ADMIN_HEALTH_REPORT",
                    "executed": False,
                    "reason": eligibility.reason,
                },
                message=f"Offline admin health report preview not executed: {eligibility.status.value}. {eligibility.reason}",
            )

        preview_summary = run_offline_admin_health_preview(
            timestamp=timestamp,
            settings=self.settings,
        )

        return TaskExecutionResult(
            task_type=self.task_type,
            mode=TaskExecutionMode.OFFLINE_PREVIEW,
            window_id=eligibility.window_id,
            scheduled_slot_istanbul=eligibility.slot_time_istanbul or "22:00",
            executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
            status="SUCCESS",
            is_preview=True,
            recipients=safe_recipients,
            network_calls_made=0,
            cost_usd=0.0,
            summary=preview_summary,
            message=f"Offline admin health report preview completed. Eligibility: {eligibility.status.value}. {eligibility.reason}",
        )


class EncryptedBackupTaskContract(BaseTaskContract):
    """Contract for daily disaster recovery backup snapshot (DESIGN ONLY)."""

    @property
    def task_type(self) -> WorkflowTaskType:
        return WorkflowTaskType.ENCRYPTED_BACKUP

    def run_preview(
        self,
        timestamp: datetime,
        repository: Optional[StateRepository] = None,
        tolerance_minutes: int = DEFAULT_TOLERANCE_MINUTES,
    ) -> TaskExecutionResult:
        from efso_radar.workflow.preview import run_offline_backup_preview

        eligibility = evaluate_task_eligibility(
            task_type=self.task_type,
            timestamp=timestamp,
            tolerance_minutes=tolerance_minutes,
            repository=repository,
            settings=self.settings,
        )

        if not eligibility.is_eligible:
            return TaskExecutionResult(
                task_type=self.task_type,
                mode=TaskExecutionMode.OFFLINE_PREVIEW,
                window_id=None,
                scheduled_slot_istanbul=eligibility.slot_time_istanbul or "02:15",
                executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
                status=f"INELIGIBLE_{eligibility.status.value}",
                is_preview=True,
                recipients=[],
                network_calls_made=0,
                cost_usd=0.0,
                summary={
                    "mode": "OFFLINE_PREVIEW",
                    "task": "ENCRYPTED_BACKUP",
                    "executed": False,
                    "reason": eligibility.reason,
                },
                message=f"Offline encrypted backup preview not executed: {eligibility.status.value}. {eligibility.reason}",
            )

        preview_summary = run_offline_backup_preview(
            timestamp=timestamp,
            settings=self.settings,
        )

        return TaskExecutionResult(
            task_type=self.task_type,
            mode=TaskExecutionMode.OFFLINE_PREVIEW,
            window_id=eligibility.window_id,
            scheduled_slot_istanbul=eligibility.slot_time_istanbul or "02:15",
            executed_at_utc=istanbul_to_utc(timestamp).isoformat(),
            status="SUCCESS",
            is_preview=True,
            recipients=[],  # Backup snapshots produce archive assets, not user chat notifications
            network_calls_made=0,
            cost_usd=0.0,
            summary=preview_summary,
            message=f"Offline encrypted backup preview completed (DESIGN ONLY). Eligibility: {eligibility.status.value}. {eligibility.reason}",
        )
