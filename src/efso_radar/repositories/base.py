"""State repository interface for Efso Radar.

Defines the contract for persistence, ensuring business logic is 100% portable
across local SQLite and remote Turso (libSQL over HTTP).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

from efso_radar.domain import (
    AdminOperationalReport,
    Article,
    DeliveryStatus,
    DurableRecoveryRecord,
    EditorialAssessment,
    EditorialKind,
    ExperimentBudgetRecord,
    NotificationRecord,
    OwnerFeedback,
    ScheduledExecutionRecord,
    Story,
    StoryUpdate,
    TelegramDeliveryRecord,
)


class StateRepository(ABC):
    """Abstract interface for radar state and persistence."""

    # --- Run Locking & Concurrency ---
    @abstractmethod
    def acquire_run_lock(self, worker_id: str, timeout_minutes: int = 15) -> bool:
        """Attempt to acquire a mutually exclusive run lock to prevent overlapping runs."""
        pass

    @abstractmethod
    def renew_run_lock(self, worker_id: str, extend_minutes: int = 15) -> bool:
        """Extend the run lock lease duration while execution is actively ongoing."""
        pass

    @abstractmethod
    def release_run_lock(self, worker_id: str) -> None:
        """Release the run lock held by worker_id."""
        pass

    # --- Article Deduplication & Tracking ---
    @abstractmethod
    def is_article_processed(self, article_id: str) -> bool:
        """Check if an article has already been seen and processed."""
        pass

    @abstractmethod
    def mark_articles_processed(self, articles: list[Article]) -> None:
        """Record articles as processed to prevent redundant reprocessing."""
        pass

    # --- Candidate Backlog & Lifecycle Integrity ---
    @abstractmethod
    def save_pending_candidates(self, candidates: list[Article]) -> None:
        """Persist fresh candidates into durable backlog so deprioritized items survive feed rotation."""
        pass

    @abstractmethod
    def get_pending_candidates(self, max_age_hours: int = 16, now: Optional[datetime] = None) -> list[Article]:
        """Retrieve unexpired candidates from backlog that are still awaiting evaluation."""
        pass

    @abstractmethod
    def mark_candidates_evaluated(self, article_ids: list[str]) -> None:
        """Transition candidates in backlog to EVALUATED status."""
        pass

    @abstractmethod
    def expire_stale_backlog_candidates(self, max_age_hours: int = 24, now: Optional[datetime] = None) -> int:
        """Transition candidates older than max_age_hours to EXPIRED status, preserving legitimate UPDATEs."""
        pass

    # --- Story Intelligence & Lineage ---
    @abstractmethod
    def get_recent_stories(self, limit: int = 80, days: int = 30) -> list[Story]:
        """Fetch recently alerted stories for LLM editorial context."""
        pass

    @abstractmethod
    def get_story_by_id(self, story_id: str) -> Optional[Story]:
        """Fetch a specific story by its ID."""
        pass

    @abstractmethod
    def record_new_story(self, story: Story, assessment: EditorialAssessment) -> None:
        """Persist a newly alerted story and its initial assessment."""
        pass

    @abstractmethod
    def record_story_update(self, update: StoryUpdate, assessment: EditorialAssessment) -> None:
        """Persist an update to an existing story."""
        pass

    @abstractmethod
    def record_editorial_assessment(self, assessment: EditorialAssessment, story_id: str) -> None:
        """Persist an AI editorial assessment for auditability and history."""
        pass

    @abstractmethod
    def has_story_been_alerted(
        self,
        story_id: str,
        kind: EditorialKind = EditorialKind.NEW,
        development_tr: Optional[str] = None,
    ) -> bool:
        """Check if an alert for a story (or specific development) was already notified or in-flight."""
        pass

    # --- Notification Reliability & Lease Management ---
    @abstractmethod
    def claim_notification_for_dispatch(
        self,
        notification_id: str,
        story_id: str,
        kind: EditorialKind,
        headline_tr: str,
        score: int,
        message_text: str,
        worker_id: str,
        lease_seconds: int = 300,
    ) -> bool:
        """Claim a notification with an expiring worker lease before attempting external dispatch."""
        pass

    @abstractmethod
    def record_notification(self, record: NotificationRecord) -> None:
        """Record the outcome of a notification dispatch or queue attempt."""
        pass

    @abstractmethod
    def mark_notification_delivered(self, notification_id: str, delivered_at: datetime) -> None:
        """Update a notification record's status to SENT after successful delivery."""
        pass

    @abstractmethod
    def record_notification_failure(
        self,
        notification_id: str,
        error_message: str,
        status: DeliveryStatus = DeliveryStatus.FAILED,
    ) -> None:
        """Mark a notification delivery as failed or uncertain with an error message."""
        pass

    @abstractmethod
    def get_held_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        """Fetch notifications held during quiet hours waiting to be delivered."""
        pass

    @abstractmethod
    def expire_stale_held_notifications(
        self, max_age_hours: int, now: Optional[datetime] = None
    ) -> int:
        """Expire held notifications that exceed the freshness window so stale news is not sent."""
        pass

    @abstractmethod
    def recover_expired_dispatch_claims(self) -> int:
        """Recover any orphaned in-flight dispatch claims from crashed processes."""
        pass

    # --- Maintenance & Migrations ---
    @abstractmethod
    def purge_expired_records(self, retention_days: int = 35) -> int:
        """Clean up old processed article records beyond the retention window."""
        pass

    @abstractmethod
    def get_schema_version(self) -> int:
        """Return the current database schema version."""
        pass

    # --- Editorial Feedback & Calibration ---
    @abstractmethod
    def record_owner_feedback(self, feedback: OwnerFeedback) -> None:
        """Store explicit owner editorial feedback (APPROVE, REJECT, TOO_WEAK, DUPLICATE, IMPORTANT_UPDATE)."""
        pass

    def import_owner_feedback_batch(self, feedbacks: list[OwnerFeedback]) -> int:
        """Idempotently store a batch of explicit owner editorial feedback records."""
        for fb in feedbacks:
            self.record_owner_feedback(fb)
        return len(feedbacks)

    @abstractmethod
    def get_owner_feedback(self, limit: int = 100) -> list[OwnerFeedback]:
        """Retrieve recorded editorial feedback for calibration and prompt tuning."""
        pass

    # --- Digest Queue & Verification Hold Persistence ---
    @abstractmethod
    def get_queued_digest_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        """Fetch notifications queued for the periodic consolidated digest."""
        pass

    @abstractmethod
    def mark_digest_notifications_dispatched(
        self, notification_ids: list[str], delivered_at: datetime
    ) -> int:
        """Transition a batch of queued digest notifications to SENT once the digest is delivered."""
        pass

    @abstractmethod
    def get_verification_held_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        """Fetch notifications held due to unverified rumor or single-source uncertainty."""
        pass

    @abstractmethod
    def get_sensitive_review_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        """Fetch notifications held in private review queue for sensitive tragedy/death news."""
        pass

    @abstractmethod
    def get_daily_sent_message_count(self, now: Optional[datetime] = None) -> int:
        """Count distinct Telegram messages delivered in the current UTC day."""
        pass

    @abstractmethod
    def get_last_digest_sent_at(self) -> Optional[datetime]:
        """Return the UTC timestamp of the most recently delivered digest batch, if any."""
        pass

    # --- Persistent AI Token & Cost Accounting ---
    @abstractmethod
    def record_ai_usage(
        self,
        usage_id: str,
        run_mode: str,
        model_name: str,
        batch_hash: str,
        candidate_count: int,
        input_tokens: int,
        output_tokens: int,
        estimated_cost_usd: float,
        response_json: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> None:
        """Persist token usage, estimated cost, and optional cached response for a Gemini API call."""
        pass

    @abstractmethod
    def get_monthly_ai_usage(
        self, year_month_prefix: Optional[str] = None, now: Optional[datetime] = None
    ) -> dict[str, float | int]:
        """Return cumulative monthly AI calls, input/output tokens, and estimated USD cost."""
        pass

    @abstractmethod
    def get_cached_ai_batch_response(self, batch_hash: str, model_name: str) -> Optional[str]:
        """Retrieve cached JSON response for an identical candidate batch and model, if available."""
        pass

    # --- Telegram Delivery Outbox & Recipient Lifecycle ---
    @abstractmethod
    def record_telegram_delivery(self, delivery: TelegramDeliveryRecord) -> None:
        """Persist or insert an initial delivery record into the durable outbox."""
        pass

    @abstractmethod
    def get_telegram_delivery(self, recipient_chat_id: str, message_key: str) -> Optional[TelegramDeliveryRecord]:
        """Fetch delivery record for a specific recipient and message key."""
        pass

    @abstractmethod
    def update_telegram_delivery_status(
        self,
        delivery_id: str,
        status: DeliveryStatus,
        error_message: Optional[str] = None,
        sent_at: Optional[datetime] = None,
    ) -> bool:
        """Update status of a delivery record (e.g. SENT, FAILED, DELIVERY_UNCERTAIN)."""
        pass

    @abstractmethod
    def reconcile_uncertain_delivery(
        self,
        delivery_id: str,
        reconciliation_proof: dict[str, Any],
        reconciled_at: Optional[datetime] = None,
        *,
        operator_authorized: bool = False,
        live_mode: bool = False,
    ) -> TelegramDeliveryRecord:
        """Reconcile a DELIVERY_UNCERTAIN record to SENT using operator-supplied evidence metadata."""
        pass

    @abstractmethod
    def is_digest_delivered_to_recipient(self, recipient_chat_id: str, message_key: str) -> bool:
        """Check if a specific digest has already been delivered to this recipient."""
        pass

    @abstractmethod
    def get_pending_telegram_deliveries(self, limit: int = 50) -> list[TelegramDeliveryRecord]:
        """Fetch outbox records pending delivery attempt."""
        pass

    # --- Durable Cloud Recovery State ---
    @abstractmethod
    def save_recovery_state(
        self,
        stage: str,
        worker_id: str,
        state_payload: dict[str, Any],
        batch_id: Optional[str] = None,
        last_processed_id: Optional[str] = None,
    ) -> None:
        """Persist or update recovery checkpoint for a specific pipeline stage."""
        pass

    @abstractmethod
    def get_recovery_state(self, stage: str) -> Optional[dict[str, Any]]:
        """Retrieve latest recovery checkpoint for a stage, if present."""
        pass

    @abstractmethod
    def clear_recovery_state(self, stage: str) -> None:
        """Clear recovery checkpoint after stage completes successfully."""
        pass

    # --- At-Most-Once Daily Admin Operational Reports ---
    @abstractmethod
    def record_admin_report(
        self,
        report_date: str,
        worker_id: str,
        status: DeliveryStatus,
        recipient_chat_id: str,
        summary_payload: dict[str, Any],
    ) -> bool:
        """Record daily admin operational report for date (Europe/Istanbul YYYY-MM-DD).

        Returns True if inserted, False if report for this date was already recorded.
        """
        pass

    @abstractmethod
    def has_admin_report_been_dispatched(self, report_date: str) -> bool:
        """Check if admin report for this Europe/Istanbul date was already dispatched."""
        pass

    # --- Experiment Budget Ledger & Monthly Consumption ---
    @abstractmethod
    def record_budget_usage(
        self,
        period_month: str,
        run_id: str,
        worker_id: str,
        calls_count: int,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        estimated_cost_usd: float,
        timestamp: Optional[datetime] = None,
        record_id: Optional[str] = None,
    ) -> bool:
        """Persist immutable budget consumption entry.

        Idempotent for identical replays; raises BudgetHistoryConflictError on conflicting values.
        """
        pass

    @abstractmethod
    def get_monthly_budget_summary(self, period_month: str) -> dict[str, Any]:
        """Retrieve aggregated token usage and cost for month (YYYY-MM)."""
        pass

    @abstractmethod
    def get_budget_record(self, record_id: str) -> Optional[dict[str, Any]]:
        """Retrieve a specific budget usage record by record_id."""
        pass

    # --- Scheduled Cloud Execution Audit History ---
    @abstractmethod
    def record_scheduled_execution_start(
        self,
        run_id: str,
        run_type: str,
        worker_id: str,
        started_at: Optional[datetime] = None,
    ) -> None:
        """Log start of a scheduled runner execution."""
        pass

    @abstractmethod
    def record_scheduled_execution_finish(
        self,
        run_id: str,
        status: str,
        articles_collected: int = 0,
        candidates_evaluated: int = 0,
        notifications_dispatched: int = 0,
        error_message: Optional[str] = None,
        finished_at: Optional[datetime] = None,
    ) -> None:
        """Log completion or failure of a scheduled runner execution."""
        pass

    @abstractmethod
    def get_scheduled_execution(self, run_id: str) -> Optional[dict[str, Any]]:
        """Retrieve execution history record by run_id."""
        pass



