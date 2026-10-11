"""State repository interface for Efso Radar.

Defines the contract for persistence, ensuring business logic is 100% portable
across local SQLite and remote Turso (libSQL over HTTP).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    NotificationRecord,
    Story,
    StoryUpdate,
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
    def record_notification_failure(self, notification_id: str, error_message: str) -> None:
        """Mark a notification delivery as failed with an error message."""
        pass

    @abstractmethod
    def get_held_notifications(self, max_age_hours: Optional[int] = None) -> list[NotificationRecord]:
        """Fetch notifications held during quiet hours waiting to be delivered."""
        pass

    @abstractmethod
    def expire_stale_held_notifications(self, max_age_hours: int) -> int:
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
