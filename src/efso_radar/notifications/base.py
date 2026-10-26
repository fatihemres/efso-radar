"""Notification channel interface for Efso Radar."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from efso_radar.domain import EditorialAssessment, NotificationRecord


class NotificationChannel(ABC):
    """Abstract interface for alert delivery channels."""

    @abstractmethod
    def send(
        self,
        assessment: EditorialAssessment,
        story_id: str,
        update_id: Optional[str] = None,
        dry_run: bool = False,
        now: Optional[datetime] = None,
    ) -> NotificationRecord:
        """Format and dispatch an alert, returning a delivery record."""
        pass

