"""News source interface for Efso Radar."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from efso_radar.domain import Article


class NewsSource(ABC):
    """Abstract interface for external news providers."""

    @abstractmethod
    def fetch_candidates(self, now: datetime, max_age_hours: int) -> tuple[list[Article], int]:
        """
        Fetch new candidate articles published within the last max_age_hours.
        Returns a tuple of (collected_articles, successful_feed_count).
        """
        pass
