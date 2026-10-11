"""Efso Radar core domain models.

Separates raw publisher articles, real-world story clusters, story updates,
editorial assessments, and notification delivery records into explicit,
type-safe domain concepts.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    """Return timezone-aware current UTC datetime."""
    return datetime.now(timezone.utc)


def normalize_text(text: str) -> str:
    """Normalize whitespace and unicode characters with Turkish character safety."""
    if not text:
        return ""
    # Map Turkish dotted/dotless I cleanly before casefold
    cleaned = text.replace("İ", "i").replace("I", "ı").casefold()
    cleaned = re.sub(r"[^\w]+", " ", cleaned, flags=re.UNICODE)
    return re.sub(r"\s+", " ", cleaned).strip()



def compute_article_id(url: str, title: str, source: str) -> str:
    """Generate a deterministic 24-character hash for an article."""
    clean_url = url.split("?", 1)[0].strip() if url else ""
    key = clean_url or f"{normalize_text(title)}::{normalize_text(source)}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]


class Market(str, Enum):
    TR = "TR"
    EN = "EN"


class EditorialKind(str, Enum):
    NEW = "NEW"
    UPDATE = "UPDATE"


class SuggestedFormat(str, Enum):
    POST = "POST"
    CAROUSEL = "CAROUSEL"
    REEL = "REEL"


class StoryCategory(str, Enum):
    GLOBAL = "GLOBAL"
    YERLI = "YERLI"
    VIRAL = "VIRAL"
    MUZIK_DIZI = "MUZIK_DIZI"
    INTERNET = "INTERNET"


class DeliveryStatus(str, Enum):
    SENT = "SENT"
    HELD_QUIET_HOURS = "HELD_QUIET_HOURS"
    DISPATCHING = "DISPATCHING"
    FAILED = "FAILED"
    SKIPPED_BELOW_THRESHOLD = "SKIPPED_BELOW_THRESHOLD"
    EXPIRED_STALE = "EXPIRED_STALE"




class Article(BaseModel):
    """An individual piece of news from a specific publisher/feed."""
    model_config = ConfigDict(frozen=True)

    id: str
    title: str = Field(min_length=1, max_length=500)
    url: str = Field(min_length=1)
    canonical_url: Optional[str] = None
    source: str = Field(default="Kaynak belirtilmemiş", max_length=150)
    market: Market = Market.TR
    published_at: datetime
    snippet: str = Field(default="", max_length=1000)
    raw_metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def create(
        cls,
        title: str,
        url: str,
        source: str,
        published_at: datetime,
        snippet: str = "",
        market: Market = Market.TR,
        canonical_url: Optional[str] = None,
        raw_metadata: Optional[dict[str, Any]] = None,
    ) -> Article:
        article_id = compute_article_id(canonical_url or url, title, source)
        return cls(
            id=article_id,
            title=title.strip(),
            url=url.strip(),
            canonical_url=(canonical_url.strip() if canonical_url else None),
            source=source.strip() or "Kaynak belirtilmemiş",
            market=market,
            published_at=published_at,
            snippet=snippet.strip(),
            raw_metadata=raw_metadata or {},
        )


class Story(BaseModel):
    """A real-world news event or story cluster, potentially reported by multiple publishers."""
    story_id: str
    headline_tr: str
    category: StoryCategory
    first_seen_at: datetime
    last_updated_at: datetime
    article_ids: list[str] = Field(default_factory=list)
    update_count: int = 0
    is_active: bool = True


class StoryUpdate(BaseModel):
    """A significant new development on an already alerted story."""
    update_id: str
    story_id: str
    development_tr: str
    detected_at: datetime
    article_ids: list[str] = Field(default_factory=list)
    significance_score: int = Field(ge=0, le=100)


class EditorialAssessment(BaseModel):
    """An editorial decision made by AI and validated against business rules."""
    article_ids: list[str] = Field(min_length=1)
    score: int = Field(ge=0, le=100)
    kind: EditorialKind
    parent_story_id: Optional[str] = None
    development_tr: Optional[str] = None
    headline_tr: str = Field(min_length=5, max_length=200)
    cover_title_tr: str = Field(min_length=3, max_length=150)
    caption_idea_tr: str = Field(min_length=10, max_length=600)
    why_efso_tr: str = Field(min_length=5, max_length=300)
    suggested_format: SuggestedFormat
    category: StoryCategory
    supporting_articles: list[Article] = Field(default_factory=list)


class NotificationRecord(BaseModel):
    """Record of an alert delivery attempt or success."""
    notification_id: str
    story_id: str
    update_id: Optional[str] = None
    kind: EditorialKind
    channel: str = "TELEGRAM"
    sent_at: datetime
    status: DeliveryStatus
    message_text: str
    headline_tr: str
    score: int
    error_message: Optional[str] = None
