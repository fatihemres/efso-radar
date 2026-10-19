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
    DUPLICATE = "DUPLICATE"
    IGNORE = "IGNORE"


class EditorialDisposition(str, Enum):
    """Explicit per-article publication priority and triage decision."""
    DIGEST = "DIGEST"    # Approved for publication in periodic digest (or immediate alert if breaking)
    RESERVE = "RESERVE"  # Worthy optional / filler content suitable for backlog buffer if content needed
    IGNORE = "IGNORE"    # Rejected, routine, irrelevant, distressing/negative, or unworthy
    HOLD = "HOLD"        # Editorially interesting but unverified rumor / sensitive claim needing review
    SENSITIVE_REVIEW = "SENSITIVE_REVIEW"  # Significant, credible sensitive tragedy/bereavement news needing private review


class VerificationStatus(str, Enum):
    """Factual confidence and source attribution category for an event."""
    OFFICIAL_CONFIRMATION = "OFFICIAL_CONFIRMATION"  # Resmî onay / ajans / menajer beyanı
    FIRST_PARTY_STATEMENT = "FIRST_PARTY_STATEMENT"  # Ünlü / sanatçı / yapımcı kendi beyanı
    REPUTABLE_REPORTING = "REPUTABLE_REPORTING"      # Saygın haber kuruluşlarının teyitli haberi
    ATTRIBUTED_CLAIM = "ATTRIBUTED_CLAIM"            # Belirli kaynağa dayandırılan haber
    UNVERIFIED_RUMOR = "UNVERIFIED_RUMOR"            # Teyitsiz söylenti / magazin dedikodusu
    SPECULATION = "SPECULATION"                      # Sosyal medya tahmini / temelsiz iddia



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
    QUEUED_FOR_DIGEST = "QUEUED_FOR_DIGEST"
    HELD_FOR_VERIFICATION = "HELD_FOR_VERIFICATION"
    HELD_FOR_SENSITIVE_REVIEW = "HELD_FOR_SENSITIVE_REVIEW"
    DELIVERY_UNCERTAIN = "DELIVERY_UNCERTAIN"


class NotificationUrgencyTier(str, Enum):
    """Distinguishes editorial interest from notification delivery urgency and verification safety."""
    BREAKING_IMMEDIATE = "BREAKING_IMMEDIATE"      # Exceptionally important, verified breaking news -> immediate alert
    PERIODIC_DIGEST = "PERIODIC_DIGEST"            # Editorially approved story -> grouped into ~2h periodic digest
    HOLD_FOR_VERIFICATION = "HOLD_FOR_VERIFICATION"  # Editorially interesting but unverified/sensitive -> requires human verification
    SENSITIVE_REVIEW = "SENSITIVE_REVIEW"          # Significant sensitive tragedy/death news -> requires private editorial review
    SUPPRESSED = "SUPPRESSED"                      # Rejected, too weak, or duplicate -> no notification


class TelegramNotificationCategory(str, Enum):
    """Logically separated message categories within the private chat notifications."""
    EDITORIAL_DIGEST = "EDITORIAL_DIGEST"
    VERIFIED_BREAKING = "VERIFIED_BREAKING"
    SENSITIVE_EDITORIAL_REVIEW = "SENSITIVE_EDITORIAL_REVIEW"
    HOLD_FOR_VERIFICATION = "HOLD_FOR_VERIFICATION"
    OPERATIONAL_HEALTH_REPORT = "OPERATIONAL_HEALTH_REPORT"




class Article(BaseModel):
    """An individual piece of news from a specific publisher/feed.
    
    Note on Media Rights:
    `image_url` and associated media metadata are extracted from publicly syndicated RSS/Atom
    enclosures solely for editorial radar assessment and link preview indexing. Capturing this metadata
    does NOT grant, imply, or substitute for copyright redistribution, publication, or commercial reuse rights.
    """
    model_config = ConfigDict(frozen=True)

    id: str
    title: str = Field(min_length=1, max_length=500)
    url: str = Field(min_length=1)
    canonical_url: Optional[str] = None
    source: str = Field(default="Kaynak belirtilmemiş", max_length=150)
    market: Market = Market.TR
    published_at: datetime
    discovered_at: Optional[datetime] = None
    source_category: Optional[str] = None
    image_url: Optional[str] = None
    reliability_score: Optional[float] = None
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
        discovered_at: Optional[datetime] = None,
        source_category: Optional[str] = None,
        image_url: Optional[str] = None,
        reliability_score: Optional[float] = None,
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
            discovered_at=discovered_at or utc_now(),
            source_category=(source_category.strip() if source_category else None),
            image_url=(image_url.strip() if image_url else None),
            reliability_score=reliability_score,
            snippet=snippet.strip(),
            raw_metadata=raw_metadata or {},
        )

    @property
    def priority_score(self) -> float:
        """Heuristic priority score assigned during discovery ranking (if ranked)."""
        return float(self.raw_metadata.get("priority_score", 0.0))

    @property
    def priority_reasons(self) -> list[str]:
        """Heuristic signals contributing to priority score."""
        return list(self.raw_metadata.get("priority_reasons", []))




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
    cover_title_tr: str = Field(default="", max_length=150)
    caption_idea_tr: str = Field(default="", max_length=800)
    relevance_reason_tr: str = Field(default="", max_length=500)
    why_efso_tr: str = Field(default="", max_length=500)
    uncertainties_tr: Optional[str] = None
    suggested_format: SuggestedFormat = SuggestedFormat.POST
    category: StoryCategory = StoryCategory.GLOBAL
    verification_status: VerificationStatus = VerificationStatus.REPUTABLE_REPORTING
    source_urls: list[str] = Field(default_factory=list)
    supporting_articles: list[Article] = Field(default_factory=list)
    is_publishable: bool = True
    disposition: Optional[EditorialDisposition] = None


class OwnerFeedbackRating(str, Enum):
    """Owner review labels for calibrating AI editorial precision."""
    APPROVE = "APPROVE"
    REJECT = "REJECT"
    TOO_WEAK = "TOO_WEAK"
    DUPLICATE = "DUPLICATE"
    IMPORTANT_UPDATE = "IMPORTANT_UPDATE"


class OwnerFeedback(BaseModel):
    """Explicit human owner feedback record for offline evaluation and calibration."""
    feedback_id: str
    story_id: Optional[str] = None
    article_id: Optional[str] = None
    rating: OwnerFeedbackRating
    notes: str = ""
    created_at: datetime = Field(default_factory=utc_now)



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
    category: Optional[TelegramNotificationCategory] = None
