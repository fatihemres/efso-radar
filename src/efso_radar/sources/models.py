"""Data models for Efso Radar news discovery sources and metrics."""
from __future__ import annotations

from enum import Enum
from typing import NamedTuple, Optional
from pydantic import BaseModel, ConfigDict, Field

from efso_radar.domain import Article, Market


class FeedFormat(str, Enum):
    """Supported syndicated feed format types."""
    RSS2 = "RSS2"
    ATOM = "ATOM"
    GOOGLE_NEWS = "GOOGLE_NEWS"


class SourceDefinition(BaseModel):
    """Configuration definition for an individual news feed source."""
    model_config = ConfigDict(frozen=True)

    source_id: str = Field(description="Unique identifier for the feed source, e.g. 'variety'")
    name: str = Field(description="Display publisher/feed name, e.g. 'Variety'")
    url: str = Field(description="Direct RSS/Atom feed endpoint URL")
    market: Market = Field(default=Market.TR, description="Target market/language (TR or EN)")
    category: str = Field(default="GENERAL_ENTERTAINMENT", description="Editorial category classification")
    format: FeedFormat = Field(default=FeedFormat.RSS2, description="Feed payload format")
    reliability_score: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description=(
            "Configurable editorial heuristic reflecting general journalistic reputation "
            "and editorial fit for Efso Magazin; NOT a mathematical probability of objective factual truth."
        ),
    )
    enabled: bool = Field(default=True, description="Whether this feed is actively monitored")
    timeout_seconds: float = Field(default=14.0, ge=1.0, le=60.0, description="HTTP connection/read timeout")
    headers: dict[str, str] = Field(default_factory=dict, description="Custom HTTP headers if required")


class ParseResult(NamedTuple):
    """Result of feed XML parsing with raw item counts and filtering breakdown."""
    articles: list[Article]
    raw_seen: int
    stale_count: int
    invalid_count: int


class DiscoveryMetrics(BaseModel):
    """Observability metrics tracking the complete multi-stage news discovery funnel."""
    sources_attempted: int = 0
    sources_succeeded: int = 0
    raw_feed_items_seen: int = 0         # Total <item> and <entry> tags parsed across all XML feeds
    stale_items_skipped: int = 0         # Items older than max_age_hours or in the future
    invalid_items_skipped: int = 0       # Items missing title, link, or unparseable
    parsed_articles: int = 0             # Successfully parsed valid articles within freshness window
    prefilter_rejected: int = 0          # Rejected by deterministic relevance prefilter (sports/econ/blotter)
    url_duplicates_removed: int = 0      # Duplicate canonical URLs within the batch
    eligible_fresh_articles: int = 0     # Articles passed to deduplication & clustering
    story_clusters_formed: int = 0       # Cross-publisher event clusters formed
    selected_for_ai: int = 0             # Final top candidates selected (<= max_candidates)
    deprioritized_by_limit: int = 0      # Eligible candidates dropped due to candidate limit
    source_errors: dict[str, str] = Field(default_factory=dict)
    duration_seconds: float = 0.0

    # Backwards compatibility properties
    @property
    def articles_discovered(self) -> int:
        return self.parsed_articles

    @property
    def articles_rejected(self) -> int:
        return self.prefilter_rejected

    @property
    def unique_candidates(self) -> int:
        return self.eligible_fresh_articles

    @property
    def duplicate_candidates(self) -> int:
        return self.url_duplicates_removed

    def summary(self) -> str:
        return (
            f"Discovery Funnel: attempted={self.sources_attempted}, succeeded={self.sources_succeeded}, "
            f"raw_seen={self.raw_feed_items_seen}, stale_skipped={self.stale_items_skipped}, "
            f"parsed={self.parsed_articles}, prefilter_rejected={self.prefilter_rejected}, "
            f"url_dups={self.url_duplicates_removed}, eligible={self.eligible_fresh_articles}, "
            f"clusters={self.story_clusters_formed}, selected_for_ai={self.selected_for_ai}, "
            f"deprioritized={self.deprioritized_by_limit}, duration={self.duration_seconds:.2f}s"
        )
