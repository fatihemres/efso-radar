"""Configurable registry of verified Turkish and international entertainment news sources."""
from __future__ import annotations

import logging
from typing import Optional

from efso_radar.domain import Market
from efso_radar.sources.models import FeedFormat, SourceDefinition

logger = logging.getLogger("efso-radar.sources.registry")

# Verified active direct publisher RSS/Atom feeds
DEFAULT_SOURCES: list[SourceDefinition] = [
    # --- Turkish Entertainment & Pop Culture ---
    SourceDefinition(
        source_id="hurriyet_kelebek",
        name="Hürriyet Kelebek",
        url="https://www.hurriyet.com.tr/rss/kelebek",
        market=Market.TR,
        category="MAGAZIN_TR",
        format=FeedFormat.RSS2,
        reliability_score=0.95,
        enabled=True,
    ),
    SourceDefinition(
        source_id="onedio_magazin",
        name="Onedio Magazin",
        url="https://onedio.com/Publisher/publisher-magazin.rss",
        market=Market.TR,
        category="VIRAL_INTERNET_TR",
        format=FeedFormat.RSS2,
        reliability_score=0.90,
        enabled=True,
    ),
    SourceDefinition(
        source_id="haberturk_magazin",
        name="Habertürk Magazin",
        url="https://www.haberturk.com/rss/magazin.xml",
        market=Market.TR,
        category="MAGAZIN_TR",
        format=FeedFormat.RSS2,
        reliability_score=0.92,
        enabled=True,
    ),
    SourceDefinition(
        source_id="milliyet_cadde",
        name="Milliyet Cadde",
        url="https://www.milliyet.com.tr/rss/rssNew/magazinRss.xml",
        market=Market.TR,
        category="MAGAZIN_TR",
        format=FeedFormat.RSS2,
        reliability_score=0.90,
        enabled=True,
    ),
    SourceDefinition(
        source_id="ntv_yasam",
        name="NTV Yaşam & Sanat",
        url="https://www.ntv.com.tr/yasam.rss",
        market=Market.TR,
        category="CULTURE_LIFESTYLE_TR",
        format=FeedFormat.ATOM,
        reliability_score=0.95,
        enabled=True,
    ),

    # --- Global Entertainment, Music, Film & Pop Culture ---
    SourceDefinition(
        source_id="variety",
        name="Variety",
        url="https://variety.com/feed/",
        market=Market.EN,
        category="FILM_TV_GLOBAL",
        format=FeedFormat.RSS2,
        reliability_score=0.98,
        enabled=True,
    ),
    SourceDefinition(
        source_id="deadline",
        name="Deadline",
        url="https://deadline.com/feed/",
        market=Market.EN,
        category="HOLLYWOOD_GLOBAL",
        format=FeedFormat.RSS2,
        reliability_score=0.96,
        enabled=True,
    ),
    SourceDefinition(
        source_id="billboard",
        name="Billboard",
        url="https://www.billboard.com/feed/",
        market=Market.EN,
        category="MUSIC_GLOBAL",
        format=FeedFormat.RSS2,
        reliability_score=0.98,
        enabled=True,
    ),
    SourceDefinition(
        source_id="rolling_stone",
        name="Rolling Stone",
        url="https://www.rollingstone.com/feed/",
        market=Market.EN,
        category="MUSIC_CULTURE_GLOBAL",
        format=FeedFormat.RSS2,
        reliability_score=0.95,
        enabled=True,
    ),
    SourceDefinition(
        source_id="tmz",
        name="TMZ",
        url="https://www.tmz.com/rss.xml",
        market=Market.EN,
        category="VIRAL_CELEBRITY_GLOBAL",
        format=FeedFormat.RSS2,
        reliability_score=0.90,
        enabled=True,
    ),
    SourceDefinition(
        source_id="e_news",
        name="E! News",
        url="https://www.eonline.com/syndication/feeds/rssfeeds/topstories.xml",
        market=Market.EN,
        category="CELEBRITY_LIFESTYLE_GLOBAL",
        format=FeedFormat.RSS2,
        reliability_score=0.92,
        enabled=True,
    ),
]

# Explicit catalog of researched candidate sources that were excluded or disabled
EXCLUDED_SOURCES_CATALOG: dict[str, dict[str, str]] = {
    "people": {
        "name": "People.com",
        "url": "https://people.com/feed/",
        "reason": "Excluded: Endpoint returns HTTP 402 / Anti-bot Cloudflare paywall blocking for direct RSS requests.",
    },
    "etonline": {
        "name": "Entertainment Tonight",
        "url": "https://www.etonline.com/news/rss",
        "reason": "Excluded from active rotation: Endpoint provides only 2 items and publishes irregularly (stale dates >6 days).",
    },
}


class SourceRegistry:
    """Manages news sources with filtering by market, category, and active status."""

    def __init__(self, sources: Optional[list[SourceDefinition]] = None):
        self._sources: dict[str, SourceDefinition] = {
            s.source_id: s for s in (sources or DEFAULT_SOURCES)
        }

    def get_all(self) -> list[SourceDefinition]:
        return list(self._sources.values())

    def get_active(self, market: Optional[Market] = None) -> list[SourceDefinition]:
        active = [s for s in self._sources.values() if s.enabled]
        if market:
            active = [s for s in active if s.market == market]
        return active

    def get_by_id(self, source_id: str) -> Optional[SourceDefinition]:
        return self._sources.get(source_id)

    def register(self, source: SourceDefinition) -> None:
        self._sources[source.source_id] = source

    def set_enabled(self, source_id: str, enabled: bool) -> bool:
        if source_id in self._sources:
            s = self._sources[source_id]
            self._sources[source_id] = SourceDefinition(
                source_id=s.source_id,
                name=s.name,
                url=s.url,
                market=s.market,
                category=s.category,
                format=s.format,
                reliability_score=s.reliability_score,
                enabled=enabled,
                timeout_seconds=s.timeout_seconds,
                headers=s.headers,
            )
            return True
        return False
