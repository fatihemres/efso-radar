"""News sources package for Efso Radar."""
from efso_radar.sources.base import NewsSource
from efso_radar.sources.filter import NewsRelevancePrefilter
from efso_radar.sources.google_news import GoogleNewsSource
from efso_radar.sources.models import DiscoveryMetrics, FeedFormat, SourceDefinition
from efso_radar.sources.multi_source import MultiSourceDiscoveryEngine
from efso_radar.sources.parsers import FeedParser
from efso_radar.sources.registry import (
    DEFAULT_SOURCES,
    EXCLUDED_SOURCES_CATALOG,
    SourceRegistry,
)
from efso_radar.sources.rss import RSSFeedSource

__all__ = [
    "NewsSource",
    "GoogleNewsSource",
    "RSSFeedSource",
    "MultiSourceDiscoveryEngine",
    "SourceRegistry",
    "SourceDefinition",
    "FeedFormat",
    "DiscoveryMetrics",
    "FeedParser",
    "NewsRelevancePrefilter",
    "DEFAULT_SOURCES",
    "EXCLUDED_SOURCES_CATALOG",
]

