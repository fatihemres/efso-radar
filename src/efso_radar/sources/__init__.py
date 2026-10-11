"""News sources package for Efso Radar."""
from efso_radar.sources.base import NewsSource
from efso_radar.sources.google_news import GoogleNewsSource

__all__ = ["NewsSource", "GoogleNewsSource"]
