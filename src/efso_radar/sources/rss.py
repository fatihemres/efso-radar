"""Direct RSS/Atom feed client with bounded retries, SSL verification, and fault isolation."""
from __future__ import annotations

import logging
import ssl
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Optional

from efso_radar.domain import Article
from efso_radar.sources.models import SourceDefinition
from efso_radar.sources.parsers import FeedParser

logger = logging.getLogger("efso-radar.sources.rss")

# Try to use certifi bundle for robust cross-platform SSL verification
try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    SSL_CONTEXT = ssl.create_default_context()

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 EfsoRadar/3.0 (+news-monitor; contact: @efsomagazin)"


class RSSFeedSource:
    """Client for fetching and parsing a single RSS or Atom feed."""

    def __init__(
        self,
        source_def: SourceDefinition,
        user_agent: str = DEFAULT_USER_AGENT,
        max_retries: int = 2,
    ):
        self.source_def = source_def
        self.user_agent = user_agent
        self.max_retries = max_retries

    def fetch_result(self, now: Optional[datetime] = None, max_age_hours: int = 16):
        """Download feed XML and parse articles returning full ParseResult metrics."""
        headers = {"User-Agent": self.user_agent, "Accept": "application/rss+xml, application/xml, text/xml, */*"}
        if self.source_def.headers:
            headers.update(self.source_def.headers)

        req = urllib.request.Request(self.source_def.url, headers=headers)
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=self.source_def.timeout_seconds) as resp:
                    if resp.status != 200:
                        raise urllib.error.HTTPError(
                            self.source_def.url, resp.status, f"HTTP status {resp.status}", resp.headers, None
                        )
                    xml_bytes = resp.read(5_000_000)  # Max 5MB per feed
                    return FeedParser.parse_feed(
                        xml_bytes=xml_bytes,
                        source_def=self.source_def,
                        now=now,
                        max_age_hours=max_age_hours,
                    )
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(0.35 * (2 ** attempt))

        logger.warning("Feed '%s' (%s) failed after %d attempts: %s",
                       self.source_def.name, self.source_def.url, self.max_retries + 1, last_error)
        raise last_error or RuntimeError(f"Unknown failure fetching {self.source_def.name}")

    def fetch(self, now: Optional[datetime] = None, max_age_hours: int = 16) -> list[Article]:
        """Download feed XML and return parsed articles list."""
        return self.fetch_result(now=now, max_age_hours=max_age_hours).articles

