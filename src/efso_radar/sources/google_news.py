"""Google News RSS provider implementation.

Fetches pop-culture, celebrity, and entertainment news across Turkish and global markets.
Resilient against single-feed network or XML parsing failures.
"""
from __future__ import annotations

import html
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Any, Optional

from efso_radar.domain import Article, Market
from efso_radar.sources.base import NewsSource

logger = logging.getLogger("efso-radar.sources.google_news")

DEFAULT_QUERIES: list[tuple[Market, str]] = [
    (Market.TR, "magazin ünlüler when:1d"),
    (Market.TR, "influencer fenomen YouTube TikTok when:1d"),
    (Market.TR, "şarkıcı oyuncu aşk evlilik ayrılık when:1d"),
    (Market.TR, "Netflix dizi film oyuncu müzik when:1d"),
    (Market.TR, "ünlü konser yeni şarkı albüm when:1d"),
    (Market.EN, "celebrity entertainment breaking when:1d"),
    (Market.EN, "pop star music album concert when:1d"),
    (Market.EN, "viral celebrity interview moment when:1d"),
    (Market.EN, "Hollywood streaming casting series when:1d"),
    (Market.EN, "TikTok influencer pop culture trending when:1d"),
    (Market.EN, "Rihanna OR Bieber OR Zendaya OR Sabrina Carpenter OR Billie Eilish when:1d"),
]


class _HTMLCleaner(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.fragments: list[str] = []

    def handle_data(self, data: str) -> None:
        self.fragments.append(data)


def clean_html_text(raw: str) -> str:
    """Strip HTML markup and collapse excess whitespace."""
    if not raw:
        return ""
    parser = _HTMLCleaner()
    parser.feed(raw)
    cleaned = " ".join(parser.fragments)
    return re.sub(r"\s+", " ", html.unescape(cleaned)).strip()


class GoogleNewsSource(NewsSource):
    """Google News RSS aggregator with per-query fault isolation."""

    def __init__(
        self,
        queries: Optional[list[tuple[Market, str]]] = None,
        timeout_seconds: float = 18.0,
        user_agent: str = "EfsoRadar/2.0 (+news-monitor; contact: @efsomagazin)",
    ):
        self.queries = queries or DEFAULT_QUERIES
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent

    def build_url(self, market: Market, query: str) -> str:
        params = {"q": query}
        if market == Market.TR:
            params.update({"hl": "tr", "gl": "TR", "ceid": "TR:tr"})
        else:
            params.update({"hl": "en-US", "gl": "US", "ceid": "US:en"})
        return "https://news.google.com/rss/search?" + urllib.parse.urlencode(params)

    def download_xml(self, url: str) -> bytes:
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as resp:
            return resp.read(2_000_000)

    def parse_rss_feed(
        self, xml_bytes: bytes, market: Market, now: datetime, max_age_hours: int = 16
    ) -> list[Article]:
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            logger.warning("XML parsing error: %s", exc)
            return []

        articles: list[Article] = []
        utc_now_val = now.astimezone(timezone.utc)
        max_age_delta = timedelta(hours=max_age_hours)

        for item in root.findall("./channel/item"):
            title = clean_html_text(item.findtext("title") or "")
            link = (item.findtext("link") or "").strip()
            if not title or not link or not link.startswith("https://"):
                continue

            raw_date = (item.findtext("pubDate") or "").strip()
            try:
                published = parsedate_to_datetime(raw_date).astimezone(timezone.utc)
            except (TypeError, ValueError):
                continue

            # Skip future dates (>15 min in future) or older than max_age_hours
            if published > utc_now_val + timedelta(minutes=15) or (utc_now_val - published) > max_age_delta:
                continue

            source = clean_html_text(item.findtext("source") or "")
            if source and title.endswith(" - " + source):
                title = title[: -len(" - " + source)].strip()

            snippet = clean_html_text(item.findtext("description") or "")[:350]

            article = Article.create(
                title=title[:300],
                url=link,
                source=source[:120] or "Kaynak belirtilmemiş",
                published_at=published,
                snippet=snippet,
                market=market,
            )
            articles.append(article)

        return articles

    def fetch_candidates(self, now: datetime, max_age_hours: int = 16) -> tuple[list[Article], int]:
        collected_map: dict[str, Article] = {}
        successes = 0

        for market, query in self.queries:
            try:
                url = self.build_url(market, query)
                xml_data = self.download_xml(url)
                items = self.parse_rss_feed(xml_data, market, now, max_age_hours)
                successes += 1
                for item in items:
                    collected_map[item.id] = item
            except (urllib.error.URLError, ET.ParseError, TimeoutError, OSError) as exc:
                logger.warning("Google News feed unreachable (%s, %s): %s", market.value, query, exc)
            # Gentle pacing between queries
            time.sleep(0.05)

        sorted_articles = sorted(
            collected_map.values(), key=lambda a: a.published_at, reverse=True
        )
        return sorted_articles, successes
