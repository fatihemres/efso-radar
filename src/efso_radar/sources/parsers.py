"""Robust multi-format XML/RSS/Atom feed parsing with Turkish character safety,
clean canonical URL resolution, and media enclosure extraction.
"""
from __future__ import annotations

import html
import logging
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Optional

from efso_radar.domain import Article, Market, utc_now
from efso_radar.sources.models import ParseResult, SourceDefinition

logger = logging.getLogger("efso-radar.sources.parsers")

# Tracking parameters to strip for canonical URL normalization
TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "ref",
    "ref_src",
    "oc",
    "amp",
    "cmpid",
}

# XML Namespaces used across RSS and Atom feeds
NAMESPACES = {
    "atom": "http://www.w3.org/2005/Atom",
    "dc": "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "media": "http://search.yahoo.com/mrss/",
}


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.fragments: list[str] = []
        self.image_urls: list[str] = []

    def handle_data(self, data: str) -> None:
        self.fragments.append(data)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag == "img":
            for attr_name, attr_val in attrs:
                if attr_name == "src" and attr_val and attr_val.startswith("http"):
                    self.image_urls.append(attr_val)


def clean_html(raw: str) -> tuple[str, list[str]]:
    """Strip HTML tags, unescape entities, and extract any embedded image URLs."""
    if not raw:
        return "", []
    extractor = _HTMLTextExtractor()
    try:
        extractor.feed(raw)
    except Exception:
        # Fallback to regex if HTML parser encounters unparseable malformed blob
        cleaned = re.sub(r"<[^>]+>", " ", raw)
        return re.sub(r"\s+", " ", html.unescape(cleaned)).strip(), []

    text = " ".join(extractor.fragments)
    cleaned_text = re.sub(r"\s+", " ", html.unescape(text)).strip()
    return cleaned_text, extractor.image_urls


def clean_canonical_url(url: str) -> str:
    """Normalize URL by stripping tracking parameters, fragments, and trailing slashes."""
    if not url:
        return ""
    try:
        parsed = urllib.parse.urlparse(url.strip())
        if not parsed.scheme or not parsed.netloc:
            return url.strip()

        # Parse and filter query parameters
        query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=False)
        clean_pairs = [
            (k, v) for k, v in query_pairs if k.lower() not in TRACKING_PARAMS
        ]
        clean_query = urllib.parse.urlencode(clean_pairs)

        clean_path = parsed.path.rstrip("/") if parsed.path != "/" else "/"

        normalized = urllib.parse.urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            clean_path,
            "",  # params
            clean_query,
            "",  # fragment
        ))
        return normalized
    except Exception:
        return url.strip()


def parse_datetime(raw_date: str) -> Optional[datetime]:
    """Parse publication date from RFC 822, RFC 2822, or ISO 8601 into timezone-aware UTC datetime."""
    if not raw_date:
        return None
    raw_str = raw_date.strip()

    # 1. Try RFC 822 / 2822 (standard RSS pubDate)
    try:
        dt = parsedate_to_datetime(raw_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError, IndexError):
        pass

    # 2. Try ISO 8601 / RFC 3339 (Atom published/updated, Dublin Core date)
    # Handle 'Z' suffix and variable sub-second precision
    iso_clean = raw_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        pass

    # 3. Common Turkish & European date formats fallback (e.g., '11.10.2026 14:30:00')
    for fmt in ("%d.%m.%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(raw_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


class FeedParser:
    """Parses RSS 2.0 and Atom feeds into validated Article instances."""

    @classmethod
    def parse_feed(
        cls,
        xml_bytes: bytes,
        source_def: SourceDefinition,
        now: Optional[datetime] = None,
        max_age_hours: int = 16,
    ) -> ParseResult:
        current_time = now.astimezone(timezone.utc) if now else utc_now()
        max_age_delta = timedelta(hours=max_age_hours)
        future_tolerance = timedelta(minutes=15)

        # Handle XML encoding declaration or fallback to UTF-8
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            # Attempt decoding with latin5 / windows-1254 replacement if Turkish encoding declaration failed
            try:
                decoded = xml_bytes.decode("utf-8", errors="replace")
                # Strip null bytes or control characters
                decoded = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", decoded)
                root = ET.fromstring(decoded.encode("utf-8"))
            except Exception as exc:
                logger.warning("Feed '%s' XML parsing failed completely: %s", source_def.name, exc)
                return ParseResult(articles=[], raw_seen=0, stale_count=0, invalid_count=1)

        articles: list[Article] = []
        raw_seen = 0
        stale_count = 0
        invalid_count = 0

        # Detect Atom feed root
        if root.tag == f"{{{NAMESPACES['atom']}}}feed" or root.tag == "feed":
            entries = root.findall(f".//{{{NAMESPACES['atom']}}}entry") or root.findall(".//entry")
            raw_seen = len(entries)
            for entry in entries:
                art, status = cls._parse_atom_entry(entry, source_def, current_time, max_age_delta, future_tolerance)
                if art:
                    articles.append(art)
                elif status == "stale":
                    stale_count += 1
                else:
                    invalid_count += 1
        else:
            # Assume RSS 2.0 / RSS 1.0 / Channel items
            items = root.findall(".//item")
            raw_seen = len(items)
            for item in items:
                art, status = cls._parse_rss_item(item, source_def, current_time, max_age_delta, future_tolerance)
                if art:
                    articles.append(art)
                elif status == "stale":
                    stale_count += 1
                else:
                    invalid_count += 1

        return ParseResult(
            articles=articles,
            raw_seen=raw_seen,
            stale_count=stale_count,
            invalid_count=invalid_count,
        )

    @classmethod
    def parse(
        cls,
        xml_bytes: bytes,
        source_def: SourceDefinition,
        now: Optional[datetime] = None,
        max_age_hours: int = 16,
    ) -> list[Article]:
        return cls.parse_feed(xml_bytes, source_def, now, max_age_hours).articles

    @classmethod
    def _parse_rss_item(
        cls,
        item: ET.Element,
        source_def: SourceDefinition,
        now: datetime,
        max_age: timedelta,
        future_tolerance: timedelta,
    ) -> tuple[Optional[Article], str]:
        raw_title = item.findtext("title") or ""
        title, _ = clean_html(raw_title)
        if not title:
            return None, "invalid"

        # Clean publisher suffix if appended (e.g. "Headline - Variety")
        if title.endswith(f" - {source_def.name}"):
            title = title[: -len(f" - {source_def.name}")].strip()

        # Link and canonical resolution
        raw_link = (item.findtext("link") or "").strip()
        if not raw_link:
            guid = item.find("guid")
            if guid is not None and guid.attrib.get("isPermaLink", "true").lower() == "true":
                raw_link = (guid.text or "").strip()

        if not raw_link or not raw_link.startswith(("http://", "https://")):
            return None, "invalid"

        canonical_url = clean_canonical_url(raw_link)

        # Publication date: try pubDate then Dublin Core date
        raw_date = item.findtext("pubDate") or item.findtext(f"{{{NAMESPACES['dc']}}}date") or ""
        published = parse_datetime(raw_date)
        if not published:
            return None, "invalid"

        # Freshness guard: reject future dates or stale content
        if published > now + future_tolerance or (now - published) > max_age:
            return None, "stale"

        # Description / Snippet
        raw_desc = (
            item.findtext("description")
            or item.findtext(f"{{{NAMESPACES['content']}}}encoded")
            or ""
        )
        snippet, embedded_images = clean_html(raw_desc)
        snippet = snippet[:400]

        # Media Image Extraction
        image_url: Optional[str] = None
        # 1. <enclosure type="image/..." url="..."/>
        for enc in item.findall("enclosure"):
            url = enc.attrib.get("url", "")
            enc_type = enc.attrib.get("type", "")
            if url and ("image" in enc_type or url.lower().endswith((".jpg", ".jpeg", ".png", ".webp"))):
                image_url = url
                break

        # 2. <media:content url="..."> or <media:thumbnail url="...">
        if not image_url:
            for media_tag in (f"{{{NAMESPACES['media']}}}content", f"{{{NAMESPACES['media']}}}thumbnail"):
                media_elem = item.find(media_tag)
                if media_elem is not None and media_elem.attrib.get("url"):
                    image_url = media_elem.attrib.get("url")
                    break

        # 3. Fallback to image found inside HTML description
        if not image_url and embedded_images:
            image_url = embedded_images[0]

        art = Article.create(
            title=title[:300],
            url=raw_link,
            canonical_url=canonical_url,
            source=source_def.name,
            published_at=published,
            discovered_at=now,
            snippet=snippet,
            market=source_def.market,
            source_category=source_def.category,
            image_url=image_url,
            reliability_score=source_def.reliability_score,
            raw_metadata={"feed_id": source_def.source_id, "feed_format": "RSS2"},
        )
        return art, "valid"

    @classmethod
    def _parse_atom_entry(
        cls,
        entry: ET.Element,
        source_def: SourceDefinition,
        now: datetime,
        max_age: timedelta,
        future_tolerance: timedelta,
    ) -> tuple[Optional[Article], str]:
        raw_title = (
            entry.findtext(f"{{{NAMESPACES['atom']}}}title")
            or entry.findtext("title")
            or ""
        )
        title, _ = clean_html(raw_title)
        if not title:
            return None, "invalid"

        # Link extraction
        raw_link = ""
        canonical_url = None
        links = entry.findall(f"{{{NAMESPACES['atom']}}}link") or entry.findall("link")
        for link in links:
            rel = link.attrib.get("rel", "alternate")
            href = link.attrib.get("href", "")
            if rel == "canonical" and href:
                canonical_url = clean_canonical_url(href)
            elif rel == "alternate" and href and not raw_link:
                raw_link = href
            elif not raw_link and href:
                raw_link = href

        if not raw_link:
            entry_id = (
                entry.findtext(f"{{{NAMESPACES['atom']}}}id")
                or entry.findtext("id")
                or ""
            )
            if entry_id.startswith(("http://", "https://")):
                raw_link = entry_id

        if not raw_link:
            return None, "invalid"

        if not canonical_url:
            canonical_url = clean_canonical_url(raw_link)

        # Publication date: try published then updated
        raw_date = (
            entry.findtext(f"{{{NAMESPACES['atom']}}}published")
            or entry.findtext("published")
            or entry.findtext(f"{{{NAMESPACES['atom']}}}updated")
            or entry.findtext("updated")
            or ""
        )
        published = parse_datetime(raw_date)
        if not published:
            return None, "invalid"

        # Freshness guard
        if published > now + future_tolerance or (now - published) > max_age:
            return None, "stale"

        # Summary / Content
        raw_summary = (
            entry.findtext(f"{{{NAMESPACES['atom']}}}summary")
            or entry.findtext("summary")
            or entry.findtext(f"{{{NAMESPACES['atom']}}}content")
            or entry.findtext("content")
            or ""
        )
        snippet, embedded_images = clean_html(raw_summary)
        snippet = snippet[:400]

        # Enclosure image in Atom
        image_url: Optional[str] = None
        for link in links:
            if link.attrib.get("rel") == "enclosure" or "image" in link.attrib.get("type", ""):
                image_url = link.attrib.get("href")
                break

        if not image_url and embedded_images:
            image_url = embedded_images[0]

        art = Article.create(
            title=title[:300],
            url=raw_link,
            canonical_url=canonical_url,
            source=source_def.name,
            published_at=published,
            discovered_at=now,
            snippet=snippet,
            market=source_def.market,
            source_category=source_def.category,
            image_url=image_url,
            reliability_score=source_def.reliability_score,
            raw_metadata={"feed_id": source_def.source_id, "feed_format": "ATOM"},
        )
        return art, "valid"

