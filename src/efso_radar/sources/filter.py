"""Deterministic news relevance and quality prefilter for Efso Radar.

Filters out malformed items, exact URL duplicates, and obvious non-entertainment
hard news noise (sports match stats, macroeconomic announcements, non-celebrity
traffic accidents) prior to expensive AI evaluation.
"""
from __future__ import annotations

import logging
import re
from typing import NamedTuple, Optional

from efso_radar.domain import Article, normalize_text

logger = logging.getLogger("efso-radar.sources.filter")

# Hard news noise indicators that occasionally appear in general/magazin RSS feeds
_UNRELATED_PATTERNS: list[re.Pattern] = [
    # Pure sports match statistics & lineups
    re.compile(r"\b(puan durumu|ilk 11'?ler|maç özeti|canlı maç izle|hakem düdüğü|derbi skoru|fikstür)\b", re.IGNORECASE),
    # Macroeconomic & political administrative policy
    re.compile(r"\b(asgari ücret zammı|emekli maaşı zammı|faiz kararı açıklandı|enflasyon rakamları|dolar kuru bugün|borsa günü|kabine toplantısı kararları|seçim anketi)\b", re.IGNORECASE),
    # Non-celebrity blotter accidents & regional crimes
    re.compile(r"\b(zincirleme trafik kazası|feci kaza:\s*\d+|tır ile otomobil çarpıştı|silahlı çatışmada \d+ ölü|uyuşturucu tacirlerine operasyon)\b", re.IGNORECASE),
]

# Entertainment/celebrity override indicators: if present, NEVER filter as unrelated!
_ENTERTAINMENT_OVERRIDE: list[str] = [
    "ünlü", "oyuncu", "şarkıcı", "sanatçı", "fenomen", "influencer",
    "dizi", "film", "hollywood", "netflix", "sinema", "konser", "albüm",
    "aşk", "sevgili", "evlilik", "ayrılık", "boşan", "bebek", "hamile",
    "magazin", "oscar", "grammy", "cannes", "festival", "viral", "trend",
    "celebrity", "actor", "actress", "singer", "pop star", "movie", "series",
]


class NewsRelevancePrefilter:
    """Inexpensive deterministic filter applied to candidate news articles."""

class FilterBatchResult(NamedTuple):
    approved: list[Article]
    rejected_unrelated: list[tuple[Article, str]]
    url_duplicates_count: int
    rejected_malformed: list[tuple[Article, str]]


class NewsRelevancePrefilter:
    """Inexpensive deterministic filter applied to candidate news articles."""

    @classmethod
    def filter_batch_detailed(
        cls,
        articles: list[Article],
    ) -> FilterBatchResult:
        """
        Filter a batch of raw articles with itemized rejection accounting.
        """
        approved: list[Article] = []
        seen_urls: set[str] = set()
        seen_titles_by_publisher: dict[str, set[str]] = {}
        rejected_unrelated: list[tuple[Article, str]] = []
        rejected_malformed: list[tuple[Article, str]] = []
        url_duplicates_count = 0

        for art in articles:
            # 1. Structural and URL validation
            if not cls.is_structurally_valid(art):
                rejected_malformed.append((art, "Malformed title length or invalid URL scheme"))
                continue

            # 2. Duplicate URL suppression (using canonical URL if available)
            target_url = art.canonical_url or art.url
            if target_url in seen_urls:
                url_duplicates_count += 1
                continue

            # 3. Same-publisher near-identical title deduplication
            pub_key = art.source.lower()
            norm_title = normalize_text(art.title)
            if pub_key in seen_titles_by_publisher and norm_title in seen_titles_by_publisher[pub_key]:
                url_duplicates_count += 1
                continue

            # 4. Filter unrelated hard news / sports stats / blotter crimes
            unrelated_reason = cls.get_unrelated_hard_news_reason(art.title, art.snippet)
            if unrelated_reason:
                logger.debug("Filtered out unrelated news: '%s' (%s) [%s]", art.title, art.source, unrelated_reason)
                rejected_unrelated.append((art, unrelated_reason))
                continue

            # Passed all prefilters
            seen_urls.add(target_url)
            seen_titles_by_publisher.setdefault(pub_key, set()).add(norm_title)
            approved.append(art)

        return FilterBatchResult(
            approved=approved,
            rejected_unrelated=rejected_unrelated,
            url_duplicates_count=url_duplicates_count,
            rejected_malformed=rejected_malformed,
        )

    @classmethod
    def filter_batch(
        cls,
        articles: list[Article],
    ) -> tuple[list[Article], int]:
        """
        Filter a batch of raw articles.
        Returns (approved_articles, total_rejected_count).
        """
        res = cls.filter_batch_detailed(articles)
        total_rejected = len(res.rejected_unrelated) + len(res.rejected_malformed) + res.url_duplicates_count
        return res.approved, total_rejected

    @classmethod
    def is_structurally_valid(cls, article: Article) -> bool:
        """Validate title length and URL formatting."""
        if not article.title or len(article.title.strip()) < 10:
            return False
        if len(article.title) > 350:
            return False
        if not article.url or not article.url.startswith(("http://", "https://")):
            return False
        if not article.source or not article.source.strip():
            return False
        return True

    @classmethod
    def get_unrelated_hard_news_reason(cls, title: str, snippet: str = "") -> Optional[str]:
        """Check if headline is unrelated hard news and return reason label, or None."""
        text = f"{title} {snippet}".lower()

        # If any entertainment/celebrity indicator is present, protect it
        for override in _ENTERTAINMENT_OVERRIDE:
            if override in text:
                return None

        # Check against hard news noise patterns with specific labels
        pattern_labels = [
            "sports_match_statistics",
            "macroeconomics_administrative_policy",
            "non_celebrity_blotter_accident",
        ]
        for pattern, label in zip(_UNRELATED_PATTERNS, pattern_labels):
            if pattern.search(text):
                return label

        return None

    @classmethod
    def is_unrelated_hard_news(cls, title: str, snippet: str = "") -> bool:
        """Check if headline is unrelated hard news without celebrity relevance."""
        return cls.get_unrelated_hard_news_reason(title, snippet) is not None

