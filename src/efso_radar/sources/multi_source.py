"""Multi-source news discovery engine with concurrent bounded fetching,
fault isolation, observability metrics, and deterministic prefiltering.
"""
from __future__ import annotations

import concurrent.futures
import logging
import time
from datetime import datetime
from typing import Optional

from efso_radar.domain import Article, utc_now
from efso_radar.sources.base import NewsSource
from efso_radar.sources.filter import NewsRelevancePrefilter
from efso_radar.sources.google_news import GoogleNewsSource
from efso_radar.sources.models import DiscoveryMetrics, SourceDefinition
from efso_radar.sources.registry import SourceRegistry
from efso_radar.sources.rss import RSSFeedSource

logger = logging.getLogger("efso-radar.sources.multi_source")


class MultiSourceDiscoveryEngine(NewsSource):
    """
    Orchestrates discovery across verified direct publisher feeds and Google News.
    Provides bounded concurrent fetching, independent source failure isolation,
    and detailed metrics reporting.
    """

    def __init__(
        self,
        registry: Optional[SourceRegistry] = None,
        google_news_source: Optional[GoogleNewsSource] = None,
        include_google_news: bool = True,
        max_workers: int = 6,
        user_agent: Optional[str] = None,
    ):
        self.registry = registry or SourceRegistry()
        self.google_news_source = google_news_source or (
            GoogleNewsSource() if include_google_news else None
        )
        self.include_google_news = include_google_news and (self.google_news_source is not None)
        self.max_workers = max_workers
        self.user_agent = user_agent
        self.last_metrics: Optional[DiscoveryMetrics] = None
        self.last_filter_details = None

    def fetch_candidates(self, now: datetime, max_age_hours: int = 16) -> tuple[list[Article], int]:
        """
        Fetch articles across all active direct sources and Google News.
        Returns (sorted_candidates, successful_sources_count).
        """
        start_time = time.monotonic()
        active_sources = self.registry.get_active()

        attempted = len(active_sources) + (1 if self.include_google_news else 0)
        succeeded = 0
        raw_articles: list[Article] = []
        errors: dict[str, str] = {}
        raw_seen_total = 0
        stale_skipped_total = 0
        invalid_skipped_total = 0

        # 1. Fetch direct RSS/Atom sources concurrently with bounded thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_source = {
                executor.submit(self._fetch_single_source, src_def, now, max_age_hours): src_def
                for src_def in active_sources
            }

            for future in concurrent.futures.as_completed(future_to_source):
                src_def = future_to_source[future]
                try:
                    res = future.result()
                    succeeded += 1
                    raw_articles.extend(res.articles)
                    raw_seen_total += res.raw_seen
                    stale_skipped_total += res.stale_count
                    invalid_skipped_total += res.invalid_count
                    logger.debug("Source '%s' yielded %d articles (seen=%d, stale=%d)",
                                 src_def.name, len(res.articles), res.raw_seen, res.stale_count)
                except Exception as exc:
                    err_msg = str(exc)
                    errors[src_def.name] = err_msg
                    logger.warning("Source '%s' fetch failed (isolated): %s", src_def.name, err_msg)

        # 2. Fetch Google News secondary layer if enabled
        if self.include_google_news and self.google_news_source:
            try:
                gn_articles, gn_success = self.google_news_source.fetch_candidates(now, max_age_hours)
                if gn_success > 0:
                    succeeded += 1
                    raw_articles.extend(gn_articles)
                    raw_seen_total += len(gn_articles)
                    logger.debug("Google News secondary layer yielded %d articles (%d feeds)",
                                 len(gn_articles), gn_success)
                else:
                    errors["GoogleNews"] = "All Google News query feeds failed"
            except Exception as exc:
                errors["GoogleNews"] = str(exc)
                logger.warning("Google News secondary layer failed: %s", exc)

        parsed_total = len(raw_articles)

        # 3. Deterministic relevance and duplicate prefiltering
        filter_res = NewsRelevancePrefilter.filter_batch_detailed(raw_articles)
        self.last_filter_details = filter_res
        prefilter_rejected = len(filter_res.rejected_unrelated) + len(filter_res.rejected_malformed)
        url_duplicates = filter_res.url_duplicates_count

        # 4. Deduplicate across feeds by article ID and sort by publication date
        unique_map: dict[str, Article] = {}
        for art in filter_res.approved:
            if art.id not in unique_map:
                unique_map[art.id] = art
            else:
                url_duplicates += 1

        sorted_candidates = sorted(
            unique_map.values(), key=lambda a: a.published_at, reverse=True
        )

        duration = time.monotonic() - start_time
        metrics = DiscoveryMetrics(
            sources_attempted=attempted,
            sources_succeeded=succeeded,
            raw_feed_items_seen=raw_seen_total,
            stale_items_skipped=stale_skipped_total,
            invalid_items_skipped=invalid_skipped_total,
            parsed_articles=parsed_total,
            prefilter_rejected=prefilter_rejected,
            url_duplicates_removed=url_duplicates,
            eligible_fresh_articles=len(sorted_candidates),
            source_errors=errors,
            duration_seconds=duration,
        )
        self.last_metrics = metrics
        logger.info(metrics.summary())

        return sorted_candidates, succeeded

    def _fetch_single_source(
        self,
        source_def: SourceDefinition,
        now: datetime,
        max_age_hours: int,
    ):
        client = RSSFeedSource(
            source_def=source_def,
            user_agent=self.user_agent or RSSFeedSource.__init__.__defaults__[0],
        )
        return client.fetch_result(now=now, max_age_hours=max_age_hours)

