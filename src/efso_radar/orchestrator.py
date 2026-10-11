"""Orchestrator for Efso Radar.

Coordinates news discovery, multi-level deduplication, AI editorial evaluation,
notification dispatch, and persistent state management with strict concurrency
and idempotency controls.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime
from typing import NamedTuple, Optional

from efso_radar.ai.client import AIEditorialEngine, GeminiEditorialEngine
from efso_radar.config import Settings
from efso_radar.deduplication.engine import DeduplicationEngine
from efso_radar.domain import (
    DeliveryStatus,
    EditorialKind,
    Story,
    StoryUpdate,
    utc_now,
)
from efso_radar.notifications.base import NotificationChannel
from efso_radar.notifications.telegram import TelegramNotificationChannel
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.sources.base import NewsSource
from efso_radar.sources.google_news import GoogleNewsSource

logger = logging.getLogger("efso-radar.orchestrator")


class RadarRunResult(NamedTuple):
    success: bool
    sources_succeeded: int
    articles_collected: int
    candidates_evaluated: int
    picks_approved: int
    notifications_dispatched: int
    dry_run: bool
    duration_seconds: float
    message: str = ""


class RadarOrchestrator:
    """Central pipeline coordinator for Efso Radar."""

    def __init__(
        self,
        settings: Settings,
        repository: Optional[StateRepository] = None,
        source: Optional[NewsSource] = None,
        ai_engine: Optional[AIEditorialEngine] = None,
        notifier: Optional[NotificationChannel] = None,
    ):
        self.settings = settings
        self.repository = repository or SqliteStateRepository(settings.sqlite_db_path)
        self.source = source or GoogleNewsSource(timeout_seconds=settings.http_timeout_seconds)
        self.ai_engine = ai_engine or GeminiEditorialEngine(settings)
        self.notifier = notifier or TelegramNotificationChannel(settings)
        self.deduper = DeduplicationEngine(self.repository)

    def run(
        self,
        dry_run: bool = False,
        worker_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> RadarRunResult:
        start_time = time.monotonic()
        run_now = now or utc_now()
        now = run_now
        worker = worker_id or f"worker-{run_now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"


        # 1. Idempotency & Concurrency: Acquire mutual exclusion lock
        if not dry_run and not self.repository.acquire_run_lock(worker, timeout_minutes=self.settings.lock_timeout_minutes):
            msg = f"Another execution is currently in progress. Worker {worker} yielding gracefully."
            logger.warning(msg)
            return RadarRunResult(
                success=True,
                sources_succeeded=0,
                articles_collected=0,
                candidates_evaluated=0,
                picks_approved=0,
                notifications_dispatched=0,
                dry_run=dry_run,
                duration_seconds=time.monotonic() - start_time,
                message=msg,
            )

        try:
            # 2. Maintenance: Purge old processed records
            purged = self.repository.purge_expired_records(retention_days=35)
            if purged > 0:
                logger.info("Purged %d expired article records from repository", purged)

            # 3. Load recent context stories
            recent_stories = self.repository.get_recent_stories(limit=80, days=30)

            # 3b. Deliver queued quiet-hours notifications if now outside quiet hours
            notifications_dispatched = 0
            if not dry_run:
                # Recover expired dispatch claims from any interrupted workers
                recovered = self.repository.recover_expired_dispatch_claims()
                if recovered > 0:
                    logger.warning("Recovered %d orphaned/expired dispatch claims", recovered)

                # Expire stale held notifications older than freshness window
                expired = self.repository.expire_stale_held_notifications(
                    self.settings.max_quiet_hours_hold_age_hours
                )
                if expired > 0:
                    logger.info(
                        "Expired %d stale quiet-hours notifications (>%dh old)",
                        expired,
                        self.settings.max_quiet_hours_hold_age_hours,
                    )

            if not self.settings.is_in_quiet_hours(now):
                held_records = self.repository.get_held_notifications(
                    max_age_hours=self.settings.max_quiet_hours_hold_age_hours
                )
                if held_records:
                    logger.info("Found %d queued quiet-hours notifications to deliver", len(held_records))
                    for held in held_records:
                        if not dry_run:
                            # Claim notification with lease before attempting external dispatch
                            claimed = self.repository.claim_notification_for_dispatch(
                                notification_id=held.notification_id,
                                story_id=held.story_id,
                                kind=held.kind,
                                headline_tr=held.headline_tr,
                                score=held.score,
                                message_text=held.message_text,
                                worker_id=worker,
                                lease_seconds=180,
                            )
                            if not claimed:
                                logger.info("Notification %s claimed by another worker; skipping", held.notification_id)
                                continue

                            # If real telegram channel configured, dispatch
                            try:
                                if self.settings.telegram_bot_token and self.settings.telegram_chat_id:
                                    dispatch_res = self.notifier._dispatch_telegram(
                                        held.notification_id, held.story_id, None, held.message_text, now
                                    ) if hasattr(self.notifier, "_dispatch_telegram") else held
                                    if dispatch_res.status == DeliveryStatus.SENT:
                                        self.repository.mark_notification_delivered(held.notification_id, now)
                                        notifications_dispatched += 1
                                    else:
                                        self.repository.record_notification_failure(
                                            held.notification_id,
                                            dispatch_res.error_message or "Unknown dispatch failure",
                                        )
                                else:
                                    self.repository.mark_notification_delivered(held.notification_id, now)
                                    notifications_dispatched += 1
                            except Exception as exc:
                                logger.error("Failed delivering queued notification %s: %s", held.notification_id, exc)
                                self.repository.record_notification_failure(held.notification_id, str(exc))
                        else:
                            logger.info("[DRY-RUN] Would deliver queued quiet-hours alert for %s", held.story_id)
                            notifications_dispatched += 1

            # 4. Ingestion: Fetch candidates from external news sources
            articles, successes = self.source.fetch_candidates(now, max_age_hours=self.settings.max_age_hours)
            if successes == 0:
                msg = "All news feeds failed to respond; aborting run to prevent state corruption."
                logger.error(msg)
                return RadarRunResult(
                    success=False,
                    sources_succeeded=0,
                    articles_collected=0,
                    candidates_evaluated=0,
                    picks_approved=0,
                    notifications_dispatched=notifications_dispatched,
                    dry_run=dry_run,
                    duration_seconds=time.monotonic() - start_time,
                    message=msg,
                )

            # 5. Deduplication and cross-publisher clustering
            candidates = self.deduper.filter_fresh_candidates(
                articles, max_candidates=self.settings.max_candidates
            )
            logger.info(
                "Feeds responded: %d, Articles gathered: %d, Fresh candidates: %d",
                successes,
                len(articles),
                len(candidates),
            )

            # If no fresh candidates exist, remain silent
            if not candidates:
                return RadarRunResult(
                    success=True,
                    sources_succeeded=successes,
                    articles_collected=len(articles),
                    candidates_evaluated=0,
                    picks_approved=0,
                    notifications_dispatched=notifications_dispatched,
                    dry_run=dry_run,
                    duration_seconds=time.monotonic() - start_time,
                    message="No new candidates found. Remained silent.",
                )

            # Lease renewal before AI call to prevent lock expiry during model generation
            if not dry_run:
                renewed = self.repository.renew_run_lock(worker, extend_minutes=self.settings.lock_timeout_minutes)
                if not renewed:
                    msg = f"Worker {worker} lost its run lock lease; aborting run immediately to prevent split-brain processing."
                    logger.error(msg)
                    return RadarRunResult(
                        success=False,
                        sources_succeeded=successes,
                        articles_collected=len(articles),
                        candidates_evaluated=len(candidates),
                        picks_approved=0,
                        notifications_dispatched=notifications_dispatched,
                        dry_run=dry_run,
                        duration_seconds=time.monotonic() - start_time,
                        message=msg,
                    )

            # 6. AI Editorial Evaluation
            picks = self.ai_engine.evaluate(candidates, recent_stories, now)
            logger.info("Editorial evaluation approved %d stories", len(picks))

            for pick in picks:
                if pick.kind == EditorialKind.UPDATE and pick.parent_story_id:
                    story_id = pick.parent_story_id
                    update_id = f"UPD-{now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:4]}"
                else:
                    primary_article_id = pick.article_ids[0][:8]
                    story_id = f"EFSO-{now.strftime('%Y%m%d')}-{primary_article_id}"
                    update_id = None

                # Idempotency check: Guard against duplicate alert if already notified or in-flight
                if pick.kind == EditorialKind.UPDATE:
                    if self.repository.has_story_been_alerted(story_id, kind=EditorialKind.UPDATE, development_tr=pick.development_tr):
                        logger.info("Development for story %s already alerted; skipping duplicate", story_id)
                        continue
                else:
                    if self.repository.has_story_been_alerted(story_id, kind=EditorialKind.NEW):
                        logger.info("Story %s has already been alerted; skipping duplicate notification", story_id)
                        continue

                # 7. Notification Delivery (with distinct update identity when applicable)
                try:
                    record = self.notifier.send(pick, story_id, update_id=update_id, dry_run=dry_run, now=now)
                except TypeError:
                    record = self.notifier.send(pick, story_id, dry_run=dry_run, now=now)
                if record.status in (DeliveryStatus.SENT, DeliveryStatus.HELD_QUIET_HOURS):
                    notifications_dispatched += 1

                # 8. Record in state repository atomically
                if not dry_run:
                    self.repository.record_notification(record)
                    self.repository.record_editorial_assessment(pick, story_id)

                    if pick.kind == EditorialKind.NEW:
                        story = Story(
                            story_id=story_id,
                            headline_tr=pick.headline_tr,
                            category=pick.category,
                            first_seen_at=now,
                            last_updated_at=now,
                            article_ids=pick.article_ids,
                            update_count=0,
                            is_active=True,
                        )
                        self.repository.record_new_story(story, pick)
                    elif pick.kind == EditorialKind.UPDATE and pick.parent_story_id and update_id:
                        update = StoryUpdate(
                            update_id=update_id,
                            story_id=pick.parent_story_id,
                            development_tr=pick.development_tr or "",
                            detected_at=now,
                            article_ids=pick.article_ids,
                            significance_score=pick.score,
                        )
                        self.repository.record_story_update(update, pick)

                    # Mark supporting articles as processed immediately
                    self.repository.mark_articles_processed(pick.supporting_articles)

            # 9. Mark all ingested articles from this run as processed so they won't repeat
            if not dry_run and articles:
                self.repository.mark_articles_processed(articles)

            duration = time.monotonic() - start_time
            return RadarRunResult(
                success=True,
                sources_succeeded=successes,
                articles_collected=len(articles),
                candidates_evaluated=len(candidates),
                picks_approved=len(picks),
                notifications_dispatched=notifications_dispatched,
                dry_run=dry_run,
                duration_seconds=duration,
                message=f"Completed successfully in {duration:.2f}s",
            )

        finally:
            if not dry_run:
                self.repository.release_run_lock(worker)

