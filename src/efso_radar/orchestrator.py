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

from efso_radar.ai.client import (
    AIEvaluationError,
    AIEditorialEngine,
    GeminiEditorialEngine,
    detect_sensitive_tragedy_content,
)
from efso_radar.config import Settings
from efso_radar.deduplication.engine import DeduplicationEngine
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    NotificationRecord,
    Story,
    StoryUpdate,
    SuggestedFormat,
    StoryCategory,
    TelegramNotificationCategory,
    VerificationStatus,
    utc_now,
)
from efso_radar.notifications.base import NotificationChannel
from efso_radar.notifications.formatters import (
    format_telegram_alert,
    format_telegram_breaking,
    format_telegram_digest,
    format_telegram_sensitive_review,
    format_telegram_verification_hold,
)
from efso_radar.notifications.telegram import TelegramNotificationChannel
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.sources.base import NewsSource
from efso_radar.sources.google_news import GoogleNewsSource
from efso_radar.sources.models import DiscoveryMetrics
from efso_radar.sources.multi_source import MultiSourceDiscoveryEngine

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
    discovery_metrics: Optional[DiscoveryMetrics] = None


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
        self.source = source or MultiSourceDiscoveryEngine(include_google_news=True)
        self.ai_engine = ai_engine or GeminiEditorialEngine(settings, repository=self.repository)
        self.notifier = notifier or TelegramNotificationChannel(settings)
        self.deduper = DeduplicationEngine(self.repository)

    def run_scan_only(
        self,
        dry_run: bool = False,
        worker_id: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> RadarRunResult:
        """Execute RSS discovery, deduplication, and durable backlog persistence without calling Gemini."""
        return self.run(dry_run=dry_run, worker_id=worker_id, now=now, scan_only=True)

    def is_digest_due(self, now: Optional[datetime] = None) -> bool:
        """Check if enough time has elapsed since the last consolidated digest delivery."""
        ref_now = now or utc_now()
        last_sent = self.repository.get_last_digest_sent_at()
        if last_sent is None:
            return True
        elapsed_minutes = (ref_now - last_sent).total_seconds() / 60.0
        return elapsed_minutes >= self.settings.digest_interval_minutes

    def flush_due_digest(
        self,
        dry_run: bool = False,
        now: Optional[datetime] = None,
        force: bool = False,
    ) -> int:
        """Flush queued digest notifications into a consolidated Telegram digest if due and outside quiet hours."""
        ref_now = now or utc_now()
        if self.settings.is_in_quiet_hours(ref_now):
            return 0
        if not force and not self.is_digest_due(ref_now):
            return 0

        sent_today = self.repository.get_daily_sent_message_count(now=ref_now)
        if sent_today >= self.settings.target_daily_telegram_messages:
            logger.info("Daily Telegram target (%d) reached; deferring queued digest to next window", self.settings.target_daily_telegram_messages)
            return 0

        queued = self.repository.get_queued_digest_notifications(
            max_age_hours=self.settings.effective_backlog_retention_hours(),
            now=ref_now,
        )
        if not queued:
            return 0

        batch = queued[: self.settings.max_items_per_digest]
        synthetic_assessments = [
            EditorialAssessment(
                article_ids=[q.story_id],
                score=q.score,
                kind=q.kind,
                headline_tr=q.headline_tr,
                cover_title_tr=q.headline_tr.upper()[:45],
                caption_idea_tr=q.message_text[:180] if q.message_text else q.headline_tr,
                why_efso_tr="Editoryal özet seçkisi",
                suggested_format=SuggestedFormat.POST,
                category=StoryCategory.YERLI,
            )
            for q in batch
        ]
        hours_label = max(1, self.settings.digest_interval_minutes // 60)
        digest_text = format_telegram_digest(
            synthetic_assessments,
            window_label=f"{hours_label} Saatlik Editoryal Özet",
        )

        if not dry_run:
            if hasattr(self.notifier, "send_digest_batch"):
                res = self.notifier.send_digest_batch(
                    synthetic_assessments,
                    notification_id=f"DIGEST-{ref_now.strftime('%Y%m%d%H%M%S')}",
                    window_label=f"{hours_label} Saatlik Editoryal Özet",
                    dry_run=dry_run,
                    now=ref_now,
                )
                if res.status != DeliveryStatus.SENT:
                    return 0
            elif self.settings.telegram_bot_token and self.settings.telegram_chat_id and hasattr(self.notifier, "_dispatch_telegram"):
                try:
                    res = self.notifier._dispatch_telegram(
                        f"DIGEST-{ref_now.strftime('%Y%m%d%H%M%S')}",
                        batch[0].story_id,
                        None,
                        None,
                        digest_text,
                        ref_now,
                        self.settings.telegram_chat_id,
                        TelegramNotificationCategory.EDITORIAL_DIGEST,
                        "TELEGRAM_DIGEST",
                    )
                except TypeError:
                    res = self.notifier._dispatch_telegram(
                        f"DIGEST-{ref_now.strftime('%Y%m%d%H%M%S')}",
                        batch[0].story_id,
                        None,
                        None,
                        digest_text,
                        ref_now,
                    )
                if res.status != DeliveryStatus.SENT:
                    return 0
            self.repository.mark_digest_notifications_dispatched(
                [q.notification_id for q in batch],
                delivered_at=ref_now,
            )
        return 1

    def run(
        self,
        dry_run: bool = False,
        worker_id: Optional[str] = None,
        now: Optional[datetime] = None,
        scan_only: bool = False,
        use_hybrid_digest: bool = False,
    ) -> RadarRunResult:
        start_time = time.monotonic()
        run_now = now or utc_now()
        now = run_now
        worker = worker_id or f"worker-{run_now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        effective_age_hours = self.settings.effective_max_age_hours()
        effective_backlog_hours = self.settings.effective_backlog_retention_hours()

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
                try:
                    expired = self.repository.expire_stale_held_notifications(
                        self.settings.max_quiet_hours_hold_age_hours, now=now
                    )
                except TypeError:
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
                try:
                    held_records = self.repository.get_held_notifications(
                        max_age_hours=self.settings.max_quiet_hours_hold_age_hours, now=now
                    )
                except TypeError:
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
                                        held.notification_id, held.story_id, held.update_id, None, held.message_text, now
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

            # 4. Ingestion: Fetch candidates from external news sources (interval-resilient freshness)
            articles, successes = self.source.fetch_candidates(now, max_age_hours=effective_age_hours)
            discovery_metrics = getattr(self.source, "last_metrics", None)
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
                    discovery_metrics=discovery_metrics,
                )

            # 4b. Retrieve unexpired pending candidates from durable backlog to prevent feed-rotation loss
            try:
                pending_backlog = self.repository.get_pending_candidates(
                    max_age_hours=effective_backlog_hours, now=now
                )
            except TypeError:
                pending_backlog = self.repository.get_pending_candidates(
                    max_age_hours=effective_backlog_hours
                )
            if pending_backlog:
                logger.info("Retrieved %d pending candidates from durable backlog", len(pending_backlog))
                seen_ids = {a.id for a in articles}
                for back_item in pending_backlog:
                    if back_item.id not in seen_ids:
                        articles.append(back_item)
                        seen_ids.add(back_item.id)

            # 5. Deduplication, cross-publisher clustering, and candidate prioritization
            candidates = self.deduper.filter_fresh_candidates(
                articles, max_candidates=self.settings.max_candidates
            )
            deprioritized = getattr(self.deduper, "last_deprioritized", [])

            # 5b. Scan-Only / Decoupled Discovery Mode: persist all fresh candidates to backlog without calling Gemini
            if scan_only or not self.settings.ai_evaluation_enabled:
                all_to_queue = list(candidates) + list(deprioritized)
                if not dry_run and all_to_queue:
                    self.repository.save_pending_candidates(all_to_queue)
                duration = time.monotonic() - start_time
                return RadarRunResult(
                    success=True,
                    sources_succeeded=successes,
                    articles_collected=len(articles),
                    candidates_evaluated=0,
                    picks_approved=0,
                    notifications_dispatched=notifications_dispatched,
                    dry_run=dry_run,
                    duration_seconds=duration,
                    message=f"Scan-only completed: queued {len(all_to_queue)} candidates in backlog (0 Gemini calls).",
                    discovery_metrics=discovery_metrics,
                )

            # Filter out explicitly dampened out-of-scope / grim news before AI evaluation to save tokens
            ai_candidates: list[Article] = []
            noise_dropped: list[Article] = []
            for c in candidates:
                reasons = c.raw_metadata.get("priority_reasons", [])
                p_score = float(c.raw_metadata.get("priority_score", 10.0))
                if "out_of_scope_or_grim_news_dampened" in reasons or (
                    len(candidates) > self.settings.ai_batch_size
                    and p_score < self.settings.min_discovery_priority_for_ai
                ):
                    noise_dropped.append(c)
                else:
                    ai_candidates.append(c)

            if noise_dropped and not dry_run:
                self.repository.mark_articles_processed(noise_dropped)

            candidates = ai_candidates

            # Persist deprioritized candidates into durable backlog so they survive RSS feed expiration
            if not dry_run and deprioritized:
                self.repository.save_pending_candidates(deprioritized)

            if discovery_metrics:
                discovery_metrics.story_clusters_formed = getattr(self.deduper, "last_clusters_count", len(candidates))
                discovery_metrics.selected_for_ai = len(candidates)
                discovery_metrics.deprioritized_by_limit = len(deprioritized)
                logger.info("Updated %s", discovery_metrics.summary())

            logger.info(
                "Feeds responded: %d, Articles gathered: %d, Fresh candidates for AI: %d",
                successes,
                len(articles),
                len(candidates),
            )

            # If no fresh candidates exist, check if a periodic digest is due, then remain silent
            if not candidates:
                if use_hybrid_digest or self.settings.notification_strategy == "DIGEST_ONLY":
                    notifications_dispatched += self.flush_due_digest(dry_run=dry_run, now=now)
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
                    discovery_metrics=discovery_metrics,
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
            try:
                picks = self.ai_engine.evaluate(candidates, recent_stories, now)
            except AIEvaluationError as exc:
                msg = f"AI editorial evaluation aborted: {exc}"
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
                    discovery_metrics=discovery_metrics,
                )
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

                # 7. Sensitive Review, Verification Hold & Hybrid Digest Routing vs Immediate Notification Delivery
                is_sensitive_review = (
                    getattr(pick, "disposition", None) == EditorialDisposition.SENSITIVE_REVIEW
                    or (pick.uncertainties_tr and "SENSITIVE EDITORIAL REVIEW" in pick.uncertainties_tr)
                    or detect_sensitive_tragedy_content(
                        pick.headline_tr,
                        pick.caption_idea_tr,
                        pick.relevance_reason_tr,
                        pick.why_efso_tr,
                    )
                )
                is_unverified = pick.verification_status in (
                    VerificationStatus.UNVERIFIED_RUMOR,
                    VerificationStatus.SPECULATION,
                    VerificationStatus.ATTRIBUTED_CLAIM,
                )
                should_queue_digest = (
                    (use_hybrid_digest or self.settings.notification_strategy == "DIGEST_ONLY")
                    and not is_unverified
                    and not is_sensitive_review
                    and (
                        self.settings.notification_strategy == "DIGEST_ONLY"
                        or pick.score < self.settings.breaking_alert_min_score
                    )
                )

                if is_sensitive_review:
                    notif_suffix = update_id if update_id else story_id
                    record = NotificationRecord(
                        notification_id=f"HOLD-SENSITIVE-{now.strftime('%Y%m%d%H%M%S')}-{notif_suffix}",
                        story_id=story_id,
                        update_id=update_id,
                        kind=pick.kind,
                        channel="TELEGRAM_SENSITIVE_REVIEW",
                        sent_at=now,
                        status=DeliveryStatus.HELD_FOR_SENSITIVE_REVIEW,
                        message_text=format_telegram_sensitive_review(pick),
                        headline_tr=pick.headline_tr,
                        score=pick.score,
                        error_message="Held for sensitive review: requires explicit Product Owner approval",
                        category=TelegramNotificationCategory.SENSITIVE_EDITORIAL_REVIEW,
                    )
                elif is_unverified:
                    notif_suffix = update_id if update_id else story_id
                    record = NotificationRecord(
                        notification_id=f"HOLD-VERIF-{now.strftime('%Y%m%d%H%M%S')}-{notif_suffix}",
                        story_id=story_id,
                        update_id=update_id,
                        kind=pick.kind,
                        channel="TELEGRAM",
                        sent_at=now,
                        status=DeliveryStatus.HELD_FOR_VERIFICATION,
                        message_text=format_telegram_verification_hold(pick),
                        headline_tr=pick.headline_tr,
                        score=pick.score,
                        error_message="Held for verification: unverified rumor or attributed claim",
                        category=TelegramNotificationCategory.HOLD_FOR_VERIFICATION,
                    )
                elif should_queue_digest:
                    notif_suffix = update_id if update_id else story_id
                    record = NotificationRecord(
                        notification_id=f"DIGEST-Q-{now.strftime('%Y%m%d%H%M%S')}-{notif_suffix}",
                        story_id=story_id,
                        update_id=update_id,
                        kind=pick.kind,
                        channel="TELEGRAM_DIGEST",
                        sent_at=now,
                        status=DeliveryStatus.QUEUED_FOR_DIGEST,
                        message_text=format_telegram_alert(pick),
                        headline_tr=pick.headline_tr,
                        score=pick.score,
                        category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                    )
                else:
                    try:
                        record = self.notifier.send(
                            pick,
                            story_id,
                            update_id=update_id,
                            category=TelegramNotificationCategory.VERIFIED_BREAKING,
                            dry_run=dry_run,
                            now=now,
                        )
                    except TypeError:
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

            # 8b. Flush consolidated digest if due
            if use_hybrid_digest or self.settings.notification_strategy == "DIGEST_ONLY":
                notifications_dispatched += self.flush_due_digest(dry_run=dry_run, now=now)

            # 9. Mark evaluated candidates as processed and transition backlog state
            if not dry_run and candidates:
                self.repository.mark_articles_processed(candidates)
                self.repository.mark_candidates_evaluated([c.id for c in candidates])

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
                discovery_metrics=discovery_metrics,
            )

        finally:
            if not dry_run:
                self.repository.release_run_lock(worker)


