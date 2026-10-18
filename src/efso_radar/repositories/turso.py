"""Turso (libSQL over HTTP) implementation of the StateRepository interface.

Portable with SqliteStateRepository, uses parameterized Hrana v2 HTTP pipelines,
supports remote schema migrations, run locking, and notification claim leases.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
import httpx

from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    Market,
    NotificationRecord,
    OwnerFeedback,
    OwnerFeedbackRating,
    Story,
    StoryCategory,
    StoryUpdate,
    normalize_text,
    utc_now,
)
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.schema import (
    CURRENT_SCHEMA_VERSION,
    get_all_init_statements,
    get_column_migrations,
)
from efso_radar.repositories.turso_client import TursoClientError, TursoHTTPClient

logger = logging.getLogger("efso-radar.repository.turso")


class TursoStateRepository(StateRepository):
    """Turso-backed state repository communicating over HTTP /v2/pipeline."""

    def __init__(
        self,
        database_url: str,
        auth_token: str = "",
        timeout_seconds: float = 15.0,
        transport: Optional[httpx.BaseTransport] = None,
        client: Optional[TursoHTTPClient] = None,
    ):
        self.client = client or TursoHTTPClient(
            database_url=database_url,
            auth_token=auth_token,
            timeout_seconds=timeout_seconds,
            transport=transport,
        )
        self._init_db()

    def _init_db(self) -> None:
        """Initialize Turso tables and migrations."""
        stmts: list[tuple[str, list[Any]]] = [
            (ddl.strip(), []) for ddl in get_all_init_statements() if ddl.strip()
        ]
        now_iso = utc_now().isoformat()
        stmts.append((
            """
            INSERT OR IGNORE INTO schema_migrations (version, applied_at, description)
            VALUES (?, ?, ?)
            """,
            [CURRENT_SCHEMA_VERSION, now_iso, "Turso schema initialization"],
        ))
        self.client.execute_pipeline(stmts)

        try:
            res = self.client.execute("PRAGMA table_info(notifications)")
            existing_cols = {row["name"] for row in res.fetchall() if "name" in row}
            for table, col, col_type in get_column_migrations():
                if table == "notifications" and col not in existing_cols:
                    self.client.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type};")
                    existing_cols.add(col)
        except Exception as exc:
            logger.debug("Turso column migration check: %s", exc)


    def get_schema_version(self) -> int:
        res = self.client.execute("SELECT MAX(version) AS ver FROM schema_migrations")
        row = res.fetchone()
        return int(row["ver"]) if row and row.get("ver") is not None else 0

    # --- Run Locking & Concurrency ---
    def acquire_run_lock(self, worker_id: str, timeout_minutes: int = 15) -> bool:
        now = utc_now()
        now_iso = now.isoformat()
        expires_at = (now + timedelta(minutes=timeout_minutes)).isoformat()

        res = self.client.execute(
            "SELECT worker_id, expires_at FROM run_locks WHERE lock_key = 'radar_cron'"
        )
        row = res.fetchone()
        if row:
            if row["expires_at"] > now_iso and row["worker_id"] != worker_id:
                logger.warning(
                    "Turso run lock currently held by worker %s until %s",
                    row["worker_id"],
                    row["expires_at"],
                )
                return False
            self.client.execute(
                """
                UPDATE run_locks
                SET worker_id = ?, acquired_at = ?, expires_at = ?, heartbeat_at = ?
                WHERE lock_key = 'radar_cron'
                """,
                [worker_id, now_iso, expires_at, now_iso],
            )
        else:
            self.client.execute(
                """
                INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at)
                VALUES ('radar_cron', ?, ?, ?, ?)
                """,
                [worker_id, now_iso, expires_at, now_iso],
            )
        return True

    def renew_run_lock(self, worker_id: str, extend_minutes: int = 15) -> bool:
        now = utc_now()
        now_iso = now.isoformat()
        expires_at = (now + timedelta(minutes=extend_minutes)).isoformat()
        res = self.client.execute(
            """
            UPDATE run_locks
            SET expires_at = ?, heartbeat_at = ?
            WHERE lock_key = 'radar_cron' AND worker_id = ? AND expires_at > ?
            """,
            [expires_at, now_iso, worker_id, now_iso],
        )
        return res.affected_row_count > 0

    def release_run_lock(self, worker_id: str) -> None:
        self.client.execute(
            "DELETE FROM run_locks WHERE lock_key = 'radar_cron' AND worker_id = ?",
            [worker_id],
        )

    # --- Article Deduplication & Tracking ---
    def is_article_processed(self, article_id: str) -> bool:
        res = self.client.execute(
            "SELECT 1 FROM processed_articles WHERE article_id = ?",
            [article_id],
        )
        return res.fetchone() is not None

    def mark_articles_processed(self, articles: list[Article]) -> None:
        if not articles:
            return
        now_iso = utc_now().isoformat()
        stmts: list[tuple[str, list[Any]]] = [
            (
                """
                INSERT OR IGNORE INTO processed_articles
                (article_id, canonical_url, title, source, published_at, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    a.id,
                    a.canonical_url or a.url,
                    a.title,
                    a.source,
                    a.published_at.isoformat(),
                    now_iso,
                ],
            )
            for a in articles
        ]
        self.client.execute_transaction(stmts)

    # --- Candidate Backlog & Lifecycle Integrity ---
    def save_pending_candidates(self, candidates: list[Article]) -> None:
        """Persist fresh candidates into durable backlog in Turso."""
        if not candidates:
            return
        now_iso = utc_now().isoformat()
        stmts: list[tuple[str, list[Any]]] = [
            (
                """
                INSERT OR IGNORE INTO candidate_backlog
                (article_id, url, canonical_url, title, source, market, category_hint,
                 snippet, image_url, published_at, discovered_at, status, priority_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    a.id,
                    a.url,
                    a.canonical_url,
                    a.title,
                    a.source,
                    a.market.value,
                    a.source_category or "",
                    a.snippet,
                    a.image_url,
                    a.published_at.isoformat(),
                    now_iso,
                    "QUEUED",
                    float(a.raw_metadata.get("priority_score", 0.0)),
                ],
            )
            for a in candidates
        ]
        self.client.execute_transaction(stmts)

    def get_pending_candidates(self, max_age_hours: int = 16, now: Optional[datetime] = None) -> list[Article]:
        """Retrieve unexpired candidates from backlog that are still awaiting evaluation."""
        ref_now = now or utc_now()
        cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
        res = self.client.execute(
            """
            SELECT cb.article_id, cb.url, cb.canonical_url, cb.title, cb.source, cb.market,
                   cb.category_hint, cb.snippet, cb.image_url, cb.published_at, cb.priority_score
            FROM candidate_backlog cb
            LEFT JOIN processed_articles pa ON cb.article_id = pa.article_id
            WHERE cb.status = 'QUEUED'
              AND cb.published_at >= ?
              AND pa.article_id IS NULL
            ORDER BY cb.priority_score DESC, cb.published_at DESC
            """,
            [cutoff],
        )
        return [
            Article(
                id=r["article_id"],
                title=r["title"],
                url=r["url"],
                canonical_url=r["canonical_url"],
                source=r["source"],
                market=Market(r["market"]),
                published_at=datetime.fromisoformat(r["published_at"]),
                source_category=r["category_hint"] or None,
                image_url=r["image_url"],
                snippet=r["snippet"] or "",
                raw_metadata={"priority_score": float(r["priority_score"]), "from_backlog": True},
            )
            for r in res.fetchall()
        ]

    def mark_candidates_evaluated(self, article_ids: list[str]) -> None:
        """Transition candidates in backlog to EVALUATED status."""
        if not article_ids:
            return
        stmts: list[tuple[str, list[Any]]] = [
            ("UPDATE candidate_backlog SET status = 'EVALUATED' WHERE article_id = ?", [aid])
            for aid in article_ids
        ]
        self.client.execute_transaction(stmts)

    # --- Story Intelligence & Lineage ---
    def get_recent_stories(self, limit: int = 80, days: int = 30) -> list[Story]:
        cutoff = (utc_now() - timedelta(days=days)).isoformat()
        res = self.client.execute(
            """
            SELECT story_id, headline_tr, category, first_seen_at, last_updated_at,
                   article_ids_json, update_count, is_active
            FROM stories
            WHERE last_updated_at >= ?
            ORDER BY last_updated_at DESC
            LIMIT ?
            """,
            [cutoff, limit],
        )
        return [
            Story(
                story_id=r["story_id"],
                headline_tr=r["headline_tr"],
                category=StoryCategory(r["category"]),
                first_seen_at=datetime.fromisoformat(r["first_seen_at"]),
                last_updated_at=datetime.fromisoformat(r["last_updated_at"]),
                article_ids=json.loads(r["article_ids_json"]),
                update_count=int(r["update_count"]),
                is_active=bool(r["is_active"]),
            )
            for r in res.fetchall()
        ]

    def get_story_by_id(self, story_id: str) -> Optional[Story]:
        res = self.client.execute(
            """
            SELECT story_id, headline_tr, category, first_seen_at, last_updated_at,
                   article_ids_json, update_count, is_active
            FROM stories
            WHERE story_id = ?
            """,
            [story_id],
        )
        r = res.fetchone()
        if not r:
            return None
        return Story(
            story_id=r["story_id"],
            headline_tr=r["headline_tr"],
            category=StoryCategory(r["category"]),
            first_seen_at=datetime.fromisoformat(r["first_seen_at"]),
            last_updated_at=datetime.fromisoformat(r["last_updated_at"]),
            article_ids=json.loads(r["article_ids_json"]),
            update_count=int(r["update_count"]),
            is_active=bool(r["is_active"]),
        )

    def record_new_story(self, story: Story, assessment: EditorialAssessment) -> None:
        now_iso = utc_now().isoformat()
        import uuid
        asm_id = f"ASM-{now_iso[:10]}-{uuid.uuid4().hex[:8]}"

        stmts: list[tuple[str, list[Any]]] = [
            (
                """
                INSERT OR REPLACE INTO stories
                (story_id, headline_tr, category, first_seen_at, last_updated_at,
                 article_ids_json, update_count, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    story.story_id,
                    story.headline_tr,
                    story.category.value,
                    story.first_seen_at.isoformat(),
                    story.last_updated_at.isoformat(),
                    json.dumps(story.article_ids),
                    story.update_count,
                    1 if story.is_active else 0,
                ],
            ),
            (
                """
                INSERT OR REPLACE INTO editorial_assessments
                (assessment_id, story_id, kind, score, headline_tr, cover_title_tr,
                 caption_idea_tr, why_efso_tr, suggested_format, category, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    asm_id,
                    story.story_id,
                    assessment.kind.value,
                    assessment.score,
                    assessment.headline_tr,
                    assessment.cover_title_tr,
                    assessment.caption_idea_tr,
                    assessment.why_efso_tr,
                    assessment.suggested_format.value,
                    assessment.category.value,
                    now_iso,
                ],
            ),
        ]
        self.client.execute_transaction(stmts)

    def record_story_update(self, update: StoryUpdate, assessment: EditorialAssessment) -> None:
        now_iso = utc_now().isoformat()
        import uuid
        asm_id = f"ASM-{now_iso[:10]}-{uuid.uuid4().hex[:8]}"

        stmts: list[tuple[str, list[Any]]] = [
            (
                """
                INSERT OR REPLACE INTO story_updates
                (update_id, story_id, development_tr, detected_at, article_ids_json, significance_score)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    update.update_id,
                    update.story_id,
                    update.development_tr,
                    update.detected_at.isoformat(),
                    json.dumps(update.article_ids),
                    update.significance_score,
                ],
            ),
            (
                """
                UPDATE stories
                SET last_updated_at = ?, update_count = update_count + 1
                WHERE story_id = ?
                """,
                [update.detected_at.isoformat(), update.story_id],
            ),
            (
                """
                INSERT OR REPLACE INTO editorial_assessments
                (assessment_id, story_id, kind, score, headline_tr, cover_title_tr,
                 caption_idea_tr, why_efso_tr, suggested_format, category, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    asm_id,
                    update.story_id,
                    assessment.kind.value,
                    assessment.score,
                    assessment.headline_tr,
                    assessment.cover_title_tr,
                    assessment.caption_idea_tr,
                    assessment.why_efso_tr,
                    assessment.suggested_format.value,
                    assessment.category.value,
                    now_iso,
                ],
            ),
        ]
        self.client.execute_transaction(stmts)

    def record_editorial_assessment(self, assessment: EditorialAssessment, story_id: str) -> None:
        import uuid
        now_iso = utc_now().isoformat()
        asm_id = f"ASM-{now_iso[:10]}-{uuid.uuid4().hex[:8]}"
        self.client.execute(
            """
            INSERT OR REPLACE INTO editorial_assessments
            (assessment_id, story_id, kind, score, headline_tr, cover_title_tr,
             caption_idea_tr, why_efso_tr, suggested_format, category, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                asm_id,
                story_id,
                assessment.kind.value,
                assessment.score,
                assessment.headline_tr,
                assessment.cover_title_tr,
                assessment.caption_idea_tr,
                assessment.why_efso_tr,
                assessment.suggested_format.value,
                assessment.category.value,
                now_iso,
            ],
        )

    def has_story_been_alerted(
        self,
        story_id: str,
        kind: EditorialKind = EditorialKind.NEW,
        development_tr: Optional[str] = None,
    ) -> bool:
        if kind == EditorialKind.NEW:
            res = self.client.execute(
                """
                SELECT 1 FROM notifications
                WHERE story_id = ? AND kind = 'NEW'
                  AND status IN ('SENT', 'HELD_QUIET_HOURS', 'DISPATCHING', 'QUEUED_FOR_DIGEST', 'HELD_FOR_VERIFICATION')
                LIMIT 1
                """,
                [story_id],
            )
            return res.fetchone() is not None
        else:
            if development_tr:
                clean_dev = normalize_text(development_tr)
                res = self.client.execute(
                    "SELECT development_tr FROM story_updates WHERE story_id = ?",
                    [story_id],
                )
                for r in res.fetchall():
                    if normalize_text(r.get("development_tr", "")) == clean_dev:
                        return True
            return False

    # --- Notification Reliability & Lease Management ---
    def claim_notification_for_dispatch(
        self,
        notification_id: str,
        story_id: str,
        kind: EditorialKind,
        headline_tr: str,
        score: int,
        message_text: str,
        worker_id: str,
        lease_seconds: int = 300,
    ) -> bool:
        now = utc_now()
        now_iso = now.isoformat()
        lease_exp = (now + timedelta(seconds=lease_seconds)).isoformat()

        res = self.client.execute(
            """
            SELECT status, lease_worker_id, lease_expires_at
            FROM notifications
            WHERE notification_id = ?
            """,
            [notification_id],
        )
        row = res.fetchone()

        if row:
            if row["status"] == DeliveryStatus.SENT.value:
                return False
            if (
                row["status"] == DeliveryStatus.DISPATCHING.value
                and row.get("lease_expires_at")
                and row["lease_expires_at"] > now_iso
                and row.get("lease_worker_id") != worker_id
            ):
                return False

            self.client.execute(
                """
                UPDATE notifications
                SET status = ?, lease_worker_id = ?, lease_expires_at = ?
                WHERE notification_id = ?
                """,
                [DeliveryStatus.DISPATCHING.value, worker_id, lease_exp, notification_id],
            )
        else:
            self.client.execute(
                """
                INSERT INTO notifications
                (notification_id, story_id, kind, channel, sent_at, status,
                 message_text, headline_tr, score, lease_worker_id, lease_expires_at)
                VALUES (?, ?, ?, 'TELEGRAM', ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    notification_id,
                    story_id,
                    kind.value,
                    now_iso,
                    DeliveryStatus.DISPATCHING.value,
                    message_text,
                    headline_tr,
                    score,
                    worker_id,
                    lease_exp,
                ],
            )
        return True

    def record_notification(self, record: NotificationRecord) -> None:
        self.client.execute(
            """
            INSERT OR REPLACE INTO notifications
            (notification_id, story_id, update_id, kind, channel, sent_at, status,
             message_text, headline_tr, score, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                record.notification_id,
                record.story_id,
                record.update_id,
                record.kind.value,
                record.channel,
                record.sent_at.isoformat(),
                record.status.value,
                record.message_text,
                record.headline_tr,
                record.score,
                record.error_message,
            ],
        )

    def mark_notification_delivered(self, notification_id: str, delivered_at: datetime) -> None:
        self.client.execute(
            """
            UPDATE notifications
            SET status = ?, delivered_at = ?, lease_worker_id = NULL,
            lease_expires_at = NULL, error_message = NULL
            WHERE notification_id = ?
            """,
            [DeliveryStatus.SENT.value, delivered_at.isoformat(), notification_id],
        )

    def record_notification_failure(self, notification_id: str, error_message: str) -> None:
        self.client.execute(
            """
            UPDATE notifications
            SET status = ?, lease_worker_id = NULL, lease_expires_at = NULL, error_message = ?
            WHERE notification_id = ?
            """,
            [DeliveryStatus.FAILED.value, error_message, notification_id],
        )

    def get_held_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        if max_age_hours is not None:
            ref_now = now or utc_now()
            cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
            res = self.client.execute(
                """
                SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                       status, message_text, headline_tr, score, error_message
                FROM notifications
                WHERE status = 'HELD_QUIET_HOURS' AND sent_at >= ?
                ORDER BY sent_at ASC
                """,
                [cutoff],
            )
        else:
            res = self.client.execute(
                """
                SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                       status, message_text, headline_tr, score, error_message
                FROM notifications
                WHERE status = 'HELD_QUIET_HOURS'
                ORDER BY sent_at ASC
                """
            )
        return [
            NotificationRecord(
                notification_id=r["notification_id"],
                story_id=r["story_id"],
                update_id=r.get("update_id"),
                kind=EditorialKind(r["kind"]),
                channel=r["channel"],
                sent_at=datetime.fromisoformat(r["sent_at"]),
                status=DeliveryStatus(r["status"]),
                message_text=r["message_text"],
                headline_tr=r["headline_tr"],
                score=int(r["score"]),
                error_message=r.get("error_message"),
            )
            for r in res.fetchall()
        ]

    def expire_stale_held_notifications(
        self, max_age_hours: int, now: Optional[datetime] = None
    ) -> int:
        ref_now = now or utc_now()
        cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
        res = self.client.execute(
            """
            UPDATE notifications
            SET status = ?, error_message = 'Expired due to exceeding quiet hours hold freshness limit'
            WHERE status = 'HELD_QUIET_HOURS' AND sent_at < ?
            """,
            [DeliveryStatus.EXPIRED_STALE.value, cutoff],
        )
        return res.affected_row_count

    def recover_expired_dispatch_claims(self) -> int:
        now_iso = utc_now().isoformat()
        res = self.client.execute(
            """
            UPDATE notifications
            SET status = ?, error_message = 'Dispatch lease expired without confirmation',
                lease_worker_id = NULL, lease_expires_at = NULL
            WHERE status = ? AND lease_expires_at < ?
            """,
            [DeliveryStatus.FAILED.value, DeliveryStatus.DISPATCHING.value, now_iso],
        )
        return res.affected_row_count

    # --- Maintenance & Migrations ---
    def purge_expired_records(self, retention_days: int = 35) -> int:
        cutoff = (utc_now() - timedelta(days=retention_days)).isoformat()
        res = self.client.execute(
            "DELETE FROM processed_articles WHERE processed_at < ?",
            [cutoff],
        )
        return res.affected_row_count

    # --- Editorial Feedback & Calibration ---
    def record_owner_feedback(self, feedback: OwnerFeedback) -> None:
        """Store explicit owner editorial feedback in Turso."""
        self.client.execute(
            """
            INSERT OR REPLACE INTO owner_feedback
            (feedback_id, story_id, article_id, rating, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                feedback.feedback_id,
                feedback.story_id,
                feedback.article_id,
                feedback.rating.value,
                feedback.notes,
                feedback.created_at.isoformat(),
            ],
        )

    def import_owner_feedback_batch(self, feedbacks: list[OwnerFeedback]) -> int:
        """Atomically and idempotently import a batch of owner feedback records into Turso."""
        if not feedbacks:
            return 0
        sql = """
        INSERT OR REPLACE INTO owner_feedback
        (feedback_id, story_id, article_id, rating, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        statements = [
            (
                sql,
                [
                    fb.feedback_id,
                    fb.story_id,
                    fb.article_id,
                    fb.rating.value,
                    fb.notes,
                    fb.created_at.isoformat(),
                ],
            )
            for fb in feedbacks
        ]
        self.client.execute_transaction(statements)
        return len(feedbacks)

    def get_owner_feedback(self, limit: int = 100) -> list[OwnerFeedback]:
        """Retrieve recorded editorial feedback from Turso ordered by newest first."""
        res = self.client.execute(
            """
            SELECT feedback_id, story_id, article_id, rating, notes, created_at
            FROM owner_feedback
            ORDER BY created_at DESC, feedback_id ASC
            LIMIT ?
            """,
            [limit],
        )
        results: list[OwnerFeedback] = []
        for r in res.fetchall():
            results.append(
                OwnerFeedback(
                    feedback_id=r["feedback_id"],
                    story_id=r.get("story_id"),
                    article_id=r.get("article_id"),
                    rating=OwnerFeedbackRating(r["rating"]),
                    notes=r.get("notes") or "",
                    created_at=datetime.fromisoformat(r["created_at"]),
                )
            )
        return results

    # --- Digest Queue & Verification Hold Persistence ---
    def get_queued_digest_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        if max_age_hours is not None:
            ref_now = now or utc_now()
            cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
            res = self.client.execute(
                """
                SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                       status, message_text, headline_tr, score, error_message
                FROM notifications
                WHERE status = 'QUEUED_FOR_DIGEST' AND sent_at >= ?
                ORDER BY score DESC, sent_at ASC, notification_id ASC
                """,
                [cutoff],
            )
        else:
            res = self.client.execute(
                """
                SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                       status, message_text, headline_tr, score, error_message
                FROM notifications
                WHERE status = 'QUEUED_FOR_DIGEST'
                ORDER BY score DESC, sent_at ASC, notification_id ASC
                """
            )
        return [
            NotificationRecord(
                notification_id=r["notification_id"],
                story_id=r["story_id"],
                update_id=r.get("update_id"),
                kind=EditorialKind(r["kind"]),
                channel=r["channel"],
                sent_at=datetime.fromisoformat(r["sent_at"]),
                status=DeliveryStatus(r["status"]),
                message_text=r["message_text"],
                headline_tr=r["headline_tr"],
                score=int(r["score"]),
                error_message=r.get("error_message"),
            )
            for r in res.fetchall()
        ]

    def mark_digest_notifications_dispatched(
        self, notification_ids: list[str], delivered_at: datetime
    ) -> int:
        if not notification_ids:
            return 0
        delivered_iso = delivered_at.isoformat()
        placeholders = ",".join("?" for _ in notification_ids)
        res = self.client.execute(
            f"""
            UPDATE notifications
            SET status = ?, delivered_at = ?, channel = 'TELEGRAM_DIGEST',
                lease_worker_id = NULL, lease_expires_at = NULL, error_message = NULL
            WHERE notification_id IN ({placeholders})
            """,
            [DeliveryStatus.SENT.value, delivered_iso, *notification_ids],
        )
        return res.affected_row_count

    def get_verification_held_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        if max_age_hours is not None:
            ref_now = now or utc_now()
            cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
            res = self.client.execute(
                """
                SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                       status, message_text, headline_tr, score, error_message
                FROM notifications
                WHERE status = 'HELD_FOR_VERIFICATION' AND sent_at >= ?
                ORDER BY sent_at ASC, notification_id ASC
                """,
                [cutoff],
            )
        else:
            res = self.client.execute(
                """
                SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                       status, message_text, headline_tr, score, error_message
                FROM notifications
                WHERE status = 'HELD_FOR_VERIFICATION'
                ORDER BY sent_at ASC, notification_id ASC
                """
            )
        return [
            NotificationRecord(
                notification_id=r["notification_id"],
                story_id=r["story_id"],
                update_id=r.get("update_id"),
                kind=EditorialKind(r["kind"]),
                channel=r["channel"],
                sent_at=datetime.fromisoformat(r["sent_at"]),
                status=DeliveryStatus(r["status"]),
                message_text=r["message_text"],
                headline_tr=r["headline_tr"],
                score=int(r["score"]),
                error_message=r.get("error_message"),
            )
            for r in res.fetchall()
        ]

    def get_daily_sent_message_count(self, now: Optional[datetime] = None) -> int:
        ref_now = (now or utc_now()).astimezone(timezone.utc)
        day_prefix = ref_now.strftime("%Y-%m-%d")
        res = self.client.execute(
            """
            SELECT channel, delivered_at, sent_at, notification_id
            FROM notifications
            WHERE status = 'SENT'
              AND (COALESCE(delivered_at, sent_at) LIKE ?)
            """,
            [f"{day_prefix}%"],
        )
        individual_count = 0
        digest_batches: set[str] = set()
        for r in res.fetchall():
            if r.get("channel") == "TELEGRAM_DIGEST":
                batch_key = r.get("delivered_at") or r.get("sent_at") or ""
                digest_batches.add(str(batch_key))
            else:
                individual_count += 1
        return individual_count + len(digest_batches)

    def get_last_digest_sent_at(self) -> Optional[datetime]:
        res = self.client.execute(
            """
            SELECT COALESCE(delivered_at, sent_at) AS ts
            FROM notifications
            WHERE status = 'SENT' AND channel = 'TELEGRAM_DIGEST'
            ORDER BY COALESCE(delivered_at, sent_at) DESC
            LIMIT 1
            """
        )
        row = res.fetchone()
        if row and row.get("ts"):
            return datetime.fromisoformat(str(row["ts"]))
        return None

    # --- Persistent AI Token & Cost Accounting ---
    def record_ai_usage(
        self,
        usage_id: str,
        run_mode: str,
        model_name: str,
        batch_hash: str,
        candidate_count: int,
        input_tokens: int,
        output_tokens: int,
        estimated_cost_usd: float,
        response_json: Optional[str] = None,
        created_at: Optional[datetime] = None,
    ) -> None:
        ts_iso = (created_at or utc_now()).isoformat()
        self.client.execute(
            """
            INSERT OR REPLACE INTO ai_usage_ledger
            (usage_id, run_mode, model_name, batch_hash, candidate_count,
             input_tokens, output_tokens, estimated_cost_usd, response_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                usage_id,
                run_mode,
                model_name,
                batch_hash,
                candidate_count,
                input_tokens,
                output_tokens,
                estimated_cost_usd,
                response_json,
                ts_iso,
            ],
        )

    def get_monthly_ai_usage(
        self, year_month_prefix: Optional[str] = None, now: Optional[datetime] = None
    ) -> dict[str, float | int]:
        ref_now = (now or utc_now()).astimezone(timezone.utc)
        ym = year_month_prefix or ref_now.strftime("%Y-%m")
        res = self.client.execute(
            """
            SELECT COUNT(*) AS call_count,
                   COALESCE(SUM(input_tokens), 0) AS input_tokens,
                   COALESCE(SUM(output_tokens), 0) AS output_tokens,
                   COALESCE(SUM(estimated_cost_usd), 0.0) AS total_cost_usd
            FROM ai_usage_ledger
            WHERE created_at LIKE ?
            """,
            [f"{ym}%"],
        )
        row = res.fetchone() or {}
        return {
            "call_count": int(row.get("call_count") or 0),
            "input_tokens": int(row.get("input_tokens") or 0),
            "output_tokens": int(row.get("output_tokens") or 0),
            "total_cost_usd": round(float(row.get("total_cost_usd") or 0.0), 6),
        }

    def get_cached_ai_batch_response(self, batch_hash: str, model_name: str) -> Optional[str]:
        res = self.client.execute(
            """
            SELECT response_json
            FROM ai_usage_ledger
            WHERE batch_hash = ? AND model_name = ? AND response_json IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [batch_hash, model_name],
        )
        row = res.fetchone()
        return str(row["response_json"]) if row and row.get("response_json") else None


