"""SQLite implementation of the StateRepository interface.

Supports local Windows development, zero external dependencies, WAL mode,
robust concurrency locking, lease-based dispatching, story intelligence, and schema migrations.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Generator, Optional

from efso_radar.domain import (
    AdminOperationalReport,
    Article,
    BudgetHistoryConflictError,
    DeliveryIdentityConflictError,
    DeliveryStateError,
    DeliveryStatus,
    DeliveryStatusRegressionError,
    DurableRecoveryRecord,
    EditorialAssessment,
    EditorialKind,
    ExperimentBudgetRecord,
    Market,
    NotificationRecord,
    OwnerFeedback,
    OwnerFeedbackRating,
    ScheduledExecutionRecord,
    Story,
    StoryCategory,
    StoryUpdate,
    SuggestedFormat,
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    normalize_text,
    utc_now,
    validate_delivery_status_transition,
)
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.schema import (
    CURRENT_SCHEMA_VERSION,
    get_all_init_statements,
    get_column_migrations,
)

logger = logging.getLogger("efso-radar.repository.sqlite")


class SqliteStateRepository(StateRepository):
    """SQLite-based persistent state storage."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Create tables, indexes, and run migrations."""
        with self._connection() as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")

            for ddl in get_all_init_statements():
                conn.execute(ddl)

            # Check and safely add missing columns to pre-existing tables
            cursor = conn.execute("PRAGMA table_info(notifications)")
            existing_cols = {row["name"] for row in cursor.fetchall()}
            for table, col, col_type in get_column_migrations():
                if table == "notifications" and col not in existing_cols:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type};")
                    existing_cols.add(col)

            # Record schema version in migrations table
            now_iso = utc_now().isoformat()
            conn.execute(
                """
                INSERT OR IGNORE INTO schema_migrations (version, applied_at, description)
                VALUES (?, ?, ?)
                """,
                (CURRENT_SCHEMA_VERSION, now_iso, "Current schema initialization"),
            )

    def get_schema_version(self) -> int:
        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT MAX(version) AS ver FROM schema_migrations"
            )
            row = cursor.fetchone()
            return int(row["ver"]) if row and row["ver"] is not None else 0

    # --- Run Locking & Concurrency ---
    def acquire_run_lock(self, worker_id: str, timeout_minutes: int = 15) -> bool:
        now = utc_now()
        now_iso = now.isoformat()
        expires_at = (now + timedelta(minutes=timeout_minutes)).isoformat()

        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at)
                VALUES ('radar_cron', ?, ?, ?, ?)
                ON CONFLICT(lock_key) DO UPDATE SET
                    worker_id = excluded.worker_id,
                    acquired_at = excluded.acquired_at,
                    expires_at = excluded.expires_at,
                    heartbeat_at = excluded.heartbeat_at
                WHERE run_locks.expires_at < ? OR run_locks.worker_id = ?
                """,
                (worker_id, now_iso, expires_at, now_iso, now_iso, worker_id),
            )
            return cursor.rowcount > 0

    def renew_run_lock(self, worker_id: str, extend_minutes: int = 15) -> bool:
        now = utc_now()
        now_iso = now.isoformat()
        expires_at = (now + timedelta(minutes=extend_minutes)).isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                UPDATE run_locks
                SET expires_at = ?, heartbeat_at = ?
                WHERE lock_key = 'radar_cron' AND worker_id = ? AND expires_at >= ?
                """,
                (expires_at, now_iso, worker_id, now_iso),
            )
            return cursor.rowcount > 0

    def release_run_lock(self, worker_id: str) -> None:
        with self._connection() as conn:
            conn.execute(
                "DELETE FROM run_locks WHERE lock_key = 'radar_cron' AND worker_id = ?",
                (worker_id,),
            )

    # --- Article Deduplication & Tracking ---
    def is_article_processed(self, article_id: str) -> bool:
        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM processed_articles WHERE article_id = ?",
                (article_id,),
            )
            return cursor.fetchone() is not None

    def mark_articles_processed(self, articles: list[Article]) -> None:
        if not articles:
            return
        now_iso = utc_now().isoformat()
        records = [
            (
                a.id,
                a.canonical_url or a.url,
                a.title,
                a.source,
                a.published_at.isoformat(),
                now_iso,
            )
            for a in articles
        ]
        with self._connection() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO processed_articles
                (article_id, canonical_url, title, source, published_at, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                records,
            )

    # --- Candidate Backlog & Lifecycle Integrity ---
    def save_pending_candidates(self, candidates: list[Article]) -> None:
        """Persist fresh candidates into durable backlog so deprioritized items survive feed rotation."""
        if not candidates:
            return
        now_iso = utc_now().isoformat()
        records = [
            (
                a.id,
                a.url,
                a.canonical_url,
                a.title,
                a.source,
                a.market.value,
                a.source_category or a.raw_metadata.get("kind") or "",
                a.snippet,
                a.image_url,
                a.published_at.isoformat(),
                now_iso,
                "QUEUED",
                float(a.raw_metadata.get("priority_score", 0.0)),
            )
            for a in candidates
        ]
        with self._connection() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO candidate_backlog
                (article_id, url, canonical_url, title, source, market, category_hint,
                 snippet, image_url, published_at, discovered_at, status, priority_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )

    def get_pending_candidates(self, max_age_hours: int = 24, now: Optional[datetime] = None) -> list[Article]:
        """Retrieve unexpired candidates from backlog that are still awaiting evaluation.
        
        Preserves candidates within max_age_hours, plus any genuine UPDATE candidates regardless of age.
        """
        ref_now = now or utc_now()
        cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT cb.article_id, cb.url, cb.canonical_url, cb.title, cb.source, cb.market,
                       cb.category_hint, cb.snippet, cb.image_url, cb.published_at, cb.priority_score
                FROM candidate_backlog cb
                LEFT JOIN processed_articles pa ON cb.article_id = pa.article_id
                WHERE cb.status = 'QUEUED'
                  AND (cb.published_at >= ? OR cb.category_hint = 'UPDATE')
                  AND pa.article_id IS NULL
                ORDER BY cb.priority_score DESC, cb.published_at DESC
                """,
                (cutoff,),
            )
            rows = cursor.fetchall()
            return [
                Article(
                    id=row["article_id"],
                    title=row["title"],
                    url=row["url"],
                    canonical_url=row["canonical_url"],
                    source=row["source"],
                    market=Market(row["market"]),
                    published_at=datetime.fromisoformat(row["published_at"]),
                    source_category=row["category_hint"] or None,
                    image_url=row["image_url"],
                    snippet=row["snippet"] or "",
                    raw_metadata={"priority_score": row["priority_score"], "from_backlog": True},
                )
                for row in rows
            ]

    def mark_candidates_evaluated(self, article_ids: list[str]) -> None:
        """Transition candidates in backlog to EVALUATED status."""
        if not article_ids:
            return
        with self._connection() as conn:
            conn.executemany(
                "UPDATE candidate_backlog SET status = 'EVALUATED' WHERE article_id = ?",
                [(aid,) for aid in article_ids],
            )

    def expire_stale_backlog_candidates(self, max_age_hours: int = 24, now: Optional[datetime] = None) -> int:
        """Transition candidates older than max_age_hours to EXPIRED status, preserving legitimate UPDATEs."""
        ref_now = now or utc_now()
        cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                UPDATE candidate_backlog
                SET status = 'EXPIRED'
                WHERE status = 'QUEUED'
                  AND published_at < ?
                  AND (category_hint IS NULL OR category_hint != 'UPDATE')
                """,
                (cutoff,),
            )
            return cursor.rowcount

    # --- Story Intelligence & Lineage ---
    def get_recent_stories(self, limit: int = 80, days: int = 30) -> list[Story]:
        cutoff = (utc_now() - timedelta(days=days)).isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT story_id, headline_tr, category, first_seen_at, last_updated_at,
                       article_ids_json, update_count, is_active
                FROM stories
                WHERE last_updated_at >= ?
                ORDER BY last_updated_at DESC
                LIMIT ?
                """,
                (cutoff, limit),
            )
            rows = cursor.fetchall()
            return [
                Story(
                    story_id=r["story_id"],
                    headline_tr=r["headline_tr"],
                    category=StoryCategory(r["category"]),
                    first_seen_at=datetime.fromisoformat(r["first_seen_at"]),
                    last_updated_at=datetime.fromisoformat(r["last_updated_at"]),
                    article_ids=json.loads(r["article_ids_json"]),
                    update_count=r["update_count"],
                    is_active=bool(r["is_active"]),
                )
                for r in rows
            ]

    def get_story_by_id(self, story_id: str) -> Optional[Story]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT story_id, headline_tr, category, first_seen_at, last_updated_at,
                       article_ids_json, update_count, is_active
                FROM stories
                WHERE story_id = ?
                """,
                (story_id,),
            )
            r = cursor.fetchone()
            if not r:
                return None
            return Story(
                story_id=r["story_id"],
                headline_tr=r["headline_tr"],
                category=StoryCategory(r["category"]),
                first_seen_at=datetime.fromisoformat(r["first_seen_at"]),
                last_updated_at=datetime.fromisoformat(r["last_updated_at"]),
                article_ids=json.loads(r["article_ids_json"]),
                update_count=r["update_count"],
                is_active=bool(r["is_active"]),
            )

    def record_new_story(self, story: Story, assessment: EditorialAssessment) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO stories
                (story_id, headline_tr, category, first_seen_at, last_updated_at,
                 article_ids_json, update_count, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    story.story_id,
                    story.headline_tr,
                    story.category.value,
                    story.first_seen_at.isoformat(),
                    story.last_updated_at.isoformat(),
                    json.dumps(story.article_ids),
                    story.update_count,
                    1 if story.is_active else 0,
                ),
            )
            self._record_editorial_assessment_in_conn(conn, assessment, story.story_id)

    def record_story_update(self, update: StoryUpdate, assessment: EditorialAssessment) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO story_updates
                (update_id, story_id, development_tr, detected_at, article_ids_json, significance_score)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    update.update_id,
                    update.story_id,
                    update.development_tr,
                    update.detected_at.isoformat(),
                    json.dumps(update.article_ids),
                    update.significance_score,
                ),
            )
            conn.execute(
                """
                UPDATE stories
                SET last_updated_at = ?, update_count = update_count + 1
                WHERE story_id = ?
                """,
                (update.detected_at.isoformat(), update.story_id),
            )
            self._record_editorial_assessment_in_conn(conn, assessment, update.story_id)

    def _record_editorial_assessment_in_conn(
        self, conn: sqlite3.Connection, assessment: EditorialAssessment, story_id: str
    ) -> None:
        import uuid
        now_iso = utc_now().isoformat()
        conn.execute(
            """
            INSERT OR REPLACE INTO editorial_assessments
            (assessment_id, story_id, kind, score, headline_tr, cover_title_tr,
             caption_idea_tr, why_efso_tr, suggested_format, category, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"ASM-{now_iso[:10]}-{uuid.uuid4().hex[:8]}",
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
            ),
        )

    def record_editorial_assessment(self, assessment: EditorialAssessment, story_id: str) -> None:
        with self._connection() as conn:
            self._record_editorial_assessment_in_conn(conn, assessment, story_id)

    def has_story_been_alerted(
        self,
        story_id: str,
        kind: EditorialKind = EditorialKind.NEW,
        development_tr: Optional[str] = None,
    ) -> bool:
        with self._connection() as conn:
            if kind == EditorialKind.NEW:
                cursor = conn.execute(
                    """
                    SELECT 1 FROM notifications
                    WHERE story_id = ? AND kind = 'NEW'
                      AND status IN ('SENT', 'HELD_QUIET_HOURS', 'DISPATCHING', 'QUEUED_FOR_DIGEST', 'HELD_FOR_VERIFICATION', 'HELD_FOR_SENSITIVE_REVIEW', 'DELIVERY_UNCERTAIN')
                    LIMIT 1
                    """,
                    (story_id,),
                )
                return cursor.fetchone() is not None
            else:
                # For updates: check if this development was already recorded for this story
                if development_tr:
                    clean_dev = normalize_text(development_tr)
                    cursor = conn.execute(
                        """
                        SELECT development_tr FROM story_updates
                        WHERE story_id = ?
                        """,
                        (story_id,),
                    )
                    rows = cursor.fetchall()
                    for r in rows:
                        if normalize_text(r["development_tr"]) == clean_dev:
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

        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT status, lease_worker_id, lease_expires_at
                FROM notifications
                WHERE notification_id = ?
                """,
                (notification_id,),
            )
            row = cursor.fetchone()

            if row:
                if row["status"] == DeliveryStatus.SENT.value:
                    return False
                # If currently claimed by another active worker, cannot claim
                if (
                    row["status"] == DeliveryStatus.DISPATCHING.value
                    and row["lease_expires_at"]
                    and row["lease_expires_at"] > now_iso
                    and row["lease_worker_id"] != worker_id
                ):
                    return False

                conn.execute(
                    """
                    UPDATE notifications
                    SET status = ?, lease_worker_id = ?, lease_expires_at = ?
                    WHERE notification_id = ?
                    """,
                    (DeliveryStatus.DISPATCHING.value, worker_id, lease_exp, notification_id),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO notifications
                    (notification_id, story_id, kind, channel, sent_at, status,
                     message_text, headline_tr, score, lease_worker_id, lease_expires_at)
                    VALUES (?, ?, ?, 'TELEGRAM', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
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
                    ),
                )
            return True

    def record_notification(self, record: NotificationRecord) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO notifications
                (notification_id, story_id, update_id, kind, channel, sent_at, status,
                 message_text, headline_tr, score, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
                ),
            )

    def mark_notification_delivered(self, notification_id: str, delivered_at: datetime) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE notifications
                SET status = ?, delivered_at = ?, lease_worker_id = NULL,
                    lease_expires_at = NULL, error_message = NULL
                WHERE notification_id = ?
                """,
                (DeliveryStatus.SENT.value, delivered_at.isoformat(), notification_id),
            )

    def record_notification_failure(
        self,
        notification_id: str,
        error_message: str,
        status: DeliveryStatus = DeliveryStatus.FAILED,
    ) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE notifications
                SET status = ?, lease_worker_id = NULL, lease_expires_at = NULL, error_message = ?
                WHERE notification_id = ?
                """,
                (status.value, error_message, notification_id),
            )

    def get_held_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        with self._connection() as conn:
            if max_age_hours is not None:
                ref_now = now or utc_now()
                cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'HELD_QUIET_HOURS' AND sent_at >= ?
                    ORDER BY sent_at ASC
                    """,
                    (cutoff,),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'HELD_QUIET_HOURS'
                    ORDER BY sent_at ASC
                    """
                )
            rows = cursor.fetchall()
            return [
                NotificationRecord(
                    notification_id=r["notification_id"],
                    story_id=r["story_id"],
                    update_id=r["update_id"] if "update_id" in r.keys() else None,
                    kind=EditorialKind(r["kind"]),
                    channel=r["channel"],
                    sent_at=datetime.fromisoformat(r["sent_at"]),
                    status=DeliveryStatus(r["status"]),
                    message_text=r["message_text"],
                    headline_tr=r["headline_tr"],
                    score=r["score"],
                    error_message=r["error_message"],
                )
                for r in rows
            ]

    def expire_stale_held_notifications(
        self, max_age_hours: int, now: Optional[datetime] = None
    ) -> int:
        ref_now = now or utc_now()
        cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                UPDATE notifications
                SET status = ?, error_message = 'Expired due to exceeding quiet hours hold freshness limit'
                WHERE status = 'HELD_QUIET_HOURS' AND sent_at < ?
                """,
                (DeliveryStatus.EXPIRED_STALE.value, cutoff),
            )
            return cursor.rowcount

    def recover_expired_dispatch_claims(self) -> int:
        now_iso = utc_now().isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                UPDATE notifications
                SET status = ?, error_message = 'Dispatch lease expired without confirmation',
                    lease_worker_id = NULL, lease_expires_at = NULL
                WHERE status = ? AND lease_expires_at < ?
                """,
                (DeliveryStatus.FAILED.value, DeliveryStatus.DISPATCHING.value, now_iso),
            )
            return cursor.rowcount

    # --- Maintenance & Migrations ---
    def purge_expired_records(self, retention_days: int = 35) -> int:
        cutoff = (utc_now() - timedelta(days=retention_days)).isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                "DELETE FROM processed_articles WHERE processed_at < ?",
                (cutoff,),
            )
            return cursor.rowcount

    def migrate_from_legacy_json(self, json_path: Path) -> int:
        """Migrate data from legacy data/state.json if available and database is empty."""
        if not json_path.exists():
            return 0

        with self._connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) AS cnt FROM processed_articles")
            if cursor.fetchone()["cnt"] > 0:
                return 0

        try:
            content = json.loads(json_path.read_text(encoding="utf-8"))
            processed = content.get("processed", {})
            sent = content.get("sent", [])
        except Exception as exc:
            logger.warning("Failed to parse legacy JSON for migration: %s", exc)
            return 0

        migrated_count = 0
        now_iso = utc_now().isoformat()
        with self._connection() as conn:
            articles_to_insert = [
                (
                    article_id,
                    "",
                    "Legacy migrated article",
                    "legacy_state_json",
                    processed_time or now_iso,
                    processed_time or now_iso,
                )
                for article_id, processed_time in processed.items()
            ]
            if articles_to_insert:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO processed_articles
                    (article_id, canonical_url, title, source, published_at, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    articles_to_insert,
                )
                migrated_count += len(articles_to_insert)

            for s in sent:
                story_id = s.get("story_id")
                if not story_id:
                    continue
                sent_at = s.get("sent_at", now_iso)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO stories
                    (story_id, headline_tr, category, first_seen_at, last_updated_at,
                     article_ids_json, update_count, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        story_id,
                        s.get("headline_tr", ""),
                        StoryCategory.GLOBAL.value,
                        sent_at,
                        sent_at,
                        json.dumps([]),
                        1 if s.get("kind") == "UPDATE" else 0,
                        1,
                    ),
                )

        logger.info("Migrated %d legacy records from %s to SQLite", migrated_count, json_path)
        return migrated_count

    # --- Editorial Feedback & Calibration ---
    def record_owner_feedback(self, feedback: OwnerFeedback) -> None:
        """Store explicit owner editorial feedback."""
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO owner_feedback
                (feedback_id, story_id, article_id, rating, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    feedback.feedback_id,
                    feedback.story_id,
                    feedback.article_id,
                    feedback.rating.value,
                    feedback.notes,
                    feedback.created_at.isoformat(),
                ),
            )

    def import_owner_feedback_batch(self, feedbacks: list[OwnerFeedback]) -> int:
        """Atomically and idempotently import a batch of owner feedback records into SQLite."""
        if not feedbacks:
            return 0
        with self._connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO owner_feedback
                (feedback_id, story_id, article_id, rating, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        fb.feedback_id,
                        fb.story_id,
                        fb.article_id,
                        fb.rating.value,
                        fb.notes,
                        fb.created_at.isoformat(),
                    )
                    for fb in feedbacks
                ],
            )
        return len(feedbacks)

    def get_owner_feedback(self, limit: int = 100) -> list[OwnerFeedback]:
        """Retrieve recorded editorial feedback ordered by newest first."""
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT feedback_id, story_id, article_id, rating, notes, created_at
                FROM owner_feedback
                ORDER BY created_at DESC, feedback_id ASC
                LIMIT ?
                """,
                (limit,),
            )
            results: list[OwnerFeedback] = []
            for row in cursor.fetchall():
                results.append(
                    OwnerFeedback(
                        feedback_id=row["feedback_id"],
                        story_id=row["story_id"],
                        article_id=row["article_id"],
                        rating=OwnerFeedbackRating(row["rating"]),
                        notes=row["notes"] or "",
                        created_at=datetime.fromisoformat(row["created_at"]),
                    )
                )
            return results

    # --- Digest Queue & Verification Hold Persistence ---
    def get_queued_digest_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        with self._connection() as conn:
            if max_age_hours is not None:
                ref_now = now or utc_now()
                cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'QUEUED_FOR_DIGEST' AND sent_at >= ?
                    ORDER BY score DESC, sent_at ASC, notification_id ASC
                    """,
                    (cutoff,),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'QUEUED_FOR_DIGEST'
                    ORDER BY score DESC, sent_at ASC, notification_id ASC
                    """
                )
            rows = cursor.fetchall()
            return [
                NotificationRecord(
                    notification_id=r["notification_id"],
                    story_id=r["story_id"],
                    update_id=r["update_id"] if "update_id" in r.keys() else None,
                    kind=EditorialKind(r["kind"]),
                    channel=r["channel"],
                    sent_at=datetime.fromisoformat(r["sent_at"]),
                    status=DeliveryStatus(r["status"]),
                    message_text=r["message_text"],
                    headline_tr=r["headline_tr"],
                    score=r["score"],
                    error_message=r["error_message"],
                )
                for r in rows
            ]

    def mark_digest_notifications_dispatched(
        self, notification_ids: list[str], delivered_at: datetime
    ) -> int:
        if not notification_ids:
            return 0
        delivered_iso = delivered_at.isoformat()
        placeholders = ",".join("?" for _ in notification_ids)
        with self._connection() as conn:
            cursor = conn.execute(
                f"""
                UPDATE notifications
                SET status = ?, delivered_at = ?, channel = 'TELEGRAM_DIGEST',
                    lease_worker_id = NULL, lease_expires_at = NULL, error_message = NULL
                WHERE notification_id IN ({placeholders})
                """,
                [DeliveryStatus.SENT.value, delivered_iso, *notification_ids],
            )
            return cursor.rowcount

    def get_verification_held_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        with self._connection() as conn:
            if max_age_hours is not None:
                ref_now = now or utc_now()
                cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'HELD_FOR_VERIFICATION' AND sent_at >= ?
                    ORDER BY sent_at ASC, notification_id ASC
                    """,
                    (cutoff,),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'HELD_FOR_VERIFICATION'
                    ORDER BY sent_at ASC, notification_id ASC
                    """
                )
            rows = cursor.fetchall()
            return [
                NotificationRecord(
                    notification_id=r["notification_id"],
                    story_id=r["story_id"],
                    update_id=r["update_id"] if "update_id" in r.keys() else None,
                    kind=EditorialKind(r["kind"]),
                    channel=r["channel"],
                    sent_at=datetime.fromisoformat(r["sent_at"]),
                    status=DeliveryStatus(r["status"]),
                    message_text=r["message_text"],
                    headline_tr=r["headline_tr"],
                    score=r["score"],
                    error_message=r["error_message"],
                )
                for r in rows
            ]

    def get_sensitive_review_notifications(
        self, max_age_hours: Optional[int] = None, now: Optional[datetime] = None
    ) -> list[NotificationRecord]:
        with self._connection() as conn:
            if max_age_hours is not None:
                ref_now = now or utc_now()
                cutoff = (ref_now - timedelta(hours=max_age_hours)).isoformat()
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'HELD_FOR_SENSITIVE_REVIEW' AND sent_at >= ?
                    ORDER BY sent_at ASC, notification_id ASC
                    """,
                    (cutoff,),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT notification_id, story_id, update_id, kind, channel, sent_at,
                           status, message_text, headline_tr, score, error_message
                    FROM notifications
                    WHERE status = 'HELD_FOR_SENSITIVE_REVIEW'
                    ORDER BY sent_at ASC, notification_id ASC
                    """
                )
            rows = cursor.fetchall()
            return [
                NotificationRecord(
                    notification_id=r["notification_id"],
                    story_id=r["story_id"],
                    update_id=r["update_id"] if "update_id" in r.keys() else None,
                    kind=EditorialKind(r["kind"]),
                    channel=r["channel"],
                    sent_at=datetime.fromisoformat(r["sent_at"]),
                    status=DeliveryStatus(r["status"]),
                    message_text=r["message_text"],
                    headline_tr=r["headline_tr"],
                    score=r["score"],
                    error_message=r["error_message"],
                )
                for r in rows
            ]

    def get_daily_sent_message_count(self, now: Optional[datetime] = None) -> int:
        ref_now = (now or utc_now()).astimezone(timezone.utc)
        day_prefix = ref_now.strftime("%Y-%m-%d")
        with self._connection() as conn:
            # Individual SENT messages + distinct digest batches delivered today
            cursor = conn.execute(
                """
                SELECT channel, delivered_at, sent_at, notification_id
                FROM notifications
                WHERE status = 'SENT'
                  AND (COALESCE(delivered_at, sent_at) LIKE ?)
                """,
                (f"{day_prefix}%",),
            )
            individual_count = 0
            digest_batches: set[str] = set()
            for r in cursor.fetchall():
                if r["channel"] == "TELEGRAM_DIGEST":
                    batch_key = r["delivered_at"] or r["sent_at"]
                    digest_batches.add(batch_key)
                else:
                    individual_count += 1
            return individual_count + len(digest_batches)

    def get_last_digest_sent_at(self) -> Optional[datetime]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT COALESCE(delivered_at, sent_at) AS ts
                FROM notifications
                WHERE status = 'SENT' AND channel = 'TELEGRAM_DIGEST'
                ORDER BY COALESCE(delivered_at, sent_at) DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            if row and row["ts"]:
                return datetime.fromisoformat(row["ts"])
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
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO ai_usage_ledger
                (usage_id, run_mode, model_name, batch_hash, candidate_count,
                 input_tokens, output_tokens, estimated_cost_usd, response_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
                ),
            )

    def get_monthly_ai_usage(
        self, year_month_prefix: Optional[str] = None, now: Optional[datetime] = None
    ) -> dict[str, float | int]:
        ref_now = (now or utc_now()).astimezone(timezone.utc)
        ym = year_month_prefix or ref_now.strftime("%Y-%m")
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT COUNT(*) AS call_count,
                       COALESCE(SUM(input_tokens), 0) AS input_tokens,
                       COALESCE(SUM(output_tokens), 0) AS output_tokens,
                       COALESCE(SUM(estimated_cost_usd), 0.0) AS total_cost_usd
                FROM ai_usage_ledger
                WHERE created_at LIKE ?
                """,
                (f"{ym}%",),
            )
            row = cursor.fetchone()
            return {
                "call_count": int(row["call_count"] or 0),
                "input_tokens": int(row["input_tokens"] or 0),
                "output_tokens": int(row["output_tokens"] or 0),
                "total_cost_usd": round(float(row["total_cost_usd"] or 0.0), 6),
            }

    def get_cached_ai_batch_response(self, batch_hash: str, model_name: str) -> Optional[str]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT response_json
                FROM ai_usage_ledger
                WHERE batch_hash = ? AND model_name = ? AND response_json IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (batch_hash, model_name),
            )
            row = cursor.fetchone()
            return row["response_json"] if row else None

    # --- Telegram Delivery Outbox & Recipient Lifecycle ---
    def record_telegram_delivery(self, delivery: TelegramDeliveryRecord) -> None:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT delivery_id, message_key, recipient_role, recipient_chat_id,
                       category, status, sent_at, payload_json
                FROM telegram_delivery_records
                WHERE recipient_chat_id = ? AND message_key = ?
                """,
                (delivery.recipient_chat_id, delivery.message_key),
            )
            existing = cursor.fetchone()
            if existing:
                # 1. Reject conflicting identities
                if existing["recipient_role"] != delivery.recipient_role:
                    raise DeliveryIdentityConflictError(
                        f"Conflicting recipient role for {delivery.masked_recipient_chat_id}:{delivery.message_key}: "
                        f"existing '{existing['recipient_role']}' vs new '{delivery.recipient_role}'"
                    )
                if existing["category"] != delivery.category.value:
                    raise DeliveryIdentityConflictError(
                        f"Conflicting notification category for {delivery.masked_recipient_chat_id}:{delivery.message_key}: "
                        f"existing '{existing['category']}' vs new '{delivery.category.value}'"
                    )

                # 2. Legal status transition validation
                existing_status = DeliveryStatus(existing["status"])
                validate_delivery_status_transition(existing_status, delivery.status)

                # 3. Terminal SENT or DELIVERY_UNCERTAIN idempotent replay
                if existing_status == DeliveryStatus.SENT and delivery.status == DeliveryStatus.SENT:
                    return
                if existing_status == DeliveryStatus.DELIVERY_UNCERTAIN and delivery.status == DeliveryStatus.DELIVERY_UNCERTAIN:
                    return

            conn.execute(
                """
                INSERT INTO telegram_delivery_records (
                    delivery_id, message_key, recipient_role, recipient_chat_id,
                    category, status, attempt_count, max_attempts, error_message,
                    payload_json, created_at, updated_at, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(recipient_chat_id, message_key) DO UPDATE SET
                    status = excluded.status,
                    attempt_count = MAX(telegram_delivery_records.attempt_count, excluded.attempt_count),
                    error_message = excluded.error_message,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at,
                    sent_at = COALESCE(telegram_delivery_records.sent_at, excluded.sent_at)
                WHERE telegram_delivery_records.status NOT IN ('SENT', 'DELIVERY_UNCERTAIN')
                """,
                (
                    delivery.delivery_id,
                    delivery.message_key,
                    delivery.recipient_role,
                    delivery.recipient_chat_id,
                    delivery.category.value,
                    delivery.status.value,
                    delivery.attempt_count,
                    delivery.max_attempts,
                    delivery.error_message,
                    json.dumps(delivery.payload, ensure_ascii=False),
                    delivery.created_at.isoformat(),
                    delivery.updated_at.isoformat(),
                    delivery.sent_at.isoformat() if delivery.sent_at else None,
                ),
            )

    def get_telegram_delivery(self, recipient_chat_id: str, message_key: str) -> Optional[TelegramDeliveryRecord]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT delivery_id, message_key, recipient_role, recipient_chat_id,
                       category, status, attempt_count, max_attempts, error_message,
                       payload_json, created_at, updated_at, sent_at
                FROM telegram_delivery_records
                WHERE recipient_chat_id = ? AND message_key = ?
                """,
                (recipient_chat_id, message_key),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return TelegramDeliveryRecord(
                delivery_id=row["delivery_id"],
                message_key=row["message_key"],
                recipient_role=row["recipient_role"],
                recipient_chat_id=row["recipient_chat_id"],
                category=TelegramNotificationCategory(row["category"]),
                status=DeliveryStatus(row["status"]),
                attempt_count=int(row["attempt_count"]),
                max_attempts=int(row["max_attempts"]),
                error_message=row["error_message"],
                payload=json.loads(row["payload_json"] or "{}"),
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                sent_at=datetime.fromisoformat(row["sent_at"]) if row["sent_at"] else None,
            )

    def update_telegram_delivery_status(
        self,
        delivery_id: str,
        status: DeliveryStatus,
        error_message: Optional[str] = None,
        sent_at: Optional[datetime] = None,
    ) -> bool:
        now_iso = utc_now().isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT status, sent_at FROM telegram_delivery_records WHERE delivery_id = ?",
                (delivery_id,),
            )
            row = cursor.fetchone()
            if not row:
                return False
            existing_status = DeliveryStatus(row["status"])
            validate_delivery_status_transition(existing_status, status)
            if existing_status == DeliveryStatus.SENT and status == DeliveryStatus.SENT:
                return True
            if existing_status == DeliveryStatus.DELIVERY_UNCERTAIN and status == DeliveryStatus.DELIVERY_UNCERTAIN:
                return True

            sent_iso = sent_at.isoformat() if sent_at else (now_iso if status == DeliveryStatus.SENT else None)
            cursor = conn.execute(
                """
                UPDATE telegram_delivery_records
                SET status = ?,
                    error_message = ?,
                    updated_at = ?,
                    sent_at = COALESCE(sent_at, ?),
                    attempt_count = attempt_count + 1
                WHERE delivery_id = ?
                  AND status NOT IN ('SENT', 'DELIVERY_UNCERTAIN')
                """,
                (status.value, error_message, now_iso, sent_iso, delivery_id),
            )
            return cursor.rowcount > 0

    def reconcile_uncertain_delivery(
        self,
        delivery_id: str,
        reconciliation_proof: dict[str, Any],
        reconciled_at: Optional[datetime] = None,
        *,
        operator_authorized: bool = False,
        live_mode: bool = False,
    ) -> TelegramDeliveryRecord:
        """Reconcile a DELIVERY_UNCERTAIN record to SENT using operator-supplied evidence metadata.

        NOTE:
        - The metadata strings in reconciliation_proof represent operator-supplied evidence metadata only,
          NOT independently verified authoritative evidence.
        - Unattended or automated flows must NEVER invoke this method.
        - In live operation (live_mode=True), reconciliation is permanently disabled in this milestone.
        - Operator authorization (operator_authorized=True or proof['operator_authorized']=True) is strictly
          required. Empty or fabricated metadata strings alone do not authorize delivery-state transitions.
        """
        if live_mode:
            raise DeliveryStateError(
                "Live automatic reconciliation of DELIVERY_UNCERTAIN records is disabled in Phase 5B.3B. "
                "Independent proof verification is not implemented."
            )

        if not isinstance(reconciliation_proof, dict):
            raise ValueError("Reconciliation proof must be a dictionary.")
        method = str(reconciliation_proof.get("reconciliation_method", "")).strip()
        verifier = str(reconciliation_proof.get("verified_by", "")).strip()
        ref = str(reconciliation_proof.get("proof_reference", "")).strip()
        if not method or not verifier or not ref:
            raise ValueError(
                "Reconciliation proof requires non-empty operator-supplied metadata "
                "('reconciliation_method', 'verified_by', and 'proof_reference')."
            )

        is_authorized = operator_authorized or bool(reconciliation_proof.get("operator_authorized", False))
        if not is_authorized:
            raise DeliveryStateError(
                "Reconciliation requires explicit operator authorization (operator_authorized=True); "
                "operator-supplied metadata strings alone do not authorize delivery-state transitions."
            )

        rec_dt = reconciled_at or utc_now()
        rec_iso = rec_dt.isoformat()

        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT delivery_id, message_key, recipient_role, recipient_chat_id,
                       category, status, attempt_count, max_attempts, error_message,
                       payload_json, created_at, updated_at, sent_at
                FROM telegram_delivery_records
                WHERE delivery_id = ?
                """,
                (delivery_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise DeliveryStateError(f"Delivery record '{delivery_id}' not found.")

            existing_status = DeliveryStatus(row["status"])
            if existing_status != DeliveryStatus.DELIVERY_UNCERTAIN:
                raise DeliveryStateError(
                    f"Cannot reconcile delivery '{delivery_id}' with status '{existing_status.value}'; "
                    "only DELIVERY_UNCERTAIN records can be reconciled."
                )

            payload = json.loads(row["payload_json"] or "{}")
            payload["original_uncertainty_evidence"] = {
                "error_message": row["error_message"],
                "marked_uncertain_at": row["updated_at"],
            }
            op_meta = {
                "reconciliation_method": method,
                "verified_by": verifier,
                "proof_reference": ref,
                "proof_notes": reconciliation_proof.get("proof_notes"),
                "operator_authorized": True,
                "reconciled_at": rec_iso,
            }
            payload["operator_reconciliation_metadata"] = op_meta
            payload["reconciliation"] = op_meta

            conn.execute(
                """
                UPDATE telegram_delivery_records
                SET status = 'SENT',
                    updated_at = ?,
                    sent_at = ?,
                    payload_json = ?
                WHERE delivery_id = ? AND status = 'DELIVERY_UNCERTAIN'
                """,
                (rec_iso, rec_iso, json.dumps(payload, ensure_ascii=False), delivery_id),
            )

            return TelegramDeliveryRecord(
                delivery_id=row["delivery_id"],
                message_key=row["message_key"],
                recipient_role=row["recipient_role"],
                recipient_chat_id=row["recipient_chat_id"],
                category=TelegramNotificationCategory(row["category"]),
                status=DeliveryStatus.SENT,
                attempt_count=int(row["attempt_count"]),
                max_attempts=int(row["max_attempts"]),
                error_message=row["error_message"],
                payload=payload,
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=rec_dt,
                sent_at=rec_dt,
            )

    def is_digest_delivered_to_recipient(self, recipient_chat_id: str, message_key: str) -> bool:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT 1 FROM telegram_delivery_records
                WHERE recipient_chat_id = ? AND message_key = ? AND status = 'SENT'
                """,
                (recipient_chat_id, message_key),
            )
            return cursor.fetchone() is not None

    def get_pending_telegram_deliveries(self, limit: int = 50) -> list[TelegramDeliveryRecord]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT delivery_id, message_key, recipient_role, recipient_chat_id,
                       category, status, attempt_count, max_attempts, error_message,
                       payload_json, created_at, updated_at, sent_at
                FROM telegram_delivery_records
                WHERE status IN ('PENDING', 'DISPATCHING')
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,),
            )
            records: list[TelegramDeliveryRecord] = []
            for row in cursor.fetchall():
                records.append(
                    TelegramDeliveryRecord(
                        delivery_id=row["delivery_id"],
                        message_key=row["message_key"],
                        recipient_role=row["recipient_role"],
                        recipient_chat_id=row["recipient_chat_id"],
                        category=TelegramNotificationCategory(row["category"]),
                        status=DeliveryStatus(row["status"]),
                        attempt_count=int(row["attempt_count"]),
                        max_attempts=int(row["max_attempts"]),
                        error_message=row["error_message"],
                        payload=json.loads(row["payload_json"] or "{}"),
                        created_at=datetime.fromisoformat(row["created_at"]),
                        updated_at=datetime.fromisoformat(row["updated_at"]),
                        sent_at=datetime.fromisoformat(row["sent_at"]) if row["sent_at"] else None,
                    )
                )
            return records

    # --- Durable Cloud Recovery State ---
    def save_recovery_state(
        self,
        stage: str,
        worker_id: str,
        state_payload: dict[str, Any],
        batch_id: Optional[str] = None,
        last_processed_id: Optional[str] = None,
    ) -> None:
        now_iso = utc_now().isoformat()
        state_key = f"{stage}::current"
        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO durable_recovery_state (
                    state_key, stage, worker_id, batch_id, last_processed_id,
                    state_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    stage = excluded.stage,
                    worker_id = excluded.worker_id,
                    batch_id = excluded.batch_id,
                    last_processed_id = excluded.last_processed_id,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    state_key,
                    stage,
                    worker_id,
                    batch_id,
                    last_processed_id,
                    json.dumps(state_payload, ensure_ascii=False),
                    now_iso,
                    now_iso,
                ),
            )

    def get_recovery_state(self, stage: str) -> Optional[dict[str, Any]]:
        state_key = f"{stage}::current"
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT state_key, stage, worker_id, batch_id, last_processed_id,
                       state_json, created_at, updated_at
                FROM durable_recovery_state
                WHERE state_key = ?
                """,
                (state_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "state_key": row["state_key"],
                "stage": row["stage"],
                "worker_id": row["worker_id"],
                "batch_id": row["batch_id"],
                "last_processed_id": row["last_processed_id"],
                "state_payload": json.loads(row["state_json"] or "{}"),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }

    def clear_recovery_state(self, stage: str) -> None:
        state_key = f"{stage}::current"
        with self._connection() as conn:
            conn.execute("DELETE FROM durable_recovery_state WHERE state_key = ?", (state_key,))

    # --- At-Most-Once Daily Admin Operational Reports ---
    def record_admin_report(
        self,
        report_date: str,
        worker_id: str,
        status: DeliveryStatus,
        recipient_chat_id: str,
        summary_payload: dict[str, Any],
    ) -> bool:
        now_iso = utc_now().isoformat()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO admin_operational_reports (
                    report_date, dispatched_at, worker_id, status, recipient_chat_id, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    report_date,
                    now_iso,
                    worker_id,
                    status.value,
                    recipient_chat_id,
                    json.dumps(summary_payload, ensure_ascii=False),
                ),
            )
            return cursor.rowcount > 0

    def has_admin_report_been_dispatched(self, report_date: str) -> bool:
        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM admin_operational_reports WHERE report_date = ? AND status = 'SENT'",
                (report_date,),
            )
            return cursor.fetchone() is not None

    # --- Experiment Budget Ledger & Monthly Consumption ---
    def record_budget_usage(
        self,
        period_month: str,
        run_id: str,
        worker_id: str,
        calls_count: int,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        estimated_cost_usd: float,
        timestamp: Optional[datetime] = None,
        record_id: Optional[str] = None,
    ) -> bool:
        clean_run = run_id.strip()
        rec_id = record_id or f"bud_{period_month}_{clean_run}"
        ts_iso = (timestamp or utc_now()).isoformat()

        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT record_id, period_month, run_id, worker_id, timestamp, calls_count,
                       prompt_tokens, completion_tokens, total_tokens, estimated_cost_usd
                FROM experiment_budget_history
                WHERE record_id = ?
                """,
                (rec_id,),
            )
            existing = cursor.fetchone()
            if existing:
                is_identical = (
                    existing["period_month"] == period_month
                    and existing["run_id"] == clean_run
                    and existing["worker_id"] == worker_id
                    and existing["timestamp"] == ts_iso
                    and int(existing["calls_count"]) == int(calls_count)
                    and int(existing["prompt_tokens"]) == int(prompt_tokens)
                    and int(existing["completion_tokens"]) == int(completion_tokens)
                    and int(existing["total_tokens"]) == int(total_tokens)
                    and abs(float(existing["estimated_cost_usd"]) - float(estimated_cost_usd)) < 1e-9
                )
                if is_identical:
                    return True
                raise BudgetHistoryConflictError(
                    f"Conflict in immutable budget event '{rec_id}': existing "
                    f"(period={existing['period_month']}, run={existing['run_id']}, worker={existing['worker_id']}, "
                    f"ts={existing['timestamp']}, calls={existing['calls_count']}, prompt={existing['prompt_tokens']}, "
                    f"cost={existing['estimated_cost_usd']}) vs new "
                    f"(period={period_month}, run={clean_run}, worker={worker_id}, ts={ts_iso}, "
                    f"calls={calls_count}, prompt={prompt_tokens}, cost={estimated_cost_usd})"
                )

            conn.execute(
                """
                INSERT INTO experiment_budget_history (
                    record_id, period_month, run_id, worker_id, timestamp,
                    calls_count, prompt_tokens, completion_tokens, total_tokens,
                    estimated_cost_usd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rec_id,
                    period_month,
                    clean_run,
                    worker_id,
                    ts_iso,
                    calls_count,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    estimated_cost_usd,
                ),
            )
            return True

    def get_monthly_budget_summary(self, period_month: str) -> dict[str, Any]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT COUNT(*) AS entry_count,
                       COALESCE(SUM(calls_count), 0) AS total_calls,
                       COALESCE(SUM(prompt_tokens), 0) AS total_prompt_tokens,
                       COALESCE(SUM(completion_tokens), 0) AS total_completion_tokens,
                       COALESCE(SUM(total_tokens), 0) AS total_tokens,
                       COALESCE(SUM(estimated_cost_usd), 0.0) AS total_cost_usd
                FROM experiment_budget_history
                WHERE period_month = ?
                """,
                (period_month,),
            )
            row = cursor.fetchone()
            return {
                "period_month": period_month,
                "entry_count": int(row["entry_count"] or 0),
                "total_calls": int(row["total_calls"] or 0),
                "total_prompt_tokens": int(row["total_prompt_tokens"] or 0),
                "total_completion_tokens": int(row["total_completion_tokens"] or 0),
                "total_tokens": int(row["total_tokens"] or 0),
                "total_cost_usd": round(float(row["total_cost_usd"] or 0.0), 6),
            }

    def get_budget_record(self, record_id: str) -> Optional[dict[str, Any]]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT record_id, period_month, run_id, worker_id, timestamp,
                       calls_count, prompt_tokens, completion_tokens, total_tokens,
                       estimated_cost_usd
                FROM experiment_budget_history
                WHERE record_id = ?
                """,
                (record_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "record_id": row["record_id"],
                "period_month": row["period_month"],
                "run_id": row["run_id"],
                "worker_id": row["worker_id"],
                "timestamp": row["timestamp"],
                "calls_count": int(row["calls_count"]),
                "prompt_tokens": int(row["prompt_tokens"]),
                "completion_tokens": int(row["completion_tokens"]),
                "total_tokens": int(row["total_tokens"]),
                "estimated_cost_usd": float(row["estimated_cost_usd"]),
            }

    # --- Scheduled Cloud Execution Audit History ---
    def record_scheduled_execution_start(
        self,
        run_id: str,
        run_type: str,
        worker_id: str,
        started_at: Optional[datetime] = None,
    ) -> None:
        start_iso = (started_at or utc_now()).isoformat()
        with self._connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO scheduled_execution_history (
                    run_id, run_type, worker_id, started_at, status
                ) VALUES (?, ?, ?, ?, 'RUNNING')
                """,
                (run_id, run_type, worker_id, start_iso),
            )

    def record_scheduled_execution_finish(
        self,
        run_id: str,
        status: str,
        articles_collected: int = 0,
        candidates_evaluated: int = 0,
        notifications_dispatched: int = 0,
        error_message: Optional[str] = None,
        finished_at: Optional[datetime] = None,
    ) -> None:
        fin_iso = (finished_at or utc_now()).isoformat()
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE scheduled_execution_history
                SET finished_at = ?,
                    status = ?,
                    articles_collected = ?,
                    candidates_evaluated = ?,
                    notifications_dispatched = ?,
                    error_message = ?
                WHERE run_id = ?
                """,
                (
                    fin_iso,
                    status,
                    articles_collected,
                    candidates_evaluated,
                    notifications_dispatched,
                    error_message,
                    run_id,
                ),
            )

    def get_scheduled_execution(self, run_id: str) -> Optional[dict[str, Any]]:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT run_id, run_type, worker_id, started_at, finished_at,
                       status, articles_collected, candidates_evaluated,
                       notifications_dispatched, error_message
                FROM scheduled_execution_history
                WHERE run_id = ?
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return dict(row)


