"""Database schema definitions and versioned migration engine.

Portable across local SQLite and remote Turso (libSQL). Uses standard SQL
data definition statements supported by both engines.
"""
from __future__ import annotations

import logging
from typing import NamedTuple

logger = logging.getLogger("efso-radar.repository.schema")

CURRENT_SCHEMA_VERSION = 4


class MigrationStep(NamedTuple):
    version: int
    description: str
    statements: list[str]


MIGRATIONS: list[MigrationStep] = [
    MigrationStep(
        version=1,
        description="Initial core radar schema",
        statements=[
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL,
                description TEXT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS run_locks (
                lock_key TEXT PRIMARY KEY,
                worker_id TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                heartbeat_at TEXT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS processed_articles (
                article_id TEXT PRIMARY KEY,
                canonical_url TEXT,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                published_at TEXT NOT NULL,
                processed_at TEXT NOT NULL
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_processed_articles_processed_at
                ON processed_articles (processed_at);
            """,
            """
            CREATE TABLE IF NOT EXISTS stories (
                story_id TEXT PRIMARY KEY,
                headline_tr TEXT NOT NULL,
                category TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_updated_at TEXT NOT NULL,
                article_ids_json TEXT NOT NULL,
                update_count INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_stories_last_updated
                ON stories (last_updated_at);
            """,
            """
            CREATE TABLE IF NOT EXISTS story_updates (
                update_id TEXT PRIMARY KEY,
                story_id TEXT NOT NULL,
                development_tr TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                article_ids_json TEXT NOT NULL,
                significance_score INTEGER NOT NULL,
                FOREIGN KEY(story_id) REFERENCES stories(story_id)
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_story_updates_story_id
                ON story_updates (story_id);
            """,
            """
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id TEXT PRIMARY KEY,
                story_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                channel TEXT NOT NULL,
                sent_at TEXT NOT NULL,
                status TEXT NOT NULL,
                message_text TEXT NOT NULL,
                headline_tr TEXT NOT NULL,
                score INTEGER NOT NULL,
                error_message TEXT
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_notifications_status
                ON notifications (status);
            """,
        ],
    ),
    MigrationStep(
        version=2,
        description="Enhanced story intelligence, lease tracking, and assessment records",
        statements=[
            """
            CREATE TABLE IF NOT EXISTS editorial_assessments (
                assessment_id TEXT PRIMARY KEY,
                story_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                score INTEGER NOT NULL,
                headline_tr TEXT NOT NULL,
                cover_title_tr TEXT NOT NULL,
                caption_idea_tr TEXT NOT NULL,
                why_efso_tr TEXT NOT NULL,
                suggested_format TEXT NOT NULL,
                category TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_assessments_story_id
                ON editorial_assessments (story_id);
            """,
        ],
    ),
    MigrationStep(
        version=3,
        description="Owner feedback storage for AI calibration",
        statements=[
            """
            CREATE TABLE IF NOT EXISTS owner_feedback (
                feedback_id TEXT PRIMARY KEY,
                story_id TEXT,
                article_id TEXT,
                rating TEXT NOT NULL,
                notes TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_owner_feedback_rating
                ON owner_feedback (rating);
            """,
        ],
    ),
    MigrationStep(
        version=4,
        description="Candidate backlog persistence to prevent premature loss of deprioritized stories",
        statements=[
            """
            CREATE TABLE IF NOT EXISTS candidate_backlog (
                article_id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                canonical_url TEXT,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                market TEXT NOT NULL,
                category_hint TEXT,
                snippet TEXT NOT NULL,
                image_url TEXT,
                published_at TEXT NOT NULL,
                discovered_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'QUEUED',
                priority_score REAL NOT NULL DEFAULT 0.0
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_candidate_backlog_status
                ON candidate_backlog (status, discovered_at);
            """,
        ],
    ),
]


def get_column_migrations() -> list[tuple[str, str, str]]:
    """Return required (table, column, col_type) migrations for existing schemas."""
    return [
        ("notifications", "update_id", "TEXT"),
        ("notifications", "delivered_at", "TEXT"),
        ("notifications", "lease_worker_id", "TEXT"),
        ("notifications", "lease_expires_at", "TEXT"),
    ]



def get_all_init_statements() -> list[str]:
    """Return all DDL statements required to initialize a fresh database from scratch."""
    stmts: list[str] = [
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL,
            description TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS run_locks (
            lock_key TEXT PRIMARY KEY,
            worker_id TEXT NOT NULL,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS processed_articles (
            article_id TEXT PRIMARY KEY,
            canonical_url TEXT,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            published_at TEXT NOT NULL,
            processed_at TEXT NOT NULL
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_processed_articles_processed_at
            ON processed_articles (processed_at);
        """,
        """
        CREATE TABLE IF NOT EXISTS stories (
            story_id TEXT PRIMARY KEY,
            headline_tr TEXT NOT NULL,
            category TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_updated_at TEXT NOT NULL,
            article_ids_json TEXT NOT NULL,
            update_count INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_stories_last_updated
            ON stories (last_updated_at);
        """,
        """
        CREATE TABLE IF NOT EXISTS story_updates (
            update_id TEXT PRIMARY KEY,
            story_id TEXT NOT NULL,
            development_tr TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            article_ids_json TEXT NOT NULL,
            significance_score INTEGER NOT NULL,
            FOREIGN KEY(story_id) REFERENCES stories(story_id)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_story_updates_story_id
            ON story_updates (story_id);
        """,
        """
        CREATE TABLE IF NOT EXISTS editorial_assessments (
            assessment_id TEXT PRIMARY KEY,
            story_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            score INTEGER NOT NULL,
            headline_tr TEXT NOT NULL,
            cover_title_tr TEXT NOT NULL,
            caption_idea_tr TEXT NOT NULL,
            why_efso_tr TEXT NOT NULL,
            suggested_format TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_assessments_story_id
            ON editorial_assessments (story_id);
        """,
        """
        CREATE TABLE IF NOT EXISTS notifications (
            notification_id TEXT PRIMARY KEY,
            story_id TEXT NOT NULL,
            update_id TEXT,
            kind TEXT NOT NULL,
            channel TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            delivered_at TEXT,
            status TEXT NOT NULL,
            message_text TEXT NOT NULL,
            headline_tr TEXT NOT NULL,
            score INTEGER NOT NULL,
            lease_worker_id TEXT,
            lease_expires_at TEXT,
            error_message TEXT
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_notifications_status
            ON notifications (status);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_notifications_story_id
            ON notifications (story_id);
        """,
        """
        CREATE TABLE IF NOT EXISTS owner_feedback (
            feedback_id TEXT PRIMARY KEY,
            story_id TEXT,
            article_id TEXT,
            rating TEXT NOT NULL,
            notes TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_owner_feedback_rating
            ON owner_feedback (rating);
        """,
        """
        CREATE TABLE IF NOT EXISTS candidate_backlog (
            article_id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            canonical_url TEXT,
            title TEXT NOT NULL,
            source TEXT NOT NULL,
            market TEXT NOT NULL,
            category_hint TEXT,
            snippet TEXT NOT NULL,
            image_url TEXT,
            published_at TEXT NOT NULL,
            discovered_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'QUEUED',
            priority_score REAL NOT NULL DEFAULT 0.0
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_candidate_backlog_status
            ON candidate_backlog (status, discovered_at);
        """,
    ]
    return stmts
