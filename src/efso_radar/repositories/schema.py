"""Database schema definitions and versioned migration engine.

Portable across local SQLite and remote Turso (libSQL). Uses standard SQL
data definition statements supported by both engines.
"""
from __future__ import annotations

import logging
from typing import NamedTuple

logger = logging.getLogger("efso-radar.repository.schema")

CURRENT_SCHEMA_VERSION = 6


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
    MigrationStep(
        version=5,
        description="Persistent AI token usage ledger and batch cache for cost protection",
        statements=[
            """
            CREATE TABLE IF NOT EXISTS ai_usage_ledger (
                usage_id TEXT PRIMARY KEY,
                run_mode TEXT NOT NULL,
                model_name TEXT NOT NULL,
                batch_hash TEXT NOT NULL,
                candidate_count INTEGER NOT NULL,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                estimated_cost_usd REAL NOT NULL,
                response_json TEXT,
                created_at TEXT NOT NULL
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_ai_usage_ledger_created_at
                ON ai_usage_ledger (created_at);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_ai_usage_ledger_batch_hash
                ON ai_usage_ledger (batch_hash, model_name);
            """,
        ],
    ),
    MigrationStep(
        version=6,
        description="Persistent state contracts: outbox, recovery, budget, admin reports, and run history",
        statements=[
            """
            CREATE TABLE IF NOT EXISTS telegram_delivery_records (
                delivery_id TEXT PRIMARY KEY,
                message_key TEXT NOT NULL,
                recipient_role TEXT NOT NULL,
                recipient_chat_id TEXT NOT NULL,
                category TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                error_message TEXT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                sent_at TEXT,
                CONSTRAINT uq_recipient_message UNIQUE (recipient_chat_id, message_key)
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_delivery_records_status
                ON telegram_delivery_records (status, updated_at);
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_telegram_delivery_records_role
                ON telegram_delivery_records (recipient_role, created_at);
            """,
            """
            CREATE TABLE IF NOT EXISTS durable_recovery_state (
                state_key TEXT PRIMARY KEY,
                stage TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                batch_id TEXT,
                last_processed_id TEXT,
                state_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_durable_recovery_stage
                ON durable_recovery_state (stage, updated_at);
            """,
            """
            CREATE TABLE IF NOT EXISTS admin_operational_reports (
                report_date TEXT PRIMARY KEY,
                dispatched_at TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                status TEXT NOT NULL,
                recipient_chat_id TEXT NOT NULL,
                summary_json TEXT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS experiment_budget_history (
                record_id TEXT PRIMARY KEY,
                period_month TEXT NOT NULL,
                run_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                calls_count INTEGER NOT NULL DEFAULT 0,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                estimated_cost_usd REAL NOT NULL DEFAULT 0.0
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_budget_history_period
                ON experiment_budget_history (period_month, timestamp);
            """,
            """
            CREATE TABLE IF NOT EXISTS scheduled_execution_history (
                run_id TEXT PRIMARY KEY,
                run_type TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                articles_collected INTEGER NOT NULL DEFAULT 0,
                candidates_evaluated INTEGER NOT NULL DEFAULT 0,
                notifications_dispatched INTEGER NOT NULL DEFAULT 0,
                error_message TEXT
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_scheduled_execution_started
                ON scheduled_execution_history (started_at);
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
        """
        CREATE TABLE IF NOT EXISTS ai_usage_ledger (
            usage_id TEXT PRIMARY KEY,
            run_mode TEXT NOT NULL,
            model_name TEXT NOT NULL,
            batch_hash TEXT NOT NULL,
            candidate_count INTEGER NOT NULL,
            input_tokens INTEGER NOT NULL,
            output_tokens INTEGER NOT NULL,
            estimated_cost_usd REAL NOT NULL,
            response_json TEXT,
            created_at TEXT NOT NULL
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_ai_usage_ledger_created_at
            ON ai_usage_ledger (created_at);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_ai_usage_ledger_batch_hash
            ON ai_usage_ledger (batch_hash, model_name);
        """,
        """
        CREATE TABLE IF NOT EXISTS telegram_delivery_records (
            delivery_id TEXT PRIMARY KEY,
            message_key TEXT NOT NULL,
            recipient_role TEXT NOT NULL,
            recipient_chat_id TEXT NOT NULL,
            category TEXT NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            error_message TEXT,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            sent_at TEXT,
            CONSTRAINT uq_recipient_message UNIQUE (recipient_chat_id, message_key)
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_telegram_delivery_records_status
            ON telegram_delivery_records (status, updated_at);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_telegram_delivery_records_role
            ON telegram_delivery_records (recipient_role, created_at);
        """,
        """
        CREATE TABLE IF NOT EXISTS durable_recovery_state (
            state_key TEXT PRIMARY KEY,
            stage TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            batch_id TEXT,
            last_processed_id TEXT,
            state_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_durable_recovery_stage
            ON durable_recovery_state (stage, updated_at);
        """,
        """
        CREATE TABLE IF NOT EXISTS admin_operational_reports (
            report_date TEXT PRIMARY KEY,
            dispatched_at TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            status TEXT NOT NULL,
            recipient_chat_id TEXT NOT NULL,
            summary_json TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS experiment_budget_history (
            record_id TEXT PRIMARY KEY,
            period_month TEXT NOT NULL,
            run_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            calls_count INTEGER NOT NULL DEFAULT 0,
            prompt_tokens INTEGER NOT NULL DEFAULT 0,
            completion_tokens INTEGER NOT NULL DEFAULT 0,
            total_tokens INTEGER NOT NULL DEFAULT 0,
            estimated_cost_usd REAL NOT NULL DEFAULT 0.0
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_budget_history_period
            ON experiment_budget_history (period_month, timestamp);
        """,
        """
        CREATE TABLE IF NOT EXISTS scheduled_execution_history (
            run_id TEXT PRIMARY KEY,
            run_type TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            articles_collected INTEGER NOT NULL DEFAULT 0,
            candidates_evaluated INTEGER NOT NULL DEFAULT 0,
            notifications_dispatched INTEGER NOT NULL DEFAULT 0,
            error_message TEXT
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_scheduled_execution_started
            ON scheduled_execution_history (started_at);
        """,
    ]
    return stmts
