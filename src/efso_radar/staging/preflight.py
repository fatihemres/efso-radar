"""Read-only cloud preflight validator for Efso Radar staging environments.

Non-mutating verification of schema v6 tables, columns, indexes, constraints,
repository contract compatibility, persistent state capabilities, and data integrity
across SQLite and Turso (libSQL) backends.
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.domain import DeliveryStatus
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository

logger = logging.getLogger("efso-radar.staging.preflight")

SCHEMA_V6_TABLES: dict[str, list[str]] = {
    "schema_migrations": ["version", "applied_at", "description"],
    "run_locks": ["lock_key", "worker_id", "acquired_at", "expires_at", "heartbeat_at"],
    "processed_articles": ["article_id", "canonical_url", "title", "source", "published_at", "processed_at"],
    "stories": [
        "story_id", "headline_tr", "category", "first_seen_at",
        "last_updated_at", "article_ids_json", "update_count", "is_active",
    ],
    "story_updates": [
        "update_id", "story_id", "development_tr", "detected_at",
        "article_ids_json", "significance_score",
    ],
    "notifications": [
        "notification_id", "story_id", "update_id", "kind", "channel",
        "sent_at", "delivered_at", "status", "message_text", "headline_tr",
        "score", "lease_worker_id", "lease_expires_at", "error_message",
    ],
    "editorial_assessments": [
        "assessment_id", "story_id", "kind", "score", "headline_tr",
        "cover_title_tr", "caption_idea_tr", "why_efso_tr", "suggested_format",
        "category", "created_at",
    ],
    "owner_feedback": ["feedback_id", "story_id", "article_id", "rating", "notes", "created_at"],
    "candidate_backlog": [
        "article_id", "url", "canonical_url", "title", "source", "market",
        "category_hint", "snippet", "image_url", "published_at", "discovered_at",
        "status", "priority_score",
    ],
    "ai_usage_ledger": [
        "usage_id", "run_mode", "model_name", "batch_hash", "candidate_count",
        "input_tokens", "output_tokens", "estimated_cost_usd", "response_json", "created_at",
    ],
    "telegram_delivery_records": [
        "delivery_id", "message_key", "recipient_role", "recipient_chat_id",
        "category", "status", "attempt_count", "max_attempts", "error_message",
        "payload_json", "created_at", "updated_at", "sent_at",
    ],
    "durable_recovery_state": [
        "state_key", "stage", "worker_id", "batch_id", "last_processed_id",
        "state_json", "created_at", "updated_at",
    ],
    "admin_operational_reports": [
        "report_date", "dispatched_at", "worker_id", "status",
        "recipient_chat_id", "summary_json",
    ],
    "experiment_budget_history": [
        "record_id", "period_month", "run_id", "worker_id", "timestamp",
        "calls_count", "prompt_tokens", "completion_tokens", "total_tokens",
        "estimated_cost_usd",
    ],
    "scheduled_execution_history": [
        "run_id", "run_type", "worker_id", "started_at", "finished_at",
        "status", "articles_collected", "candidates_evaluated",
        "notifications_dispatched", "error_message",
    ],
}

SCHEMA_V6_INDEXES: dict[str, list[str]] = {
    "processed_articles": ["idx_processed_articles_processed_at"],
    "stories": ["idx_stories_last_updated"],
    "story_updates": ["idx_story_updates_story_id"],
    "notifications": ["idx_notifications_status"],
    "editorial_assessments": ["idx_assessments_story_id"],
    "candidate_backlog": ["idx_candidate_backlog_status"],
    "ai_usage_ledger": ["idx_ai_usage_ledger_created_at", "idx_ai_usage_ledger_batch_hash"],
    "telegram_delivery_records": ["idx_telegram_delivery_records_status", "idx_telegram_delivery_records_role"],
    "durable_recovery_state": ["idx_durable_recovery_stage"],
    "experiment_budget_history": ["idx_budget_history_period"],
    "scheduled_execution_history": ["idx_scheduled_execution_started"],
}

SCHEMA_V6_CONSTRAINTS: dict[str, list[str]] = {
    "telegram_delivery_records": ["uq_recipient_message"],
}


def execute_read_query(repo: StateRepository, sql: str, params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
    """Execute a strictly read-only SQL query against SQLite or Turso repository."""
    clean_sql = sql.strip().upper()
    prohibited_verbs = ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ", "CREATE ", "ATTACH ", "DETACH ")
    if any(clean_sql.startswith(verb) for verb in prohibited_verbs):
        raise ValueError(f"Preflight query safety violation: query must be strictly read-only: {sql[:30]}")

    if isinstance(repo, SqliteStateRepository):
        with repo._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params or [])
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
    elif isinstance(repo, TursoStateRepository):
        res = repo.client.execute(sql, params or [])
        return res.fetchall()
    else:
        raise TypeError(f"Unsupported repository type for preflight inspection: {type(repo)}")


class PreflightReport(BaseModel):
    """Structured report of cloud staging preflight validation."""
    success: bool = False
    repository_type: str = ""
    schema_version: int = 0
    expected_schema_version: int = CURRENT_SCHEMA_VERSION
    tables_checked: int = 0
    columns_checked: int = 0
    indexes_checked: int = 0
    constraints_checked: int = 0
    missing_tables: list[str] = Field(default_factory=list)
    missing_columns: dict[str, list[str]] = Field(default_factory=dict)
    missing_indexes: list[str] = Field(default_factory=list)
    missing_constraints: list[str] = Field(default_factory=list)
    integrity_issues: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    schema_fingerprint_before: str = ""
    schema_fingerprint_after: str = ""

    @property
    def is_non_mutating(self) -> bool:
        """True if the database schema was verified to be strictly unchanged before and after preflight."""
        return (
            bool(self.schema_fingerprint_before)
            and self.schema_fingerprint_before == self.schema_fingerprint_after
        )

    def summary(self) -> str:
        status_str = "PASSED" if self.success else "FAILED"
        lines = [
            f"=== CLOUD STAGING PREFLIGHT REPORT [{status_str}] ===",
            f"Repository Type: {self.repository_type}",
            f"Schema Version: {self.schema_version} (Expected: >={self.expected_schema_version})",
            f"Non-Mutating Invariant Verified: {'YES' if self.is_non_mutating else 'NO'}",
            f"Checks: {self.tables_checked} tables, {self.columns_checked} columns, "
            f"{self.indexes_checked} indexes, {self.constraints_checked} constraints",
        ]
        if self.missing_tables:
            lines.append("Missing Tables:")
            for t in self.missing_tables:
                lines.append(f"  - {t}")
        if self.missing_columns:
            lines.append("Missing Columns:")
            for t, cols in self.missing_columns.items():
                lines.append(f"  - {t}: {', '.join(cols)}")
        if self.missing_indexes:
            lines.append("Missing Indexes:")
            for idx in self.missing_indexes:
                lines.append(f"  - {idx}")
        if self.missing_constraints:
            lines.append("Missing Constraints:")
            for c in self.missing_constraints:
                lines.append(f"  - {c}")
        if self.integrity_issues:
            lines.append("Data Integrity Issues:")
            for issue in self.integrity_issues:
                lines.append(f"  - {issue}")
        if self.errors:
            lines.append("Errors:")
            for err in self.errors:
                lines.append(f"  - {err}")
        if self.warnings:
            lines.append("Warnings:")
            for warn in self.warnings:
                lines.append(f"  - {warn}")
        return "\n".join(lines)


class CloudPreflightValidator:
    """Non-mutating preflight validation engine for cloud staging repositories."""

    def __init__(self, repo: StateRepository, allow_operational_db: bool = False):
        self.repo = repo
        self._assert_isolation(allow_operational_db)

    def _assert_isolation(self, allow_operational_db: bool) -> None:
        """Ensure the validator fails closed if directed towards real production database."""
        if not allow_operational_db and isinstance(self.repo, SqliteStateRepository):
            db_path = getattr(self.repo, "db_path", None)
            if db_path is not None:
                resolved = Path(db_path).resolve()
                prod_path = Path("data/radar.db").resolve()
                if resolved == prod_path:
                    raise PermissionError(
                        "Safety violation: CloudPreflightValidator cannot be invoked directly on "
                        "operational database data/radar.db without explicit authorization."
                    )

    def _compute_schema_fingerprint(self) -> str:
        """Compute an MD5 fingerprint of all existing database objects from sqlite_master."""
        try:
            rows = execute_read_query(
                self.repo,
                "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
            )
            serialized = json.dumps(rows, sort_keys=True)
            return hashlib.md5(serialized.encode("utf-8")).hexdigest()
        except Exception as exc:
            logger.warning("Could not compute schema fingerprint: %s", exc)
            return ""

    def validate(self) -> PreflightReport:
        """Execute full non-mutating preflight validation."""
        report = PreflightReport(
            repository_type=type(self.repo).__name__,
            schema_fingerprint_before=self._compute_schema_fingerprint(),
        )

        try:
            # 1. Schema Version Check
            self._check_schema_version(report)

            # 2. Schema Structure Check (Tables, Columns, Indexes, Constraints)
            self._check_schema_structure(report)

            # 3. Contract & Capability Verification
            self._check_contract_capabilities(report)

            # 4. State & Delivery Integrity Verification
            self._check_state_integrity(report)

        except Exception as exc:
            report.errors.append(f"Preflight validation encountered unexpected error: {type(exc).__name__}: {exc}")

        # Post-validation fingerprint check
        report.schema_fingerprint_after = self._compute_schema_fingerprint()

        # Success determination
        report.success = (
            len(report.errors) == 0
            and len(report.missing_tables) == 0
            and len(report.missing_columns) == 0
            and len(report.missing_indexes) == 0
            and len(report.missing_constraints) == 0
            and len(report.integrity_issues) == 0
            and report.schema_version >= CURRENT_SCHEMA_VERSION
            and report.is_non_mutating
        )

        return report

    def _check_schema_version(self, report: PreflightReport) -> None:
        try:
            ver = self.repo.get_schema_version()
            report.schema_version = ver
            if ver < CURRENT_SCHEMA_VERSION:
                report.errors.append(
                    f"Schema version regression or outdated: found {ver}, expected >={CURRENT_SCHEMA_VERSION}"
                )
        except Exception as exc:
            report.errors.append(f"Failed to retrieve schema version: {exc}")

    def _check_schema_structure(self, report: PreflightReport) -> None:
        # Retrieve all existing tables
        table_rows = execute_read_query(
            self.repo,
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )
        existing_tables: dict[str, str] = {
            r["name"]: (r.get("sql") or "") for r in table_rows if r.get("name")
        }

        # Check tables
        for table_name, expected_cols in SCHEMA_V6_TABLES.items():
            report.tables_checked += 1
            if table_name not in existing_tables:
                report.missing_tables.append(table_name)
                continue

            # Check columns via PRAGMA table_info
            try:
                col_rows = execute_read_query(self.repo, f"PRAGMA table_info({table_name})")
                actual_cols = {c["name"] for c in col_rows if c.get("name")}
                missing_for_table = [col for col in expected_cols if col not in actual_cols]
                report.columns_checked += len(expected_cols)
                if missing_for_table:
                    report.missing_columns[table_name] = missing_for_table
            except Exception as exc:
                report.errors.append(f"Failed inspecting columns for table {table_name}: {exc}")

        # Check indexes
        for table_name, expected_indexes in SCHEMA_V6_INDEXES.items():
            if table_name in report.missing_tables:
                continue
            try:
                idx_rows = execute_read_query(self.repo, f"PRAGMA index_list({table_name})")
                actual_indexes = {r["name"] for r in idx_rows if r.get("name")}
                for expected_idx in expected_indexes:
                    report.indexes_checked += 1
                    if expected_idx not in actual_indexes:
                        report.missing_indexes.append(expected_idx)
            except Exception as exc:
                report.errors.append(f"Failed inspecting indexes for table {table_name}: {exc}")

        # Check constraints
        for table_name, expected_constraints in SCHEMA_V6_CONSTRAINTS.items():
            if table_name in report.missing_tables:
                continue
            table_ddl = existing_tables.get(table_name, "").upper()
            try:
                idx_rows = execute_read_query(self.repo, f"PRAGMA index_list({table_name})")
                actual_indexes = {r["name"] for r in idx_rows if r.get("name")}
                for expected_constraint in expected_constraints:
                    report.constraints_checked += 1
                    # A unique constraint creates a unique index or is named in DDL
                    found_in_ddl = expected_constraint.upper() in table_ddl
                    found_in_indexes = any(
                        expected_constraint.lower() in idx_name.lower()
                        for idx_name in actual_indexes
                    )
                    if not (found_in_ddl or found_in_indexes):
                        report.missing_constraints.append(f"{table_name}.{expected_constraint}")
            except Exception as exc:
                report.errors.append(f"Failed inspecting constraints for table {table_name}: {exc}")

    def _check_contract_capabilities(self, report: PreflightReport) -> None:
        """Verify repository contract capabilities required for cloud staging."""
        required_methods = [
            "get_schema_version",
            "acquire_run_lock",
            "release_run_lock",
            "renew_run_lock",
            "save_recovery_state",
            "get_recovery_state",
            "clear_recovery_state",
            "claim_telegram_delivery",
            "record_telegram_delivery",
            "get_telegram_delivery",
            "update_telegram_delivery_status",
            "reconcile_uncertain_delivery",
            "record_budget_usage",
            "get_monthly_budget_summary",
            "record_scheduled_execution_start",
            "record_scheduled_execution_finish",
        ]
        for method_name in required_methods:
            if not hasattr(self.repo, method_name) or not callable(getattr(self.repo, method_name)):
                report.errors.append(f"Repository contract violation: missing required method '{method_name}'")

    def _check_state_integrity(self, report: PreflightReport) -> None:
        """Verify delivery and recovery state integrity without mutating any row."""
        # 1. Delivery records status validity
        valid_statuses = {s.value for s in DeliveryStatus}
        try:
            records = execute_read_query(
                self.repo,
                "SELECT DISTINCT status FROM telegram_delivery_records",
            )
            for r in records:
                st = r.get("status")
                if st and st not in valid_statuses:
                    report.integrity_issues.append(
                        f"Invalid delivery status in telegram_delivery_records: '{st}'"
                    )
        except Exception:
            # Table might be missing (already reported in structure check)
            pass

        # 2. Recovery state JSON validity
        try:
            states = execute_read_query(
                self.repo,
                "SELECT state_key, state_json FROM durable_recovery_state LIMIT 50",
            )
            for r in states:
                raw_json = r.get("state_json")
                if raw_json:
                    try:
                        json.loads(raw_json)
                    except Exception as json_err:
                        report.integrity_issues.append(
                            f"Corrupt recovery JSON in durable_recovery_state for key '{r.get('state_key')}': {json_err}"
                        )
        except Exception:
            pass

        # 3. Duplicate claims check on telegram_delivery_records
        try:
            duplicates = execute_read_query(
                self.repo,
                """
                SELECT recipient_chat_id, message_key, COUNT(*) as cnt
                FROM telegram_delivery_records
                GROUP BY recipient_chat_id, message_key
                HAVING COUNT(*) > 1
                """,
            )
            if duplicates:
                report.integrity_issues.append(
                    f"Found {len(duplicates)} duplicate delivery identities violating uniqueness contract"
                )
        except Exception:
            pass
