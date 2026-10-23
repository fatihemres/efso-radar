"""Read-only cloud preflight validator for Efso Radar staging environments.

Non-mutating verification of schema v6 tables, columns, indexes, constraints,
repository contract compatibility, persistent state capabilities, and data integrity
across SQLite and Turso (libSQL) backends.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field

from efso_radar.domain import DeliveryStatus
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.migration_ledger import mask_text_chat_ids
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

SCHEMA_V6_COLUMN_TYPES: dict[str, dict[str, str]] = {
    "schema_migrations": {"version": "INTEGER", "applied_at": "TEXT", "description": "TEXT"},
    "run_locks": {"lock_key": "TEXT", "worker_id": "TEXT", "acquired_at": "TEXT", "expires_at": "TEXT", "heartbeat_at": "TEXT"},
    "processed_articles": {"article_id": "TEXT", "title": "TEXT", "source": "TEXT", "published_at": "TEXT", "processed_at": "TEXT"},
    "stories": {"story_id": "TEXT", "headline_tr": "TEXT", "category": "TEXT", "first_seen_at": "TEXT", "last_updated_at": "TEXT", "article_ids_json": "TEXT", "update_count": "INTEGER", "is_active": "INTEGER"},
    "story_updates": {"update_id": "TEXT", "story_id": "TEXT", "development_tr": "TEXT", "detected_at": "TEXT", "article_ids_json": "TEXT", "significance_score": "INTEGER"},
    "notifications": {"notification_id": "TEXT", "story_id": "TEXT", "kind": "TEXT", "channel": "TEXT", "sent_at": "TEXT", "status": "TEXT", "message_text": "TEXT", "headline_tr": "TEXT", "score": "INTEGER"},
    "editorial_assessments": {"assessment_id": "TEXT", "story_id": "TEXT", "kind": "TEXT", "score": "INTEGER", "headline_tr": "TEXT", "created_at": "TEXT"},
    "owner_feedback": {"feedback_id": "TEXT", "rating": "TEXT", "notes": "TEXT", "created_at": "TEXT"},
    "candidate_backlog": {"article_id": "TEXT", "url": "TEXT", "title": "TEXT", "source": "TEXT", "market": "TEXT", "snippet": "TEXT", "published_at": "TEXT", "discovered_at": "TEXT", "status": "TEXT", "priority_score": "REAL"},
    "ai_usage_ledger": {"usage_id": "TEXT", "run_mode": "TEXT", "model_name": "TEXT", "batch_hash": "TEXT", "candidate_count": "INTEGER", "input_tokens": "INTEGER", "output_tokens": "INTEGER", "estimated_cost_usd": "REAL", "created_at": "TEXT"},
    "telegram_delivery_records": {
        "delivery_id": "TEXT", "message_key": "TEXT", "recipient_role": "TEXT", "recipient_chat_id": "TEXT",
        "category": "TEXT", "status": "TEXT", "attempt_count": "INTEGER", "max_attempts": "INTEGER",
        "payload_json": "TEXT", "created_at": "TEXT", "updated_at": "TEXT",
    },
    "durable_recovery_state": {"state_key": "TEXT", "stage": "TEXT", "worker_id": "TEXT", "state_json": "TEXT", "created_at": "TEXT", "updated_at": "TEXT"},
    "admin_operational_reports": {"report_date": "TEXT", "dispatched_at": "TEXT", "worker_id": "TEXT", "status": "TEXT", "recipient_chat_id": "TEXT", "summary_json": "TEXT"},
    "experiment_budget_history": {"record_id": "TEXT", "period_month": "TEXT", "run_id": "TEXT", "worker_id": "TEXT", "timestamp": "TEXT", "calls_count": "INTEGER", "prompt_tokens": "INTEGER", "completion_tokens": "INTEGER", "total_tokens": "INTEGER", "estimated_cost_usd": "REAL"},
    "scheduled_execution_history": {"run_id": "TEXT", "run_type": "TEXT", "worker_id": "TEXT", "started_at": "TEXT", "status": "TEXT", "articles_collected": "INTEGER", "candidates_evaluated": "INTEGER", "notifications_dispatched": "INTEGER"},
}

SCHEMA_V6_CONSTRAINTS: dict[str, list[str]] = {}

SCHEMA_V6_CRITICAL_UNIQUE_CONSTRAINTS: dict[str, list[set[str]]] = {
    "telegram_delivery_records": [{"recipient_chat_id", "message_key"}],
}


def _types_are_compatible(actual: str, expected: str) -> bool:
    """Check if actual SQL column type is compatible with expected type."""
    act = actual.strip().upper()
    exp = expected.strip().upper()
    if exp == "TEXT":
        return act in ("TEXT", "VARCHAR", "CHAR", "CLOB", "")
    elif exp == "INTEGER":
        return act in ("INTEGER", "INT", "TINYINT", "SMALLINT", "BIGINT")
    elif exp == "REAL":
        return act in ("REAL", "FLOAT", "DOUBLE", "NUMERIC")
    return act == exp


def validate_read_only_sql(sql: str) -> None:
    """Validate that SQL string is strictly read-only and safe to execute.

    Rejects:
    - Empty queries
    - Multiple statements (semicolon separation)
    - Mutating PRAGMAs or PRAGMAs outside allowed read-only inspection set
    - CTEs containing data-modifying statements (INSERT, UPDATE, DELETE, etc.)
    - Any query containing mutating DML or DDL keywords anywhere in the statement
    """
    if not sql or not sql.strip():
        raise ValueError("Preflight query safety violation: SQL query is empty.")

    # Comments are never needed for introspection and can be used to hide payloads
    # from naive scanners (e.g. a quote inside a comment desyncing literal stripping).
    if "--" in sql or "/*" in sql or "*/" in sql:
        raise ValueError("Preflight query safety violation: SQL comments are prohibited.")
    cleaned = sql.strip()

    # Reject multiple statements
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    if ";" in cleaned:
        raise ValueError("Preflight query safety violation: multiple statements are prohibited.")

    # Strip string literals to avoid false positives for keywords inside strings
    no_strings = re.sub(r"'(''|[^'])*'", "'STRING'", cleaned)
    no_strings = re.sub(r'"(""|[^"])*"', '"STRING"', no_strings)
    upper = no_strings.upper()

    # Check allowed starting verbs
    if upper.startswith("PRAGMA"):
        # Strict allowlist of exact read-only forms. Anything else (assignment, `name(value)`
        # setter syntax such as `PRAGMA user_version(17)`, schema-qualified names) is rejected.
        # Introspection pragmas that take an object name: PRAGMA name(identifier)
        arg_pragmas = {"table_info", "table_xinfo", "index_list", "index_info", "index_xinfo", "foreign_key_list"}
        # Introspection pragmas that must be bare (no argument, no assignment)
        bare_pragmas = {"collation_list", "database_list", "compile_options", "user_version"}
        m_arg = re.fullmatch(r"PRAGMA\s+([A-Z_]+)\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)", upper, flags=re.IGNORECASE)
        m_bare = re.fullmatch(r"PRAGMA\s+([A-Z_]+)", upper, flags=re.IGNORECASE)
        if m_arg and m_arg.group(1).lower() in arg_pragmas:
            return
        if m_bare and m_bare.group(1).lower() in bare_pragmas:
            return
        raise ValueError(
            f"Preflight query safety violation: PRAGMA is not an allowed read-only form: {sql[:40]}"
        )

    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError(
            f"Preflight query safety violation: query must start with SELECT, WITH, or allowed PRAGMA: {sql[:40]}"
        )

    # Strictly prohibit mutating verbs anywhere in the statement (e.g. in CTEs or subqueries)
    prohibited_verbs = {
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "REPLACE",
        "ATTACH", "DETACH", "VACUUM", "REINDEX", "UPSERT", "SAVEPOINT",
        "RELEASE", "GRANT", "REVOKE", "PRAGMA", "LOAD_EXTENSION", "BEGIN", "COMMIT",
        "ROLLBACK", "ANALYZE",
    }
    words = set(re.findall(r"\b[A-Za-z_]+\b", upper))
    found_prohibited = words.intersection(prohibited_verbs)
    if found_prohibited:
        raise ValueError(
            f"Preflight query safety violation: query contains prohibited mutating keywords "
            f"{sorted(found_prohibited)}: {sql[:50]}"
        )


def execute_read_query(repo: StateRepository, sql: str, params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
    """Execute a strictly read-only SQL query against SQLite or Turso repository.

    Guarantees:
    - Queries must pass strict read-only validation (no DML, no DDL, no CTE writes, no mutating PRAGMAs).
    - Refuses execution against the operational database data/radar.db.
    - Uses genuinely read-only file connections (?mode=ro) and PRAGMA query_only = ON when inspecting SQLite files.
    - Never creates or initializes missing database files; fails closed if the file is missing, invalid, or inaccessible.
    """
    validate_read_only_sql(sql)

    if isinstance(repo, SqliteStateRepository):
        db_path = getattr(repo, "db_path", None)
        if db_path is None or not str(db_path).strip() or str(db_path).strip() == ":memory:":
            raise FileNotFoundError(
                "Preflight read-only query failed: SQLite repository does not have a valid existing database file path."
            )
        resolved = Path(db_path).resolve()
        prod_path = Path("data/radar.db").resolve()
        if resolved == prod_path or (resolved.name == "radar.db" and "data" in resolved.parts):
            raise PermissionError(
                "Safety violation: execute_read_query cannot execute against operational database data/radar.db."
            )
        if not resolved.exists() or not resolved.is_file():
            raise FileNotFoundError(
                f"Preflight read-only query failed: SQLite database file does not exist or is not a regular file: {resolved.name}"
            )

        uri_path = f"file:{resolved.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri_path, uri=True, timeout=15.0)
        try:
            conn.execute("PRAGMA query_only = ON;")
            cursor = conn.cursor()
            cursor.execute(sql, params or [])
            cols = [d[0] for d in cursor.description] if cursor.description else []
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        finally:
            conn.close()

    elif isinstance(repo, TursoStateRepository):
        res = repo.client.execute(sql, params or [])
        return res.fetchall()
    else:
        raise TypeError(f"Unsupported repository type for preflight inspection: {type(repo)}")


class PreflightReport(BaseModel):
    """Structured report of cloud staging preflight validation.

    NOTE ON NON-MUTATING INVARIANT:
    The schema fingerprint check computes an MD5 digest of all schema definitions
    (tables, indexes, triggers, views) in sqlite_master before and after preflight.
    This guarantees that database schema objects were strictly unmutated by the inspection.
    Row content integrity and data preservation are separately verified via cryptographic
    content hashing in the staging cutover reconciler.
    """
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
    type_mismatches: dict[str, dict[str, str]] = Field(default_factory=dict)
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
            f"Non-Mutating Schema Invariant Verified: {'YES' if self.is_non_mutating else 'NO'}",
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
        if self.type_mismatches:
            lines.append("Column Type Mismatches:")
            for t, mismatches in self.type_mismatches.items():
                for col, detail in mismatches.items():
                    lines.append(f"  - {t}.{col}: {detail}")
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
                if resolved == prod_path or (resolved.name == "radar.db" and "data" in resolved.parts):
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
            logger.warning("Could not compute schema fingerprint: %s", type(exc).__name__)
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
            report.errors.append(f"Preflight validation encountered unexpected error: {type(exc).__name__}")

        # Post-validation fingerprint check
        report.schema_fingerprint_after = self._compute_schema_fingerprint()

        # Success determination
        report.success = (
            len(report.errors) == 0
            and len(report.missing_tables) == 0
            and len(report.missing_columns) == 0
            and len(report.type_mismatches) == 0
            and len(report.missing_indexes) == 0
            and len(report.missing_constraints) == 0
            and len(report.integrity_issues) == 0
            and report.schema_version >= CURRENT_SCHEMA_VERSION
            and report.is_non_mutating
        )

        return report

    def _check_schema_version(self, report: PreflightReport) -> None:
        try:
            rows = execute_read_query(self.repo, "SELECT MAX(version) AS ver FROM schema_migrations")
            ver = int(rows[0]["ver"]) if rows and rows[0].get("ver") is not None else 0
            report.schema_version = ver
            if ver < CURRENT_SCHEMA_VERSION:
                report.errors.append(
                    f"Schema version regression or outdated: found {ver}, expected >={CURRENT_SCHEMA_VERSION}"
                )
        except Exception as exc:
            report.errors.append(f"Failed to retrieve schema version: {type(exc).__name__}")

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

            # Check columns and critical types via PRAGMA table_info
            try:
                col_rows = execute_read_query(self.repo, f"PRAGMA table_info({table_name})")
                actual_cols_map = {c["name"]: (c.get("type") or "") for c in col_rows if c.get("name")}
                missing_for_table = [col for col in expected_cols if col not in actual_cols_map]
                report.columns_checked += len(expected_cols)
                if missing_for_table:
                    report.missing_columns[table_name] = missing_for_table

                # Check critical column types
                expected_type_map = SCHEMA_V6_COLUMN_TYPES.get(table_name, {})
                for col_name, exp_type in expected_type_map.items():
                    if col_name in actual_cols_map:
                        act_type = actual_cols_map[col_name]
                        if not _types_are_compatible(act_type, exp_type):
                            if table_name not in report.type_mismatches:
                                report.type_mismatches[table_name] = {}
                            report.type_mismatches[table_name][col_name] = (
                                f"Incompatible type: expected {exp_type}, got {act_type}"
                            )
                            report.errors.append(
                                f"Column type mismatch on {table_name}.{col_name}: expected {exp_type}, got {act_type}"
                            )
            except Exception as exc:
                report.errors.append(f"Failed inspecting columns for table {table_name}: {type(exc).__name__}")

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
                report.errors.append(f"Failed inspecting indexes for table {table_name}: {type(exc).__name__}")

        # Check named constraints
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
                report.errors.append(f"Failed inspecting constraints for table {table_name}: {type(exc).__name__}")

        # Verify critical uniqueness semantics (e.g. UNIQUE(recipient_chat_id, message_key)) strictly from
        # schema and index metadata. DDL text and constraint/index names are never used as proof: a real,
        # non-partial UNIQUE index whose key columns are exactly the required set must exist, and every
        # identity column in that set must enforce NOT NULL (since SQLite UNIQUE allows duplicate NULLs).
        # Any missing/unsupported metadata fails closed.
        for table_name, critical_unique_sets in SCHEMA_V6_CRITICAL_UNIQUE_CONSTRAINTS.items():
            if table_name in report.missing_tables:
                continue
            cols_strs = [", ".join(sorted(s)) for s in critical_unique_sets]
            try:
                col_info_rows = execute_read_query(self.repo, f"PRAGMA table_info({table_name})")
                if not col_info_rows:
                    raise ValueError("table_info metadata returned no columns")
                notnull_cols: set[str] = set()
                for c_row in col_info_rows:
                    if "name" not in c_row or "notnull" not in c_row or not c_row.get("name"):
                        raise ValueError("table_info metadata is missing required fields")
                    if int(c_row["notnull"]) == 1:
                        notnull_cols.add(str(c_row["name"]))

                idx_rows = execute_read_query(self.repo, f"PRAGMA index_list({table_name})")
                unique_index_cols: list[frozenset[str]] = []
                for idx in idx_rows:
                    if "unique" not in idx or "partial" not in idx or not idx.get("name"):
                        raise ValueError("index_list metadata is missing required fields")
                    if int(idx["unique"]) != 1:
                        continue
                    if int(idx["partial"]) != 0:
                        continue  # partial unique index does not enforce table-wide uniqueness
                    info_rows = execute_read_query(self.repo, f"PRAGMA index_xinfo({idx['name']})")
                    key_rows = []
                    for r in info_rows:
                        if "key" not in r or "name" not in r or "cid" not in r:
                            raise ValueError("index_xinfo metadata is missing required fields")
                        if int(r["key"]) == 1:
                            key_rows.append(r)
                    # Expression (cid -2) or rowid (cid -1) key parts have no real column name
                    if not key_rows or any(r["name"] is None or int(r["cid"]) < 0 for r in key_rows):
                        continue
                    names = [r["name"] for r in key_rows]
                    if len(set(names)) != len(names):
                        continue
                    unique_index_cols.append(frozenset(names))

                for expected_cols_set, cols_str in zip(critical_unique_sets, cols_strs):
                    report.constraints_checked += 1
                    has_unique_index = frozenset(expected_cols_set) in unique_index_cols
                    nullable_cols = sorted(col for col in expected_cols_set if col not in notnull_cols)
                    if not has_unique_index or nullable_cols:
                        report.missing_constraints.append(f"{table_name}.UNIQUE({cols_str})")
                        if not has_unique_index:
                            report.errors.append(
                                f"Uniqueness constraint missing on {table_name} for ({cols_str})"
                            )
                        if nullable_cols:
                            report.errors.append(
                                f"Uniqueness identity columns on {table_name} must enforce NOT NULL: "
                                f"{', '.join(nullable_cols)}"
                            )
            except Exception as exc:
                # Only the exception class is reported; metadata failures must fail the preflight.
                report.errors.append(
                    f"Failed inspecting uniqueness semantics for table {table_name}: {type(exc).__name__}"
                )
                for cols_str in cols_strs:
                    report.missing_constraints.append(f"{table_name}.UNIQUE({cols_str})")

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

    def _table_exists(self, table_name: str) -> bool:
        rows = execute_read_query(
            self.repo,
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            [table_name],
        )
        return bool(rows)

    def _check_state_integrity(self, report: PreflightReport) -> None:
        """Verify delivery and recovery state integrity without mutating any row.

        Diagnostics contain only safe categories and counts, never row values or raw exception text.
        Genuinely missing tables are skipped (already reported by the structure check); any other
        query failure is reported as an error so preflight fails closed.
        """
        valid_statuses = {s.value for s in DeliveryStatus}

        def _guarded(table: str, label: str, fn) -> None:
            try:
                if not self._table_exists(table):
                    return
                fn()
            except Exception as exc:
                report.errors.append(
                    f"State integrity check failed ({label}): query error {type(exc).__name__}"
                )

        # 1. Delivery records status validity
        def _check_statuses() -> None:
            records = execute_read_query(
                self.repo,
                "SELECT DISTINCT status FROM telegram_delivery_records",
            )
            invalid = sum(1 for r in records if r.get("status") not in valid_statuses)
            if invalid:
                report.integrity_issues.append(
                    f"Invalid delivery status: found {invalid} distinct invalid value(s) in telegram_delivery_records"
                )

        _guarded("telegram_delivery_records", "delivery status", _check_statuses)

        # 2. Recovery state JSON validity
        def _check_recovery_json() -> None:
            states = execute_read_query(
                self.repo,
                "SELECT state_json FROM durable_recovery_state",
            )
            corrupt = 0
            for r in states:
                raw_json = r.get("state_json")
                if not isinstance(raw_json, str) or not raw_json.strip():
                    corrupt += 1
                    continue
                try:
                    json.loads(raw_json)
                except (ValueError, TypeError):
                    corrupt += 1
            if corrupt:
                report.integrity_issues.append(
                    f"Corrupt recovery JSON: found {corrupt} record(s) in durable_recovery_state"
                )

        _guarded("durable_recovery_state", "recovery state JSON", _check_recovery_json)

        # 3. Duplicate claims check on telegram_delivery_records
        def _check_duplicates() -> None:
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

        _guarded("telegram_delivery_records", "duplicate delivery identities", _check_duplicates)