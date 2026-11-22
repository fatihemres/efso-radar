"""Hermetic offline tests for B1 Schema Phase Gate and Mock-Only Operator Handoff (Issue #99).

Covers:
1. Positive canonical Schema v6 (15 tables, 129 columns, 15 explicit indexes) and
   candidate staging_run_journal fixture classifications (in-memory & OS temp).
2. Adversarial schema tampering (migrations ledger, missing/extra/renamed tables,
   indexes, unique constraints, foreign keys, columns, hidden/generated columns,
   forbidden DDL clauses, views, triggers, virtual tables, temp shadows, attached DBs,
   and malformed/partial candidate journal artifacts).
3. Engine-level read-only proof (set_authorizer, trace_callback, fingerprints,
   total_changes, row_factory), strict temp/in-memory isolation without path leakage,
   sanitized errors/summaries, and inert Pydantic strict model protections.
4. Regression checks proving existing LIVE-01 / CloudPreflightValidator /
   LiveReadyStagingOperator / StagingJournalHranaSeam boundaries remain intact.
"""
from __future__ import annotations

import ast
from contextlib import closing
import io
from pathlib import Path
import sqlite3
import tempfile
import traceback
from typing import Any
import unittest
from unittest.mock import patch

import httpx
from pydantic import ValidationError

from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION, MIGRATIONS, get_all_init_statements
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso_client import TursoHTTPClient
from efso_radar.staging import schema_phase_gate
from efso_radar.staging.journal import apply_candidate_schema_v7
from efso_radar.staging.journal_hrana_seam import StagingJournalHranaSeam
from efso_radar.staging.live01_readonly import Live01Report, execute_live01_readonly
from efso_radar.staging.live_ready_operator import (
    LiveReadyStagingOperator,
    main as live_ready_operator_main,
)
from efso_radar.staging.preflight import CloudPreflightValidator, PreflightReport
from efso_radar.staging.runner import (
    EXACT_STAGING_DESTINATION,
    BudgetTrackingTransport,
    HermeticStagingTransport,
)
from efso_radar.staging.schema_phase_gate import (
    ManualNextGate,
    MockOperatorHandoff,
    MockTestEligibility,
    PhaseGateReasonCode,
    SchemaPhase,
    SchemaPhaseClassification,
    build_mock_operator_handoff,
    classify_offline_schema_phase,
    evaluate_offline_schema_phase_gate,
)


_ALLOWLISTED_READONLY_PRAGMAS = frozenset({
    "database_list",
    "data_version",
    "table_xinfo",
    "table_info",
    "index_list",
    "index_xinfo",
    "foreign_key_list",
})


def _make_canonical_v6_conn(*, full_history: bool = False) -> sqlite3.Connection:
    """Create a canonical in-memory Schema v6 SQLite fixture."""
    conn = sqlite3.connect(":memory:")
    for stmt in get_all_init_statements():
        conn.execute(stmt)
    if full_history:
        for step in MIGRATIONS:
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at, description) VALUES (?, ?, ?)",
                (step.version, f"2026-10-25T15:0{step.version}:00Z", step.description),
            )
    else:
        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at, description) VALUES (?, ?, ?)",
            (6, "2026-10-27T14:00:00Z", "Schema v6"),
        )
    conn.commit()
    return conn


class TestCanonicalV6AndCandidateJournalPositiveControls(unittest.TestCase):
    def test_canonical_v6_in_memory_fixture_classifies_v6_baseline_and_handoff(self):
        with closing(_make_canonical_v6_conn()) as conn:
            classification = classify_offline_schema_phase(conn)
            self.assertEqual(classification.phase, SchemaPhase.V6_BASELINE_OFFLINE)
            self.assertEqual(classification.evidence_provenance, "OFFLINE_FIXTURE_ONLY")
            self.assertEqual(classification.operational_schema_version_contract, 6)
            self.assertEqual(classification.observed_schema_version, 6)
            self.assertEqual(classification.expected_v6_table_count, 15)
            self.assertEqual(classification.observed_v6_tables_matched, 15)
            self.assertEqual(classification.expected_v6_column_count, 129)
            self.assertEqual(classification.observed_v6_columns_matched, 129)
            self.assertEqual(classification.expected_v6_explicit_index_count, 15)
            self.assertEqual(classification.observed_v6_explicit_indexes_matched, 15)
            self.assertFalse(classification.candidate_journal_present)
            self.assertFalse(classification.candidate_journal_valid)
            self.assertFalse(classification.candidate_journal_is_approved_v7_migration)
            self.assertTrue(classification.non_mutating_verified)
            self.assertEqual(classification.reason_codes, ())

            handoff = evaluate_offline_schema_phase_gate(conn)
            self.assertEqual(handoff.phase, SchemaPhase.V6_BASELINE_OFFLINE)
            self.assertEqual(handoff.evidence_provenance, "OFFLINE_FIXTURE_ONLY")
            self.assertEqual(handoff.authorization_scope, "NONE_OFFLINE_MOCK_ONLY")
            self.assertEqual(handoff.remote_state, "UNKNOWN")
            self.assertEqual(handoff.schema_v7_adoption_status, "NOT_APPROVED_NOT_MIGRATED")
            self.assertEqual(
                handoff.automation_policy,
                "NO_AUTO_RECOVERY_NO_AUTO_CLEANUP_NO_AUTO_RETRY",
            )
            self.assertEqual(
                handoff.mock_test_eligibility,
                MockTestEligibility.LEGACY_V6_MOCK_LIVE01_AND_RUNNER_ONLY,
            )
            self.assertTrue(handoff.eligible_for_mock_v6_live01)
            self.assertFalse(handoff.eligible_for_mock_journal_seam)
            self.assertEqual(
                handoff.manual_next_gate,
                ManualNextGate.SEPARATE_PO_B2_COST_AND_B3_LIVE_READ_GO_REQUIRED,
            )
            self.assertEqual(handoff.observed_schema_version, 6)
            self.assertTrue(handoff.non_mutating_verified)
            self.assertEqual(handoff.reason_codes, ())
            self.assertNotIn("LIVE_READY", handoff.summary())
            self.assertIn("phase=V6_BASELINE_OFFLINE", handoff.summary())

    def test_canonical_v6_temp_sqlite_repository_fixture_classifies_v6_baseline(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "canonical_v6_repo.db"
            repo = SqliteStateRepository.create_owned_synthetic_fixture(db_path)
            repo.close()
            with closing(sqlite3.connect(str(db_path))) as conn:
                handoff = evaluate_offline_schema_phase_gate(conn)
            self.assertEqual(handoff.phase, SchemaPhase.V6_BASELINE_OFFLINE)
            self.assertTrue(handoff.eligible_for_mock_v6_live01)
            self.assertFalse(handoff.eligible_for_mock_journal_seam)
            self.assertTrue(handoff.non_mutating_verified)
            self.assertEqual(handoff.reason_codes, ())

    def test_canonical_v6_full_sequential_migrations_1_to_6_classifies_v6_baseline(self):
        with closing(_make_canonical_v6_conn(full_history=True)) as conn:
            classification = classify_offline_schema_phase(conn)
            self.assertEqual(classification.phase, SchemaPhase.V6_BASELINE_OFFLINE)
            self.assertEqual(classification.observed_schema_version, 6)
            self.assertEqual(classification.reason_codes, ())

    def test_candidate_journal_in_memory_and_temp_file_classify_candidate_journal_offline(self):
        with closing(_make_canonical_v6_conn()) as mem_conn:
            apply_candidate_schema_v7(mem_conn)
            cl = classify_offline_schema_phase(mem_conn)
            self.assertEqual(cl.phase, SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE)
            self.assertEqual(cl.observed_schema_version, 6)
            self.assertEqual(cl.observed_v6_tables_matched, 15)
            self.assertEqual(cl.observed_v6_columns_matched, 129)
            self.assertEqual(cl.observed_v6_explicit_indexes_matched, 15)
            self.assertTrue(cl.candidate_journal_present)
            self.assertTrue(cl.candidate_journal_valid)
            self.assertFalse(cl.candidate_journal_is_approved_v7_migration)
            self.assertTrue(cl.non_mutating_verified)
            self.assertEqual(cl.reason_codes, ())

            handoff = evaluate_offline_schema_phase_gate(mem_conn)
            self.assertEqual(handoff.phase, SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE)
            self.assertEqual(handoff.schema_v7_adoption_status, "NOT_APPROVED_NOT_MIGRATED")
            self.assertEqual(
                handoff.mock_test_eligibility,
                MockTestEligibility.CANDIDATE_JOURNAL_MOCK_SEAM_ONLY,
            )
            self.assertFalse(handoff.eligible_for_mock_v6_live01)
            self.assertTrue(handoff.eligible_for_mock_journal_seam)
            self.assertEqual(
                handoff.manual_next_gate,
                ManualNextGate.SEPARATE_PO_REVIEW_AND_FUTURE_B4_MIGRATION_GO_REQUIRED,
            )
            self.assertNotIn("LIVE_READY", handoff.summary())

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
            db_path = Path(td) / "candidate_journal_v6.db"
            repo = SqliteStateRepository.create_owned_synthetic_fixture(db_path)
            repo.close()
            with closing(sqlite3.connect(str(db_path))) as file_conn:
                apply_candidate_schema_v7(file_conn)
                file_conn.commit()
                handoff = evaluate_offline_schema_phase_gate(file_conn)
            self.assertEqual(handoff.phase, SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE)
            self.assertTrue(handoff.eligible_for_mock_journal_seam)
            self.assertEqual(handoff.observed_schema_version, 6)

    def test_strict_v6_only_mode_rejects_candidate_journal_present(self):
        with closing(_make_canonical_v6_conn()) as conn:
            apply_candidate_schema_v7(conn)
            cl = classify_offline_schema_phase(conn, allow_candidate_journal=False)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertTrue(cl.candidate_journal_present)
            self.assertTrue(cl.candidate_journal_valid)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_JOURNAL_PRESENT, cl.reason_codes)

            handoff = evaluate_offline_schema_phase_gate(conn, allow_candidate_journal=False)
            self.assertEqual(handoff.phase, SchemaPhase.DENY)
            self.assertFalse(handoff.eligible_for_mock_v6_live01)
            self.assertFalse(handoff.eligible_for_mock_journal_seam)
            self.assertEqual(handoff.mock_test_eligibility, MockTestEligibility.NONE_DENIED)


class TestAdversarialSchemaPhaseTampering(unittest.TestCase):
    def test_schema_migrations_tampering_cases(self):
        # 1. Missing schema_migrations table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE schema_migrations")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.MISSING_V6_TABLE, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.SCHEMA_MIGRATIONS_TABLE_MISSING, cl.reason_codes)

        # 2. Empty schema_migrations
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DELETE FROM schema_migrations")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.SCHEMA_VERSION_MISSING, cl.reason_codes)

        # 3. Outdated version 5 only
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("UPDATE schema_migrations SET version = 5 WHERE version = 6")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertEqual(cl.observed_schema_version, 5)
            self.assertIn(PhaseGateReasonCode.SCHEMA_VERSION_OUTDATED, cl.reason_codes)

        # 4. Future version 7 without journal
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("UPDATE schema_migrations SET version = 7 WHERE version = 6")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertEqual(cl.observed_schema_version, 7)
            self.assertIn(PhaseGateReasonCode.UNAPPROVED_FUTURE_SCHEMA_VERSION, cl.reason_codes)

        # 5. Future version 7 WITH canonical candidate journal -> still DENY
        with closing(_make_canonical_v6_conn()) as conn:
            apply_candidate_schema_v7(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at, description) "
                "VALUES (7, '2026-11-22T00:00:00Z', 'Unapproved v7')"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertEqual(cl.observed_schema_version, 7)
            self.assertTrue(cl.candidate_journal_present)
            self.assertTrue(cl.candidate_journal_valid)
            self.assertIn(PhaseGateReasonCode.UNAPPROVED_FUTURE_SCHEMA_VERSION, cl.reason_codes)

        # 6. Gapped migration history (1, 4, 6)
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at, description) VALUES "
                "(1, '2026-10-01T00:00:00Z', 'v1'), (4, '2026-10-04T00:00:00Z', 'v4')"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_SCHEMA_MIGRATION_HISTORY, cl.reason_codes)

        # 7. Version below 1 (0, 6)
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at, description) "
                "VALUES (0, '2026-10-01T00:00:00Z', 'v0')"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_SCHEMA_MIGRATION_HISTORY, cl.reason_codes)

        # 8. Whitespace applied_at / description
        for field_name in ("applied_at", "description"):
            with self.subTest(field_name=field_name):
                with closing(_make_canonical_v6_conn()) as conn:
                    conn.execute(f"UPDATE schema_migrations SET {field_name} = '   ' WHERE version = 6")
                    cl = classify_offline_schema_phase(conn)
                    self.assertEqual(cl.phase, SchemaPhase.DENY)
                    self.assertIn(PhaseGateReasonCode.MALFORMED_SCHEMA_MIGRATION_ROW, cl.reason_codes)

    def test_table_inventory_tampering_missing_extra_and_renamed_15_tables(self):
        # Missing canonical table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE run_locks")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertEqual(cl.observed_v6_tables_matched, 14)
            self.assertIn(PhaseGateReasonCode.MISSING_V6_TABLE, cl.reason_codes)

        # Extra foreign table (16 tables)
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE TABLE rogue_table (id TEXT PRIMARY KEY)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_TABLE_OBJECT, cl.reason_codes)

        # Renamed table keeping exact 15-table count
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE owner_feedback")
            conn.execute("CREATE TABLE owner_feedback_renamed (feedback_id TEXT PRIMARY KEY)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.MISSING_V6_TABLE, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_TABLE_OBJECT, cl.reason_codes)

    def test_index_tampering_missing_extra_renamed_partial_unique_desc_collation_expression(self):
        # Missing canonical indexes (including index not in preflight SCHEMA_V6_INDEXES)
        for idx_name in (
            "idx_stories_last_updated",
            "idx_owner_feedback_rating",
            "idx_notifications_story_id",
        ):
            with self.subTest(missing_idx=idx_name):
                with closing(_make_canonical_v6_conn()) as conn:
                    conn.execute(f"DROP INDEX {idx_name}")
                    cl = classify_offline_schema_phase(conn)
                    self.assertEqual(cl.phase, SchemaPhase.DENY)
                    self.assertEqual(cl.observed_v6_explicit_indexes_matched, 14)
                    self.assertIn(PhaseGateReasonCode.MISSING_V6_INDEX, cl.reason_codes)

        # Extra index on canonical table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE INDEX idx_extra_story_cat ON stories (category)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_INDEX_OBJECT, cl.reason_codes)

        # Renamed index with identical columns
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP INDEX idx_stories_last_updated")
            conn.execute("CREATE INDEX idx_stories_last_updated_renamed ON stories (last_updated_at)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.MISSING_V6_INDEX, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_INDEX_OBJECT, cl.reason_codes)

        # Partial, UNIQUE, DESC, NOCASE collation, and expression replacements for canonical index
        variants = [
            "CREATE INDEX idx_stories_last_updated ON stories (last_updated_at) WHERE is_active = 1",
            "CREATE UNIQUE INDEX idx_stories_last_updated ON stories (last_updated_at)",
            "CREATE INDEX idx_stories_last_updated ON stories (last_updated_at DESC)",
            "CREATE INDEX idx_stories_last_updated ON stories (last_updated_at COLLATE NOCASE)",
            "CREATE INDEX idx_stories_last_updated ON stories (LOWER(last_updated_at))",
        ]
        for ddl in variants:
            with self.subTest(ddl=ddl):
                with closing(_make_canonical_v6_conn()) as conn:
                    conn.execute("DROP INDEX idx_stories_last_updated")
                    conn.execute(ddl)
                    cl = classify_offline_schema_phase(conn)
                    self.assertEqual(cl.phase, SchemaPhase.DENY)
                    self.assertTrue(
                        PhaseGateReasonCode.V6_INDEX_METADATA_MISMATCH in cl.reason_codes
                        or PhaseGateReasonCode.V6_INDEX_DDL_MISMATCH in cl.reason_codes
                    )

    def test_unique_constraint_and_foreign_key_tampering(self):
        # 1. Missing UNIQUE(recipient_chat_id, message_key) on telegram_delivery_records
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE telegram_delivery_records")
            conn.execute(
                """
                CREATE TABLE telegram_delivery_records (
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
                    sent_at TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX idx_telegram_delivery_records_status ON telegram_delivery_records (status, updated_at)"
            )
            conn.execute(
                "CREATE INDEX idx_telegram_delivery_records_role ON telegram_delivery_records (recipient_role, created_at)"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_UNIQUE_CONSTRAINT_MISMATCH, cl.reason_codes)

        # 2. Nullable recipient_chat_id with UNIQUE constraint
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE telegram_delivery_records")
            conn.execute(
                """
                CREATE TABLE telegram_delivery_records (
                    delivery_id TEXT PRIMARY KEY,
                    message_key TEXT NOT NULL,
                    recipient_role TEXT NOT NULL,
                    recipient_chat_id TEXT,
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
                )
                """
            )
            conn.execute(
                "CREATE INDEX idx_telegram_delivery_records_status ON telegram_delivery_records (status, updated_at)"
            )
            conn.execute(
                "CREATE INDEX idx_telegram_delivery_records_role ON telegram_delivery_records (recipient_role, created_at)"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_UNIQUE_CONSTRAINT_MISMATCH, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.V6_COLUMN_METADATA_MISMATCH, cl.reason_codes)

        # 3. Dropped FK on story_updates
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE story_updates")
            conn.execute(
                """
                CREATE TABLE story_updates (
                    update_id TEXT PRIMARY KEY,
                    story_id TEXT NOT NULL,
                    development_tr TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    article_ids_json TEXT NOT NULL,
                    significance_score INTEGER NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX idx_story_updates_story_id ON story_updates (story_id)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_FOREIGN_KEY_MISMATCH, cl.reason_codes)

        # 4. Unexpected extra FK on admin_operational_reports
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE admin_operational_reports")
            conn.execute(
                """
                CREATE TABLE admin_operational_reports (
                    report_date TEXT PRIMARY KEY,
                    dispatched_at TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    recipient_chat_id TEXT NOT NULL,
                    summary_json TEXT NOT NULL,
                    FOREIGN KEY(worker_id) REFERENCES run_locks(lock_key)
                )
                """
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_FOREIGN_KEY_MISMATCH, cl.reason_codes)

    def test_column_metadata_and_forbidden_ddl_clauses(self):
        # Extra column
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("ALTER TABLE run_locks ADD COLUMN extra_col TEXT")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_COLUMN_COUNT_MISMATCH, cl.reason_codes)

        # Hidden / generated virtual column
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE run_locks")
            conn.execute(
                """
                CREATE TABLE run_locks (
                    lock_key TEXT PRIMARY KEY,
                    worker_id TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    gen_col TEXT GENERATED ALWAYS AS ('x') VIRTUAL
                )
                """
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_COLUMN_COUNT_MISMATCH, cl.reason_codes)

        # Swapped columns + wrong type + dropped NOT NULL + wrong DEFAULT
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE run_locks")
            conn.execute(
                """
                CREATE TABLE run_locks (
                    lock_key TEXT PRIMARY KEY,
                    acquired_at INTEGER DEFAULT 'x',
                    worker_id TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL
                )
                """
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_COLUMN_METADATA_MISMATCH, cl.reason_codes)

        # Forbidden DDL clauses: CHECK, ON CONFLICT, WITHOUT ROWID, AUTOINCREMENT
        bad_run_locks_ddls = [
            (
                "CREATE TABLE run_locks (lock_key TEXT PRIMARY KEY, worker_id TEXT NOT NULL CHECK(length(worker_id) > 0), "
                "acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL)",
                PhaseGateReasonCode.V6_TABLE_DDL_MISMATCH,
            ),
            (
                "CREATE TABLE run_locks (lock_key TEXT PRIMARY KEY ON CONFLICT IGNORE, worker_id TEXT NOT NULL, "
                "acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL)",
                PhaseGateReasonCode.V6_TABLE_DDL_MISMATCH,
            ),
            (
                "CREATE TABLE run_locks (lock_key TEXT PRIMARY KEY, worker_id TEXT NOT NULL, "
                "acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL) WITHOUT ROWID",
                PhaseGateReasonCode.V6_TABLE_DDL_MISMATCH,
            ),
        ]
        for ddl, expected_code in bad_run_locks_ddls:
            with self.subTest(ddl=ddl):
                with closing(_make_canonical_v6_conn()) as conn:
                    conn.execute("DROP TABLE run_locks")
                    conn.execute(ddl)
                    cl = classify_offline_schema_phase(conn)
                    self.assertEqual(cl.phase, SchemaPhase.DENY)
                    self.assertIn(expected_code, cl.reason_codes)

        # AUTOINCREMENT creates sqlite_sequence internal table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE schema_migrations")
            conn.execute(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY AUTOINCREMENT,
                    applied_at TEXT NOT NULL,
                    description TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at, description) VALUES (6, '2026-10-27T14:00:00Z', 'Schema v6')"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.V6_TABLE_DDL_MISMATCH, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_INTERNAL_SCHEMA_OBJECT, cl.reason_codes)

    def test_views_triggers_and_virtual_tables_rejected(self):
        # Spoofed view impersonating canonical v6 table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE run_locks")
            conn.execute("CREATE VIEW run_locks AS SELECT 'k' AS lock_key")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_VIEW_OBJECT, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.MISSING_V6_TABLE, cl.reason_codes)

        # Extra view
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE VIEW rogue_view AS SELECT 1 AS ok")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_VIEW_OBJECT, cl.reason_codes)

        # Unexpected trigger in main
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute(
                "CREATE TRIGGER rogue_trg AFTER INSERT ON stories BEGIN SELECT 1; END"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_TRIGGER_OBJECT, cl.reason_codes)

        # Virtual table impersonating v6 table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("DROP TABLE run_locks")
            conn.execute("CREATE VIRTUAL TABLE run_locks USING fts5(lock_key)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_VIRTUAL_TABLE, cl.reason_codes)

    def test_candidate_journal_tampering_and_partial_artifacts(self):
        # Missing journal index
        with closing(_make_canonical_v6_conn()) as conn:
            apply_candidate_schema_v7(conn)
            conn.execute("DROP INDEX idx_staging_run_journal_status")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertTrue(cl.candidate_journal_present)
            self.assertFalse(cl.candidate_journal_valid)
            self.assertIn(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID, cl.reason_codes)

        # Orphan journal index without journal table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE INDEX idx_staging_run_journal_status ON stories (category)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertTrue(cl.candidate_journal_present)
            self.assertFalse(cl.candidate_journal_valid)
            self.assertIn(PhaseGateReasonCode.PARTIAL_OR_ORPHAN_JOURNAL_INDEX, cl.reason_codes)

        # Superficially 10-column table with wrong column metadata
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute(
                """
                CREATE TABLE staging_run_journal (
                    run_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    fencing_token TEXT NOT NULL DEFAULT '1',
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    manifest_seal_hex TEXT NOT NULL,
                    target_inventory_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX idx_staging_run_journal_status ON staging_run_journal (status, updated_at)"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID, cl.reason_codes)

        # Extra index on staging_run_journal
        with closing(_make_canonical_v6_conn()) as conn:
            apply_candidate_schema_v7(conn)
            conn.execute("CREATE INDEX idx_journal_extra ON staging_run_journal (owner_id)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_INDEX_OBJECT, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID, cl.reason_codes)

        # Trigger on staging_run_journal
        with closing(_make_canonical_v6_conn()) as conn:
            apply_candidate_schema_v7(conn)
            conn.execute(
                "CREATE TRIGGER trg_journal AFTER INSERT ON staging_run_journal BEGIN SELECT 1; END"
            )
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_TRIGGER_OBJECT, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID, cl.reason_codes)

        # Spoofed view staging_run_journal
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE VIEW staging_run_journal AS SELECT 'r1' AS run_id")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_VIEW_OBJECT, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID, cl.reason_codes)

        # Virtual table impersonating staging_run_journal
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE VIRTUAL TABLE staging_run_journal USING fts5(run_id, owner_id)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertTrue(cl.candidate_journal_present)
            self.assertFalse(cl.candidate_journal_valid)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_VIRTUAL_TABLE, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID, cl.reason_codes)

        # Case-mismatched table "STAGING_RUN_JOURNAL"
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute('CREATE TABLE "STAGING_RUN_JOURNAL" (run_id TEXT PRIMARY KEY)')
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_TABLE_OBJECT, cl.reason_codes)
            self.assertIn(PhaseGateReasonCode.CANDIDATE_JOURNAL_SCHEMA_INVALID, cl.reason_codes)

        # Valid candidate journal combined with drifted v6 table must DENY
        with closing(_make_canonical_v6_conn()) as conn:
            apply_candidate_schema_v7(conn)
            conn.execute("DROP TABLE run_locks")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertTrue(cl.candidate_journal_present)
            self.assertTrue(cl.candidate_journal_valid)
            self.assertIn(PhaseGateReasonCode.MISSING_V6_TABLE, cl.reason_codes)

    def test_temp_shadow_objects_and_attached_databases_rejected(self):
        # TEMP shadow staging_run_journal
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE TEMP TABLE staging_run_journal (run_id TEXT PRIMARY KEY)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.TEMP_SHADOW_OBJECT_DISALLOWED, cl.reason_codes)

        # Case-insensitive uppercase TEMP shadow under PRAGMA case_sensitive_like=ON
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("PRAGMA case_sensitive_like=ON")
            conn.execute('CREATE TEMP TABLE "STORIES" (story_id TEXT PRIMARY KEY)')
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.TEMP_SHADOW_OBJECT_DISALLOWED, cl.reason_codes)

        # TEMP trigger
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE TEMP TABLE helper_t (id INT)")
            conn.execute("CREATE TEMP TRIGGER t_trg AFTER INSERT ON helper_t BEGIN SELECT 1; END")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.TEMP_TRIGGER_DISALLOWED, cl.reason_codes)

        # Unrelated TEMP table
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("CREATE TEMP TABLE unrelated_tmp (id TEXT)")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.UNEXPECTED_TEMP_OBJECT, cl.reason_codes)

        # Attached extra :memory: DB
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute("ATTACH DATABASE ':memory:' AS aux")
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.DENY)
            self.assertIn(PhaseGateReasonCode.ATTACHED_DATABASE_DISALLOWED, cl.reason_codes)


class TestReadOnlyIsolationAndInertHandoff(unittest.TestCase):
    def _assert_read_only_on_conn(self, conn: sqlite3.Connection, expected_phase: SchemaPhase) -> None:
        conn.execute(
            "INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, "
            "article_ids_json, update_count, is_active) VALUES "
            "('st_seed_1', 'Seed Headline', 'magazin', '2026-11-22T00:00:00Z', '2026-11-22T00:00:00Z', '[]', 0, 1)"
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        changes_before = conn.total_changes
        rows_before = conn.execute("SELECT story_id, headline_tr FROM stories").fetchall()
        rows_before_tuples = [tuple(r) for r in rows_before]

        traced_sql: list[str] = []
        auth_denials: list[tuple[Any, ...]] = []

        def trace_cb(sql_text: str) -> None:
            traced_sql.append(sql_text)

        def read_only_authorizer(action: int, arg1: Any, arg2: Any, db_name: Any, trig: Any) -> int:
            if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION):
                return sqlite3.SQLITE_OK
            if action == sqlite3.SQLITE_PRAGMA and str(arg1 or "").lower() in _ALLOWLISTED_READONLY_PRAGMAS:
                return sqlite3.SQLITE_OK
            auth_denials.append((action, arg1, arg2, db_name, trig))
            return sqlite3.SQLITE_DENY

        conn.set_trace_callback(trace_cb)
        conn.set_authorizer(read_only_authorizer)
        try:
            classification = classify_offline_schema_phase(conn)
            handoff = build_mock_operator_handoff(classification)
        finally:
            conn.set_authorizer(None)
            conn.set_trace_callback(None)

        self.assertEqual(auth_denials, [], f"Authorizer caught non-read-only operations: {auth_denials}")
        self.assertEqual(classification.phase, expected_phase)
        self.assertEqual(handoff.phase, expected_phase)
        self.assertTrue(classification.non_mutating_verified)
        self.assertEqual(
            classification.schema_fingerprint_before,
            classification.schema_fingerprint_after,
        )
        self.assertEqual(
            classification.content_fingerprint_before,
            classification.content_fingerprint_after,
        )
        self.assertIs(conn.row_factory, sqlite3.Row)
        self.assertEqual(conn.total_changes, changes_before)

        rows_after = [tuple(r) for r in conn.execute("SELECT story_id, headline_tr FROM stories").fetchall()]
        self.assertEqual(rows_after, rows_before_tuples)

        forbidden_verbs = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "REPLACE")
        for sql in traced_sql:
            first_word = sql.strip().split(None, 1)[0].upper() if sql.strip() else ""
            self.assertNotIn(first_word, forbidden_verbs, f"Mutating SQL traced: {sql}")

    def test_authorizer_trace_callback_and_fingerprints_prove_strictly_read_only(self):
        with closing(_make_canonical_v6_conn()) as v6_conn:
            self._assert_read_only_on_conn(v6_conn, SchemaPhase.V6_BASELINE_OFFLINE)

        with closing(_make_canonical_v6_conn()) as cand_conn:
            apply_candidate_schema_v7(cand_conn)
            self._assert_read_only_on_conn(cand_conn, SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE)

        with closing(_make_canonical_v6_conn()) as bad_conn:
            bad_conn.execute("CREATE TABLE extra_tampered (id TEXT PRIMARY KEY)")
            bad_conn.commit()
            self._assert_read_only_on_conn(bad_conn, SchemaPhase.DENY)

    def test_isolation_rejects_non_temp_and_attached_non_temp_without_path_leakage(self):
        sentinel = "SECRET_SENTINEL_DB_PATH_99"
        with (
            tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as fixture_dir,
            tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as other_temp_dir,
        ):
            ext_db = Path(fixture_dir) / f"{sentinel}_main.db"
            aux_db = Path(fixture_dir) / f"{sentinel}_aux.db"
            with closing(sqlite3.connect(str(aux_db))) as aux_init:
                aux_init.execute("CREATE TABLE aux_t (id INT)")

            with closing(sqlite3.connect(str(ext_db))) as ext_conn:
                with patch("tempfile.gettempdir", return_value=other_temp_dir):
                    caught_exc = None
                    active_tb = ""
                    frames: list[traceback.FrameSummary] = []
                    try:
                        classify_offline_schema_phase(ext_conn)
                    except PermissionError as exc:
                        caught_exc = exc
                        active_tb = traceback.format_exc()
                        frames = traceback.extract_tb(exc.__traceback__)

                    self.assertIsNotNone(caught_exc)
                    self.assertGreater(len(frames), 0)
                    self.assertIn("HERMETIC_FIXTURE_ISOLATION_VIOLATION", str(caught_exc))
                    self.assertNotIn(sentinel, str(caught_exc))
                    self.assertNotIn(sentinel, active_tb)

            with closing(sqlite3.connect(":memory:")) as mem_conn:
                mem_conn.execute(f"ATTACH DATABASE '{aux_db}' AS aux")
                with patch("tempfile.gettempdir", return_value=other_temp_dir):
                    caught_exc = None
                    active_tb = ""
                    frames = []
                    try:
                        evaluate_offline_schema_phase_gate(mem_conn)
                    except PermissionError as exc:
                        caught_exc = exc
                        active_tb = traceback.format_exc()
                        frames = traceback.extract_tb(exc.__traceback__)

                    self.assertIsNotNone(caught_exc)
                    self.assertGreater(len(frames), 0)
                    self.assertIn("HERMETIC_FIXTURE_ISOLATION_VIOLATION", str(caught_exc))
                    self.assertNotIn(sentinel, str(caught_exc))
                    self.assertNotIn(sentinel, active_tb)

    def test_non_sqlite_connections_and_connection_subclass_rejected(self):
        class ConnSubclass(sqlite3.Connection):
            pass

        with closing(_make_canonical_v6_conn()) as mem_conn, closing(ConnSubclass(":memory:")) as sub_conn:
            real_http = httpx.HTTPTransport(retries=0)
            client = TursoHTTPClient(
                database_url=EXACT_STAGING_DESTINATION,
                auth_token="SECRET_TOKEN_99",
                transport=real_http,
            )
            self.addCleanup(client.close)
            hermetic_transport = HermeticStagingTransport(mem_conn)

            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
                repo = SqliteStateRepository.create_owned_synthetic_fixture(Path(td) / "sub.db")
                self.addCleanup(repo.close)
                bad_targets = [
                    client,
                    real_http,
                    hermetic_transport,
                    repo,
                    "data/radar.db",
                    Path("data/radar.db"),
                    None,
                    sub_conn,
                ]
                for target in bad_targets:
                    with self.subTest(target_type=type(target).__name__):
                        with self.assertRaises(PermissionError) as ctx:
                            classify_offline_schema_phase(target)  # type: ignore[arg-type]
                        msg = str(ctx.exception)
                        self.assertIn("HERMETIC_FIXTURE_CONNECTION_REQUIRED", msg)
                        self.assertNotIn("SECRET_TOKEN_99", msg)
                        self.assertNotIn("radar.db", msg)

    def test_closed_connection_fails_closed_to_unknown(self):
        conn = _make_canonical_v6_conn()
        conn.close()
        handoff = evaluate_offline_schema_phase_gate(conn)
        self.assertEqual(handoff.phase, SchemaPhase.UNKNOWN)
        self.assertEqual(handoff.mock_test_eligibility, MockTestEligibility.NONE_UNKNOWN)
        self.assertFalse(handoff.eligible_for_mock_v6_live01)
        self.assertFalse(handoff.eligible_for_mock_journal_seam)
        self.assertFalse(handoff.non_mutating_verified)
        self.assertIn(PhaseGateReasonCode.INSPECTION_READ_ERROR, handoff.reason_codes)

    def test_custom_dict_row_factory_on_candidate_journal_fails_closed_to_unknown(self):
        def dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
            return {col[0]: row[idx] for idx, col in enumerate(cursor.description or ())}

        with closing(_make_canonical_v6_conn()) as conn:
            apply_candidate_schema_v7(conn)
            conn.row_factory = dict_factory
            cl = classify_offline_schema_phase(conn)
            self.assertEqual(cl.phase, SchemaPhase.UNKNOWN)
            self.assertIn(PhaseGateReasonCode.INSPECTION_READ_ERROR, cl.reason_codes)
            self.assertIs(conn.row_factory, dict_factory)

    def test_sentinel_never_leaks_from_hostile_ddl_names_or_migrations(self):
        sentinel = "SECRET_SENTINEL_HOSTILE_DDL_99"
        with closing(_make_canonical_v6_conn()) as conn:
            conn.execute(f"CREATE TABLE tbl_{sentinel} (id TEXT PRIMARY KEY /* {sentinel} */)")
            conn.execute(f"CREATE INDEX idx_{sentinel} ON tbl_{sentinel} (id)")
            conn.execute(f"CREATE VIEW vw_{sentinel} AS SELECT 1 AS c")
            conn.execute(f"CREATE TRIGGER trg_{sentinel} AFTER INSERT ON tbl_{sentinel} BEGIN SELECT 1; END")
            conn.execute(
                "UPDATE schema_migrations SET description = ? WHERE version = 6",
                (f"v6 {sentinel}",),
            )
            cl = classify_offline_schema_phase(conn)
            handoff = build_mock_operator_handoff(cl)

        self.assertEqual(handoff.phase, SchemaPhase.DENY)
        self.assertNotIn(sentinel, cl.model_dump_json())
        self.assertNotIn(sentinel, handoff.model_dump_json())
        self.assertNotIn(sentinel, handoff.summary())

    def test_inert_handoff_model_hardening_and_untrusted_evidence(self):
        self.assertNotIn("live_authorized", MockOperatorHandoff.model_fields)
        self.assertNotIn("live_authorized", SchemaPhaseClassification.model_fields)

        with closing(_make_canonical_v6_conn()) as conn:
            handoff = evaluate_offline_schema_phase_gate(conn)

        with self.assertRaises(ValidationError):
            handoff.phase = SchemaPhase.UNKNOWN  # type: ignore[misc]

        with self.assertRaises(ValidationError):
            MockOperatorHandoff(  # type: ignore[call-arg]
                **handoff.model_dump(),
                live_authorized=True,
            )

        with self.assertRaises(ValidationError):
            MockOperatorHandoff(  # type: ignore[call-arg]
                **handoff.model_dump(),
                token="forbid",
            )

        with self.assertRaises(ValidationError):
            MockOperatorHandoff(
                **{**handoff.model_dump(), "notes": ("LIVE_GO_APPROVED",)},
            )

        with self.assertRaises(ValidationError):
            MockOperatorHandoff(
                **{**handoff.model_dump(), "schema_fingerprint_sha256": "not-a-sha256"},
            )

        # Missing/None classification -> UNKNOWN
        unknown_handoff = build_mock_operator_handoff(None)
        self.assertEqual(unknown_handoff.phase, SchemaPhase.UNKNOWN)
        self.assertIn(PhaseGateReasonCode.UNOBSERVED_OR_MISSING_EVIDENCE, unknown_handoff.reason_codes)

        # Untrusted or forged external evidence -> DENY
        forged_classification = SchemaPhaseClassification.model_construct(
            phase=SchemaPhase.V6_BASELINE_OFFLINE,
            observed_schema_version=7,
            observed_v6_tables_matched=15,
            observed_v6_columns_matched=129,
            observed_v6_explicit_indexes_matched=15,
            candidate_journal_present=False,
            candidate_journal_valid=False,
            schema_fingerprint_before="a" * 64,
            schema_fingerprint_after="a" * 64,
            content_fingerprint_before="b" * 64,
            content_fingerprint_after="b" * 64,
            non_mutating_verified=True,
            reason_codes=(),
        )
        forged_non_hex_classification = SchemaPhaseClassification.model_construct(
            phase=SchemaPhase.V6_BASELINE_OFFLINE,
            observed_schema_version=6,
            observed_v6_tables_matched=15,
            observed_v6_columns_matched=129,
            observed_v6_explicit_indexes_matched=15,
            candidate_journal_present=False,
            candidate_journal_valid=False,
            schema_fingerprint_before="not-a-sha256",
            schema_fingerprint_after="not-a-sha256",
            content_fingerprint_before="b" * 64,
            content_fingerprint_after="b" * 64,
            non_mutating_verified=True,
            reason_codes=(),
        )
        remote_dict = {"phase": "V6_BASELINE_OFFLINE", "remote_state": "CLEAN", "live_authorized": True}
        preflight_ok = PreflightReport(success=True, schema_version=6, tables_checked=15, columns_checked=129)
        live01_ok = Live01Report(
            passed=True,
            stage="COLLISION_CHECK",
            request_count=47,
            schema_version=6,
            tables_checked=15,
            columns_checked=129,
            collision_checks=5,
        )
        for untrusted in (
            forged_classification,
            forged_non_hex_classification,
            remote_dict,
            preflight_ok,
            live01_ok,
        ):
            with self.subTest(untrusted_type=type(untrusted).__name__):
                denied = build_mock_operator_handoff(untrusted)  # type: ignore[arg-type]
                self.assertEqual(denied.phase, SchemaPhase.DENY)
                self.assertFalse(denied.eligible_for_mock_v6_live01)
                self.assertFalse(denied.eligible_for_mock_journal_seam)
                self.assertIn(
                    PhaseGateReasonCode.UNTRUSTED_OR_REMOTE_CLAIMED_EVIDENCE,
                    denied.reason_codes,
                )

    def test_module_imports_and_operational_schema_version_unchanged(self):
        self.assertEqual(CURRENT_SCHEMA_VERSION, 6)
        self.assertEqual(len(MIGRATIONS), 6)

        mod_path = Path(schema_phase_gate.__file__).resolve()
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported_modules: set[str] = set()
        imported_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_modules.add(alias.name.split(".")[0])
                    imported_names.add(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imported_modules.add(node.module.split(".")[0])
                    imported_modules.add(node.module)
                for alias in node.names:
                    imported_names.add(alias.name)

        for forbidden_mod in ("httpx", "urllib", "socket", "requests", "aiohttp"):
            self.assertNotIn(forbidden_mod, imported_modules)
        for forbidden_sym in ("apply_candidate_schema_v7", "SCHEMA_V7_STAGING_JOURNAL_DDL"):
            self.assertNotIn(forbidden_sym, imported_names)


class TestExistingLiveOperatorAndSeamRegressions(unittest.TestCase):
    def test_execute_live01_readonly_still_passes_on_canonical_v6_without_journal(self):
        with closing(_make_canonical_v6_conn()) as conn:
            handoff = evaluate_offline_schema_phase_gate(conn)
            self.assertEqual(handoff.phase, SchemaPhase.V6_BASELINE_OFFLINE)
            self.assertTrue(handoff.eligible_for_mock_v6_live01)

            transport = HermeticStagingTransport(conn)
            report = execute_live01_readonly("mock-readonly-token", inner_transport=transport)
            self.assertTrue(report.passed, report.summary())
            self.assertEqual(report.schema_version, 6)
            self.assertEqual(report.tables_checked, 15)
            self.assertGreaterEqual(report.columns_checked, 129)
            self.assertEqual(report.collision_checks, 5)

    def test_live_ready_operator_preflight_still_passes_on_v6_without_journal(self):
        with (
            closing(_make_canonical_v6_conn()) as conn,
            tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td,
        ):
            handoff = evaluate_offline_schema_phase_gate(conn)
            self.assertTrue(handoff.eligible_for_mock_v6_live01)

            budget = BudgetTrackingTransport(HermeticStagingTransport(conn))
            client = TursoHTTPClient(
                database_url=EXACT_STAGING_DESTINATION,
                auth_token="synthetic-offline-only",
                transport=budget,
            )
            self.addCleanup(client.close)
            op = LiveReadyStagingOperator(
                client=client,
                destination_url=EXACT_STAGING_DESTINATION,
                manifest_dir=Path(td),
                live_authorized=False,
            )
            op._create_and_persist_manifest()
            op._run_live_01()
            self.assertTrue(op._preflight_accepted)
            self.assertEqual(op._preflight_provenance, "VERIFIED_IN_SESSION")

    def test_cloud_preflight_validator_semantics_unchanged(self):
        with (
            closing(_make_canonical_v6_conn()) as conn,
            tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td,
        ):
            # Persist temp copy because CloudPreflightValidator uses SqliteStateRepository file paths
            db_path = Path(td) / "preflight_extra_table.db"
            repo = SqliteStateRepository.create_owned_synthetic_fixture(db_path)
            with repo._connection() as rconn:
                rconn.execute("CREATE TABLE unrelated_extra_table (id TEXT PRIMARY KEY)")
            repo.close()

            with closing(sqlite3.connect(str(db_path))) as fconn:
                handoff = evaluate_offline_schema_phase_gate(fconn)
                self.assertEqual(handoff.phase, SchemaPhase.DENY)
                self.assertIn(PhaseGateReasonCode.UNEXPECTED_TABLE_OBJECT, handoff.reason_codes)

            check_repo = SqliteStateRepository(db_path)
            self.addCleanup(check_repo.close)
            preflight_report = CloudPreflightValidator(repo=check_repo).validate()
            self.assertTrue(preflight_report.success)

    def test_existing_live_deny_guards_unchanged(self):
        with closing(_make_canonical_v6_conn()) as conn:
            hermetic_client = TursoHTTPClient(
                database_url=EXACT_STAGING_DESTINATION,
                auth_token="synthetic-offline-only",
                transport=HermeticStagingTransport(conn),
            )
            real_http_client = TursoHTTPClient(
                database_url=EXACT_STAGING_DESTINATION,
                auth_token="synthetic-offline-only",
                transport=httpx.HTTPTransport(retries=0),
            )
            self.addCleanup(hermetic_client.close)
            self.addCleanup(real_http_client.close)

            with self.assertRaisesRegex(PermissionError, "LIVE_TURSO_UNAUTHORIZED"):
                LiveReadyStagingOperator(client=hermetic_client, live_authorized=True)

            with self.assertRaisesRegex(PermissionError, "Hermetic security boundary violation"):
                LiveReadyStagingOperator(client=real_http_client, live_authorized=False)

            with self.assertRaisesRegex(PermissionError, "Hermetic security boundary violation"):
                StagingJournalHranaSeam(client=real_http_client, owner_id="runner_a")

            # v6 baseline without staging_run_journal cannot initialize StagingJournalHranaSeam
            with self.assertRaises(Exception):
                StagingJournalHranaSeam(client=hermetic_client, owner_id="runner_a")

        # CLI --live / --execute / --enable-writes / empty argv refuse execution cleanly
        with patch("httpx.Client") as http_client_cls:
            for argv in (["--live"], ["--execute"], ["--enable-writes"], []):
                with self.subTest(argv=argv):
                    with patch("sys.stderr", new_callable=io.StringIO):
                        rc = live_ready_operator_main(argv)
                    self.assertEqual(rc, 2)
            self.assertEqual(http_client_cls.call_count, 0)

        # Candidate journal fixture works with StagingJournalHranaSeam
        with (
            closing(_make_canonical_v6_conn()) as cand_conn,
            tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as marker_dir,
        ):
            apply_candidate_schema_v7(cand_conn)
            handoff = evaluate_offline_schema_phase_gate(cand_conn)
            self.assertEqual(handoff.phase, SchemaPhase.V6_WITH_CANDIDATE_JOURNAL_OFFLINE)
            self.assertTrue(handoff.eligible_for_mock_journal_seam)

            seam_client = TursoHTTPClient(
                database_url=EXACT_STAGING_DESTINATION,
                auth_token="synthetic-offline-only",
                transport=HermeticStagingTransport(cand_conn),
            )
            self.addCleanup(seam_client.close)
            seam = StagingJournalHranaSeam(
                client=seam_client,
                owner_id="runner_a",
                marker_dir=Path(marker_dir),
            )
            self.assertEqual(seam.owner_id, "runner_a")


if __name__ == "__main__":
    unittest.main()
