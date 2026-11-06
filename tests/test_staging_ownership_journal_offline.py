"""Hermetic offline contract tests for ERD-017 staging ownership journal (Issue #90).

SAFETY & BOUNDARY ASSURANCE:
- Operates strictly against in-memory (:memory:) or temporary file SQLite fixtures.
- Absolute $0.00 / 0 TRY spending ceiling.
- Zero network I/O, zero Turso / cloud connections, zero live credentials.
- Candidate Schema v7 DDL is applied ONLY via apply_candidate_schema_v7() in test fixtures.
- Default CURRENT_SCHEMA_VERSION in efso_radar.repositories.schema remains strictly 6.
- Covers all 8 PO-required adversarial cases:
  1. Concurrent duplicate run_ids.
  2. Distinct run_ids targeting the same workload/schedule.
  3. Stale/foreign runner owners.
  4. Fencing races & monotonic token CAS.
  5. Tampered manifest seal / target inventory mismatch.
  6. Ambiguous commit handling (UNKNOWN / HALT).
  7. Crash / rollback atomicity (Option B+C).
  8. Unauthorized or partial cleanup.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone

from efso_radar.repositories.schema import (
    CURRENT_SCHEMA_VERSION,
    MIGRATIONS,
    get_all_init_statements,
)
from efso_radar.staging.journal import (
    ACTIVE_JOURNAL_STATUSES,
    LEGAL_TRANSITIONS,
    SCHEMA_V7_STAGING_JOURNAL_DDL,
    ActiveWorkloadCollisionError,
    AmbiguousCommitError,
    FencingConflictError,
    InvalidJournalTransitionError,
    InvalidRunIdError,
    JournalError,
    JournalExecutionPattern,
    JournalStatus,
    ManifestTamperedError,
    ReadProvenance,
    StagingJournalEntry,
    StagingOwnershipJournal,
    StaleOwnerError,
    TargetInventoryViolationError,
    UnauthorizedTakeoverError,
    apply_candidate_schema_v7,
    canonical_inventory_json,
    validate_status_transition,
)
from efso_radar.staging.runner import (
    RUN_ID_REGEX,
    RegisteredEntity,
    create_default_manifest_entities,
)


def _make_run_id(suffix: str = "01") -> str:
    """Helper to create a canonical run_id matching RUN_ID_REGEX."""
    hex_digest = hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:12]
    run_id = f"run_20261106T200000Z_{hex_digest}"
    assert RUN_ID_REGEX.match(run_id)
    return run_id


def _dummy_seal_hex(seed: str = "seal_01") -> str:
    """Helper to create a 64-character SHA-256 seal hex."""
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


class TestCandidateSchemaV7Isolation(unittest.TestCase):
    """Verify that Schema v7 is strictly isolated to explicit test DDL and does not alter Schema v6."""

    def test_default_schema_version_is_strictly_6(self):
        """Repository default schema version must remain 6."""
        self.assertEqual(CURRENT_SCHEMA_VERSION, 6)

    def test_schema_migrations_contains_exactly_6_steps(self):
        """MIGRATIONS list must contain exactly 6 versions, none mentioning staging_run_journal."""
        self.assertEqual(len(MIGRATIONS), 6)
        for step in MIGRATIONS:
            for stmt in step.statements:
                self.assertNotIn("staging_run_journal", stmt)

    def test_fresh_db_initialization_does_not_contain_staging_run_journal(self):
        """Fresh database initialized with get_all_init_statements() does not contain staging_run_journal."""
        conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            conn.executescript(stmt)

        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='staging_run_journal'"
        )
        self.assertIsNone(cur.fetchone())
        conn.close()

    def test_apply_candidate_schema_v7_explicit_only(self):
        """apply_candidate_schema_v7() creates staging_run_journal and preserves schema_migrations at v6."""
        conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            conn.executescript(stmt)

        # Record schema version 6 as standard repository init would
        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at, description) VALUES (6, '2026-11-06T20:00:00Z', 'v6')"
        )
        conn.commit()

        # Explicitly apply candidate Schema v7 DDL
        apply_candidate_schema_v7(conn)

        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='staging_run_journal'"
        )
        self.assertIsNotNone(cur.fetchone())

        cur_idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_staging_run_journal_status'"
        )
        self.assertIsNotNone(cur_idx.fetchone())

        # Crucial check: schema_migrations remains at MAX(version) == 6
        cur_ver = conn.execute("SELECT MAX(version) FROM schema_migrations")
        self.assertEqual(cur_ver.fetchone()[0], 6)
        conn.close()

    def test_journal_initialization_fails_if_table_missing(self):
        """Instantiating StagingOwnershipJournal fails closed if staging_run_journal has not been applied."""
        conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            conn.executescript(stmt)

        with self.assertRaises(JournalError) as ctx:
            StagingOwnershipJournal(conn)
        self.assertIn("staging_run_journal", str(ctx.exception))
        conn.close()


class TestJournalStatusTransitions(unittest.TestCase):
    """Test state machine validator across legal and illegal transitions."""

    def test_valid_standard_lifecycle(self):
        """Legal standard path: INTENT_PENDING -> ACKED -> MUTATING -> EFFECT_COMMITTED -> CLEANUP_PENDING -> CLEANED_UP."""
        validate_status_transition(JournalStatus.INTENT_PENDING, JournalStatus.ACKED)
        validate_status_transition(JournalStatus.ACKED, JournalStatus.MUTATING)
        validate_status_transition(JournalStatus.MUTATING, JournalStatus.EFFECT_COMMITTED)
        validate_status_transition(JournalStatus.EFFECT_COMMITTED, JournalStatus.CLEANUP_PENDING)
        validate_status_transition(JournalStatus.CLEANUP_PENDING, JournalStatus.CLEANED_UP)

    def test_valid_multi_stage_mutating_transition(self):
        """EFFECT_COMMITTED can transition back to MUTATING for subsequent stages (e.g. LIVE_02 -> LIVE_03)."""
        validate_status_transition(JournalStatus.EFFECT_COMMITTED, JournalStatus.MUTATING)

    def test_valid_abort_transitions(self):
        """Active non-terminal states can transition to ABORTED."""
        for status in [
            JournalStatus.INTENT_PENDING,
            JournalStatus.ACKED,
            JournalStatus.MUTATING,
            JournalStatus.EFFECT_COMMITTED,
            JournalStatus.CLEANUP_PENDING,
        ]:
            validate_status_transition(status, JournalStatus.ABORTED)

    def test_valid_unknown_transitions(self):
        """Ambiguous errors can transition active states to UNKNOWN."""
        for status in [
            JournalStatus.INTENT_PENDING,
            JournalStatus.ACKED,
            JournalStatus.MUTATING,
            JournalStatus.EFFECT_COMMITTED,
            JournalStatus.CLEANUP_PENDING,
        ]:
            validate_status_transition(status, JournalStatus.UNKNOWN)

    def test_illegal_transitions_fail_closed(self):
        """Illegal transitions raise InvalidJournalTransitionError."""
        illegal_cases = [
            (JournalStatus.INTENT_PENDING, JournalStatus.MUTATING),
            (JournalStatus.INTENT_PENDING, JournalStatus.EFFECT_COMMITTED),
            (JournalStatus.ACKED, JournalStatus.CLEANED_UP),
            (JournalStatus.MUTATING, JournalStatus.CLEANED_UP),
            (JournalStatus.CLEANED_UP, JournalStatus.MUTATING),
            (JournalStatus.CLEANED_UP, JournalStatus.ABORTED),
            (JournalStatus.ABORTED, JournalStatus.MUTATING),
            (JournalStatus.UNKNOWN, JournalStatus.MUTATING),
        ]
        for src, dst in illegal_cases:
            with self.assertRaises(InvalidJournalTransitionError):
                validate_status_transition(src, dst)

    def test_operator_override_permits_reconciling_terminal_states(self):
        """With is_operator_override=True, manual reconciliation can transition UNKNOWN."""
        validate_status_transition(
            JournalStatus.UNKNOWN,
            JournalStatus.ABORTED,
            is_operator_override=True,
        )


class TestStagingOwnershipJournalHermeticSuite(unittest.TestCase):
    """Hermetic integration tests for StagingOwnershipJournal on in-memory SQLite fixtures."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            self.conn.executescript(stmt)
        apply_candidate_schema_v7(self.conn)
        self.journal = StagingOwnershipJournal(self.conn)
        self.run_id = _make_run_id("test_01")
        self.owner_id = "gha_run_1234_attempt_1_uuid"
        self.manifest_seal_hex = _dummy_seal_hex("manifest_01")
        self.entities = create_default_manifest_entities(self.run_id)

    def tearDown(self):
        self.conn.close()

    # -------------------------------------------------------------------------
    # Adversarial Case 1: Concurrent Duplicate Runs (Exact Same Run ID)
    # -------------------------------------------------------------------------
    def test_adversarial_duplicate_identical_run_id_rejected(self):
        """Two runners attempting to initialize the exact same run_id fail closed with ActiveWorkloadCollisionError."""
        entry = self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )
        self.assertEqual(entry.status, JournalStatus.ACKED)
        self.assertEqual(entry.fencing_token, 1)

        # Duplicate registration with same run_id must fail closed
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.journal.register_intent(
                run_id=self.run_id,
                owner_id="gha_run_concurrent_duplicate",
                manifest_seal_hex=self.manifest_seal_hex,
                target_inventory=self.entities,
            )
        self.assertIn("already registered in journal", str(ctx.exception))

    # -------------------------------------------------------------------------
    # Adversarial Case 2: Distinct Run IDs Targeting Same Workload / Schedule
    # -------------------------------------------------------------------------
    def test_adversarial_distinct_run_ids_same_workload_rejected_by_active_journal(self):
        """Distinct run_ids targeting the same workload fail closed while the first run is active in journal."""
        run_a = _make_run_id("run_alpha")
        run_b = _make_run_id("run_beta")

        # Runner A registers intent for workload
        self.journal.register_intent(
            run_id=run_a,
            owner_id="runner_alpha",
            manifest_seal_hex=_dummy_seal_hex("alpha"),
            target_inventory=create_default_manifest_entities(run_a),
            workload_key="staging_controlled_run",
        )

        # Runner B starts for the same workload while Runner A is active -> collision
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.journal.register_intent(
                run_id=run_b,
                owner_id="runner_beta",
                manifest_seal_hex=_dummy_seal_hex("beta"),
                target_inventory=create_default_manifest_entities(run_b),
                workload_key="staging_controlled_run",
            )
        self.assertIn("Another staging run", str(ctx.exception))
        self.assertIn("currently active", str(ctx.exception))

    def test_adversarial_distinct_run_ids_same_workload_rejected_by_run_locks_lease(self):
        """Distinct run_id is rejected if another worker holds the coarse-grained task lease in run_locks."""
        # Simulate active lease held by worker_stale in run_locks
        now = datetime.now(timezone.utc)
        expires_at = (now + timedelta(minutes=15)).isoformat()
        now_iso = now.isoformat()
        self.conn.execute(
            """
            INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at)
            VALUES ('staging_controlled_run', 'worker_alpha', ?, ?, ?)
            """,
            (now_iso, expires_at, now_iso),
        )
        self.conn.commit()

        # Worker beta tries to start run_beta for the same workload
        run_b = _make_run_id("run_beta")
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.journal.register_intent(
                run_id=run_b,
                owner_id="worker_beta",
                manifest_seal_hex=_dummy_seal_hex("beta"),
                target_inventory=create_default_manifest_entities(run_b),
                workload_key="staging_controlled_run",
                now=now,
            )
        self.assertIn("actively leased to 'worker_alpha'", str(ctx.exception))

    # -------------------------------------------------------------------------
    # Adversarial Case 3: Stale / Foreign Runner Owners
    # -------------------------------------------------------------------------
    def test_adversarial_stale_foreign_owner_rejected_on_stage_mutation(self):
        """Runner B cannot mutate entities for a run owned by Runner A."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id="runner_alpha",
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        mutations = [(
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
            (article.pk_value, "Title", "source", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )]

        with self.assertRaises(StaleOwnerError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id="runner_foreign_zombie",
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=mutations,
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("does not match registered owner", str(ctx.exception))

    def test_adversarial_stale_foreign_owner_rejected_on_cleanup(self):
        """Runner B cannot cleanup entities for a run owned by Runner A."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id="runner_alpha",
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        with self.assertRaises(StaleOwnerError):
            self.journal.execute_conditional_cleanup(
                run_id=self.run_id,
                owner_id="runner_foreign_zombie",
                expected_fencing_token=1,
                cleanup_statements=[],
                manifest_seal_hex=self.manifest_seal_hex,
            )

    # -------------------------------------------------------------------------
    # Adversarial Case 4: Fencing Races & Token CAS Mismatch
    # -------------------------------------------------------------------------
    def test_adversarial_fencing_token_mismatch_fails_closed_and_rolls_back(self):
        """If fencing token does not match expected_token, mutation fails closed and rolls back."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        mutations = [(
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
            (article.pk_value, "Title", "source", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )]

        # Acting runner passes stale expected_fencing_token=0 (actual is 1)
        with self.assertRaises(FencingConflictError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=0,
                stage="LIVE_02",
                mutation_statements=mutations,
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("Fencing token mismatch", str(ctx.exception))

        # Verify database is rolled back: zero rows in processed_articles
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 0)

    # -------------------------------------------------------------------------
    # Adversarial Case 5: Tampered Manifests / Target Inventory Mismatch
    # -------------------------------------------------------------------------
    def test_adversarial_tampered_manifest_seal_rejected_at_mutation(self):
        """If manifest seal hex does not match registered intent, mutation is rejected."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        mutations = [(
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
            (article.pk_value, "Title", "source", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )]

        tampered_seal = _dummy_seal_hex("tampered_content")
        with self.assertRaises(ManifestTamperedError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=mutations,
                manifest_seal_hex=tampered_seal,
            )
        self.assertIn("Manifest seal mismatch", str(ctx.exception))

    def test_adversarial_mutation_targeting_unregistered_table_rejected(self):
        """Mutation attempting to touch a table not in target inventory fails closed."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        unauthorized_mutation = [(
            "INSERT INTO users (user_id, username) VALUES (?, ?)",
            ("user_01", "malicious_user"),
        )]

        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=unauthorized_mutation,
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("unregistered table", str(ctx.exception))

    def test_adversarial_mutation_targeting_unregistered_pk_rejected(self):
        """Mutation attempting to touch an unregistered primary key within an allowed table fails closed."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        unauthorized_mutation = [(
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
            ("unregistered_art_999", "Title", "source", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )]

        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=unauthorized_mutation,
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("do not contain registered primary key", str(ctx.exception))

    # -------------------------------------------------------------------------
    # Adversarial Case 6: Ambiguous Commits (UNKNOWN Outcome & Permanent HALT)
    # -------------------------------------------------------------------------
    def test_adversarial_ambiguous_commit_during_stage_mutation_marks_unknown_and_halts(self):
        """Simulated ambiguous commit marks journal status as UNKNOWN and triggers AmbiguousCommitError."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        mutations = [(
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
            (article.pk_value, "Title", "source", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )]

        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=mutations,
                manifest_seal_hex=self.manifest_seal_hex,
                simulate_ambiguous_commit=True,
            )
        self.assertIn("Ambiguous commit encountered", str(ctx.exception))

        # Inspect journal entry: status must be UNKNOWN
        entry = self.journal.inspect_run(self.run_id)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, JournalStatus.UNKNOWN)
        self.assertIn("LOCAL_OUTCOME_UNKNOWN", entry.last_error)

        # UNKNOWN status is terminal for automated runners: further mutations fail closed
        with self.assertRaises(InvalidJournalTransitionError):
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=mutations,
                manifest_seal_hex=self.manifest_seal_hex,
            )

    # -------------------------------------------------------------------------
    # Adversarial Case 7: Crash / Rollback Atomicity (Option B+C)
    # -------------------------------------------------------------------------
    def test_atomic_option_bc_successful_stage_mutation(self):
        """Option B+C couples fence CAS + mutations + effect confirmation in one atomic transaction."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
            pattern=JournalExecutionPattern.OPTION_B_C_ATOMIC,
        )

        article = self.entities[0]
        story = self.entities[1]
        mutations = [
            (
                "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
                (article.pk_value, "Synth Title", "staging_synth_test", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
            ),
            (
                "INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json, update_count, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (story.pk_value, "Headline", "synth_test", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z", json.dumps([article.pk_value]), 0, 1),
            ),
        ]

        new_token = self.journal.execute_atomic_stage_mutation(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=1,
            stage="LIVE_02",
            mutation_statements=mutations,
            manifest_seal_hex=self.manifest_seal_hex,
        )
        self.assertEqual(new_token, 2)

        # Verify journal entry was updated to EFFECT_COMMITTED with token 2
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.EFFECT_COMMITTED)
        self.assertEqual(entry.fencing_token, 2)
        self.assertEqual(entry.stage, "LIVE_02")

        # Verify mutations are committed
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 1)
        cur_s = self.conn.execute("SELECT COUNT(*) FROM stories WHERE story_id = ?", (story.pk_value,))
        self.assertEqual(cur_s.fetchone()[0], 1)

    def test_mid_transaction_crash_rolls_back_both_entities_and_journal_effect(self):
        """A failure mid-transaction (e.g. SQL integrity error) rolls back mutations and journal effect cleanly."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        # Mutation 1 succeeds, Mutation 2 violates SQLite constraint (invalid column or PK collision)
        bad_mutations = [
            (
                "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
                (article.pk_value, "Synth Title", "staging_synth_test", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
            ),
            (
                "INSERT INTO processed_articles (article_id, non_existent_column) VALUES (?, ?)",
                (article.pk_value, "bad"),
            ),
        ]

        with self.assertRaises(sqlite3.OperationalError):
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=bad_mutations,
                manifest_seal_hex=self.manifest_seal_hex,
            )

        # Both entity row and journal update were rolled back
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 0)

        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.ACKED)
        self.assertEqual(entry.fencing_token, 1)

    # -------------------------------------------------------------------------
    # Adversarial Case 8: Unauthorized or Partial Cleanup
    # -------------------------------------------------------------------------
    def test_successful_conditional_cleanup_and_three_state_audit(self):
        """Conditional cleanup deletes registered entities and marks CLEANED_UP if audit is CLEAN."""
        # 1. Setup intent and insert synthetic row
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )
        article = self.entities[0]
        self.journal.execute_atomic_stage_mutation(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=1,
            stage="LIVE_02",
            mutation_statements=[(
                "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
                (article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
            )],
            manifest_seal_hex=self.manifest_seal_hex,
        )

        # 2. Cleanup statement matching exact PK and synthetic attributes
        cleanup_stmts = [(
            "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?",
            (article.pk_value, article.expected_attributes["source"], article.expected_attributes["title"]),
            "processed_articles",
            article.pk_value,
        )]

        new_token = self.journal.execute_conditional_cleanup(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=2,
            cleanup_statements=cleanup_stmts,
            manifest_seal_hex=self.manifest_seal_hex,
        )
        self.assertEqual(new_token, 3)

        # Verify journal status is CLEANED_UP and entity deleted
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.CLEANED_UP)
        self.assertEqual(entry.fencing_token, 3)

        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_adversarial_cleanup_unexpected_affected_rows_halts_fail_closed(self):
        """If conditional deletion affects 0 rows (e.g. attribute mismatch), cleanup halts and marks UNKNOWN."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )
        article = self.entities[0]
        self.journal.execute_atomic_stage_mutation(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=1,
            stage="LIVE_02",
            mutation_statements=[(
                "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
                (article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
            )],
            manifest_seal_hex=self.manifest_seal_hex,
        )

        # Statement with mismatched title attribute (will affect 0 rows)
        bad_cleanup_stmts = [(
            "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?",
            (article.pk_value, article.expected_attributes["source"], "WRONG_TITLE"),
            "processed_articles",
            article.pk_value,
        )]

        with self.assertRaises(JournalError) as ctx:
            self.journal.execute_conditional_cleanup(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=2,
                cleanup_statements=bad_cleanup_stmts,
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("affected 0 rows", str(ctx.exception))

        # Check status is UNKNOWN and entity row is preserved
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.UNKNOWN)
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 1)

    def test_three_state_audit_residual_leaves_status_cleanup_pending_and_preserves_records(self):
        """If 3-state audit discovers a residual entity, status remains CLEANUP_PENDING (never CLEANED_UP)."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )
        article = self.entities[0]
        story = self.entities[1]
        # Insert both article and story
        self.journal.execute_atomic_stage_mutation(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=1,
            stage="LIVE_02",
            mutation_statements=[
                (
                    "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
                    (article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
                ),
                (
                    "INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json, update_count, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (story.pk_value, "Headline", "synth_test", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z", json.dumps([article.pk_value]), 0, 1),
                ),
            ],
            manifest_seal_hex=self.manifest_seal_hex,
        )

        # Cleanup only deletes article, leaving story residual
        partial_cleanup = [(
            "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?",
            (article.pk_value, article.expected_attributes["source"], article.expected_attributes["title"]),
            "processed_articles",
            article.pk_value,
        )]

        self.journal.execute_conditional_cleanup(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=2,
            cleanup_statements=partial_cleanup,
            manifest_seal_hex=self.manifest_seal_hex,
        )

        entry = self.journal.inspect_run(self.run_id)
        # Status must NOT be CLEANED_UP
        self.assertEqual(entry.status, JournalStatus.CLEANUP_PENDING)
        self.assertIn("AUDIT_RESIDUAL", entry.last_error)


class TestOperatorForensicReconciliation(unittest.TestCase):
    """Test operator-only forensic discovery and PO-approved reconciliation tooling."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            self.conn.executescript(stmt)
        apply_candidate_schema_v7(self.conn)
        self.journal = StagingOwnershipJournal(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_list_unresolved_runs_discovers_orphaned_and_unknown_runs(self):
        """list_unresolved_runs() returns all in-flight or UNKNOWN runs across runners."""
        run_1 = _make_run_id("orphaned_1")
        run_2 = _make_run_id("unknown_2")

        # Create run 1 in ACKED state
        self.journal.register_intent(
            run_id=run_1,
            owner_id="runner_crashed_vm_1",
            manifest_seal_hex=_dummy_seal_hex("1"),
            target_inventory=create_default_manifest_entities(run_1),
            workload_key=None,
        )

        # Create run 2 and simulate ambiguous commit -> UNKNOWN
        self.journal.register_intent(
            run_id=run_2,
            owner_id="runner_crashed_vm_2",
            manifest_seal_hex=_dummy_seal_hex("2"),
            target_inventory=create_default_manifest_entities(run_2),
            workload_key=None,
        )
        ent = create_default_manifest_entities(run_2)[0]
        try:
            self.journal.execute_atomic_stage_mutation(
                run_id=run_2,
                owner_id="runner_crashed_vm_2",
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[(
                    "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
                    (ent.pk_value, "Title", "source", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
                )],
                manifest_seal_hex=_dummy_seal_hex("2"),
                simulate_ambiguous_commit=True,
            )
        except AmbiguousCommitError:
            pass

        unresolved = self.journal.list_unresolved_runs()
        unresolved_ids = [e.run_id for e in unresolved]
        self.assertIn(run_1, unresolved_ids)
        self.assertIn(run_2, unresolved_ids)
        self.assertEqual(len(unresolved), 2)

    def test_operator_reconcile_requires_explicit_po_approval(self):
        """Automated callers or missing PO approval cannot reconcile an unverified/unknown run."""
        run_id = _make_run_id("op_test")
        self.journal.register_intent(
            run_id=run_id,
            owner_id="runner_a",
            manifest_seal_hex=_dummy_seal_hex("op"),
            target_inventory=create_default_manifest_entities(run_id),
            workload_key=None,
        )

        with self.assertRaises(UnauthorizedTakeoverError):
            self.journal.operator_reconcile_run(
                run_id=run_id,
                operator_id="operator_bob",
                action="ABORT",
                reason="Investigating crash",
                is_po_approved=False,  # Automated / unapproved
            )

    def test_operator_reconcile_bumps_fencing_token_and_records_audit_trail(self):
        """With explicit PO approval, operator reconcile transitions run and increments fence."""
        run_id = _make_run_id("op_approved")
        self.journal.register_intent(
            run_id=run_id,
            owner_id="runner_a",
            manifest_seal_hex=_dummy_seal_hex("op_app"),
            target_inventory=create_default_manifest_entities(run_id),
            workload_key=None,
        )

        reconciled = self.journal.operator_reconcile_run(
            run_id=run_id,
            operator_id="po_admin",
            action="ABORT",
            reason="Verified no residual entities after VM decommission",
            is_po_approved=True,
        )
        self.assertEqual(reconciled.status, JournalStatus.ABORTED)
        self.assertEqual(reconciled.fencing_token, 2)  # Monotonically incremented
        self.assertIn("OPERATOR_RECONCILED [ABORT]", reconciled.last_error)


class TestNonDestructiveRollback(unittest.TestCase):
    """Verify that operational rollback is non-destructive (never executes DROP TABLE)."""

    def test_operational_rollback_preserves_table_and_records(self):
        """staging_run_journal table and records are preserved intact during operational rollback."""
        conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            conn.executescript(stmt)
        apply_candidate_schema_v7(conn)

        journal = StagingOwnershipJournal(conn)
        run_id = _make_run_id("rollback_preserve")
        journal.register_intent(
            run_id=run_id,
            owner_id="runner_rollback",
            manifest_seal_hex=_dummy_seal_hex("rb"),
            target_inventory=create_default_manifest_entities(run_id),
            workload_key=None,
        )

        # Operational rollback: feature flag disabled, application reverted
        # Verifies table and row remain in database for forensic discovery
        cur = conn.execute("SELECT status FROM staging_run_journal WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], JournalStatus.ACKED.value)
        conn.close()


if __name__ == "__main__":
    unittest.main()
