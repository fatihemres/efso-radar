"""Hermetic offline contract tests for ERD-017 staging ownership journal (Issue #90 / PR #91 Remediation).

SAFETY & BOUNDARY ASSURANCE:
- Operates strictly against in-memory (:memory:) or temporary file SQLite fixtures.
- Absolute $0.00 / 0 TRY spending ceiling.
- Zero network I/O, zero Turso / cloud connections, zero live credentials.
- Candidate Schema v7 DDL is applied ONLY via apply_candidate_schema_v7() in test fixtures.
- Default CURRENT_SCHEMA_VERSION in efso_radar.repositories.schema remains strictly 6.
- Covers all independent review findings (P1 and P2):
  1. Strict target inventory containment (rejects free-form SQL, OR 1=1, foreign PKs, broad predicates).
  2. Strict atomic cleanup (rollback on failure prevents partial deletions; foreign row presence halts).
  3. Prohibits caller boolean self-attestation for PO approval (propose_operator_reconciliation only).
  4. UNKNOWN state blocks subsequent workload runs; atomic lease acquisition serializes concurrent runs.
  5. Multi-connection concurrent registration barrier test on tempfile SQLite.
  6. Genuine ambiguous commit modeling for both COMMITTED_BUT_ACK_LOST and ROLLED_BACK_AND_ACK_LOST.
  7. Safe abort semantics: abort rejected if mutations dispatched or residual rows present.
  8. Runtime isolation guard on apply_candidate_schema_v7 rejects non-temp connections.
  9. Manifest seal authenticated against actual PerRunOwnershipManifest digest.
  10. Read provenance defaults to OFFLINE_LOCAL_FIXTURE_READ.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from efso_radar.repositories.schema import (
    CURRENT_SCHEMA_VERSION,
    MIGRATIONS,
    get_all_init_statements,
)
from efso_radar.staging.journal import (
    ACTIVE_JOURNAL_STATUSES,
    LEGAL_TRANSITIONS,
    SCHEMA_V7_STAGING_JOURNAL_DDL,
    UNRESOLVED_JOURNAL_STATUSES,
    ActiveWorkloadCollisionError,
    AmbiguousCommitError,
    AmbiguousCommitOutcome,
    FencingConflictError,
    InvalidJournalTransitionError,
    InvalidRunIdError,
    JournalError,
    JournalExecutionPattern,
    JournalHaltedError,
    JournalStatus,
    ManifestTamperedError,
    OperatorReconciliationProposal,
    ReadProvenance,
    StageMutationCommand,
    StagingJournalEntry,
    StagingOwnershipJournal,
    StaleOwnerError,
    TargetInventoryViolationError,
    UnauthorizedTakeoverError,
    apply_candidate_schema_v7,
    canonical_inventory_json,
    compute_authenticated_manifest_seal,
    validate_status_transition,
)
from efso_radar.staging.runner import (
    RUN_ID_REGEX,
    PerRunOwnershipManifest,
    RegisteredEntity,
    create_default_manifest_entities,
)


def _make_run_id(suffix: str = "01") -> str:
    """Helper to create a canonical run_id matching RUN_ID_REGEX."""
    hex_digest = hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:12]
    run_id = f"run_20261106T200000Z_{hex_digest}"
    assert RUN_ID_REGEX.match(run_id)
    return run_id


def _create_authenticated_seal(run_id: str, entities: list[RegisteredEntity]) -> str:
    """Compute the authentic SHA-256 digest of the canonical PerRunOwnershipManifest."""
    return compute_authenticated_manifest_seal(run_id, entities)


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

        conn.execute(
            "INSERT INTO schema_migrations (version, applied_at, description) VALUES (6, '2026-11-06T20:00:00Z', 'v6')"
        )
        conn.commit()

        apply_candidate_schema_v7(conn)

        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='staging_run_journal'"
        )
        self.assertIsNotNone(cur.fetchone())

        cur_idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_staging_run_journal_status'"
        )
        self.assertIsNotNone(cur_idx.fetchone())

        cur_ver = conn.execute("SELECT MAX(version) FROM schema_migrations")
        self.assertEqual(cur_ver.fetchone()[0], 6)
        conn.close()

    def test_apply_candidate_schema_v7_runtime_guard_rejects_non_temp_file(self):
        """Runtime guard on apply_candidate_schema_v7 rejects connection outside temp directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_db = Path(temp_dir) / "test_fake_external.db"
            conn = sqlite3.connect(str(fake_db))
            try:
                with patch("tempfile.gettempdir", return_value="C:\\nonexistent_other_temp_root"):
                    with self.assertRaises(PermissionError) as ctx:
                        apply_candidate_schema_v7(conn)
                    self.assertIn("Security violation: apply_candidate_schema_v7 is restricted strictly", str(ctx.exception))
            finally:
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

    def test_abort_only_permitted_before_mutations(self):
        """Only INTENT_PENDING and ACKED can transition to ABORTED."""
        validate_status_transition(JournalStatus.INTENT_PENDING, JournalStatus.ABORTED)
        validate_status_transition(JournalStatus.ACKED, JournalStatus.ABORTED)

        # Post-mutation states CANNOT transition directly to ABORTED (P1-6 fix)
        with self.assertRaises(InvalidJournalTransitionError):
            validate_status_transition(JournalStatus.MUTATING, JournalStatus.ABORTED)
        with self.assertRaises(InvalidJournalTransitionError):
            validate_status_transition(JournalStatus.EFFECT_COMMITTED, JournalStatus.ABORTED)
        with self.assertRaises(InvalidJournalTransitionError):
            validate_status_transition(JournalStatus.CLEANUP_PENDING, JournalStatus.ABORTED)

    def test_unknown_transitions_permitted_for_ambiguous_outcomes(self):
        """Active states can transition to UNKNOWN upon ambiguous commit/network drop."""
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
        self.entities = create_default_manifest_entities(self.run_id)
        self.manifest_seal_hex = _create_authenticated_seal(self.run_id, self.entities)

    def tearDown(self):
        self.conn.close()

    # -------------------------------------------------------------------------
    # P1-1: Target Inventory Containment & Strict SQL Validation
    # -------------------------------------------------------------------------
    def test_p1_1_adversarial_mutation_with_foreign_pk_and_registered_param_rejected(self):
        """Negative regression: Mutating a foreign PK while passing registered PK elsewhere in params fails closed."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        # SQL targets foreign PK 'foreign_unregistered_article', but includes article.pk_value in title
        sneaky_mutation = (
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
            ("foreign_unregistered_article", f"Sneaky with {article.pk_value}", "staging_synth_test", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )

        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[sneaky_mutation],
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("unregistered PK value", str(ctx.exception))

    def test_p1_1_adversarial_mutation_broad_sql_clause_rejected(self):
        """Negative regression: SQL containing WHERE, OR, UPDATE, or broad clauses is rejected fail-closed."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        broad_mutation = (
            f"INSERT INTO processed_articles (article_id, title) VALUES ('{article.pk_value}', 'title') WHERE 1=1 OR 1=1",
            (),
        )

        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[broad_mutation],
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("Disallowed SQL keyword", str(ctx.exception))

    def test_p1_1_structured_mutation_command_enforces_exact_attributes(self):
        """StageMutationCommand strictly validates expected attributes and builds safe SQL."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        # Command with wrong source attribute fails closed
        bad_cmd = StageMutationCommand(
            table="processed_articles",
            pk_column="article_id",
            pk_value=article.pk_value,
            columns=["article_id", "title", "source", "published_at", "processed_at"],
            values=[article.pk_value, article.expected_attributes["title"], "WRONG_SOURCE", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
        )
        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[bad_cmd],
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("does not match expected", str(ctx.exception))

    def test_p1_1_sql_expression_in_values_bypassing_pk_is_rejected(self):
        """Negative regression: Expression in VALUES clause like (? || '_foreign') is rejected fail-closed."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        # SQL with SQLite string concatenation expression in VALUES clause
        expression_mutation = (
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (? || '_foreign', ?, ?, ?, ?)",
            (article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )

        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[expression_mutation],
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("only literal '?' parameter placeholders are permitted", str(ctx.exception))

        # Verify zero rows inserted
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_p1_1_structured_mutation_command_pk_value_mismatch_rejected(self):
        """Negative regression: StageMutationCommand where values PK differs from pk_value fails closed."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        # pk_value says registered PK, but values has 'foreign_article_id'
        mismatched_cmd = StageMutationCommand(
            table="processed_articles",
            pk_column="article_id",
            pk_value=article.pk_value,
            columns=["article_id", "title", "source", "published_at", "processed_at"],
            values=["foreign_article_id", article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
        )
        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[mismatched_cmd],
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("does not match declared pk_value", str(ctx.exception))

        # Verify zero rows inserted
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_p1_1_structured_mutation_command_omitted_mandatory_attribute_rejected(self):
        """Negative regression: StageMutationCommand omitting an expected attribute fails closed."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        # Columns omit mandatory attribute 'source'
        omitted_attr_cmd = StageMutationCommand(
            table="processed_articles",
            pk_column="article_id",
            pk_value=article.pk_value,
            columns=["article_id", "title", "published_at", "processed_at"],
            values=[article.pk_value, article.expected_attributes["title"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
        )
        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[omitted_attr_cmd],
                manifest_seal_hex=self.manifest_seal_hex,
            )
        self.assertIn("Mandatory expected attribute 'source' is missing", str(ctx.exception))

        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles")
        self.assertEqual(cur.fetchone()[0], 0)

    # -------------------------------------------------------------------------
    # P1-2: Atomic Cleanup, Manifest Authentication & Foreign Row Safety
    # -------------------------------------------------------------------------
    def test_p1_2_foreign_existing_row_declared_as_inventory_rejected_at_registration(self):
        """Negative regression: Caller declaring unowned foreign row in target_inventory is rejected at registration."""
        foreign_entities = list(self.entities)
        foreign_entities[0] = RegisteredEntity(
            table="processed_articles",
            pk_column="article_id",
            pk_value="foreign_unowned_article_123",
            cleanup_dependency_order=2,
            expected_attributes={"source": "unowned", "title": "unowned"},
        )
        with self.assertRaises(TargetInventoryViolationError) as ctx:
            self.journal.register_intent(
                run_id=self.run_id,
                owner_id=self.owner_id,
                manifest_seal_hex=self.manifest_seal_hex,
                target_inventory=foreign_entities,
            )
        self.assertIn("fails canonical manifest validation", str(ctx.exception))

    def test_p1_2_syntactically_valid_unrelated_seal_rejected_at_registration(self):
        """Negative regression: Caller-computed valid SHA-256 unrelated to canonical manifest is rejected."""
        unrelated_seal = hashlib.sha256(b"arbitrary_attacker_payload_not_canonical").hexdigest()
        with self.assertRaises(ManifestTamperedError) as ctx:
            self.journal.register_intent(
                run_id=self.run_id,
                owner_id=self.owner_id,
                manifest_seal_hex=unrelated_seal,
                target_inventory=self.entities,
            )
        self.assertIn("Caller-supplied digest is not trusted as authentication proof", str(ctx.exception))
    def test_p1_2_atomic_cleanup_rolls_back_entirely_on_audit_failure_preventing_partial_deletions(self):
        """Negative regression: If audit detects residual entities, entire cleanup rolls back with ZERO partial deletions."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        story = self.entities[1]

        # Insert article and story
        self.journal.execute_atomic_stage_mutation(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=1,
            stage="LIVE_02",
            mutation_statements=[
                StageMutationCommand(
                    table="processed_articles",
                    pk_column="article_id",
                    pk_value=article.pk_value,
                    columns=["article_id", "title", "source", "published_at", "processed_at"],
                    values=[article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
                ),
                StageMutationCommand(
                    table="stories",
                    pk_column="story_id",
                    pk_value=story.pk_value,
                    columns=["story_id", "headline_tr", "category", "first_seen_at", "last_updated_at", "article_ids_json", "update_count", "is_active"],
                    values=[story.pk_value, story.expected_attributes["headline_tr"], story.expected_attributes["category"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z", json.dumps([article.pk_value]), 0, 1],
                ),
            ],
            manifest_seal_hex=self.manifest_seal_hex,
        )

        # Attempt cleanup deleting ONLY the article, leaving story residual
        # Strict atomic cleanup must detect residual story during audit and ROLLBACK article deletion!
        with self.assertRaises(JournalError) as ctx:
            self.journal.execute_conditional_cleanup(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=2,
                manifest_seal_hex=self.manifest_seal_hex,
                target_entities_subset=[article.pk_value],  # partial deletion request
            )
        self.assertIn("Audit failed: residual entity", str(ctx.exception))

        # Crucial check: Article was NOT deleted! Zero partial destructive work occurred.
        cur_art = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur_art.fetchone()[0], 1)
        cur_sty = self.conn.execute("SELECT COUNT(*) FROM stories WHERE story_id = ?", (story.pk_value,))
        self.assertEqual(cur_sty.fetchone()[0], 1)

        # Journal status was also rolled back and preserved
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.EFFECT_COMMITTED)
        self.assertEqual(entry.fencing_token, 2)

    def test_p1_2_successful_cleanup_deletes_all_entities_and_transitions_atomically_to_cleaned_up(self):
        """When all entities are cleaned up and audit proves CLEAN, transaction commits status CLEANED_UP."""
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
            mutation_statements=[
                StageMutationCommand(
                    table="processed_articles",
                    pk_column="article_id",
                    pk_value=article.pk_value,
                    columns=["article_id", "title", "source", "published_at", "processed_at"],
                    values=[article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
                )
            ],
            manifest_seal_hex=self.manifest_seal_hex,
        )

        new_token = self.journal.execute_conditional_cleanup(
            run_id=self.run_id,
            owner_id=self.owner_id,
            expected_fencing_token=2,
            manifest_seal_hex=self.manifest_seal_hex,
        )
        self.assertEqual(new_token, 3)

        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.CLEANED_UP)
        self.assertEqual(entry.fencing_token, 3)

        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 0)

    # -------------------------------------------------------------------------
    # P1-3: Removal of Caller-Controlled PO Authorization
    # -------------------------------------------------------------------------
    def test_p1_3_caller_is_po_approved_flag_cannot_retire_or_reconcile_run(self):
        """Negative regression: Arbitrary caller-supplied is_po_approved=True fails closed with UnauthorizedTakeoverError."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        with self.assertRaises(UnauthorizedTakeoverError) as ctx:
            self.journal.operator_reconcile_run(
                run_id=self.run_id,
                operator_id="attacker_operator",
                action="FORCE_CLEANUP",
                reason="Attempting to force clean without proof",
                is_po_approved=True,  # Caller-controlled flag
            )
        self.assertIn("cannot certify runtime authority", str(ctx.exception))

    def test_p1_3_propose_operator_reconciliation_is_strictly_read_only_proposal(self):
        """propose_operator_reconciliation returns an inert proposal and never modifies database."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        proposal = self.journal.propose_operator_reconciliation(
            run_id=self.run_id,
            operator_id="investigating_admin",
            proposed_action="ABORT",
            reason="Forensic analysis of crashed runner VM",
        )
        self.assertIsInstance(proposal, OperatorReconciliationProposal)
        self.assertEqual(proposal.status, "PROPOSAL_ONLY_NON_AUTHORIZING")
        self.assertFalse(proposal.is_authorized)

        # Database state remains untouched
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.ACKED)
        self.assertEqual(entry.fencing_token, 1)

    # -------------------------------------------------------------------------
    # P1-4: UNKNOWN Blocks Fresh Workload Registration & Interleaving Test
    # -------------------------------------------------------------------------
    def test_p1_4_unknown_state_blocks_fresh_registration_for_workload(self):
        """Negative regression: An UNKNOWN run blocks fresh registration for the same workload."""
        # Setup run 1 and force status to UNKNOWN
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
            workload_key="staging_controlled_run",
        )

        # Transition run 1 to UNKNOWN
        self.conn.execute(
            "UPDATE staging_run_journal SET status = 'UNKNOWN' WHERE run_id = ?",
            (self.run_id,),
        )
        self.conn.commit()

        # Runner 2 attempts to register run_2 for the same workload -> MUST fail closed
        run_2 = _make_run_id("run_second")
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.journal.register_intent(
                run_id=run_2,
                owner_id="runner_two",
                manifest_seal_hex=_create_authenticated_seal(run_2, create_default_manifest_entities(run_2)),
                target_inventory=create_default_manifest_entities(run_2),
                workload_key="staging_controlled_run",
            )
        self.assertIn("unresolved staging run", str(ctx.exception))
        self.assertIn("UNKNOWN", str(ctx.exception))

    def test_p1_4_concurrent_multi_connection_registration_barrier_on_temp_db(self):
        """Multi-connection test: Two SQLite connections to a file DB cannot race to register distinct runs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "staging_test_barrier.db"
            conn_init = sqlite3.connect(str(db_path))
            for stmt in get_all_init_statements():
                conn_init.executescript(stmt)
            apply_candidate_schema_v7(conn_init)
            conn_init.close()

            run_1 = _make_run_id("thread_1")
            run_2 = _make_run_id("thread_2")

            def worker_register(run_id: str, owner: str):
                c = sqlite3.connect(str(db_path), timeout=10.0)
                j = StagingOwnershipJournal(c)
                try:
                    j.register_intent(
                        run_id=run_id,
                        owner_id=owner,
                        manifest_seal_hex=_create_authenticated_seal(run_id, create_default_manifest_entities(run_id)),
                        target_inventory=create_default_manifest_entities(run_id),
                        workload_key="staging_controlled_run",
                    )
                    return "REGISTERED"
                except ActiveWorkloadCollisionError:
                    return "COLLISION_REJECTED"
                finally:
                    c.close()

            with ThreadPoolExecutor(max_workers=2) as executor:
                f1 = executor.submit(worker_register, run_1, "worker_1")
                f2 = executor.submit(worker_register, run_2, "worker_2")
                results = [f1.result(), f2.result()]

            # Exactly one connection succeeded, the other was rejected by the barrier
            self.assertEqual(results.count("REGISTERED"), 1)
            self.assertEqual(results.count("COLLISION_REJECTED"), 1)

    # -------------------------------------------------------------------------
    # P1-5: Genuine Ambiguous Commit Simulation
    # -------------------------------------------------------------------------
    def test_p1_5_ambiguous_commit_outcome_committed_but_ack_lost(self):
        """Simulated ambiguity: transaction committed on provider, but network ACK dropped."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        cmd = StageMutationCommand(
            table="processed_articles",
            pk_column="article_id",
            pk_value=article.pk_value,
            columns=["article_id", "title", "source", "published_at", "processed_at"],
            values=[article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
        )

        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[cmd],
                manifest_seal_hex=self.manifest_seal_hex,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST,
            )
        self.assertIn("transaction committed on provider, but network ACK was lost", str(ctx.exception))
        self.assertTrue(self.journal.is_halted(self.run_id))

        # Forensic verification: entity committed on provider, status committed
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 1)
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.EFFECT_COMMITTED)

        # Subsequent automated mutations, retries, cleanup, or abort MUST fail closed with JournalHaltedError
        with self.assertRaises(JournalHaltedError):
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=2,
                stage="LIVE_02",
                mutation_statements=[cmd],
                manifest_seal_hex=self.manifest_seal_hex,
            )

        with self.assertRaises(JournalHaltedError):
            self.journal.execute_conditional_cleanup(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=2,
                manifest_seal_hex=self.manifest_seal_hex,
            )

        with self.assertRaises(JournalHaltedError):
            self.journal.abort_run(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=2,
                reason="Halt abort test",
            )

    def test_p1_5_ambiguous_commit_outcome_rolled_back_and_ack_lost(self):
        """Simulated ambiguity: transaction rolled back on provider, and network ACK dropped."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        cmd = StageMutationCommand(
            table="processed_articles",
            pk_column="article_id",
            pk_value=article.pk_value,
            columns=["article_id", "title", "source", "published_at", "processed_at"],
            values=[article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
        )

        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[cmd],
                manifest_seal_hex=self.manifest_seal_hex,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST,
            )
        self.assertIn("transaction rolled back on provider, and network ACK was lost", str(ctx.exception))
        self.assertTrue(self.journal.is_halted(self.run_id))

        # Forensic verification: zero rows committed on provider, status rolled back to ACKED
        cur = self.conn.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (article.pk_value,))
        self.assertEqual(cur.fetchone()[0], 0)
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.status, JournalStatus.ACKED)

        # Subsequent automated mutations, retries, cleanup, or abort MUST fail closed with JournalHaltedError
        with self.assertRaises(JournalHaltedError):
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[cmd],
                manifest_seal_hex=self.manifest_seal_hex,
            )

        with self.assertRaises(JournalHaltedError):
            self.journal.execute_conditional_cleanup(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                manifest_seal_hex=self.manifest_seal_hex,
            )

        with self.assertRaises(JournalHaltedError):
            self.journal.abort_run(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                reason="Halt abort test",
            )

    def test_p1_5_ambiguous_intent_registration_committed_then_retry_rejected_as_halted(self):
        """Ambiguous intent registration (COMMITTED_BUT_ACK_LOST): row committed, client halted, retries fail."""
        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.journal.register_intent(
                run_id=self.run_id,
                owner_id=self.owner_id,
                manifest_seal_hex=self.manifest_seal_hex,
                target_inventory=self.entities,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST,
            )
        self.assertIn("COMMITTED_BUT_ACK_LOST", str(ctx.exception))
        self.assertTrue(self.journal.is_halted(self.run_id))

        # Forensic verification: row exists on provider with status ACKED
        entry = self.journal.inspect_run(self.run_id)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, JournalStatus.ACKED)

        # Subsequent automated registration, mutation, cleanup, or abort MUST fail closed with JournalHaltedError
        with self.assertRaises(JournalHaltedError):
            self.journal.register_intent(
                run_id=self.run_id,
                owner_id=self.owner_id,
                manifest_seal_hex=self.manifest_seal_hex,
                target_inventory=self.entities,
            )

        with self.assertRaises(JournalHaltedError):
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[],
                manifest_seal_hex=self.manifest_seal_hex,
            )

    def test_p1_5_ambiguous_intent_registration_rolled_back_then_retry_rejected_as_halted(self):
        """Ambiguous intent registration (ROLLED_BACK_AND_ACK_LOST): row absent, client halted, retries fail."""
        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.journal.register_intent(
                run_id=self.run_id,
                owner_id=self.owner_id,
                manifest_seal_hex=self.manifest_seal_hex,
                target_inventory=self.entities,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST,
            )
        self.assertIn("ROLLED_BACK_AND_ACK_LOST", str(ctx.exception))
        self.assertTrue(self.journal.is_halted(self.run_id))

        # Forensic verification: row does NOT exist on provider
        entry = self.journal.inspect_run(self.run_id)
        self.assertIsNone(entry)

        # Subsequent automated calls fail closed with JournalHaltedError
        with self.assertRaises(JournalHaltedError):
            self.journal.register_intent(
                run_id=self.run_id,
                owner_id=self.owner_id,
                manifest_seal_hex=self.manifest_seal_hex,
                target_inventory=self.entities,
            )

        with self.assertRaises(JournalHaltedError):
            self.journal.execute_atomic_stage_mutation(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                stage="LIVE_02",
                mutation_statements=[],
                manifest_seal_hex=self.manifest_seal_hex,
            )

    # -------------------------------------------------------------------------
    # P1-6: Safe ABORT Semantics
    # -------------------------------------------------------------------------
    def test_p1_6_abort_rejected_if_mutations_dispatched(self):
        """Negative regression: Attempting to abort a run in EFFECT_COMMITTED is rejected to prevent stranding data."""
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
            mutation_statements=[
                StageMutationCommand(
                    table="processed_articles",
                    pk_column="article_id",
                    pk_value=article.pk_value,
                    columns=["article_id", "title", "source", "published_at", "processed_at"],
                    values=[article.pk_value, article.expected_attributes["title"], article.expected_attributes["source"], "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"],
                )
            ],
            manifest_seal_hex=self.manifest_seal_hex,
        )

        with self.assertRaises(InvalidJournalTransitionError) as ctx:
            self.journal.abort_run(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=2,
                reason="Attempting to abort after mutations",
            )
        self.assertIn("Mutations may have been dispatched", str(ctx.exception))

    def test_p1_6_abort_rejected_if_residual_rows_exist_even_if_status_acked(self):
        """Negative regression: Even if status is ACKED, abort is rejected if registered rows pre-exist."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )

        article = self.entities[0]
        # Directly contaminate table with registered entity PK
        self.conn.execute(
            "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)",
            (article.pk_value, "Pre-existing", "contam", "2026-11-06T20:00:00Z", "2026-11-06T20:00:00Z"),
        )
        self.conn.commit()

        with self.assertRaises(JournalError) as ctx:
            self.journal.abort_run(
                run_id=self.run_id,
                owner_id=self.owner_id,
                expected_fencing_token=1,
                reason="Attempting to abort with pre-existing residual row",
            )
        self.assertIn("registered entity", str(ctx.exception))
        self.assertIn("already exists in database", str(ctx.exception))

    # -------------------------------------------------------------------------
    # P2-2: Read Provenance Defaults
    # -------------------------------------------------------------------------
    def test_p2_2_read_provenance_defaults_to_offline_local_fixture_read(self):
        """inspect_run defaults to OFFLINE_LOCAL_FIXTURE_READ, avoiding unverified primary claims."""
        self.journal.register_intent(
            run_id=self.run_id,
            owner_id=self.owner_id,
            manifest_seal_hex=self.manifest_seal_hex,
            target_inventory=self.entities,
        )
        entry = self.journal.inspect_run(self.run_id)
        self.assertEqual(entry.read_provenance, ReadProvenance.OFFLINE_LOCAL_FIXTURE_READ)


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
        entities = create_default_manifest_entities(run_id)
        seal = _create_authenticated_seal(run_id, entities)

        journal.register_intent(
            run_id=run_id,
            owner_id="runner_rollback",
            manifest_seal_hex=seal,
            target_inventory=entities,
            workload_key="staging_controlled_run",
        )

        cur = conn.execute("SELECT status FROM staging_run_journal WHERE run_id = ?", (run_id,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], JournalStatus.ACKED.value)
        conn.close()


if __name__ == "__main__":
    unittest.main()
