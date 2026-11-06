"""Adversarial Offline Integration Suite: Journal <-> Mock Hrana Seam (Issue #92 / ERD-017).

OFFLINE ENGINEERING VERIFICATION ONLY:
- Operates strictly against isolated, temporary file-backed SQLite database fixtures in tempfile.gettempdir().
- Two independent synthetic runner sessions (Runner A and Runner B) with separate connections,
  transports, clients, and scoped marker directories.
- Zero network I/O, zero Turso/cloud connections, zero credentials or tokens.
- Absolute $0.00 / 0 TRY spending ceiling.
- Covers all 8 PO-mandated adversarial scenarios:
  1. Duplicate run ID and distinct run IDs targeting the same workload.
  2. Stale-owner attempts and fencing races.
  3. Atomic rollback on failed mutation or journal update.
  4. Ambiguous intent COMMIT and effect COMMIT: committed/ACK-lost and rolled-back/ACK-lost.
  5. Crash before and after COMMIT.
  6. Runner B reconstruction without access to Runner A's local marker files.
  7. No blind retry, takeover, or cleanup on UNKNOWN.
  8. Exact inventory ownership, conditional deletion, and CLEAN/RESIDUAL/UNKNOWN audits.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import TursoHTTPClient
from efso_radar.staging.journal import (
    ActiveWorkloadCollisionError,
    AmbiguousCommitError,
    AmbiguousCommitOutcome,
    FencingConflictError,
    HaltMarkerPersistenceError,
    JournalError,
    JournalExecutionPattern,
    JournalHaltedError,
    JournalStatus,
    ManifestTamperedError,
    StageMutationCommand,
    StaleOwnerError,
    TargetInventoryViolationError,
    apply_candidate_schema_v7,
    compute_canonical_inventory_fingerprint,
)
from efso_radar.staging.journal_hrana_seam import (
    StagingJournalHranaSeam,
    build_live02_synthetic_mutations,
    _assert_fixture_isolation,
)
from efso_radar.staging.offline_recovery_evidence import (
    ReviewDecision,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
    PerRunOwnershipManifest,
    create_default_manifest_entities,
)

RUN_ID_A = "run_20261107T120000Z_a1b2c3d4e5f6"
RUN_ID_B = "run_20261107T120100Z_b2c3d4e5f6a1"
WORKLOAD_KEY = "staging_controlled_run"


def _init_shared_db(db_path: Path) -> None:
    """Initialize candidate Schema v6 + candidate Schema v7 on shared test database file."""
    conn = sqlite3.connect(db_path)
    try:
        # Enable WAL mode for concurrent multi-connection testing
        conn.execute("PRAGMA journal_mode=WAL;")
        for stmt in get_all_init_statements():
            conn.execute(stmt)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-11-07T12:00:00Z', 'Schema v6 baseline')"
        )
        conn.commit()
        apply_candidate_schema_v7(conn)
    finally:
        conn.close()


class _BaseTwoRunnerFixture(unittest.TestCase):
    """Base fixture providing two independent synthetic runner sessions on a shared temp DB."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.base_dir = Path(self.temp_dir.name)

        self.db_path = self.base_dir / "shared_mock_staging.db"
        _init_shared_db(self.db_path)

        # Scoped, completely isolated marker directories for Runner A and Runner B
        self.marker_dir_a = self.base_dir / "runner_a_markers"
        self.marker_dir_a.mkdir()
        self.marker_dir_b = self.base_dir / "runner_b_markers"
        self.marker_dir_b.mkdir()

        # Runner A session
        self.conn_a = sqlite3.connect(self.db_path, timeout=5.0)
        self.addCleanup(self.conn_a.close)
        self.transport_a = HermeticStagingTransport(self.conn_a)
        self.budget_a = BudgetTrackingTransport(self.transport_a)
        self.client_a = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-offline-only",
            transport=self.budget_a,
        )
        self.addCleanup(self.client_a.close)
        self.seam_a = StagingJournalHranaSeam(
            client=self.client_a,
            owner_id="gha_runner_a_worker_01",
            marker_dir=self.marker_dir_a,
            workload_key=WORKLOAD_KEY,
        )

        # Runner B session
        self.conn_b = sqlite3.connect(self.db_path, timeout=5.0)
        self.addCleanup(self.conn_b.close)
        self.transport_b = HermeticStagingTransport(self.conn_b)
        self.budget_b = BudgetTrackingTransport(self.transport_b)
        self.client_b = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-offline-only",
            transport=self.budget_b,
        )
        self.addCleanup(self.client_b.close)
        self.seam_b = StagingJournalHranaSeam(
            client=self.client_b,
            owner_id="gha_runner_b_worker_02",
            marker_dir=self.marker_dir_b,
            workload_key=WORKLOAD_KEY,
        )

        self.entities_a = create_default_manifest_entities(RUN_ID_A)
        self.seal_a = compute_canonical_inventory_fingerprint(RUN_ID_A, self.entities_a)

        self.entities_b = create_default_manifest_entities(RUN_ID_B)
        self.seal_b = compute_canonical_inventory_fingerprint(RUN_ID_B, self.entities_b)


# =============================================================================
# Scenario 1: Duplicate run ID and distinct run IDs targeting the same workload
# =============================================================================

class TestScenario1WorkloadCollisions(_BaseTwoRunnerFixture):
    """Cover duplicate identical run IDs and distinct run IDs on the same workload."""

    def test_duplicate_run_id_registration_rejected(self):
        """Runner B cannot register the exact same run_id already registered by Runner A."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # Runner B tries to register identical RUN_ID_A
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.seam_b.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )
        self.assertIn("already registered in journal", str(ctx.exception))

    def test_distinct_run_id_rejected_while_prior_run_unresolved(self):
        """Runner B cannot register distinct RUN_ID_B while RUN_ID_A is in an unresolved state."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # RUN_ID_A is in ACKED status (unresolved); Runner B tries to register distinct RUN_ID_B
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.seam_b.register_intent(
                run_id=RUN_ID_B,
                manifest_seal_hex=self.seal_b,
                target_inventory=self.entities_b,
            )
        self.assertIn(f"unresolved staging run '{RUN_ID_A}' exists", str(ctx.exception))

    def test_distinct_run_id_admitted_only_after_cleanup(self):
        """Runner B can register distinct RUN_ID_B once RUN_ID_A has reached CLEANED_UP."""
        # Runner A completes lifecycle
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token_after_mut = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )
        self.seam_a.execute_conditional_cleanup(
            run_id=RUN_ID_A,
            expected_fencing_token=token_after_mut,
            manifest_seal_hex=self.seal_a,
        )
        entry_a = self.seam_a.inspect_run(RUN_ID_A)
        self.assertEqual(entry_a.status, JournalStatus.CLEANED_UP)

        # Now Runner B registers distinct RUN_ID_B on the same workload
        entry_b = self.seam_b.register_intent(
            run_id=RUN_ID_B,
            manifest_seal_hex=self.seal_b,
            target_inventory=self.entities_b,
        )
        self.assertEqual(entry_b.run_id, RUN_ID_B)
        self.assertEqual(entry_b.status, JournalStatus.ACKED)


# =============================================================================
# Scenario 2: Stale-owner attempts and fencing races
# =============================================================================

class TestScenario2StaleOwnerAndFencingRaces(_BaseTwoRunnerFixture):
    """Cover stale-owner mutations, fencing races, and CAS verification."""

    def test_stale_owner_cannot_mutate_or_cleanup(self):
        """Runner B cannot dispatch mutations or cleanup for a run owned by Runner A."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        # Runner B attempts to mutate RUN_ID_A
        with self.assertRaises(StaleOwnerError) as ctx:
            self.seam_b.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("does not match registered owner", str(ctx.exception))

        # Runner B attempts to cleanup RUN_ID_A
        with self.assertRaises(StaleOwnerError) as ctx_clean:
            self.seam_b.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("is not registered owner", str(ctx_clean.exception))

        # Verify zero mutations occurred on the shared database
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_fencing_race_stale_token_aborts_all_mutations(self):
        """If fencing token was bumped, delayed attempt with stale token fails CAS and rolls back."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        # First mutation succeeds: bumps fence to 2
        new_token = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )
        self.assertEqual(new_token, 2)

        # Delayed attempt passes stale expected token 1
        with self.assertRaises(FencingConflictError) as ctx:
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-03",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Fencing token mismatch", str(ctx.exception))

    def test_concurrent_fence_bump_rolls_back_in_flight_mutations(self):
        """If fence is bumped on DB during CAS, transaction rolls back and entities are not written."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        # Concurrent session directly bumps fence behind Runner A's back
        self.conn_b.execute(
            "UPDATE staging_run_journal SET fencing_token = 99 WHERE run_id = ?",
            (RUN_ID_A,),
        )
        self.conn_b.commit()

        # Runner A attempts mutation expecting token 1
        with self.assertRaises(FencingConflictError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

        # Assert no entity rows were committed
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)


# =============================================================================
# Scenario 3: Atomic rollback on failed mutation or journal update
# =============================================================================

class TestScenario3AtomicRollbackOnFailure(_BaseTwoRunnerFixture):
    """Cover atomic rollback on failed entity mutations or journal updates."""

    def test_sql_constraint_error_in_mutation_rolls_back_entire_batch(self):
        """If one mutation in the batch fails, journal CAS and prior mutations roll back."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # Inject a pre-existing row to cause a primary key constraint error on processed_articles
        self.conn_b.execute(
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, 'https://example.com/conflict', 'Conflict', 'source', '2026-10-25T15:00:00Z', '2026-10-25T15:00:00Z')",
            (f"synth_art_{RUN_ID_A}_01",),
        )
        self.conn_b.commit()

        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        with self.assertRaises(JournalError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

        # Verification: stories table was NOT written to; journal status was NOT committed to EFFECT_COMMITTED
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM stories WHERE story_id = ?", (f"synth_story_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)

        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertEqual(entry.status, JournalStatus.ACKED)
        self.assertEqual(entry.fencing_token, 1)

    def test_unregistered_mutation_fails_closed_before_dispatch(self):
        """Mutations targeting unregistered entities are rejected fail-closed before dispatch."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        foreign_cmd = StageMutationCommand(
            table="processed_articles",
            pk_column="article_id",
            pk_value="synth_art_foreign_unregistered",
            columns=["article_id", "canonical_url", "title", "source", "published_at", "processed_at"],
            values=["synth_art_foreign_unregistered", "https://example.com/f", "Foreign", "staging_synth_test", "2026-10-25T15:00:00Z", "2026-10-25T15:00:00Z"],
        )
        with self.assertRaises(TargetInventoryViolationError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=[foreign_cmd],
                manifest_seal_hex=self.seal_a,
            )


# =============================================================================
# Scenario 4: Ambiguous intent COMMIT and effect COMMIT
# =============================================================================

class TestScenario4AmbiguousCommitOutcomes(_BaseTwoRunnerFixture):
    """Cover intent COMMIT and effect COMMIT ambiguity: committed/ACK-lost vs rolled-back/ACK-lost."""

    def test_intent_commit_committed_but_ack_lost(self):
        """Intent committed on mock DB, but ACK lost: client halts fail-closed; rows exist on DB."""
        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST,
            )

        # Client is locally halted
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Provider state: row DOES exist on DB in ACKED status
        entry = self.seam_b.inspect_run(RUN_ID_A)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, JournalStatus.ACKED)

        # Retrying or mutating fails closed
        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=build_live02_synthetic_mutations(RUN_ID_A),
                manifest_seal_hex=self.seal_a,
            )

    def test_intent_commit_rolled_back_and_ack_lost(self):
        """Intent rolled back on mock DB, ACK lost: client halts fail-closed; 0 rows exist on DB."""
        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST,
            )

        # Client is locally halted
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Provider state: row DOES NOT exist on DB
        entry = self.seam_b.inspect_run(RUN_ID_A)
        self.assertIsNone(entry)

    def test_effect_commit_committed_but_ack_lost_wire_simulation(self):
        """Wire transport injection: COMMIT persists on server, but network drops returning response."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # Inject network drop AFTER commit is persisted on the mock server
        self.transport_a.fail_after_commit_network = True
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

        # Client A is permanently halted
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # On DB: transaction committed! Both rows exist and journal is EFFECT_COMMITTED
        entry = self.seam_b.inspect_run(RUN_ID_A)
        self.assertEqual(entry.status, JournalStatus.EFFECT_COMMITTED)
        self.assertEqual(entry.fencing_token, 2)

        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 1)

    def test_effect_commit_rolled_back_and_ack_lost_wire_simulation(self):
        """Wire transport injection: COMMIT times out before persistence; server transaction rolled back."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # Inject timeout BEFORE commit
        self.transport_a.fail_on_commit_timeout = True
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

        # Client A is halted
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # On DB: mutations were NOT committed
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)


# =============================================================================
# Scenario 5: Crash before and after COMMIT
# =============================================================================

class TestScenario5CrashSimulation(_BaseTwoRunnerFixture):
    """Cover crash before COMMIT (automatic rollback) and crash after COMMIT (persistence)."""

    def test_crash_before_commit_rolls_back_automatically(self):
        """Simulate Runner A process abrupt death mid-transaction before COMMIT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # Runner A begins transaction and executes mutations directly on its connection without commit
        self.conn_a.execute("BEGIN IMMEDIATE")
        self.conn_a.execute(
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, 'https://example.com/synth', 'Title', 'source', '2026-10-25T15:00:00Z', '2026-10-25T15:00:00Z')",
            (f"synth_art_{RUN_ID_A}_01",),
        )

        # Process crashes: connection is closed without COMMIT
        self.conn_a.close()

        # Runner B connects to DB: zero partial rows exist
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_crash_after_commit_preserves_remote_state(self):
        """Process crashes after COMMIT: provider records remain intact for discovery."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        # Runner A abruptly closes (simulating process termination)
        self.conn_a.close()

        # Runner B inspects DB: discovers EFFECT_COMMITTED and registered entities
        entry = self.seam_b.inspect_run(RUN_ID_A)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, JournalStatus.EFFECT_COMMITTED)

        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 1)


# =============================================================================
# Scenario 6: Runner B reconstruction without access to Runner A's local marker files
# =============================================================================

class TestScenario6RunnerBReconstruction(_BaseTwoRunnerFixture):
    """Cover Runner B reconstruction with zero access to Runner A's local marker files."""

    def test_runner_b_discovers_unresolved_run_without_local_marker_files(self):
        """Runner B has no access to Runner A's marker directory, but discovers unresolved state in DB."""
        # Runner A suffers ambiguous commit and writes local halt marker in marker_dir_a
        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST,
            )

        # Assert marker exists ONLY in marker_dir_a, NOT in marker_dir_b
        marker_a = self.marker_dir_a / f"efso_radar_journal_halt_{RUN_ID_A}.lock"
        self.assertTrue(marker_a.is_file())
        marker_b = self.marker_dir_b / f"efso_radar_journal_halt_{RUN_ID_A}.lock"
        self.assertFalse(marker_b.exists())

        # Runner B starts up with empty marker_dir_b
        self.assertFalse(self.seam_b.is_halted(RUN_ID_A))

        # Runner B attempts to start fresh run on workload: blocked by DB journal!
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.seam_b.register_intent(
                run_id=RUN_ID_B,
                manifest_seal_hex=self.seal_b,
                target_inventory=self.entities_b,
            )
        self.assertIn(f"unresolved staging run '{RUN_ID_A}' exists", str(ctx.exception))

    def test_runner_b_forensic_review_synthesizes_residual_decision(self):
        """Runner B reconstructs forensic evidence via review_recovery_evidence."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        manifest = PerRunOwnershipManifest(
            run_id=RUN_ID_A,
            created_at="2026-11-06T20:00:00Z",
            registered_entities=self.entities_a,
        )
        manifest_bytes = manifest.model_dump_json(indent=2).encode("utf-8")

        # Runner B inspects shared DB without local markers
        review = self.seam_b.review_recovery_evidence(
            run_id=RUN_ID_A,
            manifest_bytes=manifest_bytes,
        )
        self.assertEqual(review.decision, ReviewDecision.RESIDUAL)


# =============================================================================
# Scenario 7: No blind retry, takeover, or cleanup on UNKNOWN
# =============================================================================

class TestScenario7NoBlindRetryOrTakeoverOnUnknown(_BaseTwoRunnerFixture):
    """Cover refusal of blind retries, automated takeover, or cleanup on UNKNOWN."""

    def test_halted_run_blocks_subsequent_operations(self):
        """Halted run fails closed on all automated mutation, retry, and cleanup attempts."""
        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST,
            )

        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=build_live02_synthetic_mutations(RUN_ID_A),
                manifest_seal_hex=self.seal_a,
            )

        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                manifest_seal_hex=self.seal_a,
            )

    def test_propose_operator_reconciliation_is_strictly_read_only(self):
        """Operator reconciliation proposal is strictly non-authorizing and does not modify DB."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        proposal = self.seam_b.propose_operator_reconciliation(
            run_id=RUN_ID_A,
            proposed_action="MANUAL_CLEANUP_AFTER_PO_GO",
            reason="Forensic inspection confirmed orphaned residual rows.",
        )
        self.assertEqual(proposal.status, "PROPOSAL_ONLY_NON_AUTHORIZING")
        self.assertFalse(proposal.is_authorized)

        # Database state is untouched
        entry = self.seam_b.inspect_run(RUN_ID_A)
        self.assertEqual(entry.status, JournalStatus.ACKED)


# =============================================================================
# Scenario 8: Exact inventory ownership, conditional deletion, and audits
# =============================================================================

class TestScenario8ConditionalCleanupAndAudits(_BaseTwoRunnerFixture):
    """Cover exact inventory conditional cleanup and CLEAN / RESIDUAL / UNKNOWN audits."""

    def test_nominal_cleanup_deletes_entities_and_transitions_to_cleaned_up(self):
        """Clean conditional cleanup deletes owned rows and updates journal to CLEANED_UP."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token_after_mut = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        # Verify audit before cleanup: RESIDUAL
        audit_before = self.seam_a.audit_workload(RUN_ID_A)
        self.assertEqual(audit_before, AuditOutcomeState.RESIDUAL)

        # Cleanup
        final_token = self.seam_a.execute_conditional_cleanup(
            run_id=RUN_ID_A,
            expected_fencing_token=token_after_mut,
            manifest_seal_hex=self.seal_a,
        )
        self.assertEqual(final_token, token_after_mut + 1)

        # Verify audit after cleanup: CLEAN
        audit_after = self.seam_a.audit_workload(RUN_ID_A)
        self.assertEqual(audit_after, AuditOutcomeState.CLEAN)

        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertEqual(entry.status, JournalStatus.CLEANED_UP)

        # Check DB directly
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)
        cur.execute("SELECT COUNT(*) FROM stories WHERE story_id = ?", (f"synth_story_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_foreign_entity_ownership_mismatch_halts_audit(self):
        """If foreign rows exist with unexpected attributes, audit halts fail-closed."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        # Insert entity row with altered/tampered source
        self.conn_b.execute(
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, 'https://example.com/synth', 'Title', 'FOREIGN_ATTRIBUTION', '2026-10-25T15:00:00Z', '2026-10-25T15:00:00Z')",
            (f"synth_art_{RUN_ID_A}_01",),
        )
        self.conn_b.commit()

        audit = self.seam_a.audit_workload(RUN_ID_A)
        self.assertEqual(audit, AuditOutcomeState.OWNERSHIP_MISMATCH_HALT)


# =============================================================================
# Architectural Boundaries & Non-Proof Contract
# =============================================================================

class TestArchitecturalBoundariesAndNonProofContract(_BaseTwoRunnerFixture):
    """Assert that passing offline hermetic tests does NOT prove remote durability."""

    def test_offline_fixture_does_not_prove_remote_durability(self):
        """Offline SQLite fixtures do not prove remote Turso durability or cross-VM survival."""
        # 1. Backing database is strictly in temporary directory
        _assert_fixture_isolation(self.conn_a)

        # 2. Schema version in production schema.py remains 6
        from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
        self.assertEqual(CURRENT_SCHEMA_VERSION, 6)

        # 3. Remote durability disclaimer
        seam_doc = StagingJournalHranaSeam.__doc__ or ""
        self.assertIn("Strictly offline", seam_doc)
        self.assertIn("$0.00 / 0 TRY", seam_doc)
