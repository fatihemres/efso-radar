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
from typing import Optional
import unittest

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import (
    TursoClientError,
    TursoHTTPClient,
)
from efso_radar.staging.journal import (
    ActiveWorkloadCollisionError,
    AmbiguousCommitError,
    AmbiguousCommitOutcome,
    FencingConflictError,
    HaltMarkerPersistenceError,
    InvalidJournalTransitionError,
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
    AmbiguousRollbackError,
    CleanupVerificationError,
    CrossRunnerDiscoveryError,
    MAX_DISCOVERY_LIMIT,
    StagingJournalHranaSeam,
    build_live02_synthetic_mutations,
    _assert_fixture_isolation,
)
from efso_radar.staging.offline_recovery_evidence import (
    CommitState,
    CrossRunnerJournalStatus,
    CrossRunnerProvenance,
    CrossRunnerReview,
    EntityObservation,
    QueryState,
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

    def test_same_owner_different_run_id_rejected_when_prior_run_unresolved_and_lease_expired(self):
        """Even with same owner and expired lease, prior unresolved run prevents new run registration."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        # Manually expire the lease in run_locks
        self.conn_a.execute(
            "UPDATE run_locks SET expires_at = '2020-01-01T00:00:00Z'"
        )
        self.conn_a.commit()

        # Runner A (same owner!) attempts to register a distinct run_id
        with self.assertRaises(ActiveWorkloadCollisionError) as ctx:
            self.seam_a.register_intent(
                run_id=RUN_ID_B,
                manifest_seal_hex=self.seal_b,
                target_inventory=self.entities_b,
            )
        self.assertIn("unresolved staging run", str(ctx.exception))

        # Verify RUN_ID_B was not admitted in staging_run_journal
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM staging_run_journal WHERE run_id = ?", (RUN_ID_B,))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_concurrent_registration_race_one_winner(self):
        """Concurrent intent registrations for distinct runs against the same workload permit exactly one winner."""
        # Seam A registers first
        entry_a = self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        self.assertEqual(entry_a.status, JournalStatus.ACKED)

        # Seam B attempts to register RUN_ID_B concurrently
        with self.assertRaises(ActiveWorkloadCollisionError):
            self.seam_b.register_intent(
                run_id=RUN_ID_B,
                manifest_seal_hex=self.seal_b,
                target_inventory=self.entities_b,
            )

        # Shared DB has only RUN_ID_A
        cur = self.conn_b.cursor()
        cur.execute("SELECT run_id FROM staging_run_journal")
        rows = [r[0] for r in cur.fetchall()]
        self.assertEqual(rows, [RUN_ID_A])


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

    def test_mutation_rejected_when_journal_status_tampered(self):
        """CAS rejects mutation if journal status is unexpectedly modified."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        # Tamper status directly on DB to CLEANED_UP
        self.conn_b.execute(
            "UPDATE staging_run_journal SET status = ? WHERE run_id = ?",
            (JournalStatus.CLEANED_UP.value, RUN_ID_A),
        )
        self.conn_b.commit()

        with self.assertRaises((InvalidJournalTransitionError, FencingConflictError)):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

    def test_mutation_rejected_when_lease_expired(self):
        """CAS rejects mutation if worker lock lease has expired in run_locks."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        # Expire lock lease directly on DB
        self.conn_b.execute(
            "UPDATE run_locks SET expires_at = '2020-01-01T00:00:00Z'"
        )
        self.conn_b.commit()

        with self.assertRaises(FencingConflictError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

        # Verify no mutations were written
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

    def test_cleanup_fails_closed_when_foreign_attribute_row_remains(self):
        """If an entity row has foreign attributes, cleanup DELETE leaves it, in-transaction audit catches it, and rolls back."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        # Tamper the source attribute of the article in the database
        self.conn_b.execute(
            "UPDATE processed_articles SET source = 'foreign_tampered' WHERE article_id = ?",
            (f"synth_art_{RUN_ID_A}_01",),
        )
        self.conn_b.commit()

        # Cleanup should fail fail-closed
        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("In-transaction cleanup audit failed", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Status must NOT be CLEANED_UP
        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.CLEANED_UP)

        # The foreign row is still preserved (transaction rolled back)
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 1)

    def test_cleanup_rejects_partial_entities_subset(self):
        """Partial entity subsets are rejected fail-closed before dispatch."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        # Attempt partial cleanup subset (only article, not story)
        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
                target_entities_subset=[f"synth_art_{RUN_ID_A}_01"],
            )
        self.assertIn("Partial cleanup subset requested", str(ctx.exception))

        # Status is still EFFECT_COMMITTED and entities exist
        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertEqual(entry.status, JournalStatus.EFFECT_COMMITTED)
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 1)

    def test_cleanup_in_transaction_audit_query_failure_rolls_back(self):
        """If an audit query errors during in-transaction audit, cleanup rolls back and halts."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        # Hook to fail SELECT audit query inside cleanup
        def failing_audit_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                for req in reqs:
                    sql = req.get("stmt", {}).get("sql", "")
                    if "SELECT * FROM" in sql:
                        # Return an error for each audit query to preserve cardinality
                        return httpx.Response(
                            200,
                            json={
                                "baton": body.get("baton"),
                                "results": [{
                                    "type": "error",
                                    "error": {"code": "SQLITE_IOERR", "message": "Simulated audit disk I/O failure"},
                                } for _ in reqs],
                            },
                            request=request,
                        )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = failing_audit_hook

        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Simulated audit disk I/O failure", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Status not CLEANED_UP
        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.CLEANED_UP)

    def test_cleanup_fails_closed_when_delete_statement_returns_error(self):
        """If a DELETE statement in cleanup Phase 1 fails, cleanup fails closed, rolls back, and latches HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        def failing_delete_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_delete = any("DELETE FROM" in r.get("stmt", {}).get("sql", "").upper() for r in reqs)
                if has_delete:
                    results = [
                        {"type": "ok", "response": {"result": {"affected_row_count": 0, "rows": []}}},  # BEGIN
                        {"type": "ok", "response": {"result": {"affected_row_count": 1, "rows": []}}},  # CAS
                    ]
                    for _ in reqs[2:]:
                        results.append({
                            "type": "error",
                            "error": {"code": "SQLITE_LOCKED", "message": "Simulated table lock timeout"},
                        })
                    return httpx.Response(
                        200,
                        json={"baton": body.get("baton", "b_phase1_del_fail"), "results": results},
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = failing_delete_hook

        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Simulated table lock timeout", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Check DB: journal status is NOT CLEANED_UP
        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.CLEANED_UP)

    def test_cleanup_fails_closed_on_truncated_audit_response(self):
        """If in-transaction audit (Phase 2) returns truncated results, cleanup rolls back and latches HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        def truncated_audit_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                is_audit = (
                    len(reqs) > 1
                    and all("SELECT *" in r.get("stmt", {}).get("sql", "").upper() for r in reqs)
                )
                if is_audit:
                    return httpx.Response(
                        200,
                        json={
                            "baton": body.get("baton", "b_audit_trunc"),
                            "results": [
                                {"type": "ok", "response": {"result": {"rows": []}}}
                            ],
                        },
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = truncated_audit_hook

        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Truncated in-transaction cleanup audit response", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.CLEANED_UP)

    def test_cleanup_fails_closed_when_audit_response_omits_rows_field(self):
        """If Phase 2 audit response has correct count and type: 'ok' but omits 'rows', cleanup fails closed, rolls back, preserves foreign rows, and latches HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        # Insert a real foreign row in SQLite fixture
        self.conn_b.execute(
            "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
            "VALUES (?, 'https://example.com/foreign', 'Foreign Article', 'FOREIGN_SRC', '2026-10-25T15:00:00Z', '2026-10-25T15:00:00Z')",
            ("synth_art_foreign_01",),
        )
        self.conn_b.commit()

        commit_dispatched = []

        def missing_rows_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_commit = any(
                    r.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
                    for r in reqs
                )
                if has_commit:
                    commit_dispatched.append(True)
                is_audit = (
                    len(reqs) > 1
                    and all("SELECT *" in r.get("stmt", {}).get("sql", "").upper() for r in reqs)
                )
                if is_audit:
                    # Return exact matching count of results with type: 'ok', but result dictionary OMITS 'rows' field
                    return httpx.Response(
                        200,
                        json={
                            "baton": body.get("baton", "b_audit_no_rows"),
                            "results": [
                                {"type": "ok", "response": {"result": {}}}
                                for _ in reqs
                            ],
                        },
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = missing_rows_hook

        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("missing explicit 'rows' array", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))
        self.assertEqual(len(commit_dispatched), 0, "Phase 4 COMMIT must NEVER be dispatched when audit evidence is malformed")

        # Status not CLEANED_UP
        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.CLEANED_UP)

        # Foreign row strictly preserved in database fixture
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", ("synth_art_foreign_01",))
        self.assertEqual(cur.fetchone()[0], 1)

    def test_cleanup_fails_closed_when_final_journal_update_fails(self):
        """If final journal update in Phase 3 affects 0 rows or fails, COMMIT is never issued and run is HALTED."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        commit_issued = []

        def failing_phase3_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_commit = any(
                    r.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
                    for r in reqs
                )
                if has_commit:
                    commit_issued.append(True)
                has_cleaned_up_update = any(
                    "UPDATE STAGING_RUN_JOURNAL SET STATUS" in r.get("stmt", {}).get("sql", "").upper()
                    for r in reqs
                )
                if has_cleaned_up_update:
                    results = []
                    for _ in reqs:
                        results.append({
                            "type": "ok",
                            "response": {"result": {"affected_row_count": 0, "rows": []}},
                        })
                    return httpx.Response(
                        200,
                        json={"baton": body.get("baton", "b_p3_fail"), "results": results},
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = failing_phase3_hook

        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("affected 0 rows (expected 1)", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))
        self.assertEqual(len(commit_issued), 0, "COMMIT must never be dispatched if Phase 3 validation fails")

        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.CLEANED_UP)

    def test_cleanup_missing_commit_acknowledgment_latches_halt(self):
        """If COMMIT acknowledgment in Phase 4 is malformed or error, AmbiguousCommitError is raised and run latches HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        def malformed_commit_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_commit = any(
                    r.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
                    for r in reqs
                )
                if has_commit:
                    return httpx.Response(
                        200,
                        json={"baton": body.get("baton", "b_c"), "results": []},
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = malformed_commit_hook

        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Cleanup COMMIT failed or unverified", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))


# =============================================================================
# Hermetic HTTP Boundary & Mock Hrana Lifecycle Regressions (Security Review)
# =============================================================================

class TestHermeticHTTPBoundarySecurity(_BaseTwoRunnerFixture):
    """Cover hermetic HTTP client and transport boundary enforcement."""

    def test_pre_initialized_cached_client_rejected_at_construction(self):
        """TursoHTTPClient with pre-initialized internal _client is rejected fail-closed."""
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="hermetic-token",
            transport=self.transport_a,
        )
        # Force initialization of internal httpx client
        _ = client._get_client()
        self.assertIsNotNone(client._client)

        with self.assertRaises(PermissionError) as ctx:
            StagingJournalHranaSeam(
                client=client,
                owner_id="test_worker",
                marker_dir=self.marker_dir_a,
            )
        self.assertIn("_client is not None", str(ctx.exception))

    def test_read_only_client_rejected_at_construction(self):
        """TursoHTTPClient configured with read_only=True is rejected fail-closed."""
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="hermetic-token",
            transport=self.transport_a,
            read_only=True,
        )
        with self.assertRaises(PermissionError) as ctx:
            StagingJournalHranaSeam(
                client=client,
                owner_id="test_worker",
                marker_dir=self.marker_dir_a,
            )
        self.assertIn("read_only=True", str(ctx.exception))

    def test_cached_transport_mismatch_rejected_before_dispatch(self):
        """If client._client._transport is swapped after seam creation, dispatch rejects fail-closed."""
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="hermetic-token",
            transport=self.transport_a,
        )
        seam = StagingJournalHranaSeam(
            client=client,
            owner_id="test_worker",
            marker_dir=self.marker_dir_a,
        )
        # Initialize internal client
        _ = client._get_client()

        # Swap the transport underneath
        dummy_transport = httpx.HTTPTransport()
        client._client._transport = dummy_transport

        with self.assertRaises(PermissionError) as ctx:
            seam.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )
        self.assertIn("cached transport identity mismatch", str(ctx.exception))

    def test_transport_swap_before_first_dispatch_rejected_without_network(self):
        """Swapping client.transport on an uninitialized client is rejected before any dispatch without network."""
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="hermetic-token",
            transport=self.transport_a,
        )
        self.assertIsNone(client._client, "Client must be fresh and uninitialized")

        seam = StagingJournalHranaSeam(
            client=client,
            owner_id="test_worker",
            marker_dir=self.marker_dir_a,
        )

        intercepted_requests = []

        def mock_handler(request: httpx.Request) -> httpx.Response:
            intercepted_requests.append(request)
            return httpx.Response(200, json={})

        swapped_transport = httpx.MockTransport(mock_handler)
        client.transport = swapped_transport

        # Mutating path: register_intent
        with self.assertRaises(PermissionError) as ctx:
            seam.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )
        self.assertIn("client transport identity mismatch", str(ctx.exception))

        # Read path 1: audit_workload
        with self.assertRaises(PermissionError) as ctx:
            seam.audit_workload(RUN_ID_A)
        self.assertIn("client transport identity mismatch", str(ctx.exception))

        # Read path 2: review_recovery_evidence
        with self.assertRaises(PermissionError) as ctx:
            seam.review_recovery_evidence(run_id=RUN_ID_A)
        self.assertIn("client transport identity mismatch", str(ctx.exception))

        # Verify zero requests reached the swapped transport
        self.assertEqual(len(intercepted_requests), 0, "No network/HTTP requests may be issued after transport mismatch")


class TestMockHranaTransactionLifecycle(_BaseTwoRunnerFixture):
    """Cover mock Hrana transaction lifecycle semantics and failure injections."""

    def test_timeout_before_commit_does_not_commit_on_subsequent_read(self):
        """A timeout before COMMIT rolls back provider state; subsequent read never commits writes."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        # Inject timeout right before COMMIT persistence on server
        self.transport_a.fail_on_commit_timeout = True

        with self.assertRaises((AmbiguousCommitError, JournalError, httpx.TimeoutException)):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

        # Disable timeout injection and issue an unrelated read query on Runner A
        self.transport_a.fail_on_commit_timeout = False
        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, JournalStatus.ACKED)

        # Independent database inspection via conn_b: zero mutation rows leaked!
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)
        cur.execute("SELECT COUNT(*) FROM stories WHERE story_id = ?", (f"synth_story_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)

        # Journal status in DB was not committed to EFFECT_COMMITTED
        cur.execute("SELECT status FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.assertEqual(cur.fetchone()[0], JournalStatus.ACKED.value)

    def test_nested_begin_in_mock_transport_returns_error(self):
        """Mock Hrana transport rejects nested BEGIN within an active transaction."""
        # Dispatch two BEGIN statements in the same pipeline via seam_a
        res = self.seam_a._post_pipeline([
            {"type": "execute", "stmt": {"sql": "BEGIN"}},
            {"type": "execute", "stmt": {"sql": "BEGIN"}},
        ])
        results = res.get("results", [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].get("type"), "ok")
        self.assertEqual(results[1].get("type"), "error")
        err = results[1].get("error", {})
        self.assertEqual(err.get("code"), "SQLITE_ERROR")
        self.assertIn("cannot start a transaction within a transaction", err.get("message", ""))

        # Clean up by rolling back the opened transaction
        baton = res.get("baton")
        if baton:
            self.seam_a._post_pipeline([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}], baton=baton)

    def test_missing_session_baton_in_transaction_latches_halt(self):
        """Hrana transaction response without a session baton fails closed and latches HALT."""
        def no_baton_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_begin = any(
                    r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN"
                    for r in reqs
                )
                if has_begin:
                    return httpx.Response(
                        200,
                        json={"results": [{"type": "ok"}] * len(reqs)},
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = no_baton_hook

        with self.assertRaises(TursoClientError) as ctx:
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )
        self.assertIn("missing valid session baton", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_truncated_hrana_results_rolls_back_and_latches_halt(self):
        """Hrana transaction returning fewer results than statements rolls back and latches HALT."""
        def truncated_results_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_begin = any(
                    r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN"
                    for r in reqs
                )
                if has_begin:
                    return httpx.Response(
                        200,
                        json={
                            "baton": "b_trunc_test",
                            "results": [{"type": "ok", "response": {"result": {"affected_row_count": 0, "rows": []}}}],
                        },
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = truncated_results_hook

        with self.assertRaises(TursoClientError) as ctx:
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )
        self.assertIn("Malformed or truncated Hrana transaction results", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_statement_result_type_error_rolls_back_and_latches_halt(self):
        """Statement returning non-ok type in Hrana transaction rolls back and latches HALT."""
        def stmt_error_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_begin = any(
                    r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN"
                    for r in reqs
                )
                if has_begin:
                    results = [{"type": "ok", "response": {"result": {"affected_row_count": 0, "rows": []}}}]
                    results.append({
                        "type": "error",
                        "error": {"code": "SQLITE_IOERR", "message": "Simulated disk corruption"},
                    })
                    for _ in reqs[2:]:
                        results.append({"type": "ok", "response": {"result": {"affected_row_count": 0, "rows": []}}})
                    return httpx.Response(
                        200,
                        json={"baton": "b_err_test", "results": results},
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = stmt_error_hook

        with self.assertRaises(TursoClientError) as ctx:
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )
        self.assertIn("Statement 0 failed in Hrana transaction: Simulated disk corruption", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_missing_or_malformed_commit_acknowledgment_latches_halt(self):
        """Malformed or error COMMIT acknowledgment raises AmbiguousCommitError and latches HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        def malformed_commit_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                has_commit = any(
                    r.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
                    for r in reqs
                )
                if has_commit:
                    return httpx.Response(
                        200,
                        json={"baton": "b_c", "results": [{"type": "error", "error": {"message": "Simulated commit write conflict"}}]},
                        request=request,
                    )
            except Exception:
                pass
            return None

        self.transport_a.custom_response_hook = malformed_commit_hook

        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Hrana COMMIT failed or unverified", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

    def test_initial_pipeline_timeout_latches_halt_and_prohibits_retry(self):
        """Transport timeout during initial BEGIN+statements pipeline latches HALT and prohibits retry."""
        def initial_timeout_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_begin = any(r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN" for r in reqs)
            has_commit = any(r.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT" for r in reqs)
            if has_begin and not has_commit:
                raise httpx.TimeoutException("Simulated network timeout during BEGIN+statements pipeline")
            return None

        self.transport_a.custom_response_hook = initial_timeout_hook

        with self.assertRaises(TursoClientError) as ctx:
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )
        self.assertIn("Initial pipeline request failed in Hrana transaction", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Disable hook and attempt retry on halted run: strictly rejected fail-closed
        self.transport_a.custom_response_hook = None
        with self.assertRaises(JournalHaltedError):
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )

        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.assertEqual(cur.fetchone()[0], 0)

    def test_cleanup_audit_timeout_latches_halt_and_prohibits_cleanup(self):
        """Transport timeout during cleanup in-transaction audit latches HALT and prohibits automated retry."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )

        def audit_timeout_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            is_audit = (
                len(reqs) > 1
                and all("SELECT *" in r.get("stmt", {}).get("sql", "").upper() for r in reqs)
            )
            if is_audit:
                raise httpx.TimeoutException("Simulated timeout during Phase 2 cleanup audit dispatch")
            return None

        self.transport_a.custom_response_hook = audit_timeout_hook

        with self.assertRaises(CleanupVerificationError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Phase 2 audit pipeline dispatch failed in cleanup", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Disable hook and attempt automated cleanup retry: strictly rejected fail-closed
        self.transport_a.custom_response_hook = None
        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )

    def test_commit_with_null_result_latches_halt(self):
        """COMMIT response with results: [null] does not crash with AttributeError, latches HALT and raises AmbiguousCommitError."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        def null_commit_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_commit = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
                for r in reqs
            )
            if has_commit:
                try:
                    self.transport_a.conn.rollback()
                except Exception:
                    pass
                return httpx.Response(
                    200,
                    json={"baton": "b_null_c", "results": [None]},
                    request=request,
                )
            return None

        self.transport_a.custom_response_hook = null_commit_hook

        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Hrana COMMIT failed or unverified", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Direct DB check: status is NOT EFFECT_COMMITTED
        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.EFFECT_COMMITTED)

        # Prohibit automatic retry on halted run
        self.transport_a.custom_response_hook = None
        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

    def test_commit_with_invalid_json_latches_halt(self):
        """COMMIT returning non-JSON payload latches HALT and raises AmbiguousCommitError."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        def invalid_json_commit_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_commit = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "COMMIT"
                for r in reqs
            )
            if has_commit:
                try:
                    self.transport_a.conn.rollback()
                except Exception:
                    pass
                return httpx.Response(
                    200,
                    text="<html><body>502 Bad Gateway - Proxy Error</body></html>",
                    headers={"content-type": "text/html"},
                    request=request,
                )
            return None

        self.transport_a.custom_response_hook = invalid_json_commit_hook

        with self.assertRaises(AmbiguousCommitError) as ctx:
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Ambiguous COMMIT on Hrana transaction", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        entry = self.seam_a.inspect_run(RUN_ID_A)
        self.assertNotEqual(entry.status, JournalStatus.EFFECT_COMMITTED)

        # Prohibit automatic retry on halted run
        self.transport_a.custom_response_hook = None
        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

    def test_mutation_rowcount_mismatch_with_unconfirmed_rollback_latches_halt(self):
        """Rowcount/fencing mismatch followed by ROLLBACK timeout latches HALT and denies retry."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        def rollback_timeout_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_begin = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN"
                for r in reqs
            )
            has_rollback = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "ROLLBACK"
                for r in reqs
            )
            if has_begin:
                # Concurrent session bumps fence behind Runner A's back after inspect_run passes
                self.conn_b.execute(
                    "UPDATE staging_run_journal SET fencing_token = 99 WHERE run_id = ?",
                    (RUN_ID_A,),
                )
                self.conn_b.commit()
                return None
            if has_rollback:
                raise httpx.TimeoutException("Simulated network timeout during ROLLBACK dispatch")
            return None

        self.transport_a.custom_response_hook = rollback_timeout_hook

        with self.assertRaises(AmbiguousRollbackError) as ctx:
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("ROLLBACK could not be confirmed", str(ctx.exception))
        self.assertIn("Transaction outcome UNKNOWN; run locally HALTED", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Reset hook: subsequent automated actions denied with JournalHaltedError
        self.transport_a.custom_response_hook = None
        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )

        # Evidence preserved: unwritten entity rows not committed, DB entry preserved
        cur = self.conn_b.cursor()
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 0)
        cur.execute("SELECT fencing_token FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.assertEqual(cur.fetchone()[0], 99)

    def test_mutation_rowcount_mismatch_with_503_rollback_latches_halt(self):
        """Rowcount/fencing mismatch followed by ROLLBACK HTTP 503 latches HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)

        def rollback_503_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_begin = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN"
                for r in reqs
            )
            has_rollback = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "ROLLBACK"
                for r in reqs
            )
            if has_begin:
                self.conn_b.execute(
                    "UPDATE staging_run_journal SET fencing_token = 99 WHERE run_id = ?",
                    (RUN_ID_A,),
                )
                self.conn_b.commit()
                return None
            if has_rollback:
                return httpx.Response(503, json={"error": {"message": "Service Unavailable"}}, request=request)
            return None

        self.transport_a.custom_response_hook = rollback_503_hook

        with self.assertRaises(AmbiguousRollbackError) as ctx:
            self.seam_a.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("ROLLBACK could not be confirmed", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))
        self.transport_a.custom_response_hook = None

    def test_cleanup_cas_mismatch_with_unconfirmed_rollback_latches_halt(self):
        """Cleanup CAS mismatch followed by ROLLBACK with statement error latches HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )
        self.assertEqual(token2, 2)

        def rollback_error_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_begin = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN"
                for r in reqs
            )
            has_rollback = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "ROLLBACK"
                for r in reqs
            )
            if has_begin:
                # Concurrent update bumps fence on DB during cleanup CAS pipeline
                self.conn_b.execute(
                    "UPDATE staging_run_journal SET fencing_token = 99 WHERE run_id = ?",
                    (RUN_ID_A,),
                )
                self.conn_b.commit()
                return None
            if has_rollback:
                return httpx.Response(
                    200,
                    json={
                        "baton": body.get("baton", "mock_baton"),
                        "results": [{"type": "error", "error": {"message": "synthetic rollback statement failure"}}],
                    },
                    request=request,
                )
            return None

        self.transport_a.custom_response_hook = rollback_error_hook

        with self.assertRaises(AmbiguousRollbackError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("ROLLBACK could not be confirmed", str(ctx.exception))
        self.assertIn("synthetic rollback statement failure", str(ctx.exception))
        self.assertTrue(self.seam_a.is_halted(RUN_ID_A))

        # Status not marked CLEANED_UP
        cur = self.conn_b.cursor()
        cur.execute("SELECT status FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertNotEqual(row[0], JournalStatus.CLEANED_UP.value)

        # Registered entities preserved (evidence preserved)
        cur.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = ?", (f"synth_art_{RUN_ID_A}_01",))
        self.assertEqual(cur.fetchone()[0], 1)

        # Reset hook: automated cleanup denied with JournalHaltedError
        self.transport_a.custom_response_hook = None
        with self.assertRaises(JournalHaltedError):
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )

    def test_cleanup_cas_mismatch_with_confirmed_rollback_raises_fencing_conflict_without_halt(self):
        """Cleanup CAS mismatch with confirmed ROLLBACK raises FencingConflictError without HALT."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        token2 = self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )
        self.assertEqual(token2, 2)

        def cas_mismatch_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_begin = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "BEGIN"
                for r in reqs
            )
            if has_begin:
                self.conn_b.execute(
                    "UPDATE staging_run_journal SET fencing_token = 99 WHERE run_id = ?",
                    (RUN_ID_A,),
                )
                self.conn_b.commit()
            return None

        self.transport_a.custom_response_hook = cas_mismatch_hook

        # Hook only bumps fence on BEGIN; ROLLBACK is acknowledged normally by HermeticStagingTransport
        with self.assertRaises(FencingConflictError) as ctx:
            self.seam_a.execute_conditional_cleanup(
                run_id=RUN_ID_A,
                expected_fencing_token=token2,
                manifest_seal_hex=self.seal_a,
            )
        self.assertIn("Fencing check failed; transaction rolled back fail-closed", str(ctx.exception))
        # Crucially: local HALT is NOT latched because ROLLBACK was confirmed successful
        self.assertFalse(self.seam_a.is_halted(RUN_ID_A))
        self.transport_a.custom_response_hook = None

    def test_registration_rowcount_mismatch_with_unconfirmed_rollback_raises_ambiguous_rollback_error_and_latches_halt(self):
        """Unresolved run + registration rowcount mismatch + unacknowledged ROLLBACK propagates AmbiguousRollbackError and latches HALT."""
        # 1. RUN_ID_A registered and remains unresolved (ACKED)
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # 2. Intercept registration ROLLBACK for Runner B on distinct RUN_ID_B
        def rollback_timeout_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
            except Exception:
                body = {}
            reqs = body.get("requests", [])
            has_rollback = any(
                r.get("stmt", {}).get("sql", "").strip().upper() == "ROLLBACK"
                for r in reqs
            )
            if has_rollback:
                raise httpx.ReadTimeout("Simulated timeout during registration ROLLBACK")
            return None

        self.transport_b.custom_response_hook = rollback_timeout_hook

        # 3. Runner B attempts to register RUN_ID_B:
        # Atomic registration query finds unresolved RUN_ID_A, affecting 0 rows instead of 1.
        # Dispatched ROLLBACK times out, so AmbiguousRollbackError must reach the caller
        # (NOT masked by _diagnose_registration_failure into ActiveWorkloadCollisionError).
        with self.assertRaises(AmbiguousRollbackError) as ctx:
            self.seam_b.register_intent(
                run_id=RUN_ID_B,
                manifest_seal_hex=self.seal_b,
                target_inventory=self.entities_b,
            )
        self.assertIn("ROLLBACK could not be confirmed", str(ctx.exception))
        self.assertTrue(self.seam_b.is_halted(RUN_ID_B))

        # 4. Same-run retry is denied fail-closed with JournalHaltedError
        self.transport_b.custom_response_hook = None
        with self.assertRaises(JournalHaltedError):
            self.seam_b.register_intent(
                run_id=RUN_ID_B,
                manifest_seal_hex=self.seal_b,
                target_inventory=self.entities_b,
            )

        # 5. Positive control: when backend transaction is clean and ROLLBACK is confirmed,
        # ordinary ActiveWorkloadCollisionError is raised and HALT is not latched.
        if self.conn_b.in_transaction:
            self.conn_b.rollback()
        self.transport_b.active_batons.clear()
        self.transport_b.in_transaction.clear()

        fresh_run_id = "run_20261107T120200Z_c3d4e5f6a1b2"
        fresh_entities = create_default_manifest_entities(fresh_run_id)
        fresh_seal = compute_canonical_inventory_fingerprint(fresh_run_id, fresh_entities)
        with self.assertRaises(ActiveWorkloadCollisionError) as collision_ctx:
            self.seam_b.register_intent(
                run_id=fresh_run_id,
                manifest_seal_hex=fresh_seal,
                target_inventory=fresh_entities,
            )
        self.assertIn(f"unresolved staging run '{RUN_ID_A}' exists", str(collision_ctx.exception))
        self.assertFalse(self.seam_b.is_halted(fresh_run_id))


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


# =============================================================================
# Scenario 9: Cross-Runner Mock Wire Journal Discovery & Recovery Reconstruction
# =============================================================================

class TestScenario9CrossRunnerMockWireDiscovery(_BaseTwoRunnerFixture):
    """Adversarial cross-runner journal discovery & inert recovery evidence reconstruction over mock Hrana wire."""

    def test_point1_runner_a_intent_discovered_by_runner_b_via_mock_hrana_wire_only(self):
        """Runner A registers intent; Runner B discovers it over mock Hrana wire without knowing run ID."""
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        # Runner B starts with empty marker dir, no manifest file, and no prior knowledge of RUN_ID_A
        self.assertEqual(len(list(self.marker_dir_b.iterdir())), 0)

        # Wire request tracing: prove the discovery genuinely traversed mock Hrana wire
        captured_requests: list[httpx.Request] = []

        def wire_spy_hook(request: httpx.Request) -> Optional[httpx.Response]:
            captured_requests.append(request)
            return None

        self.transport_b.custom_response_hook = wire_spy_hook
        req_count_before = self.budget_b.request_count

        discovered = self.seam_b.discover_unresolved_runs()

        req_count_after = self.budget_b.request_count
        self.assertEqual(req_count_after - req_count_before, 1, "Discovery query must traverse BudgetTrackingTransport wire")
        self.assertEqual(len(captured_requests), 1, "Discovery query must hit HermeticStagingTransport wire")

        # Verify wire request structure: POST /v2/pipeline with allowlisted SELECT
        req = captured_requests[0]
        self.assertEqual(req.method, "POST")
        self.assertTrue(req.url.path.endswith("/v2/pipeline"))
        req_body = json.loads(req.content.decode("utf-8"))
        sql = req_body["requests"][0]["stmt"]["sql"]
        self.assertIn("FROM staging_run_journal", sql)
        self.assertIn("LIMIT ?", sql)

        # Verify discovered record
        self.assertEqual(len(discovered), 1)
        entry = discovered[0]
        self.assertEqual(entry.run_id, RUN_ID_A)
        self.assertEqual(entry.owner_id, "gha_runner_a_worker_01")
        self.assertEqual(entry.status, JournalStatus.ACKED)
        self.assertEqual(entry.manifest_seal_hex, self.seal_a)

        # Evidence reconstruction without passing run_id
        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.run_id, RUN_ID_A)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.INTENT_RECORDED)
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")

    def test_point2_runner_a_commits_effects_runner_b_reconstructs_residual_evidence_without_local_markers(self):
        """Runner A commits synthetic effects; Runner B reconstructs residual evidence without local marker files."""
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

        # Runner B has empty marker directory
        self.assertEqual(len(list(self.marker_dir_b.iterdir())), 0)

        # Reconstruct evidence without knowing run_id
        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.run_id, RUN_ID_A)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.EFFECT_COMMITTED)
        self.assertEqual(review.decision, ReviewDecision.RESIDUAL)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")

        # Explicit alias reconstruct_cross_runner_recovery_evidence produces identical inert outcome
        alias_review = self.seam_b.reconstruct_cross_runner_recovery_evidence()
        self.assertEqual(alias_review.run_id, RUN_ID_A)
        self.assertEqual(alias_review.decision, ReviewDecision.RESIDUAL)
        self.assertEqual(alias_review.remote_state, "UNKNOWN")
        self.assertEqual(alias_review.action, "MANUAL_REVIEW_ONLY")

    def test_point3_lost_commit_acknowledgments_never_permit_retry_or_takeover(self):
        """Lost COMMIT acknowledgments are never interpreted as permission to retry or take over."""
        # Case 3a: Intent COMMIT ack lost (COMMITTED_BUT_ACK_LOST)
        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST,
            )

        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.run_id, RUN_ID_A)
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")

        # Runner B cannot retry Runner A's run or take over workload
        with self.assertRaises(ActiveWorkloadCollisionError):
            self.seam_b.register_intent(
                run_id=RUN_ID_A,
                manifest_seal_hex=self.seal_a,
                target_inventory=self.entities_a,
            )

        with self.assertRaises(StaleOwnerError):
            self.seam_b.execute_atomic_stage_mutation(
                run_id=RUN_ID_A,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=build_live02_synthetic_mutations(RUN_ID_A),
            )

        # Case 3b: Intent COMMIT rolled back and ack lost (ROLLED_BACK_AND_ACK_LOST)
        fresh_run_id = "run_20261107T120500Z_c3d4e5f6a1b2"
        fresh_entities = create_default_manifest_entities(fresh_run_id)
        fresh_seal = compute_canonical_inventory_fingerprint(fresh_run_id, fresh_entities)

        # Clean DB of prior run
        self.conn_a.execute("DELETE FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.conn_a.commit()

        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.register_intent(
                run_id=fresh_run_id,
                manifest_seal_hex=fresh_seal,
                target_inventory=fresh_entities,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.ROLLED_BACK_AND_ACK_LOST,
            )

        discovered = self.seam_b.discover_unresolved_runs()
        self.assertEqual(len(discovered), 0, "Rolled-back transaction leaves zero journal rows")
        empty_review = self.seam_b.review_recovery_evidence()
        self.assertEqual(empty_review.journal_status, CrossRunnerJournalStatus.ABSENT)
        self.assertEqual(empty_review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(empty_review.remote_state, "UNKNOWN")
        self.assertEqual(empty_review.action, "MANUAL_REVIEW_ONLY")

        # Case 3c: Effect COMMIT ack lost (COMMITTED_BUT_ACK_LOST)
        run_id_3 = "run_20261107T120600Z_d4e5f6a1b2c3"
        entities_3 = create_default_manifest_entities(run_id_3)
        seal_3 = compute_canonical_inventory_fingerprint(run_id_3, entities_3)

        self.seam_a.register_intent(
            run_id=run_id_3,
            manifest_seal_hex=seal_3,
            target_inventory=entities_3,
        )
        mutations = build_live02_synthetic_mutations(run_id_3)
        with self.assertRaises(AmbiguousCommitError):
            self.seam_a.execute_atomic_stage_mutation(
                run_id=run_id_3,
                expected_fencing_token=1,
                stage="LIVE-02",
                mutation_statements=mutations,
                manifest_seal_hex=seal_3,
                simulate_ambiguous_commit=AmbiguousCommitOutcome.COMMITTED_BUT_ACK_LOST,
            )

        review_ambig = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_ambig.run_id, run_id_3)
        # Provider transaction committed effects before ACK was lost:
        # DB status is EFFECT_COMMITTED with owned rows present -> forensic decision is RESIDUAL,
        # but crucial safety invariants remote_state=UNKNOWN and action=MANUAL_REVIEW_ONLY remain strictly non-authorizing
        self.assertEqual(review_ambig.journal_status, CrossRunnerJournalStatus.EFFECT_COMMITTED)
        self.assertEqual(review_ambig.decision, ReviewDecision.RESIDUAL)
        self.assertEqual(review_ambig.remote_state, "UNKNOWN")
        self.assertEqual(review_ambig.action, "MANUAL_REVIEW_ONLY")

        # Runner B is still strictly prohibited from taking over or mutating Runner A's run
        with self.assertRaises(StaleOwnerError):
            self.seam_b.execute_atomic_stage_mutation(
                run_id=run_id_3,
                expected_fencing_token=2,
                stage="LIVE-02",
                mutation_statements=mutations,
            )

    def test_point4_missing_malformed_duplicate_or_corrupted_journal_records_fail_closed(self):
        """Missing, malformed, duplicate or corrupted journal records fail closed."""
        # 4a: Missing records
        discovered = self.seam_b.discover_unresolved_runs()
        self.assertEqual(len(discovered), 0)
        review = self.seam_b.review_recovery_evidence()
        self.assertIsNone(review.run_id)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.ABSENT)
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)

        # 4b: Malformed run_id
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "malformed_noncanonical_id",
                "worker_01",
                1,
                "LIVE-02",
                JournalStatus.ACKED.value,
                "a" * 64,
                "[]",
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertIn("invalid run_id", str(ctx.exception))

        review_malformed = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_malformed.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_malformed.journal_status, CrossRunnerJournalStatus.CORRUPTED)

        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.commit()

        # 4c: Corrupted target_inventory_json
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                RUN_ID_A,
                "worker_01",
                1,
                "LIVE-02",
                JournalStatus.ACKED.value,
                self.seal_a,
                "{invalid_json_content",
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertIn("invalid JSON", str(ctx.exception))

        review_corrupt = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_corrupt.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_corrupt.journal_status, CrossRunnerJournalStatus.CORRUPTED)

        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.commit()

        # 4d: Tampered manifest seal
        raw_inv = json.dumps([e.model_dump() for e in self.entities_a])
        tampered_seal = "0" * 64
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                RUN_ID_A,
                "worker_01",
                1,
                "LIVE-02",
                JournalStatus.ACKED.value,
                tampered_seal,
                raw_inv,
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertIn("manifest seal mismatch", str(ctx.exception))

        review_tampered = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_tampered.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_tampered.journal_status, CrossRunnerJournalStatus.CORRUPTED)

        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.commit()

        # 4e: Discovery result overflow (more than MAX_DISCOVERY_LIMIT runs)
        for i in range(11):
            r_id = f"run_20261107T12{i:02d}00Z_{i:012x}"
            ents = create_default_manifest_entities(r_id)
            seal = compute_canonical_inventory_fingerprint(r_id, ents)
            self.conn_a.execute(
                """
                INSERT INTO staging_run_journal (
                    run_id, owner_id, fencing_token, stage, status,
                    manifest_seal_hex, target_inventory_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r_id,
                    "worker_01",
                    1,
                    "LIVE-02",
                    JournalStatus.ACKED.value,
                    seal,
                    json.dumps([e.model_dump() for e in ents]),
                    f"2026-11-07T12:{i:02d}:00Z",
                    f"2026-11-07T12:{i:02d}:00Z",
                ],
            )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs(max_results=10)
        self.assertIn("DISCOVERY_RESULT_OVERFLOW", str(ctx.exception))

        review_overflow = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_overflow.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_overflow.journal_status, CrossRunnerJournalStatus.UNREADABLE)

        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.commit()

        # 4f: Duplicate active runs on workload
        for r_id in (RUN_ID_A, RUN_ID_B):
            ents = create_default_manifest_entities(r_id)
            seal = compute_canonical_inventory_fingerprint(r_id, ents)
            self.conn_a.execute(
                """
                INSERT INTO staging_run_journal (
                    run_id, owner_id, fencing_token, stage, status,
                    manifest_seal_hex, target_inventory_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r_id,
                    "worker_01",
                    1,
                    "LIVE-02",
                    JournalStatus.ACKED.value,
                    seal,
                    json.dumps([e.model_dump() for e in ents]),
                    "2026-11-07T12:00:00Z",
                    "2026-11-07T12:00:00Z",
                ],
            )
        self.conn_a.commit()

        review_dup = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_dup.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_dup.journal_status, CrossRunnerJournalStatus.UNREADABLE)
        self.assertIn("Multiple unresolved staging runs discovered", review_dup.reasons[0])

    def test_point5_foreign_entities_or_ownership_mismatches_block_recovery(self):
        """Foreign entities or ownership mismatches block recovery fail-closed."""
        # 5a: Foreign attribution on entity row
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

        # Tamper the source attribution of the article in the database fixture
        self.conn_a.execute(
            "UPDATE processed_articles SET source = 'FOREIGN_ATTRIBUTION' WHERE article_id = ?",
            (f"synth_art_{RUN_ID_A}_01",),
        )
        self.conn_a.commit()

        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.run_id, RUN_ID_A)
        self.assertEqual(review.decision, ReviewDecision.OWNERSHIP_MISMATCH_HALT)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")

        # 5b: Unauthorized table in journal target inventory
        unauth_ents = [
            {
                "table": "passwords",
                "pk_column": "id",
                "pk_value": "secret",
                "cleanup_dependency_order": 1,
                "expected_attributes": {},
            }
        ] * 5
        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                RUN_ID_A,
                "worker_01",
                1,
                "LIVE-02",
                JournalStatus.ACKED.value,
                "b" * 64,
                json.dumps(unauth_ents),
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertIn("unauthorized table", str(ctx.exception))
        self.assertNotIn("passwords", str(ctx.exception))

        review_unauth = self.seam_b.review_recovery_evidence()
        self.assertNotIn("passwords", "".join(review_unauth.reasons))
        self.assertEqual(review_unauth.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_unauth.journal_status, CrossRunnerJournalStatus.CORRUPTED)

    def test_point6_transport_swapping_and_unexpected_network_dispatch_rejected(self):
        """Transport swapping and missing transports are rejected before any network dispatch."""
        intercepted = []

        def spy_handler(request: httpx.Request) -> httpx.Response:
            intercepted.append(request)
            return httpx.Response(200, json={})

        swapped = httpx.MockTransport(spy_handler)
        self.client_b.transport = swapped

        with self.assertRaises(PermissionError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertIn("client transport identity mismatch", str(ctx.exception))

        with self.assertRaises(PermissionError) as ctx:
            self.seam_b.review_recovery_evidence()
        self.assertIn("client transport identity mismatch", str(ctx.exception))

        with self.assertRaises(PermissionError) as ctx:
            self.seam_b.inspect_run_wire(RUN_ID_A)
        self.assertIn("client transport identity mismatch", str(ctx.exception))

        self.assertEqual(len(intercepted), 0, "Zero HTTP requests may reach swapped transport")

        # Transport is None
        self.client_b.transport = None
        with self.assertRaises(PermissionError):
            self.seam_b.discover_unresolved_runs()

    def test_point7_empty_or_incomplete_discovery_result_never_authorizes_new_work(self):
        """An empty or incomplete discovery result never authorizes new work."""
        # 7a: Empty discovery
        review = self.seam_b.review_recovery_evidence()
        self.assertIsNone(review.run_id)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.ABSENT)
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")
        self.assertIn("Empty discovery result never authorizes new work", review.reasons[1])

        # 7b: Incomplete entity observations
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )

        def failing_entity_query_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                reqs = body.get("requests", [])
                for r in reqs:
                    sql = r.get("stmt", {}).get("sql", "")
                    if "FROM stories" in sql:
                        return httpx.Response(
                            200,
                            json={
                                "baton": body.get("baton"),
                                "results": [
                                    {"type": "error", "error": {"code": "SQLITE_IOERR", "message": "Simulated disk failure"}},
                                    {"type": "ok"},
                                ],
                            },
                            request=request,
                        )
            except Exception:
                pass
            return None

        self.transport_b.custom_response_hook = failing_entity_query_hook
        review_incomplete = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_incomplete.run_id, RUN_ID_A)
        self.assertEqual(review_incomplete.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_incomplete.remote_state, "UNKNOWN")
        self.assertEqual(review_incomplete.action, "MANUAL_REVIEW_ONLY")

    def test_point8_discovery_and_inspection_produce_zero_database_mutations(self):
        """Discovery and inspection paths produce zero database mutations."""
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

        # Snapshot entire DB state across all tables before Runner B operations
        def get_db_fingerprint(conn: sqlite3.Connection) -> dict[str, list[tuple]]:
            tables = ("staging_run_journal", "processed_articles", "stories", "run_locks", "durable_recovery_state", "schema_migrations")
            fp = {}
            for t in tables:
                cur = conn.execute(f"SELECT * FROM {t} ORDER BY 1")
                fp[t] = cur.fetchall()
            return fp

        before_state = get_db_fingerprint(self.conn_a)

        # Runner B executes all discovery & inspection paths
        _ = self.seam_b.discover_unresolved_runs()
        _ = self.seam_b.inspect_run_wire(RUN_ID_A)
        _ = self.seam_b.review_recovery_evidence()
        _ = self.seam_b.reconstruct_cross_runner_recovery_evidence()
        _ = self.seam_b.list_unresolved_runs()

        after_state = get_db_fingerprint(self.conn_a)
        self.assertEqual(before_state, after_state, "Discovery and inspection must produce zero database mutations")

    def test_point9_all_recovery_conclusions_remain_inert_and_manual_only(self):
        """All recovery conclusions remain strictly inert with remote_state=UNKNOWN and action=MANUAL_REVIEW_ONLY."""
        # Test clean discovery
        rev_clean = self.seam_b.review_recovery_evidence()
        self.assertEqual(rev_clean.remote_state, "UNKNOWN")
        self.assertEqual(rev_clean.action, "MANUAL_REVIEW_ONLY")
        self.assertIn(rev_clean.provenance, (CrossRunnerProvenance.HERMETIC_SIMULATION, CrossRunnerProvenance.UNVERIFIED_CANDIDATE))

        # Test intent registered
        self.seam_a.register_intent(
            run_id=RUN_ID_A,
            manifest_seal_hex=self.seal_a,
            target_inventory=self.entities_a,
        )
        rev_intent = self.seam_b.review_recovery_evidence()
        self.assertEqual(rev_intent.remote_state, "UNKNOWN")
        self.assertEqual(rev_intent.action, "MANUAL_REVIEW_ONLY")
        self.assertIn(rev_intent.provenance, (CrossRunnerProvenance.HERMETIC_SIMULATION, CrossRunnerProvenance.UNVERIFIED_CANDIDATE))

        # Test effects committed
        mutations = build_live02_synthetic_mutations(RUN_ID_A)
        self.seam_a.execute_atomic_stage_mutation(
            run_id=RUN_ID_A,
            expected_fencing_token=1,
            stage="LIVE-02",
            mutation_statements=mutations,
            manifest_seal_hex=self.seal_a,
        )
        rev_effect = self.seam_b.review_recovery_evidence()
        self.assertEqual(rev_effect.remote_state, "UNKNOWN")
        self.assertEqual(rev_effect.action, "MANUAL_REVIEW_ONLY")
        self.assertIn(rev_effect.provenance, (CrossRunnerProvenance.HERMETIC_SIMULATION, CrossRunnerProvenance.UNVERIFIED_CANDIDATE))

        # Verify no executable mutation, retry or cleanup channels exist on CrossRunnerReview
        for attr in ("execute", "retry", "takeover", "cleanup", "mutate"):
            self.assertFalse(hasattr(rev_clean, attr))
            self.assertFalse(hasattr(rev_intent, attr))
            self.assertFalse(hasattr(rev_effect, attr))


    def test_point10_wire_absence_never_falls_back_to_local_journal_and_reports_unknown(self):
        """P1-1: Wire ABSENT never falls back to local SQLite journal and reports AuditOutcomeState.UNKNOWN."""
        # 1. Runner A registers intent and effects in shared DB fixture, so local rows exist.
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

        # Confirm local journal row and entity rows genuinely exist in the database
        cur = self.conn_b.execute("SELECT status FROM staging_run_journal WHERE run_id = ?", (RUN_ID_A,))
        self.assertIsNotNone(cur.fetchone())

        # 2. Intercept mock wire so that staging_run_journal queries return a valid empty row set (genuine wire ABSENT)
        expected_cols = [
            {"name": c}
            for c in (
                "run_id",
                "owner_id",
                "fencing_token",
                "stage",
                "status",
                "manifest_seal_hex",
                "target_inventory_json",
                "created_at",
                "updated_at",
                "last_error",
            )
        ]

        def wire_absent_hook(request: httpx.Request) -> Optional[httpx.Response]:
            if b"staging_run_journal" in request.content:
                return httpx.Response(
                    200,
                    json={
                        "baton": None,
                        "base_url": None,
                        "results": [
                            {
                                "type": "ok",
                                "response": {
                                    "result": {
                                        "cols": expected_cols,
                                        "rows": [],
                                        "affected_row_count": 0,
                                    }
                                },
                            },
                            {"type": "ok", "response": {}},
                        ],
                    },
                )
            return None

        self.transport_b.custom_response_hook = wire_absent_hook

        # Confirm that inspect_run_wire genuinely succeeds and returns None (valid wire read returning zero rows)
        self.assertIsNone(self.seam_b.inspect_run_wire(RUN_ID_A))

        # Snapshot DB before audit
        cur = self.conn_b.execute("SELECT COUNT(*) FROM staging_run_journal")
        journal_count_before = cur.fetchone()[0]

        # 3. Call audit_workload: must return UNKNOWN, not CLEAN or RESIDUAL, with zero local fallback
        audit_res = self.seam_b.audit_workload(RUN_ID_A)
        self.assertEqual(audit_res, AuditOutcomeState.UNKNOWN)

        # 4. Assert zero mutations occurred to DB
        cur = self.conn_b.execute("SELECT COUNT(*) FROM staging_run_journal")
        journal_count_after = cur.fetchone()[0]
        self.assertEqual(journal_count_before, journal_count_after)

    def test_point11_sanitized_error_reporting_never_leaks_sentinel_or_payload_data(self):
        """P1-2: Untrusted values (sentinels, raw IDs, SQL injections) never leak into error messages or reasons."""
        sentinel_secret = "ATTACKER_SECRET_SENTINEL_XYZ_98765"
        malicious_sql = "DROP TABLE staging_run_journal; -- ' OR '1'='1"

        # 1. Invalid run_id with sentinel
        bad_run_row = [
            f"run_{sentinel_secret}",
            "worker_01",
            1,
            "LIVE-02",
            JournalStatus.ACKED.value,
            "0" * 64,
            "[]",
            "2026-11-07T12:00:00Z",
            "2026-11-07T12:00:00Z",
        ]
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            bad_run_row,
        )
        self.conn_a.commit()

        # Exception must not leak sentinel
        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertNotIn(sentinel_secret, str(ctx.exception))
        self.assertEqual(ctx.exception.code, "INVALID_RUN_ID")

        # Review report reasons must not leak sentinel
        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        for r in review.reasons:
            self.assertNotIn(sentinel_secret, r)
            self.assertNotIn(malicious_sql, r)
        self.assertNotIn(sentinel_secret, str(review))

        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.commit()

        # 2. Corrupted inventory payload with malicious SQL injection and sentinel
        corrupted_inv = json.dumps({"secret": sentinel_secret, "injection": malicious_sql})
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                RUN_ID_A,
                "worker_01",
                1,
                "LIVE-02",
                JournalStatus.ACKED.value,
                self.seal_a,
                corrupted_inv,
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertNotIn(sentinel_secret, str(ctx.exception))
        self.assertNotIn(malicious_sql, str(ctx.exception))

        review_inv = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_inv.decision, ReviewDecision.UNKNOWN)
        for r in review_inv.reasons:
            self.assertNotIn(sentinel_secret, r)
            self.assertNotIn(malicious_sql, r)

    def test_point12_malformed_nested_inventory_types_handled_inertly_without_type_error(self):
        """P1-3: Malformed nested inventory types ([] or {} for table/pk) never throw uncaught TypeError."""
        cases = [
            # table as list
            [{"table": ["unhashable_list"], "pk_column": "id", "pk_value": "val", "cleanup_dependency_order": 1, "expected_attributes": {}}] * 5,
            # table as dict
            [{"table": {"nested": "dict"}, "pk_column": "id", "pk_value": "val", "cleanup_dependency_order": 1, "expected_attributes": {}}] * 5,
            # pk_column as list
            [{"table": "processed_articles", "pk_column": [], "pk_value": "val", "cleanup_dependency_order": 1, "expected_attributes": {}}] * 5,
            # pk_value as list
            [{"table": "processed_articles", "pk_column": "article_id", "pk_value": [], "cleanup_dependency_order": 1, "expected_attributes": {}}] * 5,
            # expected_attributes as non-dict
            [{"table": "processed_articles", "pk_column": "article_id", "pk_value": "val", "cleanup_dependency_order": 1, "expected_attributes": "invalid_string"}] * 5,
        ]

        for idx, unauth_ents in enumerate(cases):
            self.conn_a.execute("DELETE FROM staging_run_journal")
            self.conn_a.execute(
                """
                INSERT INTO staging_run_journal (
                    run_id, owner_id, fencing_token, stage, status,
                    manifest_seal_hex, target_inventory_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    RUN_ID_A,
                    "worker_01",
                    1,
                    "LIVE-02",
                    JournalStatus.ACKED.value,
                    "f" * 64,
                    json.dumps(unauth_ents),
                    "2026-11-07T12:00:00Z",
                    "2026-11-07T12:00:00Z",
                ],
            )
            self.conn_a.commit()

            # Must not raise TypeError
            with self.assertRaises(CrossRunnerDiscoveryError):
                self.seam_b.discover_unresolved_runs()

            # Must return typed CrossRunnerReview with ReviewDecision.UNKNOWN
            review = self.seam_b.review_recovery_evidence()
            self.assertIsInstance(review, CrossRunnerReview)
            self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
            self.assertEqual(review.remote_state, "UNKNOWN")
            self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")

    def test_point13_malformed_hrana_envelopes_fail_closed_and_transport_swap_raises_permission_error(self):
        """P1-3: Malformed Hrana 200 envelopes return inert review, while transport swap raises PermissionError."""
        # 1. Malformed envelope: empty results list
        def empty_results_hook(request: httpx.Request) -> Optional[httpx.Response]:
            return httpx.Response(200, json={"results": []})

        self.transport_b.custom_response_hook = empty_results_hook

        # Discovery query handles empty results fail-closed
        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertEqual(ctx.exception.code, "WIRE_QUERY_MALFORMED")

        # Review returns typed inert UNKNOWN
        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.UNREADABLE)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")

        # 2. Malformed envelope: missing results key
        def missing_results_hook(request: httpx.Request) -> Optional[httpx.Response]:
            return httpx.Response(200, json={"type": "ok"})

        self.transport_b.custom_response_hook = missing_results_hook
        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertEqual(ctx.exception.code, "WIRE_QUERY_MALFORMED")

        # 3. CRUCIAL: Transport swap attempt must raise PermissionError and NEVER be swallowed
        self.transport_b.custom_response_hook = None
        foreign_transport = httpx.MockTransport(lambda req: httpx.Response(200, json={}))
        self.seam_b._pinned_transport = foreign_transport

        with self.assertRaises(PermissionError):
            self.seam_b.discover_unresolved_runs()

        with self.assertRaises(PermissionError):
            self.seam_b.review_recovery_evidence()

        with self.assertRaises(PermissionError):
            self.seam_b.audit_workload(RUN_ID_A)

    def test_point14_oversized_payload_ceilings_and_discovery_boundary_duplicates(self):
        """P2-1: Strict payload length ceilings reject oversized records, and discovery checks duplicates at boundary."""
        # 1. Oversized target_inventory_json (> 4096 bytes)
        huge_payload = json.dumps({"padding": "x" * 5000})
        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                RUN_ID_A,
                "worker_01",
                1,
                "LIVE-02",
                JournalStatus.ACKED.value,
                self.seal_a,
                huge_payload,
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertEqual(ctx.exception.code, "OVERSIZED_INVENTORY")

        review_huge = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_huge.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_huge.journal_status, CrossRunnerJournalStatus.CORRUPTED)

        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.commit()

        # 2. Oversized stage (> 64 chars)
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                RUN_ID_A,
                "worker_01",
                1,
                "S" * 100,
                JournalStatus.ACKED.value,
                self.seal_a,
                "[]",
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertEqual(ctx.exception.code, "INVALID_STAGE")

        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.commit()

        # 3. Discovery boundary duplicate check
        valid_row = {
            "run_id": RUN_ID_A,
            "owner_id": "worker_01",
            "fencing_token": 1,
            "stage": "LIVE-02",
            "status": JournalStatus.ACKED.value,
            "manifest_seal_hex": self.seal_a,
            "target_inventory_json": json.dumps([e.model_dump() for e in self.entities_a]),
            "created_at": "2026-11-07T12:00:00Z",
            "updated_at": "2026-11-07T12:00:00Z",
            "last_error": None,
        }

        def dup_rows_hook(request: httpx.Request) -> Optional[httpx.Response]:
            if b"staging_run_journal" in request.content:
                cols = [{"name": k} for k in valid_row.keys()]
                hrana_row = [{"type": "text", "value": str(v)} if v is not None else {"type": "null"} for v in valid_row.values()]
                hrana_row[2] = {"type": "integer", "value": "1"}
                return httpx.Response(
                    200,
                    json={
                        "baton": None,
                        "base_url": None,
                        "results": [
                            {
                                "type": "ok",
                                "response": {
                                    "result": {
                                        "cols": cols,
                                        "rows": [hrana_row, hrana_row],
                                        "affected_row_count": 0,
                                    }
                                },
                            },
                            {"type": "ok", "response": {}},
                        ],
                    },
                )
            return None

        self.transport_b.custom_response_hook = dup_rows_hook

        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertEqual(ctx.exception.code, "DUPLICATE_DISCOVERY_RESULT")

        review_dup = self.seam_b.review_recovery_evidence()
        self.assertEqual(review_dup.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_dup.journal_status, CrossRunnerJournalStatus.UNREADABLE)
        self.assertEqual(review_dup.remote_state, "UNKNOWN")
        self.assertEqual(review_dup.action, "MANUAL_REVIEW_ONLY")
    def test_point15_forged_foreign_pk_with_matching_seal_fails_closed_before_entity_select(self):
        """P1-1: A forged foreign PK with a recomputed matching seal fails closed before any entity SELECT."""
        forged_run_id = RUN_ID_A
        # Create forged 5-entity inventory: allowlisted table processed_articles, but targeting a foreign victim PK
        forged_entities = [e.model_dump() for e in create_default_manifest_entities(forged_run_id)]
        forged_pk = "victim_secret_art_999"
        forged_entities[0]["pk_value"] = forged_pk

        # Recompute a matching seal over this forged inventory
        matching_seal = compute_canonical_inventory_fingerprint(forged_run_id, entities=forged_entities)

        # Insert forged record into staging_run_journal
        self.conn_a.execute("DELETE FROM staging_run_journal")
        self.conn_a.execute(
            """
            INSERT INTO staging_run_journal (
                run_id, owner_id, fencing_token, stage, status,
                manifest_seal_hex, target_inventory_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                forged_run_id,
                "worker_01",
                1,
                "LIVE-02",
                JournalStatus.ACKED.value,
                matching_seal,
                json.dumps(forged_entities),
                "2026-11-07T12:00:00Z",
                "2026-11-07T12:00:00Z",
            ],
        )
        self.conn_a.commit()

        # Set up request spy on Runner B transport to trace all executed wire queries
        captured_sql: list[str] = []

        def spy_hook(request: httpx.Request) -> Optional[httpx.Response]:
            try:
                body = json.loads(request.content.decode("utf-8"))
                for req in body.get("requests", []):
                    stmt = req.get("stmt", {})
                    if "sql" in stmt:
                        captured_sql.append(stmt["sql"])
            except Exception:
                pass
            return None

        self.transport_b.custom_response_hook = spy_hook

        # 1. Wire discovery must fail closed
        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertEqual(ctx.exception.code, "INVENTORY_VALIDATION_ERROR")
        self.assertNotIn(forged_pk, str(ctx.exception))

        # 2. Wire inspection / recovery review must report CORRUPTED / UNKNOWN inert review
        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.CORRUPTED)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")
        self.assertNotIn(forged_pk, str(review))

        # 3. CRUCIAL: Query tracing proves zero foreign-target reads occurred
        # No SELECT statement must contain the forged foreign PK or target the foreign article
        for sql in captured_sql:
            self.assertNotIn(forged_pk, sql)
            # Only allowlisted discovery SELECT on staging_run_journal should have run
            self.assertTrue(
                "staging_run_journal" in sql or "BEGIN" in sql or "COMMIT" in sql,
                f"Unexpected entity query executed: {sql}",
            )

    def test_point16_malformed_hrana_integer_cell_suppresses_chained_exception_and_traceback_leakage(self):
        """P1-2: Malformed Hrana integer cell with sentinel suppresses chained exception and traceback leakage."""
        import traceback

        sentinel_secret = "ATTACKER_SECRET_INT_SENTINEL_987654321"

        # Mock wire returns an integer cell whose value string contains the sentinel
        def malformed_int_hook(request: httpx.Request) -> Optional[httpx.Response]:
            if b"staging_run_journal" in request.content:
                cols = [
                    {"name": c}
                    for c in (
                        "run_id",
                        "owner_id",
                        "fencing_token",
                        "stage",
                        "status",
                        "manifest_seal_hex",
                        "target_inventory_json",
                        "created_at",
                        "updated_at",
                        "last_error",
                    )
                ]
                malformed_row = [
                    {"type": "text", "value": RUN_ID_A},
                    {"type": "text", "value": "worker_01"},
                    {"type": "integer", "value": sentinel_secret},  # Malformed integer cell!
                    {"type": "text", "value": "LIVE-02"},
                    {"type": "text", "value": JournalStatus.ACKED.value},
                    {"type": "text", "value": self.seal_a},
                    {"type": "text", "value": json.dumps([e.model_dump() for e in self.entities_a])},
                    {"type": "text", "value": "2026-11-07T12:00:00Z"},
                    {"type": "text", "value": "2026-11-07T12:00:00Z"},
                    {"type": "null"},
                ]
                return httpx.Response(
                    200,
                    json={
                        "baton": None,
                        "base_url": None,
                        "results": [
                            {
                                "type": "ok",
                                "response": {
                                    "result": {
                                        "cols": cols,
                                        "rows": [malformed_row],
                                        "affected_row_count": 0,
                                    }
                                },
                            },
                            {"type": "ok", "response": {}},
                        ],
                    },
                )
            return None

        self.transport_b.custom_response_hook = malformed_int_hook

        # 1. Direct discovery raises CrossRunnerDiscoveryError
        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()

        err = ctx.exception
        self.assertEqual(err.code, "WIRE_QUERY_MALFORMED")
        self.assertNotIn(sentinel_secret, str(err))

        # Verify that chained exception context / cause is completely suppressed
        self.assertIsNone(err.__cause__)
        self.assertTrue(err.__suppress_context__)

        # Format complete exception traceback as an error logger would
        tb_lines = traceback.format_exception(type(err), err, err.__traceback__)
        tb_text = "".join(tb_lines)

        # Sentinel must NOT appear in formatted traceback
        self.assertNotIn(sentinel_secret, tb_text)

        # 2. Review evidence returns inert UNKNOWN / UNREADABLE
        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.UNREADABLE)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")
        self.assertNotIn(sentinel_secret, str(review))

    def test_point17_malformed_query_metadata_fails_to_unreadable_never_reported_as_absent(self):
        """P2-2: Malformed execute metadata in 2-element results array reports UNREADABLE, never genuine ABSENT."""
        # Mock wire returns execute + close results, but execute result has empty response/cols metadata
        def malformed_meta_hook(request: httpx.Request) -> Optional[httpx.Response]:
            if b"staging_run_journal" in request.content:
                return httpx.Response(
                    200,
                    json={
                        "baton": None,
                        "base_url": None,
                        "results": [
                            {"type": "ok", "response": {}},  # Missing result / cols!
                            {"type": "ok", "response": {}},
                        ],
                    },
                )
            return None

        self.transport_b.custom_response_hook = malformed_meta_hook

        # 1. Direct discovery must fail closed with WIRE_QUERY_MALFORMED
        with self.assertRaises(CrossRunnerDiscoveryError) as ctx:
            self.seam_b.discover_unresolved_runs()
        self.assertEqual(ctx.exception.code, "WIRE_QUERY_MALFORMED")

        # 2. Review evidence must classify this as UNREADABLE, NEVER genuine ABSENT
        review = self.seam_b.review_recovery_evidence()
        self.assertEqual(review.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review.journal_status, CrossRunnerJournalStatus.UNREADABLE)
        self.assertNotEqual(review.journal_status, CrossRunnerJournalStatus.ABSENT)
        self.assertEqual(review.remote_state, "UNKNOWN")
        self.assertEqual(review.action, "MANUAL_REVIEW_ONLY")

        # 3. Scoped inspection also classifies as UNREADABLE, never ABSENT
        review_scoped = self.seam_b.review_recovery_evidence(run_id=RUN_ID_A)
        self.assertEqual(review_scoped.decision, ReviewDecision.UNKNOWN)
        self.assertEqual(review_scoped.journal_status, CrossRunnerJournalStatus.UNREADABLE)
        self.assertNotEqual(review_scoped.journal_status, CrossRunnerJournalStatus.ABSENT)
