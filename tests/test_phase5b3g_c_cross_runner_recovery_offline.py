"""Strictly offline contract tests for cross-runner ownership and recovery (Issue #45).

Safety boundary:
- Hermetic offline evaluation only: zero database access, zero network calls, zero live tokens.
- Models candidate cross-runner ownership journal entries and entity observations.
- All classification decisions strictly produce inert reviews (MANUAL_REVIEW_ONLY, UNKNOWN/HALT).
- Proves caller-supplied hashes, missing journals, or ambiguous commits never authorize mutations or cleanup.
- Validates canonical target inventory binding and ensures raw secrets/foreign IDs never leak into reports.
"""
from __future__ import annotations

import hashlib

from efso_radar.staging.offline_recovery_evidence import (
    CommitState,
    CrossRunnerJournalEntry,
    CrossRunnerJournalStatus,
    CrossRunnerProvenance,
    CrossRunnerReview,
    EntityObservation,
    HermeticSnapshot,
    QueryState,
    ReviewDecision,
    review_cross_runner_recovery_evidence,
)
from efso_radar.staging.runner import (
    RUN_ID_REGEX,
    create_default_manifest_entities,
)


def _valid_run_id(suffix: str = "01") -> str:
    hex_token = hashlib.sha256(suffix.encode("utf-8")).hexdigest()[:12]
    run_id = f"run_20261029T200000Z_{hex_token}"
    assert RUN_ID_REGEX.fullmatch(run_id)
    return run_id


def _sample_journal_entry(
    run_id: str,
    runner_id: str = "runner_a",
    status: CrossRunnerJournalStatus = CrossRunnerJournalStatus.INTENT_RECORDED,
    intent_stage: str = "LIVE_02",
    target_entities: tuple[str, ...] | None = None,
    commit_state: CommitState = CommitState.ACKNOWLEDGED,
) -> CrossRunnerJournalEntry:
    if target_entities is None:
        target_entities = tuple(f"{e.table}:{e.pk_value}" for e in create_default_manifest_entities(run_id))
    return CrossRunnerJournalEntry(
        run_id=run_id,
        runner_id=runner_id,
        status=status,
        intent_stage=intent_stage,
        target_entities=target_entities,
        journal_commit_state=commit_state,
        created_at="2026-10-29T20:00:00Z",
        journal_sha256="a" * 64,
    )


def _sample_snapshot_with_owned_entities(run_id: str, commit_state: CommitState = CommitState.ACKNOWLEDGED) -> HermeticSnapshot:
    inventory = create_default_manifest_entities(run_id)
    observations = []
    for ent in inventory:
        row = {ent.pk_column: ent.pk_value}
        for k, v in ent.expected_attributes.items():
            row["worker_id" if k == "worker_ids" else k] = v[0] if isinstance(v, list) else v
        observations.append(
            EntityObservation(
                table=ent.table,
                pk_column=ent.pk_column,
                pk_value=ent.pk_value,
                query_state=QueryState.OBSERVED,
                rows=(row,),
            )
        )
    return HermeticSnapshot(
        manifest_sha256="b" * 64,
        observations=tuple(observations),
        commit_state=commit_state,
    )


class TestCrossRunnerOwnershipRecoveryContract:
    """Test suite verifying cross-runner ownership journal and recovery contract invariants."""

    def test_missing_journal_fails_closed_to_unknown_manual_review(self) -> None:
        """Scenario 1: Missing journal row fails closed; never authorizes mutations or cleanup."""
        run_id = _valid_run_id("missing")
        # Case A: journal_entry is None
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=None,
        )
        assert isinstance(review, CrossRunnerReview)
        assert review.decision == ReviewDecision.UNKNOWN
        assert review.journal_status == CrossRunnerJournalStatus.ABSENT
        assert review.remote_state == "UNKNOWN"
        assert review.action == "MANUAL_REVIEW_ONLY"
        assert any("journal is ABSENT" in r for r in review.reasons)
        assert any("Missing journal never authorizes" in r for r in review.reasons)

        # Case B: journal_entry explicitly ABSENT
        entry_absent = _sample_journal_entry(run_id, status=CrossRunnerJournalStatus.ABSENT)
        review_b = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry_absent,
        )
        assert review_b.decision == ReviewDecision.UNKNOWN
        assert review_b.action == "MANUAL_REVIEW_ONLY"

    def test_ambiguous_journal_commit_halts_to_unknown(self) -> None:
        """Scenario 2: Intent registration itself suffered ambiguous COMMIT; effect is strictly blocked."""
        run_id = _valid_run_id("ambig_j")
        entry = _sample_journal_entry(
            run_id=run_id,
            runner_id="runner_a",
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
            commit_state=CommitState.AMBIGUOUS,
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_a",
            journal_entry=entry,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert review.remote_state == "UNKNOWN"
        assert review.action == "MANUAL_REVIEW_ONLY"
        assert any("ambiguous COMMIT" in r for r in review.reasons)
        assert any("Subsequent effect mutations are strictly forbidden" in r for r in review.reasons)

    def test_journal_ack_with_ambiguous_effect_commit_remains_unknown(self) -> None:
        """Scenario 3: Journal intent registration acknowledged, but effect transaction was ambiguous."""
        run_id = _valid_run_id("ambig_eff")
        entry = _sample_journal_entry(
            run_id=run_id,
            runner_id="runner_a",
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
            commit_state=CommitState.ACKNOWLEDGED,
        )
        snapshot = _sample_snapshot_with_owned_entities(run_id, commit_state=CommitState.AMBIGUOUS)
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
            snapshot=snapshot,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert review.remote_state == "UNKNOWN"
        assert review.action == "MANUAL_REVIEW_ONLY"
        assert any("Effect transaction COMMIT was ambiguous" in r for r in review.reasons)

    def test_conflicting_owner_or_run_mismatch_halts_with_ownership_mismatch(self) -> None:
        """Scenario 4: Conflicting owner or run mismatch immediately halts fail-closed."""
        run_id = _valid_run_id("owner_a")
        other_run_id = _valid_run_id("owner_b")

        # Case A: Journal run ID does not match expected run ID
        mismatched_entry = _sample_journal_entry(run_id=other_run_id)
        review_a = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=mismatched_entry,
        )
        assert review_a.decision == ReviewDecision.OWNERSHIP_MISMATCH_HALT
        assert review_a.action == "MANUAL_REVIEW_ONLY"
        assert any("does not match expected canonical run ID" in r for r in review_a.reasons)

        # Case B: Entity observation contains foreign owner attributes
        entry = _sample_journal_entry(run_id=run_id)
        foreign_obs = EntityObservation(
            table="processed_articles",
            pk_column="article_id",
            pk_value=f"synth_art_{run_id}_01",
            query_state=QueryState.OBSERVED,
            rows=({"article_id": f"synth_art_{run_id}_01", "source": "FOREIGN_ATTACKER_SOURCE", "title": "foreign"},),
        )
        foreign_snapshot = HermeticSnapshot(
            manifest_sha256="c" * 64,
            observations=(foreign_obs,),
            commit_state=CommitState.ACKNOWLEDGED,
        )
        review_b = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
            snapshot=foreign_snapshot,
        )
        assert review_b.decision == ReviewDecision.OWNERSHIP_MISMATCH_HALT
        assert any("Foreign ownership marker detected" in r for r in review_b.reasons)

    def test_runner_a_lost_runner_b_review_preserves_inert_manual_review_only(self) -> None:
        """Scenario 5: Runner A crashed/lost mid-session; Runner B inspects recorded journal and snapshot."""
        run_id = _valid_run_id("lost_ra")
        entry = _sample_journal_entry(
            run_id=run_id,
            runner_id="runner_a",  # Original runner that crashed
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
        )
        snapshot = _sample_snapshot_with_owned_entities(run_id)

        # Runner B reviews Runner A's residual work
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",  # Subsequent runner
            journal_entry=entry,
            snapshot=snapshot,
        )
        assert review.decision == ReviewDecision.RESIDUAL
        assert review.journal_status == CrossRunnerJournalStatus.INTENT_RECORDED
        assert review.remote_state == "UNKNOWN"
        assert review.action == "MANUAL_REVIEW_ONLY"
        assert any("Runner A may have crashed before effect acknowledgment" in r for r in review.reasons)
        assert any("Verify remote database Block Writes status" in c for c in review.manual_checks)

    def test_attempted_replay_rejected_with_ownership_mismatch_halt(self) -> None:
        """Scenario 6: Caller-flagged replay of an already committed or aborted run is classified as halt.

        Note: This is an offline contract classification of caller-flagged re-execution attempts,
        not a live distributed runtime anti-replay or fencing system.
        """
        run_id = _valid_run_id("replay")
        entry = _sample_journal_entry(
            run_id=run_id,
            runner_id="runner_a",
            status=CrossRunnerJournalStatus.EFFECT_COMMITTED,
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_a",
            journal_entry=entry,
            attempted_replay=True,
        )
        assert review.decision == ReviewDecision.OWNERSHIP_MISMATCH_HALT
        assert review.action == "MANUAL_REVIEW_ONLY"
        assert any("Attempted re-execution or replay" in r for r in review.reasons)
        assert any("offline classification of caller-flagged re-execution" in r for r in review.reasons)

    def test_unreadable_or_corrupted_journal_fails_closed_to_unknown(self) -> None:
        """Scenario 7: Unreadable or corrupted journal fails closed without granting permissions."""
        run_id = _valid_run_id("corrupt")

        # Corrupted journal
        entry_corrupt = _sample_journal_entry(run_id=run_id, status=CrossRunnerJournalStatus.CORRUPTED)
        review_corrupt = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry_corrupt,
        )
        assert review_corrupt.decision == ReviewDecision.UNKNOWN
        assert review_corrupt.journal_status == CrossRunnerJournalStatus.CORRUPTED
        assert review_corrupt.action == "MANUAL_REVIEW_ONLY"
        assert any("CORRUPTED" in r for r in review_corrupt.reasons)

        # Unreadable journal
        entry_unread = _sample_journal_entry(run_id=run_id, status=CrossRunnerJournalStatus.UNREADABLE)
        review_unread = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry_unread,
        )
        assert review_unread.decision == ReviewDecision.UNKNOWN
        assert review_unread.journal_status == CrossRunnerJournalStatus.UNREADABLE
        assert review_unread.action == "MANUAL_REVIEW_ONLY"
        assert any("UNREADABLE" in r for r in review_unread.reasons)

    def test_incomplete_observations_fail_closed_to_unknown(self) -> None:
        """Scenario 8: Incomplete observations on registered entities fail closed to UNKNOWN."""
        run_id = _valid_run_id("incompl")
        entry = _sample_journal_entry(run_id=run_id, status=CrossRunnerJournalStatus.INTENT_RECORDED)

        # Snapshot with only 1 observation instead of all 5 registered entities
        partial_obs = EntityObservation(
            table="processed_articles",
            pk_column="article_id",
            pk_value=f"synth_art_{run_id}_01",
            query_state=QueryState.OBSERVED,
            rows=({"article_id": f"synth_art_{run_id}_01", "source": "staging_synth_test", "title": f"SYNTH_ARTICLE_{run_id}_01"},),
        )
        incomplete_snapshot = HermeticSnapshot(
            manifest_sha256="d" * 64,
            observations=(partial_obs,),
            commit_state=CommitState.ACKNOWLEDGED,
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
            snapshot=incomplete_snapshot,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert any("Incomplete observations: not all canonical registered entities were observed" in r for r in review.reasons)

    def test_caller_supplied_hash_alone_does_not_grant_remote_authorization(self) -> None:
        """Scenario 9: Caller-supplied hash or digest alone never establishes ownership or grants permission."""
        run_id = _valid_run_id("hashproof")
        entry = _sample_journal_entry(
            run_id=run_id,
            runner_id="runner_a",
            status=CrossRunnerJournalStatus.EFFECT_COMMITTED,
        )
        # Even with candidate provenance and valid matching hash, outcome remains inert and unauthenticated
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
            provenance=CrossRunnerProvenance.UNVERIFIED_CANDIDATE,
        )
        assert review.remote_state == "UNKNOWN"
        assert review.action == "MANUAL_REVIEW_ONLY"
        assert review.provenance == CrossRunnerProvenance.UNVERIFIED_CANDIDATE
        assert any("unverified caller-supplied assertions" in r for r in review.reasons)

    def test_noncanonical_or_invalid_inputs_fail_closed(self) -> None:
        """Scenario 10: Invalid or malformed run IDs fail closed to UNKNOWN."""
        review = review_cross_runner_recovery_evidence(
            expected_run_id="INVALID_RUN_ID!@#",
            reviewing_runner_id="runner_b",
            journal_entry=None,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert any("noncanonical or malformed" in r for r in review.reasons)

    # -----------------------------------------------------------------------
    # P2-1 Negative Regression Tests: Target Inventory & Coherence Validation
    # -----------------------------------------------------------------------

    def test_empty_target_inventory_fails_closed_to_unknown(self) -> None:
        """P2-1: Empty target inventory fails closed to UNKNOWN even with owned snapshot rows."""
        run_id = _valid_run_id("emptytgt")
        entry = _sample_journal_entry(
            run_id=run_id,
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
            target_entities=(),
        )
        snapshot = _sample_snapshot_with_owned_entities(run_id)
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
            snapshot=snapshot,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert any("Journal target inventory is empty" in r for r in review.reasons)

    def test_foreign_target_inventory_halts_with_ownership_mismatch(self) -> None:
        """P2-1: Journal target inventory with foreign entities halts with OWNERSHIP_MISMATCH_HALT."""
        run_id = _valid_run_id("foreigntgt")
        foreign_targets = (
            f"processed_articles:synth_art_{run_id}_01",
            f"stories:synth_story_{run_id}_01",
            f"processed_articles:synth_art_{run_id}_02",
            "run_locks:foreign_lock_attacker",
            f"durable_recovery_state:synth_stage_{run_id}::current",
        )
        entry = _sample_journal_entry(
            run_id=run_id,
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
            target_entities=foreign_targets,
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
        )
        assert review.decision == ReviewDecision.OWNERSHIP_MISMATCH_HALT
        assert any("foreign or unregistered entities" in r for r in review.reasons)

    def test_incomplete_target_inventory_fails_closed_to_unknown(self) -> None:
        """P2-1: Incomplete target inventory (<5 canonical entities) fails closed to UNKNOWN."""
        run_id = _valid_run_id("incomptgt")
        all_targets = tuple(f"{e.table}:{e.pk_value}" for e in create_default_manifest_entities(run_id))
        entry = _sample_journal_entry(
            run_id=run_id,
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
            target_entities=all_targets[:3],  # only 3 of 5
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert any("Journal target inventory is incomplete" in r for r in review.reasons)

    def test_duplicate_target_inventory_fails_closed_to_unknown(self) -> None:
        """P2-1: Duplicate targets in inventory fail closed to UNKNOWN."""
        run_id = _valid_run_id("duptgt")
        all_targets = tuple(f"{e.table}:{e.pk_value}" for e in create_default_manifest_entities(run_id))
        dup_targets = (all_targets[0], all_targets[0], all_targets[1], all_targets[2], all_targets[3])
        entry = _sample_journal_entry(
            run_id=run_id,
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
            target_entities=dup_targets,
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert any("contains duplicate entries" in r for r in review.reasons)

    def test_noncanonical_or_unsupported_stage_fails_closed_to_unknown(self) -> None:
        """P2-1: Noncanonical intent stage fails closed to UNKNOWN."""
        run_id = _valid_run_id("badstage")
        entry = _sample_journal_entry(
            run_id=run_id,
            intent_stage="FORGED_STAGE_UNSUPPORTED",
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert any("intent stage is noncanonical" in r for r in review.reasons)

    def test_unreported_commit_state_on_intent_recorded_fails_closed(self) -> None:
        """P2-1: Intent recorded with unproven/unreported commit state fails closed to UNKNOWN."""
        run_id = _valid_run_id("unreported")
        entry = _sample_journal_entry(
            run_id=run_id,
            status=CrossRunnerJournalStatus.INTENT_RECORDED,
            commit_state=CommitState.NOT_REPORTED,
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert any("without reported commit state" in r for r in review.reasons)

    # -----------------------------------------------------------------------
    # P2-2 Negative Regression Tests: Sanitized Report & No Identifier Leakage
    # -----------------------------------------------------------------------

    def test_sentinel_secret_identifiers_never_leak_into_serialized_report(self) -> None:
        """P2-2: Sentinel secret-like tokens in observations or foreign IDs are never echoed in model_dump_json()."""
        run_id = _valid_run_id("sanitized")
        secret_bearer = "SECRET_BEARER_TOKEN_9876543210"
        secret_bot_token = "bot123456789:ABCdefGhIjkLmNoPqRsTuVwXyZ"
        secret_foreign_table = "foreign_secret_audit_table"
        secret_foreign_pk = "secret_pk_credential_vault_item"

        entry = _sample_journal_entry(run_id=run_id)

        # 1. Unrecognized entity outside canonical registered inventory fails closed to UNKNOWN
        foreign_obs = EntityObservation(
            table=secret_foreign_table,
            pk_column="vault_key",
            pk_value=secret_foreign_pk,
            query_state=QueryState.OBSERVED,
            rows=({
                "vault_key": secret_foreign_pk,
                "token": secret_bearer,
                "bot": secret_bot_token,
            },),
        )
        snapshot = HermeticSnapshot(
            manifest_sha256="f" * 64,
            observations=(foreign_obs,),
            commit_state=CommitState.ACKNOWLEDGED,
        )

        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
            snapshot=snapshot,
        )

        serialized = review.model_dump_json()
        assert secret_bearer not in serialized, "Bearer secret token must not leak into report serialization"
        assert secret_bot_token not in serialized, "Bot token must not leak into report serialization"
        assert secret_foreign_table not in serialized, "Foreign table name must not leak into report serialization"
        assert secret_foreign_pk not in serialized, "Foreign PK value must not leak into report serialization"
        assert review.decision == ReviewDecision.UNKNOWN

        # 2. Recognized entity with foreign attribute payload halts with OWNERSHIP_MISMATCH_HALT
        entities = create_default_manifest_entities(run_id)
        article_entity = entities[0]
        secret_attacker_token = "SECRET_ATTACKER_ATTRIBUTE_PAYLOAD_XYZ"
        mismatch_obs = EntityObservation(
            table=article_entity.table,
            pk_column=article_entity.pk_column,
            pk_value=article_entity.pk_value,
            query_state=QueryState.OBSERVED,
            rows=({
                article_entity.pk_column: article_entity.pk_value,
                "source": secret_attacker_token,
            },),
        )
        snapshot_mismatch = HermeticSnapshot(
            manifest_sha256="f" * 64,
            observations=(mismatch_obs,),
            commit_state=CommitState.ACKNOWLEDGED,
        )
        review_mismatch = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id="runner_b",
            journal_entry=entry,
            snapshot=snapshot_mismatch,
        )
        serialized_mismatch = review_mismatch.model_dump_json()
        assert review_mismatch.decision == ReviewDecision.OWNERSHIP_MISMATCH_HALT
        assert secret_attacker_token not in serialized_mismatch, "Attacker payload must not leak into report serialization"

    def test_invalid_runner_id_is_safely_sanitized_and_never_leaked(self) -> None:
        """P2-2: Malformed or secret-like reviewing runner ID is sanitized and fails closed."""
        run_id = _valid_run_id("badrunner")
        secret_runner_id = "ATTACKER_SECRET_RUNNER_TOKEN_!@#$%"

        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id=secret_runner_id,
            journal_entry=None,
        )
        assert review.decision == ReviewDecision.UNKNOWN
        assert review.reviewing_runner_id == "INVALID_RUNNER_ID"
        assert secret_runner_id not in review.model_dump_json()

    def test_valid_format_runner_id_secret_never_leaked_into_serialized_report(self) -> None:
        """P2: Valid-format runner ID (e.g. bearer token) is kept private and never serialized into review report."""
        run_id = _valid_run_id("validfmt")
        secret_runner_id = "SECRET_BEARER_TOKEN_9876543210"

        # 1. Review with valid-format secret runner ID produces fixed role label in report
        entry = _sample_journal_entry(
            run_id=run_id,
            runner_id="runner_original",
            status=CrossRunnerJournalStatus.EFFECT_COMMITTED,
        )
        review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id=secret_runner_id,
            journal_entry=entry,
        )
        assert review.reviewing_runner_id == "VALIDATED_RECOVERY_RUNNER"
        assert secret_runner_id not in review.model_dump_json(), "Valid-format secret runner ID must not leak into serialized report"

        # 2. Replay classification still correctly compares internal raw runner ID
        replay_entry = _sample_journal_entry(
            run_id=run_id,
            runner_id=secret_runner_id,
            status=CrossRunnerJournalStatus.EFFECT_COMMITTED,
        )
        replay_review = review_cross_runner_recovery_evidence(
            expected_run_id=run_id,
            reviewing_runner_id=secret_runner_id,
            journal_entry=replay_entry,
        )
        assert replay_review.decision == ReviewDecision.OWNERSHIP_MISMATCH_HALT
        assert replay_review.reviewing_runner_id == "VALIDATED_RECOVERY_RUNNER"
        assert secret_runner_id not in replay_review.model_dump_json(), "Secret runner ID must not leak even on replay halt"
