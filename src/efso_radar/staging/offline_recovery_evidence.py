"""Pure, bounded orphan evidence review. No I/O, SQL, recovery or authorization.

Inputs are test-owned manifest bytes, a previously captured seal digest, and one
mock snapshot. HERMETIC_SIMULATION describes supplied evidence, not authenticated
remote provenance. This is an accidental-misuse guardrail, not an in-process
security boundary. Existing recovery CLI/store reports remain unchanged.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints, ValidationError

from efso_radar.staging.manifest_seal import MAX_MANIFEST_BYTES
from efso_radar.staging.runner import (
    RUN_ID_REGEX,
    PerRunOwnershipManifest,
    RegisteredEntity,
    create_default_manifest_entities,
)


class _FrozenModel(BaseModel):
    model_config = {
        "extra": "forbid",
        "frozen": True,
        "strict": True,
        "revalidate_instances": "always",
    }


class QueryState(str, Enum):
    OBSERVED = "OBSERVED"
    FAILED = "FAILED"
    UNAVAILABLE = "UNAVAILABLE"
    AMBIGUOUS = "AMBIGUOUS"


class CommitState(str, Enum):
    NOT_REPORTED = "NOT_REPORTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    AMBIGUOUS = "AMBIGUOUS"


class EntityEvidence(str, Enum):
    ABSENT_IN_SIMULATION = "ABSENT_IN_SIMULATION"
    PRESENT_OWNED = "PRESENT_OWNED"
    OWNERSHIP_MISMATCH = "OWNERSHIP_MISMATCH"
    DUPLICATE_CARDINALITY = "DUPLICATE_CARDINALITY"
    INCOMPLETE = "INCOMPLETE"
    QUERY_FAILED = "QUERY_FAILED"
    QUERY_UNAVAILABLE = "QUERY_UNAVAILABLE"
    QUERY_AMBIGUOUS = "QUERY_AMBIGUOUS"
    CONTRADICTORY = "CONTRADICTORY"
    UNOBSERVED = "UNOBSERVED"


class ReviewDecision(str, Enum):
    UNKNOWN = "UNKNOWN"
    RESIDUAL = "RESIDUAL"
    OWNERSHIP_MISMATCH_HALT = "OWNERSHIP_MISMATCH_HALT"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class ManifestEvidence(str, Enum):
    VALID = "VALID"
    MISSING = "MISSING"
    TAMPERED = "TAMPERED"
    INVALID = "INVALID"


_Text = Annotated[str, StringConstraints(max_length=512)]
_Column = Annotated[str, StringConstraints(max_length=64)]
_Row = Annotated[dict[_Column, _Text], Field(max_length=3)]
# Bounded ASCII ISO-8601 profile (20-32 characters), with calendar validation below.
_CREATED_AT_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})"
)


class EntityObservation(_FrozenModel):
    """At most two PK/ownership projections; two rows demonstrate bad cardinality."""

    table: _Column
    pk_column: _Column
    pk_value: _Text
    query_state: QueryState
    rows: tuple[_Row, ...] = Field(default=(), max_length=2)


class HermeticSnapshot(_FrozenModel):
    """One bounded mock observation per canonical entity; never accepts a client."""

    provenance: Literal["HERMETIC_SIMULATION"] = "HERMETIC_SIMULATION"
    manifest_sha256: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
    observations: tuple[EntityObservation, ...] = Field(max_length=5)
    commit_state: CommitState = CommitState.NOT_REPORTED


class EntityReview(_FrozenModel):
    entity: RegisteredEntity
    evidence: EntityEvidence
    observed_count: int | None = None


class RecoveryReview(_FrozenModel):
    """Inert review artifact: no audit CLEAN value or executable action channel."""

    run_id: str | None
    provenance: Literal["HERMETIC_SIMULATION"] = "HERMETIC_SIMULATION"
    manifest_evidence: ManifestEvidence
    decision: ReviewDecision
    remote_state: Literal["UNKNOWN"] = "UNKNOWN"
    action: Literal["MANUAL_REVIEW_ONLY"] = "MANUAL_REVIEW_ONLY"
    entities: tuple[EntityReview, ...]
    uncertainties: tuple[str, ...]
    manual_checks: tuple[str, ...] = (
        "Review the preserved source manifest and all five synthetic identities.",
        "Review ownership, cardinality, transaction uncertainty and dependency ordering.",
        "Obtain separate PO authorization and an independently reviewed adapter before any remote check.",
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate field")
        result[key] = value
    return result


def _classify(entity: RegisteredEntity, observation: EntityObservation) -> EntityReview:
    rows = observation.rows
    if observation.query_state != QueryState.OBSERVED:
        evidence = (
            EntityEvidence.CONTRADICTORY
            if rows
            else {
                QueryState.FAILED: EntityEvidence.QUERY_FAILED,
                QueryState.UNAVAILABLE: EntityEvidence.QUERY_UNAVAILABLE,
                QueryState.AMBIGUOUS: EntityEvidence.QUERY_AMBIGUOUS,
            }[observation.query_state]
        )
        return EntityReview(entity=entity, evidence=evidence)
    # A directly observed foreign canonical value takes precedence over missing
    # columns and duplicate rows. Unconfirmed query results never reach this check.
    expected = {entity.pk_column: entity.pk_value}
    expected.update(
        {
            ("worker_id" if k == "worker_ids" else k): v
            for k, v in entity.expected_attributes.items()
        }
    )
    if any(
        (row[k] not in value if isinstance(value, list) else row[k] != value)
        for row in rows
        for k, value in expected.items()
        if k in row
    ):
        return EntityReview(
            entity=entity,
            evidence=EntityEvidence.OWNERSHIP_MISMATCH,
            observed_count=len(rows),
        )
    if len(rows) > 1:
        return EntityReview(
            entity=entity,
            evidence=EntityEvidence.DUPLICATE_CARDINALITY,
            observed_count=len(rows),
        )
    if not rows:
        return EntityReview(
            entity=entity,
            evidence=EntityEvidence.ABSENT_IN_SIMULATION,
            observed_count=0,
        )
    row = rows[0]
    if set(row) != set(expected):
        evidence = EntityEvidence.INCOMPLETE
    else:
        evidence = EntityEvidence.PRESENT_OWNED
    return EntityReview(entity=entity, evidence=evidence, observed_count=1)


def review_recovery_evidence(
    *,
    expected_run_id: str,
    manifest_bytes: bytes | None,
    sealed_sha256: str,
    snapshot: HermeticSnapshot | None,
) -> RecoveryReview:
    """Classify only supplied bytes/projections, preserving every input on all paths.

    Capture bytes/digest via the existing owned fixture store and OfflineManifestSeal.
    No paths, repositories, callbacks, transport objects or SQL are accepted here.
    Missing queries are UNOBSERVED, never absence. Even five absent observations
    cannot establish rollback or remote CLEAN. Ambiguous COMMIT always stays UNKNOWN.
    """
    safe_run = (
        expected_run_id
        if type(expected_run_id) is str and RUN_ID_REGEX.fullmatch(expected_run_id)
        else None
    )
    inventory = create_default_manifest_entities(safe_run) if safe_run else []
    reviews = tuple(
        EntityReview(entity=e, evidence=EntityEvidence.UNOBSERVED)
        for e in sorted(
            inventory, key=lambda e: e.cleanup_dependency_order, reverse=True
        )
    )
    reasons = [
        "Remote state and rollback are unverified; local absence is simulation only."
    ]

    def report(state: ManifestEvidence, decision: ReviewDecision) -> RecoveryReview:
        return RecoveryReview(
            run_id=safe_run,
            manifest_evidence=state,
            decision=decision,
            entities=reviews,
            uncertainties=tuple(reasons),
        )

    if manifest_bytes is None:
        reasons.append(
            "Local manifest missing; obtain owner review of preserved evidence."
        )
        return report(ManifestEvidence.MISSING, ReviewDecision.UNKNOWN)
    if (
        not safe_run
        or type(manifest_bytes) is not bytes
        or not 0 < len(manifest_bytes) <= MAX_MANIFEST_BYTES
        or type(sealed_sha256) is not str
        or re.fullmatch(r"[a-f0-9]{64}", sealed_sha256) is None
    ):
        reasons.append("Invalid manifest input or missing seal proof.")
        return report(ManifestEvidence.INVALID, ReviewDecision.UNKNOWN)
    if hashlib.sha256(manifest_bytes).hexdigest() != sealed_sha256:
        reasons.append("Manifest differs from the captured seal.")
        return report(ManifestEvidence.TAMPERED, ReviewDecision.UNKNOWN)
    try:
        payload = json.loads(manifest_bytes, object_pairs_hook=_unique_object)
        # The new byte-input boundary rejects type coercion; legacy file reports
        # retain their historical parsing behavior and fields.
        PerRunOwnershipManifest.model_validate(payload, strict=True)
        manifest = PerRunOwnershipManifest.validate_payload(payload)
        if (
            not _CREATED_AT_RE.fullmatch(manifest.created_at)
            or datetime.fromisoformat(manifest.created_at).utcoffset() is None
        ):
            raise ValueError("Invalid manifest timestamp")
        if manifest.run_id != safe_run:
            raise ValueError("Run mismatch")
    except (ValueError, TypeError, RecursionError, PermissionError):
        reasons.append("Local manifest payload failed canonical validation.")
        return report(ManifestEvidence.INVALID, ReviewDecision.UNKNOWN)
    try:
        if type(snapshot) is not HermeticSnapshot:
            raise ValueError("Snapshot unavailable")
        snapshot = HermeticSnapshot.model_validate(snapshot)
        if snapshot.manifest_sha256 != sealed_sha256:
            raise ValueError("Snapshot seal mismatch")
    except (ValueError, TypeError, ValidationError):
        reasons.append("Snapshot unavailable, malformed or bound to another seal.")
        return report(ManifestEvidence.VALID, ReviewDecision.UNKNOWN)

    observations: dict[tuple[str, str, str], list[EntityObservation]] = {}
    keys = {(e.table, e.pk_column, e.pk_value) for e in inventory}
    unexpected = False
    for obs in snapshot.observations:
        key = (obs.table, obs.pk_column, obs.pk_value)
        if key not in keys:
            unexpected = True
        else:
            observations.setdefault(key, []).append(obs)
    results = []
    for review in reviews:
        entity = review.entity
        found = observations.get((entity.table, entity.pk_column, entity.pk_value), [])
        classified = [_classify(entity, obs) for obs in found]
        mismatch = next(
            (r for r in classified if r.evidence == EntityEvidence.OWNERSHIP_MISMATCH),
            None,
        )
        if mismatch is not None:
            results.append(mismatch)
        elif len(found) > 1:
            results.append(
                EntityReview(
                    entity=entity, evidence=EntityEvidence.DUPLICATE_CARDINALITY
                )
            )
        elif found:
            results.append(classified[0])
        else:
            results.append(review)
    reviews = tuple(results)
    states = {r.entity.pk_value: r.evidence for r in reviews}
    # LIVE-02's article/story are atomic peers. One absent and one present contradicts atomicity.
    peers = [states[e.pk_value] for e in inventory[:2]]
    contradiction = (
        EntityEvidence.ABSENT_IN_SIMULATION in peers
        and EntityEvidence.PRESENT_OWNED in peers
    )
    acknowledged_missing = snapshot.commit_state == CommitState.ACKNOWLEDGED and any(
        s != EntityEvidence.PRESENT_OWNED for s in peers
    )
    uncertain = any(
        r.evidence
        not in (EntityEvidence.PRESENT_OWNED, EntityEvidence.ABSENT_IN_SIMULATION)
        for r in reviews
    )
    if unexpected:
        reasons.append(
            "Unexpected observation identity; evidence is outside the registered inventory."
        )
    if contradiction:
        reasons.append(
            "LIVE-02 atomic peers contradict each other; transaction outcome is unknown."
        )
    if acknowledged_missing:
        reasons.append(
            "Acknowledged COMMIT lacks both fully owned LIVE-02 peers; evidence contradicts the acknowledgement."
        )
    if states[inventory[2].pk_value] == EntityEvidence.PRESENT_OWNED:
        reasons.append(
            "Tentative rollback article is present in simulation; rollback is not established."
        )
    if uncertain:
        reasons.append(
            "Incomplete, conflicting, unavailable or invalid entity evidence requires review."
        )
    if snapshot.commit_state == CommitState.AMBIGUOUS:
        reasons.append("Ambiguous COMMIT remains UNKNOWN; no retry is authorized.")
    if EntityEvidence.OWNERSHIP_MISMATCH in states.values():
        decision = ReviewDecision.OWNERSHIP_MISMATCH_HALT
    elif (
        uncertain
        or unexpected
        or contradiction
        or acknowledged_missing
        or snapshot.commit_state == CommitState.AMBIGUOUS
    ):
        decision = ReviewDecision.UNKNOWN
    elif EntityEvidence.PRESENT_OWNED in states.values():
        decision = ReviewDecision.RESIDUAL
    else:
        decision = ReviewDecision.REVIEW_REQUIRED
    return report(ManifestEvidence.VALID, decision)


# ---------------------------------------------------------------------------
# Cross-Runner Ownership & Recovery Architecture Contract (Issue #45)
# ---------------------------------------------------------------------------


class CrossRunnerJournalStatus(str, Enum):
    """Inert status enumeration for a candidate remote run ownership journal entry."""

    ABSENT = "ABSENT"
    INTENT_RECORDED = "INTENT_RECORDED"
    EFFECT_COMMITTED = "EFFECT_COMMITTED"
    ABORTED = "ABORTED"
    CORRUPTED = "CORRUPTED"
    UNREADABLE = "UNREADABLE"


class CrossRunnerProvenance(str, Enum):
    """Provenance classifier for cross-runner observation inputs."""

    HERMETIC_SIMULATION = "HERMETIC_SIMULATION"
    UNVERIFIED_CANDIDATE = "UNVERIFIED_CANDIDATE"
    AUTHENTICATED_REMOTE = "AUTHENTICATED_REMOTE"


class CrossRunnerJournalEntry(_FrozenModel):
    """Inert typed contract for a candidate cross-runner ownership journal row.

    Captures intent-before-effect sequencing, runner identity, and journal commit state.
    This is an offline evidence data model; it does not issue SQL or connect to a provider.
    """

    run_id: str
    runner_id: str
    status: CrossRunnerJournalStatus
    intent_stage: str
    target_entities: tuple[str, ...] = Field(default=(), max_length=10)
    journal_commit_state: CommitState = CommitState.ACKNOWLEDGED
    created_at: str
    journal_sha256: str | None = None


class CrossRunnerReview(_FrozenModel):
    """Inert review report for cross-runner recovery evidence.

    Permanent safety boundaries:
    - remote_state is always 'UNKNOWN'
    - action is always 'MANUAL_REVIEW_ONLY'
    - Never grants remote execution permission, cleanup SQL, or automatic retry.
    """

    run_id: str | None
    reviewing_runner_id: str
    provenance: CrossRunnerProvenance = CrossRunnerProvenance.HERMETIC_SIMULATION
    journal_status: CrossRunnerJournalStatus
    decision: ReviewDecision
    remote_state: Literal["UNKNOWN"] = "UNKNOWN"
    action: Literal["MANUAL_REVIEW_ONLY"] = "MANUAL_REVIEW_ONLY"
    reasons: tuple[str, ...]
    manual_checks: tuple[str, ...] = (
        "Verify remote database Block Writes status and ensure writes remain blocked.",
        "Inspect remote tables for conflicting runner IDs and orphaned synthetic keys.",
        "Confirm whether journal intent commit was acknowledged or ambiguous on the wire.",
        "Obtain explicit Product Owner authorization before any manual cleanup or recovery dispatch.",
    )


def review_cross_runner_recovery_evidence(
    *,
    expected_run_id: str,
    reviewing_runner_id: str,
    journal_entry: CrossRunnerJournalEntry | None,
    snapshot: HermeticSnapshot | None = None,
    provenance: CrossRunnerProvenance = CrossRunnerProvenance.HERMETIC_SIMULATION,
    attempted_replay: bool = False,
) -> CrossRunnerReview:
    """Classify cross-runner ownership journal and snapshot evidence offline.

    Evaluates:
    - Unique run and runner identity boundaries
    - Intent-before-effect sequencing vs ambiguous COMMIT on intent registration
    - Ambiguous effect commits with acknowledged intent
    - Conflicting runner ownership
    - Runner A crash / Runner B subsequent review
    - Replay attempts of committed/aborted runs
    - Missing, corrupted, or unreadable journal entries
    - Incomplete or contradictory observations

    Guarantees:
    - Caller-supplied hash alone is never ownership proof.
    - Missing or corrupted journal never authorizes mutations, cleanup, or CLEAN.
    - All outcomes are inert review records; remote_state remains 'UNKNOWN' and
      action remains 'MANUAL_REVIEW_ONLY'.
    """
    safe_run = (
        expected_run_id
        if type(expected_run_id) is str and RUN_ID_REGEX.fullmatch(expected_run_id)
        else None
    )
    reasons: list[str] = [
        "Cross-runner recovery evidence evaluated under offline inert safety contract; "
        "remote state is strictly UNKNOWN."
    ]

    def report(
        j_status: CrossRunnerJournalStatus,
        decision: ReviewDecision,
        extra_reasons: list[str],
    ) -> CrossRunnerReview:
        all_reasons = tuple(reasons + extra_reasons)
        return CrossRunnerReview(
            run_id=safe_run,
            reviewing_runner_id=str(reviewing_runner_id),
            provenance=provenance,
            journal_status=j_status,
            decision=decision,
            reasons=all_reasons,
        )

    if not safe_run:
        return report(
            CrossRunnerJournalStatus.ABSENT,
            ReviewDecision.UNKNOWN,
            ["Expected run_id is noncanonical or malformed."],
        )

    if not reviewing_runner_id or not isinstance(reviewing_runner_id, str):
        return report(
            CrossRunnerJournalStatus.ABSENT,
            ReviewDecision.UNKNOWN,
            ["Reviewing runner_id is invalid or empty."],
        )

    # 1. Missing journal entry
    if journal_entry is None or journal_entry.status == CrossRunnerJournalStatus.ABSENT:
        return report(
            CrossRunnerJournalStatus.ABSENT,
            ReviewDecision.UNKNOWN,
            [
                "Cross-runner ownership journal is ABSENT; cannot verify ownership across runners without a persistent journal.",
                "Missing journal never authorizes mutations, automatic cleanup, or retries.",
            ],
        )

    # 2. Corrupted or unreadable journal
    if journal_entry.status == CrossRunnerJournalStatus.CORRUPTED:
        return report(
            CrossRunnerJournalStatus.CORRUPTED,
            ReviewDecision.UNKNOWN,
            [
                "Cross-runner ownership journal record is CORRUPTED; schema or checksum invalid.",
                "Corrupted journal never authorizes mutations or cleanup.",
            ],
        )

    if journal_entry.status == CrossRunnerJournalStatus.UNREADABLE:
        return report(
            CrossRunnerJournalStatus.UNREADABLE,
            ReviewDecision.UNKNOWN,
            [
                "Cross-runner ownership journal query failed or is UNREADABLE; transport/read error.",
                "Unreadable journal state requires manual investigation.",
            ],
        )

    # 3. Intent registration itself had an ambiguous COMMIT
    if journal_entry.journal_commit_state == CommitState.AMBIGUOUS:
        return report(
            journal_entry.status,
            ReviewDecision.UNKNOWN,
            [
                "Intent registration suffered an ambiguous COMMIT; durability of the intent is unproven.",
                "Subsequent effect mutations are strictly forbidden when intent COMMIT is ambiguous.",
            ],
        )

    # 4. Run ID mismatch on journal entry
    if journal_entry.run_id != safe_run:
        return report(
            journal_entry.status,
            ReviewDecision.OWNERSHIP_MISMATCH_HALT,
            [
                f"Journal run_id '{journal_entry.run_id}' does not match expected run_id '{safe_run}'.",
                "Cross-runner ownership mismatch; execution halted.",
            ],
        )

    # 5. Attempted replay of an already committed or aborted run
    if attempted_replay or (
        journal_entry.status in (CrossRunnerJournalStatus.EFFECT_COMMITTED, CrossRunnerJournalStatus.ABORTED)
        and journal_entry.runner_id == reviewing_runner_id
    ):
        return report(
            journal_entry.status,
            ReviewDecision.OWNERSHIP_MISMATCH_HALT,
            [
                f"Attempted replay of run '{safe_run}' with terminal journal status '{journal_entry.status.value}'.",
                "Replaying previous run IDs is strictly prohibited to prevent duplicate mutations.",
            ],
        )

    # 6. Evaluate snapshot entity observations if provided
    extra_reasons: list[str] = []
    has_mismatch = False
    has_owned = False
    has_uncertain = False

    if snapshot is not None:
        if snapshot.commit_state == CommitState.AMBIGUOUS:
            extra_reasons.append(
                "Effect transaction COMMIT was ambiguous; remote effect outcome is unknown."
            )
            has_uncertain = True

        inventory = create_default_manifest_entities(safe_run)
        keys = {(e.table, e.pk_column, e.pk_value) for e in inventory}
        observed_keys = set()

        for obs in snapshot.observations:
            key = (obs.table, obs.pk_column, obs.pk_value)
            observed_keys.add(key)
            if key not in keys:
                extra_reasons.append(f"Observation on unexpected entity: {key}.")
                has_uncertain = True
                continue

            matching_entity = next(e for e in inventory if (e.table, e.pk_column, e.pk_value) == key)
            rev = _classify(matching_entity, obs)

            if rev.evidence == EntityEvidence.OWNERSHIP_MISMATCH:
                extra_reasons.append(f"Foreign ownership marker detected on {obs.table}:{obs.pk_value}.")
                has_mismatch = True
            elif rev.evidence == EntityEvidence.PRESENT_OWNED:
                has_owned = True
            elif rev.evidence in (
                EntityEvidence.INCOMPLETE,
                EntityEvidence.QUERY_FAILED,
                EntityEvidence.QUERY_UNAVAILABLE,
                EntityEvidence.QUERY_AMBIGUOUS,
                EntityEvidence.CONTRADICTORY,
                EntityEvidence.DUPLICATE_CARDINALITY,
            ):
                extra_reasons.append(f"Observation uncertainty on {obs.table}:{obs.pk_value} ({rev.evidence.value}).")
                has_uncertain = True

        if len(observed_keys) < len(keys):
            extra_reasons.append(
                f"Incomplete observations: observed {len(observed_keys)} of {len(keys)} registered entities."
            )
            has_uncertain = True

    # 7. Runner A lost / Runner B review decision synthesis
    if has_mismatch:
        return report(journal_entry.status, ReviewDecision.OWNERSHIP_MISMATCH_HALT, extra_reasons)

    if has_uncertain:
        return report(journal_entry.status, ReviewDecision.UNKNOWN, extra_reasons)

    # If journal intent was recorded but effect is pending/unconfirmed
    if journal_entry.status == CrossRunnerJournalStatus.INTENT_RECORDED:
        if has_owned:
            extra_reasons.append(
                "Journal indicates INTENT_RECORDED and owned entity rows were observed; "
                "Runner A may have crashed before effect acknowledgment or cleanup."
            )
            return report(journal_entry.status, ReviewDecision.RESIDUAL, extra_reasons)
        extra_reasons.append(
            "Journal indicates INTENT_RECORDED but no effect rows confirmed; outcome remains UNKNOWN."
        )
        return report(journal_entry.status, ReviewDecision.UNKNOWN, extra_reasons)

    if journal_entry.status == CrossRunnerJournalStatus.EFFECT_COMMITTED:
        if has_owned:
            extra_reasons.append(
                "Journal indicates EFFECT_COMMITTED and owned entity rows observed; residual state awaiting cleanup."
            )
            return report(journal_entry.status, ReviewDecision.RESIDUAL, extra_reasons)
        extra_reasons.append(
            "Journal indicates EFFECT_COMMITTED but no entity rows observed; review required."
        )
        return report(journal_entry.status, ReviewDecision.REVIEW_REQUIRED, extra_reasons)

    return report(journal_entry.status, ReviewDecision.UNKNOWN, extra_reasons)
