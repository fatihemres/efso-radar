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
    # Derive ownership columns from the canonical manifest, including either lock worker.
    attrs = entity.expected_attributes
    expected = {entity.pk_column: entity.pk_value}
    expected.update(
        {("worker_id" if k == "worker_ids" else k): v for k, v in attrs.items()}
    )
    row = rows[0]
    if set(row) != set(expected):
        evidence = EntityEvidence.INCOMPLETE
    elif any(
        row[k] not in v if isinstance(v, list) else row[k] != v
        for k, v in expected.items()
    ):
        evidence = EntityEvidence.OWNERSHIP_MISMATCH
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
        for e in sorted(inventory, key=lambda e: e.cleanup_dependency_order)
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
        if manifest.run_id != safe_run:
            raise ValueError("Run mismatch")
    except (ValueError, TypeError, RecursionError):
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
        if len(found) > 1:
            results.append(
                EntityReview(
                    entity=entity, evidence=EntityEvidence.DUPLICATE_CARDINALITY
                )
            )
        elif found:
            results.append(_classify(entity, found[0]))
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
