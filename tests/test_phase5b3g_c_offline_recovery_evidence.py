"""Issue #39: synthetic manifest bytes and bounded, in-memory Schema v6 evidence."""

from __future__ import annotations

from contextlib import ExitStack, closing
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.staging.manifest_seal import MAX_MANIFEST_BYTES, OfflineManifestSeal
from efso_radar.staging.offline_recovery_evidence import (
    CommitState,
    EntityEvidence,
    EntityObservation,
    HermeticSnapshot,
    ManifestEvidence,
    QueryState,
    RecoveryReview,
    ReviewDecision,
    review_recovery_evidence,
)
from efso_radar.staging.persistent_manifest_store import (
    HermeticTestFixtureScope,
    HermeticTestManifestStore,
    ManifestStoreSecurityError,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    PerRunOwnershipManifest,
    create_default_manifest_entities,
    recover_manifest_offline,
)


RUN_ID = "run_20261029T090000Z_a1b2c3d4e5f6"
STAMP = "2026-10-29T09:00:00Z"
SECRET = "Bearer fixture-secret https://private.invalid/database chat_id=123456789"


@dataclass
class Fixture:
    store: HermeticTestManifestStore
    path: Path
    seal: OfflineManifestSeal

    def review(self, observations=(), *, commit=CommitState.NOT_REPORTED, **overrides):
        """Each call proves byte preservation, no file/network/SQLite activity or log leaks."""
        before = self.path.read_bytes() if self.path.exists() else None
        params = dict(
            expected_run_id=RUN_ID,
            manifest_bytes=before,
            sealed_sha256=self.seal.sha256,
            snapshot=HermeticSnapshot(
                manifest_sha256=self.seal.sha256,
                observations=tuple(observations),
                commit_state=commit,
            ),
        )
        params.update(overrides)
        try:
            with ExitStack() as stack:
                for target in (
                    "builtins.open",
                    "io.open",
                    "pathlib.Path.open",
                    "pathlib.Path.stat",
                    "pathlib.Path.read_bytes",
                    "pathlib.Path.write_bytes",
                    "pathlib.Path.unlink",
                    "socket.create_connection",
                    "socket.socket",
                    "sqlite3.connect",
                    "httpx.Client.send",
                    "httpx.HTTPTransport.handle_request",
                ):
                    stack.enter_context(
                        patch(
                            target,
                            side_effect=AssertionError("Pure classifier attempted I/O"),
                        )
                    )
                result = review_recovery_evidence(**params)
            rendered = result.model_dump_json()
            assert result.remote_state == "UNKNOWN"
            assert result.action == "MANUAL_REVIEW_ONLY"
            assert "CLEAN" not in rendered
            for forbidden in (
                SECRET,
                "Bearer",
                "https://",
                "libsql://",
                "123456789",
                "DELETE",
                "INSERT",
                "UPDATE",
                str(self.path),
            ):
                assert forbidden not in rendered
            return result
        finally:
            assert (self.path.read_bytes() if self.path.exists() else None) == before


@pytest.fixture
def fixture(tmp_path):
    with HermeticTestFixtureScope(tmp_path):
        store = HermeticTestManifestStore(tmp_path / "manifests")
        path = store.save_manifest(
            PerRunOwnershipManifest(
                run_id=RUN_ID,
                created_at=STAMP,
                registered_entities=create_default_manifest_entities(RUN_ID),
            )
        )
        _, seal = store.load_manifest(RUN_ID)
        yield Fixture(store, path, seal)


def observations(present=(), *, beta=False):
    result = []
    for i, entity in enumerate(create_default_manifest_entities(RUN_ID)):
        row = {entity.pk_column: entity.pk_value}
        for key, value in entity.expected_attributes.items():
            row["worker_id" if key == "worker_ids" else key] = (
                value[int(beta)] if isinstance(value, list) else value
            )
        result.append(
            EntityObservation(
                table=entity.table,
                pk_column=entity.pk_column,
                pk_value=entity.pk_value,
                query_state=QueryState.OBSERVED,
                rows=(row,) if i in present else (),
            )
        )
    return result


def by_key(report):
    return {r.entity.pk_value: r.evidence for r in report.entities}


def test_all_absent_is_only_simulated_absence_never_remote_clean_or_rollback(fixture):
    result = fixture.review(observations())
    assert result.decision == ReviewDecision.REVIEW_REQUIRED
    assert len(result.entities) == 5
    assert all(
        r.evidence == EntityEvidence.ABSENT_IN_SIMULATION for r in result.entities
    )
    assert "rollback are unverified" in result.uncertainties[0]
    assert result.model_dump_json() == fixture.review(observations()).model_dump_json()


@pytest.mark.parametrize("beta", [False, True])
def test_exact_inventory_ownership_attributes_rollback_and_dependency_order(
    fixture, beta
):
    result = fixture.review(observations(range(5), beta=beta))
    assert result.decision == ReviewDecision.RESIDUAL
    expected = sorted(
        create_default_manifest_entities(RUN_ID),
        key=lambda e: e.cleanup_dependency_order,
    )
    assert [r.entity for r in result.entities] == expected
    assert all(r.evidence == EntityEvidence.PRESENT_OWNED for r in result.entities)
    assert any(r.entity.pk_value.endswith("_02_rollback") for r in result.entities)


@pytest.mark.parametrize(
    "stage,present,beta",
    [
        ("LIVE-02", (0, 1), False),
        ("LIVE-03", (0, 1, 2), False),
        ("LIVE-04", (0, 1, 3), False),
        ("LIVE-05", (0, 1, 3), True),
        ("LIVE-06", (0, 1, 3, 4), True),
        ("LIVE-07", (0, 1, 3, 4), True),
    ],
)
def test_partial_sessions_from_real_in_memory_schema_v6(fixture, stage, present, beta):
    """Build stage-shaped remnants with actual Schema v6, project only ownership columns."""
    canonical = create_default_manifest_entities(RUN_ID)
    supplied = observations(range(5), beta=beta)
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.row_factory = sqlite3.Row
        for stmt in get_all_init_statements():
            conn.execute(stmt)
        for i in present:
            entity = canonical[i]
            row = dict(supplied[i].rows[0])
            # Fill schema-required fixture fields without copying schema definitions.
            for column in conn.execute(f"PRAGMA table_info({entity.table})"):
                if column["notnull"] and column["dflt_value"] is None:
                    row.setdefault(column["name"], STAMP)
            columns = ", ".join(row)
            conn.execute(
                f"INSERT INTO {entity.table} ({columns}) VALUES ({', '.join('?' for _ in row)})",
                list(row.values()),
            )
        conn.commit()
        traced = []
        conn.set_trace_callback(traced.append)
        snapshot = []
        for entity, projection in zip(canonical, supplied):
            columns = ", ".join(projection.rows[0])
            rows = conn.execute(
                f"SELECT {columns} FROM {entity.table} WHERE {entity.pk_column} = ? LIMIT 2",
                [entity.pk_value],
            ).fetchall()
            snapshot.append(
                projection.model_copy(update={"rows": tuple(dict(r) for r in rows)})
            )
        result = fixture.review(
            snapshot,
            commit=(
                CommitState.AMBIGUOUS
                if stage == "LIVE-07"
                else CommitState.NOT_REPORTED
            ),
        )
        assert result.decision == (
            ReviewDecision.UNKNOWN if stage == "LIVE-07" else ReviewDecision.RESIDUAL
        )
        assert sum(r.observed_count or 0 for r in result.entities) == len(present)
        assert len(traced) == 5 and all(sql.startswith("SELECT ") for sql in traced)


@pytest.mark.parametrize("present", [(0,), (1,), (0, 3, 4), (1, 2, 3)])
def test_contradictory_atomic_peers_remain_unknown(fixture, present):
    result = fixture.review(observations(present))
    assert result.decision == ReviewDecision.UNKNOWN
    assert any("atomic peers" in reason for reason in result.uncertainties)


@pytest.mark.parametrize("present", [(), (0, 1), tuple(range(5))])
def test_ambiguous_commit_stays_unknown_even_with_complete_mock_queries(
    fixture, present
):
    assert (
        fixture.review(observations(present), commit=CommitState.AMBIGUOUS).decision
        == ReviewDecision.UNKNOWN
    )


@pytest.mark.parametrize("present", [(), (0,), (1,), (3, 4)])
def test_acknowledged_commit_without_owned_atomic_peers_is_contradictory(
    fixture, present
):
    result = fixture.review(observations(present), commit=CommitState.ACKNOWLEDGED)
    assert result.decision == ReviewDecision.UNKNOWN
    assert any("Acknowledged COMMIT" in reason for reason in result.uncertainties)


def test_acknowledged_commit_with_owned_peers_is_still_only_residual_simulation(
    fixture,
):
    assert (
        fixture.review(observations((0, 1)), commit=CommitState.ACKNOWLEDGED).decision
        == ReviewDecision.RESIDUAL
    )


@pytest.mark.parametrize("index", range(5))
def test_every_pk_and_ownership_attribute_is_checked_without_echoing_values(
    fixture, index, caplog
):
    base = observations(range(5))
    for key in base[index].rows[0]:
        rows = ({**base[index].rows[0], key: SECRET},)
        obs = list(base)
        obs[index] = base[index].model_copy(update={"rows": rows})
        result = fixture.review(obs)
        assert result.decision == ReviewDecision.OWNERSHIP_MISMATCH_HALT
        assert by_key(result)[base[index].pk_value] == EntityEvidence.OWNERSHIP_MISMATCH
    assert SECRET not in caplog.text


@pytest.mark.parametrize("index", range(5))
def test_missing_ownership_proof_is_unknown(fixture, index):
    obs = observations(range(5))
    row = dict(obs[index].rows[0])
    row.pop(next(iter(row)))
    obs[index] = obs[index].model_copy(update={"rows": (row,)})
    result = fixture.review(obs)
    assert result.decision == ReviewDecision.UNKNOWN
    assert by_key(result)[obs[index].pk_value] == EntityEvidence.INCOMPLETE


@pytest.mark.parametrize(
    "query", [QueryState.FAILED, QueryState.UNAVAILABLE, QueryState.AMBIGUOUS]
)
@pytest.mark.parametrize("index", range(5))
def test_read_transport_errors_never_mean_absent_and_do_not_retry(
    fixture, query, index
):
    obs = observations()
    obs[index] = obs[index].model_copy(update={"query_state": query})
    result = fixture.review(obs)
    assert result.decision == ReviewDecision.UNKNOWN
    assert by_key(result)[obs[index].pk_value].value == "QUERY_" + query.value


def test_failed_query_with_rows_is_contradictory(fixture):
    obs = observations(range(5))
    obs[2] = obs[2].model_copy(update={"query_state": QueryState.FAILED})
    result = fixture.review(obs)
    assert result.decision == ReviewDecision.UNKNOWN
    assert by_key(result)[obs[2].pk_value] == EntityEvidence.CONTRADICTORY


@pytest.mark.parametrize("index", range(5))
def test_duplicate_rows_halt_for_every_entity(fixture, index):
    obs = observations(range(5))
    obs[index] = obs[index].model_copy(update={"rows": obs[index].rows * 2})
    result = fixture.review(obs)
    assert result.decision == ReviewDecision.UNKNOWN
    assert by_key(result)[obs[index].pk_value] == EntityEvidence.DUPLICATE_CARDINALITY


def test_duplicate_observations_missing_and_foreign_identity_are_not_absence(fixture):
    obs = observations()
    for supplied in (
        (),
        obs[:4],
        [obs[0], obs[0], *obs[2:]],
        [obs[0].model_copy(update={"pk_value": SECRET}), *obs[1:]],
    ):
        result = fixture.review(supplied)
        assert result.decision == ReviewDecision.UNKNOWN
        assert any(r.evidence == EntityEvidence.UNOBSERVED for r in result.entities)
    assert fixture.review([obs[0], obs[0], *obs[2:]]).entities


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"null",
        b"[]",
        b"not json",
        b"\xff",
        b"{}",
        b"{" + b'"a":1,"a":2}',
        b"[" * 2000,
        b"x" * (MAX_MANIFEST_BYTES + 1),
    ],
    ids=[
        "empty",
        "null",
        "array",
        "text",
        "encoding",
        "object",
        "duplicate-fields",
        "deep-nesting",
        "oversized",
    ],
)
def test_malformed_or_oversized_manifest_bytes_are_unknown(fixture, payload):
    result = fixture.review(
        manifest_bytes=payload, sealed_sha256=hashlib.sha256(payload).hexdigest()
    )
    assert result.manifest_evidence == ManifestEvidence.INVALID
    assert result.decision == ReviewDecision.UNKNOWN


@pytest.mark.parametrize(
    "change", ["run_id", "ownership", "pk", "order", "extra", "duplicate", "count"]
)
def test_canonical_manifest_validation_reused(fixture, change):
    data = json.loads(fixture.path.read_bytes())
    if change == "run_id":
        data["run_id"] = SECRET
    elif change == "ownership":
        data["registered_entities"][0]["expected_attributes"]["title"] = SECRET
    elif change == "pk":
        data["registered_entities"][0]["pk_column"] = "radar_cron"
    elif change == "order":
        data["registered_entities"][0]["cleanup_dependency_order"] = 0
    elif change == "extra":
        data["credential"] = SECRET
    elif change == "duplicate":
        data["registered_entities"][1] = data["registered_entities"][0]
    else:
        data["registered_entities"].pop()
    raw = json.dumps(data).encode()
    result = fixture.review(
        manifest_bytes=raw, sealed_sha256=hashlib.sha256(raw).hexdigest()
    )
    assert result.manifest_evidence == ManifestEvidence.INVALID


@pytest.mark.parametrize(
    "field,value",
    [("version", True), ("version", "1"), ("cleanup_dependency_order", "2")],
)
def test_manifest_types_are_not_coerced_at_new_byte_boundary(fixture, field, value):
    data = json.loads(fixture.path.read_bytes())
    if field == "cleanup_dependency_order":
        data["registered_entities"][0][field] = value
    else:
        data[field] = value
    raw = json.dumps(data).encode()
    result = fixture.review(
        manifest_bytes=raw, sealed_sha256=hashlib.sha256(raw).hexdigest()
    )
    assert result.manifest_evidence == ManifestEvidence.INVALID


def test_actual_fixture_tamper_and_missing_manifest_preserved(fixture):
    original = fixture.path.read_bytes()
    fixture.path.write_bytes(original + b" ")
    assert fixture.review(observations()).manifest_evidence == ManifestEvidence.TAMPERED
    fixture.path.unlink()  # Test-owned missing-file injection, never recovery behavior.
    assert fixture.review(observations()).manifest_evidence == ManifestEvidence.MISSING
    assert not fixture.path.exists()


@pytest.mark.parametrize(
    "overrides",
    [
        {"snapshot": None},
        {"snapshot": object()},
        {"sealed_sha256": ""},
        {"sealed_sha256": SECRET},
        {"manifest_bytes": SECRET},
        {"expected_run_id": SECRET},
        {"expected_run_id": RUN_ID + "\n"},
    ],
)
def test_missing_proof_and_invalid_types_are_fail_closed(fixture, overrides):
    assert fixture.review(**overrides).decision == ReviewDecision.UNKNOWN


def test_snapshot_validation_bounds_and_wrong_seal(fixture):
    base = observations(range(5))
    for field, value in (
        ("rows", base[0].rows * 3),
        ("rows", ({"x": "y" * 513},)),
        ("rows", ({str(i): "v" for i in range(4)},)),
        ("query_state", "CLEAN"),
        ("table", "x" * 65),
    ):
        bad = base[0].model_copy(update={field: value})
        # model_construct bypasses input parsing; classifier must revalidate.
        snapshot = HermeticSnapshot.model_construct(
            manifest_sha256=fixture.seal.sha256, observations=(bad, *base[1:])
        )
        assert fixture.review(snapshot=snapshot).decision == ReviewDecision.UNKNOWN
    for updates in (
        {"observations": tuple(base) * 2},
        {"manifest_sha256": "0" * 64},
        {"provenance": "REMOTE_VERIFIED"},
        {"commit_state": "CLEAN"},
    ):
        snapshot = HermeticSnapshot.model_construct(
            manifest_sha256=fixture.seal.sha256, observations=tuple(base)
        ).model_copy(update=updates)
        assert fixture.review(snapshot=snapshot).decision == ReviewDecision.UNKNOWN


def test_raw_manifest_metadata_is_never_in_review_artifact(fixture):
    data = json.loads(fixture.path.read_bytes())
    data["created_at"] = SECRET
    raw = json.dumps(data).encode()
    digest = hashlib.sha256(raw).hexdigest()
    snapshot = HermeticSnapshot(
        manifest_sha256=digest, observations=tuple(observations())
    )
    assert (
        fixture.review(
            manifest_bytes=raw, sealed_sha256=digest, snapshot=snapshot
        ).decision
        == ReviewDecision.REVIEW_REQUIRED
    )


def test_historical_reports_and_deletion_boundary_remain_unchanged(fixture):
    before = fixture.path.read_bytes()
    review = fixture.review(observations())
    for report in (
        recover_manifest_offline(fixture.path),
        fixture.store.validate_recovery_offline(RUN_ID),
    ):
        assert report.audit_state == AuditOutcomeState.OFFLINE_VALIDATION_PASSED
        assert report.manifest_preserved and report.entities_count == 5
        assert set(type(report).model_fields) == {
            "manifest_path",
            "run_id",
            "valid_schema",
            "entities_count",
            "audit_state",
            "manifest_preserved",
            "summary_message",
        }
    assert fixture.store.delete_manifest_if_clean(RUN_ID, review.decision) is False
    with pytest.raises(ManifestStoreSecurityError):
        fixture.store.delete_manifest_if_clean(RUN_ID, AuditOutcomeState.CLEAN)
    with pytest.raises(ValueError):
        ReviewDecision("CLEAN")
    with pytest.raises(ValidationError):
        RecoveryReview.model_validate({**review.model_dump(), "remote_state": "CLEAN"})
    assert fixture.path.read_bytes() == before
