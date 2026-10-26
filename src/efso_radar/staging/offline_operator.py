"""Offline-only sealed orchestration of the existing eight-stage mock runner.

This class inherits the parent runner's exact hermetic transport checks.
It intentionally does NOT accept real Turso HTTP clients, create credentials,
handle Block Writes, or provide an executable live entry point.

The statement inventory is deliberately fixed. If the underlying staging
simulation changes its synthetic SQL, update and independently review this
file and its adversarial tests; do not silently pass new SQL through.
"""
from __future__ import annotations

import json
from typing import Any

from efso_radar.staging.manifest_seal import OfflineManifestSeal
from efso_radar.staging.runner import (
    AuditOutcomeState,
    StagingSessionRunner,
    create_default_manifest_entities,
)


def _norm(sql: str) -> str:
    return " ".join(sql.split())


# Exactly the eight known synthetic mutation templates used by the G-B
# runner. These are not general SQL parsing or authorization rules.
_INSERT_ARTICLE = (
    "INSERT INTO processed_articles (article_id, canonical_url, title, source, published_at, processed_at) "
    "VALUES (?, ?, ?, ?, ?, ?)"
)
_INSERT_STORY = (
    "INSERT INTO stories (story_id, headline_tr, category, first_seen_at, last_updated_at, article_ids_json, update_count, is_active) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)
_INSERT_LOCK = (
    "INSERT INTO run_locks (lock_key, worker_id, acquired_at, expires_at, heartbeat_at) "
    "VALUES (?, ?, ?, ?, ?) "
    "ON CONFLICT(lock_key) DO UPDATE SET "
    "worker_id = excluded.worker_id, "
    "acquired_at = excluded.acquired_at, "
    "expires_at = excluded.expires_at, "
    "heartbeat_at = excluded.heartbeat_at "
    "WHERE expires_at < excluded.acquired_at OR worker_id = excluded.worker_id"
)
_INSERT_RECOVERY = (
    "INSERT INTO durable_recovery_state (state_key, stage, worker_id, state_json, created_at, updated_at) "
    "VALUES (?, ?, ?, ?, ?, ?) "
    "ON CONFLICT(state_key) DO UPDATE SET "
    "worker_id = excluded.worker_id, "
    "state_json = excluded.state_json, "
    "updated_at = excluded.updated_at"
)

# SQL maps to (table, operation, parameter count) and never to arbitrary names.
_MUTATING_TEMPLATES = {
    _norm(_INSERT_ARTICLE): ("processed_articles", "insert", 6),
    _norm(_INSERT_STORY): ("stories", "insert", 8),
    _norm(_INSERT_LOCK): ("run_locks", "insert", 5),
    _norm(_INSERT_RECOVERY): ("durable_recovery_state", "insert", 6),
    "DELETE FROM processed_articles WHERE article_id = ? AND source = ? AND title = ?":
        ("processed_articles", "delete", 3),
    "DELETE FROM stories WHERE story_id = ? AND category = ? AND headline_tr = ?":
        ("stories", "delete", 3),
    "DELETE FROM run_locks WHERE lock_key = ? AND (worker_id = ? OR worker_id = ?)":
        ("run_locks", "delete", 3),
    "DELETE FROM durable_recovery_state WHERE state_key = ? AND stage = ? AND worker_id = ?":
        ("durable_recovery_state", "delete", 3),
}


class SealedHermeticStagingRunner(StagingSessionRunner):
    """Run all eight stages with offline-only preflight and sealed write gates.

    This is an additive test harness, not an implementation of LIVE-02–08
    remote write orchestration. The inherited __init__ and run() keep the
    in-memory SQLite transport restriction and no-network CI guarantee.
    """

    _seal: OfflineManifestSeal | None = None
    _preflight_accepted: bool = False

    def _create_and_persist_manifest(self) -> None:
        super()._create_and_persist_manifest()
        assert self.manifest_path is not None
        self._seal = OfflineManifestSeal.from_persisted(self.manifest_path, self.run_id)

    def _run_live_01(self) -> None:
        # Fresh per-run baseline checks must succeed before ANY synthetic write.
        super()._run_live_01()
        self._preflight_accepted = True

    def _deny(self, code: str) -> None:
        # Preserve possible residual rows; never silently run cleanup after a
        # write firewall or seal failure.
        self.halt_before_cleanup = True
        self.audit_state = AuditOutcomeState.UNKNOWN
        raise PermissionError(f"OFFLINE_SEALED_GATE_{code}: mutation refused")

    def _guard_mutation(self, sql: str, params: list[Any] | None) -> None:
        if not self._preflight_accepted or self._seal is None:
            self._deny("PREFLIGHT_REQUIRED")
        spec = _MUTATING_TEMPLATES.get(_norm(sql))
        if spec is None:
            self._deny("SQL_NOT_ALLOWLISTED")
        table, operation, count = spec
        if type(params) is not list or len(params) != count:
            self._deny("PARAMETERS_INVALID")
        # Reject custom objects, nested values and unhashable payloads before
        # checking synthetic marker allowlists; malformed arguments must never
        # bypass the UNKNOWN/preserve-manifest failure path.
        if any(type(value) not in (str, int, float, bool) for value in params):
            self._deny("PARAMETERS_INVALID")

        assert params is not None
        target = params[0]
        entity = next(
            (e for e in create_default_manifest_entities(self.run_id)
             if e.table == table and e.pk_value == target),
            None,
        )
        if entity is None:
            self._deny("UNREGISTERED_IDENTITY")

        try:
            self._seal.require_registered_identity(
                entity.table, entity.pk_column, entity.pk_value
            )
        except (PermissionError, ValueError, OSError):
            self._deny("MANIFEST_INVALID")

        expected = entity.expected_attributes
        if table == "processed_articles":
            if operation == "insert":
                correct_url = {
                    f"https://example.com/synth/{self.run_id}/01",
                    f"https://example.com/synth/{self.run_id}/02",
                    f"https://example.com/synth/{self.run_id}/02_duplicate",
                }
                if (
                    params[1] not in correct_url
                    or params[2] != expected["title"]
                    or params[3] != expected["source"]
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["source"], expected["title"]]:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "stories":
            if operation == "insert":
                try:
                    article_ids = json.loads(params[5])
                except (ValueError, TypeError):
                    self._deny("OWNERSHIP_MISMATCH")
                if (
                    params[1] != expected["headline_tr"]
                    or params[2] != expected["category"]
                    or article_ids != [f"synth_art_{self.run_id}_01"]
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["category"], expected["headline_tr"]]:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "run_locks":
            workers = expected["worker_ids"]
            if operation == "insert":
                if params[1] not in workers:
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != workers:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "durable_recovery_state":
            if operation == "insert":
                if params[1:3] != [expected["stage"], expected["worker_id"]]:
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["stage"], expected["worker_id"]]:
                self._deny("OWNERSHIP_MISMATCH")

    def _execute(self, sql: str, params: list[Any] | None = None) -> Any:
        if not isinstance(sql, str):
            self._deny("SQL_NOT_ALLOWLISTED")
        if _norm(sql).upper().startswith("SELECT "):
            # Reads remain confined to the parent's in-memory HTTP mock.
            return super()._execute(sql, params)
        self._guard_mutation(sql, params)
        return super()._execute(sql, params)

    def _execute_transaction(self, statements: list[tuple[str, list[Any]]]) -> list[Any]:
        if not isinstance(statements, list) or not statements:
            self._deny("TRANSACTION_INVALID")
        # Validate ALL proposed statements before dispatching a transaction.
        for item in statements:
            if (
                not isinstance(item, tuple)
                or len(item) != 2
                or not isinstance(item[0], str)
            ):
                self._deny("TRANSACTION_INVALID")
            self._guard_mutation(item[0], item[1])
        return super()._execute_transaction(statements)
