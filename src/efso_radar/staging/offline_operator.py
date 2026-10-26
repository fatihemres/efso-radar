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
    _preflight_attempted: bool = False

    def _create_and_persist_manifest(self) -> None:
        # A manifest and its preflight approval form ONE immutable session.
        # Re-entry must not replace the on-disk manifest or refresh the seal.
        # Check both parent fields: a partial persistence/sealing failure may
        # have created the manifest without assigning _seal yet.
        if (
            self.halt_before_cleanup
            or self.manifest is not None
            or self.manifest_path is not None
            or self._seal is not None
            or self._preflight_attempted
            or self._preflight_accepted
        ):
            self._deny("MANIFEST_REENTRY")
        self._preflight_accepted = False
        try:
            super()._create_and_persist_manifest()
            if self.manifest_path is None:
                self._deny("MANIFEST_MISSING")
            self._seal = OfflineManifestSeal.from_persisted(
                self.manifest_path, self.run_id
            )
        except Exception:
            # Do not permit retry after a partial filesystem write and do not
            # expose the underlying path/manifest/permission error.
            self._deny("MANIFEST_PREPARATION_FAILED")

    def _run_live_01(self) -> None:
        # Preflight is a one-shot gate, not a refreshable boolean. A second
        # attempt after success or failure permanently halts the session.
        if (
            self.halt_before_cleanup
            or self._preflight_attempted
            or self._preflight_accepted
        ):
            self._deny("PREFLIGHT_REENTRY")
        if self._seal is None or self.manifest_path is None:
            self._deny("MANIFEST_REQUIRED")
        self._preflight_accepted = False
        self._preflight_attempted = True
        try:
            super()._run_live_01()
        except Exception:
            self._deny("PREFLIGHT_FAILED")
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
        if any(
            type(value) not in (str, int, float, bool)
            or (type(value) is str and len(value) > 512)
            for value in params
        ):
            self._deny("PARAMETERS_INVALID")
        if type(params[0]) is not str:
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
        except Exception:
            # Unexpected validation errors also fail closed, without leaking
            # any raw manifest or filesystem data into a session report.
            self._deny("MANIFEST_INVALID")

        expected = entity.expected_attributes
        stamp = "2026-10-25T15:00:00Z"
        if table == "processed_articles":
            if operation == "insert":
                # Pair each canonical key with only its matching synthetic URL.
                suffix = "/01" if target == f"synth_art_{self.run_id}_01" else "/02"
                allowed_urls = {f"https://example.com/synth/{self.run_id}{suffix}"}
                if suffix == "/02":
                    allowed_urls.add(f"https://example.com/synth/{self.run_id}/02_duplicate")
                if (
                    params[1] not in allowed_urls
                    or params[2:4] != [expected["title"], expected["source"]]
                    or params[4:] != [stamp, stamp]
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
                    params[1:3] != [expected["headline_tr"], expected["category"]]
                    or params[3:5] != [stamp, stamp]
                    or article_ids != [f"synth_art_{self.run_id}_01"]
                    or type(params[6]) is not int
                    or type(params[7]) is not int
                    or params[6:] != [1, 1]
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["category"], expected["headline_tr"]]:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "run_locks":
            workers = expected["worker_ids"]
            if operation == "insert":
                accepted = (
                    [workers[0], stamp, "2026-10-25T15:05:00Z", stamp],
                    [workers[1], stamp, "2026-10-25T15:05:00Z", stamp],
                    [
                        workers[1],
                        "2026-10-25T15:06:00Z",
                        "2026-10-25T15:11:00Z",
                        "2026-10-25T15:06:00Z",
                    ],
                )
                if params[1:] not in accepted:
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != workers:
                self._deny("OWNERSHIP_MISMATCH")
        elif table == "durable_recovery_state":
            if operation == "insert":
                if (
                    params[1:3] != [expected["stage"], expected["worker_id"]]
                    or params[3] != json.dumps({"cursor": 100})
                    or params[4:] != [stamp, stamp]
                ):
                    self._deny("OWNERSHIP_MISMATCH")
            elif params[1:] != [expected["stage"], expected["worker_id"]]:
                self._deny("OWNERSHIP_MISMATCH")

    def _guard_read(self, sql: str, params: list[Any] | None) -> None:
        """Accept only known single-PK reads against this run's five identities.

        The parent preflight validator reads schema through its own hermetic
        read-only client. These are only the runner's direct stage reads.
        """
        if (
            type(params) is not list
            or len(params) != 1
            or type(params[0]) is not str
            or self._seal is None
        ):
            self._deny("READ_NOT_ALLOWLISTED")
        assert params is not None
        entity = next(
            (
                e for e in create_default_manifest_entities(self.run_id)
                if e.pk_value == params[0]
            ),
            None,
        )
        if entity is None:
            self._deny("READ_NOT_ALLOWLISTED")
        table, pk = entity.table, entity.pk_column
        approved = {
            f"SELECT 1 AS found FROM {table} WHERE {pk} = ?",
            f"SELECT * FROM {table} WHERE {pk} = ?",
            f"SELECT COUNT(*) AS cnt FROM {table} WHERE {pk} = ?",
        }
        specific = {
            "processed_articles": (
                f"SELECT title, source FROM {table} WHERE {pk} = ?",
                f"SELECT article_id FROM {table} WHERE {pk} = ?",
            ),
            "stories": (f"SELECT headline_tr, category FROM {table} WHERE {pk} = ?",),
            "run_locks": (f"SELECT worker_id FROM {table} WHERE {pk} = ?",),
            "durable_recovery_state": (
                f"SELECT state_json FROM {table} WHERE {pk} = ?",
            ),
        }
        approved.update(specific[table])
        if _norm(sql) not in approved:
            self._deny("READ_NOT_ALLOWLISTED")
        try:
            self._seal.require_registered_identity(table, pk, entity.pk_value)
        except Exception:
            # Unexpected validation errors also fail closed, without leaking
            # any raw manifest or filesystem data into a session report.
            self._deny("MANIFEST_INVALID")

    def _execute(self, sql: str, params: list[Any] | None = None) -> Any:
        if self.halt_before_cleanup:
            self._deny("SESSION_HALTED")
        if not isinstance(sql, str):
            self._deny("SQL_NOT_ALLOWLISTED")
        if len(sql) > 1024:
            self._deny("SQL_NOT_ALLOWLISTED")
        if _norm(sql).upper().startswith("SELECT "):
            self._guard_read(sql, params)
            return super()._execute(sql, params)
        self._guard_mutation(sql, params)
        return super()._execute(sql, params)

    def _execute_transaction(self, statements: list[tuple[str, list[Any]]]) -> list[Any]:
        if self.halt_before_cleanup:
            self._deny("SESSION_HALTED")
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
