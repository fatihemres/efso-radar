"""Deny-by-default Hrana *mock transport* boundary for a future G-C adapter.

This is NOT a live staging transport, token broker, PO grant, or SQL executor.
The only permitted backing transport is the existing concrete in-memory
HermeticStagingTransport inside BudgetTrackingTransport. A future live
implementation must have its own security review and explicit PO approval.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

import httpx

from efso_radar.repositories.turso_client import (
    normalize_turso_url,
    serialize_hrana_arg,
)
from efso_radar.staging.manifest_seal import OfflineManifestSeal
from efso_radar.staging.offline_operator import _MUTATING_TEMPLATES, _norm
from efso_radar.staging.runner import (
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    create_default_manifest_entities,
    verify_hermetic_session_transport,
)


_MAX_JSON_BYTES = 16_384
_MAX_REQUEST_PARTS = 8
_MAX_INTENTS = 16
_MAX_MOCK_REQUESTS = 60


class MockHranaDenied(PermissionError):
    """An unsafe mock protocol request permanently revokes this boundary."""


@dataclass(frozen=True)
class AuthorizedMockIntent:
    """One exact immutable intent; not a grant of real-world DB permission."""

    sql: str
    args: tuple[str | int, ...]


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("DUPLICATE_JSON_KEY")
        output[key] = value
    return output


def _wire_args(args: tuple[str | int, ...]) -> list[dict[str, Any]]:
    if type(args) is not tuple or len(args) > 8:
        raise ValueError("Invalid mock argument tuple")
    if any(
        (type(v) is str and len(v) > 512)
        or (type(v) is int and not -(2**63) <= v < 2**63)
        or type(v) not in (str, int)
        for v in args
    ):
        raise ValueError("Invalid mock argument scalar")
    return [serialize_hrana_arg(v) for v in args]


def _intent_key(sql: str, args: list[dict[str, Any]]) -> str:
    return json.dumps([sql, args], separators=(",", ":"), sort_keys=True)


class GuardedHermeticHranaTransport(httpx.BaseTransport):
    """Enforce SQL and parameter allowlists *at the mock outbound edge*.

    Unlike high-level runner wrappers, this also observes direct calls to
    TursoHTTPClient.execute_pipeline / execute_transaction / executescript
    when that client was constructed with THIS transport. It cannot protect
    unrelated client instances; it never wraps a real network transport.

    Mock transaction baton handling is one-shot: uncertain COMMIT => UNKNOWN,
    permanently halted, without blind retry or unsafe cleanup.
    """

    def __init__(
        self,
        *,
        inner: BudgetTrackingTransport,
        seal: OfflineManifestSeal,
        approved_intents: tuple[AuthorizedMockIntent, ...],
    ) -> None:
        verify_hermetic_session_transport(inner)
        if type(seal) is not OfflineManifestSeal:
            raise TypeError("Mock outbound boundary requires an offline seal.")
        seal.assert_unchanged()
        if (
            type(approved_intents) is not tuple
            or not 0 < len(approved_intents) <= _MAX_INTENTS
        ):
            raise ValueError("Expected a bounded tuple of approved mock intents.")

        allowed: set[str] = set()
        inventory = create_default_manifest_entities(seal.run_id)
        for item in approved_intents:
            if type(item) is not AuthorizedMockIntent or type(item.sql) is not str:
                raise ValueError("Invalid mock intent.")
            sql = item.sql
            if len(sql) > 1024 or _norm(sql) != sql:
                raise ValueError("Noncanonical mock SQL.")
            values = _wire_args(item.args)
            if not item.args or type(item.args[0]) is not str:
                raise ValueError("Registered primary key is required.")

            spec = _MUTATING_TEMPLATES.get(sql)
            if spec is not None:
                table, operation, count = spec
                if len(item.args) != count:
                    raise ValueError("Wrong mock mutation parameter count.")
            else:
                # Only a single registered-PK existence read is accepted.
                matches = [
                    e for e in inventory
                    if sql == f"SELECT 1 AS found FROM {e.table} WHERE {e.pk_column} = ?"
                    and e.pk_value == item.args[0]
                ]
                if len(matches) != 1 or len(item.args) != 1:
                    raise ValueError("SQL not part of the safe mock inventory.")
                table = matches[0].table
                operation = "read"

            entity = next(
                (e for e in inventory if e.table == table and e.pk_value == item.args[0]),
                None,
            )
            if entity is None:
                raise ValueError("Mock SQL identity not registered.")
            seal.require_registered_identity(
                entity.table, entity.pk_column, entity.pk_value
            )
            attrs = entity.expected_attributes
            if operation == "insert":
                if table == "processed_articles" and (
                    item.args[2] != attrs["title"] or item.args[3] != attrs["source"]
                ):
                    raise ValueError("Unregistered synthetic article marker.")
                if table == "stories" and (
                    item.args[1] != attrs["headline_tr"]
                    or item.args[2] != attrs["category"]
                ):
                    raise ValueError("Unregistered synthetic story marker.")
                if table == "run_locks" and item.args[1] not in attrs["worker_ids"]:
                    raise ValueError("Unregistered synthetic lock owner.")
                if table == "durable_recovery_state" and (
                    item.args[1] != attrs["stage"]
                    or item.args[2] != attrs["worker_id"]
                ):
                    raise ValueError("Unregistered synthetic recovery marker.")
            if operation == "delete":
                expected = {
                    "processed_articles": (attrs["source"], attrs["title"]),
                    "stories": (attrs["category"], attrs["headline_tr"]),
                    "run_locks": tuple(attrs["worker_ids"]),
                    "durable_recovery_state": (attrs["stage"], attrs["worker_id"]),
                }[table]
                if item.args[1:] != expected:
                    raise ValueError("Unregistered synthetic cleanup marker.")
            key = _intent_key(sql, values)
            if key in allowed:
                raise ValueError("Duplicate mock intent would allow retries.")
            allowed.add(key)

        self._inner = inner
        self._seal = seal
        self._allow = frozenset(allowed)
        self._spent: set[str] = set()
        self._endpoint = normalize_turso_url(
            EXACT_STAGING_DESTINATION, allow_localhost_http=False
        )
        self._pending_baton: str | None = None
        self._requests = 0
        self.halted = False
        self.unknown = False

    def _deny(self, code: str) -> None:
        self.halted = True
        self.unknown = True
        # Never place SQL, tokens, URL contents, or wire payload in messages.
        raise MockHranaDenied("MOCK_HRANA_DENIED_" + code)

    def _validate(self, request: httpx.Request) -> str:
        verify_hermetic_session_transport(self._inner)
        if request.method != "POST" or str(request.url) != self._endpoint:
            self._deny("DESTINATION")
        raw = request.content
        if not raw or len(raw) > _MAX_JSON_BYTES:
            self._deny("BODY_SIZE")
        try:
            obj = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_object_without_duplicates,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError()),
            )
        except (ValueError, UnicodeError, TypeError):
            self._deny("MALFORMED_JSON")
        if type(obj) is not dict or set(obj) - {"baton", "requests"}:
            self._deny("ENVELOPE")
        parts = obj.get("requests")
        if type(parts) is not list or not 1 <= len(parts) <= _MAX_REQUEST_PARTS:
            self._deny("REQUEST_SHAPE")
        baton = obj.get("baton")
        if self._pending_baton is not None:
            if baton != self._pending_baton or len(parts) != 2:
                self._deny("BATON_STATE")
            if parts != [
                {"type": "execute", "stmt": {"sql": "COMMIT"}},
                {"type": "close"},
            ] and parts != [
                {"type": "execute", "stmt": {"sql": "ROLLBACK"}},
                {"type": "close"},
            ]:
                self._deny("CONTROL_STATEMENT")
            return "transaction_end"

        if baton is not None:
            self._deny("UNEXPECTED_BATON")
        transaction = parts[0] == {"type": "execute", "stmt": {"sql": "BEGIN"}}
        statements = parts[1:] if transaction else parts[:-1]
        if not transaction and parts[-1] != {"type": "close"}:
            self._deny("PIPELINE_CLOSE")
        if not statements or len(statements) > _MAX_REQUEST_PARTS - 1:
            self._deny("STATEMENT_COUNT")
        keys = []
        for part in statements:
            if type(part) is not dict or set(part) != {"type", "stmt"}:
                self._deny("STATEMENT_SHAPE")
            if part["type"] != "execute":
                self._deny("STATEMENT_TYPE")
            stmt = part["stmt"]
            if type(stmt) is not dict or set(stmt) - {"sql", "args"}:
                self._deny("STATEMENT_SHAPE")
            if type(stmt.get("sql")) is not str:
                self._deny("SQL_TYPE")
            args = stmt.get("args", [])
            if type(args) is not list:
                self._deny("ARGS_TYPE")
            key = _intent_key(stmt["sql"], args)
            if key not in self._allow or key in self._spent or key in keys:
                self._deny("SQL_OR_ARGS_NOT_ALLOWED")
            keys.append(key)
        self._spent.update(keys)  # consume BEFORE dispatch; never blind retry
        return "transaction_begin" if transaction else "pipeline"

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if self.halted:
            self._deny("SESSION_HALTED")
        if self._requests >= _MAX_MOCK_REQUESTS:
            self._deny("REQUEST_BUDGET")
        try:
            self._seal.assert_unchanged()
            action = self._validate(request)
            # The response must acknowledge every Hrana operation, not just
            # an arbitrary nonempty prefix of a multi-statement batch.
            try:
                request_parts = json.loads(request.content)["requests"]
                expected_responses = len(request_parts)
            except Exception:
                self._deny("MALFORMED_JSON")
        except MockHranaDenied:
            raise
        except Exception:
            self._deny("PRE_DISPATCH")
        self._requests += 1
        try:
            response = self._inner.handle_request(request)
            if response.status_code != 200:
                self._deny("NON_200_UNKNOWN")
            data = response.json()
            if type(data) is not dict or type(data.get("results")) is not list:
                self._deny("RESPONSE_SHAPE")
            if len(data["results"]) != expected_responses or any(
                type(item) is not dict or item.get("type") != "ok"
                for item in data["results"]
            ):
                self._deny("RESPONSE_ERROR")
            if action == "transaction_begin":
                baton = data.get("baton")
                if type(baton) is not str or not baton.startswith("mock-baton-"):
                    self._deny("MISSING_BATON")
                self._pending_baton = baton
            elif action == "transaction_end":
                self._pending_baton = None
            return response
        except MockHranaDenied:
            raise
        except Exception:
            # A transport failure (including COMMIT timeout) is UNKNOWN.
            self._deny("TRANSPORT_OR_PROTOCOL_UNKNOWN")
