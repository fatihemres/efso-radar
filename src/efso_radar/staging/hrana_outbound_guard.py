"""Deny-by-default Hrana outbound security boundary for Phase 5B.3G-C.

This is NOT a live staging transport, token broker, PO grant, or SQL executor.
Enforces:
- Deny-by-default live Turso execution (live_authorized=False)
- Exact destination allowlisting and HTTPS pipeline endpoint
- Request envelope, payload size, and JSON non-duplication
- Transaction baton lifecycle enforcement
- Pre-registered manifest seal verification and SQL statement allowlisting
- Fixed mock validation and dynamic synthetic parameter verification
- Strict request budget ceiling (<100 requests) with 20-request cleanup reserve
- Session deadline (<120.0s elapsed)
- Ambiguous transaction handling (COMMIT uncertainty => UNKNOWN, halted)
- Zero secret / SQL payload leakage in errors
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import re
import time
from typing import Any, Callable

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
_MAX_SESSION_REQUESTS = 100
_TEST_CUTOFF_REQUESTS = 80
_SESSION_DEADLINE_SECONDS = 120.0
_ISO_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


class MockHranaDenied(PermissionError):
    """An unsafe protocol request permanently revokes this boundary."""


@dataclass(frozen=True)
class AuthorizedMockIntent:
    """One exact immutable intent; not a grant of real-world DB permission."""

    sql: str
    args: tuple[str | int, ...]


def is_valid_iso_timestamp(val: Any) -> bool:
    """Verify string is a strict ISO 8601 UTC timestamp."""
    return isinstance(val, str) and bool(_ISO_TIMESTAMP_RE.match(val))


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


_CANONICAL_STAMP = "2026-10-25T15:00:00Z"


def _require_canonical_mutation(
    *,
    table: str,
    operation: str,
    args: tuple[str | int, ...],
    run_id: str,
    attrs: dict[str, Any],
) -> None:
    """Check EVERY bound value against the fixed synthetic stage inventory.

    Intents are untrusted caller inputs: matching the SQL template and row's
    PK/ownership markers alone is not enough. This policy remains mock-only.
    """
    key = args[0]
    stamp = _CANONICAL_STAMP
    if operation == "delete":
        tails = {
            "processed_articles": (attrs["source"], attrs["title"]),
            "stories": (attrs["category"], attrs["headline_tr"]),
            "run_locks": tuple(attrs["worker_ids"]),
            "durable_recovery_state": (attrs["stage"], attrs["worker_id"]),
        }
        if args[1:] != tails[table]:
            raise ValueError("Noncanonical mock cleanup parameters.")
        return

    if operation != "insert":
        raise ValueError("Unexpected mock mutation operation.")

    if table == "processed_articles":
        if key == f"synth_art_{run_id}_01":
            urls = (f"https://example.com/synth/{run_id}/01",)
        elif key == f"synth_art_{run_id}_02_rollback":
            urls = (
                f"https://example.com/synth/{run_id}/02",
                f"https://example.com/synth/{run_id}/02_duplicate",
            )
        else:
            raise ValueError("Noncanonical mock article key.")
        if args[1] not in urls or args[2:] != (
            attrs["title"], attrs["source"], stamp, stamp
        ):
            raise ValueError("Noncanonical mock article payload.")
        return

    if table == "stories":
        expected = (
            attrs["headline_tr"],
            attrs["category"],
            stamp,
            stamp,
            json.dumps([f"synth_art_{run_id}_01"]),
            1,
            1,
        )
        if args[1:] != expected:
            raise ValueError("Noncanonical mock story payload.")
        return

    if table == "run_locks":
        alpha, beta = attrs["worker_ids"]
        allowed = (
            (alpha, stamp, "2026-10-25T15:05:00Z", stamp),
            (beta, stamp, "2026-10-25T15:05:00Z", stamp),
            (
                beta,
                "2026-10-25T15:06:00Z",
                "2026-10-25T15:11:00Z",
                "2026-10-25T15:06:00Z",
            ),
        )
        if args[1:] not in allowed:
            raise ValueError("Noncanonical mock lease payload.")
        return

    if table == "durable_recovery_state":
        if args[1:] != (
            attrs["stage"],
            attrs["worker_id"],
            json.dumps({"cursor": 100}),
            stamp,
            stamp,
        ):
            raise ValueError("Noncanonical mock recovery payload.")
        return

    raise ValueError("Unsupported mock mutation table.")


def validate_dynamic_mutation_args(
    *,
    table: str,
    operation: str,
    args: tuple[str | int, ...],
    run_id: str,
    attrs: dict[str, Any],
) -> None:
    """Validate all mutation parameters against dynamic session identities and attributes."""
    key = args[0]
    if operation == "delete":
        tails = {
            "processed_articles": (attrs["source"], attrs["title"]),
            "stories": (attrs["category"], attrs["headline_tr"]),
            "run_locks": tuple(attrs["worker_ids"]),
            "durable_recovery_state": (attrs["stage"], attrs["worker_id"]),
        }
        if args[1:] != tails[table]:
            raise ValueError("Noncanonical dynamic cleanup parameters.")
        return

    if operation != "insert":
        raise ValueError("Unexpected dynamic mutation operation.")

    if table == "processed_articles":
        if key == f"synth_art_{run_id}_01":
            urls = (f"https://example.com/synth/{run_id}/01",)
        elif key == f"synth_art_{run_id}_02_rollback":
            urls = (
                f"https://example.com/synth/{run_id}/02",
                f"https://example.com/synth/{run_id}/02_duplicate",
            )
        else:
            raise ValueError("Noncanonical dynamic article key.")
        if (
            args[1] not in urls
            or args[2] != attrs["title"]
            or args[3] != attrs["source"]
            or not is_valid_iso_timestamp(args[4])
            or not is_valid_iso_timestamp(args[5])
        ):
            raise ValueError("Noncanonical dynamic article payload.")
        return

    if table == "stories":
        if (
            key != f"synth_story_{run_id}_01"
            or args[1] != attrs["headline_tr"]
            or args[2] != attrs["category"]
            or not is_valid_iso_timestamp(args[3])
            or not is_valid_iso_timestamp(args[4])
            or args[5] != json.dumps([f"synth_art_{run_id}_01"])
            or args[6] != 1
            or args[7] != 1
        ):
            raise ValueError("Noncanonical dynamic story payload.")
        return

    if table == "run_locks":
        alpha, beta = attrs["worker_ids"]
        if (
            key != f"synth_lock_{run_id}"
            or args[1] not in (alpha, beta)
            or not is_valid_iso_timestamp(args[2])
            or not is_valid_iso_timestamp(args[3])
            or not is_valid_iso_timestamp(args[4])
        ):
            raise ValueError("Noncanonical dynamic lease payload.")
        return

    if table == "durable_recovery_state":
        if (
            key != f"synth_stage_{run_id}::current"
            or args[1] != attrs["stage"]
            or args[2] != attrs["worker_id"]
            or args[3] != json.dumps({"cursor": 100})
            or not is_valid_iso_timestamp(args[4])
            or not is_valid_iso_timestamp(args[5])
        ):
            raise ValueError("Noncanonical dynamic recovery payload.")
        return

    raise ValueError("Unsupported dynamic mutation table.")


class HranaOutboundSecurityBoundary(httpx.BaseTransport):
    """Enforce deny-by-default Hrana security boundary at the outbound HTTP transport edge.

    Observes direct HTTP pipeline requests before network dispatch.
    Enforces:
    - Default deny of live Turso connections (live_authorized=False)
    - Exact destination allowlisting and HTTPS endpoint
    - Request payload size, duplicate key rejection, and JSON envelope
    - One-shot transaction baton lifecycle
    - Allowlisted SQL statement & argument verification against sealed manifest
    - Request count ceiling (<100) and cleanup reserve (80 test cutoff)
    - Session deadline (<120.0s elapsed)
    - No blind retry (spent intents consumed before dispatch)
    - Ambiguous outcome fail-closed permanently halting session
    - Zero credential / SQL payload leakage
    """

    def __init__(
        self,
        *,
        inner: httpx.BaseTransport,
        seal: OfflineManifestSeal,
        approved_intents: tuple[AuthorizedMockIntent, ...],
        live_authorized: bool = False,
        dynamic_validation: bool = False,
        clock: Callable[[], float] = time.monotonic,
        max_requests: int = _MAX_SESSION_REQUESTS,
        test_cutoff_requests: int = _TEST_CUTOFF_REQUESTS,
        cleanup_reserve_requests: int = 20,
        session_timeout_seconds: float = _SESSION_DEADLINE_SECONDS,
        verify_inner_hermetic: bool = False,
    ) -> None:
        if verify_inner_hermetic:
            verify_hermetic_session_transport(inner)
        elif not live_authorized:
            # Deny by default: real network transports strictly rejected without authorization
            is_hermetic = (
                type(inner) is BudgetTrackingTransport
                or hasattr(inner, "_conn")
                or getattr(inner, "is_hermetic", False)
            )
            if not is_hermetic:
                raise PermissionError(
                    "LIVE_TURSO_UNAUTHORIZED: Live Turso outbound execution is permanently "
                    "disabled by default pending PO authorization."
                )

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
            if operation in ("insert", "delete"):
                if dynamic_validation:
                    validate_dynamic_mutation_args(
                        table=table,
                        operation=operation,
                        args=item.args,
                        run_id=seal.run_id,
                        attrs=entity.expected_attributes,
                    )
                else:
                    _require_canonical_mutation(
                        table=table,
                        operation=operation,
                        args=item.args,
                        run_id=seal.run_id,
                        attrs=entity.expected_attributes,
                    )
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
        self._live_authorized = live_authorized
        self._clock = clock
        self._started_at = clock()
        self._max_requests = max_requests
        self._test_cutoff_requests = test_cutoff_requests
        self._cleanup_reserve_requests = cleanup_reserve_requests
        self._session_timeout_seconds = session_timeout_seconds
        self._in_cleanup = False
        self._verify_inner_hermetic = verify_inner_hermetic

    def set_cleanup_phase(self) -> None:
        """Mark transport as entering cleanup phase, permitting use of reserve."""
        self._in_cleanup = True

    def _deny(self, code: str) -> None:
        self.halted = True
        self.unknown = True
        # Never place SQL, tokens, URL contents, or wire payload in messages.
        raise MockHranaDenied("MOCK_HRANA_DENIED_" + code)

    def _validate(self, request: httpx.Request) -> str:
        if self._verify_inner_hermetic:
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
        if self._clock() - self._started_at >= self._session_timeout_seconds:
            self._deny("SESSION_DEADLINE_EXCEEDED")
        if self._requests >= self._max_requests:
            self._deny("REQUEST_BUDGET")
        if not self._in_cleanup and self._requests >= self._test_cutoff_requests:
            self._deny("CLEANUP_RESERVE")
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

    def close(self) -> None:
        if hasattr(self._inner, "close"):
            self._inner.close()


class GuardedHermeticHranaTransport(HranaOutboundSecurityBoundary):
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
        super().__init__(
            inner=inner,
            seal=seal,
            approved_intents=approved_intents,
            live_authorized=False,
            dynamic_validation=False,
            max_requests=_MAX_MOCK_REQUESTS,
            test_cutoff_requests=_MAX_MOCK_REQUESTS,
            cleanup_reserve_requests=0,
            session_timeout_seconds=_SESSION_DEADLINE_SECONDS,
            verify_inner_hermetic=True,
        )
