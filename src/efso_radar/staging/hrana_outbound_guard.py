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
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Optional

import httpx

from efso_radar.repositories.turso_client import (
    normalize_turso_url,
    serialize_hrana_arg,
    validate_read_only_sql,
)
from efso_radar.staging.manifest_seal import OfflineManifestSeal
from efso_radar.staging.offline_operator import _MUTATING_TEMPLATES, _norm
from efso_radar.staging.runner import (
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
    RegisteredEntity,
    create_default_manifest_entities,
    verify_hermetic_session_transport,
)


_EXACT_PREFLIGHT_QUERIES = frozenset({
    _norm("SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"),
    _norm("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"),
    _norm("SELECT name FROM sqlite_master WHERE type='table' AND name = ?"),
    _norm("SELECT MAX(version) AS ver FROM schema_migrations"),
    _norm("SELECT DISTINCT status FROM telegram_delivery_records"),
    _norm("SELECT state_json FROM durable_recovery_state"),
    _norm(
        "SELECT recipient_chat_id, message_key, COUNT(*) as cnt "
        "FROM telegram_delivery_records "
        "GROUP BY recipient_chat_id, message_key "
        "HAVING COUNT(*) > 1"
    ),
    _norm("SELECT 1 AS ping"),
    _norm("SELECT 1 AS live01_probe"),
    _norm("SELECT 1 AS found"),
    _norm("SELECT 1"),
})

_SYNTHETIC_READ_TEMPLATES: dict[str, tuple[str, str, int, frozenset[str]]] = {
    # LIVE-02 read-backs
    _norm("SELECT title, source FROM processed_articles WHERE article_id = ?"): (
        "processed_articles", "article_id", 1, frozenset({"LIVE-02"})
    ),
    _norm("SELECT headline_tr, category FROM stories WHERE story_id = ?"): (
        "stories", "story_id", 1, frozenset({"LIVE-02"})
    ),
    # LIVE-03 read-back
    _norm("SELECT article_id FROM processed_articles WHERE article_id = ?"): (
        "processed_articles", "article_id", 1, frozenset({"LIVE-03"})
    ),
    # LIVE-05 read-back
    _norm("SELECT worker_id FROM run_locks WHERE lock_key = ?"): (
        "run_locks", "lock_key", 1, frozenset({"LIVE-05"})
    ),
    # LIVE-06 pre-read & read-back
    _norm("SELECT state_json FROM durable_recovery_state WHERE state_key = ?"): (
        "durable_recovery_state", "state_key", 1, frozenset({"LIVE-06"})
    ),
    # LIVE-07 reconciliation reads
    _norm("SELECT 1 AS f FROM processed_articles WHERE article_id = ?"): (
        "processed_articles", "article_id", 1, frozenset({"LIVE-07"})
    ),
    _norm("SELECT 1 AS f FROM stories WHERE story_id = ?"): (
        "stories", "story_id", 1, frozenset({"LIVE-07"})
    ),
    # LIVE-08 pre-deletion ownership reads
    _norm("SELECT * FROM processed_articles WHERE article_id = ?"): (
        "processed_articles", "article_id", 1, frozenset({"LIVE-08"})
    ),
    _norm("SELECT * FROM stories WHERE story_id = ?"): (
        "stories", "story_id", 1, frozenset({"LIVE-08"})
    ),
    _norm("SELECT * FROM run_locks WHERE lock_key = ?"): (
        "run_locks", "lock_key", 1, frozenset({"LIVE-08"})
    ),
    _norm("SELECT * FROM durable_recovery_state WHERE state_key = ?"): (
        "durable_recovery_state", "state_key", 1, frozenset({"LIVE-08"})
    ),
    # LIVE-08 3-state audit reads (COUNT)
    _norm("SELECT COUNT(*) AS cnt FROM processed_articles WHERE article_id = ?"): (
        "processed_articles", "article_id", 1, frozenset({"LIVE-08"})
    ),
    _norm("SELECT COUNT(*) AS cnt FROM stories WHERE story_id = ?"): (
        "stories", "story_id", 1, frozenset({"LIVE-08"})
    ),
    _norm("SELECT COUNT(*) AS cnt FROM run_locks WHERE lock_key = ?"): (
        "run_locks", "lock_key", 1, frozenset({"LIVE-08"})
    ),
    _norm("SELECT COUNT(*) AS cnt FROM durable_recovery_state WHERE state_key = ?"): (
        "durable_recovery_state", "state_key", 1, frozenset({"LIVE-08"})
    ),
    # Baseline pre-existence collision checks in LIVE-01 / LIVE-08
    _norm("SELECT 1 AS found FROM processed_articles WHERE article_id = ?"): (
        "processed_articles", "article_id", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
    _norm("SELECT 1 AS found FROM stories WHERE story_id = ?"): (
        "stories", "story_id", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
    _norm("SELECT 1 AS found FROM run_locks WHERE lock_key = ?"): (
        "run_locks", "lock_key", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
    _norm("SELECT 1 AS found FROM durable_recovery_state WHERE state_key = ?"): (
        "durable_recovery_state", "state_key", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
    _norm("SELECT 1 AS found FROM processed_articles WHERE article_id = ? LIMIT 1"): (
        "processed_articles", "article_id", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
    _norm("SELECT 1 AS found FROM stories WHERE story_id = ? LIMIT 1"): (
        "stories", "story_id", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
    _norm("SELECT 1 AS found FROM run_locks WHERE lock_key = ? LIMIT 1"): (
        "run_locks", "lock_key", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
    _norm("SELECT 1 AS found FROM durable_recovery_state WHERE state_key = ? LIMIT 1"): (
        "durable_recovery_state", "state_key", 1, frozenset({"LIVE-01", "LIVE-08"})
    ),
}

_STAGES_ORDER = (
    "LIVE-01",
    "LIVE-02",
    "LIVE-03",
    "LIVE-04",
    "LIVE-05",
    "LIVE-06",
    "LIVE-07",
    "LIVE-08",
)

_STAGE_ALLOWED_MUTATIONS: dict[str, frozenset[tuple[str, str]]] = {
    "LIVE-01": frozenset(),
    "LIVE-02": frozenset({("processed_articles", "insert"), ("stories", "insert")}),
    "LIVE-03": frozenset({("processed_articles", "insert")}),
    "LIVE-04": frozenset({("run_locks", "insert")}),
    "LIVE-05": frozenset({("run_locks", "insert")}),
    "LIVE-06": frozenset({("durable_recovery_state", "insert")}),
    "LIVE-07": frozenset(),
    "LIVE-08": frozenset({
        ("processed_articles", "delete"),
        ("stories", "delete"),
        ("run_locks", "delete"),
        ("durable_recovery_state", "delete"),
    }),
}

_MAX_JSON_BYTES = 16_384
_MAX_REQUEST_PARTS = 8
_MAX_INTENTS = 16
_MAX_MOCK_REQUESTS = 60
_MAX_SESSION_REQUESTS = 100
_TEST_CUTOFF_REQUESTS = 80
_SESSION_DEADLINE_SECONDS = 120.0
_ISO_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def validate_budget_invariants(
    *,
    request_ceiling: int,
    request_cutoff: int,
    cleanup_reserve: int,
    session_deadline_seconds: float,
    is_session: bool = True,
) -> None:
    """Validate numeric budget and deadline invariants fail-closed at instantiation."""
    for name, val in [
        ("request_ceiling", request_ceiling),
        ("request_cutoff", request_cutoff),
        ("cleanup_reserve", cleanup_reserve),
        ("session_deadline_seconds", session_deadline_seconds),
    ]:
        if isinstance(val, bool):
            raise TypeError(f"Invalid boolean type for {name}: {val!r}")
        if not isinstance(val, (int, float)):
            raise TypeError(f"Invalid numeric type for {name}: {type(val).__name__}")
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            raise ValueError(f"Non-finite float value for {name}: {val!r}")
        if val <= 0 and (name != "cleanup_reserve" or is_session):
            raise ValueError(f"{name} must be positive, got {val}")
        if val < 0:
            raise ValueError(f"{name} cannot be negative, got {val}")

    if request_ceiling > 100:
        raise ValueError(f"request_ceiling ({request_ceiling}) exceeds absolute limit of 100.")
    if request_cutoff > 80:
        raise ValueError(f"request_cutoff ({request_cutoff}) exceeds limit of 80.")
    if is_session:
        if cleanup_reserve < 20:
            raise ValueError(f"cleanup_reserve ({cleanup_reserve}) must be at least 20.")
        if request_cutoff + cleanup_reserve > request_ceiling:
            raise ValueError(
                f"request_cutoff ({request_cutoff}) + cleanup_reserve ({cleanup_reserve}) "
                f"exceeds request_ceiling ({request_ceiling})."
            )
    else:
        if request_cutoff > request_ceiling:
            raise ValueError(f"request_cutoff ({request_cutoff}) exceeds request_ceiling ({request_ceiling}).")

    if session_deadline_seconds > 120.0:
        raise ValueError(f"session_deadline_seconds ({session_deadline_seconds}) exceeds 120.0s ceiling.")


class MockHranaDenied(PermissionError):
    """An unsafe protocol request permanently revokes this boundary."""


@dataclass(frozen=True)
class AuthorizedMockIntent:
    """One exact immutable intent; not a grant of real-world DB permission."""

    sql: str
    args: tuple[str | int, ...]


def is_valid_iso_timestamp(val: Any) -> bool:
    """Verify string is a strict ISO 8601 UTC timestamp with calendar validity.

    Enforces:
    - String type matching canonical wire format ending in 'Z'
    - Real calendar date parsing (valid months 1..12, valid days per month, leap years)
    - Valid 24-hour time (hours 0..23, minutes 0..59, seconds 0..59)
    - Strict UTC timezone
    """
    if not isinstance(val, str) or not _ISO_TIMESTAMP_RE.match(val):
        return False
    try:
        dt = datetime.fromisoformat(val)
        return dt.tzinfo == timezone.utc
    except (ValueError, OverflowError):
        return False


def _verify_exact_hermetic_transport(inner: Any) -> None:
    """Strict fail-closed verification of hermetic transport ownership.

    Rejects duck-typing, fake attributes (_conn, is_hermetic), custom subclasses,
    and real network transports.
    """
    if inner is None:
        raise PermissionError("HERMETIC_TRANSPORT_REQUIRED: transport cannot be None.")
    if isinstance(inner, (httpx.HTTPTransport, httpx.AsyncHTTPTransport)):
        raise PermissionError("LIVE_TURSO_UNAUTHORIZED: Real HTTP network transport is strictly disallowed.")

    if type(inner) is BudgetTrackingTransport:
        verify_hermetic_session_transport(inner)
        return

    if type(inner) is HermeticStagingTransport:
        conn = getattr(inner, "conn", None)
        if not isinstance(conn, sqlite3.Connection):
            raise PermissionError("HERMETIC_TRANSPORT_REQUIRED: missing sqlite3.Connection.")
        cur = conn.cursor()
        cur.execute("PRAGMA database_list;")
        for row in cur.fetchall():
            db_file = row[2] if len(row) > 2 else ""
            if db_file and db_file != "" and not str(db_file).startswith(":memory:"):
                raise PermissionError("HERMETIC_TRANSPORT_REQUIRED: file-backed database rejected.")
        return

    raise PermissionError(
        f"HERMETIC_TRANSPORT_REQUIRED: Untrusted transport type {type(inner).__name__} rejected."
    )


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
        if table == "processed_articles":
            expected_tail = (attrs["source"], attrs["title"])
        elif table == "stories":
            expected_tail = (attrs["category"], attrs["headline_tr"])
        elif table == "run_locks":
            expected_tail = tuple(attrs["worker_ids"])
        elif table == "durable_recovery_state":
            expected_tail = (attrs["stage"], attrs["worker_id"])
        else:
            raise ValueError("Unsupported mock cleanup table.")

        if args[1:] != expected_tail:
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
    """Validate dynamic synthetic mutation parameters against sealed manifest attributes."""
    key = str(args[0])
    if operation == "delete":
        if table == "processed_articles":
            expected_tail = (attrs["source"], attrs["title"])
        elif table == "stories":
            expected_tail = (attrs["category"], attrs["headline_tr"])
        elif table == "run_locks":
            expected_tail = tuple(attrs["worker_ids"])
        elif table == "durable_recovery_state":
            expected_tail = (attrs["stage"], attrs["worker_id"])
        else:
            raise ValueError("Unsupported dynamic cleanup table.")

        if args[1:] != expected_tail:
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


def _extract_hrana_val(a: Any) -> Any:
    if isinstance(a, dict) and "type" in a:
        val = a.get("value")
        if a["type"] == "integer" and isinstance(val, (str, int)):
            return int(val)
        if a["type"] == "float" and isinstance(val, (str, float, int)):
            return float(val)
        return val
    return a


class HranaOutboundSecurityBoundary(httpx.BaseTransport):
    """Enforce deny-by-default Hrana security boundary at the outbound HTTP transport edge.

    Observes direct HTTP pipeline requests before dispatch.
    Enforces:
    - Default deny of live Turso connections (rejects non-hermetic transports)
    - Exact destination allowlisting and HTTPS endpoint
    - Request payload size, duplicate key rejection, and JSON envelope
    - One-shot transaction baton lifecycle (opaque string batons)
    - Allowlisted SQL statement & argument verification against sealed manifest
    - Request count ceiling (<100) and cleanup reserve (80 test cutoff)
    - Session deadline (<120.0s elapsed)
    - Ambiguous outcome fail-closed permanently halting session
    - Zero credential / SQL payload leakage
    """

    def __init__(
        self,
        *,
        inner: httpx.BaseTransport,
        seal: OfflineManifestSeal,
        approved_intents: Optional[tuple[AuthorizedMockIntent, ...]] = None,
        live_authorized: bool = False,
        dynamic_validation: bool = False,
        session_mode: bool = False,
        clock: Callable[[], float] = time.monotonic,
        max_requests: int = _MAX_SESSION_REQUESTS,
        test_cutoff_requests: int = _TEST_CUTOFF_REQUESTS,
        cleanup_reserve_requests: int = 20,
        session_timeout_seconds: float = _SESSION_DEADLINE_SECONDS,
        verify_inner_hermetic: bool = False,
    ) -> None:
        if live_authorized:
            raise PermissionError(
                "LIVE_TURSO_UNAUTHORIZED: Live Turso outbound execution is permanently "
                "disabled by default pending PO authorization."
            )

        # Strict hermetic transport verification (no duck typing bypasses)
        _verify_exact_hermetic_transport(inner)

        validate_budget_invariants(
            request_ceiling=max_requests,
            request_cutoff=test_cutoff_requests,
            cleanup_reserve=cleanup_reserve_requests,
            session_deadline_seconds=session_timeout_seconds,
            is_session=(approved_intents is None),
        )

        if type(seal) is not OfflineManifestSeal:
            raise TypeError("Mock outbound boundary requires an offline seal.")
        seal.assert_unchanged()

        self._inner = inner
        self._seal = seal
        self._dynamic_validation = dynamic_validation
        self._clock = clock
        self._started_at = clock()
        self._max_requests = max_requests
        self._test_cutoff_requests = test_cutoff_requests
        self._cleanup_reserve_requests = cleanup_reserve_requests
        self._session_timeout_seconds = session_timeout_seconds
        self._in_cleanup = False
        self._endpoint = normalize_turso_url(
            EXACT_STAGING_DESTINATION, allow_localhost_http=False
        )
        self._pending_baton: str | None = None
        self._requests = 0
        self.halted = False
        self.unknown = False
        self._live_authorized = False
        self._inventory = create_default_manifest_entities(seal.run_id)

        # Wire-level gate and stage tracking (cannot be escalated via public boolean)
        self._preflight_passed: bool = False
        self._human_window_confirmed: bool = False
        self._active_stage: str | None = None
        self._stage_sequence_index: int = 0
        self._preflight_queries_count: int = 0
        self._preflight_collision_checks: set[str] = set()
        self._spent_mutations: set[tuple[str, str, str, str]] = set()
        self._session_closed: bool = False

        # Determine mode: session_mode or legacy approved_intents tuple
        if approved_intents is None:
            self._session_mode = True
            self._allow: frozenset[str] = frozenset()
            self._spent: set[str] = set()
        else:
            self._session_mode = False
            if (
                type(approved_intents) is not tuple
                or not 0 < len(approved_intents) <= _MAX_INTENTS
            ):
                raise ValueError("Expected a bounded tuple of approved mock intents.")

            allowed: set[str] = set()
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
                    matches = [
                        e for e in self._inventory
                        if sql == f"SELECT 1 AS found FROM {e.table} WHERE {e.pk_column} = ?"
                        and e.pk_value == item.args[0]
                    ]
                    if len(matches) != 1 or len(item.args) != 1:
                        raise ValueError("SQL not part of the safe mock inventory.")
                    table = matches[0].table
                    operation = "read"

                entity = next(
                    (e for e in self._inventory if e.table == table and e.pk_value == item.args[0]),
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

            self._allow = frozenset(allowed)
            self._spent = set()

    def set_cleanup_phase(self) -> None:
        """Mark transport as entering cleanup phase, permitting use of reserve strictly during LIVE-08."""
        if self._active_stage != "LIVE-08" or self._stage_sequence_index != 8:
            self._deny("CLEANUP_RESERVE_ESCALATION")
        self._in_cleanup = True

    def record_preflight_completed(self, manifest_sha256: str) -> None:
        """Record preflight completion at wire boundary under sealed manifest assertion."""
        self._seal.assert_unchanged()
        if manifest_sha256 != self._seal.sha256:
            self._deny("MANIFEST_MISMATCH")
        if self._session_closed or self.halted:
            self._deny("SESSION_HALTED")
        if self._preflight_queries_count < 10 or len(self._preflight_collision_checks) < 5:
            self._deny("PREFLIGHT_EVIDENCE_MISSING")
        if self._preflight_passed:
            self._deny("PREFLIGHT_ALREADY_RECORDED")
        self._preflight_passed = True
        self._stage_sequence_index = 1

    def record_human_window_confirmed(
        self,
        marker_or_operator: Any,
        manifest_sha256_or_marker: str,
        manifest_sha256: str | None = None,
        *,
        operator: Any = None,
    ) -> None:
        """Record human approval window at wire boundary under sealed manifest and operator attestation."""
        if manifest_sha256 is None:
            # Legacy 2-argument call: record_human_window_confirmed(marker, manifest_sha256)
            marker = marker_or_operator
            sha = manifest_sha256_or_marker
            op = operator
        else:
            # 3-argument call: record_human_window_confirmed(operator, marker, manifest_sha256)
            op = marker_or_operator
            marker = manifest_sha256_or_marker
            sha = manifest_sha256

        self._seal.assert_unchanged()
        if sha != self._seal.sha256:
            self._deny("MANIFEST_MISMATCH")
        if self._session_closed or self.halted:
            self._deny("SESSION_HALTED")
        if not self._preflight_passed:
            self._deny("PREFLIGHT_REQUIRED")
        if self._human_window_confirmed:
            self._deny("HUMAN_WINDOW_ALREADY_CONFIRMED")
        if self._stage_sequence_index != 1:
            self._deny("OUT_OF_ORDER_STAGE")
        if marker != "MOCK_ONLY":
            self._deny("INVALID_HUMAN_MARKER")
        if op is None or not getattr(op, "_preflight_accepted", False) or getattr(op, "halt_before_cleanup", False):
            self._deny("OPERATOR_PREFLIGHT_NOT_ACCEPTED")
        if getattr(op, "security_boundary", None) is not self:
            self._deny("OPERATOR_BOUNDARY_MISMATCH")

        self._human_window_confirmed = True
        self._stage_sequence_index = 2

    def record_active_stage(self, stage_id: str, manifest_sha256: str) -> None:
        """Record active controlled live stage at wire boundary under sealed manifest assertion."""
        self._seal.assert_unchanged()
        if manifest_sha256 != self._seal.sha256:
            self._deny("MANIFEST_MISMATCH")
        if self._session_closed or self.halted:
            self._deny("SESSION_HALTED")
        if self._active_stage is not None:
            self._deny("STAGE_ALREADY_ACTIVE")
        if stage_id not in _STAGES_ORDER:
            self._deny("INVALID_STAGE")

        # Sequence ordering validation
        if stage_id == "LIVE-01":
            if self._stage_sequence_index != 0:
                self._deny("OUT_OF_ORDER_STAGE")
        elif stage_id == "LIVE-02":
            if not self._preflight_passed:
                self._deny("PREFLIGHT_REQUIRED")
            if not self._human_window_confirmed:
                self._deny("HUMAN_WINDOW_REQUIRED")
            if self._stage_sequence_index != 2:
                self._deny("OUT_OF_ORDER_STAGE")
        elif stage_id == "LIVE-08":
            if not self._preflight_passed:
                self._deny("PREFLIGHT_REQUIRED")
            if not self._human_window_confirmed:
                self._deny("HUMAN_WINDOW_REQUIRED")
            if self._stage_sequence_index < 2:
                self._deny("OUT_OF_ORDER_STAGE")
            self._stage_sequence_index = 8
        else:
            expected_index = _STAGES_ORDER.index(stage_id) + 1
            if self._stage_sequence_index != expected_index:
                self._deny("OUT_OF_ORDER_STAGE")

        self._active_stage = stage_id

    def record_stage_completed(self, stage_id: str, manifest_sha256: str) -> None:
        """Record stage completion under sealed manifest assertion and evidence verification."""
        self._seal.assert_unchanged()
        if manifest_sha256 != self._seal.sha256:
            self._deny("MANIFEST_MISMATCH")
        if self._session_closed or self.halted:
            self._deny("SESSION_HALTED")
        if self._active_stage != stage_id:
            self._deny("STAGE_MISMATCH")

        if stage_id == "LIVE-01":
            if self._preflight_queries_count < 10 or len(self._preflight_collision_checks) < 5:
                self._deny("PREFLIGHT_EVIDENCE_MISSING")
            self._preflight_passed = True
            self._stage_sequence_index = 1
        else:
            self._stage_sequence_index += 1

        self._active_stage = None

    def close_session(self) -> None:
        """Permanently close and invalidate this security boundary session."""
        self._session_closed = True

    def _deny(self, code: str) -> None:
        self.halted = True
        self.unknown = True
        # Never place SQL, tokens, URL contents, or wire payload in messages.
        raise MockHranaDenied("MOCK_HRANA_DENIED_" + code)

    def _validate_session_statement(self, sql: str, args: list[Any]) -> None:
        """Verify statement against wire gates, read least privilege, and sealed manifest."""
        norm_sql = _norm(sql)
        spec = _MUTATING_TEMPLATES.get(norm_sql)
        if spec is not None:
            # Wire-level gate checks before any mutation dispatch
            if not self._preflight_passed:
                self._deny("PREFLIGHT_REQUIRED")
            if not self._human_window_confirmed:
                self._deny("HUMAN_WINDOW_REQUIRED")
            if self._active_stage is None:
                self._deny("NO_ACTIVE_STAGE")

            table, operation, count = spec
            allowed_mutations = _STAGE_ALLOWED_MUTATIONS.get(self._active_stage, frozenset())
            if (table, operation) not in allowed_mutations:
                self._deny("STAGE_MUTATION_MISMATCH")

            if len(args) != count:
                self._deny("PARAMETERS_INVALID")

            raw_args = [_extract_hrana_val(a) for a in args]
            pk_value = str(raw_args[0]) if raw_args else ""

            # Mutation replay protection (strictly single-shot in LIVE-02 and LIVE-08)
            mutation_key = (self._active_stage, table, operation, pk_value)
            if self._active_stage in ("LIVE-02", "LIVE-08") and mutation_key in self._spent_mutations:
                self._deny("MUTATION_ALREADY_SPENT")

            entity = next((e for e in self._inventory if e.table == table and e.pk_value == pk_value), None)
            if entity is None:
                self._deny("UNREGISTERED_IDENTITY")

            try:
                self._seal.require_registered_identity(entity.table, entity.pk_column, entity.pk_value)
                if self._dynamic_validation:
                    validate_dynamic_mutation_args(
                        table=table,
                        operation=operation,
                        args=tuple(raw_args),
                        run_id=self._seal.run_id,
                        attrs=entity.expected_attributes,
                    )
                else:
                    _require_canonical_mutation(
                        table=table,
                        operation=operation,
                        args=tuple(raw_args),
                        run_id=self._seal.run_id,
                        attrs=entity.expected_attributes,
                    )
            except MockHranaDenied:
                raise
            except Exception:
                self._deny("PARAMETERS_INVALID")

            self._spent_mutations.add(mutation_key)
            return

        upper = norm_sql.upper()
        if not (upper.startswith("SELECT ") or upper.startswith("WITH ") or upper.startswith("PRAGMA ")):
            self._deny("SQL_NOT_ALLOWLISTED")

        try:
            validate_read_only_sql(sql)
        except Exception:
            self._deny("SQL_NOT_READ_ONLY")

        # In LIVE-01: strictly preflight and baseline collision check queries allowed
        if self._active_stage == "LIVE-01":
            if upper.startswith("PRAGMA "):
                if not (
                    upper.startswith("PRAGMA TABLE_INFO(")
                    or upper.startswith("PRAGMA INDEX_LIST(")
                    or upper.startswith("PRAGMA INDEX_XINFO(")
                    or upper.startswith("PRAGMA FOREIGN_KEY_")
                    or upper == "PRAGMA FOREIGN_KEYS"
                ):
                    self._deny("READ_NOT_ALLOWLISTED")
                self._preflight_queries_count += 1
                return

            if norm_sql in _EXACT_PREFLIGHT_QUERIES:
                if norm_sql == "SELECT name FROM sqlite_master WHERE type='table' AND name = ?":
                    if len(args) != 1 or not isinstance(_extract_hrana_val(args[0]), str):
                        self._deny("READ_NOT_ALLOWLISTED")
                else:
                    if len(args) != 0:
                        self._deny("READ_NOT_ALLOWLISTED")
                self._preflight_queries_count += 1
                return

            # Collision check queries during LIVE-01
            read_spec = _SYNTHETIC_READ_TEMPLATES.get(norm_sql)
            if read_spec is not None:
                table, pk_col, count, allowed_stages = read_spec
                if "LIVE-01" in allowed_stages and len(args) == count:
                    raw_arg = str(_extract_hrana_val(args[0]))
                    entity = next((e for e in self._inventory if e.table == table and e.pk_value == raw_arg), None)
                    if entity is not None:
                        self._preflight_collision_checks.add(raw_arg)
                        self._preflight_queries_count += 1
                        return

            self._deny("READ_NOT_ALLOWLISTED")

        # Outside LIVE-01: preflight queries and unallowlisted table reads are strictly forbidden.
        # Only exact allowlisted synthetic read templates matching self._active_stage are permitted.
        if self._active_stage is None:
            self._deny("READ_NOT_ALLOWLISTED")

        read_spec = _SYNTHETIC_READ_TEMPLATES.get(norm_sql)
        if read_spec is None:
            self._deny("READ_NOT_ALLOWLISTED")

        table, pk_col, count, allowed_stages = read_spec
        if self._active_stage not in allowed_stages:
            self._deny("READ_NOT_ALLOWLISTED")

        if len(args) != count:
            self._deny("READ_NOT_ALLOWLISTED")

        raw_arg = str(_extract_hrana_val(args[0]))
        if self._active_stage == "LIVE-07" and table == "stories":
            if (
                raw_arg != f"synth_story_{self._seal.run_id}_01"
                and raw_arg != f"synth_story_nonexistent_{self._seal.run_id}"
            ):
                self._deny("READ_NOT_ALLOWLISTED")
        else:
            entity = next((e for e in self._inventory if e.table == table and e.pk_value == raw_arg), None)
            if entity is None:
                self._deny("READ_NOT_ALLOWLISTED")

        return

    def _is_session_statement_allowed(self, sql: str, args: list[Any]) -> bool:
        """Legacy compatibility check."""
        try:
            self._validate_session_statement(sql, args)
            return True
        except MockHranaDenied:
            return False

    def _validate(self, request: httpx.Request) -> str:
        _verify_exact_hermetic_transport(self._inner)
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
            if baton != self._pending_baton:
                self._deny("BATON_STATE")
            if parts in (
                [{"type": "execute", "stmt": {"sql": "COMMIT"}}, {"type": "close"}],
                [{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}],
            ):
                return "transaction_end"
            # Transaction step
            for part in parts:
                if type(part) is not dict or set(part) != {"type", "stmt"}:
                    self._deny("STATEMENT_SHAPE")
                if part["type"] != "execute":
                    self._deny("STATEMENT_TYPE")
                stmt = part["stmt"]
                sql = stmt.get("sql", "")
                args = stmt.get("args", [])
                if self._session_mode:
                    self._validate_session_statement(sql, args)
                else:
                    key = _intent_key(sql, args)
                    if key not in self._allow or key in self._spent:
                        self._deny("SQL_OR_ARGS_NOT_ALLOWED")
                    self._spent.add(key)
            return "transaction_step"

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

            if self._session_mode:
                self._validate_session_statement(stmt["sql"], args)
            else:
                key = _intent_key(stmt["sql"], args)
                if key not in self._allow or key in self._spent or key in keys:
                    self._deny("SQL_OR_ARGS_NOT_ALLOWED")
                keys.append(key)

        if not self._session_mode:
            self._spent.update(keys)  # consume BEFORE dispatch; never blind retry
        return "transaction_begin" if transaction else "pipeline"

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        now = self._clock()
        if now - self._started_at >= self._session_timeout_seconds:
            self._deny("DEADLINE")
        if self._requests + 1 >= self._max_requests:
            self._deny("REQUEST_CEILING")
        if not self._in_cleanup and self._requests >= self._test_cutoff_requests:
            self._deny("TEST_CUTOFF")
        if self.halted or self._session_closed:
            self._deny("SESSION_HALTED")

        action = ""
        expected_responses = 0
        try:
            self._seal.assert_unchanged()
            action = self._validate(request)
            data = json.loads(request.content.decode("utf-8"))
            expected_responses = len(data["requests"])
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
            if len(data["results"]) != expected_responses:
                self._deny("RESPONSE_ERROR")
            if any(
                type(item) is not dict or item.get("type") not in ("ok", "error")
                for item in data["results"]
            ):
                self._deny("RESPONSE_SHAPE")
            if not self._session_mode and any(item.get("type") != "ok" for item in data["results"]):
                self._deny("RESPONSE_ERROR")
            if action == "transaction_begin":
                baton = data.get("baton")
                if type(baton) is not str or not baton.strip():
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
        self.close_session()
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
            session_mode=False,
            max_requests=_MAX_MOCK_REQUESTS,
            test_cutoff_requests=_MAX_MOCK_REQUESTS,
            cleanup_reserve_requests=0,
            session_timeout_seconds=_SESSION_DEADLINE_SECONDS,
            verify_inner_hermetic=True,
        )
