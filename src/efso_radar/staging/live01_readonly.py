"""PO-gated LIVE-01: remote Turso schema/collision inspection, strictly read-only.

This module never runs as part of CI or ordinary application startup.
No token is stored in env, .env, logs, GitHub, or on disk.
The established offline staging runner remains hermetic-only.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import getpass
import json
import secrets
import sys
import time
from typing import Callable, Optional

import httpx

from efso_radar.repositories.turso import TursoReadOnlyStateRepository
from efso_radar.repositories.turso_client import TursoHTTPClient, validate_read_only_sql
from efso_radar.staging.preflight import CloudPreflightValidator
from efso_radar.staging.runner import (
    EXACT_STAGING_DESTINATION,
    create_default_manifest_entities,
    validate_staging_destination,
)


MAX_READ_REQUESTS = 60
MAX_SESSION_SECONDS = 120.0
PER_REQUEST_TIMEOUT_SECONDS = 8.0
EXACT_PIPELINE = validate_staging_destination(EXACT_STAGING_DESTINATION) + "/v2/pipeline"


class Live01ReadOnlyTransport(httpx.BaseTransport):
    """Bounded HTTP transport; each request must contain one verified SELECT/PRAGMA.

    No retries. Once the deadline is reached, no new network requests are dispatched.
    This does not forcibly interrupt a request already in flight.
    """

    def __init__(
        self,
        inner: httpx.BaseTransport,
        clock: Callable[[], float] = time.monotonic,
        max_requests: int = MAX_READ_REQUESTS,
        timeout_seconds: float = MAX_SESSION_SECONDS,
    ):
        if not isinstance(max_requests, int) or isinstance(max_requests, bool) or not 0 < max_requests <= MAX_READ_REQUESTS:
            raise ValueError("Invalid LIVE-01 request limit.")
        if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or not 0 < timeout_seconds <= MAX_SESSION_SECONDS:
            raise ValueError("Invalid LIVE-01 session timeout.")
        self.inner = inner
        self.clock = clock
        self.started_at = clock()
        self.max_requests = max_requests
        self.timeout_seconds = timeout_seconds
        self.sent = 0

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "POST" or str(request.url) != EXACT_PIPELINE:
            raise PermissionError("LIVE-01 destination/method blocked before dispatch.")
        if self.clock() - self.started_at >= self.timeout_seconds:
            raise PermissionError("LIVE-01 deadline exceeded; no further requests.")
        if self.sent >= self.max_requests:
            raise PermissionError("LIVE-01 request limit exceeded; no further requests.")
        try:
            payload = json.loads(request.content)
            requests = payload["requests"]
            if (
                len(requests) != 2
                or requests[0]["type"] != "execute"
                or requests[1] != {"type": "close"}
                or not isinstance(requests[0]["stmt"].get("sql"), str)
            ):
                raise ValueError("Unexpected Hrana request shape.")
            validate_read_only_sql(requests[0]["stmt"]["sql"])
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            raise PermissionError("LIVE-01 non-read-only or malformed Hrana request blocked.") from None
        self.sent += 1
        return self.inner.handle_request(request)

    def close(self) -> None:
        self.inner.close()


@dataclass(frozen=True)
class Live01Report:
    passed: bool
    stage: str
    request_count: int
    schema_version: int
    tables_checked: int
    columns_checked: int
    collision_checks: int
    failure_category: str = ""

    def summary(self) -> str:
        result = "PASS" if self.passed else "STOP"
        # Never include token, synthetic keys, database contents or raw exception text.
        return (
            f"LIVE-01 {result} | stage={self.stage} | requests={self.request_count}/60 "
            f"| schema_v={self.schema_version} | tables_checked={self.tables_checked} "
            f"| columns_checked={self.columns_checked} | collision_checks={self.collision_checks} "
            f"| category={self.failure_category or 'NONE'}"
        )


def execute_live01_readonly(
    auth_token: str,
    *,
    target: str = EXACT_STAGING_DESTINATION,
    inner_transport: Optional[httpx.BaseTransport] = None,
    clock: Callable[[], float] = time.monotonic,
) -> Live01Report:
    """Authorized LIVE-01 only. No writes, manifest, mutation, or remote cleanup.

    Caller must supply a short-lived, database-scoped *read-only* Turso token.
    The transport is injectable only for hermetic tests, not via the CLI.
    """
    endpoint = validate_staging_destination(target)
    if not isinstance(auth_token, str) or not auth_token.strip():
        raise ValueError("A database-scoped read-only Turso token is required.")

    transport = Live01ReadOnlyTransport(
        inner_transport if inner_transport is not None else httpx.HTTPTransport(retries=0),
        clock=clock,
    )
    client = TursoHTTPClient(
        database_url=endpoint,
        auth_token=auth_token,
        read_only=True,
        timeout_seconds=PER_REQUEST_TIMEOUT_SECONDS,
        transport=transport,
    )
    stage = "PREFLIGHT"
    schema_version = tables_checked = columns_checked = collision_checks = 0
    failure = ""
    try:
        repository = TursoReadOnlyStateRepository(database_url=endpoint, client=client)
        preflight = CloudPreflightValidator(repo=repository).validate()
        schema_version = preflight.schema_version
        tables_checked = preflight.tables_checked
        columns_checked = preflight.columns_checked
        if (
            not preflight.success
            or not preflight.schema_fingerprint_before
            or preflight.schema_fingerprint_before != preflight.schema_fingerprint_after
            or preflight.tables_checked != 15
            or preflight.columns_checked < 129
        ):
            failure = "SCHEMA_OR_INTEGRITY_GATE"
        else:
            stage = "COLLISION_CHECK"
            run_id = (
                "run_"
                + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                + "_"
                + secrets.token_hex(6)
            )
            # Exactly the planned five synthetic keys. Never persist or log them.
            entities = create_default_manifest_entities(run_id)
            for entity in entities:
                # Identifiers are from sealed built-in allowlists, never user input.
                rows = client.execute(
                    f"SELECT 1 AS found FROM {entity.table} WHERE {entity.pk_column} = ? LIMIT 1",
                    [entity.pk_value],
                ).fetchall()
                collision_checks += 1
                if rows:
                    failure = "SYNTHETIC_KEY_COLLISION"
                    break
    except Exception as exc:
        # No raw exception values: HTTP clients/validation errors may contain sensitive data.
        failure = "READ_ERROR_" + type(exc).__name__
    finally:
        client.close()

    return Live01Report(
        passed=not failure and collision_checks == 5,
        stage=stage,
        request_count=transport.sent,
        schema_version=schema_version,
        tables_checked=tables_checked,
        columns_checked=columns_checked,
        collision_checks=collision_checks,
        failure_category=failure,
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Strict read-only Turso LIVE-01 preflight, requires explicit PO approval.")
    parser.add_argument(
        "--authorized-readonly-live01",
        action="store_true",
        help="Explicitly invoke the PO-authorized read-only LIVE-01 remote inspection.",
    )
    args = parser.parse_args(argv)
    if not args.authorized_readonly_live01:
        print("STOP: No network call. Pass --authorized-readonly-live01 only with explicit PO authorization.")
        return 2
    if not sys.stdin.isatty():
        print("STOP: interactive terminal required (no piped secrets).", file=sys.stderr)
        return 2
    try:
        token = getpass.getpass("Paste short-lived READ-ONLY staging token (hidden): ")
        report = execute_live01_readonly(token)
        print(report.summary())
        return 0 if report.passed else 1
    except Exception as exc:
        print(f"LIVE-01 STOP | category=PRECHECK_{type(exc).__name__} (no raw details)", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
