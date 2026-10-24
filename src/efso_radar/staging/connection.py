"""Safe operator-controlled staging connection readiness inspection for Efso Radar.

Strictly non-mutating:
- Never initializes schema, tables, or indexes.
- Validates URL scheme, endpoint syntax, and credentials presence.
- Executes read-only ping (`SELECT 1 AS ping`) and measures round-trip latency.
- Inspects schema presence and version non-mutatingly from sqlite_master.
- Enforces read-only query safety at the client execution boundary.
- Sanitizes tokens and endpoints in reports and logs.
- Closes all network connections cleanly.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any, Optional
import httpx
from pydantic import BaseModel, Field

from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.turso_client import (
    TursoClientError,
    TursoHTTPClient,
    mask_turso_url,
    normalize_turso_url,
    validate_read_only_sql,
)
from efso_radar.staging.recovery_inspection import sanitize_diagnostic_text

logger = logging.getLogger("efso-radar.staging.connection")


class StagingConnectionReport(BaseModel):
    """Structured report of Turso staging connection readiness."""
    success: bool = False
    endpoint_masked: str = ""
    latency_ms: float = 0.0
    is_reachability_verified: bool = False
    is_schema_initialized: bool = False
    schema_version: int = 0
    table_count: int = 0
    tables_found: list[str] = Field(default_factory=list)
    error_message: Optional[str] = None
    warning_message: Optional[str] = None

    def summary(self) -> str:
        status_str = "PASSED" if self.success else "FAILED"
        schema_desc = (
            f"INITIALIZED (v{self.schema_version})"
            if self.is_schema_initialized
            else "UNINITIALIZED (0 tables — initialization required)"
        )
        lines = [
            f"=== TURSO STAGING CONNECTION REPORT [{status_str}] ===",
            f"Endpoint: {self.endpoint_masked}",
            f"Reachability: {'VERIFIED' if self.is_reachability_verified else 'FAILED'} ({self.latency_ms:.1f}ms)",
            f"Schema Status: {schema_desc}",
            f"Tables Present: {self.table_count}",
        ]
        if self.tables_found:
            lines.append(f"Tables: {', '.join(sorted(self.tables_found))}")
        if self.error_message:
            lines.append(f"Error: {self.error_message}")
        if self.warning_message:
            lines.append(f"Warning: {self.warning_message}")
        return "\n".join(lines)


def mask_token(token: str) -> str:
    """Safely mask token for display without revealing sensitive characters."""
    if not token or not token.strip():
        return "<EMPTY>"
    tok = token.strip()
    if len(tok) <= 8:
        return "[REDACTED]"
    return f"{tok[:4]}...[REDACTED]...{tok[-4:]}"


def get_staging_credentials_from_env() -> tuple[str, str]:
    """Read staging credentials directly from process os.environ without reading .env or application configs.

    Returns:
        (database_url, auth_token)

    Raises:
        ValueError: If credentials are empty, missing, or only partially provided.
    """
    raw_url = os.environ.get("TURSO_DATABASE_URL", "").strip()
    raw_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

    if not raw_url and not raw_token:
        raise ValueError(
            "Missing Turso staging credentials: both TURSO_DATABASE_URL and TURSO_AUTH_TOKEN "
            "must be set in process environment variables."
        )
    if not raw_url:
        raise ValueError("Missing TURSO_DATABASE_URL in process environment variables.")
    if not raw_token:
        raise ValueError("Missing TURSO_AUTH_TOKEN in process environment variables.")

    return raw_url, raw_token


def check_staging_connection(
    database_url: str,
    auth_token: str = "",
    timeout_seconds: float = 10.0,
    transport: Optional[httpx.BaseTransport] = None,
) -> StagingConnectionReport:
    """Perform safe, strictly non-mutating connection readiness check against a Turso libSQL endpoint.

    Guarantees:
    - Zero DDL, DML, or table creation operations are executed.
    - Uses strictly read-only queries validated by validate_read_only_sql.
    - Enforces read-only mode at the HTTP client execution boundary.
    - Masks database URL and redacts authentication token in the report.
    - Closes HTTP transport cleanly upon completion or exception.
    """
    masked_url = mask_turso_url(database_url)
    report = StagingConnectionReport(endpoint_masked=masked_url)

    # 1. URL and credential validation
    try:
        normalized_url = normalize_turso_url(database_url, allow_localhost_http=True)
        report.endpoint_masked = mask_turso_url(normalized_url)
    except Exception as exc:
        report.success = False
        report.error_message = sanitize_diagnostic_text(f"Invalid Turso endpoint URL: {exc}")
        return report

    if not auth_token or not auth_token.strip():
        # Require token for remote non-localhost hosts
        from urllib.parse import urlparse
        host = (urlparse(normalized_url).hostname or "").lower()
        if host not in ("localhost", "127.0.0.1", "::1") and transport is None:
            report.success = False
            report.error_message = "Authentication token is missing. Turso remote connections require TURSO_AUTH_TOKEN."
            return report

    # 2. Establish connection client in strict read-only mode
    client: Optional[TursoHTTPClient] = None
    try:
        client = TursoHTTPClient(
            database_url=database_url,
            auth_token=auth_token,
            timeout_seconds=timeout_seconds,
            transport=transport,
            read_only=True,
        )

        # 3. Read-only ping and latency measurement
        t0 = time.perf_counter()
        res_ping = client.execute("SELECT 1 AS ping")
        t1 = time.perf_counter()

        ping_row = res_ping.fetchone()
        if not ping_row or ping_row.get("ping") != 1:
            report.success = False
            report.error_message = "Remote endpoint responded but ping query did not return expected result."
            return report

        report.is_reachability_verified = True
        report.latency_ms = round((t1 - t0) * 1000.0, 2)

        # 4. Check schema presence non-mutatingly from sqlite_master
        res_tables = client.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        tables = [r["name"] for r in res_tables.fetchall() if r.get("name")]
        report.table_count = len(tables)
        report.tables_found = tables

        # 5. Check schema version if schema_migrations table exists
        if "schema_migrations" in tables:
            res_ver = client.execute("SELECT MAX(version) AS ver FROM schema_migrations")
            ver_row = res_ver.fetchone()
            report.schema_version = int(ver_row["ver"]) if ver_row and ver_row.get("ver") is not None else 0

        if report.schema_version >= CURRENT_SCHEMA_VERSION and len(tables) >= 15:
            report.is_schema_initialized = True
            report.success = True
        else:
            report.is_schema_initialized = False
            # Reachability confirmed, but schema initialization is required before preflight.
            report.success = True
            report.warning_message = (
                f"Database is reachable but schema v6 is not fully initialized "
                f"({len(tables)} tables found, schema version {report.schema_version}). "
                f"Run authorized schema initialization before executing preflight validation."
            )

    except Exception as exc:
        report.success = False
        report.error_message = sanitize_diagnostic_text(f"Connection readiness check failed: {exc}")
    finally:
        if client is not None:
            client.close()

    return report


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point for operator staging connection readiness inspection."""
    parser = argparse.ArgumentParser(
        description="Efso Radar — Turso Staging Safe Connection Readiness Check",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Execute safe, strictly read-only connectivity test against configured Turso staging endpoint",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration format and exit without establishing network connections",
    )
    args = parser.parse_args(argv)

    try:
        url, token = get_staging_credentials_from_env()
    except ValueError as exc:
        print(f"\nERROR: {exc}\n", file=sys.stderr)
        return 1

    masked_url = mask_turso_url(url)
    masked_tok = mask_token(token)

    print("\n" + "=" * 80)
    print("EFSO RADAR — TURSO STAGING CONNECTION READINESS")
    print("=" * 80)
    print(f"Turso Endpoint: {masked_url}")
    print(f"Auth Token: {masked_tok}")
    print("=" * 80)

    if args.dry_run:
        # Validate URL format strictly without network
        try:
            normalize_turso_url(url)
            print("\n[DRY-RUN COMPLETE] Configuration syntax validated. Zero network connections attempted.\n")
            return 0
        except Exception as exc:
            print(f"\n[DRY-RUN FAILED] Invalid URL syntax: {exc}\n", file=sys.stderr)
            return 1

    if not args.check:
        print(
            "\nSafety Note: To perform a read-only staging connection check, provide the --check flag.\n"
            "This ensures live endpoints are only probed when explicitly authorized by an operator.\n"
        )
        return 0

    print("\nProbing endpoint (read-only SELECT 1)...")
    report = check_staging_connection(
        database_url=url,
        auth_token=token,
    )
    print("\n" + report.summary() + "\n")
    return 0 if report.success else 1


if __name__ == "__main__":
    sys.exit(main())
