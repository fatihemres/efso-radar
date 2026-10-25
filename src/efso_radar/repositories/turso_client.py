"""Official Turso (libSQL) HTTP Hrana v2 pipeline client.

Implements parameter serialization, URL normalization, batched execution,
read-only query safety, and type mapping over Turso's /v2/pipeline endpoint.
Requires zero C-extensions or external platform-dependent binaries.
"""
from __future__ import annotations

import base64
import logging
import re
from typing import Any, Optional
from urllib.parse import urlparse
import httpx

logger = logging.getLogger("efso-radar.repository.turso_client")

ALLOWED_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "[::1]"})


class TursoClientError(Exception):
    """Raised when Turso libSQL HTTP API returns an error or fails."""
    pass


def _format_http_status_error(status_code: int) -> str:
    """Format safe HTTP status error without untrusted response body content."""
    categories = {
        400: "Bad Request",
        401: "Unauthorized / Authentication Failed",
        403: "Forbidden / Insufficient Permissions",
        404: "Endpoint Not Found",
        408: "Request Timeout",
        429: "Rate Limit Exceeded",
        500: "Internal Server Error",
        502: "Bad Gateway",
        503: "Service Unavailable",
        504: "Gateway Timeout",
    }
    category = categories.get(status_code, "HTTP Error")
    return f"Turso API returned HTTP status {status_code} ({category})."


SAFE_HRANA_ERROR_CATEGORIES: dict[str, str] = {
    "SQLITE_ERROR": "Generic SQL execution failure",
    "SQLITE_BUSY": "Database engine is busy or locked",
    "SQLITE_LOCKED": "Database table is locked",
    "SQLITE_NOMEM": "A malloc has failed",
    "SQLITE_READONLY": "Attempt to write a readonly database",
    "SQLITE_IOERR": "Disk I/O error occurred",
    "SQLITE_CORRUPT": "Database disk image is malformed",
    "SQLITE_NOTFOUND": "Unknown opcode or table not found",
    "SQLITE_FULL": "Insertion failed because database is full",
    "SQLITE_CANTOPEN": "Unable to open the database file",
    "SQLITE_PROTOCOL": "Database lock protocol error",
    "SQLITE_SCHEMA": "Database schema has changed",
    "SQLITE_TOOBIG": "String or BLOB exceeds size limit",
    "SQLITE_CONSTRAINT": "Abort due to constraint violation",
    "SQLITE_MISMATCH": "Data type mismatch",
    "SQLITE_MISUSE": "Library used incorrectly",
    "SQLITE_AUTH": "Authorization denied",
    "SQLITE_RANGE": "Column index out of range",
    "SQLITE_NOTADB": "File is not a database",
    "HRANA_ERROR": "Hrana protocol execution error",
}


def _sanitize_hrana_error(raw_code: Any) -> tuple[str, str]:
    """Validate remote error code and return a safe (code, category) pair without untrusted messages."""
    if isinstance(raw_code, str):
        cleaned_code = raw_code.strip().upper()
        if re.fullmatch(r"^[A-Z0-9_]{1,32}$", cleaned_code):
            category = SAFE_HRANA_ERROR_CATEGORIES.get(cleaned_code, "Remote database statement execution failed")
            return cleaned_code, category
    return "UNKNOWN", "Remote database statement execution failed"


def mask_turso_url(url: str) -> str:
    """Return a safely sanitized database URL for logging (no credentials, tokens, arbitrary paths, or query params).

    Guarantees:
    - Never preserves secret-bearing arbitrary URL paths, query parameters, or userinfo.
    - Discards malformed raw input and returns a safe fallback placeholder.
    - Canonicalizes valid endpoints to scheme://hostname[:port]/v2/pipeline.
    """
    if not url or not isinstance(url, str) or not url.strip():
        return "<EMPTY>"
    raw = url.strip()
    try:
        # Strip userinfo before parsing
        if "@" in raw:
            scheme_part, rest = raw.split("://", 1) if "://" in raw else ("https", raw)
            rest = rest.split("@", 1)[1]
            raw = f"{scheme_part}://{rest}"

        if "://" in raw:
            scheme, remainder = raw.split("://", 1)
            scheme = scheme.lower()
        else:
            scheme = "https"
            remainder = raw

        effective_scheme = "https" if scheme in ("libsql", "https") else ("http" if scheme == "http" else None)
        if effective_scheme is None:
            return "<unsupported_scheme_url>"

        parsed = urlparse(f"{effective_scheme}://{remainder}")
        host = (parsed.hostname or "").lower()
        if not host:
            return "<invalid_host_url>"

        port_str = f":{parsed.port}" if parsed.port and parsed.port not in (80, 443) else ""
        formatted_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
        return f"{effective_scheme}://{formatted_host}{port_str}/v2/pipeline"
    except Exception:
        return "<malformed_url>"


def normalize_turso_url(raw_url: str, allow_localhost_http: bool = True) -> str:
    """Normalize and strictly validate Turso database URL to the /v2/pipeline endpoint.

    Guarantees:
    - Rejects empty, whitespace, or malformed URLs.
    - Strictly permits only 'libsql://', 'https://', and 'http://' (for exact loopback hosts only).
    - Prohibits insecure remote 'http://' endpoints, regardless of parameters, substrings, or paths.
    - Prohibits embedded username/password credentials in the URL.
    - Normalizes 'libsql://' to 'https://' and appends '/v2/pipeline'.
    """
    if not raw_url or not isinstance(raw_url, str) or not raw_url.strip():
        raise ValueError("Turso database URL cannot be empty.")

    url = raw_url.strip()

    # Reject embedded userinfo/passwords in URL to prevent credential leakage in logs
    if "@" in url:
        raise ValueError(
            "Turso database URL must not contain embedded username or password credentials. "
            "Supply authentication credentials exclusively via TURSO_AUTH_TOKEN."
        )

    if "://" in url:
        scheme, remainder = url.split("://", 1)
        scheme = scheme.lower()
    else:
        scheme = "https"
        remainder = url

    allowed_schemes = {"libsql", "https", "http"}
    if scheme not in allowed_schemes:
        raise ValueError(
            f"Unsupported Turso URL scheme: '{scheme}'. "
            f"Only 'libsql://' and 'https://' (or 'http://' for localhost) are supported."
        )

    effective_scheme = "https" if scheme == "libsql" else scheme
    parsed = urlparse(f"{effective_scheme}://{remainder}")

    if parsed.username or parsed.password:
        raise ValueError(
            "Turso database URL must not contain embedded username or password credentials. "
            "Supply authentication credentials exclusively via TURSO_AUTH_TOKEN."
        )

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise ValueError(f"Invalid Turso database URL: missing hostname in '{mask_turso_url(url)}'.")

    if effective_scheme == "http":
        if hostname not in ALLOWED_LOOPBACK_HOSTS:
            raise ValueError(
                f"Insecure unencrypted HTTP connection to remote Turso host '{hostname}' is prohibited. "
                f"Use 'https://' or 'libsql://'."
            )
        if not allow_localhost_http:
            raise ValueError("Plaintext HTTP connection to localhost is disabled.")

    port_str = f":{parsed.port}" if parsed.port and parsed.port not in (80, 443) else ""
    formatted_host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    return f"{effective_scheme}://{formatted_host}{port_str}/v2/pipeline"


def validate_read_only_sql(sql: str) -> None:
    """Strictly validate that an outbound SQL statement is read-only (SELECT, WITH, or allowed PRAGMA).

    Raises ValueError if any DDL, DML, CTE writes, or mutating PRAGMAs are detected.
    """
    if not sql or not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL query cannot be empty.")

    cleaned = sql.strip()

    # Reject SQL comments to prevent token-splitting evasion
    if "--" in cleaned or "/*" in cleaned or "*/" in cleaned:
        raise ValueError("Preflight query safety violation: SQL comments are prohibited in read-only queries.")

    # Reject multiple statements
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    if ";" in cleaned:
        raise ValueError("Preflight query safety violation: multiple statements are prohibited.")

    # Strip string literals to avoid false positives for keywords inside strings
    no_strings = re.sub(r"'(''|[^'])*'", "'STRING'", cleaned)
    no_strings = re.sub(r'"(""|[^"])*"', '"STRING"', no_strings)
    upper = no_strings.upper()

    # Check allowed starting verbs
    if upper.startswith("PRAGMA"):
        arg_pragmas = {"table_info", "table_xinfo", "index_list", "index_info", "index_xinfo", "foreign_key_list"}
        bare_pragmas = {"collation_list", "database_list", "compile_options", "user_version"}
        m_arg = re.fullmatch(r"PRAGMA\s+([A-Z_]+)\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)", upper, flags=re.IGNORECASE)
        m_bare = re.fullmatch(r"PRAGMA\s+([A-Z_]+)", upper, flags=re.IGNORECASE)
        if m_arg and m_arg.group(1).lower() in arg_pragmas:
            return
        if m_bare and m_bare.group(1).lower() in bare_pragmas:
            return
        raise ValueError(
            f"Preflight query safety violation: PRAGMA is not an allowed read-only form: {sql[:40]}"
        )

    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise ValueError(
            f"Preflight query safety violation: query must start with SELECT, WITH, or allowed PRAGMA: {sql[:40]}"
        )

    # Strictly prohibit mutating verbs anywhere in the statement
    prohibited_verbs = {
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "REPLACE",
        "ATTACH", "DETACH", "VACUUM", "REINDEX", "UPSERT", "SAVEPOINT",
        "RELEASE", "GRANT", "REVOKE", "PRAGMA", "LOAD_EXTENSION", "BEGIN", "COMMIT",
        "ROLLBACK", "ANALYZE",
    }
    words = set(re.findall(r"\b[A-Za-z_]+\b", upper))
    found_prohibited = words.intersection(prohibited_verbs)
    if found_prohibited:
        raise ValueError(
            f"Preflight query safety violation: query contains prohibited mutating keywords "
            f"{sorted(found_prohibited)}: {sql[:50]}"
        )


def serialize_hrana_arg(val: Any) -> dict[str, Any]:
    """Serialize a Python value into a Hrana/libSQL v2 typed parameter value."""
    if val is None:
        return {"type": "null"}
    elif isinstance(val, bool):
        return {"type": "integer", "value": "1" if val else "0"}
    elif isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    elif isinstance(val, float):
        return {"type": "float", "value": val}
    elif isinstance(val, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(val).decode("ascii")}
    else:
        return {"type": "text", "value": str(val)}


def deserialize_hrana_value(item: dict[str, Any]) -> Any:
    """Deserialize a Hrana/libSQL v2 value into a native Python object."""
    val_type = item.get("type", "null")
    if val_type == "null":
        return None
    elif val_type == "integer":
        raw = item.get("value")
        return int(raw) if raw is not None else None
    elif val_type == "float":
        return float(item.get("value", 0.0))
    elif val_type == "text":
        return item.get("value", "")
    elif val_type == "blob":
        raw_b64 = item.get("base64", "")
        return base64.b64decode(raw_b64)
    return item.get("value")


class TursoQueryResult:
    """Result of an individual statement execution."""

    def __init__(
        self,
        cols: list[str],
        rows: list[dict[str, Any]],
        affected_row_count: int = 0,
        last_insert_rowid: Optional[int] = None,
    ):
        self.cols = cols
        self.rows = rows
        self.affected_row_count = affected_row_count
        self.last_insert_rowid = last_insert_rowid

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows

    def fetchone(self) -> Optional[dict[str, Any]]:
        return self.rows[0] if self.rows else None


class TursoHTTPClient:
    """Client for Turso / libSQL over HTTP /v2/pipeline."""

    def __init__(
        self,
        database_url: str,
        auth_token: str = "",
        timeout_seconds: float = 15.0,
        transport: Optional[httpx.BaseTransport] = None,
        read_only: bool = False,
    ):
        self.pipeline_url = normalize_turso_url(database_url, allow_localhost_http=True)
        self.masked_url = mask_turso_url(self.pipeline_url)
        self.auth_token = auth_token.strip() if auth_token else ""
        if "\n" in self.auth_token or "\r" in self.auth_token:
            raise ValueError("Turso auth token contains invalid newline characters.")
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self.read_only = bool(read_only)
        self._client: Optional[httpx.Client] = None

    def __repr__(self) -> str:
        ro_flag = ", read_only=True" if self.read_only else ""
        return f"TursoHTTPClient(pipeline_url='{self.masked_url}'{ro_flag})"

    def __enter__(self) -> TursoHTTPClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _get_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            headers = {
                "Content-Type": "application/json",
            }
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
            self._client = httpx.Client(
                headers=headers,
                timeout=self.timeout_seconds,
                transport=self.transport,
            )
        return self._client

    def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            self._client.close()

    def ping(self) -> bool:
        """Execute non-mutating SELECT 1 to verify remote connectivity."""
        res = self.execute("SELECT 1 AS ping")
        row = res.fetchone()
        return bool(row and row.get("ping") == 1)

    def execute_pipeline(
        self, statements: list[tuple[str, list[Any]]]
    ) -> list[TursoQueryResult]:
        """Execute multiple SQL statements with parameters in a single HTTP pipeline request."""
        if not statements:
            return []

        if self.read_only:
            for sql, _ in statements:
                validate_read_only_sql(sql)

        requests_payload: list[dict[str, Any]] = []
        for sql, args in statements:
            stmt_obj: dict[str, Any] = {"sql": sql.strip()}
            if args:
                stmt_obj["args"] = [serialize_hrana_arg(a) for a in args]
            requests_payload.append({"type": "execute", "stmt": stmt_obj})

        # Add a close operation to signal pipeline completion
        requests_payload.append({"type": "close"})

        client = self._get_client()
        try:
            resp = client.post(self.pipeline_url, json={"requests": requests_payload})
        except httpx.HTTPError as exc:
            err_type = type(exc).__name__
            if isinstance(exc, httpx.TimeoutException):
                raise TursoClientError(f"HTTP connection to Turso timed out ({err_type}).") from None
            raise TursoClientError(f"HTTP connection to Turso failed ({err_type}).") from None

        if resp.status_code != 200:
            raise TursoClientError(_format_http_status_error(resp.status_code))

        data = resp.json()
        raw_results = data.get("results", [])
        parsed_results: list[TursoQueryResult] = []

        for idx, res in enumerate(raw_results[:-1]):  # Exclude close response
            res_type = res.get("type")
            if res_type == "error":
                err = res.get("error", {})
                valid_code, category = _sanitize_hrana_error(err.get("code"))
                raise TursoClientError(f"Turso statement {idx} error ({valid_code}): {category}")

            response_data = res.get("response", {})
            exec_result = response_data.get("result", {})

            cols = [c.get("name", f"col_{i}") for i, c in enumerate(exec_result.get("cols", []))]
            rows_data = exec_result.get("rows", [])
            parsed_rows: list[dict[str, Any]] = []

            for row in rows_data:
                row_dict = {}
                for col_name, cell in zip(cols, row):
                    row_dict[col_name] = deserialize_hrana_value(cell)
                parsed_rows.append(row_dict)

            affected = exec_result.get("affected_row_count", 0)
            last_id = exec_result.get("last_insert_rowid")
            parsed_results.append(TursoQueryResult(cols, parsed_rows, affected, last_id))

        return parsed_results

    def execute_transaction(
        self, statements: list[tuple[str, list[Any]]]
    ) -> list[TursoQueryResult]:
        """Execute multiple SQL statements atomically in an explicit transaction over /v2/pipeline."""
        if not statements:
            return []

        if self.read_only:
            raise PermissionError("Transactions are prohibited on a read-only client.")

        requests_payload: list[dict[str, Any]] = [
            {"type": "execute", "stmt": {"sql": "BEGIN"}}
        ]
        for sql, args in statements:
            stmt_obj: dict[str, Any] = {"sql": sql.strip()}
            if args:
                stmt_obj["args"] = [serialize_hrana_arg(a) for a in args]
            requests_payload.append({"type": "execute", "stmt": stmt_obj})

        client = self._get_client()
        try:
            resp = client.post(self.pipeline_url, json={"baton": None, "requests": requests_payload})
        except httpx.HTTPError as exc:
            err_type = type(exc).__name__
            if isinstance(exc, httpx.TimeoutException):
                raise TursoClientError(f"HTTP connection to Turso timed out during transaction ({err_type}).") from None
            raise TursoClientError(f"HTTP connection to Turso failed during transaction ({err_type}).") from None

        if resp.status_code != 200:
            raise TursoClientError(_format_http_status_error(resp.status_code))

        data = resp.json()
        baton = data.get("baton")
        raw_results = data.get("results", [])

        if not raw_results:
            raise TursoClientError("Turso transaction response malformed: empty results received.")

        # Check for error on BEGIN (index 0)
        begin_res = raw_results[0]
        if begin_res.get("type") == "error":
            err = begin_res.get("error", {})
            valid_code, category = _sanitize_hrana_error(err.get("code"))
            if baton:
                try:
                    client.post(
                        self.pipeline_url,
                        json={"baton": baton, "requests": [{"type": "close"}]},
                    )
                except Exception:
                    pass
            raise TursoClientError(f"Turso transaction BEGIN failed ({valid_code}): {category}")

        # Check for errors in user statements (index 1 onwards)
        has_error = False
        error_code = None
        for idx, res in enumerate(raw_results[1:]):
            if res.get("type") == "error":
                has_error = True
                err = res.get("error", {})
                error_code = err.get("code")
                break

        if has_error:
            if baton:
                try:
                    client.post(
                        self.pipeline_url,
                        json={"baton": baton, "requests": [{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}]},
                    )
                except Exception as rollback_exc:
                    err_type = type(rollback_exc).__name__
                    logger.warning("Turso transaction rollback failed (%s).", err_type)
            valid_code, category = _sanitize_hrana_error(error_code)
            raise TursoClientError(f"Turso transaction aborted due to statement error ({valid_code}): {category}")

        # Enforce that session baton exists for multi-statement transaction
        if not baton:
            raise TursoClientError("Turso transaction protocol error: server did not return a session baton for transaction.")

        # Explicitly commit if all statements succeeded
        try:
            commit_resp = client.post(
                self.pipeline_url,
                json={"baton": baton, "requests": [{"type": "execute", "stmt": {"sql": "COMMIT"}}, {"type": "close"}]},
            )
        except httpx.HTTPError as exc:
            err_type = type(exc).__name__
            raise TursoClientError(
                f"Ambiguous transaction outcome: network error during Turso COMMIT ({err_type}). "
                "The transaction may or may not have committed."
            ) from None

        if commit_resp.status_code != 200:
            if commit_resp.status_code in (500, 502, 503, 504):
                raise TursoClientError(
                    f"Ambiguous transaction outcome: Turso COMMIT returned HTTP status {commit_resp.status_code}. "
                    "The transaction may or may not have committed."
                )
            raise TursoClientError(_format_http_status_error(commit_resp.status_code))

        commit_data = commit_resp.json()
        commit_results = commit_data.get("results", [])
        if not commit_results:
            raise TursoClientError("Turso COMMIT response malformed: missing execution result.")

        if commit_results[0].get("type") == "error":
            err = commit_results[0].get("error", {})
            valid_code, category = _sanitize_hrana_error(err.get("code"))
            raise TursoClientError(f"Turso COMMIT failed ({valid_code}): {category}")

        # Parse results for user statements (skipping BEGIN at index 0)
        parsed_results: list[TursoQueryResult] = []
        for res in raw_results[1:]:
            response_data = res.get("response", {})
            exec_result = response_data.get("result", {})
            cols = [c.get("name", f"col_{i}") for i, c in enumerate(exec_result.get("cols", []))]
            rows_data = exec_result.get("rows", [])
            parsed_rows: list[dict[str, Any]] = []
            for row in rows_data:
                row_dict = {}
                for col_name, cell in zip(cols, row):
                    row_dict[col_name] = deserialize_hrana_value(cell)
                parsed_rows.append(row_dict)
            affected = exec_result.get("affected_row_count", 0)
            last_id = exec_result.get("last_insert_rowid")
            parsed_results.append(TursoQueryResult(cols, parsed_rows, affected, last_id))

        return parsed_results

    def execute(self, sql: str, args: Optional[list[Any]] = None) -> TursoQueryResult:
        """Execute a single SQL statement."""
        if self.read_only:
            validate_read_only_sql(sql)
        results = self.execute_pipeline([(sql, args or [])])
        return results[0]

    def executescript(self, script: str) -> None:
        """Execute a multi-statement DDL script."""
        if self.read_only:
            raise PermissionError("Multi-statement script execution is prohibited on a read-only client.")
        statements = [s.strip() for s in script.split(";") if s.strip()]
        if not statements:
            return
        pipeline_items = [(s, []) for s in statements]
        self.execute_pipeline(pipeline_items)
