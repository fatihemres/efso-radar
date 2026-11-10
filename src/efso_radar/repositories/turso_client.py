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


class TursoAmbiguousTransactionError(TursoClientError):
    """Raised when a Turso multi-statement transaction commit outcome is ambiguous (UNKNOWN)."""
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


MAX_HRANA_RESULT_ROWS: int = 5000
MAX_HRANA_RESPONSE_BYTES: int = 5 * 1024 * 1024  # 5 MB


def deserialize_hrana_value(item: Any) -> Any:
    """Deserialize a Hrana/libSQL v2 value into a native Python object."""
    if not isinstance(item, dict):
        raise TursoClientError(f"Malformed Hrana cell: expected dict, got {type(item).__name__}.")

    val_type = item.get("type")
    if val_type == "null":
        return None
    elif val_type == "integer":
        raw = item.get("value")
        if raw is None:
            return None
        try:
            return int(raw)
        except Exception:
            raise TursoClientError("Invalid integer value in Hrana cell.") from None
    elif val_type == "float":
        raw = item.get("value")
        if raw is None:
            return 0.0
        try:
            return float(raw)
        except Exception:
            raise TursoClientError("Invalid float value in Hrana cell.") from None
    elif val_type == "text":
        raw = item.get("value")
        if not isinstance(raw, str):
            raise TursoClientError("Invalid text value in Hrana cell.")
        return raw
    elif val_type == "blob":
        raw_b64 = item.get("base64")
        if not isinstance(raw_b64, str):
            raise TursoClientError("Invalid blob value in Hrana cell.")
        try:
            return base64.b64decode(raw_b64)
        except Exception:
            raise TursoClientError("Invalid base64 payload in Hrana blob cell.") from None
    else:
        raise TursoClientError(f"Unrecognized or unsupported Hrana cell type: '{val_type}'.")


def _parse_hrana_exec_result(res: Any, max_rows: int = MAX_HRANA_RESULT_ROWS) -> TursoQueryResult:
    """Validate and decode a single Hrana execute statement result object."""
    if not isinstance(res, dict):
        raise TursoClientError(f"Malformed statement result: expected dict, got {type(res).__name__}.")

    res_type = res.get("type")
    if res_type == "error":
        err = res.get("error", {})
        valid_code, category = _sanitize_hrana_error(err.get("code") if isinstance(err, dict) else None)
        raise TursoClientError(f"Turso statement execution error ({valid_code}): {category}")

    if res_type != "ok":
        raise TursoClientError(f"Unexpected statement result type: '{res_type}'. Expected 'ok'.")

    response_data = res.get("response")
    if not isinstance(response_data, dict):
        raise TursoClientError(f"Malformed statement response: expected dict, got {type(response_data).__name__}.")

    resp_type = response_data.get("type")
    if resp_type != "execute":
        raise TursoClientError(f"Unexpected statement response type: '{resp_type}'. Expected 'execute'.")

    exec_result = response_data.get("result")
    if not isinstance(exec_result, dict):
        raise TursoClientError(f"Malformed execute result: expected dict, got {type(exec_result).__name__}.")

    raw_cols = exec_result.get("cols")
    if not isinstance(raw_cols, list):
        raise TursoClientError("Missing or invalid 'cols' list in execute result.")

    cols: list[str] = []
    for i, c in enumerate(raw_cols):
        if not isinstance(c, dict) or not isinstance(c.get("name"), str) or not c.get("name"):
            raise TursoClientError(f"Invalid column descriptor at index {i} in Hrana result.")
        cols.append(c["name"])

    raw_rows = exec_result.get("rows")
    if not isinstance(raw_rows, list):
        raise TursoClientError("Missing or invalid 'rows' list in execute result.")

    if len(raw_rows) > max_rows:
        raise TursoClientError(f"Hrana result row count {len(raw_rows)} exceeds maximum limit {max_rows}.")

    parsed_rows: list[dict[str, Any]] = []
    for row_idx, row in enumerate(raw_rows):
        if not isinstance(row, (list, tuple)):
            raise TursoClientError(f"Invalid row format at index {row_idx}: expected list, got {type(row).__name__}.")
        if len(row) != len(cols):
            raise TursoClientError(
                f"Row width mismatch at index {row_idx}: expected {len(cols)} cells, got {len(row)}."
            )
        row_dict = {}
        for col_name, cell in zip(cols, row):
            row_dict[col_name] = deserialize_hrana_value(cell)
        parsed_rows.append(row_dict)

    affected = exec_result.get("affected_row_count", 0)
    if not isinstance(affected, int) or affected < 0:
        affected = 0
    last_id = exec_result.get("last_insert_rowid")
    if last_id is not None and (not isinstance(last_id, int) or last_id < 0):
        last_id = None

    return TursoQueryResult(cols, parsed_rows, affected, last_id)


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
        self.database_url = database_url
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
        self,
        statements: list[tuple[str, list[Any]]],
        max_rows: int = MAX_HRANA_RESULT_ROWS,
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

        if len(resp.content) > MAX_HRANA_RESPONSE_BYTES:
            raise TursoClientError(
                f"Response payload size {len(resp.content)} bytes exceeds maximum allowed limit {MAX_HRANA_RESPONSE_BYTES}."
            )

        try:
            data = resp.json()
        except Exception as exc:
            err_type = type(exc).__name__
            raise TursoClientError(f"Turso response malformed: invalid JSON payload ({err_type}).") from None

        if not isinstance(data, dict):
            raise TursoClientError("Turso response malformed: response root is not an object.")

        raw_results = data.get("results")
        if not isinstance(raw_results, list):
            raise TursoClientError("Turso response malformed: missing or invalid results list.")

        expected_count = len(statements) + 1
        if len(raw_results) != expected_count:
            raise TursoClientError(
                f"Unexpected pipeline results count: expected {expected_count}, got {len(raw_results)}."
            )

        # Validate close response (last element)
        close_res = raw_results[-1]
        if not isinstance(close_res, dict):
            raise TursoClientError("Malformed pipeline close result: expected dict.")
        if close_res.get("type") == "error":
            err = close_res.get("error", {})
            valid_code, category = _sanitize_hrana_error(err.get("code") if isinstance(err, dict) else None)
            raise TursoClientError(f"Pipeline close error ({valid_code}): {category}")
        if close_res.get("type") != "ok":
            raise TursoClientError(f"Pipeline close failed with unexpected type: '{close_res.get('type')}'.")

        close_resp = close_res.get("response")
        if close_resp is not None:
            if not isinstance(close_resp, dict) or close_resp.get("type") != "close":
                got_type = close_resp.get("type") if isinstance(close_resp, dict) else type(close_resp).__name__
                raise TursoClientError(
                    f"Pipeline close response type mismatch: expected 'close', got '{got_type}'."
                )

        parsed_results: list[TursoQueryResult] = []
        for res in raw_results[:-1]:
            parsed_results.append(_parse_hrana_exec_result(res, max_rows=max_rows))

        return parsed_results

    def execute_transaction(
        self,
        statements: list[tuple[str, list[Any]]],
        max_rows: int = MAX_HRANA_RESULT_ROWS,
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

        try:
            data = resp.json()
        except Exception as exc:
            err_type = type(exc).__name__
            raise TursoClientError(f"Turso transaction response malformed: invalid JSON payload ({err_type}).") from None

        if not isinstance(data, dict):
            raise TursoClientError("Turso transaction response malformed: response root is not an object.")

        baton = data.get("baton")
        raw_results = data.get("results")

        if not isinstance(raw_results, list) or len(raw_results) == 0:
            raise TursoClientError("Turso transaction response malformed: missing result list.")

        # Validate BEGIN (index 0)
        begin_res = raw_results[0]
        if not isinstance(begin_res, dict):
            raise TursoClientError("Turso transaction response malformed: invalid BEGIN result format.")
        if begin_res.get("type") == "error":
            err = begin_res.get("error", {})
            valid_code, category = _sanitize_hrana_error(err.get("code") if isinstance(err, dict) else None)
            if baton:
                try:
                    client.post(
                        self.pipeline_url,
                        json={"baton": baton, "requests": [{"type": "close"}]},
                    )
                except Exception:
                    pass
            raise TursoClientError(f"Turso transaction BEGIN failed ({valid_code}): {category}")
        elif begin_res.get("type") != "ok":
            if baton:
                try:
                    client.post(
                        self.pipeline_url,
                        json={"baton": baton, "requests": [{"type": "close"}]},
                    )
                except Exception:
                    pass
            raise TursoClientError("Turso transaction response malformed: unexpected BEGIN response type.")

        # Check if result list is truncated
        if len(raw_results) < len(requests_payload):
            if baton:
                try:
                    client.post(
                        self.pipeline_url,
                        json={"baton": baton, "requests": [{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}]},
                    )
                except Exception:
                    pass
            raise TursoClientError("Turso transaction response malformed: truncated result list.")

        # Validate and parse all user statement results BEFORE issuing COMMIT
        parsed_results: list[TursoQueryResult] = []
        validation_error: Optional[TursoClientError] = None

        for idx, res in enumerate(raw_results[1:]):
            if not isinstance(res, dict):
                validation_error = TursoClientError(
                    "Turso transaction response malformed: statement result item is missing or invalid."
                )
                break
            res_type = res.get("type")
            if res_type == "error":
                err = res.get("error", {})
                valid_code, category = _sanitize_hrana_error(err.get("code") if isinstance(err, dict) else None)
                validation_error = TursoClientError(
                    f"Turso transaction aborted due to statement error ({valid_code}): {category}"
                )
                break
            elif res_type != "ok":
                validation_error = TursoClientError(
                    "Turso transaction aborted due to unexpected statement response type."
                )
                break

            try:
                parsed_results.append(_parse_hrana_exec_result(res, max_rows=max_rows))
            except TursoClientError as exc:
                validation_error = exc
                break

        if validation_error is not None:
            if baton:
                try:
                    client.post(
                        self.pipeline_url,
                        json={"baton": baton, "requests": [{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}]},
                    )
                except Exception as rollback_exc:
                    err_type = type(rollback_exc).__name__
                    logger.warning("Turso transaction rollback failed (%s).", err_type)
            raise validation_error

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
            raise TursoAmbiguousTransactionError(
                f"Ambiguous transaction outcome (UNKNOWN): network error during Turso COMMIT ({err_type}). "
                "The transaction may or may not have committed."
            ) from None

        if commit_resp.status_code != 200:
            if commit_resp.status_code in (500, 502, 503, 504):
                raise TursoAmbiguousTransactionError(
                    f"Ambiguous transaction outcome (UNKNOWN): Turso COMMIT returned HTTP status {commit_resp.status_code}. "
                    "The transaction may or may not have committed."
                )
            raise TursoAmbiguousTransactionError(
                f"Ambiguous transaction outcome (UNKNOWN): Turso COMMIT returned unexpected HTTP status {commit_resp.status_code}. "
                "The transaction may or may not have committed."
            )

        try:
            commit_data = commit_resp.json()
        except Exception as exc:
            err_type = type(exc).__name__
            raise TursoAmbiguousTransactionError(
                f"Ambiguous transaction outcome (UNKNOWN): malformed JSON in Turso COMMIT response ({err_type}). "
                "The transaction may or may not have committed."
            ) from None

        if not isinstance(commit_data, dict):
            raise TursoAmbiguousTransactionError(
                "Ambiguous transaction outcome (UNKNOWN): Turso COMMIT response is not an object. "
                "The transaction may or may not have committed."
            )

        commit_results = commit_data.get("results")
        if not isinstance(commit_results, list) or len(commit_results) == 0:
            raise TursoAmbiguousTransactionError(
                "Ambiguous transaction outcome (UNKNOWN): missing or truncated result list in Turso COMMIT response. "
                "The transaction may or may not have committed."
            )

        commit_first = commit_results[0]
        if not isinstance(commit_first, dict):
            raise TursoAmbiguousTransactionError(
                "Ambiguous transaction outcome (UNKNOWN): invalid result format in Turso COMMIT response. "
                "The transaction may or may not have committed."
            )

        commit_type = commit_first.get("type")
        if commit_type == "error":
            err = commit_first.get("error", {})
            valid_code, category = _sanitize_hrana_error(err.get("code") if isinstance(err, dict) else None)
            raise TursoClientError(f"Turso COMMIT failed ({valid_code}): {category}")
        elif commit_type != "ok":
            raise TursoAmbiguousTransactionError(
                "Ambiguous transaction outcome (UNKNOWN): unexpected result type in Turso COMMIT response. "
                "The transaction may or may not have committed."
            )

        # Validate complete expected successful COMMIT response structure
        commit_resp_data = commit_first.get("response")
        if (
            not isinstance(commit_resp_data, dict)
            or commit_resp_data.get("type") != "execute"
            or not isinstance(commit_resp_data.get("result"), dict)
        ):
            raise TursoAmbiguousTransactionError(
                "Ambiguous transaction outcome (UNKNOWN): missing or malformed execution result in Turso COMMIT response. "
                "The transaction may or may not have committed."
            )

        # Return pre-parsed statements validated prior to COMMIT
        return parsed_results

    def execute(
        self,
        sql: str,
        args: Optional[list[Any]] = None,
        max_rows: int = MAX_HRANA_RESULT_ROWS,
    ) -> TursoQueryResult:
        """Execute a single SQL statement."""
        if self.read_only:
            validate_read_only_sql(sql)
        results = self.execute_pipeline([(sql, args or [])], max_rows=max_rows)
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
