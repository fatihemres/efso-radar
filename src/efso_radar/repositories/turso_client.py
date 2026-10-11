"""Official Turso (libSQL) HTTP Hrana v2 pipeline client.

Implements parameter serialization, URL normalization, batched execution,
and type mapping over Turso's /v2/pipeline endpoint.
Requires zero C-extensions or external platform-dependent binaries.
"""
from __future__ import annotations

import base64
import logging
from typing import Any, Optional
import httpx

logger = logging.getLogger("efso-radar.repository.turso_client")


class TursoClientError(Exception):
    """Raised when Turso libSQL HTTP API returns an error or fails."""
    pass


def normalize_turso_url(raw_url: str) -> str:
    """Normalize libsql:// or https:// database URL to the /v2/pipeline endpoint."""
    url = raw_url.strip()
    if url.startswith("libsql://"):
        url = "https://" + url[len("libsql://"):]
    elif not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url

    url = url.rstrip("/")
    if not url.endswith("/v2/pipeline"):
        url += "/v2/pipeline"
    return url


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
    ):
        self.pipeline_url = normalize_turso_url(database_url)
        self.auth_token = auth_token
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self._client: Optional[httpx.Client] = None

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

    def execute_pipeline(
        self, statements: list[tuple[str, list[Any]]]
    ) -> list[TursoQueryResult]:
        """
        Execute multiple SQL statements with parameters in a single HTTP pipeline request.
        Statements are tuples of (sql, args).
        """
        if not statements:
            return []

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
            raise TursoClientError(f"HTTP connection to Turso failed: {exc}") from exc

        if resp.status_code != 200:
            raise TursoClientError(
                f"Turso API returned HTTP status {resp.status_code}: {resp.text[:300]}"
            )

        data = resp.json()
        raw_results = data.get("results", [])
        parsed_results: list[TursoQueryResult] = []

        for idx, res in enumerate(raw_results[:-1]):  # Exclude close response
            res_type = res.get("type")
            if res_type == "error":
                err = res.get("error", {})
                msg = err.get("message", "Unknown SQL error")
                code = err.get("code", "UNKNOWN")
                raise TursoClientError(f"Turso statement {idx} error ({code}): {msg}")

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
        """
        Execute multiple SQL statements atomically in an explicit transaction over /v2/pipeline.
        Uses Hrana session baton to guarantee all statements succeed before committing,
        or explicitly rolling back if any statement fails.
        """
        if not statements:
            return []

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
            raise TursoClientError(f"HTTP connection to Turso failed during transaction execution: {exc}") from exc

        if resp.status_code != 200:
            raise TursoClientError(
                f"Turso API returned HTTP status {resp.status_code}: {resp.text[:300]}"
            )

        data = resp.json()
        baton = data.get("baton")
        raw_results = data.get("results", [])

        # Check if any statement produced an error
        failed_idx: Optional[int] = None
        error_msg = ""
        error_code = "UNKNOWN"

        for idx, res in enumerate(raw_results):
            if res.get("type") == "error":
                failed_idx = idx - 1  # 0 is BEGIN
                err = res.get("error", {})
                error_msg = err.get("message", "Unknown SQL error")
                error_code = err.get("code", "UNKNOWN")
                break

        if failed_idx is not None:
            # Transaction failed: explicitly rollback and close session
            if baton:
                try:
                    client.post(
                        self.pipeline_url,
                        json={
                            "baton": baton,
                            "requests": [
                                {"type": "execute", "stmt": {"sql": "ROLLBACK"}},
                                {"type": "close"},
                            ],
                        },
                    )
                except Exception as rollback_err:
                    logger.warning("Failed sending explicit rollback to Turso session: %s", rollback_err)
            raise TursoClientError(
                f"Turso transaction aborted due to error in statement {failed_idx} ({error_code}): {error_msg}"
            )

        # All statements succeeded: commit and close session
        if baton:
            try:
                commit_resp = client.post(
                    self.pipeline_url,
                    json={
                        "baton": baton,
                        "requests": [
                            {"type": "execute", "stmt": {"sql": "COMMIT"}},
                            {"type": "close"},
                        ],
                    },
                )
            except httpx.HTTPError as exc:
                raise TursoClientError(
                    f"Ambiguous transaction outcome: network error during Turso COMMIT ({exc}). "
                    "The transaction may or may not have committed."
                ) from exc

            if commit_resp.status_code != 200:
                raise TursoClientError(
                    f"Turso COMMIT returned HTTP status {commit_resp.status_code}: {commit_resp.text[:200]}"
                )

            commit_data = commit_resp.json()
            commit_results = commit_data.get("results", [])
            if commit_results and commit_results[0].get("type") == "error":
                err = commit_results[0].get("error", {})
                raise TursoClientError(f"Turso COMMIT failed: {err.get('message')}")

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
        results = self.execute_pipeline([(sql, args or [])])
        return results[0]

    def executescript(self, script: str) -> None:
        """Execute a multi-statement DDL script."""
        statements = [s.strip() for s in script.split(";") if s.strip()]
        if not statements:
            return
        pipeline_items = [(s, []) for s in statements]
        self.execute_pipeline(pipeline_items)
