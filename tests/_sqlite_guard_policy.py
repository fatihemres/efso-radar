"""Pure, stdlib-only shared SQLite connect argument and target validation policy (Issue #108 / T1D).

Centralizes fail-closed ``sqlite3.connect`` argument extraction, immutable
target normalization, canonical in-memory URI allowlisting, strict percent/NUL
decoding, and platform-aware disk-path syntax validation without filesystem
resolution, network calls, or ``sys.path`` / import side effects.
"""
from __future__ import annotations

import os
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Literal, Optional


class SqliteGuardPolicyError(ValueError):
    """Raised fail-closed when a SQLite connection call or target is unsafe or ambiguous."""


@dataclass(frozen=True)
class ParsedSqliteConnectTarget:
    """Validated, immutable representation of a ``sqlite3.connect`` call."""

    kind: Literal["memory", "disk"]
    uri: bool
    stable_database: str | bytes
    raw_database_str: str
    lexical_path: Optional[str]
    safe_args: tuple[Any, ...]
    safe_kwargs: dict[str, Any]


_SQLITE_CONNECT_POSITIONAL_PARAMS: tuple[str, ...] = (
    "database",
    "timeout",
    "detect_types",
    "isolation_level",
    "check_same_thread",
    "factory",
    "cached_statements",
    "uri",
)

_SQLITE_CONNECT_ALLOWED_KWARGS: frozenset[str] = frozenset(
    _SQLITE_CONNECT_POSITIONAL_PARAMS + ("autocommit",)
)

_HEX_DIGITS: frozenset[str] = frozenset("0123456789abcdefABCDEF")

_CANONICAL_MEMORY_URIS: frozenset[str] = frozenset({
    "file::memory:",
    "file::memory:?cache=shared",
    "file::memory:?cache=private",
    "file::memory:?mode=memory",
    "file::memory:?mode=memory&cache=shared",
    "file::memory:?cache=shared&mode=memory",
    "file::memory:?mode=memory&cache=private",
    "file::memory:?cache=private&mode=memory",
})

_CANONICAL_NAMED_MEMORY_URI_RE = re.compile(
    r"\Afile:[A-Za-z0-9_-]+\?(?:mode=memory|mode=memory&cache=(?:shared|private)|cache=(?:shared|private)&mode=memory)\Z"
)

_ALLOWED_DISK_URI_QUERY_VALUES: dict[str, frozenset[str]] = {
    "mode": frozenset({"ro", "rw", "rwc"}),
    "cache": frozenset({"shared", "private"}),
    "immutable": frozenset({"0", "1"}),
}

_WINDOWS_RESERVED_DEVICE_NAMES: frozenset[str] = frozenset({
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "CONIN$",
    "CONOUT$",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
})


def extract_sqlite_connect_args(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> tuple[Any, bool, bool]:
    """Extract ``(database, uri, database_from_kwarg)`` matching ``sqlite3.connect`` semantics."""
    if len(args) > len(_SQLITE_CONNECT_POSITIONAL_PARAMS):
        raise SqliteGuardPolicyError(
            f"Too many positional arguments ({len(args)}) passed to sqlite3.connect"
        )

    for key in kwargs:
        if key not in _SQLITE_CONNECT_ALLOWED_KWARGS:
            raise SqliteGuardPolicyError(
                f"Unsupported keyword argument {key!r} passed to sqlite3.connect"
            )

    for idx, param_name in enumerate(_SQLITE_CONNECT_POSITIONAL_PARAMS):
        if idx < len(args) and param_name in kwargs:
            raise SqliteGuardPolicyError(
                f"Ambiguous duplicate {param_name!r} argument passed to sqlite3.connect"
            )

    if len(args) >= 1:
        database = args[0]
        database_from_kwarg = False
    elif "database" in kwargs:
        database = kwargs["database"]
        database_from_kwarg = True
    else:
        raise SqliteGuardPolicyError("sqlite3.connect called without required 'database' argument")

    if len(args) >= 8:
        uri_raw = args[7]
    else:
        uri_raw = kwargs.get("uri", False)

    if not isinstance(uri_raw, bool):
        raise SqliteGuardPolicyError(
            f"Ambiguous non-boolean uri={uri_raw!r} passed to sqlite3.connect"
        )

    return database, uri_raw, database_from_kwarg


def _normalize_stable_database(database: Any) -> tuple[str | bytes, str]:
    """Normalize ``database`` once into an immutable ``str | bytes`` plus validated UTF-8 text."""
    if isinstance(database, os.PathLike):
        try:
            raw_obj = os.fspath(database)
        except Exception as exc:
            raise SqliteGuardPolicyError(
                f"Failed to resolve PathLike sqlite3.connect database target: {database!r}"
            ) from exc
    else:
        raw_obj = database

    if type(raw_obj) is bytes:
        if b"\x00" in raw_obj:
            raise SqliteGuardPolicyError(f"Null byte in bytes SQLite target rejected: {raw_obj!r}")
        try:
            raw_str = raw_obj.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise SqliteGuardPolicyError(
                f"Non-UTF-8 SQLite database bytes rejected: {raw_obj!r}"
            ) from exc
    elif type(raw_obj) is str:
        raw_str = raw_obj
    else:
        raise SqliteGuardPolicyError(
            f"Unsupported SQLite database target type: {type(raw_obj).__name__}"
        )

    if any(0xD800 <= ord(ch) <= 0xDFFF for ch in raw_str):
        raise SqliteGuardPolicyError("Surrogate code point in SQLite target rejected")

    if "\x00" in raw_str or not raw_str or not raw_str.strip():
        raise SqliteGuardPolicyError(f"Empty or null-byte SQLite target rejected: {raw_str!r}")

    return raw_obj, raw_str


def _strict_unquote_no_nul(component: str, *, full_uri: str, label: str) -> str:
    """Strictly percent-decode a UTF-8 URI component, rejecting malformed escapes or decoded NUL."""
    i = 0
    n = len(component)
    while i < n:
        if component[i] == "%":
            if (
                i + 2 >= n
                or component[i + 1] not in _HEX_DIGITS
                or component[i + 2] not in _HEX_DIGITS
            ):
                raise SqliteGuardPolicyError(
                    f"Malformed percent-encoding in SQLite URI {label}: {full_uri!r}"
                )
            i += 3
        else:
            i += 1

    raw_bytes = urllib.parse.unquote_to_bytes(component)
    if b"\x00" in raw_bytes:
        raise SqliteGuardPolicyError(
            f"Decoded NUL byte in SQLite URI {label} rejected: {full_uri!r}"
        )

    try:
        decoded = raw_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SqliteGuardPolicyError(
            f"Invalid UTF-8 percent-encoding in SQLite URI {label}: {full_uri!r}"
        ) from exc

    if "\x00" in decoded or any(0xD800 <= ord(ch) <= 0xDFFF for ch in decoded):
        raise SqliteGuardPolicyError(
            f"Decoded NUL or surrogate character in SQLite URI {label} rejected: {full_uri!r}"
        )
    return decoded


def _validate_disk_path_syntax(path_str: str, *, full_target: str, is_windows: bool) -> None:
    """Reject ambiguous memory prefixes and unsafe Windows UNC/device/drive-relative/ADS paths."""
    if "\x00" in path_str or not path_str or not path_str.strip():
        raise SqliteGuardPolicyError(f"Empty disk path in SQLite target rejected: {full_target!r}")

    if path_str.lstrip().lower().startswith(":memory:"):
        raise SqliteGuardPolicyError(
            f"Non-canonical :memory: SQLite target rejected: {full_target!r}"
        )

    if not is_windows:
        return

    if path_str.startswith(("\\\\", "//", "\\/", "/\\")):
        raise SqliteGuardPolicyError(
            f"Windows UNC or Win32 device SQLite path rejected without probe: {full_target!r}"
        )

    if path_str.startswith(("/", "\\")):
        raise SqliteGuardPolicyError(
            f"Ambiguous Windows drive-less rooted SQLite path rejected: {full_target!r}"
        )

    if (
        len(path_str) >= 2
        and path_str[0].isascii()
        and path_str[0].isalpha()
        and path_str[1] == ":"
    ):
        if len(path_str) < 3 or path_str[2] not in ("/", "\\"):
            raise SqliteGuardPolicyError(
                f"Ambiguous Windows drive-relative SQLite path rejected: {full_target!r}"
            )
        remainder = path_str[2:]
    else:
        remainder = path_str

    if ":" in remainder:
        raise SqliteGuardPolicyError(
            f"Windows Alternate Data Stream or unexpected colon in SQLite path rejected: {full_target!r}"
        )

    for segment in re.split(r"[\\/]+", remainder):
        if not segment or segment in (".", ".."):
            continue
        if segment[-1] in (" ", "."):
            raise SqliteGuardPolicyError(
                f"Trailing dot or space in Windows SQLite path segment {segment!r} rejected: {full_target!r}"
            )
        stem = segment.split(".", 1)[0].upper()
        if stem in _WINDOWS_RESERVED_DEVICE_NAMES:
            raise SqliteGuardPolicyError(
                f"Windows reserved device name {stem!r} in SQLite path rejected: {full_target!r}"
            )


def _validate_disk_uri_query(raw_query: str, *, full_uri: str) -> None:
    """Validate SQLite disk-URI query string fail-closed against strict allowlist."""
    if raw_query == "":
        raise SqliteGuardPolicyError(f"Empty trailing query in SQLite URI rejected: {full_uri!r}")

    seen_keys: set[str] = set()
    for segment in raw_query.split("&"):
        if not segment or "=" not in segment:
            raise SqliteGuardPolicyError(
                f"Malformed SQLite URI query parameter rejected: {full_uri!r}"
            )
        k_raw, v_raw = segment.split("=", 1)
        k = _strict_unquote_no_nul(k_raw, full_uri=full_uri, label="query key")
        v = _strict_unquote_no_nul(v_raw, full_uri=full_uri, label="query value")
        if k in seen_keys:
            raise SqliteGuardPolicyError(
                f"Duplicate SQLite URI query key {k!r} rejected: {full_uri!r}"
            )
        seen_keys.add(k)
        allowed_vals = _ALLOWED_DISK_URI_QUERY_VALUES.get(k)
        if allowed_vals is None or v not in allowed_vals:
            raise SqliteGuardPolicyError(
                f"Unsupported or ambiguous SQLite URI query parameter {k}={v!r} rejected: {full_uri!r}"
            )


def parse_sqlite_connect_call(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    is_windows: Optional[bool] = None,
) -> ParsedSqliteConnectTarget:
    """Validate and classify a ``sqlite3.connect`` call into canonical memory or lexical disk target.

    Does NOT call ``os.path.abspath`` or ``os.path.realpath`` so callers can evaluate
    both lexical and resolved operational-path policies without triggering remote/device probes
    on syntactically invalid inputs. ``safe_args`` and ``safe_kwargs`` embed the single
    immutable ``stable_database`` string/bytes normalized from any ``PathLike``.
    """
    platform_is_windows = (os.name == "nt") if is_windows is None else is_windows
    database, uri, database_from_kwarg = extract_sqlite_connect_args(args, kwargs)
    stable_database, raw_str = _normalize_stable_database(database)

    safe_kwargs = dict(kwargs)
    if database_from_kwarg:
        safe_args = tuple(args)
        safe_kwargs["database"] = stable_database
    else:
        safe_args = (stable_database, *args[1:])

    if raw_str == ":memory:":
        return ParsedSqliteConnectTarget(
            kind="memory",
            uri=uri,
            stable_database=stable_database,
            raw_database_str=raw_str,
            lexical_path=None,
            safe_args=safe_args,
            safe_kwargs=safe_kwargs,
        )

    if raw_str.strip().lower() == ":memory:" or raw_str.lstrip().lower().startswith(":memory:"):
        raise SqliteGuardPolicyError(
            f"Ambiguous or non-canonical :memory: SQLite target rejected: {raw_str!r}"
        )

    # Never assume uri=False guarantees disabled URI parsing under SQLITE_USE_URI builds/configs.
    if not uri:
        if raw_str.lstrip().lower().startswith("file:"):
            raise SqliteGuardPolicyError(
                f"SQLite file: string with uri=False is ambiguous or disk-backed; rejected: {raw_str!r}"
            )
        _validate_disk_path_syntax(raw_str, full_target=raw_str, is_windows=platform_is_windows)
        return ParsedSqliteConnectTarget(
            kind="disk",
            uri=uri,
            stable_database=stable_database,
            raw_database_str=raw_str,
            lexical_path=raw_str,
            safe_args=safe_args,
            safe_kwargs=safe_kwargs,
        )

    if not raw_str.startswith("file:"):
        if raw_str.lstrip().lower().startswith("file:"):
            raise SqliteGuardPolicyError(
                f"Non-canonical SQLite file: URI scheme or leading whitespace rejected: {raw_str!r}"
            )
        _validate_disk_path_syntax(raw_str, full_target=raw_str, is_windows=platform_is_windows)
        return ParsedSqliteConnectTarget(
            kind="disk",
            uri=uri,
            stable_database=stable_database,
            raw_database_str=raw_str,
            lexical_path=raw_str,
            safe_args=safe_args,
            safe_kwargs=safe_kwargs,
        )

    if raw_str in _CANONICAL_MEMORY_URIS or _CANONICAL_NAMED_MEMORY_URI_RE.fullmatch(raw_str):
        return ParsedSqliteConnectTarget(
            kind="memory",
            uri=uri,
            stable_database=stable_database,
            raw_database_str=raw_str,
            lexical_path=None,
            safe_args=safe_args,
            safe_kwargs=safe_kwargs,
        )

    if "#" in raw_str:
        raise SqliteGuardPolicyError(f"SQLite URI fragment rejected fail-closed: {raw_str!r}")

    raw_path_part, has_query, raw_query = raw_str[5:].partition("?")
    if has_query:
        _validate_disk_uri_query(raw_query, full_uri=raw_str)

    if raw_path_part.startswith("//"):
        after_slashes = raw_path_part[2:]
        if "/" not in after_slashes:
            raise SqliteGuardPolicyError(f"Malformed SQLite URI authority rejected: {raw_str!r}")
        authority, rest = after_slashes.split("/", 1)
        if authority.lower() not in ("", "localhost"):
            raise SqliteGuardPolicyError(
                f"Unsupported SQLite URI authority {authority!r} rejected: {raw_str!r}"
            )
        decoded_rest = _strict_unquote_no_nul(rest, full_uri=raw_str, label="path")
        if not decoded_rest:
            raise SqliteGuardPolicyError(
                f"Empty path in SQLite authority URI rejected: {raw_str!r}"
            )
        if decoded_rest.startswith(("/", "\\")):
            raise SqliteGuardPolicyError(
                f"Ambiguous leading separator in SQLite authority URI rejected: {raw_str!r}"
            )
        if platform_is_windows:
            if not (
                len(decoded_rest) >= 3
                and decoded_rest[0].isascii()
                and decoded_rest[0].isalpha()
                and decoded_rest[1] == ":"
                and decoded_rest[2] in ("/", "\\")
            ):
                raise SqliteGuardPolicyError(
                    f"Windows SQLite authority URI must use an explicit drive root: {raw_str!r}"
                )
            disk_path_str = decoded_rest
        else:
            disk_path_str = "/" + decoded_rest
    else:
        disk_path_str = _strict_unquote_no_nul(raw_path_part, full_uri=raw_str, label="path")

    _validate_disk_path_syntax(disk_path_str, full_target=raw_str, is_windows=platform_is_windows)

    return ParsedSqliteConnectTarget(
        kind="disk",
        uri=uri,
        stable_database=stable_database,
        raw_database_str=raw_str,
        lexical_path=disk_path_str,
        safe_args=safe_args,
        safe_kwargs=safe_kwargs,
    )
