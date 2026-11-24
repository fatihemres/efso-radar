"""Pytest hermetic test guardrails (Issue #104 / T1B; hardened Issue #108 / T1D).

Provides an autouse function-scoped guardrail fixture that fails closed on:
1. Unintended real outbound network connections (`socket`, `urllib.request`,
   `http.client`, `httpx.HTTPTransport`, `httpx.AsyncHTTPTransport`).
2. Unintended opens, SQLite connections, or destructive operations targeting
   owner operational paths under `REPO_ROOT` (`data/`, `.env*` other than
   `.env.example`, `backups/` / `backup/`, and root-level operational
   databases / encrypted archives / recovery manifests), or ambiguous/malformed
   SQLite targets via shared `tests._sqlite_guard_policy`.
3. Unrestored CWD or `os.environ` mutations across tests.

Legitimate canonical in-memory SQLite (`:memory:`, canonical `file::memory:`
and `file:<name>?mode=memory` forms), `httpx.MockTransport`, tracked repository
fixtures (`tests/fixtures/`, `.env.example`), and test-owned temporary paths
(`tmp_path`, `tempfile.TemporaryDirectory`) remain permitted.
"""
from __future__ import annotations

import builtins
import http.client
import io
import os
import shutil
import socket
import sqlite3
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import httpx
import pytest

try:
    from tests._sqlite_guard_policy import (  # type: ignore[import-not-found]
        SqliteGuardPolicyError,
        parse_sqlite_connect_call,
    )
except ModuleNotFoundError as exc:
    if exc.name not in ("tests", "tests._sqlite_guard_policy"):
        raise
    from _sqlite_guard_policy import (  # type: ignore[import-not-found,no-redef]
        SqliteGuardPolicyError,
        parse_sqlite_connect_call,
    )

REPO_ROOT: Path = Path(__file__).resolve().parent.parent

RESIDUAL_LIMITATIONS: tuple[str, ...] = (
    "Autouse pytest fixtures run AFTER test module collection and top-level imports; "
    "collection-time decorators, top-level file/network calls, and script import side effects "
    "(e.g. sys.path or sys.stdout.reconfigure in scripts.preview_editorial_digest) are not "
    "intercepted by this fixture.",
    "Child Python or shell subprocesses (subprocess.run / subprocess.Popen) start fresh "
    "interpreters and do not inherit in-process Python monkeypatches.",
    "Direct C-level syscalls or raw integer file descriptors (open(fd)) bypass Python-level "
    "path wrappers; fd-relative dir_fd calls validate effective paths via /proc/self/fd "
    "(or fcntl.F_GETPATH where supported) and fail closed when resolution is unavailable.",
    "These Python-level monkeypatches are NOT an OS-level sandbox. "
    "Path.exists(), Path.stat(), and os.stat() metadata probes remain unblocked (and "
    "zero-arg Settings.from_env() still probes CWD .env unless overridden), so running "
    "tests only inside an isolated clean worktree (no .env, only tracked data/.gitkeep) "
    "remains mandatory for CI and agent verification.",
    "Blocking socket.socket.connect also blocks Windows asyncio ProactorEventLoop self-pipe "
    "initialization inside guarded tests unless asyncio event loops are created outside the guard.",
)

# Capture original functions once at module import so nested or restored contexts
# never stack wrappers or double-delegate.
_ORIG_BUILTINS_OPEN = builtins.open
_ORIG_IO_OPEN = io.open
_ORIG_OS_OPEN = os.open
_ORIG_OS_UNLINK = os.unlink
_ORIG_OS_REMOVE = os.remove
_ORIG_OS_RMDIR = os.rmdir
_ORIG_OS_REPLACE = os.replace
_ORIG_OS_RENAME = os.rename
_ORIG_SHUTIL_RMTREE = shutil.rmtree
_ORIG_SQLITE_CONNECT = sqlite3.connect
_ORIG_SOCKET_CONNECT = socket.socket.connect
_ORIG_SOCKET_CONNECT_EX = socket.socket.connect_ex
_ORIG_CREATE_CONNECTION = socket.create_connection
_ORIG_URLOPEN = urllib.request.urlopen
_ORIG_OPENER_OPEN = urllib.request.OpenerDirector.open
_ORIG_HTTP_CONNECT = http.client.HTTPConnection.connect
_ORIG_HTTPS_CONNECT = http.client.HTTPSConnection.connect
_ORIG_HTTPX_HANDLE = httpx.HTTPTransport.handle_request
_ORIG_HTTPX_ASYNC_HANDLE = httpx.AsyncHTTPTransport.handle_async_request

_ALLOWED_REPO_TOP_DIRS = frozenset({
    "src",
    "tests",
    "scripts",
    "docs",
    ".github",
    ".pytest_cache",
    ".hypothesis",
})

_OPERATIONAL_ROOT_EXTENSIONS = frozenset({
    ".db",
    ".db-wal",
    ".db-shm",
    ".sqlite",
    ".sqlite3",
    ".enc",
})


class HermeticGuardrailError(RuntimeError):
    """Raised fail-closed when a test attempts real network or operational-file I/O."""


def _is_blocked_normalized_path(
    norm_target: str,
    norm_repo_root: str,
    *,
    block_repo_root: bool = False,
) -> bool:
    clean_repo = norm_repo_root.rstrip("\\/")
    clean_target = norm_target.rstrip("\\/")
    if clean_target == clean_repo:
        return block_repo_root

    prefix = clean_repo + os.sep
    if not norm_target.startswith(prefix):
        return False

    rel = norm_target[len(prefix):]
    if not rel:
        return block_repo_root
    parts = [p.lower() for p in rel.split(os.sep) if p]
    if not parts:
        return block_repo_root

    top = parts[0]
    if top in _ALLOWED_REPO_TOP_DIRS:
        return False

    if top in ("data", "backups", "backup"):
        return True

    if len(parts) == 1:
        name = parts[0]
        if name == ".env.example":
            return False
        if name == ".env" or name.startswith(".env."):
            return True
        if any(name.endswith(ext) for ext in _OPERATIONAL_ROOT_EXTENSIONS):
            return True
        if name.startswith("efso_radar_staging_manifest_") and name.endswith(".json"):
            return True
        if "recovery" in name and "manifest" in name and name.endswith(".json"):
            return True

    return False


def _resolve_dir_fd(dir_fd: Any, operation: str) -> str:
    """Resolve an integer directory file descriptor to its filesystem path, failing closed."""
    if isinstance(dir_fd, bool) or not isinstance(dir_fd, int) or dir_fd < 0:
        raise HermeticGuardrailError(
            f"Hermetic guardrail blocked {operation}: invalid dir_fd={dir_fd!r} (fail closed)"
        )

    resolved: str | None = None
    proc_fd = f"/proc/self/fd/{dir_fd}"
    try:
        resolved = os.readlink(proc_fd)
    except OSError:
        resolved = None

    if resolved is None:
        try:
            import fcntl  # noqa: PLC0415

            if hasattr(fcntl, "F_GETPATH"):
                raw = fcntl.fcntl(dir_fd, fcntl.F_GETPATH, b"\0" * 1024)
                resolved = os.fsdecode(raw.split(b"\0", 1)[0])
        except Exception:
            resolved = None

    if (
        not resolved
        or not os.path.isabs(resolved)
        or resolved.endswith(" (deleted)")
        or resolved.startswith(("pipe:[", "socket:[", "anon_inode:"))
    ):
        raise HermeticGuardrailError(
            f"Hermetic guardrail blocked {operation}: cannot safely resolve dir_fd={dir_fd!r} (fail closed)"
        )
    return resolved


def _check_disk_path_not_operational(
    path_str: str,
    operation: str,
    *,
    block_repo_root: bool = False,
) -> None:
    """Reject operational targets under both lexical abspath and resolved realpath."""
    if not path_str:
        return

    norm_repo_abs = os.path.normcase(os.path.abspath(os.fspath(REPO_ROOT)))
    norm_repo_real = os.path.normcase(os.path.realpath(os.fspath(REPO_ROOT)))
    norm_target_abs = os.path.normcase(os.path.abspath(path_str))
    norm_target_real = os.path.normcase(os.path.realpath(path_str))

    for target_cand, repo_cand in (
        (norm_target_abs, norm_repo_abs),
        (norm_target_abs, norm_repo_real),
    ):
        if _is_blocked_normalized_path(
            target_cand,
            repo_cand,
            block_repo_root=block_repo_root,
        ):
            raise HermeticGuardrailError(
                f"Hermetic guardrail blocked {operation} on operational path: {path_str!r}"
            )

    # Also check realpath to defeat symlinks inside temp dirs pointing back to REPO_ROOT.
    for target_cand, repo_cand in (
        (norm_target_real, norm_repo_abs),
        (norm_target_real, norm_repo_real),
    ):
        if _is_blocked_normalized_path(
            target_cand,
            repo_cand,
            block_repo_root=block_repo_root,
        ):
            raise HermeticGuardrailError(
                f"Hermetic guardrail blocked {operation} via symlink to operational path: {path_str!r}"
            )


def check_not_operational_path(
    raw_path: Any,
    operation: str,
    *,
    dir_fd: Any = None,
    is_sqlite: bool = False,
    uri: bool = False,
    block_repo_root: bool = False,
) -> None:
    """Raise HermeticGuardrailError if `raw_path` targets an operational path under `REPO_ROOT`."""
    if is_sqlite:
        if dir_fd is not None:
            raise HermeticGuardrailError(
                f"Hermetic guardrail blocked {operation}: unexpected dir_fd={dir_fd!r} for SQLite target"
            )
        try:
            parsed = parse_sqlite_connect_call((raw_path,), {"uri": uri})
        except SqliteGuardPolicyError as exc:
            raise HermeticGuardrailError(
                f"Hermetic guardrail blocked {operation}: {exc}"
            ) from exc
        if parsed.kind == "memory" or parsed.lexical_path is None:
            return
        _check_disk_path_not_operational(
            parsed.lexical_path,
            operation,
            block_repo_root=block_repo_root,
        )
        return

    if raw_path is None or isinstance(raw_path, int):
        return

    if isinstance(raw_path, bytes):
        path_str = os.fsdecode(raw_path)
    elif isinstance(raw_path, os.PathLike):
        path_str = os.fsdecode(raw_path)
    elif isinstance(raw_path, str):
        path_str = raw_path
    else:
        return

    if dir_fd is not None and (not path_str or not os.path.isabs(path_str)):
        base_dir = _resolve_dir_fd(dir_fd, operation)
        path_str = os.path.join(base_dir, path_str) if path_str else base_dir

    if not path_str:
        return

    _check_disk_path_not_operational(
        path_str,
        operation,
        block_repo_root=block_repo_root,
    )


def _guarded_open(file: Any, *args: Any, **kwargs: Any) -> Any:
    check_not_operational_path(file, "open")
    return _ORIG_IO_OPEN(file, *args, **kwargs)


def _guarded_os_open(path: Any, flags: int, *args: Any, **kwargs: Any) -> int:
    check_not_operational_path(path, "os.open", dir_fd=kwargs.get("dir_fd"))
    return _ORIG_OS_OPEN(path, flags, *args, **kwargs)


def _guarded_unlink(path: Any, *args: Any, **kwargs: Any) -> None:
    check_not_operational_path(
        path,
        "os.unlink",
        dir_fd=kwargs.get("dir_fd"),
        block_repo_root=True,
    )
    _ORIG_OS_UNLINK(path, *args, **kwargs)


def _guarded_remove(path: Any, *args: Any, **kwargs: Any) -> None:
    check_not_operational_path(
        path,
        "os.remove",
        dir_fd=kwargs.get("dir_fd"),
        block_repo_root=True,
    )
    _ORIG_OS_REMOVE(path, *args, **kwargs)


def _guarded_rmdir(path: Any, *args: Any, **kwargs: Any) -> None:
    check_not_operational_path(
        path,
        "os.rmdir",
        dir_fd=kwargs.get("dir_fd"),
        block_repo_root=True,
    )
    _ORIG_OS_RMDIR(path, *args, **kwargs)


def _guarded_replace(src: Any, dst: Any, *args: Any, **kwargs: Any) -> None:
    check_not_operational_path(
        src,
        "os.replace(src)",
        dir_fd=kwargs.get("src_dir_fd"),
        block_repo_root=True,
    )
    check_not_operational_path(
        dst,
        "os.replace(dst)",
        dir_fd=kwargs.get("dst_dir_fd"),
        block_repo_root=True,
    )
    _ORIG_OS_REPLACE(src, dst, *args, **kwargs)


def _guarded_rename(src: Any, dst: Any, *args: Any, **kwargs: Any) -> None:
    check_not_operational_path(
        src,
        "os.rename(src)",
        dir_fd=kwargs.get("src_dir_fd"),
        block_repo_root=True,
    )
    check_not_operational_path(
        dst,
        "os.rename(dst)",
        dir_fd=kwargs.get("dst_dir_fd"),
        block_repo_root=True,
    )
    _ORIG_OS_RENAME(src, dst, *args, **kwargs)


def _guarded_rmtree(path: Any, *args: Any, **kwargs: Any) -> None:
    check_not_operational_path(
        path,
        "shutil.rmtree",
        dir_fd=kwargs.get("dir_fd"),
        block_repo_root=True,
    )
    _ORIG_SHUTIL_RMTREE(path, *args, **kwargs)


def _guarded_sqlite_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
    try:
        parsed = parse_sqlite_connect_call(args, kwargs)
    except SqliteGuardPolicyError as exc:
        raise HermeticGuardrailError(f"Hermetic guardrail blocked sqlite3.connect: {exc}") from exc
    if parsed.kind == "disk" and parsed.lexical_path is not None:
        _check_disk_path_not_operational(parsed.lexical_path, "sqlite3.connect")
    return _ORIG_SQLITE_CONNECT(*parsed.safe_args, **parsed.safe_kwargs)


def _blocked_socket_connect(self: Any, address: Any) -> Any:
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real network socket.connect({address!r})"
    )


def _blocked_socket_connect_ex(self: Any, address: Any) -> Any:
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real network socket.connect_ex({address!r})"
    )


def _blocked_create_connection(address: Any, *args: Any, **kwargs: Any) -> Any:
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real network socket.create_connection({address!r})"
    )


def _blocked_urlopen(url: Any, *args: Any, **kwargs: Any) -> Any:
    target = getattr(url, "full_url", url)
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real network urllib.request.urlopen({target!r})"
    )


def _blocked_opener_open(self: Any, fullurl: Any, *args: Any, **kwargs: Any) -> Any:
    target = getattr(fullurl, "full_url", fullurl)
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real network OpenerDirector.open({target!r})"
    )


def _blocked_http_connect(self: Any, *args: Any, **kwargs: Any) -> Any:
    host = getattr(self, "host", "<unknown>")
    port = getattr(self, "port", "<unknown>")
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real network http.client connect({host}:{port})"
    )


def _blocked_httpx_handle_request(self: Any, request: httpx.Request) -> httpx.Response:
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real httpx.HTTPTransport request to {request.url!s}"
    )


async def _blocked_httpx_handle_async_request(self: Any, request: httpx.Request) -> httpx.Response:
    raise HermeticGuardrailError(
        f"Hermetic guardrail blocked real httpx.AsyncHTTPTransport request to {request.url!s}"
    )


@contextmanager
def hermetic_guardrails_context() -> Iterator[None]:
    """Activate hermetic file/SQLite/network guards and restore CWD/env on exit."""
    orig_cwd = os.getcwd()
    orig_env = dict(os.environ)

    prev_builtins_open = builtins.open
    prev_io_open = io.open
    prev_os_open = os.open
    prev_os_unlink = os.unlink
    prev_os_remove = os.remove
    prev_os_rmdir = os.rmdir
    prev_os_replace = os.replace
    prev_os_rename = os.rename
    prev_shutil_rmtree = shutil.rmtree
    prev_sqlite_connect = sqlite3.connect
    prev_socket_connect = socket.socket.connect
    prev_socket_connect_ex = socket.socket.connect_ex
    prev_create_connection = socket.create_connection
    prev_urlopen = urllib.request.urlopen
    prev_opener_open = urllib.request.OpenerDirector.open
    prev_http_connect = http.client.HTTPConnection.connect
    prev_https_connect = http.client.HTTPSConnection.connect
    prev_httpx_handle = httpx.HTTPTransport.handle_request
    prev_httpx_async_handle = httpx.AsyncHTTPTransport.handle_async_request

    builtins.open = _guarded_open
    io.open = _guarded_open
    os.open = _guarded_os_open
    os.unlink = _guarded_unlink
    os.remove = _guarded_remove
    os.rmdir = _guarded_rmdir
    os.replace = _guarded_replace
    os.rename = _guarded_rename
    shutil.rmtree = _guarded_rmtree
    sqlite3.connect = _guarded_sqlite_connect
    socket.socket.connect = _blocked_socket_connect  # type: ignore[method-assign]
    socket.socket.connect_ex = _blocked_socket_connect_ex  # type: ignore[method-assign]
    socket.create_connection = _blocked_create_connection
    urllib.request.urlopen = _blocked_urlopen
    urllib.request.OpenerDirector.open = _blocked_opener_open  # type: ignore[method-assign]
    http.client.HTTPConnection.connect = _blocked_http_connect  # type: ignore[method-assign]
    http.client.HTTPSConnection.connect = _blocked_http_connect  # type: ignore[method-assign]
    httpx.HTTPTransport.handle_request = _blocked_httpx_handle_request  # type: ignore[method-assign]
    httpx.AsyncHTTPTransport.handle_async_request = _blocked_httpx_handle_async_request  # type: ignore[method-assign]

    try:
        yield
    finally:
        builtins.open = prev_builtins_open
        io.open = prev_io_open
        os.open = prev_os_open
        os.unlink = prev_os_unlink
        os.remove = prev_os_remove
        os.rmdir = prev_os_rmdir
        os.replace = prev_os_replace
        os.rename = prev_os_rename
        shutil.rmtree = prev_shutil_rmtree
        sqlite3.connect = prev_sqlite_connect
        socket.socket.connect = prev_socket_connect  # type: ignore[method-assign]
        socket.socket.connect_ex = prev_socket_connect_ex  # type: ignore[method-assign]
        socket.create_connection = prev_create_connection
        urllib.request.urlopen = prev_urlopen
        urllib.request.OpenerDirector.open = prev_opener_open  # type: ignore[method-assign]
        http.client.HTTPConnection.connect = prev_http_connect  # type: ignore[method-assign]
        http.client.HTTPSConnection.connect = prev_https_connect  # type: ignore[method-assign]
        httpx.HTTPTransport.handle_request = prev_httpx_handle  # type: ignore[method-assign]
        httpx.AsyncHTTPTransport.handle_async_request = prev_httpx_async_handle  # type: ignore[method-assign]

        try:
            if os.getcwd() != orig_cwd:
                os.chdir(orig_cwd)
        except OSError:
            os.chdir(os.fspath(REPO_ROOT))

        pytest_current = os.environ.get("PYTEST_CURRENT_TEST")
        os.environ.clear()
        os.environ.update(orig_env)
        if pytest_current is not None and "PYTEST_CURRENT_TEST" not in orig_env:
            os.environ["PYTEST_CURRENT_TEST"] = pytest_current


@pytest.fixture(autouse=True)
def _enforce_hermetic_test_guardrails() -> Iterator[None]:
    """Enforce per-test fail-closed network, operational-path, CWD, and env guardrails."""
    with hermetic_guardrails_context():
        yield
