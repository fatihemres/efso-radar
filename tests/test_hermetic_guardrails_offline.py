"""Negative controls and non-interference tests for `tests/conftest.py` (Issue #104 / T1B)."""
from __future__ import annotations

import http.client
import io
import os
import shutil
import socket
import sqlite3
import tempfile
import urllib.request
import warnings
from pathlib import Path

import httpx
import pytest

import sys
from unittest.mock import MagicMock

from efso_radar.repositories.sqlite import SqliteStateRepository

try:
    from tests._sqlite_guard_policy import (  # type: ignore[import-not-found]
        _CANONICAL_MEMORY_URIS,
        SqliteGuardPolicyError,
        parse_sqlite_connect_call,
    )
except ModuleNotFoundError as exc:
    if exc.name not in ("tests", "tests._sqlite_guard_policy"):
        raise
    from _sqlite_guard_policy import (  # type: ignore[import-not-found,no-redef]
        _CANONICAL_MEMORY_URIS,
        SqliteGuardPolicyError,
        parse_sqlite_connect_call,
    )

conftest = sys.modules.get("conftest") or sys.modules.get("tests.conftest")
if conftest is None:
    import tests.conftest as conftest  # type: ignore[import-not-found,no-redef]
REPO_ROOT = conftest.REPO_ROOT
RESIDUAL_LIMITATIONS = conftest.RESIDUAL_LIMITATIONS
HermeticGuardrailError = conftest.HermeticGuardrailError
_resolve_dir_fd = conftest._resolve_dir_fd
check_not_operational_path = conftest.check_not_operational_path
hermetic_guardrails_context = conftest.hermetic_guardrails_context


def test_network_negative_controls_fail_closed() -> None:
    """Real outbound socket, urllib, http.client, and httpx network calls must fail closed."""
    with pytest.raises(HermeticGuardrailError):
        socket.create_connection(("192.0.2.1", 443), timeout=1.0)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        with pytest.raises(HermeticGuardrailError):
            sock.connect(("192.0.2.1", 443))
        with pytest.raises(HermeticGuardrailError):
            sock.connect_ex(("192.0.2.1", 443))

    with pytest.raises(HermeticGuardrailError):
        urllib.request.urlopen("https://example.invalid/feed.xml", timeout=1.0)

    opener = urllib.request.build_opener()
    with pytest.raises(HermeticGuardrailError):
        opener.open("https://example.invalid/feed.xml", timeout=1.0)

    http_conn = http.client.HTTPConnection("example.invalid", 80, timeout=1.0)
    with pytest.raises(HermeticGuardrailError):
        http_conn.connect()

    https_conn = http.client.HTTPSConnection("example.invalid", 443, timeout=1.0)
    with pytest.raises(HermeticGuardrailError):
        https_conn.connect()

    with httpx.Client() as client:
        with pytest.raises(HermeticGuardrailError):
            client.get("https://example.invalid/api")

    # Test async httpx transport directly via handle_async_request coroutine without
    # `async with AsyncHTTPTransport()` or `asyncio.run` (the latter opens a Windows
    # ProactorEventLoop self-pipe via socket.connect() when socket.connect is guarded).
    async_transport = httpx.AsyncHTTPTransport()
    req = httpx.Request("GET", "https://example.invalid/async")
    coro = async_transport.handle_async_request(req)
    try:
        with pytest.raises(HermeticGuardrailError):
            coro.send(None)
    finally:
        coro.close()


def test_operational_file_and_sqlite_negative_controls_fail_closed(tmp_path: Path) -> None:
    """Reads, writes, SQLite connections, and destructive ops on owner paths must fail closed."""
    rel_db = Path("data/radar.db")
    abs_state = REPO_ROOT / "data" / "state.json"
    abs_csv = REPO_ROOT / "data" / "Efso_Radar_Kalibrasyon_Doldurulmus.csv"
    abs_env = REPO_ROOT / ".env"
    abs_env_local = REPO_ROOT / ".env.local"
    traversal_db = REPO_ROOT / "tests" / ".." / "data" / "radar.db"
    traversal_env = REPO_ROOT / "tests" / ".." / ".env"
    backup_enc = REPO_ROOT / "backups" / "synthetic_backup.enc"
    root_manifest = REPO_ROOT / "efso_radar_staging_manifest_run_test.json"

    with pytest.raises(HermeticGuardrailError):
        open(rel_db, "rb")
    with pytest.raises(HermeticGuardrailError):
        open(abs_state, "r", encoding="utf-8")
    with pytest.raises(HermeticGuardrailError):
        io.open(abs_csv, "r", encoding="utf-8")

    with pytest.raises(HermeticGuardrailError):
        abs_state.read_text(encoding="utf-8")
    with pytest.raises(HermeticGuardrailError):
        abs_env.read_bytes()
    with pytest.raises(HermeticGuardrailError):
        abs_env_local.write_text("MUTATION=blocked\n", encoding="utf-8")
    with pytest.raises(HermeticGuardrailError):
        backup_enc.write_bytes(b"blocked")
    with pytest.raises(HermeticGuardrailError):
        root_manifest.write_text("{}", encoding="utf-8")

    with pytest.raises(HermeticGuardrailError):
        os.open(traversal_db, os.O_RDONLY)
    with pytest.raises(HermeticGuardrailError):
        open(traversal_env, "r", encoding="utf-8")

    with pytest.raises(HermeticGuardrailError):
        sqlite3.connect("data/radar.db")
    with pytest.raises(HermeticGuardrailError):
        sqlite3.connect(abs_csv.with_name("radar.db"))
    with pytest.raises(HermeticGuardrailError):
        sqlite3.connect("file:data/radar.db?mode=ro", uri=True)
    with pytest.raises(HermeticGuardrailError):
        sqlite3.connect((REPO_ROOT / "data" / "radar.db").as_uri() + "?mode=ro", uri=True)

    with pytest.raises(HermeticGuardrailError):
        os.unlink(abs_state)
    with pytest.raises(HermeticGuardrailError):
        os.remove(abs_env)
    with pytest.raises(HermeticGuardrailError):
        os.replace(tmp_path / "src.txt", abs_state)
    with pytest.raises(HermeticGuardrailError):
        os.rename(abs_state, tmp_path / "dst.txt")
    with pytest.raises(HermeticGuardrailError):
        shutil.rmtree(REPO_ROOT / "data")


def test_legitimate_hermetic_paths_and_mocks_noninterference(tmp_path: Path) -> None:
    """In-memory SQLite, MockTransport, temp data/.env dirs, and tracked fixtures remain allowed."""
    # 1. httpx.MockTransport works normally; constructing unused HTTPTransport does not fail at init.
    mock_transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"ok": True, "url": str(request.url)})
    )
    with httpx.Client(transport=mock_transport) as client:
        resp = client.get("https://mock.example.invalid/ping")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    unused_transport = httpx.HTTPTransport(retries=0)
    unused_transport.close()

    # 2. In-memory SQLite and temp SQLite repositories work normally.
    with sqlite3.connect(":memory:") as mem_conn:
        assert mem_conn.execute("SELECT 1").fetchone() == (1,)

    with sqlite3.connect("file::memory:?cache=shared", uri=True) as uri_mem:
        assert uri_mem.execute("SELECT 2").fetchone() == (2,)

    temp_db = tmp_path / "repo.db"
    repo = SqliteStateRepository(temp_db)
    assert repo.db_path == temp_db
    with sqlite3.connect(temp_db) as file_conn:
        assert file_conn.execute("SELECT 3").fetchone() == (3,)

    # 3. Temp directories named data/ or .env remain writable and removable for test fixtures.
    temp_data_dir = tmp_path / "data"
    temp_data_dir.mkdir()
    temp_state = temp_data_dir / "synthetic_state.json"
    temp_state.write_text('{"synthetic": true}', encoding="utf-8")
    assert "synthetic" in temp_state.read_text(encoding="utf-8")

    temp_env = tmp_path / ".env"
    temp_env.write_text("EFSO_TEST=1\n", encoding="utf-8")
    assert "EFSO_TEST=1" in temp_env.read_text(encoding="utf-8")
    os.unlink(temp_env)
    shutil.rmtree(temp_data_dir)

    # 4. Tracked repo fixtures and .env.example remain readable.
    benchmark_fixture = REPO_ROOT / "tests" / "fixtures" / "editorial_benchmark.json"
    assert benchmark_fixture.exists()
    assert len(benchmark_fixture.read_text(encoding="utf-8")) > 0

    env_example = REPO_ROOT / ".env.example"
    if env_example.exists():
        assert len(env_example.read_text(encoding="utf-8")) > 0


def test_hermetic_guardrails_context_restores_env_and_cwd(tmp_path: Path) -> None:
    """hermetic_guardrails_context restores CWD and os.environ on both normal and exception exit."""
    before_cwd = os.getcwd()
    before_env_val = os.environ.get("EFSO_T1B_TRANSIENT_MARKER")
    before_pytest_curr = os.environ.get("PYTEST_CURRENT_TEST")

    with hermetic_guardrails_context():
        os.chdir(tmp_path)
        os.environ["EFSO_T1B_TRANSIENT_MARKER"] = "mutated-normal"
        assert Path(os.getcwd()).resolve() == tmp_path.resolve()

    assert os.getcwd() == before_cwd
    assert os.environ.get("EFSO_T1B_TRANSIENT_MARKER") == before_env_val
    assert os.environ.get("PYTEST_CURRENT_TEST") == before_pytest_curr

    with pytest.raises(ValueError, match="boom"):
        with hermetic_guardrails_context():
            os.chdir(tmp_path)
            os.environ["EFSO_T1B_TRANSIENT_MARKER"] = "mutated-exception"
            raise ValueError("boom")

    assert os.getcwd() == before_cwd
    assert os.environ.get("EFSO_T1B_TRANSIENT_MARKER") == before_env_val
    assert os.environ.get("PYTEST_CURRENT_TEST") == before_pytest_curr


def test_residual_limitations_are_explicitly_documented() -> None:
    """Guardrail module explicitly discloses collection/import, subprocess, fd, sandbox, and stat limitations."""
    joined = " ".join(RESIDUAL_LIMITATIONS).lower()
    assert "collection" in joined
    assert "subprocess" in joined
    assert "file descriptors" in joined
    assert "stat()" in joined or "exists()" in joined
    assert "sandbox" in joined
    assert "untouched phase 5b.3c" not in joined


def test_positive_temp_directory_cleanup_with_operational_like_basenames() -> None:
    """Temp cleanup (TemporaryDirectory & shutil.rmtree) succeeds with operational-like basenames on Linux & Windows."""
    orig_cwd = os.getcwd()
    os.chdir(REPO_ROOT)

    tmpdir = tempfile.TemporaryDirectory(prefix="efso_t1b_cleanup_")
    tmp_root = Path(tmpdir.name)
    for filename in (
        "test_phase3c2d.db",
        "shared_mock_staging.db",
        "radar.db-wal",
        "efso_radar_staging_manifest_run_20261123T000000Z_abcdef123456.json",
        ".env",
    ):
        (tmp_root / filename).write_text("synthetic\n", encoding="utf-8")

    (tmp_root / "data").mkdir()
    (tmp_root / "data" / "state.json").write_text("{}", encoding="utf-8")
    (tmp_root / "backups").mkdir()
    (tmp_root / "backups" / "archive.enc").write_bytes(b"enc")
    repo = SqliteStateRepository(tmp_root / "repo.db")
    with repo._connection() as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1

    # Cleanup runs shutil.rmtree (using dir_fd on Linux) while CWD == REPO_ROOT.
    tmpdir.cleanup()
    assert not tmp_root.exists()

    # Verify explicit shutil.rmtree with nested data/ and operational-like basenames too.
    explicit_dir = Path(tempfile.mkdtemp(prefix="efso_t1b_rmtree_"))
    (explicit_dir / "data").mkdir()
    (explicit_dir / "data" / "radar.db").write_bytes(b"sqlite")
    (explicit_dir / "test_phase3c2d.db").write_bytes(b"sqlite")
    shutil.rmtree(explicit_dir)
    assert not explicit_dir.exists()
    assert os.path.normcase(os.getcwd()) == os.path.normcase(str(REPO_ROOT)) or os.getcwd() == orig_cwd


def test_unknown_or_unsupported_dir_fd_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown, negative, boolean, deleted, pipe, or unresolvable dir_fd values fail closed."""
    for bad_fd in (-1, 999999, True):
        with pytest.raises(HermeticGuardrailError):
            check_not_operational_path("safe.txt", "os.unlink", dir_fd=bad_fd)

    with pytest.raises(HermeticGuardrailError):
        os.open("safe.txt", os.O_RDONLY, dir_fd=999999)
    with pytest.raises(HermeticGuardrailError):
        os.unlink("safe.txt", dir_fd=999999)
    with pytest.raises(HermeticGuardrailError):
        os.remove("safe.txt", dir_fd=999999)
    with pytest.raises(HermeticGuardrailError):
        os.rmdir("safe_dir", dir_fd=999999)
    with pytest.raises(HermeticGuardrailError):
        os.rename("a.txt", "b.txt", src_dir_fd=999999)
    with pytest.raises(HermeticGuardrailError):
        os.replace("a.txt", "b.txt", dst_dir_fd=999999)
    with pytest.raises(HermeticGuardrailError):
        shutil.rmtree("safe_dir", dir_fd=999999)

    with monkeypatch.context() as mp:
        mp.setattr(os, "readlink", lambda _p: "/tmp/deleted_dir (deleted)")
        with pytest.raises(HermeticGuardrailError):
            _resolve_dir_fd(999999, "os.unlink")

        mp.setattr(os, "readlink", lambda _p: "pipe:[12345]")
        with pytest.raises(HermeticGuardrailError):
            _resolve_dir_fd(999999, "os.unlink")

        def _raise_oserror(_p: str) -> str:
            raise OSError("procfs unavailable")

        mp.setattr(os, "readlink", _raise_oserror)
        with pytest.raises(HermeticGuardrailError):
            _resolve_dir_fd(999999, "os.open")


def test_fd_relative_policy_with_synthetic_standin_repo_cross_platform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-platform policy test using a synthetic stand-in repo and mocked fd resolution."""
    fake_repo = tmp_path / "synthetic_repo"
    fake_data = fake_repo / "data"
    fake_backups = fake_repo / "backups"
    fake_tests = fake_repo / "tests"
    safe_scratch = tmp_path / "safe_scratch"
    for d in (fake_data, fake_backups, fake_tests, safe_scratch):
        d.mkdir(parents=True)

    (fake_data / "state.json").write_text('{"synthetic": true}', encoding="utf-8")
    (fake_repo / ".env").write_text("SYNTHETIC_SECRET=1\n", encoding="utf-8")
    (fake_backups / "b.enc").write_bytes(b"synthetic")

    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", fake_repo)
        # Stand-in fd=100 resolves to synthetic data/ -> operational paths blocked fail-closed.
        mp.setattr(
            conftest,
            "_resolve_dir_fd",
            lambda fd, _op: str(fake_data) if fd == 100 else str(safe_scratch),
        )

        with pytest.raises(HermeticGuardrailError):
            check_not_operational_path("state.json", "os.unlink", dir_fd=100, block_repo_root=True)
        with pytest.raises(HermeticGuardrailError):
            check_not_operational_path("state.json", "os.open", dir_fd=100)
        with pytest.raises(HermeticGuardrailError):
            check_not_operational_path("../.env", "os.remove", dir_fd=100, block_repo_root=True)
        with pytest.raises(HermeticGuardrailError):
            check_not_operational_path("../backups/b.enc", "os.replace(dst)", dir_fd=100, block_repo_root=True)
        with pytest.raises(HermeticGuardrailError):
            shutil.rmtree(fake_repo)
        with pytest.raises(HermeticGuardrailError):
            os.rmdir(fake_repo)

        # Stand-in fd=200 resolves to safe_scratch -> permitted even for .db / data basenames.
        check_not_operational_path("test.db", "os.unlink", dir_fd=200, block_repo_root=True)
        check_not_operational_path("data", "os.open", dir_fd=200)


@pytest.mark.skipif(
    os.unlink not in os.supports_dir_fd or os.open not in os.supports_dir_fd,
    reason="Host OS does not support dir_fd on os.open/os.unlink",
)
def test_posix_real_dir_fd_positive_and_negative_controls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real POSIX dir_fd positive temp ops and negative synthetic stand-in repo ops."""
    fake_repo = tmp_path / "synthetic_repo_posix"
    fake_data = fake_repo / "data"
    fake_tests = fake_repo / "tests"
    safe_dir = tmp_path / "safe_scratch"
    safe_empty_sub = safe_dir / "sub_empty"
    for d in (fake_data, fake_tests, safe_dir, safe_empty_sub):
        d.mkdir(parents=True)

    state_file = fake_data / "state.json"
    db_file = fake_data / "radar.db"
    env_file = fake_repo / ".env"
    state_file.write_text('{"synthetic": true}', encoding="utf-8")
    db_file.write_bytes(b"synthetic-db")
    env_file.write_text("SYNTHETIC=1\n", encoding="utf-8")

    (safe_dir / "test.db").write_bytes(b"temp-db")
    (safe_dir / "temp.txt").write_text("temp\n", encoding="utf-8")

    repo_fd = os.open(fake_repo, os.O_RDONLY)
    data_fd = os.open(fake_data, os.O_RDONLY)
    tests_fd = os.open(fake_tests, os.O_RDONLY)
    safe_fd = os.open(safe_dir, os.O_RDONLY)
    pipe_r, pipe_w = os.pipe()
    closed_fd = os.open(safe_dir, os.O_RDONLY)
    os.close(closed_fd)

    try:
        # Redirect REPO_ROOT after opening stand-in directory descriptors (never touches real data/).
        with monkeypatch.context() as mp:
            mp.setattr(conftest, "REPO_ROOT", fake_repo)

            # Positive real dir_fd operations on test-owned safe_scratch succeed.
            opened_fd = os.open("test.db", os.O_RDONLY, dir_fd=safe_fd)
            os.close(opened_fd)
            os.rename("temp.txt", "renamed.txt", src_dir_fd=safe_fd, dst_dir_fd=safe_fd)
            assert (safe_dir / "renamed.txt").exists()
            os.unlink("test.db", dir_fd=safe_fd)
            assert not (safe_dir / "test.db").exists()
            os.rmdir("sub_empty", dir_fd=safe_fd)
            assert not safe_empty_sub.exists()

            # Negative real dir_fd operations targeting synthetic stand-in operational paths fail closed.
            with pytest.raises(HermeticGuardrailError):
                os.open("state.json", os.O_RDONLY, dir_fd=data_fd)
            with pytest.raises(HermeticGuardrailError):
                os.unlink("state.json", dir_fd=data_fd)
            with pytest.raises(HermeticGuardrailError):
                os.remove("radar.db", dir_fd=data_fd)
            with pytest.raises(HermeticGuardrailError):
                os.rename("state.json", "stolen.json", src_dir_fd=data_fd, dst_dir_fd=safe_fd)
            with pytest.raises(HermeticGuardrailError):
                os.replace("renamed.txt", "state.json", src_dir_fd=safe_fd, dst_dir_fd=data_fd)
            with pytest.raises(HermeticGuardrailError):
                os.open("../data/state.json", os.O_RDONLY, dir_fd=tests_fd)
            with pytest.raises(HermeticGuardrailError):
                os.unlink("../.env", dir_fd=tests_fd)
            with pytest.raises(HermeticGuardrailError):
                shutil.rmtree("data", dir_fd=repo_fd)
            with pytest.raises(HermeticGuardrailError):
                os.rmdir("data", dir_fd=repo_fd)

            # Real closed fd and real pipe fd fail closed.
            with pytest.raises(HermeticGuardrailError):
                os.unlink("renamed.txt", dir_fd=closed_fd)
            with pytest.raises(HermeticGuardrailError):
                os.open("renamed.txt", os.O_RDONLY, dir_fd=pipe_r)

        assert state_file.read_text(encoding="utf-8") == '{"synthetic": true}'
        assert db_file.read_bytes() == b"synthetic-db"
        assert env_file.read_text(encoding="utf-8") == "SYNTHETIC=1\n"
    finally:
        for fd in (repo_fd, data_fd, tests_fd, safe_fd, pipe_r, pipe_w):
            try:
                os.close(fd)
            except OSError:
                pass


def test_sqlite_guard_rejects_masquerades_nul_and_malformed_uris_before_delegate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shared SQLite guard rejects masquerades, encoded NUL, bad encodings, and bad args pre-delegate."""
    fake_repo = tmp_path / "synthetic_repo_sqli"
    (fake_repo / "data").mkdir(parents=True)
    fake_db = fake_repo / "data" / "radar.db"
    fake_db.write_bytes(b"synthetic-db")
    fake_env = fake_repo / ".env"
    fake_env.write_text("SYNTHETIC=1\n", encoding="utf-8")

    fake_db_uri = f"file:{fake_db.resolve().as_posix()}"
    if not fake_db_uri.startswith("file:///"):
        fake_db_uri = f"file:///{fake_db.resolve().as_posix().lstrip('/')}"

    class _SneakyStr(str):
        pass

    mock_orig = MagicMock()
    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", fake_repo)
        mp.setattr(conftest, "_ORIG_SQLITE_CONNECT", mock_orig)
        mp.chdir(fake_repo)

        adversarial_calls: list[tuple[tuple[object, ...], dict[str, object]]] = [
            # ?note=mode=memory masquerades (kw, positional, database=)
            ((f"{fake_db_uri}?note=mode=memory",), {"uri": True}),
            ((f"{fake_db_uri}?note=mode=memory", 5.0, 0, "", True, sqlite3.Connection, 128, True), {}),
            ((), {"database": f"{fake_db_uri}?note=mode=memory", "uri": True}),
            ((f"{fake_db_uri}?mode=memory&mode=rwc",), {"uri": True}),
            ((f"{fake_db_uri}#?mode=memory",), {"uri": True}),
            ((f"{fake_db_uri}?mode=memory",), {"uri": True}),
            # Broad file::memory: prefix masquerades
            (("file::memory:masquerade",), {"uri": True}),
            (("file::memory:data/radar.db",), {"uri": True}),
            (("file:%3Amemory%3A",), {"uri": True}),
            (("file::memory:?mode=rwc",), {"uri": True}),
            (("file::MEMORY:",), {"uri": True}),
            # All 3 T1C Review #2 encoded-NUL query-key reproducers
            (("file:synth?mode=memory&mode%00=rwc",), {"uri": True}),
            (("file:synth2?mode=memory&mode%00junk=rwc",), {"uri": True}),
            (("file:synth3?mode=memory&mo%64e%00=rwc",), {"uri": True}),
            # Encoded NUL in query values & paths + raw NUL
            (("file:synth4?mode=memory%00",), {"uri": True}),
            (("file:synth5?mode=memory&cache=shared%00",), {"uri": True}),
            (("file:synth6?mode=ro%00",), {"uri": True}),
            ((f"{fake_db_uri}%00?mode=ro",), {"uri": True}),
            (("file:data/radar.db%00?mode=ro",), {"uri": True}),
            (("data/radar.db\x00",), {}),
            ((b"data/radar.db\x00",), {}),
            ((":memory:\x00",), {}),
            # Malformed percent-encoding & invalid UTF-8
            (("file:synth7?mode=memory&bad=%zz",), {"uri": True}),
            (("file:synth8?mode=%2",), {"uri": True}),
            (("file:synth9?%zz=1",), {"uri": True}),
            (("file:synth%zz.db?mode=ro",), {"uri": True}),
            (("file:synth%ff.db?mode=ro",), {"uri": True}),
            (("file:synth.db?mode=r%ff",), {"uri": True}),
            ((b"data/\xff.db",), {}),
            (("data/\udc80.db",), {}),
            # Conflicting, duplicate, or unsupported URI query semantics & fragments
            (("file:synth.db?mode=ro&mode=rwc",), {"uri": True}),
            (("file:synth.db?m%6fde=ro&mode=rwc",), {"uri": True}),
            (("file:synth.db?cache=shared&cache=private",), {"uri": True}),
            (("file:synth.db?vfs=unix-none",), {"uri": True}),
            (("file:synth.db?",), {"uri": True}),
            (("file:synth.db?#frag",), {"uri": True}),
            (("file:synth.db#?mode=memory",), {"uri": True}),
            (("file:synth.db?&mode=ro",), {"uri": True}),
            (("file:synth.db?mode=ro&",), {"uri": True}),
            (("file:synth.db?mode",), {"uri": True}),
            # uri=False & positional uri=False with file:
            (("file::memory:",), {}),
            (("file::memory:",), {"uri": False}),
            (("file::memory:?cache=shared",), {"uri": False}),
            (("file:legit?mode=memory",), {"uri": False}),
            (("file:data/radar.db?mode=ro",), {"uri": False}),
            (("FILE:data/radar.db",), {"uri": False}),
            (("file::memory:", 5.0, 0, "", True, sqlite3.Connection, 128, False), {}),
            (("file:data/radar.db?mode=ro", 5.0, 0, "", True, sqlite3.Connection, 128, False), {}),
            # Unsupported/ambiguous connect args & non-canonical targets
            ((), {}),
            ((":memory:",), {"database": ":memory:"}),
            ((":memory:", 5.0, 0, "", True, sqlite3.Connection, 128, False), {"uri": False}),
            ((":memory:", 5.0), {"timeout": 10.0}),
            ((":memory:", 5.0, 0, "", True, sqlite3.Connection, 128, False, "extra"), {}),
            ((":memory:",), {"bogus_kw": 1}),
            ((":memory:",), {"uri": 1}),
            ((":memory:",), {"uri": 0}),
            ((":memory:",), {"uri": "true"}),
            ((":memory:",), {"uri": None}),
            ((":memory:", 5.0, 0, "", True, sqlite3.Connection, 128, 1), {}),
            ((None,), {}),
            ((123,), {}),
            ((["x.db"],), {}),
            ((_SneakyStr(":memory:"),), {}),
            (("",), {}),
            (("   ",), {}),
            ((" :memory: ",), {}),
            ((":MEMORY:",), {}),
            ((":memory:x",), {}),
            (("FILE:memdb?mode=memory",), {"uri": True}),
            ((" file::memory:",), {"uri": True}),
            (("file://evilhost.example/x.db?mode=ro",), {"uri": True}),
            (("file://loc%61lhost/x.db?mode=ro",), {"uri": True}),
            (("file:////tmp/x.db?mode=ro",), {"uri": True}),
            (("file:legit?mode=memory\n",), {"uri": True}),
            (("file:radar.db?mode=memory",), {"uri": True}),
        ]

        for call_args, call_kwargs in adversarial_calls:
            mock_orig.reset_mock()
            with pytest.raises(HermeticGuardrailError):
                sqlite3.connect(*call_args, **call_kwargs)
            mock_orig.assert_not_called()

        with pytest.raises(HermeticGuardrailError):
            check_not_operational_path("data/radar.db", "sqlite3.connect", dir_fd=1, is_sqlite=True)

    assert fake_db.read_bytes() == b"synthetic-db"
    assert fake_env.read_text(encoding="utf-8") == "SYNTHETIC=1\n"


def test_sqlite_guard_mutable_pathlike_validation_delegation_consistency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PathLike.__fspath__() is resolved once so delegate receives the exact validated representation."""
    fake_repo = tmp_path / "synthetic_repo_pathlike"
    (fake_repo / "data").mkdir(parents=True)
    fake_db = fake_repo / "data" / "radar.db"

    class _FlippingPathLike:
        def __init__(self, first: object, second: object) -> None:
            self.calls = 0
            self._first = first
            self._second = second

        def __fspath__(self) -> object:
            self.calls += 1
            return self._first if self.calls == 1 else self._second

    class _BrokenPathLike:
        def __fspath__(self) -> object:
            raise RuntimeError("fspath failure")

    mock_orig = MagicMock()
    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", fake_repo)
        mp.setattr(conftest, "_ORIG_SQLITE_CONNECT", mock_orig)

        flip_pos = _FlippingPathLike(":memory:", str(fake_db))
        sqlite3.connect(flip_pos)
        assert flip_pos.calls == 1
        mock_orig.assert_called_once_with(":memory:")

        mock_orig.reset_mock()
        flip_kw = _FlippingPathLike(":memory:", str(fake_db))
        sqlite3.connect(database=flip_kw, uri=False)
        assert flip_kw.calls == 1
        mock_orig.assert_called_once_with(database=":memory:", uri=False)

        mock_orig.reset_mock()
        flip_bad = _FlippingPathLike(str(fake_db), ":memory:")
        with pytest.raises(HermeticGuardrailError):
            sqlite3.connect(flip_bad)
        assert flip_bad.calls == 1
        mock_orig.assert_not_called()

        for bad_pathlike in (_BrokenPathLike(), _FlippingPathLike(123, ":memory:")):
            mock_orig.reset_mock()
            with pytest.raises(HermeticGuardrailError):
                sqlite3.connect(bad_pathlike)
            mock_orig.assert_not_called()

    # Genuine SQLite confirmation with a mutable PathLike targeting a disposable temp DB
    temp_db = tmp_path / "consistent_temp.db"
    flip_real = _FlippingPathLike(str(temp_db), str(fake_db))
    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", fake_repo)
        conn = sqlite3.connect(flip_real)
        try:
            assert conn.execute("SELECT 1").fetchone() == (1,)
        finally:
            conn.close()
        assert flip_real.calls == 1
    assert temp_db.exists()
    assert not fake_db.exists()


def test_sqlite_policy_windows_and_posix_path_semantics_without_remote_probes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shared policy enforces Windows & POSIX path semantics without UNC/device/remote probes."""
    win_bad_cases: list[tuple[object, bool]] = [
        (r"\\evil.example\share\x.db", False),
        ("//evil.example/share/x.db", False),
        ("file://evil.example/share/x.db?mode=ro", True),
        ("file:////evil.example/share/x.db?mode=ro", True),
        ("file:%5c%5cevil.example%5cshare%5cx.db?mode=ro", True),
        (r"\\.\C:\x.db", False),
        (r"\\?\C:\x.db", False),
        (r"\\?\UNC\evil\share\x.db", False),
        (r"\\.\pipe\efso_pipe", False),
        ("CON", False),
        ("NUL", False),
        (r"C:\tmp\NUL", False),
        (r"C:\tmp\AUX.db", False),
        (r"C:data\radar.db", False),
        ("C:", False),
        ("file:C:data/radar.db?mode=ro", True),
        ("file:///C:data/radar.db?mode=ro", True),
        ("file:C%3Adata/radar.db?mode=ro", True),
        (r"\data\radar.db", False),
        ("/data/radar.db", False),
        ("file:/C:/data/radar.db?mode=ro", True),
        ("file:///data/radar.db?mode=ro", True),
        (r"C:\tmp\radar.db:stream", False),
        ("radar.db:hidden:$DATA", False),
        ("file:C:/tmp/radar.db:ads?mode=ro", True),
        ("file:radar.db%3Aads?mode=ro", True),
        (r"C:\tmp\data.\radar.db", False),
        (r"C:\tmp\radar.db.", False),
        (r"C:\tmp\data \radar.db", False),
        # Review #1 P1: Windows superscript COM/LPT aliases & pre-extension spaces (raw + percent-encoded)
        (r"C:\synthetic\COM¹.db", False),
        (r"C:\synthetic\COM².db", False),
        (r"C:\synthetic\COM³.db", False),
        (r"C:\synthetic\LPT¹.db", False),
        (r"C:\synthetic\LPT².db", False),
        (r"C:\synthetic\LPT³.db", False),
        ("com¹", False),
        ("lpt².db", False),
        ("COM¹ .db", False),
        ("LPT³ .db", False),
        (r"C:\synthetic\NUL .db", False),
        (r"C:\synthetic\CON .db", False),
        (r"C:\synthetic\PRN  .sqlite", False),
        (r"C:\synthetic\AUX .db", False),
        (r"C:\synthetic\CONIN$ .db", False),
        (r"C:\synthetic\CONOUT$ .db", False),
        (r"C:\synthetic\COM1 .db", False),
        (r"C:\synthetic\LPT9 .db", False),
        ("file:C:/synthetic/COM%C2%B9.db?mode=ro", True),
        ("file:C:/synthetic/COM%C2%B2.db?mode=ro", True),
        ("file:C:/synthetic/COM%C2%B3.db?mode=ro", True),
        ("file:C:/synthetic/LPT%C2%B9.db?mode=ro", True),
        ("file:C:/synthetic/LPT%C2%B2.db?mode=ro", True),
        ("file:C:/synthetic/LPT%C2%B3.db?mode=ro", True),
        ("file:C:/synthetic/NUL%20.db?mode=ro", True),
        ("file:C:/synthetic/CON%20%20.db?mode=ro", True),
        ("file:///C:/synthetic/LPT%C2%B2%20.db?mode=ro", True),
        # Review #1 P1: Windows reserved & ASCII control chars (raw + percent-encoded)
        (r"C:\synthetic\bad<name>.db", False),
        (r"C:\synthetic\bad>name.db", False),
        (r"C:\synthetic\bad|pipe.db", False),
        (r'C:\synthetic\bad"quote.db', False),
        (r"C:\synthetic\bad*star.db", False),
        (r"C:\synthetic\bad?q.db", False),
        ("C:\\synthetic\\bad\x01ctrl.db", False),
        ("C:\\synthetic\\bad\t.db", False),
        ("C:\\synthetic\\bad\n.db", False),
        ("C:\\synthetic\\bad\x1f.db", False),
        ("C:\\synthetic\\bad\x7f.db", False),
        ("file:C:/synthetic/bad%3Cname%3E.db?mode=ro", True),
        ("file:C:/synthetic/bad%7C.db?mode=ro", True),
        ("file:C:/synthetic/bad%2A.db?mode=ro", True),
        ("file:C:/synthetic/bad%3F.db?mode=ro", True),
        ("file:C:/synthetic/bad%22.db?mode=ro", True),
        ("file:C:/synthetic/bad%09.db?mode=ro", True),
        ("file:C:/synthetic/bad%0A.db?mode=ro", True),
        ("file:C:/synthetic/bad%1F.db?mode=ro", True),
        ("file:C:/synthetic/bad%7F.db?mode=ro", True),
    ]

    def _forbid_fs_resolution(*_args: object, **_kwargs: object) -> str:
        raise AssertionError("Filesystem/UNC resolution must not run for rejected syntax")

    with monkeypatch.context() as mp:
        mp.setattr(os.path, "realpath", _forbid_fs_resolution)
        mp.setattr(os.path, "abspath", _forbid_fs_resolution)
        for bad_target, uri_flag in win_bad_cases:
            with pytest.raises(SqliteGuardPolicyError):
                parse_sqlite_connect_call((bad_target,), {"uri": uri_flag}, is_windows=True)

    win_good_cases: list[tuple[str, bool, str]] = [
        (r"C:\tmp\test.db", False, r"C:\tmp\test.db"),
        ("C:/tmp/test.db", False, "C:/tmp/test.db"),
        (r"file:C:\tmp\test.db?mode=ro", True, r"C:\tmp\test.db"),
        ("file:C:/tmp/test.db?mode=ro", True, "C:/tmp/test.db"),
        ("file:///C:/tmp/test.db?mode=ro", True, "C:/tmp/test.db"),
        ("file://localhost/C:/tmp/test.db?mode=ro", True, "C:/tmp/test.db"),
        ("sub/test.db", False, "sub/test.db"),
        ("file:sub/test.db?mode=ro", True, "sub/test.db"),
        (r"C:\synthetic\normal.db", False, r"C:\synthetic\normal.db"),
        (r"C:\synthetic\console.db", False, r"C:\synthetic\console.db"),
        (r"C:\synthetic\my_com1.db", False, r"C:\synthetic\my_com1.db"),
        ("file:C:/synthetic/normal.db?mode=ro", True, "C:/synthetic/normal.db"),
        ("file:///C:/synthetic/normal%20copy.db?mode=ro", True, "C:/synthetic/normal copy.db"),
    ]
    for good_target, uri_flag, expected_path in win_good_cases:
        parsed = parse_sqlite_connect_call((good_target,), {"uri": uri_flag}, is_windows=True)
        assert parsed.kind == "disk"
        assert parsed.lexical_path == expected_path

    posix_good_cases: list[tuple[str, bool, str]] = [
        ("/tmp/test.db", False, "/tmp/test.db"),
        ("file:/tmp/test.db?mode=ro", True, "/tmp/test.db"),
        ("file:///tmp/test.db?mode=ro", True, "/tmp/test.db"),
        ("file://localhost/tmp/test.db?mode=ro", True, "/tmp/test.db"),
        ("file:///tmp/a%20b/test.db?mode=ro", True, "/tmp/a b/test.db"),
    ]
    for good_target, uri_flag, expected_path in posix_good_cases:
        parsed = parse_sqlite_connect_call((good_target,), {"uri": uri_flag}, is_windows=False)
        assert parsed.kind == "disk"
        assert parsed.lexical_path == expected_path

    for bad_posix in (
        "file://evil.example/tmp/x.db?mode=ro",
        "file://loc%61lhost/tmp/x.db?mode=ro",
        "file:////tmp/x.db?mode=ro",
    ):
        with pytest.raises(SqliteGuardPolicyError):
            parse_sqlite_connect_call((bad_posix,), {"uri": True}, is_windows=False)

    if os.name == "nt":
        fake_repo = tmp_path / "synthetic_repo_win_runtime"
        (fake_repo / "data").mkdir(parents=True)
        mock_orig = MagicMock()
        with monkeypatch.context() as mp:
            mp.setattr(conftest, "REPO_ROOT", fake_repo)
            mp.setattr(conftest, "_ORIG_SQLITE_CONNECT", mock_orig)
            mp.setattr(os.path, "realpath", _forbid_fs_resolution)
            mp.setattr(os.path, "abspath", _forbid_fs_resolution)
            for bad_target, uri_flag in win_bad_cases:
                mock_orig.reset_mock()
                with pytest.raises(HermeticGuardrailError):
                    sqlite3.connect(bad_target, uri=uri_flag)
                mock_orig.assert_not_called()


def test_sqlite_guard_lexical_traversal_and_bidirectional_symlink_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Shared guard blocks lexical traversal and symlink aliases in both directions before delegation."""
    fake_repo = tmp_path / "synthetic_repo_sym"
    fake_data = fake_repo / "data"
    fake_tests = fake_repo / "tests"
    safe_dir = tmp_path / "safe_dir"
    for d in (fake_data, fake_tests, safe_dir):
        d.mkdir(parents=True)

    target_data_db = fake_data / "radar.db"
    outside_db = safe_dir / "outside.db"
    link_into_op = safe_dir / "link.db"
    alias_in_op = fake_data / "alias.db"

    orig_realpath = os.path.realpath
    realpath_map = {
        os.path.normcase(os.path.abspath(os.fspath(link_into_op))): str(target_data_db.resolve()),
        os.path.normcase(os.path.abspath(os.fspath(alias_in_op))): str(outside_db.resolve()),
    }

    def _mock_realpath(path: object, *, strict: bool = False) -> object:
        candidate = os.path.normcase(os.path.abspath(os.fspath(path)))  # type: ignore[arg-type]
        if candidate in realpath_map:
            return realpath_map[candidate]
        return orig_realpath(path)  # type: ignore[arg-type]

    def _to_file_uri(p: Path) -> str:
        raw = f"file:{os.path.abspath(p).replace(os.sep, '/')}"
        return raw if raw.startswith("file:///") else f"file:///{os.path.abspath(p).replace(os.sep, '/').lstrip('/')}"

    mock_orig = MagicMock()
    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", fake_repo)
        mp.setattr(conftest, "_ORIG_SQLITE_CONNECT", mock_orig)
        mp.chdir(fake_tests)

        for traversal_call, traversal_kw in (
            (("../data/radar.db",), {}),
            (("file:../data/radar.db?mode=ro",), {"uri": True}),
            (("file:%2e%2e/data/radar.db?mode=ro",), {"uri": True}),
        ):
            mock_orig.reset_mock()
            with pytest.raises(HermeticGuardrailError):
                sqlite3.connect(*traversal_call, **traversal_kw)
            mock_orig.assert_not_called()

        mp.setattr(os.path, "realpath", _mock_realpath)
        for bad_target, uri_flag in (
            (link_into_op, False),
            (f"{_to_file_uri(link_into_op)}?mode=ro", True),
            (alias_in_op, False),
            (f"{_to_file_uri(alias_in_op)}?mode=ro", True),
        ):
            mock_orig.reset_mock()
            with pytest.raises(HermeticGuardrailError):
                sqlite3.connect(bad_target, uri=uri_flag)
            mock_orig.assert_not_called()


def test_sqlite_guard_real_bidirectional_symlinks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real OS symlinks in both directions (temp -> data and data -> outside) fail closed pre-delegate."""
    fake_repo = tmp_path / "synthetic_repo_real_sym"
    fake_data = fake_repo / "data"
    safe_dir = tmp_path / "safe_real_dir"
    fake_data.mkdir(parents=True)
    safe_dir.mkdir(parents=True)

    real_data_db = fake_data / "radar.db"
    real_data_db.write_bytes(b"synthetic-data")
    real_outside_db = safe_dir / "outside.db"
    real_outside_db.write_bytes(b"synthetic-outside")

    link_to_data = safe_dir / "link_to_data.db"
    alias_in_data = fake_data / "alias_to_outside.db"
    try:
        os.symlink(real_data_db, link_to_data)
        os.symlink(real_outside_db, alias_in_data)
    except OSError as exc:
        pytest.skip(f"OS symlink creation unavailable in this environment: {exc}")

    def _to_lexical_uri(p: Path) -> str:
        raw = os.path.abspath(p).replace(os.sep, "/")
        return f"file:///{raw.lstrip('/')}?mode=ro"

    mock_orig = MagicMock()
    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", fake_repo)
        mp.setattr(conftest, "_ORIG_SQLITE_CONNECT", mock_orig)

        for target, uri_flag in (
            (link_to_data, False),
            (_to_lexical_uri(link_to_data), True),
            (alias_in_data, False),
            (_to_lexical_uri(alias_in_data), True),
        ):
            mock_orig.reset_mock()
            with pytest.raises(HermeticGuardrailError):
                sqlite3.connect(target, uri=uri_flag)
            mock_orig.assert_not_called()


def test_sqlite_guard_real_memory_and_temp_disk_positive_controls_in_disposable_dir() -> None:
    """Real SQLite allows canonical memory & temp disk connections while rejecting Review #2 NUL bypasses."""
    with tempfile.TemporaryDirectory(prefix="efso_t1d_real_sqlite_") as disp_td:
        disp_dir = Path(disp_td)
        sub_dir = disp_dir / "sub"
        sub_dir.mkdir()
        seed_db = (disp_dir / "seed.db").resolve()
        nested_db = (sub_dir / "nested.db").resolve()
        rw_db = (disp_dir / "rw_target.db").resolve()

        for db_file, val in ((seed_db, 1), (nested_db, 2)):
            init_conn = sqlite3.connect(db_file)
            try:
                init_conn.execute("CREATE TABLE t (v INT)")
                init_conn.execute("INSERT INTO t (v) VALUES (?)", (val,))
                init_conn.commit()
            finally:
                init_conn.close()

        orig_cwd = os.getcwd()
        os.chdir(disp_dir)
        try:
            mem_calls: list[tuple[tuple[object, ...], dict[str, object]]] = [
                ((":memory:",), {}),
                ((":memory:",), {"uri": True}),
                ((b":memory:",), {}),
                ((b":memory:",), {"uri": True}),
                ((":memory:", 5.0, 0, "", True, sqlite3.Connection, 128, True), {}),
            ]
            for canon_uri in sorted(_CANONICAL_MEMORY_URIS):
                mem_calls.append(((canon_uri,), {"uri": True}))
            mem_calls.extend([
                (("file:legit?mode=memory",), {"uri": True}),
                (("file:synth_mem?mode=memory&cache=shared",), {"uri": True}),
                (("file:synth_mem?cache=private&mode=memory",), {"uri": True}),
            ])

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                for m_args, m_kwargs in mem_calls:
                    conn = sqlite3.connect(*m_args, **m_kwargs)
                    try:
                        db_list = conn.execute("PRAGMA database_list").fetchall()
                        assert db_list[0][2] == ""
                    finally:
                        conn.close()

            assert not (disp_dir / "legit").exists()
            assert not (disp_dir / "synth_mem").exists()

            for bad_name, bad_uri in (
                ("synth", "file:synth?mode=memory&mode%00=rwc"),
                ("synth2", "file:synth2?mode=memory&mode%00junk=rwc"),
                ("synth3", "file:synth3?mode=memory&mo%64e%00=rwc"),
            ):
                with pytest.raises(HermeticGuardrailError):
                    sqlite3.connect(bad_uri, uri=True)
                assert not (disp_dir / bad_name).exists()

            localhost_ro_uri = f"file://localhost/{seed_db.as_posix().lstrip('/')}?mode=ro"
            ro_calls: list[tuple[object, dict[str, object], int]] = [
                (seed_db, {}, 1),
                (str(seed_db), {}, 1),
                (os.fsencode(seed_db), {}, 1),
                (f"file:{seed_db}?mode=ro", {"uri": True}, 1),
                (f"file:{seed_db.as_posix()}?mode=ro", {"uri": True}, 1),
                (f"{seed_db.as_uri()}?mode=ro", {"uri": True}, 1),
                (localhost_ro_uri, {"uri": True}, 1),
                ("file:sub/nested.db?mode=ro", {"uri": True}, 2),
                ("file:sub/nested.db?mode=rw&cache=shared&immutable=1", {"uri": True}, 2),
            ]
            for target, kw, expected_val in ro_calls:
                conn = sqlite3.connect(target, **kw)
                try:
                    assert conn.execute("SELECT v FROM t").fetchone() == (expected_val,)
                finally:
                    conn.close()

            rwc_conn = sqlite3.connect(f"file:{rw_db.as_posix()}?mode=rwc", uri=True)
            try:
                rwc_conn.execute("CREATE TABLE created_ok (id INTEGER PRIMARY KEY)")
                rwc_conn.commit()
            finally:
                rwc_conn.close()
            assert rw_db.exists()
        finally:
            os.chdir(orig_cwd)


def test_lexical_forbidden_targets_reject_without_calling_realpath(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Obviously forbidden lexical operational paths reject BEFORE calling os.path.realpath or delegates."""
    fake_repo = tmp_path / "synthetic_repo_lex"
    fake_data = fake_repo / "data"
    fake_tests = fake_repo / "tests"
    fake_data.mkdir(parents=True)
    fake_tests.mkdir(parents=True)

    fake_db = fake_data / "radar.db"
    fake_state = fake_data / "state.json"
    fake_env = fake_repo / ".env"
    fake_alias = fake_data / "alias.db"
    traversal_db = fake_tests / ".." / "data" / "radar.db"
    abs_db_uri = f"file:///{os.path.abspath(fake_db).replace(os.sep, '/').lstrip('/')}?mode=ro"

    mock_sqlite = MagicMock()
    mock_io_open = MagicMock()
    mock_os_open = MagicMock()
    mock_unlink = MagicMock()
    mock_remove = MagicMock()
    mock_rmtree = MagicMock()

    realpath_calls: list[str] = []

    def _forbid_realpath(path: object, *, strict: bool = False) -> str:
        realpath_calls.append(os.fspath(path))  # type: ignore[arg-type]
        raise AssertionError(f"os.path.realpath must not run for obvious lexical rejection: {path!r}")

    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", fake_repo)
        mp.setattr(conftest, "_ORIG_SQLITE_CONNECT", mock_sqlite)
        mp.setattr(conftest, "_ORIG_IO_OPEN", mock_io_open)
        mp.setattr(conftest, "_ORIG_OS_OPEN", mock_os_open)
        mp.setattr(conftest, "_ORIG_OS_UNLINK", mock_unlink)
        mp.setattr(conftest, "_ORIG_OS_REMOVE", mock_remove)
        mp.setattr(conftest, "_ORIG_SHUTIL_RMTREE", mock_rmtree)
        mp.chdir(fake_repo)
        mp.setattr(os.path, "realpath", _forbid_realpath)

        # Shared SQLite lexical rejections (direct, traversal, URI, relative, lexical alias in data/)
        for bad_target, uri_flag in (
            (fake_db, False),
            (traversal_db, False),
            (abs_db_uri, True),
            ("data/radar.db", False),
            ("file:data/radar.db?mode=ro", True),
            (fake_alias, False),
        ):
            mock_sqlite.reset_mock()
            with pytest.raises(HermeticGuardrailError):
                sqlite3.connect(bad_target, uri=uri_flag)
            mock_sqlite.assert_not_called()

        # Shared non-SQLite file & destructive lexical rejections
        with pytest.raises(HermeticGuardrailError):
            open(fake_state, "r", encoding="utf-8")
        with pytest.raises(HermeticGuardrailError):
            open(fake_env, "r", encoding="utf-8")
        with pytest.raises(HermeticGuardrailError):
            io.open(fake_db, "rb")
        with pytest.raises(HermeticGuardrailError):
            os.open(fake_env, os.O_RDONLY)
        with pytest.raises(HermeticGuardrailError):
            os.unlink(fake_state)
        with pytest.raises(HermeticGuardrailError):
            os.remove(fake_env)
        with pytest.raises(HermeticGuardrailError):
            shutil.rmtree(fake_data)

        assert realpath_calls == []
        mock_sqlite.assert_not_called()
        mock_io_open.assert_not_called()
        mock_os_open.assert_not_called()
        mock_unlink.assert_not_called()
        mock_remove.assert_not_called()
        mock_rmtree.assert_not_called()

    # Stage 2 verification: if REPO_ROOT itself is a symlink alias, only REPO_ROOT is resolved —
    # never the candidate pointing inside real_repo/data/.
    repo_link = tmp_path / "repo_link"
    real_repo = tmp_path / "real_repo"
    (real_repo / "data").mkdir(parents=True)
    real_repo_db = real_repo / "data" / "radar.db"
    norm_repo_link = os.path.normcase(os.path.abspath(os.fspath(repo_link)))
    stage2_realpath_calls: list[str] = []

    def _spy_repo_only_realpath(path: object, *, strict: bool = False) -> str:
        norm_p = os.path.normcase(os.path.abspath(os.fspath(path)))  # type: ignore[arg-type]
        stage2_realpath_calls.append(norm_p)
        if norm_p == norm_repo_link:
            return os.path.abspath(os.fspath(real_repo))
        raise AssertionError(f"Candidate path must not be passed to os.path.realpath: {path!r}")

    with monkeypatch.context() as mp:
        mp.setattr(conftest, "REPO_ROOT", repo_link)
        mp.setattr(conftest, "_ORIG_SQLITE_CONNECT", mock_sqlite)
        mp.setattr(os.path, "realpath", _spy_repo_only_realpath)

        mock_sqlite.reset_mock()
        with pytest.raises(HermeticGuardrailError):
            sqlite3.connect(real_repo_db)
        mock_sqlite.assert_not_called()
        assert stage2_realpath_calls == [norm_repo_link]
