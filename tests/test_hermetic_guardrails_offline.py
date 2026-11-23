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
from pathlib import Path

import httpx
import pytest

import sys

from efso_radar.repositories.sqlite import SqliteStateRepository

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
    """Guardrail module explicitly discloses collection/import, subprocess, fd, and stat limitations."""
    joined = " ".join(RESIDUAL_LIMITATIONS).lower()
    assert "collection" in joined
    assert "subprocess" in joined
    assert "file descriptors" in joined
    assert "stat()" in joined or "exists()" in joined


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
