"""Negative controls and non-interference tests for `tests/conftest.py` (Issue #104 / T1B)."""
from __future__ import annotations

import http.client
import io
import os
import shutil
import socket
import sqlite3
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
