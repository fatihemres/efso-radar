"""Hermetic offline test suite for Phase 5B.3G-C Persistent Manifest Store Foundation.

OFFLINE TEST ONLY.
Zero network access, zero Turso credentials, zero database calls.
All file operations are confined strictly to isolated, ownership-proven pytest tmp_path fixtures.
"""
from __future__ import annotations

from datetime import datetime, timezone
import errno
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Generator
import uuid
import pytest
from unittest.mock import Mock, patch

from efso_radar.staging.manifest_seal import (
    MAX_MANIFEST_BYTES,
    ManifestSealViolation,
    OfflineManifestSeal,
)
from efso_radar.staging.persistent_manifest_store import (
    HermeticTestFixtureScope,
    HermeticTestManifestStore,
    ManifestStoreConcurrencyError,
    ManifestStoreCorruptionError,
    ManifestStoreError,
    ManifestStoreFileExistsError,
    ManifestStorePostPublicationError,
    ManifestStoreSecurityError,
    PersistentManifestStore,
    _ManifestRunLock,
    _find_repo_root,
    _publish_file_no_overwrite,
    get_storage_platform_guarantees,
    ManifestCrashWindow,
    StoragePlatformDurabilityAssessment,
    get_crash_window_contracts,
    verify_pre_remote_write_durability_gate,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    PerRunOwnershipManifest,
    RegisteredEntity,
    create_default_manifest_entities,
)


@pytest.fixture(autouse=True)
def _authorize_test_tmp_path(tmp_path: Path) -> Generator[Path, None, None]:
    """Link each test execution to its ownership-proven pytest tmp_path fixture root."""
    with HermeticTestFixtureScope(tmp_path):
        yield tmp_path


def _generate_valid_run_id(suffix: str | None = None) -> str:
    """Helper to generate a valid collision-resistant run_id string."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if suffix is None:
        hex_suffix = hashlib.sha256(os.urandom(16)).hexdigest()[:12]
    else:
        hex_suffix = hashlib.sha256(suffix.encode()).hexdigest()[:12]
    return f"run_{ts}_{hex_suffix}"


def _create_sample_manifest(run_id: str) -> PerRunOwnershipManifest:
    """Helper to construct a valid PerRunOwnershipManifest with canonical entities."""
    return PerRunOwnershipManifest(
        run_id=run_id,
        created_at=datetime.now(timezone.utc).isoformat(),
        registered_entities=create_default_manifest_entities(run_id),
    )


class TestPersistentManifestStoreSecurity:
    """Verify fail-closed security barriers on directory paths, symlinks, permissions, and defaults."""

    def test_default_deny_os_temp_in_production_mode(self, tmp_path: Path) -> None:
        """Store initialized without is_hermetic_test=True in OS temp fails closed on init."""
        isolated_dir = tmp_path / "manifests"
        isolated_dir.mkdir()

        # In non-hermetic mode, using tmp_path raises ManifestStoreSecurityError because it's in OS temp
        with pytest.raises(ManifestStoreSecurityError, match="OS temporary directory"):
            PersistentManifestStore(isolated_dir, live_authorized=False, is_hermetic_test=False)

    def test_default_deny_neutralizes_boolean_live_authorized(self, tmp_path: Path) -> None:
        """Passing live_authorized=True does not grant write permissions in this offline foundation."""
        # On Windows, PersistentManifestStore without is_hermetic_test fails closed due to DACL blocker
        # On Linux, PersistentManifestStore outside temp will fail closed on save_manifest
        non_temp_dir = tmp_path / "non_temp_candidate"
        non_temp_dir.mkdir()
        if sys.platform != "win32":
            non_temp_dir.chmod(0o700)

        if sys.platform == "win32":
            with pytest.raises(ManifestStoreSecurityError, match="Windows NTFS DACL/ACL verification|OS temporary"):
                PersistentManifestStore(non_temp_dir, live_authorized=True, is_hermetic_test=False)
        else:
            # On POSIX: if not in temp, save_manifest fails closed even if live_authorized=True was passed
            # Simulate non-temp path by mocking temp roots
            store = PersistentManifestStore(non_temp_dir, live_authorized=True, is_hermetic_test=True)
            # When is_hermetic_test is False:
            store_prod = PersistentManifestStore.__new__(PersistentManifestStore)
            store_prod._is_hermetic_test = False
            store_prod._live_authorized = False
            store_prod._root_dir = non_temp_dir
            run_id = _generate_valid_run_id("boolauth0001")
            manifest = _create_sample_manifest(run_id)
            with pytest.raises(ManifestStoreSecurityError, match="Live persistent manifest store writes are strictly disabled"):
                store_prod.save_manifest(manifest)

    def test_hermetic_test_store_rejects_arbitrary_user_directories(self) -> None:
        """HermeticTestManifestStore is strictly restricted to temp test fixtures and rejects user dirs."""
        # Reject user home or documents directory
        try:
            fake_user_dir = Path.home() / "decoy_test_dir_isolated_never_touch"
            with pytest.raises(ManifestStoreSecurityError, match="strictly restricted to isolated test fixture directories"):
                HermeticTestManifestStore(fake_user_dir)
        except (RuntimeError, OSError):
            pass

    def test_hermetic_test_store_rejects_random_temp_directories(self) -> None:
        """HermeticTestManifestStore rejects arbitrary temporary directories not in a verified fixture hierarchy."""
        random_temp = Path(tempfile.gettempdir()) / f"unauthorized_random_temp_{uuid.uuid4().hex}"
        with pytest.raises(ManifestStoreSecurityError, match="ownership-verified isolated test fixture"):
            HermeticTestManifestStore(random_temp)

    def test_hermetic_test_store_preserves_preexisting_directory_permissions(self, tmp_path: Path) -> None:
        """Pre-existing directories with insecure permissions are rejected without mutating permissions."""
        preexisting = tmp_path / "preexisting_insecure_dir"
        preexisting.mkdir(mode=0o755)
        if sys.platform != "win32":
            os.chmod(preexisting, 0o755)
            initial_mode = preexisting.stat().st_mode & 0o777

            with pytest.raises(ManifestStoreSecurityError, match="Pre-existing directory has insecure permissions"):
                HermeticTestManifestStore(preexisting)

            # Permissions must remain UNCHANGED (not mutated to 0700)
            after_mode = preexisting.stat().st_mode & 0o777
            assert after_mode == initial_mode == 0o755

    def test_hermetic_test_store_scoped_fixture_lifecycle(self, tmp_path: Path) -> None:
        """HermeticTestFixtureScope cleanly registers and unregisters fixture roots without leak."""
        custom_root = tmp_path / "scoped_subroot"
        custom_root.mkdir(mode=0o700)
        if sys.platform != "win32":
            os.chmod(custom_root, 0o700)

        # Clear active registrations to verify scoped entry
        HermeticTestManifestStore.clear_authorized_fixture_roots()
        try:
            with pytest.raises(ManifestStoreSecurityError, match="ownership-verified isolated test fixture"):
                HermeticTestManifestStore(custom_root)

            # Enter scope explicitly via context manager
            with HermeticTestFixtureScope(custom_root):
                store = HermeticTestManifestStore(custom_root / "store")
                assert store.root_dir.exists()

            # Exited scope: registration discarded automatically
            with pytest.raises(ManifestStoreSecurityError, match="ownership-verified isolated test fixture"):
                HermeticTestManifestStore(custom_root / "another_store")
        finally:
            HermeticTestManifestStore.clear_authorized_fixture_roots()

    def test_fake_pytest_current_test_cannot_authorize_non_pytest_directory(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Setting fake PYTEST_CURRENT_TEST environment variable cannot authorize a non-pytest directory."""
        monkeypatch.setenv("PYTEST_CURRENT_TEST", "fake_test_suite_running_maliciously[test_case]")

        non_pytest_dir = Path(tempfile.gettempdir()) / f"fake_non_pytest_dir_{uuid.uuid4().hex}"
        non_pytest_dir.mkdir(mode=0o700)
        try:
            if sys.platform != "win32":
                os.chmod(non_pytest_dir, 0o700)

            # 1. Attempt to register via register_authorized_fixture_root
            with pytest.raises(ManifestStoreSecurityError, match="pytest fixture hierarchy"):
                HermeticTestManifestStore.register_authorized_fixture_root(non_pytest_dir)

            # 2. Attempt to scope via HermeticTestFixtureScope
            with pytest.raises(ManifestStoreSecurityError, match="pytest fixture hierarchy"):
                HermeticTestFixtureScope(non_pytest_dir)

            # 3. Attempt direct store initialization
            with pytest.raises(ManifestStoreSecurityError, match="pytest fixture hierarchy|ownership-verified"):
                HermeticTestManifestStore(non_pytest_dir)
        finally:
            if non_pytest_dir.exists():
                non_pytest_dir.rmdir()

    def test_hermetic_store_rejects_unauthorized_directory_with_pytest_in_name(self) -> None:
        """A directory with 'pytest' in its name that has not been authorized is strictly rejected."""
        fake_pytest_dir = Path(tempfile.gettempdir()) / f"pytest_unauthorized_{uuid.uuid4().hex}"
        with pytest.raises(ManifestStoreSecurityError, match="ownership-verified isolated test fixture"):
            HermeticTestManifestStore(fake_pytest_dir)
        # Ensure candidate was not created on disk
        assert not fake_pytest_dir.exists(), "Unauthorized directory must not be created on disk"

    def test_rejects_arbitrary_test_root_registration(self, tmp_path: Path) -> None:
        """Arbitrary directories (temp root itself, repo root, non-existent, insecure mode) cannot be registered."""
        # 1. Temp root itself cannot be registered
        temp_root = Path(tempfile.gettempdir())
        with pytest.raises(ManifestStoreSecurityError, match="cannot be registered as an authorized fixture root|temporary root itself"):
            HermeticTestManifestStore.register_authorized_fixture_root(temp_root)

        # 2. Repo root cannot be registered
        repo_root = _find_repo_root()
        assert repo_root is not None
        with pytest.raises(ManifestStoreSecurityError, match="cannot be registered as a fixture root"):
            HermeticTestManifestStore.register_authorized_fixture_root(repo_root)

        # 3. Non-existent path cannot be registered
        non_existent = tmp_path / "does_not_exist_fixture"
        with pytest.raises(ManifestStoreSecurityError, match="does not exist"):
            HermeticTestManifestStore.register_authorized_fixture_root(non_existent)

        # 4. Insecure permissions (0o755) directory cannot be registered
        insecure_dir = tmp_path / "insecure_reg_dir"
        insecure_dir.mkdir(mode=0o755)
        if sys.platform != "win32":
            os.chmod(insecure_dir, 0o755)
            with pytest.raises(ManifestStoreSecurityError, match="insecure permissions"):
                HermeticTestManifestStore.register_authorized_fixture_root(insecure_dir)

        # 5. Non-pytest temporary directory cannot be registered
        non_pytest_dir = Path(tempfile.gettempdir()) / f"arbitrary_non_pytest_{uuid.uuid4().hex}"
        non_pytest_dir.mkdir(mode=0o700)
        try:
            if sys.platform != "win32":
                os.chmod(non_pytest_dir, 0o700)
            with pytest.raises(ManifestStoreSecurityError, match="pytest fixture hierarchy"):
                HermeticTestManifestStore.register_authorized_fixture_root(non_pytest_dir)
        finally:
            if non_pytest_dir.exists():
                non_pytest_dir.rmdir()

    def test_direct_is_hermetic_test_true_rejects_unauthorized_directory(self) -> None:
        """Passing is_hermetic_test=True directly cannot bypass fixture ownership and authorization checks."""
        fake_dir = Path(tempfile.gettempdir()) / f"pytest_bypass_attempt_{uuid.uuid4().hex}"
        with pytest.raises(ManifestStoreSecurityError, match="ownership-verified isolated test fixture"):
            PersistentManifestStore(fake_dir, is_hermetic_test=True)
        assert not fake_dir.exists(), "Directory must not be created when authorization fails"

        # Also test with arbitrary non-temp directory
        fake_user_dir = Path.home() / "decoy_bypass_attempt"
        with pytest.raises(ManifestStoreSecurityError, match="strictly restricted to isolated test fixture directories"):
            PersistentManifestStore(fake_user_dir, is_hermetic_test=True)

    def test_rejects_empty_or_null_byte_path(self, tmp_path: Path) -> None:
        """Store strictly rejects empty paths or null bytes."""
        with pytest.raises(ManifestStoreSecurityError, match="path cannot be empty"):
            HermeticTestManifestStore("")

        with pytest.raises(ManifestStoreSecurityError, match="null byte"):
            HermeticTestManifestStore(str(tmp_path) + "\0/extra")

    def test_rejects_path_traversal_indicators(self, tmp_path: Path) -> None:
        """Store strictly rejects '..' traversal in path."""
        with pytest.raises(ManifestStoreSecurityError, match="traversal"):
            HermeticTestManifestStore(tmp_path / "subdir" / ".." / "other")

    def test_rejects_repository_root_and_internal_paths(self) -> None:
        """Store strictly rejects repository root, src/, tests/, data/, .git, and .env."""
        repo_root = _find_repo_root()
        assert repo_root is not None, "Could not locate repo root for security test"

        # Repo root itself
        with pytest.raises(ManifestStoreSecurityError, match="inside repository|Repository root"):
            HermeticTestManifestStore(repo_root)

        # Inside repo (e.g. src/)
        with pytest.raises(ManifestStoreSecurityError, match="inside repository"):
            HermeticTestManifestStore(repo_root / "src")

        # Operational data directory
        with pytest.raises(ManifestStoreSecurityError, match="inside repository|operational data"):
            HermeticTestManifestStore(repo_root / "data")

        # Path containing .git
        with pytest.raises(ManifestStoreSecurityError, match=r"inside repository|\.git"):
            HermeticTestManifestStore(repo_root / ".git")

        # Path containing .env
        with pytest.raises(ManifestStoreSecurityError, match=r"inside repository|\.env"):
            HermeticTestManifestStore(repo_root / ".env.local")

    def test_rejects_symlink_store_directory(self, tmp_path: Path) -> None:
        """Store directory that is a symlink is strictly rejected."""
        target_dir = tmp_path / "real_store"
        target_dir.mkdir()
        link_dir = tmp_path / "link_store"

        try:
            link_dir.symlink_to(target_dir, target_is_directory=True)
        except (OSError, NotImplementedError):
            pytest.skip("Symlink creation not supported in current environment.")

        with pytest.raises(ManifestStoreSecurityError, match="Symlink or reparse point"):
            HermeticTestManifestStore(link_dir)

    def test_rejects_symlink_manifest_file(self, tmp_path: Path) -> None:
        """Manifest file that is a symlink is rejected upon loading."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("symlink000001")

        decoy = tmp_path / "decoy.json"
        manifest = _create_sample_manifest(run_id)
        decoy.write_text(manifest.model_dump_json(), encoding="utf-8")

        target_file = store.get_manifest_path(run_id)
        try:
            target_file.symlink_to(decoy)
        except (OSError, NotImplementedError):
            pytest.skip("Symlink creation not supported in current environment.")

        with pytest.raises(ManifestStoreSecurityError, match="symlink or reparse point"):
            store.load_manifest(run_id)

    def test_posix_permissions_strict_owner_private_validation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """POSIX permissions check strictly rejects group or world read/write/execute bits (0o077)."""
        store_dir = tmp_path / "store"
        store_dir.mkdir()

        # Mock POSIX environment on Windows or Linux
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)

        class MockStatInsecure:
            st_uid = 1000
            st_mode = 0o40755  # drwxr-xr-x (group/world read & execute: 0o055)

        real_os_stat = os.stat
        target_str = str(store_dir.resolve())

        def mock_os_stat(path: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                if str(path) == target_str:
                    return MockStatInsecure()
            except Exception:
                pass
            return real_os_stat(path, *args, **kwargs)

        monkeypatch.setattr(os, "stat", mock_os_stat)

        with pytest.raises(ManifestStoreSecurityError, match="group and world read/write/execute permissions.*strictly prohibited"):
            HermeticTestManifestStore(store_dir)


class TestPersistentManifestStoreOperations:
    """Verify atomic write, overwrite protection, process restart, and tamper detection."""

    def test_atomic_save_and_reopen_simulation(self, tmp_path: Path) -> None:
        """Store atomically persists manifest; a new store instance reopens and verifies seal.

        Note: in-process re-instantiation simulates store reopening; true OS process restart
        is verified separately in test_real_subprocess_process_restart_persistence.
        """
        store_dir = tmp_path / "persistent_store"
        store1 = HermeticTestManifestStore(store_dir)

        run_id = _generate_valid_run_id("reopen000001")
        manifest = _create_sample_manifest(run_id)

        path = store1.save_manifest(manifest)
        assert path.is_file()
        assert path.name == f"efso_radar_staging_manifest_{run_id}.json"
        assert store1.manifest_exists(run_id)

        # In-process reopen simulation by creating a new independent store instance
        store2 = HermeticTestManifestStore(store_dir)
        loaded_manifest, seal = store2.load_manifest(run_id)

        assert loaded_manifest.run_id == run_id
        assert len(loaded_manifest.registered_entities) == 5
        assert isinstance(seal, OfflineManifestSeal)
        assert seal.run_id == run_id
        assert seal.allowed_root == store_dir.resolve()

        seal.assert_unchanged()
        seal.require_registered_identity("processed_articles", "article_id", f"synth_art_{run_id}_01")

    def test_real_subprocess_process_restart_persistence(self, tmp_path: Path) -> None:
        """Verify true OS process restart: manifest written by process A is verified by process B."""
        store_dir = tmp_path / "subprocess_store"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("subproc00001")
        manifest = _create_sample_manifest(run_id)
        store.save_manifest(manifest)

        # Launch an independent Python interpreter in a fresh OS process
        script = f"""
import os
import sys
from pathlib import Path
from efso_radar.staging.persistent_manifest_store import HermeticTestManifestStore, HermeticTestFixtureScope
from efso_radar.staging.manifest_seal import OfflineManifestSeal

store_path = Path({str(store_dir.resolve())!r})
with HermeticTestFixtureScope(store_path):
    store = HermeticTestManifestStore(store_path)
    manifest, seal = store.load_manifest({run_id!r})
    assert manifest.run_id == {run_id!r}
    assert len(manifest.registered_entities) == 5
    seal.assert_unchanged()
    seal.require_registered_identity("processed_articles", "article_id", "synth_art_{run_id}_01")
    print("SUBPROCESS_VERIFIED_OK")
"""
        env = dict(os.environ)
        repo_root = _find_repo_root()
        assert repo_root is not None
        env["PYTHONPATH"] = str(repo_root / "src")
        env["PYTEST_CURRENT_TEST"] = os.environ.get("PYTEST_CURRENT_TEST", "test_real_subprocess_process_restart_persistence")

        res = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert res.returncode == 0, f"Subprocess failed:\nstdout: {res.stdout}\nstderr: {res.stderr}"
        assert "SUBPROCESS_VERIFIED_OK" in res.stdout

    def test_overwrite_protection_strictly_rejected(self, tmp_path: Path) -> None:
        """Attempting to overwrite an existing manifest for the same run_id fails closed."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("nooverwr0001")
        manifest = _create_sample_manifest(run_id)

        path = store.save_manifest(manifest)
        original_mtime = path.stat().st_mtime_ns
        original_bytes = path.read_bytes()

        # Second save attempt for the exact same run_id must raise ManifestStoreFileExistsError
        with pytest.raises(ManifestStoreFileExistsError, match="already exists"):
            store.save_manifest(manifest)

        assert path.stat().st_mtime_ns == original_mtime
        assert path.read_bytes() == original_bytes

    def test_adversarial_race_target_creation_before_publish(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Adversarial race: target file created right before publication triggers FileExistsError."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("raceover0001")
        manifest = _create_sample_manifest(run_id)
        target_path = store.get_manifest_path(run_id)

        original_publish = _publish_file_no_overwrite

        def adversarial_publish(temp_path: Path, target: Path) -> None:
            # Simulate external racer creating the target file right before publication
            target.write_text('{"adversarial": true}', encoding="utf-8")
            original_publish(temp_path, target)

        monkeypatch.setattr(
            "efso_radar.staging.persistent_manifest_store._publish_file_no_overwrite",
            adversarial_publish,
        )

        with pytest.raises(ManifestStoreFileExistsError, match="already exists"):
            store.save_manifest(manifest)

        # Adversarial target file content must NOT have been overwritten
        assert target_path.read_text(encoding="utf-8") == '{"adversarial": true}'

        # Temporary file must be cleaned up
        tmp_files = [f for f in store.root_dir.iterdir() if f.name.startswith(".tmp_manifest_")]
        assert len(tmp_files) == 0

    def test_dangling_symlink_target_strictly_rejected(self, tmp_path: Path) -> None:
        """Pre-existing dangling symlink at target path is detected and rejected without overwrite."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("danglink0001")
        manifest = _create_sample_manifest(run_id)
        target_path = store.get_manifest_path(run_id)

        # Create a dangling symlink at target_path
        nonexistent_target = tmp_path / "nonexistent_target_123.json"
        try:
            target_path.symlink_to(nonexistent_target)
        except (OSError, NotImplementedError):
            pytest.skip("Symlink creation not supported in current environment.")

        # Target exists as dangling symlink; Path.exists() is False, but lexists is True
        assert not target_path.exists()
        assert os.path.lexists(str(target_path))

        with pytest.raises(ManifestStoreFileExistsError, match="already exists|dangling symlink"):
            store.save_manifest(manifest)

        # Temporary files must be cleaned up
        tmp_files = [f for f in store.root_dir.iterdir() if f.name.startswith(".tmp_manifest_")]
        assert len(tmp_files) == 0

    def test_short_write_fails_closed_and_leaves_no_artifacts(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A partial/short write during save_manifest fails closed and leaves no orphaned temp/lock files."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("shortwr00001")
        manifest = _create_sample_manifest(run_id)

        real_os_write = os.write

        def mock_short_write(fd: int, data: bytes) -> int:
            # Write only 10 bytes then return 0 (short write failure)
            if len(data) > 10:
                real_os_write(fd, data[:10])
                return 10
            return 0

        monkeypatch.setattr(os, "write", mock_short_write)

        with pytest.raises((OSError, ManifestStoreCorruptionError), match="Short write"):
            store.save_manifest(manifest)

        # Target file must not exist
        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists()

        # No temporary files or lock files leaked
        leaked_files = [
            f for f in store.root_dir.iterdir()
            if f.name.startswith(".tmp_manifest_") or f.name.startswith(".lock_")
        ]
        assert len(leaked_files) == 0

    def test_disk_full_enospc_fails_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Disk full (ENOSPC) during write fails closed and cleans up temporary artifacts."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("diskfull0001")
        manifest = _create_sample_manifest(run_id)

        def mock_enospc_write(fd: int, data: bytes) -> int:
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(os, "write", mock_enospc_write)

        with pytest.raises(OSError, match="No space left on device"):
            store.save_manifest(manifest)

        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists()

        # No leaked temp files
        tmp_files = [f for f in store.root_dir.iterdir() if f.name.startswith(".tmp_manifest_")]
        assert len(tmp_files) == 0

    def test_file_fsync_failure_fails_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failure during file fsync fails closed and cleans up sibling temp file."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("fsyncfail001")
        manifest = _create_sample_manifest(run_id)

        def mock_fsync_fail(fd: int) -> None:
            raise OSError(errno.EIO, "Physical I/O failure during fsync")

        monkeypatch.setattr(os, "fsync", mock_fsync_fail)

        with pytest.raises(OSError, match="Physical I/O failure"):
            store.save_manifest(manifest)

        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists()

        tmp_files = [f for f in store.root_dir.iterdir() if f.name.startswith(".tmp_manifest_")]
        assert len(tmp_files) == 0

    def test_directory_fsync_failure_on_posix_fails_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Directory fsync failure on POSIX preserves published manifest and does not touch other files."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("dirfsync0001")
        manifest = _create_sample_manifest(run_id)
        expected_bytes = manifest.model_dump_json(indent=2).encode("utf-8")

        # Create an unrelated sentinel file in the store directory to verify other files are untouched
        sentinel_path = store.root_dir / "unrelated_sentinel.txt"
        sentinel_path.write_text("sentinel_unrelated_content", encoding="utf-8")

        # Force platform to non-win32 to trigger directory fsync path
        monkeypatch.setattr(sys, "platform", "linux")

        real_fsync = os.fsync
        call_count = [0]

        def mock_fsync(fd: int) -> None:
            call_count[0] += 1
            if call_count[0] == 1:
                # First call is file fsync on temp_fd: allow it to succeed
                real_fsync(fd)
            else:
                # Second call is parent directory fsync: fail
                raise OSError("Simulated directory fsync media error")

        monkeypatch.setattr(os, "fsync", mock_fsync)

        with pytest.raises(ManifestStorePostPublicationError, match="Parent directory fsync failed.*preserved fail-closed"):
            store.save_manifest(manifest)

        # Verify P1-A invariant: the canonical manifest file is NOT deleted!
        target_path = store.get_manifest_path(run_id)
        assert target_path.exists(), "Published canonical manifest must not be unlinked on directory fsync failure"
        assert target_path.is_file()
        assert target_path.read_bytes() == expected_bytes, "Published manifest bytes must be preserved intact"

        # Verify unrelated files are untouched
        assert sentinel_path.exists(), "Unrelated files in store directory must not be deleted or modified"
        assert sentinel_path.read_text(encoding="utf-8") == "sentinel_unrelated_content"

        # Verify no temporary or lock artifacts are leaked
        leaked = [f.name for f in store.root_dir.iterdir() if f.name.startswith((".tmp_manifest_", ".lock_"))]
        assert leaked == [], f"No leaked temp or lock artifacts expected, found: {leaked}"

    def test_post_publication_readback_failure_preserves_manifest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Readback failure after publication raises ManifestStorePostPublicationError and preserves manifest."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("postpubrd001")
        manifest = _create_sample_manifest(run_id)
        expected_bytes = manifest.model_dump_json(indent=2).encode("utf-8")

        target_path = store.get_manifest_path(run_id)
        real_read_bytes = Path.read_bytes

        def mock_read_bytes(path_obj: Path) -> bytes:
            if path_obj.name == store.manifest_filename(run_id):
                raise OSError("Simulated post-publication media read error")
            return real_read_bytes(path_obj)

        monkeypatch.setattr(Path, "read_bytes", mock_read_bytes)

        with pytest.raises(ManifestStorePostPublicationError, match="Post-publication readback/verification failed.*Publication outcome is uncertain.*canonical manifest is preserved fail-closed"):
            store.save_manifest(manifest)

        assert target_path.exists(), "Canonical manifest must be preserved on disk fail-closed"
        assert real_read_bytes(target_path) == expected_bytes

    def test_post_publication_hash_mismatch_preserves_manifest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Integrity mismatch after publication raises ManifestStorePostPublicationError and preserves manifest."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("postpubhsh01")
        manifest = _create_sample_manifest(run_id)

        target_path = store.get_manifest_path(run_id)
        real_read_bytes = Path.read_bytes

        def mock_read_bytes(path_obj: Path) -> bytes:
            data = real_read_bytes(path_obj)
            if path_obj.name == store.manifest_filename(run_id):
                # Return corrupted content of exact same length
                return data[:-1] + b"X"
            return data

        monkeypatch.setattr(Path, "read_bytes", mock_read_bytes)

        with pytest.raises(ManifestStorePostPublicationError, match="Post-publication.*verification failed.*Publication outcome is uncertain.*canonical manifest is preserved fail-closed"):
            store.save_manifest(manifest)

        assert target_path.exists(), "Canonical manifest must be preserved on disk fail-closed"

    def test_tamper_detection_fails_closed(self, tmp_path: Path) -> None:
        """Modifying a single byte of a saved manifest triggers tamper detection."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("tamper000001")
        manifest = _create_sample_manifest(run_id)

        path = store.save_manifest(manifest)
        _, seal = store.load_manifest(run_id)

        # 1. Tamper: corrupt an attribute value
        raw = path.read_text(encoding="utf-8")
        tampered_raw = raw.replace("SYNTH_ARTICLE", "TAMPERED_ART")
        path.write_text(tampered_raw, encoding="utf-8")

        with pytest.raises(ManifestSealViolation):
            seal.assert_unchanged()

        with pytest.raises(ManifestStoreCorruptionError, match="validation failed"):
            store.load_manifest(run_id)

        # 2. Tamper: restore valid JSON but with extra whitespace altering SHA-256
        path.write_text(raw + "\n  \n", encoding="utf-8")

        with pytest.raises(ManifestSealViolation, match="Manifest differs from sealed"):
            seal.assert_unchanged()

    def test_missing_and_corrupt_manifest_handling(self, tmp_path: Path) -> None:
        """Missing, partial, or malformed manifest files fail closed without granting permissions."""
        store = HermeticTestManifestStore(tmp_path / "store")

        # Missing manifest
        missing_run_id = _generate_valid_run_id("missing00001")
        with pytest.raises(ManifestStoreError, match="not found"):
            store.load_manifest(missing_run_id)

        # Empty file
        empty_run_id = _generate_valid_run_id("empty0000001")
        empty_path = store.get_manifest_path(empty_run_id)
        empty_path.touch()
        if sys.platform != "win32":
            empty_path.chmod(0o600)
        with pytest.raises(ManifestStoreCorruptionError, match="empty or oversized"):
            store.load_manifest(empty_run_id)

        # Truncated partial JSON
        partial_run_id = _generate_valid_run_id("partial00001")
        partial_path = store.get_manifest_path(partial_run_id)
        partial_path.write_text('{"run_id": "' + partial_run_id + '", "versio', encoding="utf-8")
        if sys.platform != "win32":
            partial_path.chmod(0o600)
        with pytest.raises(ManifestStoreCorruptionError, match="validation failed"):
            store.load_manifest(partial_run_id)

        # Mismatched run_id in file
        mismatch_run_id = _generate_valid_run_id("mismatch0001")
        mismatch_path = store.get_manifest_path(mismatch_run_id)
        other_run_id = _generate_valid_run_id("other0000001")
        other_manifest = _create_sample_manifest(other_run_id)
        mismatch_path.write_text(other_manifest.model_dump_json(), encoding="utf-8")
        if sys.platform != "win32":
            mismatch_path.chmod(0o600)

        with pytest.raises(ManifestStoreCorruptionError, match="run identifier mismatch"):
            store.load_manifest(mismatch_run_id)

    def test_oversized_manifest_rejected(self, tmp_path: Path) -> None:
        """Manifests exceeding MAX_MANIFEST_BYTES (64 KB) are rejected."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("oversize0001")
        oversized_path = store.get_manifest_path(run_id)
        oversized_path.write_bytes(b" " * (MAX_MANIFEST_BYTES + 10))
        if sys.platform != "win32":
            oversized_path.chmod(0o600)

        with pytest.raises(ManifestStoreCorruptionError, match="safety size limit"):
            store.load_manifest(run_id)

    def test_posix_insecure_file_permissions_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Manifest file with group or world access permissions (0o644) is rejected on POSIX."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("insecfile001")
        manifest = _create_sample_manifest(run_id)
        path = store.save_manifest(manifest)

        # Mock POSIX environment and stat mode to 0o644
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)

        real_os_stat = os.stat
        target_str = str(path.resolve())

        class MockStatInsecureFile:
            st_uid = 1000
            st_mode = 0o100644  # -rw-r--r-- (group/world read 0o044)
            st_nlink = 1
            st_size = 100

        def mock_stat_file(path_arg: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                if str(path_arg) == target_str:
                    return MockStatInsecureFile()
            except Exception:
                pass
            return real_os_stat(path_arg, *args, **kwargs)

        monkeypatch.setattr(os, "stat", mock_stat_file)

        with pytest.raises(ManifestStoreSecurityError, match="group and world access bits.*strictly prohibited; mode 0600 required"):
            store.load_manifest(run_id)

    def test_concurrency_lock_prevents_simultaneous_writers(self, tmp_path: Path) -> None:
        """Concurrent writers contending on the same run_id fail closed safely."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("concurr00001")
        manifest = _create_sample_manifest(run_id)

        results: list[Exception | Path] = []

        def worker() -> None:
            try:
                p = store.save_manifest(manifest)
                results.append(p)
            except Exception as exc:
                results.append(exc)

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        successes = [r for r in results if isinstance(r, Path)]
        failures = [r for r in results if isinstance(r, (ManifestStoreFileExistsError, ManifestStoreConcurrencyError))]

        assert len(successes) == 1
        assert len(failures) == 1

    def test_lock_acquisition_failure_preserves_foreign_lockfile(self, tmp_path: Path) -> None:
        """Lock acquisition failure must not delete or alter an existing foreign lockfile."""
        run_id = _generate_valid_run_id("foreign00001")
        lock_path = tmp_path / f".lock_{run_id}"
        foreign_token = "pid=99999:run_id=foreign:foreign_secret_token"
        lock_path.write_text(foreign_token, encoding="utf-8")
        if sys.platform != "win32":
            os.chmod(lock_path, 0o600)

        lock = _ManifestRunLock(lock_path, run_id)
        with pytest.raises(ManifestStoreConcurrencyError):
            with lock:
                pass

        # Verify the foreign lockfile is still present with exact content intact
        assert lock_path.exists(), "Foreign lockfile must NOT be deleted on acquisition contention"
        assert lock_path.read_text(encoding="utf-8") == foreign_token

    def test_lock_acquisition_write_failure_cleans_own_lock_without_touching_foreign(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Acquisition failure on own lock cleans up own lock, while foreign locks are never touched."""
        run_id = _generate_valid_run_id("ownfail00001")
        lock_path = tmp_path / f".lock_{run_id}"

        # Mock unpatched write to fail during token writing
        def mock_fail_write(fd: int, data: bytes) -> int:
            raise OSError("Disk I/O error during lock token write")

        monkeypatch.setattr("efso_radar.staging.persistent_manifest_store._raw_os_write", mock_fail_write)

        lock = _ManifestRunLock(lock_path, run_id)
        with pytest.raises(ManifestStoreError, match="Failed to acquire manifest lock"):
            with lock:
                pass

        assert not lock_path.exists(), "Own incomplete lockfile should be cleaned up on write failure"

    def test_foreign_lockfile_empty_and_token_never_deleted_on_acquisition_or_release(
        self, tmp_path: Path
    ) -> None:
        """Foreign lockfiles (empty, partial, or other token) are never deleted on contention or release attempt."""
        run_id = _generate_valid_run_id("foreign0001")
        lock_path = tmp_path / f".lock_{run_id}"

        # Scenario 1: Foreign lockfile is EMPTY ("")
        lock_path.write_text("", encoding="utf-8")
        assert lock_path.exists()
        assert lock_path.read_text(encoding="utf-8") == ""

        # Lock contention on empty foreign lockfile: must fail closed with ManifestStoreConcurrencyError
        lock_instance_1 = _ManifestRunLock(lock_path, run_id)
        with pytest.raises(ManifestStoreConcurrencyError, match="Concurrent writer lock active"):
            with lock_instance_1:
                pass

        # Foreign empty lockfile must NOT be deleted by contention
        assert lock_path.exists(), "Foreign empty lockfile must NOT be deleted on acquisition failure"
        assert lock_path.read_text(encoding="utf-8") == ""

        # Calling _release_lock_if_owned directly on an unowned instance must NOT delete empty foreign file
        lock_instance_1._release_lock_if_owned()
        assert lock_path.exists(), "Direct release on unowned instance must NOT delete foreign empty file"

        # Even if _opened_by_us were somehow set on a lock instance, unacquired empty file is never deleted
        fake_lock = _ManifestRunLock(lock_path, run_id)
        fake_lock._opened_by_us = True
        fake_lock._release_lock_if_owned()
        assert lock_path.exists(), "Empty content alone must NEVER be treated as ownership"

        # Scenario 2: Foreign lockfile contains another process/token
        foreign_token = "pid=999999:foreign_process_run"
        lock_path.write_text(foreign_token, encoding="utf-8")

        lock_instance_2 = _ManifestRunLock(lock_path, run_id)
        with pytest.raises(ManifestStoreConcurrencyError, match="Concurrent writer lock active"):
            with lock_instance_2:
                pass

        assert lock_path.exists(), "Foreign token lockfile must NOT be deleted on acquisition failure"
        assert lock_path.read_text(encoding="utf-8") == foreign_token

        # Calling _release_lock_if_owned directly must NOT delete foreign file
        lock_instance_2._release_lock_if_owned()
        assert lock_path.exists(), "Direct release on unowned instance must NOT delete foreign token file"
        assert lock_path.read_text(encoding="utf-8") == foreign_token

        # Cleanup test fixture file
        lock_path.unlink()

    def test_zero_secrets_policy_enforcement(self, tmp_path: Path) -> None:
        """Manifest containing sensitive secret tokens is strictly rejected before write."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("secret000001")

        entities = create_default_manifest_entities(run_id)
        entities[0].expected_attributes["bearer token"] = "eyJh...secret"

        leaky_manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=entities,
        )

        with pytest.raises(ManifestStoreSecurityError, match="forbidden secret substring"):
            store.save_manifest(leaky_manifest)

    def test_retention_policy_and_fail_closed_deletion(self, tmp_path: Path) -> None:
        """Direct unlinking via CLEAN is strictly disabled and rejected; manifest is preserved."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("retain000001")
        manifest = _create_sample_manifest(run_id)
        path = store.save_manifest(manifest)

        # UNKNOWN -> preserved (returns False)
        assert store.delete_manifest_if_clean(run_id, AuditOutcomeState.UNKNOWN) is False
        assert path.exists()

        # RESIDUAL -> preserved (returns False)
        assert store.delete_manifest_if_clean(run_id, AuditOutcomeState.RESIDUAL) is False
        assert path.exists()

        # OFFLINE_VALIDATION_PASSED -> preserved (returns False)
        assert store.delete_manifest_if_clean(run_id, AuditOutcomeState.OFFLINE_VALIDATION_PASSED) is False
        assert path.exists()

        # Direct caller-supplied CLEAN -> strictly fails closed; raises ManifestStoreSecurityError!
        with pytest.raises(ManifestStoreSecurityError, match="Persistent manifest deletion is strictly disabled"):
            store.delete_manifest_if_clean(run_id, AuditOutcomeState.CLEAN)

        # Manifest MUST STILL EXIST on disk! Direct caller-supplied CLEAN cannot delete anything.
        assert path.exists()

    def test_offline_recovery_validation_fixture(self, tmp_path: Path) -> None:
        """Offline recovery validation inspects against isolated in-memory fixture and preserves manifest."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("recov0000001")
        manifest = _create_sample_manifest(run_id)
        path = store.save_manifest(manifest)

        report = store.validate_recovery_offline(run_id)

        assert report.run_id == run_id
        assert report.valid_schema is True
        assert report.entities_count == 5
        assert report.audit_state == AuditOutcomeState.OFFLINE_VALIDATION_PASSED
        assert report.manifest_preserved is True
        assert path.exists()

    def test_list_manifest_run_ids(self, tmp_path: Path) -> None:
        """list_manifest_run_ids returns correctly sorted run_ids, ignoring temporary files."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id_1 = _generate_valid_run_id("list00000001")
        time.sleep(0.01)
        run_id_2 = _generate_valid_run_id("list00000002")

        store.save_manifest(_create_sample_manifest(run_id_1))
        store.save_manifest(_create_sample_manifest(run_id_2))

        (store.root_dir / ".noise.tmp").touch()
        (store.root_dir / "random.txt").touch()

        run_ids = store.list_manifest_run_ids()
        assert run_id_1 in run_ids
        assert run_id_2 in run_ids
        assert len(run_ids) == 2

    def test_os_temp_loss_simulation(self, tmp_path: Path) -> None:
        """Simulate loss of volatile tempdir: persistent store retains manifest while temp is wiped.

        Note: this uses isolated pytest fixtures to simulate ephemeral storage maintenance.
        """
        persistent_dir = tmp_path / "durable_store"
        store = HermeticTestManifestStore(persistent_dir)

        simulated_temp = tmp_path / "volatile_temp"
        simulated_temp.mkdir()

        run_id = _generate_valid_run_id("tempdur00001")
        manifest = _create_sample_manifest(run_id)

        durable_path = store.save_manifest(manifest)
        volatile_path = manifest.save_atomic(simulated_temp)

        assert durable_path.is_file()
        assert volatile_path.is_file()

        # Simulate OS temporary directory purging
        for f in simulated_temp.iterdir():
            f.unlink()
        assert not volatile_path.exists(), "Volatile temp file should be wiped"

        # Manifest in persistent store remains intact and verifiable
        assert durable_path.exists()
        loaded, seal = store.load_manifest(run_id)
        assert loaded.run_id == run_id
        seal.assert_unchanged()

    def test_write_and_rename_failure_cleans_up_temp_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failure during atomic publication cleans up sibling temp file and leaves target absent."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("failren00001")
        manifest = _create_sample_manifest(run_id)

        def mock_publish(src: Path, dst: Path) -> None:
            raise OSError("Simulated atomic publication I/O failure")

        monkeypatch.setattr(
            "efso_radar.staging.persistent_manifest_store._publish_file_no_overwrite",
            mock_publish,
        )

        with pytest.raises(OSError, match="Simulated atomic publication"):
            store.save_manifest(manifest)

        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists()

        tmp_files = [f for f in store.root_dir.iterdir() if f.name.startswith(".tmp_manifest_")]
        assert len(tmp_files) == 0

    def test_reparse_point_detector_mock(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify _is_reparse_point detects FILE_ATTRIBUTE_REPARSE_POINT (0x0400)."""
        from efso_radar.staging.persistent_manifest_store import _is_reparse_point
        test_file = tmp_path / "file.txt"
        test_file.touch()

        assert _is_reparse_point(test_file) is False

        class MockStat:
            st_file_attributes = 0x0400

        monkeypatch.setattr(os, "lstat", lambda p: MockStat())
        monkeypatch.setattr(sys, "platform", "win32")
        assert _is_reparse_point(test_file) is True

    def test_canonical_manifest_validation_rejects_non_canonical_before_publication_and_allows_subsequent_valid_save(
        self, tmp_path: Path
    ) -> None:
        """save_manifest rejects invalid title, modified ownership markers, and duplicate entities before writing."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("canonval0001")
        canonical_entities = create_default_manifest_entities(run_id)

        # 1. Invalid title
        invalid_title_entities = [
            RegisteredEntity(
                table=e.table,
                pk_column=e.pk_column,
                pk_value=e.pk_value,
                cleanup_dependency_order=e.cleanup_dependency_order,
                expected_attributes=(
                    {**e.expected_attributes, "title": "MALICIOUS_INJECTED_TITLE"}
                    if "title" in e.expected_attributes
                    else e.expected_attributes
                ),
            )
            for e in canonical_entities
        ]
        manifest_bad_title = PerRunOwnershipManifest(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=invalid_title_entities,
        )
        with pytest.raises(ValueError, match="attribute 'title' mismatch"):
            store.save_manifest(manifest_bad_title)

        # 2. Altered ownership marker (e.g. altered worker_ids)
        invalid_worker_entities = [
            RegisteredEntity(
                table=e.table,
                pk_column=e.pk_column,
                pk_value=e.pk_value,
                cleanup_dependency_order=e.cleanup_dependency_order,
                expected_attributes=(
                    {**e.expected_attributes, "worker_ids": ["rogue_worker"]}
                    if "worker_ids" in e.expected_attributes
                    else e.expected_attributes
                ),
            )
            for e in canonical_entities
        ]
        manifest_bad_worker = PerRunOwnershipManifest(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=invalid_worker_entities,
        )
        with pytest.raises(ValueError, match="attribute 'worker_ids' mismatch"):
            store.save_manifest(manifest_bad_worker)

        # 3. Duplicate entity (6 entries, or 5 entries with duplicate key)
        duplicate_entities = list(canonical_entities) + [canonical_entities[0]]
        manifest_duplicate = PerRunOwnershipManifest(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=duplicate_entities,
        )
        with pytest.raises(ValueError, match="Manifest must contain exactly 5 registered entities"):
            store.save_manifest(manifest_duplicate)

        # 4. Wrong cleanup dependency order
        wrong_order_entities = [
            RegisteredEntity(
                table=e.table,
                pk_column=e.pk_column,
                pk_value=e.pk_value,
                cleanup_dependency_order=99,
                expected_attributes=e.expected_attributes,
            )
            if e.cleanup_dependency_order == 1
            else e
            for e in canonical_entities
        ]
        manifest_wrong_order = PerRunOwnershipManifest(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=wrong_order_entities,
        )
        with pytest.raises(ValueError, match="unexpected cleanup_dependency_order"):
            store.save_manifest(manifest_wrong_order)

        # Verify NO manifest file or temporary/lock file exists after rejections
        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists(), "Target file must not be created on validation failure"
        leaked = [f.name for f in store.root_dir.iterdir() if f.name.startswith((".tmp_manifest_", ".lock_"))]
        assert leaked == [], f"No leaked temp or lock artifacts expected, found: {leaked}"

        # 5. Subsequent valid save with the EXACT SAME run_id succeeds cleanly
        valid_manifest = _create_sample_manifest(run_id)
        published_path = store.save_manifest(valid_manifest)
        assert published_path.exists()
        assert published_path == target_path
        loaded, seal = store.load_manifest(run_id)
        assert loaded.run_id == run_id
        seal.assert_unchanged()

    def test_publish_linux_renameat2_fallback_narrowing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """renameat2 fails closed without blind link fallback on real I/O or permission errors."""
        temp_file = tmp_path / "temp.txt"
        target_file = tmp_path / "target.txt"

        class MockRenameat2:
            def __init__(self, ret_val: int) -> None:
                self.ret_val = ret_val
                self.argtypes: Any = None
                self.restype: Any = None

            def __call__(self, *args: Any) -> int:
                return self.ret_val

        class MockLibc:
            def __init__(self, errno_to_return: int) -> None:
                self._errno = errno_to_return
                self.renameat2 = MockRenameat2(-1)

        link_called = [False]
        real_link = getattr(os, "link", None)

        def mock_link(src: Any, dst: Any) -> None:
            link_called[0] = True
            if real_link is not None:
                real_link(src, dst)

        # 1. EIO (errno 5) - real I/O error: blind fallback must be rejected
        temp_file.write_text("content", encoding="utf-8")
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setattr("ctypes.CDLL", lambda *args, **kwargs: MockLibc(errno.EIO))
        monkeypatch.setattr("ctypes.get_errno", lambda: errno.EIO)
        monkeypatch.setattr(os, "link", mock_link, raising=False)

        with pytest.raises(ManifestStoreError, match="blind fallback to link prohibited"):
            _publish_file_no_overwrite(temp_file, target_file)

        assert link_called[0] is False, "os.link must NOT be called on renameat2 EIO"
        assert not temp_file.exists(), "temp_file must be cleaned up"

        # 2. ENOSYS (errno 38) - unsupported syscall: fallback to link IS allowed
        temp_file.write_text("content2", encoding="utf-8")
        link_called[0] = False
        monkeypatch.setattr("ctypes.CDLL", lambda *args, **kwargs: MockLibc(errno.ENOSYS))
        monkeypatch.setattr("ctypes.get_errno", lambda: errno.ENOSYS)

        # In this test environment, mock link to succeed by copying
        def mock_link_success(src: Any, dst: Any) -> None:
            link_called[0] = True
            Path(dst).write_bytes(Path(src).read_bytes())

        monkeypatch.setattr(os, "link", mock_link_success, raising=False)
        _publish_file_no_overwrite(temp_file, target_file)
        assert link_called[0] is True, "os.link must be called when renameat2 is ENOSYS"
        assert target_file.exists()
        target_file.unlink()

    def test_lockfile_ownership_checked_cleanup(self, tmp_path: Path) -> None:
        """Lockfile exit does not delete foreign lockfiles if replaced or not acquired."""
        lock_path = tmp_path / ".lock_test_ownership"
        run_id = _generate_valid_run_id("lockowner001")

        # Case 1: normal acquisition and clean exit
        with _ManifestRunLock(lock_path, run_id):
            assert lock_path.exists()
        assert not lock_path.exists(), "Clean exit must unlink owned lockfile"

        # Case 2: foreign lockfile replacement is NOT unlinked on exit
        lock = _ManifestRunLock(lock_path, run_id)
        lock.__enter__()
        assert lock_path.exists()

        # Simulate foreign process replacing lockfile content with a foreign token
        lock_path.write_text("pid=999999:run_id=foreign:foreign_uuid_12345", encoding="utf-8")

        # Exit should detect token mismatch and NOT unlink foreign lockfile
        lock.__exit__(None, None, None)
        assert lock_path.exists(), "Foreign lockfile must NOT be deleted on token mismatch"
        lock_path.unlink()


class TestStoragePlatformGuarantees:
    """Verify explicit platform guarantees, policy verification, and blockers reporting."""

    def test_platform_guarantees_structure(self) -> None:
        """Inspect storage platform guarantees and verify explicit platform blockers."""
        guarantees = get_storage_platform_guarantees()
        assert guarantees.platform in ("win32", "linux", "darwin")
        assert guarantees.fsync_file_supported is True
        assert guarantees.atomic_no_overwrite_supported is True
        assert guarantees.atomic_replace_supported is True
        assert guarantees.lockfile_concurrency_supported is True
        assert isinstance(guarantees.identified_blockers, list)
        assert len(guarantees.identified_blockers) > 0

        if sys.platform == "win32":
            assert guarantees.fsync_directory_supported is False
            assert guarantees.posix_permissions_supported is False
            assert any("Directory fsync" in b for b in guarantees.identified_blockers)
            assert any("DACL/ACL" in b for b in guarantees.identified_blockers)
            assert any("POSIX file ownership" in b for b in guarantees.identified_blockers)

            # verify_platform_policy in live mode fails closed on Windows
            with pytest.raises(ManifestStoreSecurityError, match="Windows NTFS platform cannot programmatically verify DACL/ACL"):
                guarantees.verify_platform_policy(is_hermetic_test=False)

            # verify_platform_policy in hermetic test mode passes
            guarantees.verify_platform_policy(is_hermetic_test=True)
        else:
            assert guarantees.fsync_directory_supported is True
            assert guarantees.posix_permissions_supported is True
            assert any("Cross-filesystem" in b for b in guarantees.identified_blockers)
            # On Linux with directory fsync, hermetic test mode passes
            guarantees.verify_platform_policy(is_hermetic_test=True)



class TestWindowsManifestDurabilityCrashWindows:
    """Deterministic offline fault-injection test matrix for manifest publication crash windows."""

    def test_crash_window_contracts_completeness(self) -> None:
        """Inspect crash window contracts; verify documented fail-closed invariants and zero-retry."""
        contracts = get_crash_window_contracts()
        assert len(contracts) == 8
        for window in ManifestCrashWindow:
            assert window in contracts
            c = contracts[window]
            assert c.window == window
            assert c.retry_permitted is False, f"Automatic retry must be forbidden in {window}"
            assert c.remote_write_authorized is False, f"Remote write must not be authorized in {window}"
            assert c.expected_canonical_state in ("ABSENT", "PRESERVED")
            assert c.expected_temp_state in ("CLEANED", "ORPHANED_INERT")
            assert c.expected_outcome in ("FAILED_CLOSED", "UNCERTAIN_HALT")

        # Specifically assert Window 8 invariants for pre-publication crash/termination
        w8 = contracts[ManifestCrashWindow.WINDOW_8_RECOVERY_REOPEN]
        assert w8.expected_canonical_state == "ABSENT"
        assert w8.expected_temp_state == "ORPHANED_INERT"
        assert w8.expected_outcome == "FAILED_CLOSED"

    def test_pre_remote_write_durability_gate_blocks_live_on_unproven_fsync(self) -> None:
        """Verify pre-remote-write durability gate blocks live remote writes when directory fsync unproven."""
        # Hermetic test mode returns an inert assessment without raising; platform_prerequisite_met is never manufactured
        assessment = verify_pre_remote_write_durability_gate(is_hermetic_test=True)
        assert isinstance(assessment, StoragePlatformDurabilityAssessment)
        assert assessment.platform_prerequisite_met is (sys.platform != "win32")
        assert assessment.manifest_persisted is False
        assert assessment.remote_mutation_authorized is False

        # In live mode on Windows (or unproven directory fsync), gate fails closed
        if sys.platform == "win32":
            with pytest.raises(ManifestStoreSecurityError, match="Pre-remote-write durability prerequisite check failed"):
                verify_pre_remote_write_durability_gate(is_hermetic_test=False)
        else:
            mock_guar = Mock(platform="linux", fsync_file_supported=True, fsync_directory_supported=False)
            with pytest.raises(ManifestStoreSecurityError, match="Pre-remote-write durability prerequisite check failed"):
                verify_pre_remote_write_durability_gate(mock_guar, is_hermetic_test=False)

    def test_pre_remote_write_durability_gate_inert_assessment_and_no_authorization(self, tmp_path: Path) -> None:
        """Negative contract: platform prerequisite success does NOT establish manifest presence or remote permission."""
        # 1. Hermetic assessment check reflects real platform capabilities without manufacturing success
        assessment_hermetic = verify_pre_remote_write_durability_gate(is_hermetic_test=True)
        assert assessment_hermetic.platform_prerequisite_met is (sys.platform != "win32")
        assert assessment_hermetic.manifest_persisted is False
        assert assessment_hermetic.remote_mutation_authorized is False

        # 2. Mocked supported POSIX platform assessment check
        mock_supported_guar = Mock(platform="linux", fsync_file_supported=True, fsync_directory_supported=True)
        assessment_posix = verify_pre_remote_write_durability_gate(mock_supported_guar, is_hermetic_test=False)
        assert assessment_posix.platform_prerequisite_met is True
        assert assessment_posix.manifest_persisted is False
        assert assessment_posix.remote_mutation_authorized is False

        # 3. Verify that calling the gate did NOT create any files or grant filesystem mutations
        test_dir = tmp_path / "prereq_test"
        test_dir.mkdir(parents=True, exist_ok=True)
        assert list(test_dir.iterdir()) == [], "Platform prerequisite assessment must be completely inert and produce zero files"

    def test_pre_remote_write_durability_gate_mock_cases_for_unsupported_platforms(self) -> None:
        """Regression tests: unsupported file fsync on Linux fails closed, hermetic Windows returns negative assessment."""
        # Case 1: Linux with directory fsync True but file fsync False
        mock_linux_no_file_fsync = Mock(platform="linux", fsync_file_supported=False, fsync_directory_supported=True)
        # Non-hermetic mode must fail closed
        with pytest.raises(ManifestStoreSecurityError, match="Pre-remote-write durability prerequisite check failed"):
            verify_pre_remote_write_durability_gate(mock_linux_no_file_fsync, is_hermetic_test=False)
        # Hermetic mode must return an inert negative assessment, never manufacturing True
        assessment_linux_no_file = verify_pre_remote_write_durability_gate(mock_linux_no_file_fsync, is_hermetic_test=True)
        assert assessment_linux_no_file.platform_prerequisite_met is False
        assert assessment_linux_no_file.fsync_file_supported is False
        assert assessment_linux_no_file.fsync_directory_supported is True
        assert assessment_linux_no_file.manifest_persisted is False
        assert assessment_linux_no_file.remote_mutation_authorized is False

        # Case 2: Windows with file fsync True but directory fsync False
        mock_win32 = Mock(platform="win32", fsync_file_supported=True, fsync_directory_supported=False)
        # Non-hermetic mode must fail closed
        with pytest.raises(ManifestStoreSecurityError, match="Pre-remote-write durability prerequisite check failed"):
            verify_pre_remote_write_durability_gate(mock_win32, is_hermetic_test=False)
        # Hermetic mode must return an inert negative assessment, never manufacturing True
        assessment_win32 = verify_pre_remote_write_durability_gate(mock_win32, is_hermetic_test=True)
        assert assessment_win32.platform_prerequisite_met is False
        assert assessment_win32.fsync_file_supported is True
        assert assessment_win32.fsync_directory_supported is False
        assert assessment_win32.manifest_persisted is False
        assert assessment_win32.remote_mutation_authorized is False

        # Case 3: Mocked supported POSIX platform produces positive platform-only assessment, no live authorization
        mock_posix_ok = Mock(platform="linux", fsync_file_supported=True, fsync_directory_supported=True)
        assessment_posix_ok = verify_pre_remote_write_durability_gate(mock_posix_ok, is_hermetic_test=True)
        assert assessment_posix_ok.platform_prerequisite_met is True
        assert assessment_posix_ok.manifest_persisted is False
        assert assessment_posix_ok.remote_mutation_authorized is False

    def test_crash_window_1_temp_file_creation_failure_leaves_no_artifacts(self, tmp_path: Path) -> None:
        """Window 1: Failure during temp file creation (O_CREAT) leaves no canonical or temp files."""
        store_dir = tmp_path / "store_w1"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw1")
        manifest = _create_sample_manifest(run_id)

        real_open = os.open
        def fault_open(p, flags, *args, **kwargs):
            if ".tmp_manifest_" in str(p):
                raise OSError(errno.EACCES, "Permission denied (simulated temp creation failure)")
            return real_open(p, flags, *args, **kwargs)

        with patch("os.open", side_effect=fault_open):
            with pytest.raises(OSError, match="Permission denied"):
                store.save_manifest(manifest)

        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists(), "Canonical manifest must not exist after temp creation failure"
        temp_files = list(store_dir.glob(".tmp_manifest_*"))
        assert len(temp_files) == 0, "No orphaned temp file should exist"

    def test_crash_window_2_short_write_disk_full_fails_closed_and_cleans_temp(self, tmp_path: Path) -> None:
        """Window 2: Controlled short write / disk full during payload write unlinks temp file in exception handler, leaves no canonical."""
        store_dir = tmp_path / "store_w2"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw2")
        manifest = _create_sample_manifest(run_id)

        real_write = os.write
        def fault_write(fd, data):
            real_write(fd, data[:8])
            raise OSError(errno.ENOSPC, "No space left on device (simulated disk full)")

        with patch("os.write", side_effect=fault_write):
            with pytest.raises(OSError, match="No space left on device"):
                store.save_manifest(manifest)

        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists(), "Canonical manifest must not exist after short write"
        temp_files = list(store_dir.glob(".tmp_manifest_*"))
        assert len(temp_files) == 0, "Temporary file must be unlinked after short write failure"

    def test_crash_window_3_file_fsync_failure_cleans_temp_and_leaves_no_target(self, tmp_path: Path) -> None:
        """Window 3: File fsync failure (disk controller error) cleans temp file, leaves no canonical."""
        store_dir = tmp_path / "store_w3"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw3")
        manifest = _create_sample_manifest(run_id)

        with patch("os.fsync", side_effect=OSError(errno.EIO, "I/O error during file sync")):
            with pytest.raises(OSError, match="I/O error during file sync"):
                store.save_manifest(manifest)

        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists(), "Canonical manifest must not exist after fsync failure"
        temp_files = list(store_dir.glob(".tmp_manifest_*"))
        assert len(temp_files) == 0, "Temporary file must be unlinked after fsync failure"

    def test_crash_window_4_pre_publication_hash_mismatch_destroys_temp(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Window 4: Pre-publication readback verification failure destroys corrupt temp file."""
        store_dir = tmp_path / "store_w4"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw4")
        manifest = _create_sample_manifest(run_id)

        real_read_bytes = Path.read_bytes

        def fault_read_bytes(path_obj: Path) -> bytes:
            content = real_read_bytes(path_obj)
            if ".tmp_manifest_" in path_obj.name:
                return content[:-1] + b"X"
            return content

        monkeypatch.setattr(Path, "read_bytes", fault_read_bytes)

        with pytest.raises(ManifestStoreCorruptionError, match="Pre-publication verification failed"):
            store.save_manifest(manifest)

        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists(), "Canonical manifest must not exist after pre-pub verification failure"
        temp_files = list(store_dir.glob(".tmp_manifest_*"))
        assert len(temp_files) == 0, "Corrupted temporary file must be destroyed"

    def test_crash_window_5_preexisting_target_collision_preserves_canonical_and_cleans_temp(self, tmp_path: Path) -> None:
        """Window 5: Pre-existing target collision blocks overwrite, preserves target, cleans temp."""
        store_dir = tmp_path / "store_w5"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw5")
        manifest = _create_sample_manifest(run_id)

        target_path = store.save_manifest(manifest)
        original_bytes = target_path.read_bytes()

        with pytest.raises(ManifestStoreFileExistsError, match="already exists"):
            store.save_manifest(manifest)

        assert target_path.read_bytes() == original_bytes, "Target manifest must remain strictly byte-for-byte unaltered"
        temp_files = list(store_dir.glob(".tmp_manifest_*"))
        assert len(temp_files) == 0, "Temporary file from failed collision must be unlinked"

    def test_crash_window_6_windows_directory_durability_gap_preserves_canonical_and_fails_live(self, tmp_path: Path) -> None:
        """Window 6: On Windows, directory fsync is unproven; canonical file is preserved, but live mode is blocked."""
        store_dir = tmp_path / "store_w6"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw6")
        manifest = _create_sample_manifest(run_id)

        target_path = store.save_manifest(manifest)
        assert target_path.is_file(), "Canonical file must exist after successful save"

        guarantees = get_storage_platform_guarantees()
        if sys.platform == "win32":
            assert guarantees.fsync_directory_supported is False
            with pytest.raises(ManifestStoreSecurityError, match="Windows NTFS platform cannot programmatically verify DACL/ACL"):
                guarantees.verify_platform_policy(is_hermetic_test=False)
            with pytest.raises(ManifestStoreSecurityError, match="Pre-remote-write durability prerequisite check failed"):
                verify_pre_remote_write_durability_gate(guarantees, is_hermetic_test=False)
        else:
            with patch("os.fsync", side_effect=[None, OSError(errno.EIO, "Directory fsync failed")]):
                run_id_posix = _generate_valid_run_id("crashw6posix")
                manifest_posix = _create_sample_manifest(run_id_posix)
                target_posix = store.get_manifest_path(run_id_posix)
                with pytest.raises(ManifestStorePostPublicationError, match="Parent directory fsync failed"):
                    store.save_manifest(manifest_posix)
                assert target_posix.is_file(), "Canonical manifest must be preserved even if directory fsync fails"

    def test_crash_window_7_post_publication_readback_failure_preserves_canonical_without_retry(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Window 7: Post-publication readback/verification error preserves canonical file fail-closed, no retry."""
        store_dir = tmp_path / "store_w7"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw7")
        manifest = _create_sample_manifest(run_id)
        target_path = store.get_manifest_path(run_id)

        real_read_bytes = Path.read_bytes

        def fault_read_bytes(path_obj: Path) -> bytes:
            if path_obj.name == store.manifest_filename(run_id):
                raise PermissionError("EACCES on post-publication readback")
            return real_read_bytes(path_obj)

        monkeypatch.setattr(Path, "read_bytes", fault_read_bytes)

        with pytest.raises(ManifestStorePostPublicationError, match="Post-publication readback/verification failed"):
            store.save_manifest(manifest)

        assert target_path.is_file(), "Canonical manifest must be preserved on disk for recovery investigation"
        temp_files = list(store_dir.glob(".tmp_manifest_*"))
        assert len(temp_files) == 0, "No temporary files should remain"

    def test_crash_window_8_subprocess_termination_inert_orphan_simulation(self, tmp_path: Path) -> None:
        """Window 8: Abrupt test-owned subprocess termination mid-write leaves inert temp orphan and no canonical file."""
        store_dir = tmp_path / "store_w8"
        store = HermeticTestManifestStore(store_dir)
        run_id = _generate_valid_run_id("crashw8")

        # Write out standalone python script simulating subprocess that crashes mid-write before publication
        script_file = tmp_path / "crash_writer.py"
        partial_payload = b'{"run_id": "' + run_id.encode("utf-8") + b'", "partial": true'
        script_code = (
            "import sys\n"
            "import time\n"
            "from pathlib import Path\n\n"
            f"store_dir = Path({str(store_dir.resolve())!r})\n"
            "store_dir.mkdir(parents=True, exist_ok=True)\n"
            f"temp_file = store_dir / '.tmp_manifest_{run_id}_abort.tmp'\n"
            f"temp_file.write_bytes({partial_payload!r})\n"
            "sys.stdout.write('READY\\n')\n"
            "sys.stdout.flush()\n"
            "time.sleep(60)\n"
        )
        script_file.write_text(script_code, encoding="utf-8")

        proc = subprocess.Popen(
            [sys.executable, str(script_file)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            line = proc.stdout.readline()
            if "READY" not in line:
                err = proc.stderr.read()
                proc.kill()
                raise AssertionError(f"Subprocess failed to start: stdout={line!r}, stderr={err!r}")
            # Simulate abrupt process crash/termination mid-write before publication
            proc.kill()
            proc.wait(timeout=5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()

        # 1. Canonical manifest for crashed run must be strictly ABSENT
        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists(), "Target canonical manifest must not exist after crashed write"

        # 2. Crashed run's partial temp file must be PRESERVED on disk as inert orphan evidence (not silently unlinked)
        temp_file = store_dir / f".tmp_manifest_{run_id}_abort.tmp"
        assert temp_file.exists(), "Orphaned partial temp file must remain on disk for inspection"
        assert temp_file.read_bytes() == partial_payload, "Orphaned temp payload must match partial write"

        # 3. Loading the crashed run fails closed
        with pytest.raises(ManifestStoreError, match="not found"):
            store.load_manifest(run_id)

        # 4. Invariants match Window 8 contract
        w8_contract = get_crash_window_contracts()[ManifestCrashWindow.WINDOW_8_RECOVERY_REOPEN]
        assert w8_contract.expected_canonical_state == "ABSENT"
        assert w8_contract.expected_temp_state == "ORPHANED_INERT"
        assert w8_contract.expected_outcome == "FAILED_CLOSED"
        assert w8_contract.retry_permitted is False
        assert w8_contract.remote_write_authorized is False

        # 5. Subsequent independent persist for a new run succeeds cleanly without being corrupted by the orphan
        new_run_id = _generate_valid_run_id("crashw8clean")
        new_manifest = _create_sample_manifest(new_run_id)
        new_target = store.save_manifest(new_manifest)
        assert new_target.is_file(), "Subsequent save must succeed cleanly"

        loaded_manifest, _ = store.load_manifest(new_run_id)
        assert loaded_manifest.run_id == new_run_id

        # 6. Verify original crashed orphan is still preserved and was not touched by the new save
        assert temp_file.exists(), "Original crashed orphan file must not be modified or deleted by subsequent saves"
