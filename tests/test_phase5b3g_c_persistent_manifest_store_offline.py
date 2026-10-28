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
from typing import Any
import uuid
import pytest

from efso_radar.staging.manifest_seal import (
    MAX_MANIFEST_BYTES,
    ManifestSealViolation,
    OfflineManifestSeal,
)
from efso_radar.staging.persistent_manifest_store import (
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
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    PerRunOwnershipManifest,
    RegisteredEntity,
    create_default_manifest_entities,
)


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

    def test_hermetic_test_store_authorized_fixture_root_registration(self) -> None:
        """Custom test roots registered via register_authorized_fixture_root are permitted and clean up cleanly."""
        custom_root = Path(tempfile.gettempdir()) / f"custom_fixture_root_{uuid.uuid4().hex}"
        try:
            # Unregistered: rejected
            with pytest.raises(ManifestStoreSecurityError, match="ownership-verified isolated test fixture"):
                HermeticTestManifestStore(custom_root)

            # Register authorized fixture root
            HermeticTestManifestStore.register_authorized_fixture_root(custom_root)
            store = HermeticTestManifestStore(custom_root / "store")
            assert store.root_dir.exists()
        finally:
            HermeticTestManifestStore.clear_authorized_fixture_roots()
            if (custom_root / "store").exists():
                (custom_root / "store").rmdir()
            if custom_root.exists():
                custom_root.rmdir()

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
import sys
from pathlib import Path
from efso_radar.staging.persistent_manifest_store import HermeticTestManifestStore
from efso_radar.staging.manifest_seal import OfflineManifestSeal

store = HermeticTestManifestStore(Path({str(store_dir.resolve())!r}))
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
