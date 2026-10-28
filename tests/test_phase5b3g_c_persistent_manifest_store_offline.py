"""Hermetic offline test suite for Phase 5B.3G-C Persistent Manifest Store Foundation.

OFFLINE TEST ONLY.
Zero network access, zero Turso credentials, zero database calls.
All file operations are confined strictly to isolated, ownership-proven pytest tmp_path fixtures.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import sys
import threading
import time
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
    ManifestStoreSecurityError,
    PersistentManifestStore,
    _find_repo_root,
    get_storage_platform_guarantees,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    PerRunOwnershipManifest,
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
    """Verify fail-closed security barriers on directory paths, symlinks, and defaults."""

    def test_default_deny_live_authorization(self, tmp_path: Path) -> None:
        """Store initialized without live_authorized or hermetic test mode fails closed on save."""
        # Use an isolated path outside repository
        isolated_dir = tmp_path / "manifests"
        isolated_dir.mkdir()

        # In non-hermetic mode, using tmp_path raises ManifestStoreSecurityError because it's in OS temp
        with pytest.raises(ManifestStoreSecurityError, match="OS temporary directory"):
            PersistentManifestStore(isolated_dir, live_authorized=False, is_hermetic_test=False)

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

        # Create a decoy file outside
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


class TestPersistentManifestStoreOperations:
    """Verify atomic write, overwrite protection, process restart, and tamper detection."""

    def test_atomic_save_and_reopen_restart_simulation(self, tmp_path: Path) -> None:
        """Store atomically persists manifest; a new store instance reopens and verifies seal."""
        store_dir = tmp_path / "persistent_store"
        store1 = HermeticTestManifestStore(store_dir)

        run_id = _generate_valid_run_id("reopen000001")
        manifest = _create_sample_manifest(run_id)

        path = store1.save_manifest(manifest)
        assert path.is_file()
        assert path.name == f"efso_radar_staging_manifest_{run_id}.json"
        assert store1.manifest_exists(run_id)

        # Simulate process restart by creating a new independent store instance on the same directory
        store2 = HermeticTestManifestStore(store_dir)
        loaded_manifest, seal = store2.load_manifest(run_id)

        assert loaded_manifest.run_id == run_id
        assert len(loaded_manifest.registered_entities) == 5
        assert isinstance(seal, OfflineManifestSeal)
        assert seal.run_id == run_id
        assert seal.allowed_root == store_dir.resolve()

        # Sealed inventory unchanged assertion passes
        seal.assert_unchanged()
        seal.require_registered_identity("processed_articles", "article_id", f"synth_art_{run_id}_01")

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

        # Verify file content and mtime are untouched
        assert path.stat().st_mtime_ns == original_mtime
        assert path.read_bytes() == original_bytes

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

        # Seal assertion detects schema/attribute tamper
        with pytest.raises(ManifestSealViolation):
            seal.assert_unchanged()

        # Re-loading also fails validation due to canonical entity mismatch
        with pytest.raises(ManifestStoreCorruptionError, match="validation failed"):
            store.load_manifest(run_id)

        # 2. Tamper: restore valid JSON but with extra whitespace altering SHA-256
        path.write_text(raw + "\n  \n", encoding="utf-8")

        # Seal assertion detects hash difference even if JSON remains valid
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
        with pytest.raises(ManifestStoreCorruptionError, match="empty or oversized"):
            store.load_manifest(empty_run_id)

        # Truncated partial JSON
        partial_run_id = _generate_valid_run_id("partial00001")
        partial_path = store.get_manifest_path(partial_run_id)
        partial_path.write_text('{"run_id": "' + partial_run_id + '", "versio', encoding="utf-8")
        with pytest.raises(ManifestStoreCorruptionError, match="validation failed"):
            store.load_manifest(partial_run_id)

        # Mismatched run_id in file
        mismatch_run_id = _generate_valid_run_id("mismatch0001")
        mismatch_path = store.get_manifest_path(mismatch_run_id)
        other_run_id = _generate_valid_run_id("other0000001")
        other_manifest = _create_sample_manifest(other_run_id)
        mismatch_path.write_text(other_manifest.model_dump_json(), encoding="utf-8")

        with pytest.raises(ManifestStoreCorruptionError, match="run identifier mismatch"):
            store.load_manifest(mismatch_run_id)

    def test_oversized_manifest_rejected(self, tmp_path: Path) -> None:
        """Manifests exceeding MAX_MANIFEST_BYTES (64 KB) are rejected."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("oversize0001")
        oversized_path = store.get_manifest_path(run_id)
        # Write > 64 KB of data
        oversized_path.write_bytes(b" " * (MAX_MANIFEST_BYTES + 10))

        with pytest.raises(ManifestStoreCorruptionError, match="safety size limit"):
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

        # Exactly one must succeed with Path, the other must fail with FileExistsError or ConcurrencyError
        successes = [r for r in results if isinstance(r, Path)]
        failures = [r for r in results if isinstance(r, (ManifestStoreFileExistsError, ManifestStoreConcurrencyError))]

        assert len(successes) == 1
        assert len(failures) == 1

    def test_zero_secrets_policy_enforcement(self, tmp_path: Path) -> None:
        """Manifest containing sensitive secret tokens is strictly rejected before write."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("secret000001")

        # Craft manifest containing a forbidden substring in expected_attributes
        entities = create_default_manifest_entities(run_id)
        # Inject sensitive token into attributes
        entities[0].expected_attributes["bearer token"] = "eyJh...secret"

        leaky_manifest = PerRunOwnershipManifest(
            run_id=run_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=entities,
        )

        with pytest.raises(ManifestStoreSecurityError, match="forbidden secret substring"):
            store.save_manifest(leaky_manifest)

    def test_retention_policy_on_non_clean_states(self, tmp_path: Path) -> None:
        """Manifests are strictly preserved under UNKNOWN, RESIDUAL, or FAILED; only CLEAN unlinks."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("retain000001")
        manifest = _create_sample_manifest(run_id)
        path = store.save_manifest(manifest)

        # UNKNOWN -> preserved
        assert store.delete_manifest_if_clean(run_id, AuditOutcomeState.UNKNOWN) is False
        assert path.exists()

        # RESIDUAL -> preserved
        assert store.delete_manifest_if_clean(run_id, AuditOutcomeState.RESIDUAL) is False
        assert path.exists()

        # OFFLINE_VALIDATION_PASSED -> preserved
        assert store.delete_manifest_if_clean(run_id, AuditOutcomeState.OFFLINE_VALIDATION_PASSED) is False
        assert path.exists()

        # CLEAN -> successfully unlinked
        assert store.delete_manifest_if_clean(run_id, AuditOutcomeState.CLEAN) is True
        assert not path.exists()

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
        # Manifest MUST still exist on disk!
        assert path.exists()

    def test_list_manifest_run_ids(self, tmp_path: Path) -> None:
        """list_manifest_run_ids returns correctly sorted run_ids, ignoring temporary files."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id_1 = _generate_valid_run_id("list00000001")
        time.sleep(0.01)
        run_id_2 = _generate_valid_run_id("list00000002")

        store.save_manifest(_create_sample_manifest(run_id_1))
        store.save_manifest(_create_sample_manifest(run_id_2))

        # Add a noise file
        (store.root_dir / ".noise.tmp").touch()
        (store.root_dir / "random.txt").touch()

        run_ids = store.list_manifest_run_ids()
        assert run_id_1 in run_ids
        assert run_id_2 in run_ids
        assert len(run_ids) == 2

    def test_os_temp_loss_simulation(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Simulate loss/purging of OS tempdir: persistent store retains manifest while temp is wiped."""
        # Dedicated persistent store directory outside temp
        persistent_dir = tmp_path / "durable_store"
        store = HermeticTestManifestStore(persistent_dir)

        # Temporary directory simulation
        simulated_temp = tmp_path / "volatile_temp"
        simulated_temp.mkdir()

        run_id = _generate_valid_run_id("tempdur00001")
        manifest = _create_sample_manifest(run_id)

        # Save to both persistent store and simulated volatile temp
        durable_path = store.save_manifest(manifest)
        volatile_path = manifest.save_atomic(simulated_temp)

        assert durable_path.is_file()
        assert volatile_path.is_file()

        # Simulate OS reboot or storage maintenance wiping the volatile temp directory
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
        """Failure during atomic replace cleans up sibling temp file and leaves target absent."""
        store = HermeticTestManifestStore(tmp_path / "store")
        run_id = _generate_valid_run_id("failren00001")
        manifest = _create_sample_manifest(run_id)

        def mock_replace(src: Path | str, dst: Path | str) -> None:
            raise OSError("Simulated atomic rename I/O failure")

        monkeypatch.setattr(os, "replace", mock_replace)

        with pytest.raises(OSError, match="Simulated atomic rename"):
            store.save_manifest(manifest)

        # Target file must not exist
        target_path = store.get_manifest_path(run_id)
        assert not target_path.exists()

        # No lingering .tmp files in store
        tmp_files = [f for f in store.root_dir.iterdir() if f.name.startswith(".tmp_manifest_")]
        assert len(tmp_files) == 0

    def test_reparse_point_detector_mock(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify _is_reparse_point detects FILE_ATTRIBUTE_REPARSE_POINT (0x0400)."""
        from efso_radar.staging.persistent_manifest_store import _is_reparse_point
        test_file = tmp_path / "file.txt"
        test_file.touch()

        # Normal file
        assert _is_reparse_point(test_file) is False

        # Mock stat with 0x0400 attribute
        class MockStat:
            st_file_attributes = 0x0400

        monkeypatch.setattr(os, "lstat", lambda p: MockStat())
        monkeypatch.setattr(sys, "platform", "win32")
        assert _is_reparse_point(test_file) is True


class TestStoragePlatformGuarantees:
    """Verify explicit platform guarantees and blockers reporting."""

    def test_platform_guarantees_structure(self) -> None:
        """Inspect storage platform guarantees and verify explicit platform blockers."""
        guarantees = get_storage_platform_guarantees()
        assert guarantees.platform in ("win32", "linux", "darwin")
        assert guarantees.fsync_file_supported is True
        assert guarantees.atomic_replace_supported is True
        assert guarantees.lockfile_concurrency_supported is True
        assert isinstance(guarantees.identified_blockers, list)
        assert len(guarantees.identified_blockers) > 0

        if sys.platform == "win32":
            assert guarantees.fsync_directory_supported is False
            assert guarantees.posix_permissions_supported is False
            assert any("Directory fsync" in b for b in guarantees.identified_blockers)
            assert any("POSIX file ownership" in b for b in guarantees.identified_blockers)
        else:
            assert guarantees.fsync_directory_supported is True
            assert guarantees.posix_permissions_supported is True
            assert any("Cross-filesystem" in b for b in guarantees.identified_blockers)
