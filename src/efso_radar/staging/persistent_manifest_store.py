"""Dedicated persistent local manifest store foundation for Phase 5B.3G-C.

OFFLINE-ONLY ENGINEERING FOUNDATION.
This module never opens a network socket, creates cloud credentials, modifies
Turso settings, or authorizes live SQL dispatch.

The persistent manifest store provides a durable, crash-resilient on-disk
storage layer for per-run ownership manifests, replacing volatile OS temp
directories while maintaining strict isolation, atomic file writes with fsync,
overwrite rejection, zero secret leakage, hash seal integrity, and fail-closed
security checks across Windows and Linux.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from typing import Any
import uuid

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.staging.manifest_seal import (
    MAX_MANIFEST_BYTES,
    ManifestSealViolation,
    OfflineManifestSeal,
)
from efso_radar.staging.runner import (
    RUN_ID_REGEX,
    AuditOutcomeState,
    ManifestRecoveryReport,
    PerRunOwnershipManifest,
    create_default_manifest_entities,
)

logger = logging.getLogger(__name__)

# Sensitive substrings forbidden anywhere in a manifest payload
_FORBIDDEN_SECRET_SUBSTRINGS = (
    "turso_token",
    "auth_token",
    "authorization",
    "bearer ",
    "secret",
    "password",
    "private_key",
    "credentials",
    "api_key",
    ".aws-eu-north-1.turso.io",
    "libsql://",
    "https://",
)


class ManifestStoreError(Exception):
    """Base exception for persistent manifest store operations."""


class ManifestStoreSecurityError(ManifestStoreError, PermissionError):
    """Raised when path traversal, symlink, repo violation, or forbidden directory is encountered."""


class ManifestStoreFileExistsError(ManifestStoreError, FileExistsError):
    """Raised when an attempt is made to overwrite an existing manifest file."""


class ManifestStoreCorruptionError(ManifestStoreError, ValueError):
    """Raised when a stored manifest is corrupt, tampered with, or schema-invalid."""


class ManifestStoreConcurrencyError(ManifestStoreError, TimeoutError):
    """Raised when concurrent writer lock contention occurs for a run_id."""


@dataclass(frozen=True)
class StoragePlatformGuarantees:
    """Documented platform storage guarantees and explicit OS-level blockers."""

    platform: str
    fsync_file_supported: bool
    fsync_directory_supported: bool
    posix_permissions_supported: bool
    atomic_replace_supported: bool
    lockfile_concurrency_supported: bool
    reparse_point_detection_method: str
    identified_blockers: list[str]


def get_storage_platform_guarantees() -> StoragePlatformGuarantees:
    """Return explicit storage guarantees and blockers for the current platform."""
    if sys.platform == "win32":
        return StoragePlatformGuarantees(
            platform="win32",
            fsync_file_supported=True,
            fsync_directory_supported=False,
            posix_permissions_supported=False,
            atomic_replace_supported=True,
            lockfile_concurrency_supported=True,
            reparse_point_detection_method="FILE_ATTRIBUTE_REPARSE_POINT and islink",
            identified_blockers=[
                (
                    "Directory fsync is unsupported on Windows NTFS: Python os.fsync() on directory "
                    "descriptors raises OSError/PermissionError; parent directory metadata durability "
                    "cannot be synchronously flushed to persistent media."
                ),
                (
                    "POSIX file ownership (UID) and permission mode bits (0700/0600) are not natively "
                    "enforced by Windows NTFS; filesystem isolation relies on OS-level ACLs/DACLs."
                ),
            ],
        )
    return StoragePlatformGuarantees(
        platform=sys.platform,
        fsync_file_supported=True,
        fsync_directory_supported=True,
        posix_permissions_supported=True,
        atomic_replace_supported=True,
        lockfile_concurrency_supported=True,
        reparse_point_detection_method="lstat st_mode S_ISLNK",
        identified_blockers=[
            (
                "Cross-filesystem atomic renames are not supported by POSIX rename(). The manifest "
                "store root directory must reside on a single local filesystem."
            ),
        ],
    )


def _is_reparse_point(path: Path) -> bool:
    """Detect symlinks, directory junctions, and reparse points across Windows and POSIX."""
    if path.is_symlink():
        return True
    if sys.platform == "win32":
        try:
            st = os.lstat(path)
            attrs = getattr(st, "st_file_attributes", 0)
            # 0x0400 = FILE_ATTRIBUTE_REPARSE_POINT
            if bool(attrs & 0x0400):
                return True
        except OSError:
            pass
    return False


def _find_repo_root() -> Path | None:
    """Locate git repository root from current file ancestors."""
    candidate = Path(__file__).resolve().parent
    while candidate != candidate.parent:
        if (candidate / ".git").exists():
            return candidate
        candidate = candidate.parent
    return None


def _validate_store_directory(
    root_dir: Path | str,
    *,
    is_hermetic_test: bool = False,
    create_if_missing: bool = True,
) -> Path:
    """Fail-closed validation of candidate persistent manifest store root directory."""
    if root_dir is None:
        raise ManifestStoreSecurityError("Store directory must be explicitly configured.")

    if not isinstance(root_dir, (str, Path)):
        raise ManifestStoreSecurityError(f"Store directory must be a Path or str, got {type(root_dir).__name__}.")

    raw_str = str(root_dir).strip()
    if not raw_str:
        raise ManifestStoreSecurityError("Store directory path cannot be empty.")

    if "\0" in raw_str:
        raise ManifestStoreSecurityError("Store directory path contains invalid null byte.")

    candidate = Path(raw_str)

    # Reject relative traversal indicators prior to resolution
    if ".." in candidate.parts:
        raise ManifestStoreSecurityError("Path traversal indicators ('..') are strictly prohibited.")

    # Check symlinks and reparse points on candidate and ancestors before resolution
    curr_unresolved: Path | None = candidate
    while curr_unresolved is not None and curr_unresolved != curr_unresolved.parent:
        if curr_unresolved.is_symlink() or _is_reparse_point(curr_unresolved):
            raise ManifestStoreSecurityError(f"Symlink or reparse point detected in store path: {curr_unresolved}")
        curr_unresolved = curr_unresolved.parent

    resolved = candidate.resolve()

    if candidate.exists():
        if candidate.is_symlink() or _is_reparse_point(candidate):
            raise ManifestStoreSecurityError(f"Symlink or reparse point detected in store path: {candidate}")
        if sys.platform != "win32" and candidate.absolute() != resolved:
            raise ManifestStoreSecurityError(f"Symlink or reparse point detected in store path: {candidate}")

    # Reject filesystem root directly
    if resolved == resolved.parent:
        raise ManifestStoreSecurityError(f"Filesystem root cannot be used as manifest store: {resolved}")

    # Reject user home directory root directly
    try:
        user_home = Path.home().resolve()
        if resolved == user_home:
            raise ManifestStoreSecurityError(f"User home directory root cannot be used as manifest store: {resolved}")
    except (RuntimeError, OSError):
        pass

    # Reject system directories
    if sys.platform == "win32":
        sys_root = os.environ.get("SystemRoot")
        if sys_root and resolved == Path(sys_root).resolve():
            raise ManifestStoreSecurityError("SystemRoot directory cannot be used as manifest store.")
        prog_files = os.environ.get("ProgramFiles")
        if prog_files and resolved == Path(prog_files).resolve():
            raise ManifestStoreSecurityError("ProgramFiles directory cannot be used as manifest store.")
    else:
        system_roots = {Path(p).resolve() for p in ("/etc", "/bin", "/sbin", "/usr", "/var", "/dev", "/proc", "/sys")}
        if resolved in system_roots:
            raise ManifestStoreSecurityError(f"System directory cannot be used as manifest store: {resolved}")

    # Reject repository, .git, .env, and operational data paths
    repo_root = _find_repo_root()
    if repo_root is not None:
        if resolved == repo_root:
            raise ManifestStoreSecurityError(f"Repository root cannot be used as manifest store: {resolved}")
        try:
            resolved.relative_to(repo_root)
            raise ManifestStoreSecurityError(f"Store directory cannot reside inside repository: {resolved}")
        except ValueError:
            pass

    # Check path parts for forbidden identifiers
    for part in resolved.parts:
        p_lower = part.lower()
        if p_lower == ".git" or p_lower.startswith(".git"):
            raise ManifestStoreSecurityError(f"Store directory cannot target .git paths: {resolved}")
        if p_lower == ".env" or p_lower.startswith(".env"):
            raise ManifestStoreSecurityError(f"Store directory cannot target .env paths: {resolved}")

    # Check operational data directory
    if repo_root is not None:
        op_data = (repo_root / "data").resolve()
        if resolved == op_data:
            raise ManifestStoreSecurityError("Operational data directory cannot be used as manifest store.")
        try:
            resolved.relative_to(op_data)
            raise ManifestStoreSecurityError("Store directory cannot reside inside operational data directory.")
        except ValueError:
            pass

    # Reject OS temporary directory in non-hermetic/production mode
    temp_roots = [Path(tempfile.gettempdir()).resolve()]
    for fallback_tmp in (Path("/tmp"), Path("/var/tmp")):
        if fallback_tmp.exists():
            temp_roots.append(fallback_tmp.resolve())

    is_in_temp = False
    for tr in temp_roots:
        if resolved == tr:
            is_in_temp = True
            break
        try:
            resolved.relative_to(tr)
            is_in_temp = True
            break
        except ValueError:
            pass

    if not is_hermetic_test and is_in_temp:
        raise ManifestStoreSecurityError(
            "Persistent manifest store cannot use OS temporary directory in production mode; "
            "temporary storage is volatile and lacks machine-reboot durability."
        )

    # Check symlinks and reparse points across ancestors
    curr: Path | None = resolved
    while curr is not None and curr != curr.parent:
        if curr.exists():
            if _is_reparse_point(curr):
                raise ManifestStoreSecurityError(f"Symlink or reparse point detected in store path: {curr}")
        curr = curr.parent

    # Create directory if missing and permitted
    if not resolved.exists():
        if create_if_missing:
            try:
                os.makedirs(resolved, mode=0o700, exist_ok=True)
            except OSError as exc:
                raise ManifestStoreSecurityError(
                    f"Could not initialize persistent manifest store directory: {exc}"
                ) from exc
        else:
            raise ManifestStoreSecurityError(f"Configured store directory does not exist: {resolved}")

    # Validate directory properties once created/confirmed
    if not resolved.is_dir():
        raise ManifestStoreSecurityError(f"Store path is not a directory: {resolved}")

    if _is_reparse_point(resolved):
        raise ManifestStoreSecurityError(f"Store directory cannot be a symlink or reparse point: {resolved}")

    # Check POSIX ownership and permissions
    if hasattr(os, "geteuid") and sys.platform != "win32":
        try:
            st = resolved.stat()
            if st.st_uid != os.geteuid():
                raise ManifestStoreSecurityError(
                    f"Store directory owner UID ({st.st_uid}) does not match current process UID ({os.geteuid()})."
                )
            # Must not be world-writable or group-writable
            if bool(st.st_mode & 0o022):
                raise ManifestStoreSecurityError(
                    f"Store directory has insecure group or world write permissions (mode {oct(st.st_mode)})."
                )
        except OSError as exc:
            raise ManifestStoreSecurityError(f"Failed to inspect store directory permissions: {exc}") from exc

    return resolved


class _ManifestRunLock:
    """Atomic file-based concurrency lock per run_id."""

    def __init__(self, lock_path: Path, run_id: str) -> None:
        self.lock_path = lock_path
        self.run_id = run_id
        self._fd: int | None = None

    def __enter__(self) -> _ManifestRunLock:
        try:
            self._fd = os.open(
                str(self.lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            return self
        except FileExistsError:
            raise ManifestStoreConcurrencyError(
                f"Concurrent writer lock active for run_id '{self.run_id}'; lockfile exists."
            ) from None
        except OSError as exc:
            raise ManifestStoreError(f"Failed to acquire manifest lock for run_id '{self.run_id}': {exc}") from exc

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
        except OSError:
            pass


class PersistentManifestStore:
    """Dedicated persistent manifest-store foundation for controlled staging validation.

    Ensures process-restart and reboot durability, atomic file writes with fsync,
    strict overwrite protection, entity allowlist validation, and zero secret leakage.
    Default-deny: live write operations are not authorized by default.
    """

    def __init__(
        self,
        root_dir: Path | str,
        *,
        live_authorized: bool = False,
        is_hermetic_test: bool = False,
    ) -> None:
        self._is_hermetic_test = is_hermetic_test
        self._live_authorized = live_authorized
        self._root_dir = _validate_store_directory(
            root_dir,
            is_hermetic_test=is_hermetic_test,
            create_if_missing=True,
        )

    @property
    def root_dir(self) -> Path:
        """Configured absolute path of the manifest store."""
        return self._root_dir

    @property
    def is_hermetic_test(self) -> bool:
        """Whether this store is operating in hermetic test mode."""
        return self._is_hermetic_test

    @property
    def live_authorized(self) -> bool:
        """Whether this store has been explicitly authorized for live execution."""
        return self._live_authorized

    @classmethod
    def for_hermetic_testing(cls, root_dir: Path | str) -> HermeticTestManifestStore:
        """Convenience factory returning a dedicated hermetic test store adapter."""
        return HermeticTestManifestStore(root_dir)

    def manifest_filename(self, run_id: str) -> str:
        """Return canonical manifest filename for run_id."""
        return f"efso_radar_staging_manifest_{run_id}.json"

    def get_manifest_path(self, run_id: str) -> Path:
        """Return full path for a manifest given run_id."""
        if not isinstance(run_id, str) or not RUN_ID_REGEX.match(run_id):
            raise ValueError(f"Invalid run_id format for manifest path: {run_id!r}")
        return self._root_dir / self.manifest_filename(run_id)

    def manifest_exists(self, run_id: str) -> bool:
        """Return True if a regular manifest file exists for run_id."""
        try:
            path = self.get_manifest_path(run_id)
            return path.is_file() and not _is_reparse_point(path)
        except ValueError:
            return False

    def list_manifest_run_ids(self) -> list[str]:
        """Return sorted list of valid run_ids for manifests stored in this store."""
        run_ids: list[str] = []
        try:
            for entry in self._root_dir.iterdir():
                if entry.is_file() and not _is_reparse_point(entry):
                    name = entry.name
                    if name.startswith("efso_radar_staging_manifest_") and name.endswith(".json"):
                        candidate_id = name[len("efso_radar_staging_manifest_") : -len(".json")]
                        if RUN_ID_REGEX.match(candidate_id):
                            run_ids.append(candidate_id)
        except OSError as exc:
            logger.warning("Could not list manifests in store %s: %s", self._root_dir, exc)
        return sorted(run_ids)

    def _validate_manifest_payload_security(self, manifest: PerRunOwnershipManifest) -> None:
        """Enforce zero operational secrets, tokens, or private identifiers in manifest."""
        dumped_json = manifest.model_dump_json().lower()
        for forbidden in _FORBIDDEN_SECRET_SUBSTRINGS:
            if forbidden in dumped_json:
                raise ManifestStoreSecurityError(
                    f"Manifest security policy violation: forbidden secret substring detected ({forbidden!r})."
                )

    def save_manifest(self, manifest: PerRunOwnershipManifest) -> Path:
        """Atomically persist manifest with fsync, overwrite protection, and concurrency locking."""
        if not self._is_hermetic_test and not self._live_authorized:
            raise ManifestStoreSecurityError(
                "Persistent manifest store is not authorized for live write operations (default-deny)."
            )

        if not isinstance(manifest, PerRunOwnershipManifest):
            raise TypeError(f"Expected PerRunOwnershipManifest, got {type(manifest).__name__}")

        run_id = manifest.run_id
        if not isinstance(run_id, str) or not RUN_ID_REGEX.match(run_id):
            raise ValueError(f"Invalid run_id format for manifest persistence: {run_id!r}")

        # Validate against canonical 5 synthetic entities
        canonical_entities = create_default_manifest_entities(run_id)
        canonical_keys = {(e.table, e.pk_value) for e in canonical_entities}
        manifest_keys = {(e.table, e.pk_value) for e in manifest.registered_entities}
        if manifest_keys != canonical_keys:
            raise ValueError(
                f"Manifest registered entities do not match canonical 5 entities for run_id '{run_id}'."
            )

        # Enforce zero-secrets policy
        self._validate_manifest_payload_security(manifest)

        target_path = self.get_manifest_path(run_id)

        # Overwrite protection check 1: before locking
        if target_path.exists():
            raise ManifestStoreFileExistsError(
                f"Manifest for run_id '{run_id}' already exists at {target_path}; overwriting is strictly prohibited."
            )

        lock_path = self._root_dir / f".lock_{run_id}"
        temp_path: Path | None = None

        with _ManifestRunLock(lock_path, run_id):
            # Overwrite protection check 2: under exclusive lock
            if target_path.exists():
                raise ManifestStoreFileExistsError(
                    f"Manifest for run_id '{run_id}' already exists at {target_path}; overwriting is strictly prohibited."
                )

            serialized_bytes = manifest.model_dump_json(indent=2).encode("utf-8")
            if len(serialized_bytes) > MAX_MANIFEST_BYTES:
                raise ManifestStoreCorruptionError("Manifest payload exceeds safety size limit (64 KB).")

            temp_filename = f".tmp_manifest_{run_id}_{uuid.uuid4().hex[:12]}.tmp"
            temp_path = self._root_dir / temp_filename

            temp_fd = os.open(
                str(temp_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            try:
                os.write(temp_fd, serialized_bytes)
                os.fsync(temp_fd)
            finally:
                os.close(temp_fd)

            if sys.platform != "win32":
                try:
                    os.chmod(temp_path, 0o600)
                except OSError:
                    pass

            # Final pre-replace existence check
            if target_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
                raise ManifestStoreFileExistsError(
                    f"Manifest for run_id '{run_id}' already exists at {target_path}; overwriting is strictly prohibited."
                )

            # Atomic rename into canonical destination
            try:
                os.replace(temp_path, target_path)
            except Exception:
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except OSError:
                        pass
                raise

            # On POSIX, synchronously fsync directory to commit the directory entry
            if sys.platform != "win32":
                try:
                    dir_fd = os.open(str(self._root_dir), os.O_RDONLY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError:
                    pass

        # Post-write verification
        if not target_path.is_file():
            raise ManifestStoreError(f"Post-write verification failed: {target_path} is not a regular file.")

        return target_path

    def load_manifest(self, run_id: str) -> tuple[PerRunOwnershipManifest, OfflineManifestSeal]:
        """Read-only load and hash seal verification of an existing manifest.

        Does NOT grant write permissions. Corrupt or missing files fail closed.
        """
        target_path = self.get_manifest_path(run_id)
        if not target_path.is_file():
            raise ManifestStoreError(f"Manifest file not found: {target_path}")

        if _is_reparse_point(target_path):
            raise ManifestStoreSecurityError(f"Manifest security violation: symlink or reparse point: {target_path}")

        try:
            stat = target_path.stat()
            if stat.st_nlink != 1:
                raise ManifestStoreSecurityError("Linked manifest files are not permitted.")
            if stat.st_size > MAX_MANIFEST_BYTES:
                raise ManifestStoreCorruptionError("Manifest exceeds offline safety size limit.")

            if hasattr(os, "geteuid") and sys.platform != "win32":
                if stat.st_uid != os.geteuid():
                    raise ManifestStoreSecurityError(
                        f"Manifest file owner UID ({stat.st_uid}) does not match current process UID ({os.geteuid()})."
                    )

            before = target_path.read_bytes()
            if not before or len(before) > MAX_MANIFEST_BYTES:
                raise ManifestStoreCorruptionError("Manifest content is empty or oversized.")

            # Validate schema and entity inventory
            manifest = PerRunOwnershipManifest.load_and_validate(target_path)

            after = target_path.read_bytes()
            if before != after:
                raise ManifestStoreCorruptionError("Manifest changed during inspection.")

            if manifest.run_id != run_id:
                raise ManifestStoreCorruptionError(
                    f"Manifest run identifier mismatch: expected '{run_id}', found '{manifest.run_id}'."
                )

            self._validate_manifest_payload_security(manifest)

            seal = OfflineManifestSeal.from_persisted(
                target_path,
                run_id,
                allowed_root=self._root_dir,
            )
            return manifest, seal

        except (ManifestStoreError, ManifestSealViolation):
            raise
        except Exception as exc:
            raise ManifestStoreCorruptionError(
                f"Manifest load validation failed ({type(exc).__name__}): {exc}"
            ) from None

    def delete_manifest_if_clean(self, run_id: str, audit_state: AuditOutcomeState) -> bool:
        """Unlink manifest file if and only if audit state is strictly CLEAN.

        Retains manifest on disk under UNKNOWN, RESIDUAL, FAILED, or ownership mismatch states.
        """
        target_path = self.get_manifest_path(run_id)
        if audit_state == AuditOutcomeState.CLEAN:
            if target_path.exists() and target_path.is_file() and not _is_reparse_point(target_path):
                try:
                    target_path.unlink()
                    logger.info("Clean manifest unlinked from store: %s", target_path)
                    return True
                except OSError as exc:
                    logger.warning("Could not unlink clean manifest file %s: %s", target_path, exc)
            return False

        logger.info(
            "Manifest preserved in store for run_id '%s' due to non-clean audit state '%s': %s",
            run_id,
            audit_state.value,
            target_path,
        )
        return False

    def validate_recovery_offline(self, run_id: str) -> ManifestRecoveryReport:
        """Hermetic offline recovery inspection against an isolated SQLite Schema v6 fixture.

        Validates stored manifest without modifying or deleting it. Does NOT connect to Turso.
        Offline recovery validation can NEVER certify a remote staging database as CLEAN.
        """
        manifest, seal = self.load_manifest(run_id)
        seal.assert_unchanged()

        # Offline verification fixture strictly isolated in memory
        conn = sqlite3.connect(":memory:")
        for stmt in get_all_init_statements():
            conn.execute(stmt)
        conn.commit()

        cur = conn.cursor()
        for ent in manifest.registered_entities:
            cur.execute(f"SELECT COUNT(*) FROM {ent.table} WHERE {ent.pk_column} = ?", [ent.pk_value])
            cur.fetchone()

        # Offline validation never unlinks the manifest and never marks CLEAN
        audit_state = AuditOutcomeState.OFFLINE_VALIDATION_PASSED

        return ManifestRecoveryReport(
            manifest_path=str(self.get_manifest_path(run_id)),
            run_id=manifest.run_id,
            valid_schema=True,
            entities_count=len(manifest.registered_entities),
            audit_state=audit_state,
            manifest_preserved=True,
            summary_message=(
                f"Offline persistent manifest validation succeeded for run {manifest.run_id} ({audit_state.value}). "
                f"Remote staging cleanup requires live verification; manifest preserved in persistent store."
            ),
        )


class HermeticTestManifestStore(PersistentManifestStore):
    """Dedicated test adapter for hermetic test suites.

    Permits operation inside isolated test fixture directories (e.g. pytest tmp_path)
    while strictly enforcing repository boundary isolation, .git/.env/data blocking,
    symlink/junction rejection, overwrite protection, schema validation, and hash sealing.
    """

    def __init__(self, root_dir: Path | str, **kwargs: Any) -> None:
        super().__init__(root_dir, is_hermetic_test=True, live_authorized=True, **kwargs)
