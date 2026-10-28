"""Dedicated persistent local manifest store foundation for Phase 5B.3G-C.

OFFLINE-ONLY ENGINEERING FOUNDATION.
This module never opens a network socket, creates cloud credentials, modifies
Turso settings, or authorizes live SQL dispatch.

The persistent manifest store provides a candidate local on-disk storage layer
for per-run ownership manifests, replacing volatile OS temp directories while
enforcing strict isolation, atomic file writes with fsync, overwrite rejection,
zero secret leakage, hash seal integrity, and fail-closed security checks
across Windows and Linux.

Platform limitations:
- Windows NTFS does not support directory fsync via Python standard library;
  parent directory metadata durability cannot be synchronously flushed.
- Windows NTFS DACL/ACL owner validation is not natively supported by Python
  standard library; live persistent store cannot be safely initialized on Windows.
- True reboot and power-loss durability depend on host storage hardware, write
  caches, and operating system guarantees; in-process re-instantiation simulates
  store reopening, not physical power-loss or host reboot.
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
import stat
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
    atomic_no_overwrite_supported: bool
    atomic_replace_supported: bool
    lockfile_concurrency_supported: bool
    reparse_point_detection_method: str
    identified_blockers: list[str]

    def verify_platform_policy(self, *, is_hermetic_test: bool = False) -> None:
        """Verify that current platform satisfies minimum policy requirements."""
        if not is_hermetic_test:
            if self.platform == "win32":
                raise ManifestStoreSecurityError(
                    "Windows NTFS platform cannot programmatically verify DACL/ACL owner-private "
                    "isolation via Python standard library; live persistent store cannot be authorized."
                )
            if not self.fsync_directory_supported:
                raise ManifestStoreSecurityError(
                    f"Platform '{self.platform}' does not support directory fsync; parent directory metadata "
                    "cannot be committed to persistent media."
                )


def get_storage_platform_guarantees() -> StoragePlatformGuarantees:
    """Return explicit storage guarantees and blockers for the current platform."""
    if sys.platform == "win32":
        return StoragePlatformGuarantees(
            platform="win32",
            fsync_file_supported=True,
            fsync_directory_supported=False,
            posix_permissions_supported=False,
            atomic_no_overwrite_supported=True,
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
                    "Windows NTFS DACL/ACL verification is not natively supported by Python standard "
                    "library (requires pywin32/win32security); persistent store directory cannot "
                    "programmatically verify owner-private ACL isolation on Windows."
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
        atomic_no_overwrite_supported=True,
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

    # Check OS temporary directory restrictions:
    # 1. Non-hermetic/production mode: cannot use OS temporary directory at all.
    # 2. Hermetic test mode: MUST reside inside an authorized test fixture tree (e.g. pytest tmp_path)
    #    and CANNOT be an arbitrary user or system directory.
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

    if is_hermetic_test:
        in_authorized_test_root = False
        for tr in temp_roots:
            if resolved != tr:
                try:
                    resolved.relative_to(tr)
                    in_authorized_test_root = True
                    break
                except ValueError:
                    pass
        if not in_authorized_test_root:
            raise ManifestStoreSecurityError(
                f"HermeticTestManifestStore is strictly restricted to isolated test fixture directories "
                f"within temporary storage (e.g. pytest tmp_path); arbitrary directory '{resolved}' is rejected."
            )

    # Windows DACL/ACL verification blocker check for live production mode
    if sys.platform == "win32" and not is_hermetic_test:
        raise ManifestStoreSecurityError(
            "Windows NTFS DACL/ACL verification is not natively supported by Python standard library; "
            "live persistent manifest store initialization on Windows is blocked pending ACL enforcement primitives."
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

    # Check POSIX ownership and permissions (must be strictly owner-private: 0700)
    if sys.platform != "win32":
        try:
            st = resolved.stat()
            if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
                raise ManifestStoreSecurityError(
                    f"Store directory owner UID ({st.st_uid}) does not match current process UID ({os.geteuid()})."
                )
            # Directory must be strictly owner-private: mode 0700 (stat.S_IRWXU only)
            # Reject any group or world read, write, or execute permissions (0o077)
            if bool(st.st_mode & 0o077):
                raise ManifestStoreSecurityError(
                    f"Store directory has insecure permissions (mode {oct(st.st_mode & 0o777)}): "
                    f"group and world read/write/execute permissions (0o077) are strictly prohibited; "
                    f"mode 0700 required."
                )
        except OSError as exc:
            raise ManifestStoreSecurityError(f"Failed to inspect store directory permissions: {exc}") from exc

    return resolved


def _publish_file_no_overwrite(temp_path: Path, target_path: Path) -> None:
    """Atomically publish temporary sibling file to target_path without overwriting.

    Fails closed with ManifestStoreFileExistsError if target_path exists prior to,
    or concurrently during, the publication attempt. Handles dangling symlinks,
    junctions, and race conditions across Windows and POSIX.
    """
    # 1. Pre-publication check (including dangling symlinks and reparse points)
    if os.path.lexists(str(target_path)) or target_path.is_symlink() or _is_reparse_point(target_path):
        try:
            temp_path.unlink()
        except OSError:
            pass
        raise ManifestStoreFileExistsError(
            f"Publication blocked: target path '{target_path}' already exists (or is a dangling symlink); "
            "overwriting is strictly prohibited."
        )

    if sys.platform == "win32":
        # On Windows, os.rename uses MoveFileWithProgressW without MOVEFILE_REPLACE_EXISTING.
        # If target_path exists (or is created concurrently), it raises FileExistsError (WinError 183 / 80).
        try:
            os.rename(str(temp_path), str(target_path))
        except FileExistsError as exc:
            try:
                temp_path.unlink()
            except OSError:
                pass
            raise ManifestStoreFileExistsError(
                f"Publication blocked: target '{target_path}' already exists on Windows (FileExistsError): {exc}"
            ) from exc
        except OSError as exc:
            try:
                temp_path.unlink()
            except OSError:
                pass
            if getattr(exc, "winerror", None) in (80, 183):
                raise ManifestStoreFileExistsError(
                    f"Publication blocked: target '{target_path}' already exists (WinError {getattr(exc, 'winerror', None)}): {exc}"
                ) from exc
            raise ManifestStoreError(f"Atomic rename publication failed on Windows: {exc}") from exc
    else:
        # On POSIX / Linux:
        published = False
        # Try renameat2 with RENAME_NOREPLACE (Linux kernel 3.15+)
        try:
            import ctypes
            libc = ctypes.CDLL(None, use_errno=True)
            if hasattr(libc, "renameat2"):
                libc.renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
                libc.renameat2.restype = ctypes.c_int
                # AT_FDCWD = -100, RENAME_NOREPLACE = 1
                ret = libc.renameat2(
                    -100,
                    str(temp_path).encode("utf-8"),
                    -100,
                    str(target_path).encode("utf-8"),
                    1,
                )
                if ret == 0:
                    published = True
                else:
                    errno_val = ctypes.get_errno()
                    if errno_val == 17:  # EEXIST
                        try:
                            temp_path.unlink()
                        except OSError:
                            pass
                        raise ManifestStoreFileExistsError(
                            f"Publication blocked: target '{target_path}' already exists (renameat2 RENAME_NOREPLACE EEXIST)."
                        )
        except (AttributeError, OSError, TypeError):
            pass

        if not published:
            # Fall back to os.link() then os.unlink().
            # In POSIX, os.link(temp, target) creates hard link; if target exists (even dangling symlink),
            # link fails atomically with FileExistsError (EEXIST) without modifying target.
            try:
                os.link(str(temp_path), str(target_path))
                os.unlink(str(temp_path))
                published = True
            except FileExistsError as exc:
                try:
                    temp_path.unlink()
                except OSError:
                    pass
                raise ManifestStoreFileExistsError(
                    f"Publication blocked: target '{target_path}' already exists (link EEXIST): {exc}"
                ) from exc
            except OSError as exc:
                try:
                    temp_path.unlink()
                except OSError:
                    pass
                if exc.errno == 17:  # EEXIST
                    raise ManifestStoreFileExistsError(
                        f"Publication blocked: target '{target_path}' already exists: {exc}"
                    ) from exc
                raise ManifestStoreError(f"Atomic link publication failed on POSIX: {exc}") from exc


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
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
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

    Candidate local persistent store foundation with documented platform limitations.
    Provides durable, crash-resilient local manifest storage as a candidate replacement
    for volatile OS temp storage, enforcing atomic file writes with fsync, strict overwrite
    protection, entity allowlist validation, and zero secret leakage.

    Default-deny: live write operations are strictly disabled in this offline-only foundation.
    Passing live_authorized=True is not an authorized security grant.
    """

    def __init__(
        self,
        root_dir: Path | str,
        *,
        live_authorized: bool = False,
        is_hermetic_test: bool = False,
    ) -> None:
        # In this offline foundation, live authorization is strictly disabled.
        # Boolean self-authorization via live_authorized=True is neutralized and rejected.
        self._is_hermetic_test = is_hermetic_test
        self._live_authorized = False
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
        """Whether this store has been authorized for live execution (always False offline)."""
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
        if not self._is_hermetic_test:
            raise ManifestStoreSecurityError(
                "Live persistent manifest store writes are strictly disabled in this offline-only foundation. "
                "Passing live_authorized=True is not an authorized security grant."
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

        # Overwrite protection check 1: before locking (catches existing files and dangling symlinks)
        if os.path.lexists(str(target_path)) or target_path.is_symlink() or _is_reparse_point(target_path):
            raise ManifestStoreFileExistsError(
                f"Manifest for run_id '{run_id}' already exists at {target_path}; overwriting is strictly prohibited."
            )

        lock_path = self._root_dir / f".lock_{run_id}"

        with _ManifestRunLock(lock_path, run_id):
            # Overwrite protection check 2: under exclusive lock
            if os.path.lexists(str(target_path)) or target_path.is_symlink() or _is_reparse_point(target_path):
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
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
                0o600,
            )
            try:
                # Write-all loop: verify every single byte is written
                total_written = 0
                total_bytes = len(serialized_bytes)
                while total_written < total_bytes:
                    n = os.write(temp_fd, serialized_bytes[total_written:])
                    if n <= 0:
                        raise OSError(f"Short write encountered: {n} bytes written, {total_bytes - total_written} remaining.")
                    total_written += n

                if total_written != total_bytes:
                    raise ManifestStoreCorruptionError(
                        f"Incomplete write: wrote {total_written} of {total_bytes} bytes."
                    )

                # File fsync: flush dirty file buffers to physical storage media
                os.fsync(temp_fd)
            except Exception:
                try:
                    os.close(temp_fd)
                except OSError:
                    pass
                try:
                    temp_path.unlink()
                except OSError:
                    pass
                raise
            else:
                os.close(temp_fd)

            if sys.platform != "win32":
                try:
                    os.chmod(temp_path, 0o600)
                except OSError:
                    pass

            # Pre-publication readback verification on temporary file
            try:
                temp_bytes = temp_path.read_bytes()
                if len(temp_bytes) != total_bytes:
                    temp_path.unlink(missing_ok=True)
                    raise ManifestStoreCorruptionError(
                        f"Pre-publication verification failed: expected {total_bytes} bytes, read {len(temp_bytes)} bytes."
                    )
                if hashlib.sha256(temp_bytes).hexdigest() != hashlib.sha256(serialized_bytes).hexdigest():
                    temp_path.unlink(missing_ok=True)
                    raise ManifestStoreCorruptionError(
                        "Pre-publication verification failed: SHA-256 hash mismatch on temporary file."
                    )
            except Exception:
                try:
                    temp_path.unlink()
                except OSError:
                    pass
                raise

            # Atomic publication without overwrite
            try:
                _publish_file_no_overwrite(temp_path, target_path)
            except Exception:
                try:
                    temp_path.unlink()
                except OSError:
                    pass
                raise

            # Synchronously fsync directory on POSIX to commit directory entry
            if sys.platform != "win32":
                try:
                    dir_fd = os.open(str(self._root_dir), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError as exc:
                    try:
                        target_path.unlink()
                    except OSError:
                        pass
                    raise ManifestStoreError(
                        f"Parent directory fsync failed on '{self._root_dir}': {exc}"
                    ) from exc

        # Post-write verification on published file
        if not target_path.is_file():
            raise ManifestStoreError(f"Post-write verification failed: {target_path} is not a regular file.")
        published_bytes = target_path.read_bytes()
        if published_bytes != serialized_bytes:
            raise ManifestStoreCorruptionError(
                "Post-write content verification failed: published file content differs from serialized manifest."
            )

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
            stat_res = target_path.stat()
            if stat_res.st_nlink != 1:
                raise ManifestStoreSecurityError("Linked manifest files are not permitted.")
            if stat_res.st_size > MAX_MANIFEST_BYTES:
                raise ManifestStoreCorruptionError("Manifest exceeds offline safety size limit.")

            if sys.platform != "win32":
                if hasattr(os, "geteuid") and stat_res.st_uid != os.geteuid():
                    raise ManifestStoreSecurityError(
                        f"Manifest file owner UID ({stat_res.st_uid}) does not match current process UID ({os.geteuid()})."
                    )
                # File permissions must be strictly owner-private: 0o600
                if bool(stat_res.st_mode & 0o077):
                    raise ManifestStoreSecurityError(
                        f"Manifest file has insecure permissions (mode {oct(stat_res.st_mode & 0o777)}): "
                        f"group and world access bits (0o077) are strictly prohibited; mode 0600 required."
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
        """Unlink manifest file if and only if independent, verified remote CLEAN audit is established.

        In this OFFLINE-ONLY foundation, persistent manifest deletion is strictly disabled / fail-closed.
        Caller-supplied AuditOutcomeState.CLEAN is rejected because offline modules cannot produce
        independent remote audit proofs. Manifest is always preserved.
        """
        target_path = self.get_manifest_path(run_id)
        if audit_state == AuditOutcomeState.CLEAN:
            logger.warning(
                "Persistent manifest deletion rejected: offline foundation cannot verify remote CLEAN; "
                "direct unlinking is strictly disabled. Manifest preserved at %s",
                target_path,
            )
            raise ManifestStoreSecurityError(
                f"Persistent manifest deletion is strictly disabled in offline foundation; "
                f"direct unlinking via caller-supplied CLEAN is prohibited. Manifest preserved at {target_path}."
            )

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

    Permits operation strictly inside isolated test fixture directories (e.g. pytest tmp_path)
    within temporary storage, while enforcing repository boundary isolation, .git/.env/data blocking,
    symlink/junction rejection, overwrite protection, schema validation, and hash sealing.
    Arbitrary user, system, or non-temp directories are strictly rejected.
    """

    def __init__(self, root_dir: Path | str, **kwargs: Any) -> None:
        super().__init__(root_dir, is_hermetic_test=True, live_authorized=False, **kwargs)
