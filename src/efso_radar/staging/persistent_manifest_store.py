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
- Windows DACL inspection is limited to offline test fixtures; owner-host live
  attestation is not implemented and live persistent storage remains disabled.
- True reboot and power-loss durability depend on host storage hardware, write
  caches, and operating system guarantees; in-process re-instantiation simulates
  store reopening, not physical power-loss or host reboot.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timezone
import errno
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

# Preserve unpatched os.write for internal lockfile ownership marking
_raw_os_write = os.write

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


class ManifestStorePostPublicationError(ManifestStoreError):
    """Raised when a failure occurs after the canonical manifest file has been published.

    Under fail-closed policy, the canonical manifest file is intentionally preserved on disk
    for recovery and forensic investigation, and MUST NOT be deleted or overwritten.
    """


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
                    "for live storage from offline fixture evidence; live persistent store cannot be authorized."
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
                    "Windows NTFS DACL/ACL live attestation remains unimplemented; standalone "
                    "read-only test-fixture inspection cannot authorize owner-host storage."
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


class ManifestCrashWindow(str, Enum):
    """Formal taxonomy of critical failure windows during manifest persistence."""

    WINDOW_1_TEMP_CREATION = "WINDOW_1_TEMP_CREATION"
    WINDOW_2_PARTIAL_WRITE = "WINDOW_2_PARTIAL_WRITE"
    WINDOW_3_FILE_FSYNC = "WINDOW_3_FILE_FSYNC"
    WINDOW_4_PRE_PUBLICATION_VERIFY = "WINDOW_4_PRE_PUBLICATION_VERIFY"
    WINDOW_5_PUBLICATION_COLLISION = "WINDOW_5_PUBLICATION_COLLISION"
    WINDOW_6_DIRECTORY_DURABILITY_GAP = "WINDOW_6_DIRECTORY_DURABILITY_GAP"
    WINDOW_7_POST_PUBLICATION_VERIFY = "WINDOW_7_POST_PUBLICATION_VERIFY"
    WINDOW_8_RECOVERY_REOPEN = "WINDOW_8_RECOVERY_REOPEN"


@dataclass(frozen=True)
class CrashWindowContract:
    """Documented invariants and fail-closed outcomes per crash window."""

    window: ManifestCrashWindow
    description: str
    expected_canonical_state: str  # "ABSENT" | "PRESERVED"
    expected_temp_state: str       # "CLEANED" | "ORPHANED_INERT"
    expected_outcome: str          # "FAILED_CLOSED" | "UNCERTAIN_HALT"
    retry_permitted: bool = False
    remote_write_authorized: bool = False


_CRASH_WINDOW_CONTRACTS: dict[ManifestCrashWindow, CrashWindowContract] = {
    ManifestCrashWindow.WINDOW_1_TEMP_CREATION: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_1_TEMP_CREATION,
        description="Failure before or during temporary file creation (O_CREAT / permission / descriptor exhaustion).",
        expected_canonical_state="ABSENT",
        expected_temp_state="CLEANED",
        expected_outcome="FAILED_CLOSED",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
    ManifestCrashWindow.WINDOW_2_PARTIAL_WRITE: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_2_PARTIAL_WRITE,
        description="Process crash, disk full (ENOSPC), or short write during payload serialization.",
        expected_canonical_state="ABSENT",
        expected_temp_state="CLEANED",
        expected_outcome="FAILED_CLOSED",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
    ManifestCrashWindow.WINDOW_3_FILE_FSYNC: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_3_FILE_FSYNC,
        description="File fsync failure flushing dirty buffer cache pages to drive media.",
        expected_canonical_state="ABSENT",
        expected_temp_state="CLEANED",
        expected_outcome="FAILED_CLOSED",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
    ManifestCrashWindow.WINDOW_4_PRE_PUBLICATION_VERIFY: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_4_PRE_PUBLICATION_VERIFY,
        description="Pre-publication readback verification failure (torn write or hash mismatch on temp file).",
        expected_canonical_state="ABSENT",
        expected_temp_state="CLEANED",
        expected_outcome="FAILED_CLOSED",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
    ManifestCrashWindow.WINDOW_5_PUBLICATION_COLLISION: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_5_PUBLICATION_COLLISION,
        description="Atomic publication race or preexisting target file (no-overwrite collision).",
        expected_canonical_state="PRESERVED",
        expected_temp_state="CLEANED",
        expected_outcome="FAILED_CLOSED",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
    ManifestCrashWindow.WINDOW_6_DIRECTORY_DURABILITY_GAP: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_6_DIRECTORY_DURABILITY_GAP,
        description="Publication succeeded but directory entry durability is unproven (Windows NTFS gap / POSIX dir fsync failure).",
        expected_canonical_state="PRESERVED",
        expected_temp_state="CLEANED",
        expected_outcome="UNCERTAIN_HALT",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
    ManifestCrashWindow.WINDOW_7_POST_PUBLICATION_VERIFY: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_7_POST_PUBLICATION_VERIFY,
        description="Post-publication readback, integrity check, or hash mismatch failure on canonical file.",
        expected_canonical_state="PRESERVED",
        expected_temp_state="CLEANED",
        expected_outcome="UNCERTAIN_HALT",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
    ManifestCrashWindow.WINDOW_8_RECOVERY_REOPEN: CrashWindowContract(
        window=ManifestCrashWindow.WINDOW_8_RECOVERY_REOPEN,
        description="Subprocess crash or unclean termination recovery; reopening store detects state without silent corruption.",
        expected_canonical_state="PRESERVED",
        expected_temp_state="ORPHANED_INERT",
        expected_outcome="UNCERTAIN_HALT",
        retry_permitted=False,
        remote_write_authorized=False,
    ),
}


def get_crash_window_contracts() -> dict[ManifestCrashWindow, CrashWindowContract]:
    """Return immutable dictionary of failure window invariants and safety contracts."""
    return dict(_CRASH_WINDOW_CONTRACTS)


def verify_pre_remote_write_durability_gate(
    guarantees: StoragePlatformGuarantees | None = None,
    *,
    is_hermetic_test: bool = False,
) -> None:
    """Enforce testable boundary contract: absence of provable durable ownership blocks all remote mutation.

    A success report based on file flush or process restart alone CANNOT authorize LIVE staging.
    Under Windows NTFS, parent directory metadata fsync is unproven via Python standard library;
    any non-hermetic remote write attempt must fail closed until an explicit PO architectural decision
    and verified durable mechanism are approved.
    """
    if is_hermetic_test:
        return
    if guarantees is None:
        guarantees = get_storage_platform_guarantees()
    if not guarantees.fsync_directory_supported or guarantees.platform == "win32":
        raise ManifestStoreSecurityError(
            f"Pre-remote-write durability gate failed on platform '{guarantees.platform}': "
            "parent directory metadata durability is unproven on disk. Remote mutations strictly blocked."
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


def _is_authorized_test_fixture(resolved: Path) -> bool:
    """Verify that path is strictly within an explicitly authorized, validated test fixture root.

    Substring matching on 'pytest' is strictly prohibited as it does not constitute
    proof of isolation or ownership. A directory is authorized IF AND ONLY IF it is
    equal to or a subpath of an explicitly registered, validated fixture root in
    HermeticTestManifestStore._AUTHORIZED_FIXTURE_ROOTS.
    """
    for auth_root in HermeticTestManifestStore._AUTHORIZED_FIXTURE_ROOTS:
        if resolved == auth_root:
            return True
        try:
            resolved.relative_to(auth_root)
            return True
        except ValueError:
            pass
    return False


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
        if not _is_authorized_test_fixture(resolved):
            raise ManifestStoreSecurityError(
                f"HermeticTestManifestStore is strictly restricted to ownership-verified isolated test fixture "
                f"directories (e.g. pytest tmp_path); arbitrary or unverified temporary directory '{resolved}' is rejected."
            )

    # Windows DACL/ACL verification blocker check for live production mode
    if sys.platform == "win32" and not is_hermetic_test:
        raise ManifestStoreSecurityError(
            "Windows NTFS DACL/ACL verification for live storage remains unimplemented; "
            "offline fixture evidence cannot authorize persistent manifest store initialization on Windows."
        )

    # Check symlinks and reparse points across ancestors
    curr: Path | None = resolved
    while curr is not None and curr != curr.parent:
        if curr.exists():
            if _is_reparse_point(curr):
                raise ManifestStoreSecurityError(f"Symlink or reparse point detected in store path: {curr}")
        curr = curr.parent

    existed_before = candidate.exists() or resolved.exists()

    # Create directory if missing and permitted
    if not resolved.exists():
        if create_if_missing:
            try:
                os.makedirs(resolved, mode=0o700, exist_ok=True)
                if sys.platform != "win32":
                    try:
                        os.chmod(resolved, 0o700)
                    except OSError:
                        pass
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
    # Pre-existing directories MUST NOT have their permissions mutated!
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
                if existed_before:
                    raise ManifestStoreSecurityError(
                        f"Pre-existing directory has insecure permissions (mode {oct(st.st_mode & 0o777)}): "
                        f"group and world read/write/execute permissions (0o077) are strictly prohibited; "
                        f"mutating permissions of pre-existing directories is strictly prohibited. Mode 0700 required."
                    )
                raise ManifestStoreSecurityError(
                    f"Store directory has insecure permissions (mode {oct(st.st_mode & 0o777)}): "
                    f"group and world read/write/execute permissions (0o077) are strictly prohibited; "
                    f"mode 0700 required."
                )
        except OSError as exc:
            if isinstance(exc, ManifestStoreSecurityError):
                raise
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
                    if errno_val == errno.EEXIST:  # 17
                        try:
                            temp_path.unlink()
                        except OSError:
                            pass
                        raise ManifestStoreFileExistsError(
                            f"Publication blocked: target '{target_path}' already exists (renameat2 RENAME_NOREPLACE EEXIST)."
                        )

                    # Only allow fallback to link() if renameat2 is explicitly NOT supported by the OS/filesystem:
                    # ENOSYS (syscall not implemented), EINVAL (flag RENAME_NOREPLACE not supported by fs),
                    # or EOPNOTSUPP/ENOTSUP (operation not supported on this filesystem).
                    unsupported_errnos = {
                        errno.ENOSYS,
                        errno.EINVAL,
                        getattr(errno, "EOPNOTSUPP", 95),
                        getattr(errno, "ENOTSUP", 95),
                    }
                    if errno_val not in unsupported_errnos:
                        # Real filesystem or permission error (EIO, EPERM, EACCES, EROFS, ENOSPC, etc.)
                        # Blind fallback to link() is strictly prohibited!
                        try:
                            temp_path.unlink()
                        except OSError:
                            pass
                        raise ManifestStoreError(
                            f"Atomic publication failed in renameat2 with errno {errno_val} "
                            f"({os.strerror(errno_val)}): blind fallback to link prohibited."
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
    """Atomic file-based concurrency lock per run_id with verified ownership tracking."""

    def __init__(self, lock_path: Path, run_id: str) -> None:
        self.lock_path = lock_path
        self.run_id = run_id
        self._fd: int | None = None
        self._opened_by_us: bool = False
        self._acquired: bool = False
        self._lock_token: str = f"pid={os.getpid()}:run_id={run_id}:{uuid.uuid4().hex}"
        self._dev: int | None = None
        self._ino: int | None = None

    def _release_lock_if_owned(self) -> None:
        """Release lockfile only if this instance demonstrably owns it.

        Never unlinks foreign lockfiles under any circumstances. Checks file descriptor,
        inode/device identity across platforms, and strict ownership token content before unlinking.
        Empty or missing content alone is NEVER treated as proof of ownership.
        """
        opened_by_us = self._opened_by_us
        was_acquired = self._acquired
        captured_ino = self._ino
        captured_dev = self._dev

        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None

        # If this instance never even opened the lockfile and never acquired it, we have zero ownership
        if not opened_by_us and not was_acquired:
            return

        try:
            if not os.path.lexists(str(self.lock_path)) or self.lock_path.is_symlink() or _is_reparse_point(self.lock_path):
                return

            st = self.lock_path.stat()

            # File identity check (inode & device verification)
            if captured_ino is not None and captured_dev is not None:
                if st.st_ino != captured_ino or st.st_dev != captured_dev:
                    # Inode mismatch: lockfile was replaced or is foreign! Do not unlink!
                    return
            elif not was_acquired:
                # If the lock was not fully acquired, inode/file identity is mandatory.
                # Without verified file identity from an active open descriptor, an unacquired
                # or incomplete lock cannot be unlinked under any circumstances.
                return

            # Token content verification
            try:
                content = self.lock_path.read_text(encoding="utf-8")
            except OSError:
                return

            # When lock was fully acquired, content must match self._lock_token exactly.
            if was_acquired:
                if content != self._lock_token:
                    # Foreign or altered lockfile content: do not unlink under any circumstances!
                    return
            else:
                # Acquisition was NOT completed (failed during token write).
                # Empty or missing content alone is NEVER treated as proof of ownership!
                # Unlinking incomplete lock is only permitted if this instance demonstrably
                # opened the file descriptor during the active acquisition attempt (opened_by_us is True),
                # AND file identity (inode/dev) matched above.
                if not opened_by_us:
                    return
                # If bytes were written, they must be a prefix of our token.
                # Foreign content or content differing from our token prefix is never unlinked.
                if content and not self._lock_token.startswith(content):
                    return

            self.lock_path.unlink()
        except OSError:
            pass
        finally:
            self._acquired = False
            self._opened_by_us = False

    def __enter__(self) -> _ManifestRunLock:
        try:
            self._fd = os.open(
                str(self.lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
                0o600,
            )
            self._opened_by_us = True
            # Capture device and file identity (inode/file index) while fd is open
            try:
                st = os.fstat(self._fd)
                if getattr(st, "st_ino", 0) and getattr(st, "st_dev", 0):
                    self._dev = st.st_dev
                    self._ino = st.st_ino
            except OSError:
                pass

            # Write ownership token to lockfile using unpatched write
            payload = self._lock_token.encode("utf-8")
            written = 0
            while written < len(payload):
                n = _raw_os_write(self._fd, payload[written:])
                if n <= 0:
                    raise OSError("Failed to write lock ownership token.")
                written += n

            self._acquired = True
            return self
        except FileExistsError:
            raise ManifestStoreConcurrencyError(
                f"Concurrent writer lock active for run_id '{self.run_id}'; lockfile exists."
            ) from None
        except Exception as exc:
            # On acquisition failure: ONLY release if we demonstrably created and own it.
            # Foreign lockfiles are never touched.
            self._release_lock_if_owned()
            raise ManifestStoreError(f"Failed to acquire manifest lock for run_id '{self.run_id}': {exc}") from exc

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._release_lock_if_owned()


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

    def _validate_manifest_canonical_entities(self, manifest: PerRunOwnershipManifest, run_id: str) -> None:
        """Strictly validate registered entities against canonical allowlist before persisting.

        Enforces:
        - Exactly 5 registered entities.
        - Exactly 5 unique (table, pk_value) keys.
        - Exact match for each entity:
          - pk_column matches canonical pk_column
          - cleanup_dependency_order matches canonical order
          - expected_attributes matches canonical expected_attributes exactly
            (rejects invalid title, altered worker_ids/stage/worker_id/source/category ownership markers).
        """
        registered = manifest.registered_entities
        if not isinstance(registered, list):
            raise ValueError("Manifest registered_entities must be a list.")

        if len(registered) != 5:
            raise ValueError(f"Manifest must contain exactly 5 registered entities, found {len(registered)}.")

        seen_keys: set[tuple[str, str]] = set()
        for ent in registered:
            key = (ent.table, ent.pk_value)
            if key in seen_keys:
                raise ValueError(f"Duplicate entity registration in manifest: {key}")
            seen_keys.add(key)

        canonical_entities = create_default_manifest_entities(run_id)
        canonical_map = {(e.table, e.pk_value): e for e in canonical_entities}

        missing_canonical = set(canonical_map.keys()) - seen_keys
        if missing_canonical:
            raise ValueError(f"Manifest missing canonical entities: {sorted(missing_canonical)}.")

        extra_keys = seen_keys - set(canonical_map.keys())
        if extra_keys:
            raise ValueError(f"Manifest contains unexpected non-canonical entities: {sorted(extra_keys)}.")

        for ent in registered:
            key = (ent.table, ent.pk_value)
            canonical_ent = canonical_map[key]

            if ent.pk_column != canonical_ent.pk_column:
                raise ValueError(
                    f"Entity {key} has unexpected pk_column '{ent.pk_column}', "
                    f"expected '{canonical_ent.pk_column}'."
                )

            if ent.cleanup_dependency_order != canonical_ent.cleanup_dependency_order:
                raise ValueError(
                    f"Entity {key} has unexpected cleanup_dependency_order {ent.cleanup_dependency_order}, "
                    f"expected {canonical_ent.cleanup_dependency_order}."
                )

            expected_attrs = canonical_ent.expected_attributes
            actual_attrs = ent.expected_attributes

            extra_attrs = set(actual_attrs) - set(expected_attrs)
            if extra_attrs:
                raise ValueError(f"Entity {key} has unexpected extra attributes: {sorted(extra_attrs)}.")

            missing_attrs = set(expected_attrs) - set(actual_attrs)
            if missing_attrs:
                raise ValueError(f"Entity {key} is missing expected attributes: {sorted(missing_attrs)}.")

            for attr_k, exp_val in expected_attrs.items():
                act_val = actual_attrs[attr_k]
                if act_val != exp_val:
                    raise ValueError(
                        f"Entity {key} attribute '{attr_k}' mismatch: expected {exp_val!r}, got {act_val!r}."
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

        # Enforce zero-secrets policy
        self._validate_manifest_payload_security(manifest)

        # Strict canonical manifest validation (exact 5 entities, cleanup order, ownership markers, attributes)
        self._validate_manifest_canonical_entities(manifest, run_id)

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

            # Synchronously fsync directory on POSIX to commit directory entry.
            # Publication has succeeded: under NO CIRCUMSTANCES should target_path be unlinked!
            # The canonical manifest is preserved fail-closed on disk for recovery/forensic investigation.
            if sys.platform != "win32":
                try:
                    dir_fd = os.open(str(self._root_dir), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
                except OSError as exc:
                    raise ManifestStorePostPublicationError(
                        f"Parent directory fsync failed on '{self._root_dir}' after canonical publication: {exc}. "
                        f"State is uncertain; canonical manifest '{target_path}' is preserved fail-closed on disk "
                        "for recovery investigation and was NOT unlinked."
                    ) from exc

        # Post-publication verification on published canonical file
        # Once publication succeeds, ANY subsequent failure (readback OSError, content mismatch,
        # hash mismatch, or corruption) MUST raise ManifestStorePostPublicationError.
        # Outcome is uncertain; canonical manifest is preserved fail-closed on disk;
        # automatic retries are strictly prohibited.
        try:
            if not target_path.is_file() or _is_reparse_point(target_path):
                raise ManifestStorePostPublicationError(
                    f"Post-publication verification failed: '{target_path}' is not a regular file. "
                    "Publication outcome is uncertain; canonical manifest is preserved fail-closed on disk "
                    "for recovery investigation; automatic retry is strictly prohibited."
                )
            published_bytes = target_path.read_bytes()
            if len(published_bytes) != total_bytes or published_bytes != serialized_bytes:
                raise ManifestStorePostPublicationError(
                    f"Post-publication content verification failed: published manifest at '{target_path}' "
                    f"differs from serialized bytes (read {len(published_bytes)} bytes, expected {total_bytes}). "
                    "Publication outcome is uncertain; canonical manifest is preserved fail-closed on disk "
                    "for recovery investigation; automatic retry is strictly prohibited."
                )
            if hashlib.sha256(published_bytes).hexdigest() != hashlib.sha256(serialized_bytes).hexdigest():
                raise ManifestStorePostPublicationError(
                    f"Post-publication integrity verification failed: SHA-256 hash mismatch on published file '{target_path}'. "
                    "Publication outcome is uncertain; canonical manifest is preserved fail-closed on disk "
                    "for recovery investigation; automatic retry is strictly prohibited."
                )
        except ManifestStorePostPublicationError:
            raise
        except Exception as exc:
            raise ManifestStorePostPublicationError(
                f"Post-publication readback/verification failed on '{target_path}' with {type(exc).__name__}: {exc}. "
                "Publication outcome is uncertain; canonical manifest is preserved fail-closed on disk "
                "for recovery investigation; automatic retry is strictly prohibited."
            ) from exc

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


class HermeticTestFixtureScope:
    """Explicitly scoped fixture authorization manager for hermetic test execution.

    Carries trusted test fixture scope directly from pytest's tmp_path fixture.

    Threat Model Notice:
        Hermetic test mode and fixture scoping are structural test harness guardrails
        designed to prevent test stores from accidentally targeting or mutating production,
        repository, or arbitrary user directories. They do NOT provide a security or
        privilege-separation boundary against malicious Python code running under the same
        OS user in the same process address space.
    """

    def __init__(self, fixture_root: Path | str) -> None:
        self._fixture_root: Path = self._validate_fixture_root(fixture_root)
        self._entered: bool = False

    @property
    def fixture_root(self) -> Path:
        return self._fixture_root

    @classmethod
    def _validate_fixture_root(cls, root: Path | str) -> Path:
        """Validate candidate fixture root path strictly before authorization.

        Enforces:
        - Candidate is non-empty Path or str without traversal (..) or null bytes.
        - Candidate pre-exists as a real directory.
        - Candidate and ancestors contain no symlinks, junctions, or reparse points.
        - Candidate does not target repository, .git, .env, or operational data paths.
        - Candidate does not target user home directory or system roots.
        - Candidate resides strictly within system temporary storage, and is NOT the temp root itself.
        - Candidate belongs to a genuine pytest fixture hierarchy (relative to temp root, starts with
          'pytest-of-*' or 'pytest-*'). Arbitrary temporary directories cannot self-authorize.
        - On POSIX, owner UID matches process UID and permissions are strictly 0700 (no 0o077 bits).
        """
        if root is None or not isinstance(root, (str, Path)):
            raise ManifestStoreSecurityError("Candidate fixture root must be a Path or str.")

        raw_str = str(root).strip()
        if not raw_str:
            raise ManifestStoreSecurityError("Candidate fixture root path cannot be empty.")
        if "\0" in raw_str:
            raise ManifestStoreSecurityError("Candidate fixture root path contains invalid null byte.")
        if ".." in Path(raw_str).parts:
            raise ManifestStoreSecurityError("Candidate fixture root path traversal ('..') is prohibited.")

        candidate = Path(raw_str)

        # 1. Pre-creation check
        if not candidate.exists():
            raise ManifestStoreSecurityError(
                f"Candidate fixture root does not exist: {candidate}. Pre-creation of fixture is required."
            )
        if not candidate.is_dir():
            raise ManifestStoreSecurityError(f"Candidate fixture root is not a directory: {candidate}")

        # 2. Check symlinks/reparse points across ancestors and candidate
        curr_unresolved: Path | None = candidate
        while curr_unresolved is not None and curr_unresolved != curr_unresolved.parent:
            if curr_unresolved.is_symlink() or _is_reparse_point(curr_unresolved):
                raise ManifestStoreSecurityError(f"Symlink or reparse point detected in fixture root: {curr_unresolved}")
            curr_unresolved = curr_unresolved.parent

        resolved = candidate.resolve()
        if _is_reparse_point(resolved):
            raise ManifestStoreSecurityError(f"Symlink or reparse point detected in fixture root: {resolved}")

        # 3. Prohibit repository, .git, .env, and operational data paths
        repo_root = _find_repo_root()
        if repo_root is not None:
            if resolved == repo_root:
                raise ManifestStoreSecurityError(f"Repository root cannot be registered as a fixture root: {resolved}")
            try:
                resolved.relative_to(repo_root)
                raise ManifestStoreSecurityError(f"Store directory cannot reside inside repository: {resolved}")
            except ValueError:
                pass

        # 4. Prohibit user home directory and system directories
        try:
            user_home = Path.home().resolve()
            if resolved == user_home:
                raise ManifestStoreSecurityError(f"User home directory root cannot be registered: {resolved}")
        except (RuntimeError, OSError):
            pass

        for part in resolved.parts:
            p_lower = part.lower()
            if p_lower == ".git" or p_lower.startswith(".git"):
                raise ManifestStoreSecurityError(f"Store directory cannot target .git paths: {resolved}")
            if p_lower == ".env" or p_lower.startswith(".env"):
                raise ManifestStoreSecurityError(f"Store directory cannot target .env paths: {resolved}")

        if sys.platform == "win32":
            sys_root = os.environ.get("SystemRoot")
            if sys_root and resolved == Path(sys_root).resolve():
                raise ManifestStoreSecurityError("SystemRoot directory cannot be registered as a fixture root.")
            prog_files = os.environ.get("ProgramFiles")
            if prog_files and resolved == Path(prog_files).resolve():
                raise ManifestStoreSecurityError("ProgramFiles directory cannot be registered as a fixture root.")
        else:
            system_roots = {Path(p).resolve() for p in ("/etc", "/bin", "/sbin", "/usr", "/var", "/dev", "/proc", "/sys")}
            if resolved in system_roots:
                raise ManifestStoreSecurityError(f"System directory cannot be registered: {resolved}")

        # 5. System temporary storage check
        temp_roots = [Path(tempfile.gettempdir()).resolve()]
        for fallback_tmp in (Path("/tmp"), Path("/var/tmp")):
            if fallback_tmp.exists():
                temp_roots.append(fallback_tmp.resolve())

        is_subpath_of_temp = False
        matching_temp_root: Path | None = None
        for tr in temp_roots:
            if resolved == tr:
                raise ManifestStoreSecurityError(
                    f"System temporary root directory '{tr}' cannot be registered as an authorized fixture root."
                )
            try:
                resolved.relative_to(tr)
                is_subpath_of_temp = True
                matching_temp_root = tr
                break
            except ValueError:
                pass

        if not is_subpath_of_temp or matching_temp_root is None:
            raise ManifestStoreSecurityError(
                f"Candidate fixture root '{resolved}' does not reside within system temporary storage."
            )

        # 6. Pytest fixture hierarchy provenance check:
        # Candidate must reside within an authentic pytest fixture directory hierarchy.
        rel_parts = resolved.relative_to(matching_temp_root).parts
        if not rel_parts:
            raise ManifestStoreSecurityError(
                f"Candidate fixture root '{resolved}' is the temporary root itself; rejected."
            )
        top_component = rel_parts[0].lower()
        if not (top_component.startswith("pytest-of-") or top_component.startswith("pytest-")):
            raise ManifestStoreSecurityError(
                f"Candidate fixture root '{resolved}' does not belong to a valid pytest fixture hierarchy "
                f"(expected 'pytest-of-*' or 'pytest-*' directory structure within temporary storage). "
                f"Arbitrary temporary directories cannot be authorized."
            )

        # 7. POSIX ownership and permissions verification (mode 0700)
        if sys.platform != "win32":
            try:
                st = resolved.stat()
                if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
                    raise ManifestStoreSecurityError(
                        f"Fixture root owner UID ({st.st_uid}) does not match current process UID ({os.geteuid()})."
                    )
                if bool(st.st_mode & 0o077):
                    raise ManifestStoreSecurityError(
                        f"Fixture root has insecure permissions (mode {oct(st.st_mode & 0o777)}): "
                        f"group and world access bits (0o077) are strictly prohibited; mode 0700 required."
                    )
            except OSError as exc:
                if isinstance(exc, ManifestStoreSecurityError):
                    raise
                raise ManifestStoreSecurityError(f"Failed to inspect fixture root permissions: {exc}") from exc

        return resolved

    def __enter__(self) -> HermeticTestFixtureScope:
        HermeticTestManifestStore._AUTHORIZED_FIXTURE_ROOTS.add(self._fixture_root)
        self._entered = True
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        HermeticTestManifestStore._AUTHORIZED_FIXTURE_ROOTS.discard(self._fixture_root)
        self._entered = False

    def close(self) -> None:
        self.__exit__(None, None, None)

    @classmethod
    def enter(cls, root: Path | str) -> HermeticTestFixtureScope:
        scope = cls(root)
        scope.__enter__()
        return scope


class HermeticTestManifestStore(PersistentManifestStore):
    """Dedicated test adapter for hermetic test suites.

    Permits operation strictly inside isolated test fixture directories (e.g. pytest tmp_path)
    within temporary storage, while enforcing repository boundary isolation, .git/.env/data blocking,
    symlink/junction rejection, overwrite protection, schema validation, and hash sealing.
    Arbitrary user, system, or non-temp directories are strictly rejected.

    Threat Model Notice:
        Hermetic test mode is a structural test harness guardrail designed to prevent
        tests from mutating production, repository, or arbitrary user directories. It does NOT
        provide a security or privilege-separation boundary against malicious Python code
        running under the same OS user in the same process address space.
    """

    _AUTHORIZED_FIXTURE_ROOTS: set[Path] = set()

    @classmethod
    def register_authorized_fixture_root(cls, root: Path | str) -> None:
        """Internal trusted-harness helper to register an authorized test fixture root.

        Threat Model Notice:
            Hermetic test mode and fixture scoping are structural test harness guardrails
            designed to prevent test stores from accidentally targeting or mutating production,
            repository, or arbitrary user directories. They do NOT provide a security or
            privilege-separation boundary against malicious Python code running under the same
            OS user in the same process address space.

        Restricted strictly to authentic pytest fixture hierarchies within temporary storage.
        Arbitrary directories cannot self-authorize.
        """
        validated = HermeticTestFixtureScope._validate_fixture_root(root)
        cls._AUTHORIZED_FIXTURE_ROOTS.add(validated)

    @classmethod
    def clear_authorized_fixture_roots(cls) -> None:
        """Clear registered authorized fixture roots."""
        cls._AUTHORIZED_FIXTURE_ROOTS.clear()

    def __init__(self, root_dir: Path | str, **kwargs: Any) -> None:
        super().__init__(root_dir, is_hermetic_test=True, live_authorized=False, **kwargs)
