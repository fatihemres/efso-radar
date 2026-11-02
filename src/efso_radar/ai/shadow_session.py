"""Stage B: Offline same-host one-use Gemini shadow session claim and replay-denial contract.

Issue #65 / Stage B safety standard & independent review hardening:
1. Test-Owned Fixture Scope Attestation:
   Write-capable claim operations are strictly gated behind an explicit ShadowSessionFixtureScope.
   Arbitrary user, project, desktop, or operational directories cannot self-authorize.
   Root must reside in system temporary storage under an authentic pytest fixture hierarchy (pytest-of-* or pytest-*).
2. Clean / Fresh Fixture Enforcement:
   Fixture root must be verified completely empty before the initial claim. Any foreign files,
   stray manifests, or unrecognized entries cause immediate fail-closed rejection (HALT / UNKNOWN).
   Never silently starts or auto-clears on a reused root.
3. Canonical Stable Identity:
   Operation identity is strictly derived from immutable inputs:
   - Fixed cohort ID: EFSO-SHADOW-20261030-HISTORICAL-PUBLIC-V1
   - Pinned model: gemini-3.5-flash-lite
   - Fixed purpose version: STAGE-B-SHADOW-V1
   Callers cannot provide arbitrary UUIDs or file names to bypass replay detection.
4. Pre-Dispatch Consumption & State Machine:
   State transitions from verified clean fixture directly to CONSUMED pre-dispatch.
   Any subsequent run, existing record, partial file, corrupted data, foreign identity,
   or untrusted root results in immediate HALT / REPLAY_DENIED.
   Zero TTL reset, zero deletion, zero retry, and zero cleanup on error.
5. Atomic Creation without Overwrite:
   Uses OS-level atomic creation (os.O_CREAT | os.O_EXCL) with fsync.
   Concurrent races between separate processes on the same host result in exactly one
   successful claim; all other racers fail with FileExists / ReplayDenied.
6. Error & Path Sanitization:
   Constant sanitized error messages; zero reflection of raw payload contents or sensitive paths.
   Reparse point inspection fails closed on any OSError.
7. Explicit Hardware & Platform Limits:
   - Same-host fixture-only at-most-one contract on a fixed attested root;
     NOT global one-use across arbitrary roots, NOT cross-runner, NOT exactly-once.
   - NOT a physical power-loss or host reboot durability guarantee (NTFS directory fsync
     is unsupported in Python standard library).
   - Missing state on an untrusted or previously used root is UNKNOWN, never fresh authorization.
   - Windows directory durability and owner-host ACL remain blocked.
8. Zero Provider / Network Execution:
   Contains ZERO google.genai.Client, HTTP, or network dispatch code.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import socket
import sys
import tempfile
from typing import Any, Optional

from pydantic import BaseModel, Field, ValidationError

from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.shadow_preflight import ALLOWED_SHADOW_MODEL, SHADOW_SAMPLE_ID

logger = logging.getLogger("efso-radar.ai.shadow_session")

DEFAULT_PURPOSE_VERSION: str = "STAGE-B-SHADOW-V1"
MAX_CLAIM_FILE_BYTES: int = 65_536  # 64 KB safety limit for claim files

SHADOW_SESSION_DISCLAIMER: str = (
    "STAGE B SAME-HOST ONE-USE SESSION CLAIM CONTRACT. "
    "At-most-one execution claim within a dedicated, attested test fixture root. "
    "NOT cross-runner, NOT distributed, and NOT an exactly-once execution guarantee. "
    "NOT a physical power-loss or host-reboot durability guarantee "
    "(NTFS directory metadata fsync unsupported in Python stdlib). "
    "Missing state on an untrusted or previously used root is UNKNOWN, never fresh authorization. "
    "Windows directory durability and owner-host ACL remain blocked. "
    "Contains zero live network or Gemini SDK capability."
)


class ShadowSessionError(AIEvaluationError):
    """Base exception for Stage B shadow session claim and replay prevention."""


class ShadowSessionReplayError(ShadowSessionError):
    """Raised when session claim already exists or duplicate execution is attempted."""


class ShadowSessionSecurityError(ShadowSessionError, PermissionError):
    """Raised when an untrusted, operational, un-attested, symlinked, or invalid fixture root is detected."""


class ShadowSessionCorruptedError(ShadowSessionError, ValueError):
    """Raised when a claim file exists but is corrupted, empty, truncated, or schema-invalid."""


class ShadowSessionStorageError(ShadowSessionError, OSError):
    """Raised when an I/O, atomic open, write, or fsync operation fails."""


class ShadowSessionClaimRecord(BaseModel):
    """Immutable persistent evidence of a claimed Stage B shadow evaluation session."""

    session_identity: str = Field(description="Canonical deterministic session identifier")
    cohort_id: str = Field(description="Deterministic historical news cohort identifier")
    model_name: str = Field(description="Pinned model identifier (gemini-3.5-flash-lite)")
    purpose_version: str = Field(description="Operational purpose version string")
    claimed_at_utc: str = Field(description="UTC timestamp of the claim")
    process_pid: int = Field(description="Process ID that atomically acquired the claim")
    hostname: str = Field(description="Host or runner name where the claim occurred")
    state: str = Field(default="CONSUMED", description="Execution state: strictly CONSUMED pre-dispatch")
    disclaimer: str = Field(default=SHADOW_SESSION_DISCLAIMER, description="Contract limitations and boundaries")


def compute_canonical_session_identity(
    cohort_id: str = SHADOW_SAMPLE_ID,
    model_name: str = ALLOWED_SHADOW_MODEL,
    purpose_version: str = DEFAULT_PURPOSE_VERSION,
) -> str:
    """Compute the immutable deterministic session identity.

    Callers cannot supply random UUIDs or alter cohort/model to bypass replay detection.
    """
    if cohort_id != SHADOW_SAMPLE_ID:
        raise ShadowSessionSecurityError(
            f"Invalid cohort ID '{cohort_id}': Stage B session claim is strictly bound to '{SHADOW_SAMPLE_ID}'"
        )
    if model_name != ALLOWED_SHADOW_MODEL:
        raise ShadowSessionSecurityError(
            f"Invalid model '{model_name}': Stage B session claim is strictly bound to '{ALLOWED_SHADOW_MODEL}'"
        )
    if purpose_version != DEFAULT_PURPOSE_VERSION:
        raise ShadowSessionSecurityError(
            f"Invalid purpose version '{purpose_version}': strictly bound to '{DEFAULT_PURPOSE_VERSION}'"
        )

    raw_key = f"{cohort_id}:{model_name}:{purpose_version}".encode("utf-8")
    digest = hashlib.sha256(raw_key).hexdigest()[:16]
    clean_model = model_name.replace(".", "_").replace("-", "_")
    clean_cohort = cohort_id.lower().replace("-", "_")
    return f"efso_shadow_{clean_cohort}_{clean_model}_{digest}"


def get_canonical_claim_filename(
    cohort_id: str = SHADOW_SAMPLE_ID,
    model_name: str = ALLOWED_SHADOW_MODEL,
    purpose_version: str = DEFAULT_PURPOSE_VERSION,
) -> str:
    """Return the deterministic claim filename within the fixture root."""
    identity = compute_canonical_session_identity(cohort_id, model_name, purpose_version)
    return f"claim_{identity}.json"


def _find_repo_root() -> Optional[Path]:
    """Locate git repository root from current file ancestors."""
    candidate = Path(__file__).resolve().parent
    while candidate != candidate.parent:
        if (candidate / ".git").exists():
            return candidate
        candidate = candidate.parent
    return None


def _is_reparse_point_or_symlink(path: Path) -> bool:
    """Detect symlinks, directory junctions, and reparse points across Windows and POSIX.

    Fails closed on any filesystem inspection error (except FileNotFoundError when path does not exist).
    """
    if path.is_symlink():
        return True
    if sys.platform == "win32":
        try:
            st = os.lstat(path)
            attrs = getattr(st, "st_file_attributes", 0)
            # 0x0400 = FILE_ATTRIBUTE_REPARSE_POINT
            if bool(attrs & 0x0400):
                return True
        except FileNotFoundError:
            return False
        except OSError as exc:
            # Fail closed on any attribute inspection failure
            raise ShadowSessionSecurityError(
                "Filesystem attribute inspection failed during reparse check."
            ) from exc
    return False


def validate_test_fixture_root(root_path: Path | str) -> Path:
    """Validate that candidate root is an owned, isolated test fixture directory.

    Enforces strict test-fixture-only constraints:
    - Must be a non-empty Path or str without traversal (..) or null bytes.
    - Must pre-exist as a real directory.
    - Candidate and all ancestors must contain no symlinks, junctions, or reparse points.
    - Prohibits repository root, .env, data/, src/, and .git paths.
    - Prohibits user home directory root and system roots.
    - Must reside strictly within system temporary storage, and is NOT the temp root itself.
    - Must belong to an authentic pytest fixture hierarchy (top component starts with 'pytest-of-' or 'pytest-').
    - On POSIX, owner UID must match process UID and permissions must be 0700.
    """
    if root_path is None or not isinstance(root_path, (str, Path)):
        raise ShadowSessionSecurityError("Fixture root path must be a non-empty Path or str.")

    raw_str = str(root_path).strip()
    if not raw_str:
        raise ShadowSessionSecurityError("Fixture root path cannot be empty.")
    if "\x00" in raw_str:
        raise ShadowSessionSecurityError("Null bytes forbidden in fixture root path.")

    # Path traversal check before resolution
    parts = Path(raw_str).parts
    if ".." in parts:
        raise ShadowSessionSecurityError("Path traversal ('..') is strictly forbidden in fixture root.")

    candidate = Path(raw_str)
    if not candidate.exists():
        raise ShadowSessionSecurityError("Fixture root directory does not exist.")
    if not candidate.is_dir():
        raise ShadowSessionSecurityError("Fixture root must be a directory.")

    # Symlink / reparse check across candidate and all ancestors
    curr: Optional[Path] = candidate
    while curr is not None and curr != curr.parent:
        if _is_reparse_point_or_symlink(curr):
            raise ShadowSessionSecurityError("Symlink or reparse point detected in fixture root hierarchy.")
        curr = curr.parent

    resolved = candidate.resolve()
    if _is_reparse_point_or_symlink(resolved):
        raise ShadowSessionSecurityError("Symlink or reparse point detected at resolved fixture root.")

    # Prohibit filesystem roots (e.g. C:\ or /)
    if resolved == resolved.parent:
        raise ShadowSessionSecurityError("Fixture root cannot be a filesystem root.")

    # Prohibit repository root and repo working tree locations
    repo_root = _find_repo_root()
    if repo_root is not None:
        if resolved == repo_root:
            raise ShadowSessionSecurityError("Repository root cannot be used as a test fixture root.")
        try:
            resolved.relative_to(repo_root)
            raise ShadowSessionSecurityError(
                "Fixture root cannot reside inside the repository working directory."
            )
        except ValueError:
            pass

    # Prohibit user home directory
    try:
        user_home = Path.home().resolve()
        if resolved == user_home:
            raise ShadowSessionSecurityError("User home directory root cannot be used as a fixture root.")
    except (RuntimeError, OSError):
        pass

    # Prohibit system root directories
    if sys.platform == "win32":
        sys_root = os.environ.get("SystemRoot")
        if sys_root and resolved == Path(sys_root).resolve():
            raise ShadowSessionSecurityError("SystemRoot cannot be used as a fixture root.")
        prog_files = os.environ.get("ProgramFiles")
        if prog_files and resolved == Path(prog_files).resolve():
            raise ShadowSessionSecurityError("ProgramFiles cannot be used as a fixture root.")
    else:
        system_roots = {Path(p).resolve() for p in ("/etc", "/bin", "/sbin", "/usr", "/var", "/dev", "/proc", "/sys")}
        if resolved in system_roots:
            raise ShadowSessionSecurityError("System directory cannot be used as a fixture root.")

    # System temporary storage check
    temp_roots = [Path(tempfile.gettempdir()).resolve()]
    for fallback_tmp in (Path("/tmp"), Path("/var/tmp")):
        if fallback_tmp.exists():
            temp_roots.append(fallback_tmp.resolve())

    is_subpath_of_temp = False
    matching_temp_root: Optional[Path] = None
    for tr in temp_roots:
        if resolved == tr:
            raise ShadowSessionSecurityError("System temporary root directory itself cannot be a fixture root.")
        try:
            resolved.relative_to(tr)
            is_subpath_of_temp = True
            matching_temp_root = tr
            break
        except ValueError:
            pass

    if not is_subpath_of_temp or matching_temp_root is None:
        raise ShadowSessionSecurityError("Fixture root does not reside within system temporary storage.")

    # Pytest fixture hierarchy provenance check
    rel_parts = resolved.relative_to(matching_temp_root).parts
    if not rel_parts:
        raise ShadowSessionSecurityError("Fixture root is the temporary root itself.")
    top_component = rel_parts[0].lower()
    if not (top_component.startswith("pytest-of-") or top_component.startswith("pytest-")):
        raise ShadowSessionSecurityError(
            "Fixture root does not belong to a valid pytest fixture hierarchy "
            "(expected 'pytest-of-*' or 'pytest-*' directory structure within temporary storage). "
            "Arbitrary temporary directories cannot self-authorize."
        )

    # POSIX permissions & ownership check (0700)
    if sys.platform != "win32":
        try:
            st = resolved.stat()
            if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
                raise ShadowSessionSecurityError("Fixture root owner UID does not match current process UID.")
            if bool(st.st_mode & 0o077):
                raise ShadowSessionSecurityError("Fixture root has insecure permissions (must be mode 0700).")
        except OSError as exc:
            raise ShadowSessionSecurityError("Failed to check fixture root permissions.") from exc

    return resolved


class ShadowSessionFixtureScope:
    """Explicit test-owned fixture authorization scope for Stage B shadow session claim tests.

    Carries explicit attestation that the fixture root was created by pytest's tmp_path
    or an authentic temporary test fixture within system temporary storage.
    Arbitrary user, project, or operational directories cannot self-authorize.
    """

    _AUTHORIZED_FIXTURE_ROOTS: set[Path] = set()

    def __init__(self, fixture_root: Path | str) -> None:
        self._fixture_root: Path = validate_test_fixture_root(fixture_root)
        self._entered: bool = False

    @property
    def fixture_root(self) -> Path:
        return self._fixture_root

    def __enter__(self) -> ShadowSessionFixtureScope:
        ShadowSessionFixtureScope._AUTHORIZED_FIXTURE_ROOTS.add(self._fixture_root)
        self._entered = True
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        ShadowSessionFixtureScope._AUTHORIZED_FIXTURE_ROOTS.discard(self._fixture_root)
        self._entered = False

    def close(self) -> None:
        self.__exit__(None, None, None)

    @classmethod
    def register_authorized_fixture_root(cls, root: Path | str) -> Path:
        """Register an authorized test fixture root (e.g. for test subprocess execution)."""
        validated = validate_test_fixture_root(root)
        cls._AUTHORIZED_FIXTURE_ROOTS.add(validated)
        return validated

    @classmethod
    def clear_authorized_fixture_roots(cls) -> None:
        """Clear all registered authorized fixture roots."""
        cls._AUTHORIZED_FIXTURE_ROOTS.clear()


def inspect_existing_session_claim(
    fixture_root: Path | str,
    cohort_id: str = SHADOW_SAMPLE_ID,
    model_name: str = ALLOWED_SHADOW_MODEL,
    purpose_version: str = DEFAULT_PURPOSE_VERSION,
) -> Optional[ShadowSessionClaimRecord]:
    """Inspect the fixture root for an existing claim file.

    If claim file exists:
    - If valid, returns ShadowSessionClaimRecord.
    - If corrupt, partial, foreign, or empty, raises ShadowSessionCorruptedError.
    If claim file does not exist, returns None.
    All error messages are sanitized and contain no raw payload contents or sensitive paths.
    """
    validated_root = validate_test_fixture_root(fixture_root)
    filename = get_canonical_claim_filename(cohort_id, model_name, purpose_version)
    target_path = validated_root / filename

    if not target_path.exists() and not target_path.is_symlink() and not _is_reparse_point_or_symlink(target_path):
        return None

    if target_path.is_symlink() or _is_reparse_point_or_symlink(target_path):
        raise ShadowSessionSecurityError("Symlink or reparse point detected at claim path.")

    try:
        size = target_path.stat().st_size
        if size == 0:
            raise ShadowSessionCorruptedError(
                "Existing claim file is empty (0 bytes). Partial state retained; replay denied."
            )
        if size > MAX_CLAIM_FILE_BYTES:
            raise ShadowSessionCorruptedError(
                "Existing claim file exceeds maximum size limit. Replay denied."
            )
        content = target_path.read_text(encoding="utf-8")
        data = json.loads(content)
        record = ShadowSessionClaimRecord.model_validate(data)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise ShadowSessionCorruptedError(
            "Existing claim file is malformed or schema-invalid. Replay denied."
        ) from exc
    except OSError as exc:
        raise ShadowSessionCorruptedError(
            "Failed to read existing claim file. Replay denied."
        ) from exc

    expected_identity = compute_canonical_session_identity(cohort_id, model_name, purpose_version)
    if record.session_identity != expected_identity:
        raise ShadowSessionCorruptedError(
            "Claim record identity mismatch: foreign or tampered claim file rejected; replay denied."
        )

    return record


def claim_shadow_session(
    fixture_root: Path | str,
    cohort_id: str = SHADOW_SAMPLE_ID,
    model_name: str = ALLOWED_SHADOW_MODEL,
    purpose_version: str = DEFAULT_PURPOSE_VERSION,
) -> ShadowSessionClaimRecord:
    """Atomically claim a single-use shadow session within an attested test fixture root.

    Contract:
    1. Validates fixture root boundaries (temp storage, pytest provenance, 0700).
    2. Enforces explicit ShadowSessionFixtureScope attestation before any filesystem write.
    3. Requires clean / empty fixture directory before initial claim.
       Any foreign files or stray entries cause immediate fail-closed rejection.
    4. Derives deterministic canonical filename: callers cannot vary UUIDs to evade replay.
    5. Checks existing state:
       - If valid claim exists -> raises ShadowSessionReplayError.
       - If corrupt/empty/partial file exists -> raises ShadowSessionCorruptedError.
    6. Atomically creates claim file using os.O_CREAT | os.O_EXCL | os.O_WRONLY.
       - If another process raced and won -> FileExistsError -> raises ShadowSessionReplayError.
    7. Writes serialized JSON record, flushes via os.fsync, and closes fd.
    8. On any write/fsync failure:
       - Retains file on disk (DO NOT DELETE) to ensure subsequent runs fail closed.
       - Raises ShadowSessionStorageError.
    9. Returns ShadowSessionClaimRecord in 'CONSUMED' state pre-dispatch.
    """
    validated_root = validate_test_fixture_root(fixture_root)

    # P1 Gate: Fixture root must be explicitly attested via ShadowSessionFixtureScope
    is_authorized = False
    for auth_root in ShadowSessionFixtureScope._AUTHORIZED_FIXTURE_ROOTS:
        if validated_root == auth_root:
            is_authorized = True
            break
        try:
            validated_root.relative_to(auth_root)
            is_authorized = True
            break
        except ValueError:
            pass

    if not is_authorized:
        raise ShadowSessionSecurityError(
            "Fixture root has not been attested as an authorized test-owned fixture scope. "
            "Arbitrary directories cannot execute session claims."
        )

    identity = compute_canonical_session_identity(cohort_id, model_name, purpose_version)
    filename = get_canonical_claim_filename(cohort_id, model_name, purpose_version)
    target_path = validated_root / filename

    # P2 Gate: Clean / empty fixture enforcement
    entries = list(validated_root.iterdir())
    if target_path.exists():
        if len(entries) > 1:
            raise ShadowSessionCorruptedError(
                "Fixture root contains extraneous foreign entries alongside claim file. Replay denied."
            )
        existing_record = inspect_existing_session_claim(
            validated_root,
            cohort_id=cohort_id,
            model_name=model_name,
            purpose_version=purpose_version,
        )
        if existing_record is not None:
            raise ShadowSessionReplayError(
                f"Replay denied: session '{identity}' has already been claimed and consumed."
            )
    else:
        # Fixture root MUST be completely empty before first claim
        if len(entries) > 0:
            raise ShadowSessionSecurityError(
                "Fixture root is not clean/empty: contains foreign or unverified entries. "
                "Untrusted or previously used root rejected with HALT/UNKNOWN; replay denied."
            )

    record = ShadowSessionClaimRecord(
        session_identity=identity,
        cohort_id=cohort_id,
        model_name=model_name,
        purpose_version=purpose_version,
        claimed_at_utc=datetime.now(timezone.utc).isoformat(),
        process_pid=os.getpid(),
        hostname=socket.gethostname(),
        state="CONSUMED",
        disclaimer=SHADOW_SESSION_DISCLAIMER,
    )
    payload_bytes = record.model_dump_json(indent=2).encode("utf-8")

    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY

    fd = -1
    try:
        fd = os.open(target_path, flags, 0o600)
    except FileExistsError as exc:
        raise ShadowSessionReplayError(
            "Replay denied: atomic session claim lost race; claim file already exists."
        ) from exc
    except OSError as exc:
        raise ShadowSessionStorageError("Failed to create session claim file. Replay denied.") from exc

    try:
        bytes_written = 0
        while bytes_written < len(payload_bytes):
            chunk = payload_bytes[bytes_written:]
            n = os.write(fd, chunk)
            if n <= 0:
                raise ShadowSessionStorageError("Short write encountered while writing session claim.")
            bytes_written += n
        os.fsync(fd)
    except Exception as exc:
        # FAIL-CLOSED: DO NOT DELETE THE FILE!
        logger.error(
            "I/O or fsync error during session claim creation. Partial state retained on disk for replay denial: %s",
            type(exc).__name__,
        )
        raise ShadowSessionStorageError(
            "Failed to fsync session claim to disk. State retained on disk to deny replay."
        ) from exc
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass

    return record
