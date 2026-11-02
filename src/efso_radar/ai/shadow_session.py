"""Stage B: Offline same-host one-use Gemini shadow session claim and replay-denial contract.

Issue #65 / Stage B safety standard & independent review hardening:
1. Test-Only Simulation Harness:
   Explicitly an in-process, test-fixture simulation harness for verifying atomic
   claim arbitration and replay denial within a single caller-provided test directory.
   NOT a security capability or authorization barrier against a caller selecting a different root.
   Single-host one-use is proven ONLY within a single fixed directory instance; a caller
   switching roots is NOT prevented across arbitrary roots.
2. Fixture Scope Context Gating:
   Write-capable claim operations are strictly gated behind an active ShadowSessionFixtureScope.
   Any un-attested directory, repository path, system root, or non-temp location fails before os.open.
   Zero public arbitrary root registration methods.
3. Clean / Fresh Fixture Enforcement:
   Fixture root must be verified completely empty before the initial claim. Any foreign files,
   stray manifests, or unrecognized entries cause immediate fail-closed rejection (HALT / UNKNOWN).
   Never silently starts or auto-clears on a reused root.
4. Canonical Stable Identity:
   Operation identity is strictly derived from immutable inputs:
   - Fixed cohort ID: EFSO-SHADOW-20261030-HISTORICAL-PUBLIC-V1
   - Pinned model: gemini-3.5-flash-lite
   - Fixed purpose version: STAGE-B-SHADOW-V1
   Callers cannot provide arbitrary UUIDs or file names to bypass replay detection.
5. Pre-Dispatch Consumption & State Machine:
   State transitions from verified clean fixture directly to CONSUMED pre-dispatch.
   Any subsequent run, existing record, partial file, corrupted data, foreign identity,
   or untrusted root results in immediate HALT / REPLAY_DENIED.
   Zero TTL reset, zero deletion, zero retry, and zero cleanup on error.
6. Atomic Creation without Overwrite:
   Uses OS-level atomic creation (os.O_CREAT | os.O_EXCL) with fsync.
   Concurrent races between separate processes on the same host result in exactly one
   successful claim; all other racers fail with FileExists / ReplayDenied.
7. Error & Path Sanitization (from None):
   Constant sanitized error messages; zero reflection of raw payload contents or sensitive paths.
   All internal exceptions suppressed via 'raise ... from None' to prevent leakage in tracebacks.
   Reparse point inspection fails closed on any OSError.
8. Explicit Hardware & Platform Limits:
   - Same-host fixture-only at-most-one contract on a fixed attested root;
     NOT global one-use across arbitrary roots, NOT cross-runner, NOT exactly-once.
   - NOT a physical power-loss or host reboot durability guarantee (NTFS directory fsync
     is unsupported in Python standard library).
   - Missing state on an untrusted or previously used root is UNKNOWN, never fresh authorization.
   - Windows directory durability and owner-host ACL remain blocked.
9. Zero Provider / Network Execution:
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
    "STAGE B SAME-HOST TEST-FIXTURE SIMULATION CONTRACT. "
    "Demonstrates at-most-one claim arbitration within a single attested test directory. "
    "NOT a trusted authorization barrier against a caller selecting a different root directory. "
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
        except OSError:
            # Fail closed on any attribute inspection failure; suppress raw OS error
            raise ShadowSessionSecurityError(
                "Filesystem attribute inspection failed during reparse check."
            ) from None
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

    # POSIX permissions & ownership check (0700)
    if sys.platform != "win32":
        try:
            st = resolved.stat()
            if hasattr(os, "geteuid") and st.st_uid != os.geteuid():
                raise ShadowSessionSecurityError("Fixture root owner UID does not match current process UID.")
            if bool(st.st_mode & 0o077):
                raise ShadowSessionSecurityError("Fixture root has insecure permissions (must be mode 0700).")
        except OSError:
            raise ShadowSessionSecurityError("Failed to check fixture root permissions.") from None

    return resolved


class ShadowSessionFixtureScope:
    """Explicit, test-only simulation harness for Stage B shadow session claim tests.

    Threat Model & Limitation Notice:
    - This scope is strictly an in-process, test-fixture simulation harness for testing
      concurrency and replay denial within a single caller-provided test directory.
    - It does NOT provide a cryptographic, OS-level, or authorization barrier against a
      caller selecting a different root directory.
    - Single-host one-use is proven ONLY within a single fixed directory instance; a caller
      switching roots is NOT prevented across arbitrary roots.
    - Strictly NOT cross-runner, NOT distributed, and NOT an exactly-once execution guarantee.
    - NOT a physical power-loss or host-reboot durability guarantee.
    """

    _ACTIVE_SCOPES: set[Path] = set()

    def __init__(self, fixture_root: Path | str) -> None:
        self._fixture_root: Path = validate_test_fixture_root(fixture_root)
        self._entered: bool = False

    @property
    def fixture_root(self) -> Path:
        return self._fixture_root

    def __enter__(self) -> ShadowSessionFixtureScope:
        ShadowSessionFixtureScope._ACTIVE_SCOPES.add(self._fixture_root)
        self._entered = True
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        ShadowSessionFixtureScope._ACTIVE_SCOPES.discard(self._fixture_root)
        self._entered = False

    def close(self) -> None:
        self.__exit__(None, None, None)

    @classmethod
    def is_active(cls, path: Path) -> bool:
        """Check if path is encompassed by an actively entered simulation scope."""
        for active in cls._ACTIVE_SCOPES:
            if path == active:
                return True
            try:
                path.relative_to(active)
                return True
            except ValueError:
                pass
        return False

    @classmethod
    def clear_active_scopes(cls) -> None:
        """Clear all actively registered scopes."""
        cls._ACTIVE_SCOPES.clear()


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
    except (json.JSONDecodeError, ValidationError):
        raise ShadowSessionCorruptedError(
            "Existing claim file is malformed or schema-invalid. Replay denied."
        ) from None
    except OSError:
        raise ShadowSessionCorruptedError(
            "Failed to read existing claim file. Replay denied."
        ) from None

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
    """Atomically claim a single-use shadow session within an attested test fixture scope.

    Contract:
    1. Validates fixture root boundaries (temp storage, 0700).
    2. Enforces active ShadowSessionFixtureScope context before any filesystem write.
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

    # Gated behind active ShadowSessionFixtureScope context
    if not ShadowSessionFixtureScope.is_active(validated_root):
        raise ShadowSessionSecurityError(
            "Fixture root has not been entered via an active ShadowSessionFixtureScope test context. "
            "Un-attested directories cannot execute session claims."
        )

    identity = compute_canonical_session_identity(cohort_id, model_name, purpose_version)
    filename = get_canonical_claim_filename(cohort_id, model_name, purpose_version)
    target_path = validated_root / filename

    # Clean / empty fixture enforcement
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
    except FileExistsError:
        raise ShadowSessionReplayError(
            "Replay denied: atomic session claim lost race; claim file already exists."
        ) from None
    except OSError:
        raise ShadowSessionStorageError("Failed to create session claim file. Replay denied.") from None

    try:
        bytes_written = 0
        while bytes_written < len(payload_bytes):
            chunk = payload_bytes[bytes_written:]
            n = os.write(fd, chunk)
            if n <= 0:
                raise ShadowSessionStorageError("Short write encountered while writing session claim.")
            bytes_written += n
        os.fsync(fd)
    except Exception:
        # FAIL-CLOSED: DO NOT DELETE THE FILE!
        logger.error("I/O error during session claim creation. Partial state retained on disk for replay denial.")
        raise ShadowSessionStorageError(
            "Failed to fsync session claim to disk. State retained on disk to deny replay."
        ) from None
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass

    return record
