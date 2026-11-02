"""Stage B: Offline same-host one-use Gemini shadow session claim and replay-denial contract.

Issue #65 / Stage B safety standard:
1. Same-Host Fixture-Only Contract:
   Guarantees at-most-one session execution claim within a trusted, pre-validated local
   test fixture root (e.g. pytest tmp_path). Strictly NOT distributed or cross-runner.
2. Canonical Stable Identity:
   Operation identity is strictly derived from immutable inputs:
   - Fixed cohort ID: EFSO-SHADOW-20261030-HISTORICAL-PUBLIC-V1
   - Pinned model: gemini-3.5-flash-lite
   - Fixed purpose version: STAGE-B-SHADOW-V1
   Callers cannot provide arbitrary UUIDs or file roots to bypass replay detection.
3. Pre-Dispatch Consumption & State Machine:
   State transitions from verified empty fixture directly to CONSUMED pre-dispatch.
   Any subsequent run, existing record, partial file, corrupted data, foreign identity,
   or untrusted root results in immediate HALT / REPLAY_DENIED.
   Zero TTL reset, zero deletion, zero retry, and zero cleanup on error.
4. Atomic Creation without Overwrite:
   Uses OS-level atomic creation (os.O_CREAT | os.O_EXCL) with fsync.
   Concurrent races between separate processes on the same host result in exactly one
   successful claim; all other racers fail with FileExists / ReplayDenied.
5. Strict Root Directory Isolation:
   Rejects operational paths (.env, data/, src/, repo root), path traversal (..),
   symlinks, junctions, reparse points, and unverified roots.
6. Explicit Hardware & Platform Limits:
   - Same-host fixture-only at-most-one contract; NOT cross-runner or exactly-once.
   - NOT a physical power-loss or host reboot durability guarantee (NTFS directory fsync
     is unsupported in Python standard library).
   - Missing state on an untrusted or previously used root is UNKNOWN, never fresh authorization.
   - Windows directory durability and owner-host ACL remain blocked.
7. Zero Provider / Network Execution:
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
from typing import Optional

from pydantic import BaseModel, Field, ValidationError

from efso_radar.ai.client import AIEvaluationError
from efso_radar.ai.shadow_preflight import ALLOWED_SHADOW_MODEL, SHADOW_SAMPLE_ID

logger = logging.getLogger("efso-radar.ai.shadow_session")

DEFAULT_PURPOSE_VERSION: str = "STAGE-B-SHADOW-V1"
MAX_CLAIM_FILE_BYTES: int = 65_536  # 64 KB safety limit for claim files

SHADOW_SESSION_DISCLAIMER: str = (
    "STAGE B SAME-HOST ONE-USE SESSION CLAIM CONTRACT. "
    "At-most-one execution claim within a dedicated, owned test fixture root. "
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
    """Raised when an untrusted, operational, symlinked, or invalid fixture root is detected."""


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


def validate_fixture_root(root_path: Path | str) -> Path:
    """Validate that candidate root is an owned, isolated test fixture directory.

    Rejects:
    - Empty or null paths
    - Path traversal ('..')
    - Non-existent directories or non-directory files
    - Symlinks, junctions, reparse points across root and all ancestors
    - Repository root or any location inside the repository working tree
    - System or filesystem root directories
    """
    if root_path is None or str(root_path).strip() == "":
        raise ShadowSessionSecurityError("Fixture root path cannot be empty or None.")

    raw_str = str(root_path)
    if "\x00" in raw_str:
        raise ShadowSessionSecurityError("Null bytes forbidden in fixture root path.")

    # Check for path traversal components before resolution
    parts = Path(raw_str).parts
    if ".." in parts:
        raise ShadowSessionSecurityError("Path traversal ('..') is strictly forbidden in fixture root.")

    candidate = Path(raw_str)
    if not candidate.exists():
        raise ShadowSessionSecurityError(f"Fixture root directory does not exist: '{candidate}'")
    if not candidate.is_dir():
        raise ShadowSessionSecurityError(f"Fixture root must be a directory: '{candidate}'")

    # Check symlinks and reparse points across ancestors and candidate
    curr: Optional[Path] = candidate
    while curr is not None and curr != curr.parent:
        if _is_reparse_point_or_symlink(curr):
            raise ShadowSessionSecurityError(
                f"Symlink or reparse point detected in fixture root hierarchy: '{curr}'"
            )
        curr = curr.parent

    resolved = candidate.resolve()
    if _is_reparse_point_or_symlink(resolved):
        raise ShadowSessionSecurityError(
            f"Symlink or reparse point detected at resolved fixture root: '{resolved}'"
        )

    # Reject filesystem roots (e.g. C:\ or /)
    if resolved == resolved.parent:
        raise ShadowSessionSecurityError(f"Fixture root cannot be a filesystem root: '{resolved}'")

    # Reject repository root or paths inside repository
    repo_root = _find_repo_root()
    if repo_root is not None:
        if resolved == repo_root:
            raise ShadowSessionSecurityError(
                f"Repository root cannot be used as a test fixture root: '{resolved}'"
            )
        try:
            resolved.relative_to(repo_root)
            raise ShadowSessionSecurityError(
                f"Fixture root cannot reside inside the repository working directory: '{resolved}'"
            )
        except ValueError:
            pass

    return resolved


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
    """
    validated_root = validate_fixture_root(fixture_root)
    filename = get_canonical_claim_filename(cohort_id, model_name, purpose_version)
    target_path = validated_root / filename

    if not target_path.exists() and not target_path.is_symlink() and not _is_reparse_point_or_symlink(target_path):
        return None

    # Target path exists or is a symlink/reparse point
    if target_path.is_symlink() or _is_reparse_point_or_symlink(target_path):
        raise ShadowSessionSecurityError(f"Symlink or reparse point detected at claim path: '{target_path}'")

    try:
        size = target_path.stat().st_size
        if size == 0:
            raise ShadowSessionCorruptedError(
                f"Existing claim file is empty (0 bytes) at '{target_path}'. Partial state retained; replay denied."
            )
        if size > MAX_CLAIM_FILE_BYTES:
            raise ShadowSessionCorruptedError(
                f"Existing claim file exceeds maximum size ({size} > {MAX_CLAIM_FILE_BYTES} bytes) at '{target_path}'."
            )
        content = target_path.read_text(encoding="utf-8")
        data = json.loads(content)
        record = ShadowSessionClaimRecord.model_validate(data)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise ShadowSessionCorruptedError(
            f"Existing claim file is malformed or schema-invalid at '{target_path}': {exc}. Replay denied."
        ) from exc
    except OSError as exc:
        raise ShadowSessionCorruptedError(
            f"Failed to read existing claim file at '{target_path}': {exc}. Replay denied."
        ) from exc

    # Verify identity integrity
    expected_identity = compute_canonical_session_identity(cohort_id, model_name, purpose_version)
    if record.session_identity != expected_identity:
        raise ShadowSessionCorruptedError(
            f"Claim record identity mismatch: expected '{expected_identity}', got '{record.session_identity}'. "
            f"Foreign or tampered claim file rejected; replay denied."
        )

    return record


def claim_shadow_session(
    fixture_root: Path | str,
    cohort_id: str = SHADOW_SAMPLE_ID,
    model_name: str = ALLOWED_SHADOW_MODEL,
    purpose_version: str = DEFAULT_PURPOSE_VERSION,
) -> ShadowSessionClaimRecord:
    """Atomically claim a single-use shadow session within the specified fixture root.

    Contract:
    1. Validates fixture root boundaries (must be outside repo, no symlinks/reparse, existing dir).
    2. Derives deterministic canonical filename: callers cannot vary UUIDs to evade replay.
    3. Checks existing state:
       - If valid claim exists -> raises ShadowSessionReplayError.
       - If corrupt/empty/partial file exists -> raises ShadowSessionCorruptedError.
    4. Atomically creates claim file using os.O_CREAT | os.O_EXCL | os.O_WRONLY.
       - If another process raced and won -> FileExistsError -> raises ShadowSessionReplayError.
    5. Writes serialized JSON record, flushes via os.fsync, and closes fd.
    6. On any write/fsync failure:
       - Retains file on disk (DO NOT DELETE) to ensure subsequent runs fail closed.
       - Raises ShadowSessionStorageError.
    7. Returns ShadowSessionClaimRecord in 'CONSUMED' state pre-dispatch.
    """
    validated_root = validate_fixture_root(fixture_root)
    identity = compute_canonical_session_identity(cohort_id, model_name, purpose_version)
    filename = get_canonical_claim_filename(cohort_id, model_name, purpose_version)
    target_path = validated_root / filename

    # Pre-check: if file already exists, inspect and fail closed immediately
    existing_record = inspect_existing_session_claim(
        validated_root,
        cohort_id=cohort_id,
        model_name=model_name,
        purpose_version=purpose_version,
    )
    if existing_record is not None:
        raise ShadowSessionReplayError(
            f"Replay denied: session '{identity}' has already been claimed and consumed "
            f"at {existing_record.claimed_at_utc} by PID {existing_record.process_pid} on {existing_record.hostname}."
        )

    # Build claim record
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

    # Atomic creation: os.O_CREAT | os.O_EXCL guarantees atomic creation without overwrite
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY

    fd = -1
    try:
        fd = os.open(target_path, flags, 0o600)
    except FileExistsError as exc:
        # Concurrent race between two distinct processes
        raise ShadowSessionReplayError(
            f"Replay denied: atomic session claim lost race; claim file already exists at '{target_path}'"
        ) from exc
    except OSError as exc:
        raise ShadowSessionStorageError(f"Failed to create session claim file: {exc}") from exc

    try:
        bytes_written = 0
        while bytes_written < len(payload_bytes):
            chunk = payload_bytes[bytes_written:]
            n = os.write(fd, chunk)
            if n <= 0:
                raise ShadowSessionStorageError(
                    f"Short write encountered while writing session claim ({bytes_written}/{len(payload_bytes)} bytes)"
                )
            bytes_written += n
        os.fsync(fd)
    except Exception as exc:
        # FAIL-CLOSED: DO NOT DELETE THE FILE!
        # Leaving partial or corrupted file on disk prevents any subsequent execution.
        logger.error(
            "I/O or fsync error during session claim creation. Partial state retained on disk for replay denial: %s",
            exc,
        )
        raise ShadowSessionStorageError(
            f"Failed to fsync session claim to disk. State retained on disk to deny replay: {exc}"
        ) from exc
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass

    return record
