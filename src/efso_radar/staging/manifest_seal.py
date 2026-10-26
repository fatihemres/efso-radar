"""Hermetic manifest integrity gate for a future controlled G-C write session.

OFFLINE COMPONENT ONLY. This module never opens a Turso connection, accepts a
database token, changes cloud dashboard settings or authorizes SQL dispatch.

For a future live implementation, validating a manifest is only a *necessary*
pre-dispatch condition: the caller must separately enforce SQL/table allowlists,
human write-unblock approval, request/deadline bounds and safe cleanup.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path

from efso_radar.staging.runner import PerRunOwnershipManifest


MAX_MANIFEST_BYTES = 64 * 1024


class ManifestSealViolation(PermissionError):
    """The pre-registered owned inventory is absent, altered or unauthorized."""


def _load_snapshot(path: Path, expected_run_id: str) -> tuple[PerRunOwnershipManifest, bytes]:
    # Deny any arbitrary file read, including .env and operational data files.
    if (
        not isinstance(expected_run_id, str)
        or not expected_run_id
        or path.name != f"efso_radar_staging_manifest_{expected_run_id}.json"
        or not path.is_file()
        or path.is_symlink()
    ):
        raise ManifestSealViolation("Canonical per-run staging manifest is missing or unsafe.")
    try:
        if path.stat().st_size > MAX_MANIFEST_BYTES:
            raise ManifestSealViolation("Manifest exceeds offline safety size limit.")
        before = path.read_bytes()
        if not before or len(before) > MAX_MANIFEST_BYTES:
            raise ManifestSealViolation("Manifest content is missing or too large.")
        # Existing validator checks the exact 5 registered synthetic entities,
        # key derivation, ownership attributes, no additional fields and file owner.
        manifest = PerRunOwnershipManifest.load_and_validate(path)
        after = path.read_bytes()
        if before != after:
            raise ManifestSealViolation("Manifest changed during inspection.")
        if manifest.run_id != expected_run_id:
            raise ManifestSealViolation("Manifest run identifier mismatch.")
        return manifest, before
    except ManifestSealViolation:
        raise
    except (OSError, ValueError, PermissionError) as exc:
        # Do not return raw manifest content or exception messages in errors.
        raise ManifestSealViolation(
            "Manifest validation failed; no mutations are authorized ("
            + type(exc).__name__
            + ")."
        ) from None


@dataclass(frozen=True)
class OfflineManifestSeal:
    """A tamper-evident snapshot requiring validation before each future write.

    Does not inspect or authorize actual SQL. No network or cloud access.
    """

    path: Path
    run_id: str
    sha256: str

    @classmethod
    def from_persisted(cls, path: Path, expected_run_id: str) -> OfflineManifestSeal:
        if not isinstance(path, Path):
            raise TypeError("Manifest path must be a pathlib.Path.")
        _, data = _load_snapshot(path, expected_run_id)
        return cls(path=path, run_id=expected_run_id, sha256=hashlib.sha256(data).hexdigest())

    def assert_unchanged(self) -> None:
        _, data = _load_snapshot(self.path, self.run_id)
        if hashlib.sha256(data).hexdigest() != self.sha256:
            raise ManifestSealViolation("Manifest differs from sealed pre-dispatch inventory.")

    def require_registered_identity(self, table: str, pk_column: str, pk_value: str) -> None:
        """Necessary (not sufficient) gate for a proposed synthetic write target."""
        manifest, data = _load_snapshot(self.path, self.run_id)
        if hashlib.sha256(data).hexdigest() != self.sha256:
            raise ManifestSealViolation("Manifest differs from sealed pre-dispatch inventory.")
        if not any(
            ent.table == table and ent.pk_column == pk_column and ent.pk_value == pk_value
            for ent in manifest.registered_entities
        ):
            raise ManifestSealViolation("Proposed write target is not registered as owned.")
