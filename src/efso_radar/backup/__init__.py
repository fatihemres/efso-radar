"""Encrypted backup, verified restore, and retention management for Efso Radar."""
from __future__ import annotations

from efso_radar.backup.creator import (
    BackupCreationError,
    BackupResult,
    create_backup,
    create_consistent_sqlite_snapshot,
)
from efso_radar.backup.envelope import (
    CryptographicIntegrityError,
    EnvelopeError,
    InvalidEnvelopeError,
    KeyMismatchError,
    decrypt_envelope,
    encrypt_envelope,
)
from efso_radar.backup.restore import (
    CutoverAuthorizationError,
    DecompressionBombSecurityError,
    PathTraversalSecurityError,
    RestoreVerificationError,
    RestoreVerificationResult,
    execute_production_cutover,
    verify_restore_archive,
)
from efso_radar.backup.retention import (
    BackupAsset,
    MockGitHubReleaseStorageAdapter,
    RetentionPlan,
    RetentionPolicyManager,
    RetentionPolicyViolationError,
    StorageAdapter,
)

__all__ = [
    "BackupCreationError",
    "BackupResult",
    "create_backup",
    "create_consistent_sqlite_snapshot",
    "CryptographicIntegrityError",
    "EnvelopeError",
    "InvalidEnvelopeError",
    "KeyMismatchError",
    "decrypt_envelope",
    "encrypt_envelope",
    "CutoverAuthorizationError",
    "DecompressionBombSecurityError",
    "PathTraversalSecurityError",
    "RestoreVerificationError",
    "RestoreVerificationResult",
    "execute_production_cutover",
    "verify_restore_archive",
    "BackupAsset",
    "MockGitHubReleaseStorageAdapter",
    "RetentionPlan",
    "RetentionPolicyManager",
    "RetentionPolicyViolationError",
    "StorageAdapter",
]
