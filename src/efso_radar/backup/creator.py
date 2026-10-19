"""Consistent SQLite snapshotting and encrypted backup archive packaging.

Creates point-in-time consistent backups combining the SQLite database (checkpointed
via the online backup API) and critical JSON ledgers (Telegram delivery outbox and
experiment budget ledger) into an encrypted AES-256-GCM envelope.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import sqlite3
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from efso_radar.backup.envelope import encrypt_envelope

logger = logging.getLogger("efso-radar.backup.creator")

ESSENTIAL_TABLES = (
    "schema_migrations",
    "stories",
    "notifications",
    "candidate_backlog",
    "processed_articles",
)


class BackupCreationError(Exception):
    """Raised when backup creation, snapshotting, or validation fails."""
    pass


@dataclass(frozen=True)
class BackupResult:
    """Result of a completed encrypted backup creation."""
    envelope_bytes: bytes
    manifest: dict[str, Any]
    plaintext_sha256: str
    envelope_sha256: str
    asset_name: str
    created_at_utc: str


def create_consistent_sqlite_snapshot(source_db_path: Path) -> bytes:
    """Create a point-in-time consistent, checkpointed SQLite database snapshot.

    Uses SQLite's online backup API to ensure all committed WAL frames are folded
    into the snapshot, eliminating WAL dependencies and preventing torn pages.
    Sets journal mode to DELETE in the snapshot so it is completely self-contained,
    and strips transient worker run locks to prevent deadlock upon restore.

    Args:
        source_db_path: Path to the live SQLite database file.

    Returns:
        Bytes of the complete standalone, checkpointed SQLite database.

    Raises:
        BackupCreationError: If the source database is missing, corrupted, or cannot be backed up.
    """
    import tempfile

    if not source_db_path.exists():
        raise BackupCreationError(f"Operational SQLite database does not exist: {source_db_path}")

    try:
        source_conn = sqlite3.connect(f"file:{source_db_path}?mode=ro", uri=True, timeout=15.0)
    except Exception as exc:
        raise BackupCreationError(f"Failed to open source database for backup: {exc}") from exc

    with tempfile.TemporaryDirectory(prefix="efso_snap_") as tmp_dir:
        snap_path = Path(tmp_dir) / "snapshot.db"
        try:
            target_conn = sqlite3.connect(snap_path)
            # Atomic online backup across all pages
            source_conn.backup(target_conn)
            source_conn.close()

            # Verify integrity of the snapshot
            cursor = target_conn.cursor()
            cursor.execute("PRAGMA integrity_check;")
            check_row = cursor.fetchone()
            if not check_row or check_row[0] != "ok":
                raise BackupCreationError(f"Snapshot failed SQLite integrity check: {check_row}")

            # Check essential tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = {row[0] for row in cursor.fetchall()}
            missing_tables = [t for t in ESSENTIAL_TABLES if t not in tables]
            if missing_tables:
                raise BackupCreationError(f"Database snapshot is missing essential tables: {missing_tables}")

            # Strip active run locks so restored instances do not inherit deadlocks
            if "run_locks" in tables:
                cursor.execute("DELETE FROM run_locks;")
                target_conn.commit()

            # Switch journal mode in the snapshot to DELETE so it has zero WAL dependency
            target_conn.execute("PRAGMA journal_mode = DELETE;")
            target_conn.commit()
            target_conn.close()

            raw_db_bytes = snap_path.read_bytes()
            return raw_db_bytes
        except BackupCreationError:
            raise
        except Exception as exc:
            raise BackupCreationError(f"Unexpected error during SQLite snapshotting: {exc}") from exc


def collect_table_row_counts(db_bytes: bytes) -> dict[str, int]:
    """Query and return row counts for all user tables in an SQLite database image."""
    conn = sqlite3.connect(":memory:")
    try:
        if hasattr(conn, "deserialize"):
            conn.deserialize(db_bytes)
        else:
            raise BackupCreationError("Python sqlite3 lacks deserialize capability")

        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row[0] for row in cursor.fetchall()]

        counts: dict[str, int] = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            counts[table] = int(cursor.fetchone()[0])
        return counts
    finally:
        conn.close()


def validate_json_ledger(ledger_path: Path, ledger_name: str) -> dict[str, Any]:
    """Validate that a required operational ledger file exists and contains valid JSON."""
    if not ledger_path.exists():
        raise BackupCreationError(f"Essential ledger file missing: {ledger_name} ({ledger_path})")

    try:
        content = ledger_path.read_text(encoding="utf-8")
        data = json.loads(content)
        if not isinstance(data, dict):
            raise BackupCreationError(f"Ledger {ledger_name} must contain a top-level JSON object")
        return data
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackupCreationError(f"Ledger {ledger_name} contains corrupted/invalid JSON: {exc}") from exc


def create_backup(
    key: bytes,
    db_path: Path,
    delivery_ledger_path: Path,
    budget_ledger_path: Path,
    key_id: str = "efso-2026-v1",
    git_commit_sha: str = "unknown",
    now: datetime | None = None,
) -> BackupResult:
    """Create a complete, consistent, authenticated encrypted backup archive.

    Coordinates SQLite and JSON outbox ledgers into a point-in-time consistent
    snapshot, packages them into a compressed tarball, and encrypts the archive
    under AES-256-GCM.

    Args:
        key: 32-byte AES-256 encryption key.
        db_path: Path to operational SQLite database (radar.db).
        delivery_ledger_path: Path to telegram_digest_delivery_ledger.json.
        budget_ledger_path: Path to experiment_budget_ledger.json.
        key_id: Key version identifier.
        git_commit_sha: Commit hash for lineage tracking.
        now: Optional reference timestamp (defaults to UTC now).

    Returns:
        BackupResult containing the envelope bytes, authenticated manifest, and hashes.

    Raises:
        BackupCreationError: On any validation, snapshotting, or encryption failure.
    """
    if len(key) != 32:
        raise BackupCreationError(f"AES-256-GCM key must be exactly 32 bytes; received {len(key)}")

    current_time = now or datetime.now(timezone.utc)
    current_time_utc_str = current_time.isoformat()

    logger.info("Starting consistent backup creation at %s", current_time_utc_str)

    # 1. Snapshot SQLite database
    db_bytes = create_consistent_sqlite_snapshot(db_path)
    row_counts = collect_table_row_counts(db_bytes)

    # 2. Validate JSON ledgers
    delivery_ledger_data = validate_json_ledger(delivery_ledger_path, "telegram_digest_delivery_ledger")
    budget_ledger_data = validate_json_ledger(budget_ledger_path, "experiment_budget_ledger")

    delivery_count = len(delivery_ledger_data.get("deliveries", {}))
    budget_batches_count = len(budget_ledger_data.get("batches", {}))

    delivery_ledger_bytes = json.dumps(delivery_ledger_data, indent=2, ensure_ascii=False).encode("utf-8")
    budget_ledger_bytes = json.dumps(budget_ledger_data, indent=2, ensure_ascii=False).encode("utf-8")

    # 3. Assemble tarball in memory with safe, normalized member names
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w:gz") as tar:
        # Member 1: radar.db
        ti_db = tarfile.TarInfo(name="radar.db")
        ti_db.size = len(db_bytes)
        ti_db.mtime = int(current_time.timestamp())
        tar.addfile(ti_db, io.BytesIO(db_bytes))

        # Member 2: telegram_digest_delivery_ledger.json
        ti_del = tarfile.TarInfo(name="telegram_digest_delivery_ledger.json")
        ti_del.size = len(delivery_ledger_bytes)
        ti_del.mtime = int(current_time.timestamp())
        tar.addfile(ti_del, io.BytesIO(delivery_ledger_bytes))

        # Member 3: experiment_budget_ledger.json
        ti_bud = tarfile.TarInfo(name="experiment_budget_ledger.json")
        ti_bud.size = len(budget_ledger_bytes)
        ti_bud.mtime = int(current_time.timestamp())
        tar.addfile(ti_bud, io.BytesIO(budget_ledger_bytes))

    tar_bytes = tar_buffer.getvalue()
    plaintext_sha256 = hashlib.sha256(tar_bytes).hexdigest()

    # 4. Construct Authenticated Additional Data (AAD) Manifest
    manifest = {
        "manifest_version": 1,
        "created_at_utc": current_time_utc_str,
        "source_engine": "sqlite",
        "schema_version": row_counts.get("schema_migrations", 5),
        "plaintext_sha256": plaintext_sha256,
        "plaintext_byte_size": len(tar_bytes),
        "table_row_counts": row_counts,
        "delivery_ledger_count": delivery_count,
        "budget_ledger_batch_count": budget_batches_count,
        "git_commit_sha": git_commit_sha,
    }

    # 5. Encrypt envelope
    try:
        envelope_bytes = encrypt_envelope(
            plaintext=tar_bytes,
            key=key,
            manifest=manifest,
            key_id=key_id,
            timestamp=int(current_time.timestamp()),
        )
    except Exception as exc:
        raise BackupCreationError(f"Envelope encryption failed: {exc}") from exc

    envelope_sha256 = hashlib.sha256(envelope_bytes).hexdigest()
    ts_label = current_time.strftime("%Y%m%d_%H%M%S")
    asset_name = f"efso_backup_{ts_label}_{plaintext_sha256[:8]}.efbk"

    logger.info(
        "Successfully created encrypted backup %s (Plaintext SHA: %s, Envelope SHA: %s, Size: %d bytes)",
        asset_name,
        plaintext_sha256[:12],
        envelope_sha256[:12],
        len(envelope_bytes),
    )

    return BackupResult(
        envelope_bytes=envelope_bytes,
        manifest=manifest,
        plaintext_sha256=plaintext_sha256,
        envelope_sha256=envelope_sha256,
        asset_name=asset_name,
        created_at_utc=current_time_utc_str,
    )
