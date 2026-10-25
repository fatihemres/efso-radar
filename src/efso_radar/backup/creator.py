"""Consistent SQLite snapshotting and encrypted backup archive packaging.

Creates point-in-time consistent backups combining the SQLite database (checkpointed
via the online backup API) and critical JSON ledgers (Telegram delivery outbox and
experiment budget ledger) into an encrypted AES-256-GCM envelope.
Enforces cross-store snapshot coordination barriers, strict ledger schema validation,
and true schema version extraction from schema_migrations.
"""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import sqlite3
import tarfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from efso_radar.backup.envelope import encrypt_envelope

logger = logging.getLogger("efso-radar.backup.creator")

ESSENTIAL_TABLES = (
    "schema_migrations",
    "stories",
    "notifications",
    "candidate_backlog",
    "processed_articles",
)

RECOGNIZED_DELIVERY_STATUSES = {
    "SENT",
    "DELIVERED",
    "DELIVERY_SUCCESS",
    "DELIVERY_UNCERTAIN",
    "DRY_RUN_SIMULATED",
    "ALREADY_DELIVERED",
    "VERIFIED",
    "SIMULATED",
    "FAILED",
    "PENDING",
    "HTTP_ERROR",
    "TIMEOUT_UNCERTAIN",
    "CONFIG_ERROR",
}


class BackupCreationError(Exception):
    """Raised when backup creation, snapshotting, or validation fails."""
    pass


class BackupCoordinationTimeoutError(BackupCreationError):
    """Raised when acquiring the cross-store snapshot coordination lock times out."""
    pass


class MalformedLedgerError(BackupCreationError):
    """Raised when an operational ledger contains structurally invalid or corrupted entries."""
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


class SnapshotCoordinationBarrier:
    """Coordinated cross-store consistency barrier.

    Prevents concurrent Telegram ledger writes, budget ledger updates, and
    relevant SQLite database mutations during point-in-time backup creation.
    Uses file-level advisory lock coordination with strict timeout behavior,
    deadlock avoidance, and guaranteed cleanup.
    """

    def __init__(self, coordination_dir: Path, lock_name: str = ".snapshot_coordination.lock", timeout_sec: float = 5.0):
        self.coordination_dir = coordination_dir
        self.lock_file = coordination_dir / lock_name
        self.timeout_sec = timeout_sec
        self._acquired = False
        self._lock_fd: int | None = None

    def acquire(self) -> None:
        """Acquire the coordination lock with timeout to prevent deadlocks."""
        self.coordination_dir.mkdir(parents=True, exist_ok=True)
        start_time = time.monotonic()

        while True:
            try:
                # O_CREAT | O_EXCL ensures atomic creation
                self._lock_fd = os.open(
                    str(self.lock_file),
                    os.O_CREAT | os.O_EXCL | os.O_RDWR,
                )
                # Write current process ID and timestamp
                os.write(self._lock_fd, f"pid={os.getpid()} ts={time.time()}\n".encode("utf-8"))
                self._acquired = True
                logger.debug("Acquired snapshot coordination lock at %s", self.lock_file)
                return
            except FileExistsError:
                elapsed = time.monotonic() - start_time
                if elapsed >= self.timeout_sec:
                    raise BackupCoordinationTimeoutError(
                        f"Failed to acquire snapshot coordination lock within {self.timeout_sec}s. "
                        f"A concurrent writer or backup may be active: {self.lock_file}"
                    )
                time.sleep(0.05)

    def release(self) -> None:
        """Release the coordination lock and clean up the lock file."""
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = None

        if self._acquired:
            try:
                if self.lock_file.exists():
                    self.lock_file.unlink()
            except OSError as exc:
                logger.warning("Failed to unlink coordination lock file: %s", exc)
            self._acquired = False
            logger.debug("Released snapshot coordination lock at %s", self.lock_file)

    def __enter__(self) -> SnapshotCoordinationBarrier:
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


def create_consistent_sqlite_snapshot(source_db_path: Path) -> tuple[bytes, int, dict[str, int]]:
    """Create a point-in-time consistent, checkpointed SQLite database snapshot.

    Uses SQLite's online backup API to ensure all committed WAL frames are folded
    into the snapshot, eliminating WAL dependencies and preventing torn pages.
    Sets journal mode to DELETE in the snapshot so it is completely self-contained,
    strips transient worker run locks to prevent deadlock upon restore, and queries
    the authoritative schema version from SELECT MAX(version) FROM schema_migrations.

    Args:
        source_db_path: Path to the live SQLite database file.

    Returns:
        A tuple of (snapshot_bytes, actual_schema_version, table_row_counts).

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

    with contextlib.closing(source_conn):
        with tempfile.TemporaryDirectory(prefix="efso_snap_") as tmp_dir:
            snap_path = Path(tmp_dir) / "snapshot.db"
            try:
                with contextlib.closing(sqlite3.connect(snap_path)) as target_conn:
                    # Atomic online backup across all pages
                    source_conn.backup(target_conn)

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

                    # Read true schema version from schema_migrations
                    cursor.execute("SELECT MAX(version) FROM schema_migrations;")
                    ver_row = cursor.fetchone()
                    if not ver_row or ver_row[0] is None:
                        raise BackupCreationError("schema_migrations table has no recorded migrations")
                    actual_schema_version = int(ver_row[0])

                    # Strip active run locks so restored instances do not inherit deadlocks
                    if "run_locks" in tables:
                        cursor.execute("DELETE FROM run_locks;")
                        target_conn.commit()

                    # Switch journal mode in the snapshot to DELETE so it has zero WAL dependency
                    target_conn.execute("PRAGMA journal_mode = DELETE;")
                    target_conn.commit()

                    # Collect row counts from snapshot
                    row_counts: dict[str, int] = {}
                    for table in tables:
                        if not table.startswith("sqlite_"):
                            cursor.execute(f"SELECT COUNT(*) FROM {table};")
                            row_counts[table] = int(cursor.fetchone()[0])

                raw_db_bytes = snap_path.read_bytes()
                return raw_db_bytes, actual_schema_version, row_counts
            except BackupCreationError:
                raise
            except Exception as exc:
                raise BackupCreationError(f"Unexpected error during SQLite snapshotting: {exc}") from exc


def validate_delivery_ledger_structure(data: dict[str, Any], ledger_name: str) -> None:
    """Validate structure, field types, and integrity of Telegram delivery ledger."""
    if not isinstance(data, dict):
        raise MalformedLedgerError(f"Ledger {ledger_name} must be a JSON object")

    deliveries = data.get("deliveries")
    if not isinstance(deliveries, dict):
        raise MalformedLedgerError(f"Ledger {ledger_name} missing 'deliveries' dict")

    for del_id, record in deliveries.items():
        if not isinstance(record, dict):
            raise MalformedLedgerError(f"Delivery record {del_id} in {ledger_name} must be an object")

        digest_hash = record.get("digest_hash")
        if not digest_hash or not isinstance(digest_hash, str):
            raise MalformedLedgerError(f"Delivery record {del_id} missing valid 'digest_hash'")

        status = record.get("status")
        if status and status not in RECOGNIZED_DELIVERY_STATUSES:
            raise MalformedLedgerError(
                f"Delivery record {del_id} has unrecognized status '{status}' (allowed: {RECOGNIZED_DELIVERY_STATUSES})"
            )

        # Check for recipient identity
        has_recipient = any(
            record.get(k) for k in ("recipient_role", "chat_id", "target_chat_id")
        )
        if not has_recipient:
            raise MalformedLedgerError(
                f"Delivery record {del_id} missing recipient identifier ('recipient_role' or 'chat_id')"
            )


def validate_budget_ledger_structure(data: dict[str, Any], ledger_name: str) -> None:
    """Validate structure and field types of experiment budget ledger."""
    if not isinstance(data, dict):
        raise MalformedLedgerError(f"Ledger {ledger_name} must be a JSON object")

    batches = data.get("batches")
    if batches is not None and not isinstance(batches, dict):
        raise MalformedLedgerError(f"Ledger {ledger_name} has invalid 'batches' field (expected dict)")


def validate_and_load_json_ledger(ledger_path: Path, ledger_name: str) -> dict[str, Any]:
    """Validate that a required operational ledger file exists, is valid JSON, and matches schema."""
    if not ledger_path.exists():
        raise BackupCreationError(f"Essential ledger file missing: {ledger_name} ({ledger_path})")

    try:
        content = ledger_path.read_text(encoding="utf-8")
        data = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedLedgerError(f"Ledger {ledger_name} contains corrupted/invalid JSON: {exc}") from exc

    if "delivery" in ledger_name:
        validate_delivery_ledger_structure(data, ledger_name)
    elif "budget" in ledger_name:
        validate_budget_ledger_structure(data, ledger_name)

    return data


def create_backup(
    key: bytes,
    db_path: Path,
    delivery_ledger_path: Path,
    budget_ledger_path: Path,
    key_id: str = "efso-2026-v1",
    git_commit_sha: str = "unknown",
    now: datetime | None = None,
    coordination_dir: Path | None = None,
) -> BackupResult:
    """Create a complete, cross-store consistent, authenticated encrypted backup archive.

    Coordinates SQLite and JSON outbox ledgers under a snapshot coordination
    barrier to ensure an atomic multi-store recovery point. Packages database and
    ledgers into an in-memory tarball, binds the true schema version and row counts
    into the authenticated AAD manifest, and encrypts the archive under AES-256-GCM.

    Args:
        key: 32-byte AES-256 encryption key.
        db_path: Path to operational SQLite database (radar.db).
        delivery_ledger_path: Path to telegram_digest_delivery_ledger.json.
        budget_ledger_path: Path to experiment_budget_ledger.json.
        key_id: Key version identifier.
        git_commit_sha: Commit hash for lineage tracking.
        now: Optional reference timestamp (defaults to UTC now).
        coordination_dir: Directory for cross-store lock barrier (defaults to db parent).

    LIMITATION & OPERATIONAL BOUNDARY:
    The absence of active 'run_locks' in radar.db is a necessary check, but it does NOT
    prove that all database and JSON writers are paused (e.g. ad-hoc background scripts,
    unmanaged workers, or direct outbox ledger modifications). In Phase 5B.2B, backups
    are strictly offline/quiescent operations. Live unattended backups remain disabled
    until actual cross-process writer coordination or externally verified quiescence is
    implemented.

    Returns:
        BackupResult containing envelope bytes, authenticated manifest, and hashes.

    Raises:
        BackupCreationError: On any validation, snapshotting, coordination, or encryption failure.
    """
    if len(key) != 32:
        raise BackupCreationError(f"AES-256-GCM key must be exactly 32 bytes; received {len(key)}")

    current_time = now or datetime.now(timezone.utc)
    current_time_utc_str = current_time.isoformat()
    barrier_dir = coordination_dir or db_path.parent

    logger.info("Initiating quiescent offline backup creation at %s", current_time_utc_str)

    # Enforce verified quiescence before snapshotting (Approach B: offline/quiescent-only)
    from efso_radar.backup.restore import verify_quiescence, TelegramDeliveryReconciliationGate
    verify_quiescence(db_path.parent)
    blocked, block_reason = TelegramDeliveryReconciliationGate.is_dispatch_blocked(db_path.parent)
    if blocked:
        raise BackupCreationError(f"Cannot create backup while system recovery is pending: {block_reason}")

    # Coordinated consistency barrier prevents concurrent mutations across cooperating callers
    with SnapshotCoordinationBarrier(barrier_dir):
        # 1. Snapshot SQLite database & extract true schema version
        db_bytes, actual_schema_version, row_counts = create_consistent_sqlite_snapshot(db_path)

        # 2. Validate and read JSON ledgers under lock
        delivery_ledger_data = validate_and_load_json_ledger(
            delivery_ledger_path, "telegram_digest_delivery_ledger"
        )
        budget_ledger_data = validate_and_load_json_ledger(
            budget_ledger_path, "experiment_budget_ledger"
        )

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

        # 4. Construct Authenticated Additional Data (AAD) Manifest with true schema version
        manifest = {
            "manifest_version": 1,
            "created_at_utc": current_time_utc_str,
            "source_engine": "sqlite",
            "schema_version": actual_schema_version,
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
        "Successfully created encrypted backup %s (Schema v%d, Plaintext SHA: %s, Envelope SHA: %s, Size: %d bytes)",
        asset_name,
        actual_schema_version,
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
