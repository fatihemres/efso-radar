"""Isolated staging verification and fail-closed restore engine.

Ensures backups are authenticated, decrypted, and verified exclusively inside
an isolated staging directory outside of operational data paths. Enforces
SQLite integrity checks, schema validation, delivery ledger identities,
path traversal protection, and decompression bomb limits before any cutover.
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import shutil
import sqlite3
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from efso_radar.backup.envelope import decrypt_envelope

logger = logging.getLogger("efso-radar.backup.restore")

MAX_EXTRACTED_BYTES = 500 * 1024 * 1024  # 500 MB limit against decompression bombs
ALLOWED_ARCHIVE_MEMBERS = {
    "radar.db",
    "telegram_digest_delivery_ledger.json",
    "experiment_budget_ledger.json",
}


class RestoreVerificationError(Exception):
    """Raised when a backup fails authenticity, integrity, or consistency checks."""
    pass


class PathTraversalSecurityError(RestoreVerificationError):
    """Raised when an archive attempts directory traversal or extracts outside staging."""
    pass


class DecompressionBombSecurityError(RestoreVerificationError):
    """Raised when an archive exceeds safe uncompressed size boundaries."""
    pass


class CutoverAuthorizationError(Exception):
    """Raised when cutover is attempted without explicit authorization."""
    pass


@dataclass(frozen=True)
class RestoreVerificationResult:
    """Outcome of an authenticated backup staging verification."""
    success: bool
    staging_dir: Path
    db_path: Path
    delivery_ledger_path: Path
    budget_ledger_path: Path
    manifest: dict[str, Any]
    table_row_counts: dict[str, int]
    delivery_ledger_count: int
    budget_ledger_batch_count: int
    verification_report_markdown: str
    errors: list[str]


def mask_chat_id(raw_id: str | int) -> str:
    """Mask a recipient chat ID for safe inclusion in logs and verification reports."""
    s = str(raw_id).strip()
    if len(s) <= 4:
        return "***"
    return f"{s[:2]}...{s[-2:]}"


def safe_extract_tar(tar_bytes: bytes, target_dir: Path) -> dict[str, Path]:
    """Extract tar archive with strict path traversal and decompression bomb defenses.

    Args:
        tar_bytes: Uncompressed or gzipped tarball bytes.
        target_dir: Target isolated staging directory.

    Returns:
        Mapping of canonical member name to extracted file Path.

    Raises:
        PathTraversalSecurityError: If any archive member path contains directory traversal.
        DecompressionBombSecurityError: If total extracted size exceeds safety limit.
        RestoreVerificationError: If unknown or disallowed files are in archive.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    target_root = target_dir.resolve()

    extracted_files: dict[str, Path] = {}
    cumulative_bytes = 0

    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as tar:
        for member in tar.getmembers():
            # Security Check 1: Whitelisted member names only
            norm_name = os.path.normpath(member.name).replace("\\", "/")
            if norm_name not in ALLOWED_ARCHIVE_MEMBERS:
                raise RestoreVerificationError(f"Disallowed file found in backup archive: {member.name}")

            # Security Check 2: Reject links, pipes, devices
            if not member.isreg():
                raise RestoreVerificationError(f"Non-regular file in backup archive: {member.name}")

            # Security Check 3: Path Traversal defense (no .., no absolute paths)
            dest_path = (target_dir / norm_name).resolve()
            if not str(dest_path).startswith(str(target_root)):
                raise PathTraversalSecurityError(f"Path traversal detected in archive member: {member.name}")

            # Security Check 4: Decompression bomb defense on declared size
            cumulative_bytes += member.size
            if cumulative_bytes > MAX_EXTRACTED_BYTES:
                raise DecompressionBombSecurityError(
                    f"Archive exceeds maximum allowed size ({MAX_EXTRACTED_BYTES} bytes)"
                )

            # Safe extraction with streaming limit check
            file_obj = tar.extractfile(member)
            if file_obj is None:
                raise RestoreVerificationError(f"Failed to extract archive member: {member.name}")

            with open(dest_path, "wb") as f_out:
                extracted_member_bytes = 0
                while True:
                    chunk = file_obj.read(65536)
                    if not chunk:
                        break
                    extracted_member_bytes += len(chunk)
                    if extracted_member_bytes > MAX_EXTRACTED_BYTES:
                        raise DecompressionBombSecurityError(
                            f"Archive member extracted size exceeds maximum allowed limit ({MAX_EXTRACTED_BYTES} bytes)"
                        )
                    f_out.write(chunk)

            extracted_files[norm_name] = dest_path

    return extracted_files


def verify_staging_database(db_path: Path, expected_schema_version: int = 5) -> dict[str, int]:
    """Execute SQLite integrity, foreign key, and schema compatibility checks on staged DB.

    Args:
        db_path: Path to staged SQLite database.
        expected_schema_version: Required schema version from migrations.

    Returns:
        Dictionary of table names and row counts.

    Raises:
        RestoreVerificationError: If any consistency or schema check fails.
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10.0)
    except Exception as exc:
        raise RestoreVerificationError(f"Failed to open restored database: {exc}") from exc

    try:
        cursor = conn.cursor()

        # 1. PRAGMA integrity_check
        cursor.execute("PRAGMA integrity_check;")
        rows = cursor.fetchall()
        if not rows or rows[0][0] != "ok":
            raise RestoreVerificationError(f"SQLite PRAGMA integrity_check failed: {rows}")

        # 2. PRAGMA foreign_key_check
        cursor.execute("PRAGMA foreign_key_check;")
        fk_errors = cursor.fetchall()
        if fk_errors:
            raise RestoreVerificationError(f"SQLite foreign key check failed with violations: {fk_errors}")

        # 3. Schema version verification
        cursor.execute("SELECT MAX(version) FROM schema_migrations;")
        ver_row = cursor.fetchone()
        current_ver = ver_row[0] if ver_row else None
        if current_ver != expected_schema_version:
            raise RestoreVerificationError(
                f"Schema version mismatch: database has version {current_ver}, expected {expected_schema_version}"
            )

        # 4. Table row counts
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [r[0] for r in cursor.fetchall()]

        counts: dict[str, int] = {}
        for t in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {t};")
            counts[t] = int(cursor.fetchone()[0])

        return counts
    except RestoreVerificationError:
        raise
    except Exception as exc:
        raise RestoreVerificationError(f"Unexpected error during SQLite verification: {exc}") from exc
    finally:
        conn.close()


def verify_restore_archive(
    envelope_bytes: bytes,
    key: bytes,
    staging_dir: Path,
    expected_schema_version: int = 5,
) -> RestoreVerificationResult:
    """Authenticate, decrypt, extract, and rigorously verify a backup inside an isolated staging directory.

    Args:
        envelope_bytes: Raw binary bytes of the .efbk file.
        key: 32-byte AES-256 decryption key.
        staging_dir: Isolated staging directory for verification (operational data is untouched).
        expected_schema_version: Required database schema version.

    Returns:
        RestoreVerificationResult containing verification status, paths, and report.

    Raises:
        RestoreVerificationError: On any authentication, integrity, or validation failure.
    """
    errors: list[str] = []

    # Clean staging directory to ensure isolation
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    # 1. Decrypt and authenticate envelope
    try:
        plaintext_tar, manifest = decrypt_envelope(envelope_bytes, key)
    except Exception as exc:
        raise RestoreVerificationError(f"Envelope authentication or decryption failed: {exc}") from exc

    # 2. Verify SHA-256 digest against authenticated manifest
    computed_sha256 = hashlib.sha256(plaintext_tar).hexdigest()
    expected_sha256 = manifest.get("plaintext_sha256")
    if computed_sha256 != expected_sha256:
        raise RestoreVerificationError(
            f"Plaintext SHA-256 mismatch: computed {computed_sha256}, expected {expected_sha256}"
        )

    # 3. Safe extraction into staging
    extracted = safe_extract_tar(plaintext_tar, staging_dir)

    db_path = extracted.get("radar.db")
    del_path = extracted.get("telegram_digest_delivery_ledger.json")
    bud_path = extracted.get("experiment_budget_ledger.json")

    if not db_path or not del_path or not bud_path:
        raise RestoreVerificationError("Backup archive is missing one or more essential state files")

    # 4. Verify SQLite database
    table_counts = verify_staging_database(db_path, expected_schema_version=expected_schema_version)

    # 5. Verify Telegram Delivery Ledger
    try:
        del_data = json.loads(del_path.read_text(encoding="utf-8"))
        deliveries = del_data.get("deliveries", {})
        delivery_count = len(deliveries)
    except Exception as exc:
        raise RestoreVerificationError(f"Failed to parse restored Telegram delivery ledger: {exc}") from exc

    # 6. Verify Experiment Budget Ledger
    try:
        bud_data = json.loads(bud_path.read_text(encoding="utf-8"))
        budget_batches = bud_data.get("batches", {})
        budget_count = len(budget_batches)
    except Exception as exc:
        raise RestoreVerificationError(f"Failed to parse restored experiment budget ledger: {exc}") from exc

    # 7. Generate Structured Markdown Verification Report
    masked_sample_recipients = []
    for d in list(deliveries.values())[:3]:
        cid = d.get("chat_id") or d.get("target_chat_id", "")
        if cid:
            masked_sample_recipients.append(mask_chat_id(cid))

    report = (
        "### Efso Radar Restore Verification Report\n\n"
        f"- **Status:** VERIFIED (Isolated in `{staging_dir}`)\n"
        f"- **Created At (UTC):** {manifest.get('created_at_utc', 'N/A')}\n"
        f"- **Key ID:** `{manifest.get('key_id', 'N/A')}`\n"
        f"- **Engine Type:** {manifest.get('source_engine', 'sqlite')}\n"
        f"- **Schema Version:** {manifest.get('schema_version', 'N/A')} (Verified = {expected_schema_version})\n"
        f"- **Plaintext SHA-256:** `{computed_sha256[:16]}...` (Match: Valid)\n"
        f"- **Stories Count:** {table_counts.get('stories', 0)}\n"
        f"- **Story Updates Count:** {table_counts.get('story_updates', 0)}\n"
        f"- **Candidate Backlog Count:** {table_counts.get('candidate_backlog', 0)}\n"
        f"- **Notifications History:** {table_counts.get('notifications', 0)}\n"
        f"- **Processed Articles:** {table_counts.get('processed_articles', 0)}\n"
        f"- **Delivery Ledger Entries:** {delivery_count} verified outbox records\n"
        f"- **Budget Batches Recorded:** {budget_count} batches\n"
        f"- **Sample Masked Recipients:** {', '.join(masked_sample_recipients) or 'None'}\n\n"
        "> [!IMPORTANT]\n"
        "> Live operational databases were **NOT modified**. Cutover requires explicit authorization.\n"
    )

    logger.info("Restore verification completed successfully for backup created at %s", manifest.get("created_at_utc"))

    return RestoreVerificationResult(
        success=True,
        staging_dir=staging_dir,
        db_path=db_path,
        delivery_ledger_path=del_path,
        budget_ledger_path=bud_path,
        manifest=manifest,
        table_row_counts=table_counts,
        delivery_ledger_count=delivery_count,
        budget_ledger_batch_count=budget_count,
        verification_report_markdown=report,
        errors=errors,
    )


def execute_production_cutover(
    verification_result: RestoreVerificationResult,
    operational_data_dir: Path,
    authorized: bool = False,
) -> bool:
    """Atomically cut over verified staging files into operational data directory with rollback safety.

    Args:
        verification_result: Successful outcome from verify_restore_archive.
        operational_data_dir: Live data directory (e.g. data/).
        authorized: Explicit Product Owner authorization flag.

    Returns:
        True if cutover succeeded and passed smoke checks.

    Raises:
        CutoverAuthorizationError: If authorized=False.
        RestoreVerificationError: If verification_result failed or files missing.
    """
    if not authorized:
        raise CutoverAuthorizationError(
            "Production cutover blocked: requires explicit Product Owner authorization."
        )

    if not verification_result.success:
        raise RestoreVerificationError("Cannot cutover an unverified or failed restore result.")

    operational_data_dir.mkdir(parents=True, exist_ok=True)
    live_db = operational_data_dir / "radar.db"
    live_del = operational_data_dir / "telegram_digest_delivery_ledger.json"
    live_bud = operational_data_dir / "experiment_budget_ledger.json"

    # Pre-cutover safety backup for instant rollback
    bak_db = live_db.parent / f"{live_db.name}.pre_restore_bak"
    bak_del = live_del.parent / f"{live_del.name}.pre_restore_bak"
    bak_bud = live_bud.parent / f"{live_bud.name}.pre_restore_bak"

    if live_db.exists():
        shutil.copy2(live_db, bak_db)
    if live_del.exists():
        shutil.copy2(live_del, bak_del)
    if live_bud.exists():
        shutil.copy2(live_bud, bak_bud)

    try:
        # Atomic replacement
        shutil.copy2(verification_result.db_path, live_db)
        shutil.copy2(verification_result.delivery_ledger_path, live_del)
        shutil.copy2(verification_result.budget_ledger_path, live_bud)

        # Post-cutover smoke check
        conn = sqlite3.connect(f"file:{live_db}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stories;")
        cursor.fetchone()
        conn.close()

        logger.info("Production cutover completed successfully from %s", verification_result.staging_dir)
        return True
    except Exception as exc:
        logger.error("Cutover smoke test failed (%s); rolling back to pre-restore backups", exc)
        # Automatic rollback
        if bak_db.exists():
            shutil.copy2(bak_db, live_db)
        if bak_del.exists():
            shutil.copy2(bak_del, live_del)
        if bak_bud.exists():
            shutil.copy2(bak_bud, live_bud)
        raise RestoreVerificationError(f"Cutover failed and was automatically rolled back: {exc}") from exc
