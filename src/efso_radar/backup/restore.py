"""Isolated staging verification, fail-closed restore engine, and safe cutover lifecycle.

Ensures backups are authenticated, decrypted, and verified exclusively inside
an isolated staging directory outside of operational data paths. Enforces
safe staging path checks, SQLite integrity checks, schema validation, delivery
ledger identities, path traversal protection, decompression bomb limits,
post-restore Telegram delivery reconciliation, and transactional crash-recovery cutover.
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
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from efso_radar.backup.creator import (
    validate_budget_ledger_structure,
    validate_delivery_ledger_structure,
    MalformedLedgerError,
)
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


class UnsafeStagingPathError(RestoreVerificationError):
    """Raised when an unsafe or pre-existing non-empty staging path is provided."""
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


class ProductionCutoverDisabledError(Exception):
    """Raised when live production cutover is attempted in offline phases without authorization."""
    pass


class DeliveryBlockedUncertainRecoveryError(Exception):
    """Raised when Telegram dispatch is attempted while in DELIVERY_UNCERTAIN state."""
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


@dataclass
class CutoverJournal:
    """Crash-recovery journal for multi-file cutover operations."""
    cutover_id: str
    phase: str
    target_dir: str
    timestamp_utc: str
    db_backed_up: bool = False
    delivery_backed_up: bool = False
    budget_backed_up: bool = False
    step_history: list[str] = field(default_factory=list)

    def save(self, target_dir: Path) -> Path:
        journal_path = target_dir / ".cutover_journal.json"
        data = {
            "cutover_id": self.cutover_id,
            "phase": self.phase,
            "target_dir": self.target_dir,
            "timestamp_utc": self.timestamp_utc,
            "db_backed_up": self.db_backed_up,
            "delivery_backed_up": self.delivery_backed_up,
            "budget_backed_up": self.budget_backed_up,
            "step_history": self.step_history,
        }
        journal_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return journal_path


class TelegramDeliveryReconciliationGate:
    """Post-restore delivery reconciliation safety gate.

    Guarantees that restoring an older backup never allows digests sent after the
    backup timestamp to be inadvertently resent to PRIMARY_EDITOR.
    If surviving history has gaps, enters DELIVERY_UNCERTAIN state and blocks dispatch.
    """

    def __init__(
        self,
        restored_ledger_path: Path,
        surviving_ledger_path: Path | None = None,
        backup_created_at_utc: str | None = None,
    ):
        self.restored_ledger_path = restored_ledger_path
        self.surviving_ledger_path = surviving_ledger_path
        self.backup_created_at_utc = backup_created_at_utc
        self.dispatch_blocked = False
        self.reconciliation_status = "PENDING"
        self.reconciled_hashes: set[str] = set()
        self.audit_log: list[str] = []

    def reconcile(self) -> str:
        """Reconcile restored delivery ledger with surviving operational history."""
        # Load restored ledger
        restored_data = json.loads(self.restored_ledger_path.read_text(encoding="utf-8"))
        restored_deliveries = restored_data.get("deliveries", {})

        for d in restored_deliveries.values():
            if h := d.get("digest_hash"):
                self.reconciled_hashes.add(h)

        # Check for surviving operational history
        if self.surviving_ledger_path and self.surviving_ledger_path.exists():
            try:
                surviving_data = json.loads(self.surviving_ledger_path.read_text(encoding="utf-8"))
                surviving_deliveries = surviving_data.get("deliveries", {})

                merged_count = 0
                for del_id, record in surviving_deliveries.items():
                    h = record.get("digest_hash")
                    if h:
                        self.reconciled_hashes.add(h)
                    if del_id not in restored_deliveries:
                        restored_deliveries[del_id] = record
                        merged_count += 1

                # Write merged ledger back
                restored_data["deliveries"] = restored_deliveries
                self.restored_ledger_path.write_text(
                    json.dumps(restored_data, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )

                self.reconciliation_status = "RECONCILED"
                self.dispatch_blocked = False
                msg = f"Reconciled {merged_count} post-backup delivery records from surviving ledger."
                self.audit_log.append(msg)
                return msg
            except Exception as exc:
                self.reconciliation_status = "DELIVERY_UNCERTAIN"
                self.dispatch_blocked = True
                msg = f"Surviving ledger unreadable ({exc}); entered DELIVERY_UNCERTAIN."
                self.audit_log.append(msg)
                return msg

        # No surviving ledger available — potential delivery gap
        self.reconciliation_status = "DELIVERY_UNCERTAIN"
        self.dispatch_blocked = True
        msg = (
            f"No surviving ledger found to verify delivery window after backup {self.backup_created_at_utc}. "
            "Dispatch blocked under DELIVERY_UNCERTAIN policy until operator authorization."
        )
        self.audit_log.append(msg)
        return msg

    def check_can_dispatch(self, digest_hash: str) -> None:
        """Verify whether automated dispatch can proceed for given digest hash."""
        if self.dispatch_blocked:
            raise DeliveryBlockedUncertainRecoveryError(
                f"Automated dispatch blocked: system is in {self.reconciliation_status} state. "
                "Operator authorization required to resume delivery."
            )
        if digest_hash in self.reconciled_hashes:
            raise DeliveryBlockedUncertainRecoveryError(
                f"Digest {digest_hash[:16]} was already delivered (detected in reconciled history). Resend blocked."
            )

    def authorize_resumption(self, operator_note: str, token: str) -> None:
        """Operator explicitly clears delivery gap after reviewing outbox state."""
        if not operator_note or not token:
            raise ValueError("Operator note and confirmation token required to clear DELIVERY_UNCERTAIN state")
        self.dispatch_blocked = False
        self.reconciliation_status = "AUTHORIZED_RESUMPTION"
        self.audit_log.append(f"Resumption authorized by operator: note='{operator_note}', token='{token[:8]}'")


def mask_chat_id(raw_id: str | int) -> str:
    """Mask a recipient chat ID for safe inclusion in logs and verification reports."""
    s = str(raw_id).strip()
    if len(s) <= 4:
        return "***"
    return f"{s[:2]}...{s[-2:]}"


def validate_staging_path_safety(staging_dir: Path) -> Path:
    """Validate that a caller-provided staging path is non-destructive and isolated.

    Raises:
        UnsafeStagingPathError: If path points to root, user home, repo root, data dir, or non-empty dir.
    """
    resolved = staging_dir.resolve()
    repo_root = Path.cwd().resolve()

    # 1. Refuse root directory
    if resolved == resolved.parent:
        raise UnsafeStagingPathError(f"Staging path cannot be filesystem root: {resolved}")

    # 2. Refuse user home directory
    if resolved == Path.home().resolve():
        raise UnsafeStagingPathError(f"Staging path cannot be user home directory: {resolved}")

    # 3. Refuse repository root and src directory
    if resolved == repo_root or resolved == repo_root / "src":
        raise UnsafeStagingPathError(f"Staging path cannot be repository root or code directory: {resolved}")

    # 4. Refuse operational data directory
    if resolved == repo_root / "data" or resolved.name == "data":
        if (resolved / "radar.db").exists() or (resolved / "telegram_digest_delivery_ledger.json").exists():
            raise UnsafeStagingPathError(f"Staging path cannot be live operational data directory: {resolved}")

    # 5. Pre-existing non-empty directory check
    if resolved.exists():
        entries = list(resolved.iterdir())
        if entries:
            raise UnsafeStagingPathError(
                f"Staging path {resolved} is a pre-existing non-empty directory ({len(entries)} items). "
                "Refusing to delete or overwrite user data."
            )

    return resolved


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


def verify_staging_database(
    db_path: Path,
    expected_schema_version: int = 5,
    manifest_row_counts: dict[str, int] | None = None,
) -> dict[str, int]:
    """Execute SQLite integrity, foreign key, schema compatibility, and row count checks on staged DB.

    Args:
        db_path: Path to staged SQLite database.
        expected_schema_version: Required schema version from migrations.
        manifest_row_counts: Expected table row counts recorded in the manifest.

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

        # 3. Schema version verification from SELECT MAX(version)
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

        # 5. Validate manifest row counts against actual counts
        if manifest_row_counts:
            for tbl, expected_cnt in manifest_row_counts.items():
                actual_cnt = counts.get(tbl)
                if actual_cnt != expected_cnt:
                    raise RestoreVerificationError(
                        f"Row count mismatch for table '{tbl}': database has {actual_cnt}, "
                        f"manifest recorded {expected_cnt}"
                    )

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
    staging_dir: Path | None = None,
    expected_schema_version: int = 5,
) -> RestoreVerificationResult:
    """Authenticate, decrypt, extract, and rigorously verify a backup inside an isolated staging directory.

    Guarantees that no user files or pre-existing directories are wiped on wrong-key
    or corrupt-backup failures. Decryption and authentication occur prior to filesystem creation.

    Args:
        envelope_bytes: Raw binary bytes of the .efbk file.
        key: 32-byte AES-256 decryption key.
        staging_dir: Optional isolated staging directory (defaults to clean system temp).
        expected_schema_version: Required database schema version.

    Returns:
        RestoreVerificationResult containing verification status, paths, and report.

    Raises:
        RestoreVerificationError: On any authentication, integrity, or validation failure.
    """
    errors: list[str] = []

    # 1. Decrypt and authenticate envelope FIRST before modifying any filesystem path
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

    # 3. Verify manifest schema version matches expected application version
    manifest_schema_ver = manifest.get("schema_version")
    if manifest_schema_ver != expected_schema_version:
        raise RestoreVerificationError(
            f"Manifest schema version mismatch: manifest declared {manifest_schema_ver}, "
            f"expected {expected_schema_version}"
        )

    # 4. Resolve safe staging directory (only after crypto succeeds)
    if staging_dir is not None:
        target_staging = validate_staging_path_safety(staging_dir)
        target_staging.mkdir(parents=True, exist_ok=True)
    else:
        target_staging = Path(tempfile.mkdtemp(prefix="efso_restore_staging_"))

    # 5. Safe extraction into staging
    extracted = safe_extract_tar(plaintext_tar, target_staging)

    db_path = extracted.get("radar.db")
    del_path = extracted.get("telegram_digest_delivery_ledger.json")
    bud_path = extracted.get("experiment_budget_ledger.json")

    if not db_path or not del_path or not bud_path:
        raise RestoreVerificationError(
            f"Backup archive is incomplete; missing required components: {list(extracted.keys())}"
        )

    # 6. Verify SQLite Database & Compare Row Counts
    manifest_row_counts = manifest.get("table_row_counts", {})
    table_counts = verify_staging_database(
        db_path,
        expected_schema_version=expected_schema_version,
        manifest_row_counts=manifest_row_counts,
    )

    # 7. Strictly Validate Telegram Delivery Ledger
    try:
        del_data = json.loads(del_path.read_text(encoding="utf-8"))
        validate_delivery_ledger_structure(del_data, "restored_telegram_delivery_ledger")
        deliveries = del_data.get("deliveries", {})
        delivery_count = len(deliveries)
    except MalformedLedgerError as exc:
        raise RestoreVerificationError(f"Restored Telegram delivery ledger is malformed: {exc}") from exc
    except Exception as exc:
        raise RestoreVerificationError(f"Failed to parse restored Telegram delivery ledger: {exc}") from exc

    # 8. Strictly Validate Experiment Budget Ledger
    try:
        bud_data = json.loads(bud_path.read_text(encoding="utf-8"))
        validate_budget_ledger_structure(bud_data, "restored_experiment_budget_ledger")
        budget_batches = bud_data.get("batches", {})
        budget_count = len(budget_batches)
    except MalformedLedgerError as exc:
        raise RestoreVerificationError(f"Restored experiment budget ledger is malformed: {exc}") from exc
    except Exception as exc:
        raise RestoreVerificationError(f"Failed to parse restored experiment budget ledger: {exc}") from exc

    # 9. Generate Structured Markdown Verification Report
    masked_sample_recipients = []
    for d in list(deliveries.values())[:3]:
        cid = d.get("chat_id") or d.get("target_chat_id", "")
        if cid:
            masked_sample_recipients.append(mask_chat_id(cid))

    report = (
        "### Efso Radar Restore Verification Report\n\n"
        f"- **Status:** VERIFIED (Isolated in `{target_staging}`)\n"
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
        staging_dir=target_staging,
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


def verify_quiescence(operational_data_dir: Path) -> None:
    """Verify that all writers, dispatchers, and worker runs are paused and quiescent."""
    live_db = operational_data_dir / "radar.db"
    if not live_db.exists():
        return

    try:
        conn = sqlite3.connect(f"file:{live_db}?mode=ro", uri=True, timeout=5.0)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='run_locks';")
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM run_locks;")
            active_locks = cursor.fetchone()[0]
            if active_locks > 0:
                raise RestoreVerificationError(
                    f"Quiescence check failed: {active_locks} active worker run locks detected in database"
                )
        conn.close()
    except sqlite3.OperationalError as exc:
        raise RestoreVerificationError(f"Cannot verify database quiescence: {exc}") from exc


def detect_and_recover_incomplete_cutover(operational_data_dir: Path) -> bool:
    """Detect interrupted cutovers upon startup and roll back to consistent pre-cutover state.

    Returns:
        True if an incomplete cutover was detected and successfully rolled back.
    """
    journal_path = operational_data_dir / ".cutover_journal.json"
    if not journal_path.exists():
        return False

    try:
        data = json.loads(journal_path.read_text(encoding="utf-8"))
        phase = data.get("phase")
        if phase in ("COMPLETED", "ROLLED_BACK"):
            return False

        logger.critical(
            "Detected incomplete cutover (%s) in phase '%s'! Initiating emergency rollback...",
            data.get("cutover_id"),
            phase,
        )

        live_db = operational_data_dir / "radar.db"
        live_del = operational_data_dir / "telegram_digest_delivery_ledger.json"
        live_bud = operational_data_dir / "experiment_budget_ledger.json"

        bak_db = operational_data_dir / "radar.db.pre_cutover.bak"
        bak_del = operational_data_dir / "telegram_digest_delivery_ledger.json.pre_cutover.bak"
        bak_bud = operational_data_dir / "experiment_budget_ledger.json.pre_cutover.bak"

        if bak_db.exists():
            shutil.copy2(bak_db, live_db)
        if bak_del.exists():
            shutil.copy2(bak_del, live_del)
        if bak_bud.exists():
            shutil.copy2(bak_bud, live_bud)

        data["phase"] = "ROLLED_BACK"
        data["step_history"].append("Startup recovery executed emergency rollback")
        journal_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return True
    except Exception as exc:
        logger.critical("Failed to recover incomplete cutover: %s", exc)
        return False


def execute_production_cutover(
    verification_result: RestoreVerificationResult,
    operational_data_dir: Path,
    authorized: bool = False,
    simulated: bool = False,
    inject_failure_step: int | None = None,
) -> bool:
    """Safely cut over verified staging files into operational directory with transactional crash journal.

    Requires explicit authorization and verified quiescence. Uses SQLite online backup
    for pre-cutover database snapshot, atomic os.replace on temporary files, and
    provides crash-recovery rollback at each step.

    In Phase 5B.2B, live production cutover is disabled fail-closed.
    Simulated cutovers (simulated=True) can be executed offline against test directories.

    Args:
        verification_result: Successful outcome from verify_restore_archive.
        operational_data_dir: Live data directory (e.g. data/).
        authorized: Explicit Product Owner authorization flag.
        simulated: Must be True in Phase 5B.2B to perform offline cutover validation.
        inject_failure_step: Optional step index (1, 2, 3, 4) to test rollback resilience.

    Returns:
        True if cutover succeeded and passed smoke checks.

    Raises:
        ProductionCutoverDisabledError: If live production cutover is attempted (simulated=False).
        CutoverAuthorizationError: If authorized=False.
        RestoreVerificationError: If verification_result failed, quiescence failed, or smoke test failed.
    """
    if not authorized:
        raise CutoverAuthorizationError(
            "Production cutover blocked: requires explicit Product Owner authorization."
        )

    # In Phase 5B.2B, production cutover against real live environments is disabled fail-closed
    if not simulated:
        raise ProductionCutoverDisabledError(
            "Live production cutover is disabled fail-closed in Phase 5B.2B until an authorized "
            "operational maintenance window is approved. Use simulated=True for offline validation."
        )

    if not verification_result.success:
        raise RestoreVerificationError("Cannot cutover an unverified or failed restore result.")

    operational_data_dir.mkdir(parents=True, exist_ok=True)
    live_db = operational_data_dir / "radar.db"
    live_del = operational_data_dir / "telegram_digest_delivery_ledger.json"
    live_bud = operational_data_dir / "experiment_budget_ledger.json"

    # Step 0: Quiescence check
    verify_quiescence(operational_data_dir)

    cutover_id = f"cutover_{int(time.time())}"
    journal = CutoverJournal(
        cutover_id=cutover_id,
        phase="INITIATED",
        target_dir=str(operational_data_dir),
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        step_history=["Quiescence verified"],
    )
    journal.save(operational_data_dir)

    # Step 1: Pre-cutover consistent SQLite snapshot using backup API
    bak_db = operational_data_dir / "radar.db.pre_cutover.bak"
    bak_del = operational_data_dir / "telegram_digest_delivery_ledger.json.pre_cutover.bak"
    bak_bud = operational_data_dir / "experiment_budget_ledger.json.pre_cutover.bak"

    if live_db.exists():
        try:
            live_conn = sqlite3.connect(f"file:{live_db}?mode=ro", uri=True, timeout=10.0)
            bak_conn = sqlite3.connect(bak_db)
            live_conn.backup(bak_conn)
            live_conn.close()
            bak_conn.close()
            del live_conn
            del bak_conn
            import gc
            gc.collect()
            journal.db_backed_up = True
        except Exception as exc:
            journal.phase = "FAILED"
            journal.step_history.append(f"Pre-cutover DB backup failed: {exc}")
            journal.save(operational_data_dir)
            raise RestoreVerificationError(f"Failed to create pre-cutover DB backup: {exc}") from exc

    if live_del.exists():
        shutil.copy2(live_del, bak_del)
        journal.delivery_backed_up = True

    if live_bud.exists():
        shutil.copy2(live_bud, bak_bud)
        journal.budget_backed_up = True

    journal.phase = "PRE_BACKUP_COMPLETED"
    journal.step_history.append("Consistent pre-cutover backups created")
    journal.save(operational_data_dir)

    # Step 2: Atomic multi-file replacement with step-by-step rollback
    try:
        if inject_failure_step == 1:
            raise RuntimeError("Injected fault at step 1: DB replacement")

        # Atomic replacement for radar.db
        tmp_db = operational_data_dir / "radar.db.tmp_cutover"
        shutil.copy2(verification_result.db_path, tmp_db)
        import gc
        gc.collect()
        os.replace(tmp_db, live_db)
        journal.phase = "REPLACED_DB"
        journal.step_history.append("radar.db replaced atomically")
        journal.save(operational_data_dir)

        if inject_failure_step == 2:
            raise RuntimeError("Injected fault at step 2: Delivery ledger replacement")

        # Atomic replacement for delivery ledger
        tmp_del = operational_data_dir / "telegram_digest_delivery_ledger.json.tmp_cutover"
        shutil.copy2(verification_result.delivery_ledger_path, tmp_del)
        os.replace(tmp_del, live_del)
        journal.phase = "REPLACED_DELIVERY_LEDGER"
        journal.step_history.append("delivery ledger replaced atomically")
        journal.save(operational_data_dir)

        if inject_failure_step == 3:
            raise RuntimeError("Injected fault at step 3: Budget ledger replacement")

        # Atomic replacement for budget ledger
        tmp_bud = operational_data_dir / "experiment_budget_ledger.json.tmp_cutover"
        shutil.copy2(verification_result.budget_ledger_path, tmp_bud)
        os.replace(tmp_bud, live_bud)
        journal.phase = "REPLACED_BUDGET_LEDGER"
        journal.step_history.append("budget ledger replaced atomically")
        journal.save(operational_data_dir)

        if inject_failure_step == 4:
            raise RuntimeError("Injected fault at step 4: Smoke test failure")

        # Step 3: Post-cutover smoke test
        conn = sqlite3.connect(f"file:{live_db}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stories;")
        cursor.fetchone()
        conn.close()

        journal.phase = "COMPLETED"
        journal.step_history.append("Smoke test passed successfully")
        journal.save(operational_data_dir)

        logger.info("Production cutover completed successfully from %s", verification_result.staging_dir)
        return True
    except Exception as exc:
        logger.error("Cutover failed (%s); executing verified rollback...", exc)
        # Immediate rollback
        if bak_db.exists():
            shutil.copy2(bak_db, live_db)
        if bak_del.exists():
            shutil.copy2(bak_del, live_del)
        if bak_bud.exists():
            shutil.copy2(bak_bud, live_bud)

        journal.phase = "ROLLED_BACK"
        journal.step_history.append(f"Rolled back due to error: {exc}")
        journal.save(operational_data_dir)
        raise RestoreVerificationError(f"Cutover failed and was safely rolled back: {exc}") from exc
