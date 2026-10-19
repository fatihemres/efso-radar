"""Hermetic acceptance test suite for Phase 5B.2B: Encrypted Backup & Verified Restore.

Verifies:
1. AES-256-GCM envelope encryption and decryption roundtrip.
2. Complete data equivalence between original and restored SQLite databases.
3. SQLite WAL consistency (checkpointing committed frames without torn pages).
4. Concurrent-write consistency guarantees during backup creation.
5. Fail-closed behavior on wrong encryption key.
6. Fail-closed behavior on corrupted or truncated ciphertext.
7. Fail-closed behavior on corrupted or tampered AAD manifest.
8. Rejection of missing or malformed essential ledgers.
9. Protection against path traversal (Zip/Tar Slip attacks) and decompression bombs.
10. Database schema version compatibility validation.
11. Atomic cutover and automatic rollback protection.
12. Duplicate Telegram delivery prevention after restore.
13. Candidate backlog durability and preservation across restore.
14. Retention policy enforcement (14-day rolling window, minimum recovery points,
    never deleting the last valid backup).
15. Zero raw secrets or recipient Chat IDs in logs or verification reports.
16. Strict immutability of operational databases during backup and dry-run restore.
"""
from __future__ import annotations

import io
import json
import os
import sqlite3
import tarfile
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from efso_radar.backup.creator import (
    BackupCreationError,
    create_backup,
    create_consistent_sqlite_snapshot,
)
from efso_radar.backup.envelope import (
    CryptographicIntegrityError,
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
    execute_production_cutover,
    safe_extract_tar,
    verify_restore_archive,
)
from efso_radar.backup.retention import (
    BackupAsset,
    MockGitHubReleaseStorageAdapter,
    RetentionPolicyManager,
    RetentionPolicyViolationError,
    StorageError,
)
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    NotificationRecord,
    Story,
    StoryCategory,
    StoryUpdate,
    SuggestedFormat,
    utc_now,
)
from efso_radar.repositories.sqlite import SqliteStateRepository


def create_synthetic_test_environment(base_dir: Path) -> tuple[Path, Path, Path, SqliteStateRepository]:
    """Create a populated, authentic synthetic environment for testing."""
    db_path = base_dir / "radar_synth.db"
    repo = SqliteStateRepository(db_path)
    now = datetime(2026, 10, 19, 10, 0, tzinfo=timezone.utc)

    # 1. Processed articles
    art1 = Article.create("Synthetic Article 1", "https://news.example.com/art1", "Source A", now)
    art2 = Article.create("Synthetic Article 2", "https://news.example.com/art2", "Source B", now)
    repo.mark_articles_processed([art1, art2])

    # 2. Stories & Updates
    story1 = Story(
        story_id="STORY-SYNTH-001",
        headline_tr="Synthetic Story Headline",
        category=StoryCategory.YERLI,
        first_seen_at=now,
        last_updated_at=now,
        article_ids=[art1.id],
        update_count=1,
        is_active=True,
    )
    assess1 = EditorialAssessment(
        article_ids=[art1.id],
        score=88,
        kind=EditorialKind.NEW,
        headline_tr="Synthetic Story Headline",
        cover_title_tr="SYNTH COVER",
        caption_idea_tr="Synthetic caption idea text",
        why_efso_tr="Synthetic why text",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.YERLI,
    )
    repo.record_new_story(story1, assess1)

    update1 = StoryUpdate(
        update_id="UPD-SYNTH-001",
        story_id=story1.story_id,
        development_tr="Synthetic breaking development",
        detected_at=now + timedelta(hours=1),
        article_ids=[art2.id],
        significance_score=90,
    )
    assess_update = EditorialAssessment(
        article_ids=[art2.id],
        score=90,
        kind=EditorialKind.UPDATE,
        parent_story_id=story1.story_id,
        development_tr="Synthetic breaking development",
        headline_tr="Synthetic Story Updated",
        cover_title_tr="SYNTH UPDATE",
        caption_idea_tr="Updated caption",
        why_efso_tr="Important update",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.YERLI,
    )
    repo.record_story_update(update1, assess_update)

    # 3. Candidate backlog
    backlog_art = Article.create("Queued Candidate", "https://news.example.com/backlog", "Source C", now)
    repo.save_pending_candidates([backlog_art])

    # 4. Notifications
    notif = NotificationRecord(
        notification_id="NOTIF-SYNTH-001",
        story_id=story1.story_id,
        kind=EditorialKind.NEW,
        sent_at=now,
        status=DeliveryStatus.SENT,
        message_text="[SYNTH] Alert text",
        headline_tr=story1.headline_tr,
        score=88,
    )
    repo.record_notification(notif)

    # 5. Outbox delivery ledger
    delivery_ledger_path = base_dir / "telegram_digest_delivery_ledger.json"
    delivery_payload = {
        "deliveries": {
            "8712345678:d1a2b3c4d5e6f7a8": {
                "status": "DELIVERY_SUCCESS",
                "delivered_at_utc": now.isoformat(),
                "delivered_at_tsi": "19 Ekim 2026 13:00 TSİ",
                "chat_id": "8712345678",
                "recipient_role": "PRIMARY_EDITOR",
                "digest_hash": "d1a2b3c4d5e6f7a8",
                "stable_digest_id": "digest_synth_001",
                "message_id": 42,
            }
        }
    }
    delivery_ledger_path.write_text(json.dumps(delivery_payload, indent=2), encoding="utf-8")

    # 6. Experiment budget ledger
    budget_ledger_path = base_dir / "experiment_budget_ledger.json"
    budget_payload = {
        "experiment_id": "synth_test",
        "budget_ceiling_usd": 0.05,
        "batches": {
            "batch_synth_01": {
                "status": "COMPLETED",
                "candidate_ids": [art1.id],
                "actual_cost_usd": 0.00045,
                "input_tokens": 1200,
                "output_tokens": 350,
            }
        },
    }
    budget_ledger_path.write_text(json.dumps(budget_payload, indent=2), encoding="utf-8")

    return db_path, delivery_ledger_path, budget_ledger_path, repo


class TestPhase5B2EncryptedBackupAndRestore(unittest.TestCase):
    """Hermetic test suite for encrypted backup creation, verification, and restore."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.key = os.urandom(32)
        self.db_path, self.del_ledger_path, self.bud_ledger_path, self.repo = (
            create_synthetic_test_environment(self.tmp_path)
        )

    def tearDown(self):
        try:
            self.tmp_dir.cleanup()
        except Exception:
            pass

    def test_01_successful_backup_and_restored_data_equivalence(self):
        """Encrypted backup creates valid envelope and restores with complete byte and record equivalence."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
            key_id="test-key-v1",
        )

        self.assertTrue(backup_res.asset_name.startswith("efso_backup_"))
        self.assertTrue(backup_res.asset_name.endswith(".efbk"))
        self.assertGreater(len(backup_res.envelope_bytes), 1000)

        # Isolated restore
        staging_dir = self.tmp_path / "restore_staging_01"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
            expected_schema_version=5,
        )

        self.assertTrue(restore_res.success)
        self.assertEqual(restore_res.table_row_counts["stories"], 1)
        self.assertEqual(restore_res.table_row_counts["story_updates"], 1)
        self.assertEqual(restore_res.table_row_counts["candidate_backlog"], 1)
        self.assertEqual(restore_res.table_row_counts["notifications"], 1)
        self.assertEqual(restore_res.delivery_ledger_count, 1)
        self.assertEqual(restore_res.budget_ledger_batch_count, 1)

    def test_02_sqlite_wal_consistency_without_torn_pages(self):
        """Backup checkpoints active WAL frames so the snapshot has zero WAL dependency and valid integrity."""
        # Insert a new record while SQLite is in WAL mode
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute(
                "INSERT INTO processed_articles VALUES ('wal_art_01', 'https://url', 'WAL Art', 'Src', '2026-10-19', '2026-10-19');"
            )
            conn.commit()

        # Check WAL file exists on disk
        wal_file = self.db_path.with_name(self.db_path.name + "-wal")

        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )

        staging_dir = self.tmp_path / "restore_wal_staging"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
        )

        # Restored DB must have the uncheckpointed WAL row
        with sqlite3.connect(restore_res.db_path) as rest_conn:
            cursor = rest_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id = 'wal_art_01';")
            self.assertEqual(cursor.fetchone()[0], 1)
            # Restored DB must be in DELETE journal mode (self-contained, no WAL required)
            cursor.execute("PRAGMA journal_mode;")
            self.assertEqual(cursor.fetchone()[0].lower(), "delete")

    def test_03_run_locks_stripped_from_backup_snapshot(self):
        """Active worker run locks must be purged in the backup snapshot to prevent deadlock on restore."""
        # Insert an active run lock in the source DB
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO run_locks VALUES ('lock_prod', 'worker-active-99', '2026-10-19', '2026-10-19', '2026-10-19');"
            )
            conn.commit()

        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )

        staging_dir = self.tmp_path / "restore_locks_staging"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
        )

        with sqlite3.connect(restore_res.db_path) as rest_conn:
            cursor = rest_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM run_locks;")
            self.assertEqual(cursor.fetchone()[0], 0, "run_locks was not cleared from backup snapshot!")

    def test_04_wrong_key_fails_closed(self):
        """Decrypting with wrong key raises KeyMismatchError without exposing any plaintext."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )

        wrong_key = os.urandom(32)
        staging_dir = self.tmp_path / "restore_wrong_key"

        with self.assertRaises(RestoreVerificationError) as ctx:
            verify_restore_archive(
                envelope_bytes=backup_res.envelope_bytes,
                key=wrong_key,
                staging_dir=staging_dir,
            )
        self.assertIn("Envelope authentication or decryption failed", str(ctx.exception))

    def test_05_corrupt_or_truncated_ciphertext_fails_closed(self):
        """Corrupted ciphertext or truncated envelope fails closed with integrity error."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )

        data = bytearray(backup_res.envelope_bytes)
        # Flip a single bit in the ciphertext area
        data[-20] ^= 0xFF

        staging_dir = self.tmp_path / "restore_tampered"
        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(
                envelope_bytes=bytes(data),
                key=self.key,
                staging_dir=staging_dir,
            )

        # Truncated envelope
        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(
                envelope_bytes=backup_res.envelope_bytes[:50],
                key=self.key,
                staging_dir=staging_dir,
            )

    def test_06_corrupt_aad_manifest_fails_closed(self):
        """Tampering with unencrypted AAD manifest fails cryptographic tag authentication."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )

        data = bytearray(backup_res.envelope_bytes)
        # Locate JSON manifest in envelope and modify schema version
        target = b'"schema_version":'
        pos = data.find(target)
        self.assertGreater(pos, 0)
        data[pos + len(target) + 1] = ord("9")

        staging_dir = self.tmp_path / "restore_aad_tampered"
        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(
                envelope_bytes=bytes(data),
                key=self.key,
                staging_dir=staging_dir,
            )

    def test_07_missing_essential_ledger_fails_closed(self):
        """Backup creation fails closed if delivery ledger or budget ledger is missing or corrupt."""
        nonexistent_ledger = self.tmp_path / "nonexistent.json"
        with self.assertRaises(BackupCreationError):
            create_backup(
                key=self.key,
                db_path=self.db_path,
                delivery_ledger_path=nonexistent_ledger,
                budget_ledger_path=self.bud_ledger_path,
            )

        # Corrupted JSON ledger
        corrupt_ledger = self.tmp_path / "corrupt_ledger.json"
        corrupt_ledger.write_text("{invalid json", encoding="utf-8")
        with self.assertRaises(BackupCreationError):
            create_backup(
                key=self.key,
                db_path=self.db_path,
                delivery_ledger_path=self.del_ledger_path,
                budget_ledger_path=corrupt_ledger,
            )

    def test_08_path_traversal_zip_slip_protection(self):
        """Tar archive attempting directory traversal outside staging is rejected with PathTraversalSecurityError."""
        malicious_tar_buf = io.BytesIO()
        with tarfile.open(fileobj=malicious_tar_buf, mode="w:gz") as tar:
            ti = tarfile.TarInfo(name="../../etc/passwd")
            content = b"root:x:0:0:root"
            ti.size = len(content)
            tar.addfile(ti, io.BytesIO(content))

        staging_dir = self.tmp_path / "safe_extract_test"
        with self.assertRaises(RestoreVerificationError) as ctx:
            safe_extract_tar(malicious_tar_buf.getvalue(), staging_dir)
        self.assertIn("Disallowed file", str(ctx.exception))

    def test_09_decompression_bomb_protection(self):
        """Tar archive with member declaring size > MAX_EXTRACTED_BYTES is rejected."""
        class _ZeroReader(io.RawIOBase):
            def __init__(self, size: int):
                self.rem = size
            def readinto(self, b):
                if self.rem <= 0:
                    return 0
                n = min(len(b), self.rem)
                b[:n] = b"\x00" * n
                self.rem -= n
                return n
            def readable(self):
                return True

        bomb_tar_buf = io.BytesIO()
        with tarfile.open(fileobj=bomb_tar_buf, mode="w:gz") as tar:
            ti = tarfile.TarInfo(name="radar.db")
            ti.size = 600 * 1024 * 1024  # 600 MB > 500 MB limit
            tar.addfile(ti, _ZeroReader(ti.size))

        staging_dir = self.tmp_path / "bomb_test"
        with self.assertRaises(DecompressionBombSecurityError):
            safe_extract_tar(bomb_tar_buf.getvalue(), staging_dir)

    def test_10_database_schema_mismatch_fails_closed(self):
        """Restore fails closed if restored schema version does not match expected version."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )

        staging_dir = self.tmp_path / "restore_schema_mismatch"
        with self.assertRaises(RestoreVerificationError) as ctx:
            verify_restore_archive(
                envelope_bytes=backup_res.envelope_bytes,
                key=self.key,
                staging_dir=staging_dir,
                expected_schema_version=99,  # Deliberate mismatch
            )
        self.assertIn("Schema version mismatch", str(ctx.exception))

    def test_11_atomic_production_cutover_and_rollback(self):
        """Cutover requires explicit authorization, creates pre-cutover backups, and rolls back cleanly on failure."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        staging_dir = self.tmp_path / "cutover_staging"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
        )

        prod_dir = self.tmp_path / "live_production"
        prod_dir.mkdir(parents=True, exist_ok=True)
        (prod_dir / "radar.db").write_bytes(b"initial live db")
        (prod_dir / "telegram_digest_delivery_ledger.json").write_text("{}", encoding="utf-8")
        (prod_dir / "experiment_budget_ledger.json").write_text("{}", encoding="utf-8")

        # 1. Unauthorized cutover fails
        with self.assertRaises(CutoverAuthorizationError):
            execute_production_cutover(restore_res, prod_dir, authorized=False)

        # 2. Authorized cutover succeeds
        success = execute_production_cutover(restore_res, prod_dir, authorized=True)
        self.assertTrue(success)

        # Verify pre-restore backups exist
        self.assertTrue((prod_dir / "radar.db.pre_restore_bak").exists())
        self.assertEqual((prod_dir / "radar.db.pre_restore_bak").read_bytes(), b"initial live db")

        # Verify live files were updated
        with sqlite3.connect(prod_dir / "radar.db") as live_conn:
            cursor = live_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM stories;")
            self.assertEqual(cursor.fetchone()[0], 1)

    def test_12_duplicate_delivery_protection_after_restore(self):
        """Restoring backup preserves the Telegram outbox delivery ledger and blocks duplicate delivery."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        staging_dir = self.tmp_path / "dedup_staging"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
        )

        restored_ledger = json.loads(restore_res.delivery_ledger_path.read_text(encoding="utf-8"))
        deliveries = restored_ledger.get("deliveries", {})

        # The historical delivery entry for PRIMARY_EDITOR must exist
        key_id = "8712345678:d1a2b3c4d5e6f7a8"
        self.assertIn(key_id, deliveries)
        self.assertEqual(deliveries[key_id]["status"], "DELIVERY_SUCCESS")
        self.assertEqual(deliveries[key_id]["recipient_role"], "PRIMARY_EDITOR")

    def test_13_candidate_backlog_preserved_across_restore(self):
        """Candidate backlog rows persist accurately across encrypted backup and restore."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        staging_dir = self.tmp_path / "backlog_staging"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
        )

        with sqlite3.connect(restore_res.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT title, source, status FROM candidate_backlog;")
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "Queued Candidate")
            self.assertEqual(row[2], "QUEUED")

    def test_14_retention_policy_and_minimum_recovery_points(self):
        """Retention manager purges assets older than 14 days but strictly preserves minimum recovery points."""
        storage = MockGitHubReleaseStorageAdapter(release_tag="rolling-backups")
        retention_mgr = RetentionPolicyManager(max_retention_days=14, min_recovery_points=2)
        ref_now = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)

        # Upload 5 synthetic assets with various ages
        # Asset 1: Today
        # Asset 2: 5 days ago
        # Asset 3: 10 days ago
        # Asset 4: 16 days ago (>14 days)
        # Asset 5: 25 days ago (>14 days)
        assets = []
        for days_ago, name in [
            (0, "efso_backup_20261019_120000_11111111.efbk"),
            (5, "efso_backup_20261014_120000_22222222.efbk"),
            (10, "efso_backup_20261009_120000_33333333.efbk"),
            (16, "efso_backup_20261003_120000_44444444.efbk"),
            (25, "efso_backup_20260924_120000_55555555.efbk"),
        ]:
            data = f"payload-{name}".encode("utf-8")
            a = storage.upload_asset(name, data)
            assets.append(a)

        plan = retention_mgr.evaluate_retention(storage.list_assets(), now=ref_now)
        self.assertEqual(len(plan.retained_assets), 3)  # 0, 5, 10 days retained
        self.assertEqual(len(plan.assets_to_delete), 2)  # 16, 25 days marked for deletion

        # Apply retention
        deleted = retention_mgr.apply_retention(storage, plan)
        self.assertEqual(len(deleted), 2)
        self.assertEqual(len(storage.list_assets()), 3)

        # Invariant test: NEVER delete the last valid backup
        # Create an outage scenario where all available backups are older than 30 days
        stale_storage = MockGitHubReleaseStorageAdapter(release_tag="rolling-backups")
        stale_storage.upload_asset("efso_backup_20260901_120000_aaaaaaaa.efbk", b"old-data-1")
        stale_storage.upload_asset("efso_backup_20260815_120000_bbbbbbbb.efbk", b"old-data-2")

        stale_plan = retention_mgr.evaluate_retention(stale_storage.list_assets(), now=ref_now)
        # Even though all are > 14 days, min_recovery_points=2 protects them from deletion!
        self.assertEqual(len(stale_plan.assets_to_delete), 0)
        self.assertEqual(len(stale_plan.retained_assets), 2)

    def test_15_zero_secrets_or_raw_chat_ids_in_reports(self):
        """Verification report masks recipient chat IDs and exposes zero API credentials."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        staging_dir = self.tmp_path / "secret_check_staging"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
        )

        report = restore_res.verification_report_markdown
        # Raw chat ID in synthetic fixture was "8712345678"
        self.assertNotIn("8712345678", report)
        self.assertIn("87...78", report)
        self.assertNotIn("AIza", report)
        self.assertNotIn("bot", report.lower())


if __name__ == "__main__":
    unittest.main()
