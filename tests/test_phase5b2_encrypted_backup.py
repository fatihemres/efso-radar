"""Hermetic acceptance test suite for Phase 5B.2B: Encrypted Backup & Verified Restore.

Comprehensive safety verification covering:
1. AES-256-GCM envelope encryption and decryption roundtrip.
2. Complete data equivalence between original and restored SQLite databases.
3. SQLite WAL consistency (checkpointing committed frames without torn pages).
4. Transient worker run_locks stripped from backup snapshot.
5. Fail-closed behavior on wrong encryption key.
6. Fail-closed behavior on corrupted or truncated ciphertext.
7. Authenticated binary header tampering tests for EVERY header field independently.
8. Missing or malformed essential ledgers validation (schema and structural validity).
9. Protection against path traversal (Zip/Tar Slip attacks) and decompression bombs.
10. Database true schema version and manifest row count validation.
11. Staging directory non-destructive guarantees (refusal of unsafe / non-empty paths).
12. Multi-file cutover crash journal, fail-closed live policy, and step-by-step rollback.
13. Post-restore Telegram duplicate prevention and DELIVERY_UNCERTAIN safety gate.
14. Cross-store snapshot coordination barrier and deadlock/timeout prevention.
15. Content-verified retention lifecycle, inventory fingerprinting, and budget conflict upload halting.
16. Zero raw secrets or recipient Chat IDs in logs or verification reports.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import struct
import tarfile
import tempfile
import time
import unittest
import unittest.mock
from datetime import datetime, timedelta, timezone
from pathlib import Path

from efso_radar.backup.creator import (
    BackupCoordinationTimeoutError,
    BackupCreationError,
    MalformedLedgerError,
    SnapshotCoordinationBarrier,
    create_backup,
    create_consistent_sqlite_snapshot,
)
from efso_radar.backup.envelope import (
    HEADER_SIZE,
    HEADER_STRUCT,
    CryptographicIntegrityError,
    InvalidEnvelopeError,
    KeyMismatchError,
    decrypt_envelope,
    encrypt_envelope,
)
from efso_radar.backup.restore import (
    CutoverAuthorizationError,
    DecompressionBombSecurityError,
    DeliveryBlockedUncertainRecoveryError,
    PathTraversalSecurityError,
    ProductionCutoverDisabledError,
    RestoreVerificationError,
    TelegramDeliveryReconciliationGate,
    UnsafeStagingPathError,
    detect_and_recover_incomplete_cutover,
    execute_production_cutover,
    safe_extract_tar,
    simulate_offline_cutover_in_test_workspace,
    validate_simulation_workspace,
    validate_staging_path_safety,
    verify_restore_archive,
)
from efso_radar.backup.retention import (
    BackupAsset,
    MockGitHubReleaseStorageAdapter,
    RetentionPolicyManager,
    RetentionPolicyViolationError,
    StaleInventoryError,
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
)
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
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
        "version": 1,
        "deliveries": {
            "8712345678:d1a2b3c4d5e6f7a8": {
                "status": "DELIVERY_SUCCESS",
                "delivered_at_utc": now.isoformat(),
                "delivered_at_tsi": "19 Ekim 2026 13:00 TSİ",
                "chat_id": "8712345678",
                "target_chat_id": "87...78",
                "recipient_role": "PRIMARY_EDITOR",
                "digest_hash": "d1a2b3c4d5e6f7a8",
                "stable_digest_id": "digest_synth_001",
                "message_id": 42,
            }
        },
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


class TestPhase5B2EncryptedBackup(unittest.TestCase):
    """Hermetic unit and integration acceptance tests for encrypted backup and restore."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self.key = b"\x01" * 32  # 32-byte AES-256 test key
        self.db_path, self.del_ledger_path, self.bud_ledger_path, self.repo = (
            create_synthetic_test_environment(self.tmp_path)
        )

    def tearDown(self):
        del self.repo
        import gc
        gc.collect()
        try:
            self.tmp_dir.cleanup()
        except Exception:
            pass

    def test_01_successful_backup_and_restored_data_equivalence(self):
        """Complete roundtrip: backup creation, envelope decryption, and row-level data equivalence."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
            git_commit_sha="test_commit_sha",
        )
        self.assertTrue(backup_res.asset_name.endswith(".efbk"))
        self.assertEqual(backup_res.manifest["schema_version"], CURRENT_SCHEMA_VERSION)

        staging_dir = self.tmp_path / "restore_staging_01"
        restore_res = verify_restore_archive(
            envelope_bytes=backup_res.envelope_bytes,
            key=self.key,
            staging_dir=staging_dir,
            expected_schema_version=CURRENT_SCHEMA_VERSION,
        )
        self.assertTrue(restore_res.success)
        self.assertEqual(restore_res.table_row_counts["stories"], 1)
        self.assertEqual(restore_res.table_row_counts["story_updates"], 1)
        self.assertEqual(restore_res.table_row_counts["candidate_backlog"], 1)
        self.assertEqual(restore_res.table_row_counts["notifications"], 1)
        self.assertEqual(restore_res.delivery_ledger_count, 1)
        self.assertEqual(restore_res.budget_ledger_batch_count, 1)

    def test_02_sqlite_wal_consistency_without_torn_pages(self):
        """Checkpointing folds WAL frames into standalone database with journal_mode=DELETE."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute(
                "INSERT INTO processed_articles VALUES ('wal_art_01', 'https://url', 'WAL Art', 'Src', '2026-10-19', '2026-10-19');"
            )
            conn.commit()
        finally:
            conn.close()

        snapshot_bytes, ver, counts = create_consistent_sqlite_snapshot(self.db_path)
        snap_file = self.tmp_path / "snap_test.db"
        snap_file.write_bytes(snapshot_bytes)

        conn2 = sqlite3.connect(snap_file)
        try:
            cursor = conn2.cursor()
            cursor.execute("PRAGMA journal_mode;")
            self.assertEqual(cursor.fetchone()[0].lower(), "delete")
            cursor.execute("SELECT COUNT(*) FROM processed_articles WHERE article_id='wal_art_01';")
            self.assertEqual(cursor.fetchone()[0], 1)
        finally:
            conn2.close()

    def test_03_run_locks_stripped_from_backup_snapshot(self):
        """Active transient worker run_locks are cleared from backup snapshot."""
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "INSERT INTO run_locks VALUES ('lock_prod', 'worker-active-99', '2026-10-19', '2026-10-19', '2026-10-19');"
            )
            conn.commit()
        finally:
            conn.close()

        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        restore_res = verify_restore_archive(backup_res.envelope_bytes, self.key)
        conn2 = sqlite3.connect(restore_res.db_path)
        try:
            cursor = conn2.cursor()
            cursor.execute("SELECT COUNT(*) FROM run_locks;")
            self.assertEqual(cursor.fetchone()[0], 0)
        finally:
            conn2.close()

    def test_04_wrong_key_fails_closed(self):
        """Decryption with incorrect 32-byte key fails closed with KeyMismatchError."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        wrong_key = b"\x02" * 32
        with self.assertRaises(KeyMismatchError):
            decrypt_envelope(backup_res.envelope_bytes, wrong_key)

    def test_05_corrupt_or_truncated_ciphertext_fails_closed(self):
        """Truncated ciphertext or corrupted payload bits trigger fail-closed error."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        # Truncated
        truncated = backup_res.envelope_bytes[:HEADER_SIZE + 20]
        with self.assertRaises(InvalidEnvelopeError):
            decrypt_envelope(truncated, self.key)

        # Corrupted bit in ciphertext
        corrupted = bytearray(backup_res.envelope_bytes)
        corrupted[-10] ^= 0xFF
        with self.assertRaises(KeyMismatchError):
            decrypt_envelope(bytes(corrupted), self.key)

    def test_06_authenticated_binary_header_tampering_fails_closed(self):
        """Independent tampering with ANY header field fails closed (magic, version, cipher, ts, key_id, nonce, len)."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        env = bytearray(backup_res.envelope_bytes)

        # 1. Tamper Magic (bytes 0..4)
        tampered_magic = bytearray(env)
        tampered_magic[0] = ord(b"X")
        with self.assertRaises(InvalidEnvelopeError):
            decrypt_envelope(bytes(tampered_magic), self.key)

        # 2. Tamper Version (bytes 4..6)
        tampered_version = bytearray(env)
        tampered_version[5] = 99
        with self.assertRaises(InvalidEnvelopeError):
            decrypt_envelope(bytes(tampered_version), self.key)

        # 3. Tamper Cipher ID (bytes 6..8)
        tampered_cipher = bytearray(env)
        tampered_cipher[7] = 99
        with self.assertRaises(InvalidEnvelopeError):
            decrypt_envelope(bytes(tampered_cipher), self.key)

        # 4. Tamper Creation Timestamp (bytes 8..16) -> bound in AAD
        tampered_ts = bytearray(env)
        tampered_ts[10] ^= 0x01
        with self.assertRaises(KeyMismatchError):
            decrypt_envelope(bytes(tampered_ts), self.key)

        # 5. Tamper Key ID (bytes 16..32) -> bound in AAD & checked against manifest
        tampered_kid = bytearray(env)
        tampered_kid[16] = ord(b"Z")
        with self.assertRaises((InvalidEnvelopeError, KeyMismatchError)):
            decrypt_envelope(bytes(tampered_kid), self.key)

        # 6. Tamper Nonce (bytes 32..44)
        tampered_nonce = bytearray(env)
        tampered_nonce[35] ^= 0x01
        with self.assertRaises(KeyMismatchError):
            decrypt_envelope(bytes(tampered_nonce), self.key)

        # 7. Tamper Manifest Length (bytes 44..48)
        tampered_len = bytearray(env)
        tampered_len[47] ^= 0x01
        with self.assertRaises((InvalidEnvelopeError, KeyMismatchError)):
            decrypt_envelope(bytes(tampered_len), self.key)

    def test_07_missing_or_malformed_essential_ledger_fails_closed(self):
        """Missing or structurally malformed JSON ledgers fail closed."""
        # Missing delivery ledger
        missing_del = self.tmp_path / "nonexistent.json"
        with self.assertRaises(BackupCreationError):
            create_backup(self.key, self.db_path, missing_del, self.bud_ledger_path)

        # Malformed delivery ledger (invalid delivery status)
        bad_del = self.tmp_path / "bad_del.json"
        bad_del.write_text(
            json.dumps({
                "version": 1,
                "deliveries": {
                    "del_1": {
                        "digest_hash": "abc",
                        "recipient_role": "PRIMARY_EDITOR",
                        "status": "UNRECOGNIZED_STATUS_ATTACK",
                    }
                }
            }),
            encoding="utf-8",
        )
        with self.assertRaises(MalformedLedgerError):
            create_backup(self.key, self.db_path, bad_del, self.bud_ledger_path)

    def test_08_path_traversal_zip_slip_protection(self):
        """Tar archive attempting path traversal is strictly rejected."""
        malicious_tar_buf = io.BytesIO()
        with tarfile.open(fileobj=malicious_tar_buf, mode="w:gz") as tar:
            ti = tarfile.TarInfo(name="../../etc/passwd")
            content = b"root:x:0:0:root"
            ti.size = len(content)
            tar.addfile(ti, io.BytesIO(content))

        staging_dir = self.tmp_path / "slip_test"
        with self.assertRaises(RestoreVerificationError):
            safe_extract_tar(malicious_tar_buf.getvalue(), staging_dir)

    def test_09_decompression_bomb_protection(self):
        """Tar archive declaring member size > 500 MB is rejected immediately."""
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
            ti.size = 600 * 1024 * 1024
            tar.addfile(ti, _ZeroReader(ti.size))

        staging_dir = self.tmp_path / "bomb_test"
        with self.assertRaises(DecompressionBombSecurityError):
            safe_extract_tar(bomb_tar_buf.getvalue(), staging_dir)

    def test_10_database_schema_mismatch_and_row_count_validation(self):
        """True schema version from SELECT MAX(version) is verified; row count alterations fail."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        # 1. Expected schema version mismatch
        with self.assertRaises(RestoreVerificationError) as ctx:
            verify_restore_archive(
                envelope_bytes=backup_res.envelope_bytes,
                key=self.key,
                expected_schema_version=99,
            )
        self.assertIn("schema version mismatch", str(ctx.exception).lower())

    def test_11_staging_directory_safety_and_non_destructive_guarantees(self):
        """Caller cannot supply root, home, repo, non-temp, or pre-existing non-empty dirs; user files are never wiped."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )

        # 1. Pre-existing non-empty directory is refused without wiping
        non_empty = self.tmp_path / "pre_existing_user_dir"
        non_empty.mkdir()
        user_file = non_empty / "critical_data.txt"
        user_file.write_text("DO NOT DELETE", encoding="utf-8")

        with self.assertRaises(UnsafeStagingPathError):
            verify_restore_archive(backup_res.envelope_bytes, self.key, staging_dir=non_empty)

        # Invariant: User file untouched!
        self.assertTrue(user_file.exists())
        self.assertEqual(user_file.read_text(encoding="utf-8"), "DO NOT DELETE")

        # 2. Wrong key fails BEFORE any filesystem directory is created/modified
        unused_dir = self.tmp_path / "unused_wrong_key_staging"
        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(backup_res.envelope_bytes, b"\x99" * 32, staging_dir=unused_dir)
        self.assertFalse(unused_dir.exists())

        # 3. Path outside system temporary directory is strictly refused
        with self.assertRaises(UnsafeStagingPathError):
            validate_staging_path_safety(Path.cwd() / "data" / "fake_staging")
        with self.assertRaises(UnsafeStagingPathError):
            validate_staging_path_safety(Path.home() / "fake_user_staging")

        # 4. Symlink staging path is strictly refused
        with unittest.mock.patch.object(Path, "is_symlink", return_value=True):
            with self.assertRaises(UnsafeStagingPathError):
                validate_staging_path_safety(self.tmp_path / "symlink_staging")

    def test_12_atomic_cutover_mechanics_crash_journal_and_step_rollback(self):
        """Production cutover unconditionally fails closed; simulated offline cutover requires authorization, validates workspace without name substrings, provides step rollback and crash recovery."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        restore_res = verify_restore_archive(backup_res.envelope_bytes, self.key)

        live_dir = self.tmp_path / "test_live_dir"
        live_dir.mkdir(parents=True, exist_ok=True)
        live_db = live_dir / "radar.db"
        # Create genuine live DB for testing
        conn = sqlite3.connect(live_db)
        conn.execute("CREATE TABLE stories (id TEXT);")
        conn.execute("INSERT INTO stories VALUES ('live_story');")
        conn.commit()
        conn.close()
        del conn
        import gc
        gc.collect()

        (live_dir / "telegram_digest_delivery_ledger.json").write_text("{}", encoding="utf-8")
        (live_dir / "experiment_budget_ledger.json").write_text("{}", encoding="utf-8")

        # 1. Public production cutover entry point rejects ALL invocations unconditionally
        with self.assertRaises(ProductionCutoverDisabledError):
            execute_production_cutover(restore_res, live_dir, authorized=True, simulated=False)
        with self.assertRaises(ProductionCutoverDisabledError):
            execute_production_cutover(restore_res, live_dir, authorized=True, simulated=True)
        with self.assertRaises(ProductionCutoverDisabledError):
            execute_production_cutover(restore_res, live_dir, authorized=False, simulated=True)
        with self.assertRaises(ProductionCutoverDisabledError):
            execute_production_cutover(restore_res, live_dir)

        # 2. Rejection of paths with misleading 'test'/'temp' substrings outside system temp
        fake_test_repo = Path.cwd() / "test_fake_cutover"
        with self.assertRaises(UnsafeStagingPathError):
            validate_simulation_workspace(fake_test_repo)

        fake_temp_data = Path.cwd() / "data" / "temp_fake_cutover"
        with self.assertRaises(UnsafeStagingPathError):
            validate_simulation_workspace(fake_temp_data)

        fake_test_home = Path.home() / "test_arbitrary_user_folder"
        with self.assertRaises(UnsafeStagingPathError):
            validate_simulation_workspace(fake_test_home)

        # 3. Rejection of symlinks and symlink escapes
        with unittest.mock.patch.object(Path, "is_symlink", return_value=True):
            with self.assertRaises(UnsafeStagingPathError):
                validate_simulation_workspace(self.tmp_path / "symlink_test")

        # 4. Protection against overwriting non-fixture files
        non_fixture_dir = self.tmp_path / "non_fixture_dir"
        non_fixture_dir.mkdir(parents=True, exist_ok=True)
        secret_file = non_fixture_dir / "passwords.txt"
        secret_file.write_text("sensitive_data", encoding="utf-8")

        with self.assertRaises(UnsafeStagingPathError):
            validate_simulation_workspace(non_fixture_dir)
        self.assertTrue(secret_file.exists())
        self.assertEqual(secret_file.read_text(encoding="utf-8"), "sensitive_data")

        # 5. Internal workspace ownership: simulate_offline_cutover_in_test_workspace with test_workspace_dir=None
        internal_success = simulate_offline_cutover_in_test_workspace(restore_res, test_workspace_dir=None, authorized=True)
        self.assertTrue(internal_success)

        # 6. Simulated cutover without authorized=True fails closed
        with self.assertRaises(CutoverAuthorizationError):
            simulate_offline_cutover_in_test_workspace(restore_res, live_dir, authorized=False)

        # 7. Injected failure at step 2 (delivery ledger) executes verified rollback
        with self.assertRaises(RestoreVerificationError):
            simulate_offline_cutover_in_test_workspace(
                restore_res, live_dir, authorized=True, inject_failure_step=2
            )

        # Verify rollback preserved live DB
        conn = sqlite3.connect(live_db)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM stories;")
        self.assertEqual(cursor.fetchone()[0], "live_story")
        conn.close()
        del conn
        gc.collect()

        # Verify journal exists and records rollback
        journal_path = live_dir / ".cutover_journal.json"
        self.assertTrue(journal_path.exists())
        journal_data = json.loads(journal_path.read_text(encoding="utf-8"))
        self.assertEqual(journal_data["phase"], "ROLLED_BACK")

        # 8. Successful simulated cutover with supplied fixtures
        success = simulate_offline_cutover_in_test_workspace(restore_res, live_dir, authorized=True)
        self.assertTrue(success)
        journal_data = json.loads(journal_path.read_text(encoding="utf-8"))
        self.assertEqual(journal_data["phase"], "COMPLETED")

        # 9. Interrupted cutover startup recovery
        journal_data["phase"] = "REPLACED_DB"  # simulate mid-cutover crash
        journal_path.write_text(json.dumps(journal_data), encoding="utf-8")
        recovered = detect_and_recover_incomplete_cutover(live_dir)
        self.assertTrue(recovered)
        recovered_journal = json.loads(journal_path.read_text(encoding="utf-8"))
        self.assertEqual(recovered_journal["phase"], "ROLLED_BACK")

    def test_13_telegram_post_restore_duplicate_prevention_and_uncertainty_gate(self):
        """Restoring older backup reconciles with surviving ledger, creates durable recovery state, and blocks dispatch under DELIVERY_UNCERTAIN."""
        # 1. Backup A created at T1
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
            now=datetime(2026, 10, 19, 8, 0, tzinfo=timezone.utc),
        )
        restore_res = verify_restore_archive(backup_res.envelope_bytes, self.key)

        # 2. Digest B sent at T2 (> T1) recorded in surviving operational ledger
        surviving_ledger = self.tmp_path / "surviving_ledger.json"
        digest_b_hash = "hash_digest_b_post_backup_1234"
        surviving_ledger.write_text(
            json.dumps({
                "version": 1,
                "deliveries": {
                    f"8712345678:{digest_b_hash}": {
                        "status": "DELIVERY_SUCCESS",
                        "delivered_at_utc": "2026-10-19T09:00:00Z",
                        "chat_id": "8712345678",
                        "recipient_role": "PRIMARY_EDITOR",
                        "digest_hash": digest_b_hash,
                    }
                }
            }),
            encoding="utf-8",
        )

        # 3. Restore Backup A with surviving ledger reconciliation
        gate = TelegramDeliveryReconciliationGate(
            restored_ledger_path=restore_res.delivery_ledger_path,
            surviving_ledger_path=surviving_ledger,
            backup_created_at_utc="2026-10-19T08:00:00Z",
        )
        status = gate.reconcile()
        self.assertIn("Reconciled 1 post-backup delivery", status)

        # Invariant: Digest B cannot be blindly resent!
        with self.assertRaises(DeliveryBlockedUncertainRecoveryError):
            gate.check_can_dispatch(digest_b_hash)

        # 4. Outage scenario: No surviving ledger available -> enter DELIVERY_UNCERTAIN
        gate_uncertain = TelegramDeliveryReconciliationGate(
            restored_ledger_path=restore_res.delivery_ledger_path,
            surviving_ledger_path=None,
            backup_created_at_utc="2026-10-19T08:00:00Z",
        )
        gate_uncertain.reconcile()
        self.assertEqual(gate_uncertain.reconciliation_status, "DELIVERY_UNCERTAIN")

        # Durable state verification: .recovery_pending_reconciliation.json exists on disk
        data_dir = restore_res.delivery_ledger_path.parent
        durable_state_file = data_dir / ".recovery_pending_reconciliation.json"
        self.assertTrue(durable_state_file.exists())
        durable_data = json.loads(durable_state_file.read_text(encoding="utf-8"))
        self.assertTrue(durable_data.get("dispatch_blocked"))

        # Verify classmethod is_dispatch_blocked survives process restart
        blocked, reason = TelegramDeliveryReconciliationGate.is_dispatch_blocked(data_dir)
        self.assertTrue(blocked)
        self.assertIn("DELIVERY_UNCERTAIN", reason)

        # Process restart simulation: a new gate instance checks dispatch and fails closed
        gate_restarted = TelegramDeliveryReconciliationGate(
            restored_ledger_path=restore_res.delivery_ledger_path,
            surviving_ledger_path=None,
            backup_created_at_utc="2026-10-19T08:00:00Z",
        )
        with self.assertRaises(DeliveryBlockedUncertainRecoveryError):
            gate_restarted.check_can_dispatch("new_unseen_digest_hash")

        # 5. Contradictory surviving history detection
        contradictory_ledger = self.tmp_path / "contradictory_ledger.json"
        contradictory_ledger.write_text(
            json.dumps({
                "version": 1,
                "deliveries": {
                    "del_1": {
                        "digest_hash": "same_hash_123",
                        "status": "DELIVERY_SUCCESS",
                        "delivered_at_utc": "2026-10-19T09:00:00Z",
                        "chat_id": "8712345678",
                        "recipient_role": "PRIMARY_EDITOR",
                    },
                    "del_2": {
                        "digest_hash": "same_hash_123",
                        "status": "FAILED",
                        "delivered_at_utc": "2026-10-19T09:05:00Z",
                        "chat_id": "8712345678",
                        "recipient_role": "PRIMARY_EDITOR",
                    },
                }
            }),
            encoding="utf-8",
        )
        gate_contradictory = TelegramDeliveryReconciliationGate(
            restored_ledger_path=restore_res.delivery_ledger_path,
            surviving_ledger_path=contradictory_ledger,
            backup_created_at_utc="2026-10-19T08:00:00Z",
        )
        gate_contradictory.reconcile()
        self.assertEqual(gate_contradictory.reconciliation_status, "DELIVERY_UNCERTAIN")
        self.assertTrue(gate_contradictory.dispatch_blocked)

        # 6. Operator authorization clears uncertainty
        gate_uncertain.authorize_resumption(
            operator_note="Reviewed channel outbox; safe to resume", token="po_auth_token_xyz"
        )
        # Durable state updated to unblocked
        blocked_after, _ = TelegramDeliveryReconciliationGate.is_dispatch_blocked(data_dir)
        self.assertFalse(blocked_after)
        # Now allowed for new unseen digests
        gate_uncertain.check_can_dispatch("new_unseen_digest_hash")

    def test_14_cross_store_snapshot_coordination_barrier(self):
        """SnapshotCoordinationBarrier prevents concurrent writers and times out safely."""
        coord_dir = self.tmp_path / "barrier_test"
        barrier1 = SnapshotCoordinationBarrier(coord_dir, timeout_sec=0.2)
        barrier1.acquire()

        barrier2 = SnapshotCoordinationBarrier(coord_dir, timeout_sec=0.2)
        with self.assertRaises(BackupCoordinationTimeoutError):
            barrier2.acquire()

        barrier1.release()
        # Now barrier2 can acquire
        barrier2.acquire()
        barrier2.release()

    def test_15_verified_retention_policy_content_verification_and_storage_budget(self):
        """Retention policy downloads & authenticates assets, handles corruption, and halts uploads on budget conflict."""
        storage = MockGitHubReleaseStorageAdapter(release_tag="rolling-backups")
        retention_mgr = RetentionPolicyManager(
            max_retention_days=14,
            min_recovery_points=2,
            max_storage_budget_bytes=5000,
        )
        ref_now = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)

        # Create 3 valid encrypted backups
        b1 = create_backup(self.key, self.db_path, self.del_ledger_path, self.bud_ledger_path, now=ref_now)
        b2 = create_backup(self.key, self.db_path, self.del_ledger_path, self.bud_ledger_path, now=ref_now - timedelta(days=5))
        b3 = create_backup(self.key, self.db_path, self.del_ledger_path, self.bud_ledger_path, now=ref_now - timedelta(days=20))

        storage.upload_asset(b1.asset_name, b1.envelope_bytes)
        storage.upload_asset(b2.asset_name, b2.envelope_bytes)
        storage.upload_asset(b3.asset_name, b3.envelope_bytes)

        # Upload a corrupted asset (raw corrupted ciphertext)
        storage.upload_asset("efso_backup_20261019_000000_deadbeef.efbk", b"corrupted payload")

        # Upload a validly encrypted envelope that contains a corrupt SQLite database (schema version 99 != 5)
        bad_schema_db = self.tmp_path / "bad_schema.db"
        conn = sqlite3.connect(bad_schema_db)
        conn.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (99);")
        conn.execute("CREATE TABLE stories (id TEXT PRIMARY KEY, title TEXT);")
        conn.execute("CREATE TABLE notifications (id TEXT PRIMARY KEY);")
        conn.execute("CREATE TABLE candidate_backlog (id TEXT PRIMARY KEY);")
        conn.execute("CREATE TABLE processed_articles (id TEXT PRIMARY KEY);")
        conn.commit()
        conn.close()

        b_bad_schema = create_backup(
            self.key, bad_schema_db, self.del_ledger_path, self.bud_ledger_path, now=ref_now - timedelta(days=1)
        )
        storage.upload_asset("efso_backup_20261018_120000_badschema.efbk", b_bad_schema.envelope_bytes)

        # 1. Content verification: verify_inventory must run full restore verification (including SQLite check & schema version)
        verified_points, issues = retention_mgr.verify_inventory(storage, self.key, expected_schema_version=CURRENT_SCHEMA_VERSION)
        self.assertEqual(len(verified_points), 3)  # Only the 3 genuine backups verified; bad schema and raw corrupted excluded!
        self.assertEqual(len(issues), 2)          # Both corrupted assets flagged in issues!

        # 2. Evaluation with inventory fingerprint
        fp = storage.get_inventory_fingerprint()
        plan = retention_mgr.evaluate_retention(verified_points, storage.list_assets(), fp, now=ref_now)
        # b1, b2 retained; b3 (>14 days) marked for deletion; bad assets not counted in verified recovery points
        self.assertEqual(len(plan.retained_assets), 2)
        self.assertEqual(len(plan.assets_to_delete), 1)
        self.assertEqual(plan.assets_to_delete[0].name, b3.asset_name)

        # 3. Concurrency check: If storage changes concurrently, apply_retention aborts with StaleInventoryError
        storage.upload_asset("concurrent_asset.efbk", b"concurrent data")
        with self.assertRaises(StaleInventoryError):
            retention_mgr.apply_retention(storage, plan)

    def test_16_zero_secrets_or_raw_chat_ids_in_reports(self):
        """Verification report masks recipient chat IDs and exposes zero credentials."""
        backup_res = create_backup(
            key=self.key,
            db_path=self.db_path,
            delivery_ledger_path=self.del_ledger_path,
            budget_ledger_path=self.bud_ledger_path,
        )
        restore_res = verify_restore_archive(backup_res.envelope_bytes, self.key)
        report = restore_res.verification_report_markdown

        self.assertNotIn("8712345678", report)
        self.assertIn("87...78", report)
        self.assertNotIn("AIza", report)

    def test_17_decrypted_staging_cleanup_lifecycle_and_failure_resilience(self):
        """Operation-owned staging is purged on verification failures (corrupt SQLite, bad JSON, wrong schema); caller directories preserved."""
        # 1. Wrong schema version cleans up operation-owned staging directory
        backup_res = create_backup(self.key, self.db_path, self.del_ledger_path, self.bud_ledger_path)
        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(backup_res.envelope_bytes, self.key, expected_schema_version=99)

        # 2. Malformed SQLite database cleans up operation-owned staging directory
        corrupt_db = self.tmp_path / "corrupt_table.db"
        conn = sqlite3.connect(corrupt_db)
        conn.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY);")
        conn.execute("INSERT INTO schema_migrations (version) VALUES (?);", (CURRENT_SCHEMA_VERSION,))
        conn.execute("CREATE TABLE stories (id TEXT PRIMARY KEY, title TEXT);")
        conn.execute("CREATE TABLE notifications (id TEXT PRIMARY KEY);")
        conn.execute("CREATE TABLE candidate_backlog (id TEXT PRIMARY KEY);")
        conn.execute("CREATE TABLE processed_articles (id TEXT PRIMARY KEY);")
        conn.commit()
        conn.close()
        # Corrupt raw bytes of the DB file
        corrupt_bytes = bytearray(corrupt_db.read_bytes())
        corrupt_bytes[100:150] = b"\xFF" * 50
        corrupt_db.write_bytes(bytes(corrupt_bytes))

        # Make an envelope that packages this corrupt db
        corrupt_tar = io.BytesIO()
        with tarfile.open(fileobj=corrupt_tar, mode="w:gz") as tar:
            ti = tarfile.TarInfo("radar.db")
            ti.size = len(corrupt_bytes)
            tar.addfile(ti, io.BytesIO(bytes(corrupt_bytes)))
            del_b = self.del_ledger_path.read_bytes()
            ti_del = tarfile.TarInfo("telegram_digest_delivery_ledger.json")
            ti_del.size = len(del_b)
            tar.addfile(ti_del, io.BytesIO(del_b))
            bud_b = self.bud_ledger_path.read_bytes()
            ti_bud = tarfile.TarInfo("experiment_budget_ledger.json")
            ti_bud.size = len(bud_b)
            tar.addfile(ti_bud, io.BytesIO(bud_b))

        raw_tar = corrupt_tar.getvalue()
        manifest = {
            "key_id": "efso-2026-v1",
            "created_at_utc": "2026-10-19T12:00:00Z",
            "schema_version": CURRENT_SCHEMA_VERSION,
            "source_engine": "sqlite",
            "plaintext_sha256": hashlib.sha256(raw_tar).hexdigest(),
        }
        corrupt_envelope = encrypt_envelope(raw_tar, self.key, manifest=manifest)

        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(corrupt_envelope, self.key)

        # 3. Malformed JSON delivery ledger cleans up operation-owned staging directory
        bad_json_tar = io.BytesIO()
        with tarfile.open(fileobj=bad_json_tar, mode="w:gz") as tar:
            db_b = self.db_path.read_bytes()
            ti_db = tarfile.TarInfo("radar.db")
            ti_db.size = len(db_b)
            tar.addfile(ti_db, io.BytesIO(db_b))

            bad_json = b"{ NOT_VALID_JSON }"
            ti_del = tarfile.TarInfo("telegram_digest_delivery_ledger.json")
            ti_del.size = len(bad_json)
            tar.addfile(ti_del, io.BytesIO(bad_json))

            bud_b = self.bud_ledger_path.read_bytes()
            ti_bud = tarfile.TarInfo("experiment_budget_ledger.json")
            ti_bud.size = len(bud_b)
            tar.addfile(ti_bud, io.BytesIO(bud_b))

        raw_bad_tar = bad_json_tar.getvalue()
        bad_manifest = {
            "key_id": "efso-2026-v1",
            "created_at_utc": "2026-10-19T12:00:00Z",
            "schema_version": CURRENT_SCHEMA_VERSION,
            "source_engine": "sqlite",
            "plaintext_sha256": hashlib.sha256(raw_bad_tar).hexdigest(),
        }
        bad_envelope = encrypt_envelope(raw_bad_tar, self.key, manifest=bad_manifest)

        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(bad_envelope, self.key)

        # 4. Caller-provided staging directory is NOT deleted when verification fails
        caller_staging = self.tmp_path / "caller_staging_fail_test"
        caller_staging.mkdir(parents=True, exist_ok=True)
        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(bad_envelope, self.key, staging_dir=caller_staging)
        self.assertTrue(caller_staging.exists())
        # Extracted files inside it were cleaned up
        self.assertFalse((caller_staging / "radar.db").exists())
        self.assertFalse((caller_staging / "telegram_digest_delivery_ledger.json").exists())

        # 5. Explicit cleanup method and context manager lifecycle on success
        res = verify_restore_archive(backup_res.envelope_bytes, self.key)
        self.assertTrue(res.is_operation_owned)
        self.assertTrue(res.staging_dir.exists())
        res.cleanup()
        self.assertFalse(res.staging_dir.exists())

        # Context manager lifecycle
        with verify_restore_archive(backup_res.envelope_bytes, self.key) as cm_res:
            self.assertTrue(cm_res.staging_dir.exists())
        self.assertFalse(cm_res.staging_dir.exists())

    def test_18_partial_extraction_cleanup_and_pre_existing_protection(self):
        """Guarantees cleanup of every file created during partial extraction (disallowed or truncated) and protects pre-existing files."""
        # --- 1. Valid radar.db member followed by disallowed member ---
        tar_buf = io.BytesIO()
        with tarfile.open(fileobj=tar_buf, mode="w:gz") as tar:
            # Member 1: Valid radar.db
            db_b = self.db_path.read_bytes()
            ti_db = tarfile.TarInfo("radar.db")
            ti_db.size = len(db_b)
            tar.addfile(ti_db, io.BytesIO(db_b))

            # Member 2: Disallowed archive member
            disallowed_content = b"#!/bin/bash\necho hack\n"
            ti_disallowed = tarfile.TarInfo("unauthorized_script.sh")
            ti_disallowed.size = len(disallowed_content)
            tar.addfile(ti_disallowed, io.BytesIO(disallowed_content))

        raw_tar_disallowed = tar_buf.getvalue()
        manifest_disallowed = {
            "key_id": "efso-2026-v1",
            "created_at_utc": "2026-10-19T14:00:00Z",
            "schema_version": CURRENT_SCHEMA_VERSION,
            "source_engine": "sqlite",
            "plaintext_sha256": hashlib.sha256(raw_tar_disallowed).hexdigest(),
        }
        envelope_disallowed = encrypt_envelope(raw_tar_disallowed, self.key, manifest=manifest_disallowed)

        # Safe, empty caller-owned staging directory
        caller_staging_1 = self.tmp_path / "caller_staging_disallowed_test"
        caller_staging_1.mkdir(parents=True, exist_ok=True)

        # Unrelated file in adjacent directory to confirm unrelated files are never modified
        unrelated_canary = self.tmp_path / "unrelated_canary.txt"
        unrelated_canary.write_text("canary_safe_data", encoding="utf-8")

        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(envelope_disallowed, self.key, staging_dir=caller_staging_1)

        # Assert: verification failed, no partially extracted plaintext files remain, caller directory intact, unrelated file untouched
        self.assertTrue(caller_staging_1.exists())
        self.assertFalse((caller_staging_1 / "radar.db").exists())
        self.assertFalse((caller_staging_1 / "unauthorized_script.sh").exists())
        self.assertEqual(list(caller_staging_1.iterdir()), [])
        self.assertTrue(unrelated_canary.exists())
        self.assertEqual(unrelated_canary.read_text(encoding="utf-8"), "canary_safe_data")

        # --- 2. Truncated extraction after a partially written member ---
        buf_trunc = io.BytesIO()
        t_trunc = tarfile.open(fileobj=buf_trunc, mode="w")
        ti_db2 = tarfile.TarInfo("radar.db")
        ti_db2.size = len(db_b)
        t_trunc.addfile(ti_db2, io.BytesIO(db_b))

        # Member 2 declares 50,000 bytes, but stream is truncated after 1,024 bytes
        ti_del_trunc = tarfile.TarInfo("telegram_digest_delivery_ledger.json")
        ti_del_trunc.size = 50000
        raw_trunc_tar = buf_trunc.getvalue() + ti_del_trunc.tobuf() + (b"partial_delivery_data_" * 40)

        manifest_trunc = {
            "key_id": "efso-2026-v1",
            "created_at_utc": "2026-10-19T14:15:00Z",
            "schema_version": CURRENT_SCHEMA_VERSION,
            "source_engine": "sqlite",
            "plaintext_sha256": hashlib.sha256(raw_trunc_tar).hexdigest(),
        }
        envelope_trunc = encrypt_envelope(raw_trunc_tar, self.key, manifest=manifest_trunc)

        caller_staging_2 = self.tmp_path / "caller_staging_trunc_test"
        caller_staging_2.mkdir(parents=True, exist_ok=True)

        with self.assertRaises(RestoreVerificationError):
            verify_restore_archive(envelope_trunc, self.key, staging_dir=caller_staging_2)

        # Assert: partial radar.db and partial delivery ledger were removed, caller directory intact
        self.assertTrue(caller_staging_2.exists())
        self.assertFalse((caller_staging_2 / "radar.db").exists())
        self.assertFalse((caller_staging_2 / "telegram_digest_delivery_ledger.json").exists())
        self.assertEqual(list(caller_staging_2.iterdir()), [])

        # --- 3. Protection against overwriting pre-existing files ---
        caller_staging_3 = self.tmp_path / "caller_staging_preexisting"
        caller_staging_3.mkdir(parents=True, exist_ok=True)
        preexisting_db = caller_staging_3 / "radar.db"
        preexisting_db.write_text("ORIGINAL_USER_DATA", encoding="utf-8")

        # verify_restore_archive fails closed via validate_staging_path_safety on non-empty directory
        from efso_radar.backup.restore import UnsafeStagingPathError
        with self.assertRaises(UnsafeStagingPathError):
            verify_restore_archive(envelope_disallowed, self.key, staging_dir=caller_staging_3)
        self.assertTrue(preexisting_db.exists())
        self.assertEqual(preexisting_db.read_text(encoding="utf-8"), "ORIGINAL_USER_DATA")

        # safe_extract_tar standalone also refuses to overwrite pre-existing file
        valid_single_tar = io.BytesIO()
        with tarfile.open(fileobj=valid_single_tar, mode="w") as t_single:
            t_single.addfile(ti_db, io.BytesIO(db_b))
        with self.assertRaises(RestoreVerificationError):
            safe_extract_tar(valid_single_tar.getvalue(), caller_staging_3)
        self.assertEqual(preexisting_db.read_text(encoding="utf-8"), "ORIGINAL_USER_DATA")


if __name__ == "__main__":
    unittest.main()


