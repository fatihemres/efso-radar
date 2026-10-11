"""Tests for durable notification claims, lease management, recovery, and quiet-hours staleness pruning.

Verifies mutual exclusion during dispatch, recovery of orphaned in-flight claims,
and pruning of stale overnight notifications across both SQLite and Turso repositories.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from efso_radar.domain import DeliveryStatus, EditorialKind, NotificationRecord, utc_now
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from tests.test_repository_parity import create_mock_hrana_transport


class NotificationClaimsContractMixin:
    """Contract tests for notification claiming and reliability."""

    now: datetime
    repo: Any

    def test_claim_lease_mutual_exclusion(self):
        notif_id = "NOTIF-TEST-1"
        story_id = "EFSO-STORY-1"

        # Worker 1 successfully claims notification
        claimed_w1 = self.repo.claim_notification_for_dispatch(
            notification_id=notif_id,
            story_id=story_id,
            kind=EditorialKind.NEW,
            headline_tr="Büyük Haber Başlığı",
            score=95,
            message_text="Alert text",
            worker_id="worker-alpha",
            lease_seconds=300,
        )
        self.assertTrue(claimed_w1)

        # Worker 2 attempts to claim while Worker 1 lease is active -> denied
        claimed_w2 = self.repo.claim_notification_for_dispatch(
            notification_id=notif_id,
            story_id=story_id,
            kind=EditorialKind.NEW,
            headline_tr="Büyük Haber Başlığı",
            score=95,
            message_text="Alert text",
            worker_id="worker-beta",
            lease_seconds=300,
        )
        self.assertFalse(claimed_w2)

        # Worker 1 confirms successful delivery
        self.repo.mark_notification_delivered(notif_id, self.now)

        # Worker 2 cannot claim an already SENT notification
        claimed_w2_again = self.repo.claim_notification_for_dispatch(
            notification_id=notif_id,
            story_id=story_id,
            kind=EditorialKind.NEW,
            headline_tr="Büyük Haber Başlığı",
            score=95,
            message_text="Alert text",
            worker_id="worker-beta",
            lease_seconds=300,
        )
        self.assertFalse(claimed_w2_again)

    def test_record_notification_failure(self):
        notif_id = "NOTIF-TEST-FAIL"
        story_id = "EFSO-STORY-FAIL"

        self.repo.claim_notification_for_dispatch(
            notification_id=notif_id,
            story_id=story_id,
            kind=EditorialKind.NEW,
            headline_tr="Hata Test Başlığı",
            score=88,
            message_text="Alert text",
            worker_id="worker-1",
            lease_seconds=120,
        )

        self.repo.record_notification_failure(notif_id, "Telegram HTTP 429 Too Many Requests")

        # Notification can now be re-claimed after failure if appropriate
        claimed_retry = self.repo.claim_notification_for_dispatch(
            notification_id=notif_id,
            story_id=story_id,
            kind=EditorialKind.NEW,
            headline_tr="Hata Test Başlığı",
            score=88,
            message_text="Alert text",
            worker_id="worker-2",
            lease_seconds=120,
        )
        self.assertTrue(claimed_retry)

    def test_recover_orphaned_dispatch_claims(self):
        notif_id = "NOTIF-TEST-ORPHAN"
        story_id = "EFSO-STORY-ORPHAN"

        # Claim with very short lease (simulating lease in the past)
        # We manually record with an expired lease
        expired_time = (utc_now() - timedelta(minutes=10)).isoformat()
        if hasattr(self.repo, "_connection"):
            with self.repo._connection() as conn:
                conn.execute(
                    """
                    INSERT INTO notifications
                    (notification_id, story_id, kind, channel, sent_at, status,
                     message_text, headline_tr, score, lease_worker_id, lease_expires_at)
                    VALUES (?, ?, 'NEW', 'TELEGRAM', ?, 'DISPATCHING', 'Msg', 'Title', 90, 'crashed-worker', ?)
                    """,
                    (notif_id, story_id, utc_now().isoformat(), expired_time),
                )
        else:
            self.repo.client.execute(
                """
                INSERT INTO notifications
                (notification_id, story_id, kind, channel, sent_at, status,
                 message_text, headline_tr, score, lease_worker_id, lease_expires_at)
                VALUES (?, ?, 'NEW', 'TELEGRAM', ?, 'DISPATCHING', 'Msg', 'Title', 90, 'crashed-worker', ?)
                """,
                [notif_id, story_id, utc_now().isoformat(), expired_time],
            )

        recovered = self.repo.recover_expired_dispatch_claims()
        self.assertEqual(recovered, 1)

    def test_stale_quiet_hours_notifications_expire(self):
        old_time = utc_now() - timedelta(hours=10)
        fresh_time = utc_now() - timedelta(hours=2)

        stale_record = NotificationRecord(
            notification_id="NOTIF-STALE",
            story_id="STORY-STALE",
            kind=EditorialKind.NEW,
            sent_at=old_time,
            status=DeliveryStatus.HELD_QUIET_HOURS,
            message_text="Eski magazin haberi",
            headline_tr="10 saat önceki haber",
            score=85,
        )
        fresh_record = NotificationRecord(
            notification_id="NOTIF-FRESH",
            story_id="STORY-FRESH",
            kind=EditorialKind.NEW,
            sent_at=fresh_time,
            status=DeliveryStatus.HELD_QUIET_HOURS,
            message_text="Taze magazin haberi",
            headline_tr="2 saat önceki haber",
            score=91,
        )

        self.repo.record_notification(stale_record)
        self.repo.record_notification(fresh_record)

        held_before = self.repo.get_held_notifications()
        self.assertEqual(len(held_before), 2)

        # Expire notifications older than 8 hours
        expired_count = self.repo.expire_stale_held_notifications(max_age_hours=8)
        self.assertEqual(expired_count, 1)

        held_after = self.repo.get_held_notifications()
        self.assertEqual(len(held_after), 1)
        self.assertEqual(held_after[0].notification_id, "NOTIF-FRESH")


class TestSqliteNotificationClaims(unittest.TestCase, NotificationClaimsContractMixin):
    def setUp(self):
        self.now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = SqliteStateRepository(Path(self.tmp.name))

    def tearDown(self):
        try:
            Path(self.tmp.name).unlink(missing_ok=True)
        except Exception:
            pass


class TestTursoNotificationClaims(unittest.TestCase, NotificationClaimsContractMixin):
    def setUp(self):
        self.now = datetime(2026, 10, 11, 10, 0, tzinfo=timezone.utc)
        self.sqlite_conn = sqlite3.connect(":memory:")
        self.mock_transport = create_mock_hrana_transport(self.sqlite_conn)
        self.repo = TursoStateRepository(
            database_url="libsql://efso-test.turso.io",
            auth_token="mock-token-xyz",
            transport=self.mock_transport,
        )

    def tearDown(self):
        try:
            self.sqlite_conn.close()
        except Exception:
            pass
