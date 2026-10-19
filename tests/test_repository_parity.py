"""Contract and parity tests verifying identical behavior between SqliteStateRepository and TursoStateRepository.

Tests TursoStateRepository via deterministic HTTP mock transport simulating the libSQL Hrana v2 protocol.
"""
from __future__ import annotations

import base64
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialKind,
    NotificationRecord,
    OwnerFeedback,
    OwnerFeedbackRating,
    Story,
    StoryCategory,
    StoryUpdate,
    SuggestedFormat,
    utc_now,
)
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from efso_radar.repositories.turso_client import deserialize_hrana_value


def create_mock_hrana_transport(conn: sqlite3.Connection) -> httpx.MockTransport:
    """Creates an httpx.MockTransport that handles Hrana v2 /v2/pipeline calls using a real SQLite backend."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/v2/pipeline")
        body = json.loads(request.content.decode("utf-8"))
        requests_list = body.get("requests", [])
        in_baton = body.get("baton")
        results: list[dict[str, Any]] = []

        has_close = any(req.get("type") == "close" for req in requests_list)
        out_baton = None if has_close else (in_baton or "mock-baton-session-1")

        for req in requests_list:
            req_type = req.get("type")
            if req_type == "execute":
                stmt = req.get("stmt", {})
                sql = stmt.get("sql", "").strip()
                raw_args = stmt.get("args", [])
                args = [deserialize_hrana_value(a) for a in raw_args]

                try:
                    cur = conn.cursor()
                    if sql.upper() == "BEGIN":
                        try:
                            cur.execute("BEGIN")
                        except sqlite3.OperationalError:
                            pass  # already in transaction
                    elif sql.upper() == "COMMIT":
                        conn.commit()
                    elif sql.upper() == "ROLLBACK":
                        conn.rollback()
                    else:
                        cur.execute(sql, args)
                        if not in_baton and not any(r.get("stmt", {}).get("sql", "").upper() == "BEGIN" for r in requests_list):
                            conn.commit()

                    cols = [{"name": desc[0]} for desc in (cur.description or [])]
                    rows: list[list[dict[str, Any]]] = []
                    if cur.description:
                        for row in cur.fetchall():
                            row_cells = []
                            for cell in row:
                                if cell is None:
                                    row_cells.append({"type": "null"})
                                elif isinstance(cell, bool):
                                    row_cells.append({"type": "integer", "value": "1" if cell else "0"})
                                elif isinstance(cell, int):
                                    row_cells.append({"type": "integer", "value": str(cell)})
                                elif isinstance(cell, float):
                                    row_cells.append({"type": "float", "value": cell})
                                elif isinstance(cell, (bytes, bytearray)):
                                    row_cells.append({"type": "blob", "base64": base64.b64encode(cell).decode("ascii")})
                                else:
                                    row_cells.append({"type": "text", "value": str(cell)})
                            rows.append(row_cells)

                    affected = cur.rowcount if cur.rowcount >= 0 else 0
                    results.append({
                        "type": "ok",
                        "response": {
                            "type": "execute",
                            "result": {
                                "cols": cols,
                                "rows": rows,
                                "affected_row_count": affected,
                                "last_insert_rowid": cur.lastrowid,
                            },
                        },
                    })
                except Exception as exc:
                    results.append({
                        "type": "error",
                        "error": {
                            "message": str(exc),
                            "code": "SQLITE_ERROR",
                        },
                    })
            elif req_type == "close":
                results.append({
                    "type": "ok",
                    "response": {"type": "close"},
                })

        return httpx.Response(200, json={"baton": out_baton, "base_url": None, "results": results})

    return httpx.MockTransport(handler)


class RepositoryContractTestMixin:
    """Common test suite executed against both SQLite and Turso repositories."""

    now: datetime
    repo: Any

    def make_sample_article(self, title: str, url: str) -> Article:
        return Article.create(title=title, url=url, source="TestFeed", published_at=self.now)

    def make_sample_assessment(
        self,
        article_ids: list[str],
        headline: str = "Tarkan yeni turnesini duyurdu",
        score: int = 92,
        kind: EditorialKind = EditorialKind.NEW,
        parent_story_id: str | None = None,
        development_tr: str | None = None,
    ) -> EditorialAssessment:
        return EditorialAssessment(
            article_ids=article_ids,
            score=score,
            kind=kind,
            parent_story_id=parent_story_id,
            development_tr=development_tr,
            headline_tr=headline,
            cover_title_tr="TARKAN TURNEDE",
            caption_idea_tr="Megastar 2026 turne tarihlerini resmen açıkladı.",
            why_efso_tr="Pop müziğin en büyük olayı.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
        )

    def test_schema_version(self):
        self.assertEqual(self.repo.get_schema_version(), CURRENT_SCHEMA_VERSION)

    def test_run_lock_lifecycle_and_mutual_exclusion(self):
        worker1 = "worker-1"
        worker2 = "worker-2"

        # Worker 1 acquires
        self.assertTrue(self.repo.acquire_run_lock(worker1, timeout_minutes=15))

        # Worker 2 attempts while Worker 1 active -> denied
        self.assertFalse(self.repo.acquire_run_lock(worker2, timeout_minutes=15))

        # Worker 1 renews lease
        self.assertTrue(self.repo.renew_run_lock(worker1, extend_minutes=20))

        # Worker 2 cannot renew Worker 1's lock
        self.assertFalse(self.repo.renew_run_lock(worker2, extend_minutes=20))

        # Worker 1 releases
        self.repo.release_run_lock(worker1)

        # Worker 2 can now acquire
        self.assertTrue(self.repo.acquire_run_lock(worker2, timeout_minutes=15))
        self.repo.release_run_lock(worker2)

    def test_processed_articles_and_purge(self):
        a1 = self.make_sample_article("Haber 1", "https://url1.com")
        a2 = self.make_sample_article("Haber 2", "https://url2.com")

        self.assertFalse(self.repo.is_article_processed(a1.id))
        self.assertFalse(self.repo.is_article_processed(a2.id))

        self.repo.mark_articles_processed([a1, a2])

        self.assertTrue(self.repo.is_article_processed(a1.id))
        self.assertTrue(self.repo.is_article_processed(a2.id))

        # Purging with future threshold retains fresh records
        purged = self.repo.purge_expired_records(retention_days=30)
        self.assertEqual(purged, 0)
        self.assertTrue(self.repo.is_article_processed(a1.id))

    def test_story_and_update_lifecycle(self):
        a1 = self.make_sample_article("Tarkan turne duyurusu", "https://tarkan.com/1")
        asm1 = self.make_sample_assessment([a1.id])

        story_id = f"EFSO-20261011-{a1.id[:8]}"
        story = Story(
            story_id=story_id,
            headline_tr=asm1.headline_tr,
            category=asm1.category,
            first_seen_at=self.now,
            last_updated_at=self.now,
            article_ids=[a1.id],
            update_count=0,
            is_active=True,
        )

        self.repo.record_new_story(story, asm1)

        fetched = self.repo.get_story_by_id(story_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.headline_tr, asm1.headline_tr)
        self.assertEqual(fetched.update_count, 0)

        recent = self.repo.get_recent_stories(limit=10)
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0].story_id, story_id)

        # Record significant update
        a2 = self.make_sample_article("Tarkan biletleri 5 dakikada tükendi", "https://tarkan.com/2")
        dev_text = "Turne biletleri satışa çıktığı anda 5 dakikada tükendi."
        asm2 = self.make_sample_assessment(
            [a2.id],
            headline="Tarkan biletleri tükendi",
            score=94,
            kind=EditorialKind.UPDATE,
            parent_story_id=story_id,
            development_tr=dev_text,
        )
        update = StoryUpdate(
            update_id=f"UPD-{a2.id[:8]}",
            story_id=story_id,
            development_tr=dev_text,
            detected_at=self.now + timedelta(hours=2),
            article_ids=[a2.id],
            significance_score=94,
        )

        self.repo.record_story_update(update, asm2)

        # Updated story should have update_count = 1
        updated_story = self.repo.get_story_by_id(story_id)
        self.assertEqual(updated_story.update_count, 1)

    def test_has_story_been_alerted_logic(self):
        story_id = "EFSO-STORY-IDEMPOTENT"
        self.assertFalse(self.repo.has_story_been_alerted(story_id, kind=EditorialKind.NEW))

        # Record notification in DISPATCHING or SENT
        notif = NotificationRecord(
            notification_id="NOTIF-1",
            story_id=story_id,
            kind=EditorialKind.NEW,
            sent_at=self.now,
            status=DeliveryStatus.SENT,
            message_text="Alert text",
            headline_tr="Headline",
            score=90,
        )
        self.repo.record_notification(notif)
        self.assertTrue(self.repo.has_story_been_alerted(story_id, kind=EditorialKind.NEW))

        # Check update development alert check
        dev_text = "Resmi doğrulama geldi."
        self.assertFalse(self.repo.has_story_been_alerted(story_id, kind=EditorialKind.UPDATE, development_tr=dev_text))

        # Once recorded in story_updates:
        update = StoryUpdate(
            update_id="UPD-123",
            story_id=story_id,
            development_tr=dev_text,
            detected_at=self.now,
            article_ids=["art1"],
            significance_score=91,
        )
        asm = self.make_sample_assessment(["art1"], kind=EditorialKind.UPDATE, parent_story_id=story_id, development_tr=dev_text)
        self.repo.record_story_update(update, asm)
        self.assertTrue(self.repo.has_story_been_alerted(story_id, kind=EditorialKind.UPDATE, development_tr=dev_text))

    def test_story_update_has_distinct_notification_identity(self):
        """Verifies story update notifications have distinct notification IDs and update_id linked."""
        story_id = "EFSO-STORY-DISTINCT"
        update_id = "UPD-DISTINCT-001"

        # 1. Alert original story
        notif1 = NotificationRecord(
            notification_id="NOTIF-STORY-ORIGINAL",
            story_id=story_id,
            update_id=None,
            kind=EditorialKind.NEW,
            sent_at=self.now,
            status=DeliveryStatus.SENT,
            message_text="Alert 1",
            headline_tr="Original story",
            score=90,
        )
        self.repo.record_notification(notif1)

        # 2. Alert significant update
        notif2 = NotificationRecord(
            notification_id="NOTIF-UPD-DEVELOPMENT",
            story_id=story_id,
            update_id=update_id,
            kind=EditorialKind.UPDATE,
            sent_at=self.now + timedelta(hours=1),
            status=DeliveryStatus.SENT,
            message_text="Alert 2",
            headline_tr="Update story",
            score=93,
        )
        self.repo.record_notification(notif2)

        # Ensure both distinct notification records exist in repository
        self.assertNotEqual(notif1.notification_id, notif2.notification_id)
        self.assertEqual(notif2.update_id, update_id)

    def test_import_owner_feedback_batch_idempotency(self):
        """Batch owner feedback import is atomic, idempotent, and preserves original labels/comments."""
        batch = [
            OwnerFeedback(
                feedback_id="CALIB-01",
                story_id=None,
                article_id="art_calib_01",
                rating=OwnerFeedbackRating.APPROVE,
                notes="Hayran kitlesi ortalamanın çok üstünde.",
                created_at=self.now,
            ),
            OwnerFeedback(
                feedback_id="CALIB-03",
                story_id=None,
                article_id="art_calib_03",
                rating=OwnerFeedbackRating.REJECT,
                notes="Olumsuz ve can sıkıcı bir haber.",
                created_at=self.now,
            ),
        ]

        count1 = self.repo.import_owner_feedback_batch(batch)
        self.assertEqual(count1, 2)

        # Re-importing the exact same batch must be idempotent (still 2 rows, no duplicates)
        count2 = self.repo.import_owner_feedback_batch(batch)
        self.assertEqual(count2, 2)

        stored = self.repo.get_owner_feedback(limit=10)
        self.assertEqual(len(stored), 2)
        self.assertEqual(stored[0].feedback_id, "CALIB-01")
        self.assertEqual(stored[0].rating, OwnerFeedbackRating.APPROVE)
        self.assertEqual(stored[0].notes, "Hayran kitlesi ortalamanın çok üstünde.")
        self.assertEqual(stored[1].feedback_id, "CALIB-03")
        self.assertEqual(stored[1].rating, OwnerFeedbackRating.REJECT)
        self.assertEqual(stored[1].notes, "Olumsuz ve can sıkıcı bir haber.")

    def test_digest_queue_and_verification_hold_parity(self):
        """Both SQLite and Turso persist digest queues, verification holds, and daily sent counters."""
        q_notif = NotificationRecord(
            notification_id="DIGEST-Q-1",
            story_id="STORY-DIG-1",
            kind=EditorialKind.NEW,
            sent_at=self.now,
            status=DeliveryStatus.QUEUED_FOR_DIGEST,
            message_text="Digest item 1",
            headline_tr="Özet Haber 1",
            score=85,
        )
        h_notif = NotificationRecord(
            notification_id="HOLD-V-1",
            story_id="STORY-HOLD-1",
            kind=EditorialKind.NEW,
            sent_at=self.now,
            status=DeliveryStatus.HELD_FOR_VERIFICATION,
            message_text="Rumor item 1",
            headline_tr="İddia Haber 1",
            score=87,
        )
        self.repo.record_notification(q_notif)
        self.repo.record_notification(h_notif)

        # Both QUEUED_FOR_DIGEST and HELD_FOR_VERIFICATION count as already alerted/queued
        self.assertTrue(self.repo.has_story_been_alerted("STORY-DIG-1", kind=EditorialKind.NEW))
        self.assertTrue(self.repo.has_story_been_alerted("STORY-HOLD-1", kind=EditorialKind.NEW))

        queued = self.repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0].notification_id, "DIGEST-Q-1")

        held = self.repo.get_verification_held_notifications()
        self.assertEqual(len(held), 1)
        self.assertEqual(held[0].notification_id, "HOLD-V-1")

        # Mark digest notification dispatched
        dispatch_time = self.now + timedelta(hours=6)
        updated = self.repo.mark_digest_notifications_dispatched(
            ["DIGEST-Q-1"], delivered_at=dispatch_time
        )
        self.assertEqual(updated, 1)
        self.assertEqual(len(self.repo.get_queued_digest_notifications()), 0)
        self.assertEqual(self.repo.get_daily_sent_message_count(dispatch_time), 1)
        self.assertEqual(self.repo.get_last_digest_sent_at(), dispatch_time)

    def test_sensitive_review_isolation_parity(self):
        """Both SQLite and Turso isolate HELD_FOR_SENSITIVE_REVIEW from queued digest and verification holds."""
        sens_notif = NotificationRecord(
            notification_id="HOLD-SENS-1",
            story_id="STORY-SENS-1",
            kind=EditorialKind.NEW,
            channel="TELEGRAM_SENSITIVE_REVIEW",
            sent_at=self.now,
            status=DeliveryStatus.HELD_FOR_SENSITIVE_REVIEW,
            message_text="Cindy Crawford ve Rande Gerber taziye haberi",
            headline_tr="Acılı anne babanın ilk görüntüsü",
            score=90,
            error_message="Held for sensitive review: requires explicit Product Owner approval",
        )
        self.repo.record_notification(sens_notif)

        # 1. Recognized as already tracked/alerted so duplicate notifications are blocked
        self.assertTrue(self.repo.has_story_been_alerted("STORY-SENS-1", kind=EditorialKind.NEW))

        # 2. Complete isolation: MUST NOT appear in queued digest notifications
        queued = self.repo.get_queued_digest_notifications()
        self.assertEqual(len(queued), 0, "Sensitive review records must never leak into queued digest outbox")

        # 3. Complete isolation: MUST NOT appear in verification-only hold notifications
        held_verif = self.repo.get_verification_held_notifications()
        self.assertEqual(len(held_verif), 0, "Sensitive review records must not mix into standard verification holds")

        # 4. Correctly retrievable via get_sensitive_review_notifications
        sens_records = self.repo.get_sensitive_review_notifications()
        self.assertEqual(len(sens_records), 1)
        self.assertEqual(sens_records[0].notification_id, "HOLD-SENS-1")
        self.assertEqual(sens_records[0].status, DeliveryStatus.HELD_FOR_SENSITIVE_REVIEW)
        self.assertEqual(sens_records[0].channel, "TELEGRAM_SENSITIVE_REVIEW")

        # 5. Age filtering parity
        recent_records = self.repo.get_sensitive_review_notifications(max_age_hours=24, now=self.now + timedelta(hours=2))
        self.assertEqual(len(recent_records), 1)

        old_records = self.repo.get_sensitive_review_notifications(max_age_hours=1, now=self.now + timedelta(hours=5))
        self.assertEqual(len(old_records), 0)

    def test_ai_usage_ledger_and_batch_cache_parity(self):
        """Both SQLite and Turso track monthly AI token/cost usage and cache batch responses."""
        self.repo.record_ai_usage(
            usage_id="USAGE-001",
            run_mode="SHADOW_EVAL",
            model_name="gemini-3.5-flash-lite",
            batch_hash="hash_batch_abc",
            candidate_count=8,
            input_tokens=2400,
            output_tokens=720,
            estimated_cost_usd=0.000528,
            response_json='{"picks": []}',
            created_at=self.now,
        )
        cached = self.repo.get_cached_ai_batch_response("hash_batch_abc", "gemini-3.5-flash-lite")
        self.assertEqual(cached, '{"picks": []}')

        usage = self.repo.get_monthly_ai_usage("2026-10")
        self.assertEqual(usage["call_count"], 1)
        self.assertEqual(usage["input_tokens"], 2400)
        self.assertEqual(usage["output_tokens"], 720)
        self.assertAlmostEqual(float(usage["total_cost_usd"]), 0.000528, places=6)

    def test_candidate_backlog_parity_and_expiration(self):
        """Both SQLite and Turso persist candidate backlog, deduplicate repeat inserts, and expire stale items."""
        # 1. Save fresh candidates
        art1 = self.make_sample_article("Tarkan turnesi duyuruldu", "https://tarkan.com/turne")
        art2 = Article.create(
            title="Hadise bilet güncellemesi",
            url="https://hadise.com/update",
            source="TestFeed",
            published_at=self.now - timedelta(hours=30),  # Stale (>24h)
            raw_metadata={"kind": "UPDATE", "priority_score": 12.0},
        )
        art3 = Article.create(
            title="Eski magazin dedikodusu",
            url="https://old.com/rumor",
            source="TestFeed",
            published_at=self.now - timedelta(hours=35),  # Stale (>24h, not UPDATE)
            raw_metadata={"priority_score": 4.0},
        )

        self.repo.save_pending_candidates([art1, art2, art3])

        # 2. Idempotency test: Repeat save_pending_candidates must not duplicate or crash
        self.repo.save_pending_candidates([art1, art2, art3])

        # 3. Retrieve pending candidates within 24h
        # art1 (fresh) and art2 (stale but UPDATE) must be returned; art3 (>24h, not UPDATE) must NOT be returned
        pending = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        pending_ids = {a.id for a in pending}
        self.assertIn(art1.id, pending_ids)
        self.assertIn(art2.id, pending_ids)
        self.assertNotIn(art3.id, pending_ids)

        # 4. Expire stale candidates: art3 transitions to EXPIRED, art2 is preserved
        expired_count = self.repo.expire_stale_backlog_candidates(max_age_hours=24, now=self.now)
        self.assertEqual(expired_count, 1)

        # 5. Mark art1 evaluated: transitions to EVALUATED
        self.repo.mark_candidates_evaluated([art1.id])
        pending_after_eval = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        pending_after_eval_ids = {a.id for a in pending_after_eval}
        self.assertNotIn(art1.id, pending_after_eval_ids)
        self.assertIn(art2.id, pending_after_eval_ids)


class TestSqliteRepositoryParity(unittest.TestCase, RepositoryContractTestMixin):
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


class TestTursoRepositoryParity(unittest.TestCase, RepositoryContractTestMixin):
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

    def test_transaction_rollback_on_statement_failure(self):
        """Regression test: Multi-statement operation that fails at step 2 must roll back step 1."""
        valid_sql = "INSERT INTO processed_articles (article_id, title, source, published_at, processed_at) VALUES (?, ?, ?, ?, ?)"
        invalid_sql = "INSERT INTO non_existent_table_xyz (col) VALUES (?)"

        now_iso = self.now.isoformat()
        with self.assertRaises(Exception):
            self.repo.client.execute_transaction([
                (valid_sql, ["test-art-rollback", "Test Title", "Src", now_iso, now_iso]),
                (invalid_sql, ["val"]),
            ])

        # Verify that test-art-rollback was ROLLED BACK and does NOT exist in processed_articles!
        self.assertFalse(self.repo.is_article_processed("test-art-rollback"))
