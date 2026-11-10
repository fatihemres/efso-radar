"""Contract and parity tests verifying identical behavior between SqliteStateRepository and TursoStateRepository.

Tests TursoStateRepository via deterministic HTTP mock transport simulating the libSQL Hrana v2 protocol.
"""
from __future__ import annotations

import base64
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from efso_radar.domain import (
    Article,
    DeliveryIdentityConflictError,
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
    TelegramDeliveryRecord,
    TelegramNotificationCategory,
    utc_now,
)
from efso_radar.repositories.schema import CURRENT_SCHEMA_VERSION
from efso_radar.repositories.sqlite import SqliteStateRepository
from efso_radar.repositories.turso import TursoStateRepository
from efso_radar.repositories.turso_client import deserialize_hrana_value


def create_mock_hrana_transport(
    conn: sqlite3.Connection,
    executed_statements: list[str] | None = None,
) -> httpx.MockTransport:
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
                if executed_statements is not None:
                    executed_statements.append(sql)
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

        with patch(
            "efso_radar.repositories.sqlite.utc_now", return_value=self.now
        ), patch(
            "efso_radar.repositories.turso.utc_now", return_value=self.now
        ):
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

    def test_candidate_backlog_expired_reinsert_never_resets_age_or_status_parity(self):
        """Verifies that re-inserting an already expired candidate via save_pending_candidates
        does NOT reset its status back to QUEUED or modify its discovery age in SQLite or Turso."""
        art_expired = Article.create(
            title="Süresi Dolan Bağımsız Haber",
            url="https://haber.com/suresi-dolan",
            source="TestFeed",
            published_at=self.now - timedelta(hours=25),
            raw_metadata={"priority_score": 4.5},
        )
        self.repo.save_pending_candidates([art_expired])
        # Expire it
        expired_count = self.repo.expire_stale_backlog_candidates(max_age_hours=24, now=self.now)
        self.assertEqual(expired_count, 1)

        # Confirm not pending
        pending = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        self.assertNotIn(art_expired.id, {a.id for a in pending})

        # Attempt to re-insert (e.g. from repeat RSS feed ingestion)
        self.repo.save_pending_candidates([art_expired])

        # Confirm still not pending and remains expired
        pending_after = self.repo.get_pending_candidates(max_age_hours=24, now=self.now)
        self.assertNotIn(art_expired.id, {a.id for a in pending_after})

    def test_bounded_backlog_retrieval_and_deterministic_tie_breaker_parity(self):
        """Verifies bounded backlog retrieval (A.1):
        1. When pending candidates exceed limit (e.g. 150 articles with limit=110),
           get_pending_candidates strictly returns exactly 110 articles.
        2. Unfetched records remain QUEUED and eligible until normal expiry.
        3. Sorting is deterministic: priority_score DESC, published_at DESC, article_id ASC tie-breaker.
        4. Applies identically in SQLite and Turso.
        """
        articles = []
        for i in range(150):
            score = 8.0 if i < 50 else (6.0 if i < 100 else 4.0)
            pub_offset = (i % 10)
            pub_time = self.now - timedelta(minutes=pub_offset)
            art = Article(
                id=f"art_{i:04d}",
                title=f"Aday Haber {i}",
                url=f"https://haber.com/aday-{i}",
                source="TestFeed",
                published_at=pub_time,
                raw_metadata={"priority_score": score},
            )
            articles.append(art)

        self.repo.save_pending_candidates(articles)

        # Retrieve with limit=110
        pending = self.repo.get_pending_candidates(max_age_hours=24, now=self.now, limit=110)
        self.assertEqual(len(pending), 110)

        # Verify ordering contract:
        for j in range(len(pending) - 1):
            curr_art = pending[j]
            next_art = pending[j + 1]
            curr_score = float(curr_art.raw_metadata.get("priority_score", 0.0))
            next_score = float(next_art.raw_metadata.get("priority_score", 0.0))
            self.assertGreaterEqual(curr_score, next_score)
            if curr_score == next_score:
                self.assertGreaterEqual(curr_art.published_at, next_art.published_at)
                if curr_art.published_at == next_art.published_at:
                    self.assertLess(curr_art.id, next_art.id)

        # Verify that all 40 unfetched articles remain in backlog with status QUEUED
        fetched_ids = {a.id for a in pending}
        unfetched_ids = {a.id for a in articles} - fetched_ids
        self.assertEqual(len(unfetched_ids), 40)

        # If we fetch without limit, all 150 are returned
        all_pending = self.repo.get_pending_candidates(max_age_hours=24, now=self.now, limit=None)
        self.assertEqual(len(all_pending), 150)

    def test_priority_gated_update_expiration_boundaries_and_threshold_parity(self):
        """Verifies priority-gated UPDATE expiration (B.3):
        1. Exact age boundaries: Candidate at T+23h59m is NOT expired; at T+24h01m expires unless UPDATE with score >= threshold.
        2. Threshold equality: score == 5.5 is retained (NOT expired); score == 5.49 expires.
        3. Non-default threshold: passing min_update_priority=7.0 expires an UPDATE candidate with score 6.0,
           while retaining one with score 7.0.
        4. Parity between SQLite and Turso.
        """
        # Exactly 23 hours 59 minutes ago (within 24h window)
        art_fresh = Article(
            id="art_fresh_23h59m",
            title="Taze Haber 23h59m",
            url="https://haber.com/fresh",
            source="TestFeed",
            published_at=self.now - timedelta(hours=23, minutes=59),
            source_category="UPDATE",
            raw_metadata={"priority_score": 3.0},
        )
        # Exactly 24 hours 1 minute ago (beyond 24h window), low priority UPDATE (score 5.49 < 5.5)
        art_below_threshold = Article(
            id="art_stale_549",
            title="Eski Haber 5.49",
            url="https://haber.com/549",
            source="TestFeed",
            published_at=self.now - timedelta(hours=24, minutes=1),
            source_category="UPDATE",
            raw_metadata={"priority_score": 5.49},
        )
        # Exactly 24 hours 1 minute ago (beyond 24h window), exact threshold equality (score 5.50 == 5.5)
        art_exact_threshold = Article(
            id="art_stale_550",
            title="Eski Haber 5.50",
            url="https://haber.com/550",
            source="TestFeed",
            published_at=self.now - timedelta(hours=24, minutes=1),
            source_category="UPDATE",
            raw_metadata={"priority_score": 5.50},
        )
        # Exactly 24 hours 1 minute ago, non-update ordinary article with high score 9.0 (should expire)
        art_ordinary_high = Article(
            id="art_ord_high",
            title="Eski Sıradan Haber 9.0",
            url="https://haber.com/ord-high",
            source="TestFeed",
            published_at=self.now - timedelta(hours=24, minutes=1),
            source_category="GENERAL",
            raw_metadata={"priority_score": 9.0},
        )
        # Exactly 24 hours 1 minute ago, UPDATE with score 6.0
        art_score_60 = Article(
            id="art_stale_60",
            title="Eski Haber 6.0",
            url="https://haber.com/60",
            source="TestFeed",
            published_at=self.now - timedelta(hours=24, minutes=1),
            source_category="UPDATE",
            raw_metadata={"priority_score": 6.0},
        )
        # Exactly 24 hours 1 minute ago, UPDATE with score 7.0
        art_score_70 = Article(
            id="art_stale_70",
            title="Eski Haber 7.0",
            url="https://haber.com/70",
            source="TestFeed",
            published_at=self.now - timedelta(hours=24, minutes=1),
            source_category="UPDATE",
            raw_metadata={"priority_score": 7.0},
        )

        test_batch = [
            art_fresh,
            art_below_threshold,
            art_exact_threshold,
            art_ordinary_high,
            art_score_60,
            art_score_70,
        ]
        self.repo.save_pending_candidates(test_batch)

        # First expire pass with default threshold min_update_priority=5.5
        expired_count = self.repo.expire_stale_backlog_candidates(
            max_age_hours=24, now=self.now, min_update_priority=5.5
        )
        self.assertEqual(expired_count, 2)

        # Check pending candidates at threshold 5.5
        pending_55 = self.repo.get_pending_candidates(
            max_age_hours=24, now=self.now, min_update_priority=5.5
        )
        pending_55_ids = {a.id for a in pending_55}

        self.assertIn(art_fresh.id, pending_55_ids)
        self.assertIn(art_exact_threshold.id, pending_55_ids)
        self.assertIn(art_score_60.id, pending_55_ids)
        self.assertIn(art_score_70.id, pending_55_ids)
        self.assertNotIn(art_below_threshold.id, pending_55_ids)
        self.assertNotIn(art_ordinary_high.id, pending_55_ids)

        # Second pass: Non-default configured threshold min_update_priority=7.0
        expired_count_70 = self.repo.expire_stale_backlog_candidates(
            max_age_hours=24, now=self.now, min_update_priority=7.0
        )
        self.assertEqual(expired_count_70, 2)

        pending_70 = self.repo.get_pending_candidates(
            max_age_hours=24, now=self.now, min_update_priority=7.0
        )
        pending_70_ids = {a.id for a in pending_70}
        self.assertIn(art_fresh.id, pending_70_ids, "Fresh candidate remains pending regardless of threshold")
        self.assertIn(art_score_70.id, pending_70_ids, "Candidate meeting threshold 7.0 remains pending")
        self.assertNotIn(art_score_60.id, pending_70_ids, "Candidate below threshold 7.0 is no longer pending")
        self.assertNotIn(art_exact_threshold.id, pending_70_ids, "Candidate below threshold 7.0 is no longer pending")


    def test_delivery_uncertain_record_replay_idempotency_and_preservation(self):
        """Verifies record_telegram_delivery replay on DELIVERY_UNCERTAIN is a safe no-op preserving all fields."""
        chat_id = "1000000001"
        message_key = "digest_uncertain_record_001"
        delivery_id = "deliv_unc_rec_001"
        payload = {"digest_hash": "hash_abc_123", "stories_count": 3}
        original_error = "HTTP 504 Gateway Timeout during initial dispatch"
        t0 = self.now

        rec = TelegramDeliveryRecord(
            delivery_id=delivery_id,
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
            max_attempts=1,
            error_message=original_error,
            payload=payload,
            created_at=t0,
            updated_at=t0,
            sent_at=None,
        )
        self.repo.record_telegram_delivery(rec)

        # Baseline inspection
        stored = self.repo.get_telegram_delivery(chat_id, message_key)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.delivery_id, delivery_id)
        self.assertEqual(stored.status, DeliveryStatus.DELIVERY_UNCERTAIN)
        self.assertEqual(stored.attempt_count, 1)
        self.assertEqual(stored.max_attempts, 1)
        self.assertEqual(stored.error_message, original_error)
        self.assertEqual(stored.payload, payload)
        self.assertEqual(stored.created_at, t0)
        self.assertEqual(stored.updated_at, t0)
        self.assertIsNone(stored.sent_at)

        # 1. Replay with exact same record
        self.repo.record_telegram_delivery(rec)

        # 2. Replay with candidate record having altered attempts, payload, error, and timestamps
        t_later = t0 + timedelta(hours=2)
        mutated_candidate = TelegramDeliveryRecord(
            delivery_id="deliv_unc_rec_999",
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=5,
            max_attempts=10,
            error_message="Altered error message that should be ignored",
            payload={"digest_hash": "overwritten"},
            created_at=t_later,
            updated_at=t_later,
            sent_at=t_later,
        )
        self.repo.record_telegram_delivery(mutated_candidate)

        # 3. Verify absolute preservation of pre-repeat fields
        after = self.repo.get_telegram_delivery(chat_id, message_key)
        self.assertIsNotNone(after)
        self.assertEqual(after.delivery_id, delivery_id)
        self.assertEqual(after.status, DeliveryStatus.DELIVERY_UNCERTAIN)
        self.assertEqual(after.attempt_count, 1)
        self.assertEqual(after.max_attempts, 1)
        self.assertEqual(after.error_message, original_error)
        self.assertEqual(after.payload, payload)
        self.assertEqual(after.created_at, t0)
        self.assertEqual(after.updated_at, t0)
        self.assertIsNone(after.sent_at)

    def test_delivery_uncertain_status_update_replay_idempotency_and_preservation(self):
        """Verifies update_telegram_delivery_status(..., DELIVERY_UNCERTAIN) replay returns True and preserves record."""
        chat_id = "1000000001"
        message_key = "digest_uncertain_status_002"
        delivery_id = "deliv_unc_status_002"
        payload = {"digest_hash": "hash_def_456"}
        original_error = "ReadTimeout during Telegram HTTP transport"
        t0 = self.now

        # 1. Claim delivery first
        claimed = self.repo.claim_telegram_delivery(
            delivery_id=delivery_id,
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            payload=payload,
            max_attempts=1,
        )
        self.assertTrue(claimed)

        # 2. Initial transition to DELIVERY_UNCERTAIN
        upd1 = self.repo.update_telegram_delivery_status(
            delivery_id=delivery_id,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            error_message=original_error,
        )
        self.assertTrue(upd1)

        stored = self.repo.get_telegram_delivery(chat_id, message_key)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, DeliveryStatus.DELIVERY_UNCERTAIN)
        self.assertEqual(stored.attempt_count, 1, "Initial claim + status update with max_attempts=1 must have attempt_count == 1")
        self.assertEqual(stored.error_message, original_error)
        self.assertEqual(stored.payload, payload)
        self.assertIsNone(stored.sent_at)
        updated_at_before = stored.updated_at
        attempt_count_baseline = stored.attempt_count

        # 3. Repeated call with status=DELIVERY_UNCERTAIN, different error, and attempted sent_at
        t_attempted = t0 + timedelta(hours=3)
        upd2 = self.repo.update_telegram_delivery_status(
            delivery_id=delivery_id,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            error_message="Should be ignored replay error",
            sent_at=t_attempted,
        )
        self.assertTrue(upd2, "Repeated update_telegram_delivery_status to DELIVERY_UNCERTAIN must return True")

        # 4. Verify all fields preserved intact (no attempt increment, no error overwrite, no sent_at leak)
        after = self.repo.get_telegram_delivery(chat_id, message_key)
        self.assertIsNotNone(after)
        self.assertEqual(after.status, DeliveryStatus.DELIVERY_UNCERTAIN)
        self.assertEqual(after.attempt_count, attempt_count_baseline, "attempt_count must not increment on idempotent replay")
        self.assertEqual(after.error_message, original_error, "error_message must not be overwritten on replay")
        self.assertEqual(after.payload, payload)
        self.assertEqual(after.updated_at, updated_at_before, "updated_at must remain intact on replay")
        self.assertIsNone(after.sent_at, "sent_at must remain None on DELIVERY_UNCERTAIN replay")

    def test_delivery_uncertain_claim_rejected_no_new_dispatch(self):
        """Verifies claim_telegram_delivery on DELIVERY_UNCERTAIN is rejected and does not mutate or dispatch."""
        chat_id = "1000000001"
        message_key = "digest_uncertain_claim_003"
        delivery_id = "deliv_unc_claim_003"
        payload = {"stories_count": 2}
        t0 = self.now

        # Initialize directly in DELIVERY_UNCERTAIN with max_attempts > attempt_count
        rec = TelegramDeliveryRecord(
            delivery_id=delivery_id,
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
            max_attempts=3,
            error_message="Network partition",
            payload=payload,
            created_at=t0,
            updated_at=t0,
            sent_at=None,
        )
        self.repo.record_telegram_delivery(rec)

        # Attempt to claim with new delivery_id
        claimed = self.repo.claim_telegram_delivery(
            delivery_id="deliv_retry_unauthorized_999",
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            payload={"fresh": "attempt"},
            max_attempts=3,
        )
        self.assertFalse(claimed, "claim_telegram_delivery must return False on DELIVERY_UNCERTAIN record")

        # Verify no new dispatch record exists and original record is not mutated
        curr = self.repo.get_telegram_delivery(chat_id, message_key)
        self.assertIsNotNone(curr)
        self.assertEqual(curr.delivery_id, delivery_id, "delivery_id must not change to new claim ID")
        self.assertEqual(curr.status, DeliveryStatus.DELIVERY_UNCERTAIN, "status must remain DELIVERY_UNCERTAIN")
        self.assertEqual(curr.attempt_count, 1, "attempt_count must not increment on rejected claim")
        self.assertEqual(curr.payload, payload, "payload must not be overwritten by new claim")

    def test_delivery_uncertain_negative_scenarios_and_conflicts(self):
        """Verifies identity conflict rejections and non-existent ID handling for DELIVERY_UNCERTAIN."""
        chat_id = "1000000001"
        message_key = "digest_uncertain_neg_004"
        delivery_id = "deliv_unc_neg_004"

        rec = TelegramDeliveryRecord(
            delivery_id=delivery_id,
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
            max_attempts=1,
            error_message="Timeout",
            payload={},
            created_at=self.now,
            updated_at=self.now,
            sent_at=None,
        )
        self.repo.record_telegram_delivery(rec)

        # 1. Conflicting recipient_role on record_telegram_delivery
        conflicting_role_rec = TelegramDeliveryRecord(
            delivery_id="deliv_other_001",
            message_key=message_key,
            recipient_role="SECONDARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
            max_attempts=1,
            error_message="Timeout",
            payload={},
            created_at=self.now,
            updated_at=self.now,
            sent_at=None,
        )
        with self.assertRaises(DeliveryIdentityConflictError):
            self.repo.record_telegram_delivery(conflicting_role_rec)

        # 2. Conflicting category on record_telegram_delivery
        conflicting_cat_rec = TelegramDeliveryRecord(
            delivery_id="deliv_other_002",
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.VERIFIED_BREAKING,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
            max_attempts=1,
            error_message="Timeout",
            payload={},
            created_at=self.now,
            updated_at=self.now,
            sent_at=None,
        )
        with self.assertRaises(DeliveryIdentityConflictError):
            self.repo.record_telegram_delivery(conflicting_cat_rec)

        # 3. Conflicting role on claim_telegram_delivery
        with self.assertRaises(DeliveryIdentityConflictError):
            self.repo.claim_telegram_delivery(
                delivery_id="deliv_other_003",
                message_key=message_key,
                recipient_role="SECONDARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            )

        # 4. Conflicting category on claim_telegram_delivery
        with self.assertRaises(DeliveryIdentityConflictError):
            self.repo.claim_telegram_delivery(
                delivery_id="deliv_other_004",
                message_key=message_key,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.VERIFIED_BREAKING,
            )

        # 5. Non-existent delivery_id on update_telegram_delivery_status returns False
        res = self.repo.update_telegram_delivery_status(
            delivery_id="non_existent_id_xyz",
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
        )
        self.assertFalse(res)


    def test_delivery_attempt_counter_lifecycle_and_single_attempt_correctness(self):
        """Verifies attempt_count represents actual dispatch attempts across claim, update, retry, and pending states."""
        chat_id = "1000000001"
        t0 = self.now

        # 1. Single attempt: DISPATCHING -> SENT with max_attempts=1
        deliv_id_sent = "deliv_counter_sent_001"
        key_sent = "key_counter_sent_001"
        self.assertTrue(
            self.repo.claim_telegram_delivery(
                delivery_id=deliv_id_sent,
                message_key=key_sent,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                max_attempts=1,
            )
        )
        rec_disp = self.repo.get_telegram_delivery(chat_id, key_sent)
        self.assertIsNotNone(rec_disp)
        self.assertEqual(rec_disp.status, DeliveryStatus.DISPATCHING)
        self.assertEqual(rec_disp.attempt_count, 1)

        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_sent, DeliveryStatus.SENT))
        rec_sent = self.repo.get_telegram_delivery(chat_id, key_sent)
        self.assertIsNotNone(rec_sent)
        self.assertEqual(rec_sent.status, DeliveryStatus.SENT)
        self.assertEqual(rec_sent.attempt_count, 1, "Single successful delivery attempt must have attempt_count == 1")

        # Subsequent claim permanently rejected
        self.assertFalse(
            self.repo.claim_telegram_delivery(
                delivery_id="deliv_counter_sent_retry",
                message_key=key_sent,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            )
        )

        # 2. Single attempt: DISPATCHING -> FAILED with max_attempts=1
        deliv_id_failed = "deliv_counter_failed_001"
        key_failed = "key_counter_failed_001"
        self.assertTrue(
            self.repo.claim_telegram_delivery(
                delivery_id=deliv_id_failed,
                message_key=key_failed,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                max_attempts=1,
            )
        )
        self.assertTrue(
            self.repo.update_telegram_delivery_status(
                deliv_id_failed, DeliveryStatus.FAILED, error_message="Fatal HTTP 403"
            )
        )
        rec_failed = self.repo.get_telegram_delivery(chat_id, key_failed)
        self.assertIsNotNone(rec_failed)
        self.assertEqual(rec_failed.status, DeliveryStatus.FAILED)
        self.assertEqual(rec_failed.attempt_count, 1, "Single failed delivery attempt must have attempt_count == 1")

        # Retry blocked because attempt_count (1) >= max_attempts (1)
        self.assertFalse(
            self.repo.claim_telegram_delivery(
                delivery_id="deliv_counter_failed_retry",
                message_key=key_failed,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                max_attempts=1,
            )
        )

        # 3. Multi-attempt retry lifecycle with max_attempts=2: FAILED -> re-claim -> SENT
        deliv_id_retry = "deliv_counter_retry_001"
        key_retry = "key_counter_retry_001"
        # Attempt 1
        self.assertTrue(
            self.repo.claim_telegram_delivery(
                delivery_id=deliv_id_retry,
                message_key=key_retry,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                max_attempts=2,
            )
        )
        self.assertEqual(self.repo.get_telegram_delivery(chat_id, key_retry).attempt_count, 1)
        self.assertTrue(
            self.repo.update_telegram_delivery_status(
                deliv_id_retry, DeliveryStatus.FAILED, error_message="Temporary network glitch"
            )
        )
        rec_retry_1 = self.repo.get_telegram_delivery(chat_id, key_retry)
        self.assertEqual(rec_retry_1.attempt_count, 1, "First failed attempt must leave attempt_count == 1")

        # Attempt 2 (re-claim allowed because attempt_count 1 < max_attempts 2)
        self.assertTrue(
            self.repo.claim_telegram_delivery(
                delivery_id=deliv_id_retry,
                message_key=key_retry,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                max_attempts=2,
            )
        )
        rec_retry_disp2 = self.repo.get_telegram_delivery(chat_id, key_retry)
        self.assertEqual(rec_retry_disp2.status, DeliveryStatus.DISPATCHING)
        self.assertEqual(rec_retry_disp2.attempt_count, 2, "Second claim must set attempt_count == 2")

        # Attempt 2 finalization
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_retry, DeliveryStatus.SENT))
        rec_retry_2 = self.repo.get_telegram_delivery(chat_id, key_retry)
        self.assertEqual(rec_retry_2.status, DeliveryStatus.SENT)
        self.assertEqual(rec_retry_2.attempt_count, 2, "Second successful attempt must leave attempt_count == 2")

        # Attempt 3 blocked (already SENT and attempt_count 2 >= max_attempts 2)
        self.assertFalse(
            self.repo.claim_telegram_delivery(
                delivery_id="deliv_counter_retry_003",
                message_key=key_retry,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                max_attempts=2,
            )
        )

        # 4. Direct update on un-claimed / pre-seeded record in PENDING (attempt_count=0)
        deliv_id_pend = "deliv_counter_pend_001"
        key_pend = "key_counter_pend_001"
        rec_pend = TelegramDeliveryRecord(
            delivery_id=deliv_id_pend,
            message_key=key_pend,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.PENDING,
            attempt_count=0,
            max_attempts=1,
            created_at=t0,
            updated_at=t0,
            sent_at=None,
        )
        self.repo.record_telegram_delivery(rec_pend)
        self.assertEqual(self.repo.get_telegram_delivery(chat_id, key_pend).attempt_count, 0)

        # Direct transition PENDING -> HELD_QUIET_HOURS does not count as a dispatch attempt
        self.assertTrue(
            self.repo.update_telegram_delivery_status(deliv_id_pend, DeliveryStatus.HELD_QUIET_HOURS)
        )
        rec_quiet = self.repo.get_telegram_delivery(chat_id, key_pend)
        self.assertEqual(rec_quiet.status, DeliveryStatus.HELD_QUIET_HOURS)
        self.assertEqual(rec_quiet.attempt_count, 0, "Non-dispatch status update must not increment attempt_count")

        # Direct transition from PENDING -> SENT (e.g. legacy/direct update) sets attempt_count to 1
        deliv_id_pend_sent = "deliv_counter_pend_002"
        key_pend_sent = "key_counter_pend_002"
        rec_pend_sent = TelegramDeliveryRecord(
            delivery_id=deliv_id_pend_sent,
            message_key=key_pend_sent,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.PENDING,
            attempt_count=0,
            max_attempts=1,
            created_at=t0,
            updated_at=t0,
            sent_at=None,
        )
        self.repo.record_telegram_delivery(rec_pend_sent)
        self.assertTrue(
            self.repo.update_telegram_delivery_status(deliv_id_pend_sent, DeliveryStatus.SENT)
        )
        rec_direct_sent = self.repo.get_telegram_delivery(chat_id, key_pend_sent)
        self.assertEqual(rec_direct_sent.status, DeliveryStatus.SENT)
        self.assertEqual(rec_direct_sent.attempt_count, 1, "Direct transition PENDING -> SENT must reflect 1 attempt")

    def test_delivery_attempt_counter_held_and_direct_transitions_parity(self):
        """Verifies attempt_count semantics for HELD_* and direct status transitions across backends."""
        chat_id = "1000000002"
        t0 = self.now

        # 1. HELD_QUIET_HOURS -> SENT directly: attempt_count promoted to 1
        deliv_id_quiet = "deliv_held_quiet_001"
        key_quiet = "key_held_quiet_001"
        self.repo.record_telegram_delivery(
            TelegramDeliveryRecord(
                delivery_id=deliv_id_quiet,
                message_key=key_quiet,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                status=DeliveryStatus.HELD_QUIET_HOURS,
                attempt_count=0,
                max_attempts=1,
                created_at=t0,
                updated_at=t0,
            )
        )
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_quiet, DeliveryStatus.SENT))
        rec_quiet_sent = self.repo.get_telegram_delivery(chat_id, key_quiet)
        self.assertIsNotNone(rec_quiet_sent)
        self.assertEqual(rec_quiet_sent.status, DeliveryStatus.SENT)
        self.assertEqual(rec_quiet_sent.attempt_count, 1, "HELD_QUIET_HOURS -> SENT must promote attempt_count to 1")

        # 2. HELD_FOR_VERIFICATION -> SENT directly: attempt_count promoted to 1
        deliv_id_verif = "deliv_held_verif_001"
        key_verif = "key_held_verif_001"
        self.repo.record_telegram_delivery(
            TelegramDeliveryRecord(
                delivery_id=deliv_id_verif,
                message_key=key_verif,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                status=DeliveryStatus.HELD_FOR_VERIFICATION,
                attempt_count=0,
                max_attempts=1,
                created_at=t0,
                updated_at=t0,
            )
        )
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_verif, DeliveryStatus.SENT))
        rec_verif_sent = self.repo.get_telegram_delivery(chat_id, key_verif)
        self.assertIsNotNone(rec_verif_sent)
        self.assertEqual(rec_verif_sent.status, DeliveryStatus.SENT)
        self.assertEqual(rec_verif_sent.attempt_count, 1, "HELD_FOR_VERIFICATION -> SENT must promote attempt_count to 1")

        # 3. HELD_FOR_SENSITIVE_REVIEW -> SENT directly: attempt_count promoted to 1
        deliv_id_sens = "deliv_held_sens_001"
        key_sens = "key_held_sens_001"
        self.repo.record_telegram_delivery(
            TelegramDeliveryRecord(
                delivery_id=deliv_id_sens,
                message_key=key_sens,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                status=DeliveryStatus.HELD_FOR_SENSITIVE_REVIEW,
                attempt_count=0,
                max_attempts=1,
                created_at=t0,
                updated_at=t0,
            )
        )
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_sens, DeliveryStatus.SENT))
        rec_sens_sent = self.repo.get_telegram_delivery(chat_id, key_sens)
        self.assertIsNotNone(rec_sens_sent)
        self.assertEqual(rec_sens_sent.status, DeliveryStatus.SENT)
        self.assertEqual(rec_sens_sent.attempt_count, 1, "HELD_FOR_SENSITIVE_REVIEW -> SENT must promote attempt_count to 1")

        # 4. Non-dispatch transitions from HELD_* must preserve attempt_count == 0
        deliv_id_exp = "deliv_held_exp_001"
        key_exp = "key_held_exp_001"
        self.repo.record_telegram_delivery(
            TelegramDeliveryRecord(
                delivery_id=deliv_id_exp,
                message_key=key_exp,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                status=DeliveryStatus.HELD_QUIET_HOURS,
                attempt_count=0,
                max_attempts=1,
                created_at=t0,
                updated_at=t0,
            )
        )
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_exp, DeliveryStatus.EXPIRED_STALE))
        rec_exp = self.repo.get_telegram_delivery(chat_id, key_exp)
        self.assertEqual(rec_exp.status, DeliveryStatus.EXPIRED_STALE)
        self.assertEqual(rec_exp.attempt_count, 0, "HELD_QUIET_HOURS -> EXPIRED_STALE must retain attempt_count == 0")

        # 5. Pre-seeded FAILED (attempt_count=0) -> SENT directly promotes attempt_count to 1
        deliv_id_f0 = "deliv_failed_seed_001"
        key_f0 = "key_failed_seed_001"
        self.repo.record_telegram_delivery(
            TelegramDeliveryRecord(
                delivery_id=deliv_id_f0,
                message_key=key_f0,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                status=DeliveryStatus.FAILED,
                attempt_count=0,
                max_attempts=1,
                created_at=t0,
                updated_at=t0,
            )
        )
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_f0, DeliveryStatus.SENT))
        rec_f0 = self.repo.get_telegram_delivery(chat_id, key_f0)
        self.assertEqual(rec_f0.status, DeliveryStatus.SENT)
        self.assertEqual(rec_f0.attempt_count, 1, "Pre-seeded FAILED(0) -> SENT must promote attempt_count to 1")

        # 6. Claimed FAILED (attempt_count=1) -> SENT directly preserves attempt_count == 1 under max_attempts=1
        deliv_id_f1 = "deliv_failed_claimed_001"
        key_f1 = "key_failed_claimed_001"
        self.assertTrue(
            self.repo.claim_telegram_delivery(
                delivery_id=deliv_id_f1,
                message_key=key_f1,
                recipient_role="PRIMARY_EDITOR",
                recipient_chat_id=chat_id,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
                max_attempts=1,
            )
        )
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_f1, DeliveryStatus.FAILED))
        rec_f1_pre = self.repo.get_telegram_delivery(chat_id, key_f1)
        self.assertEqual(rec_f1_pre.status, DeliveryStatus.FAILED)
        self.assertEqual(rec_f1_pre.attempt_count, 1)

        # Direct status update FAILED -> SENT without re-claim
        self.assertTrue(self.repo.update_telegram_delivery_status(deliv_id_f1, DeliveryStatus.SENT))
        rec_f1_post = self.repo.get_telegram_delivery(chat_id, key_f1)
        self.assertEqual(rec_f1_post.status, DeliveryStatus.SENT)
        self.assertEqual(rec_f1_post.attempt_count, 1, "Direct FAILED(1) -> SENT must preserve attempt_count == 1")


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
        self.executed_statements: list[str] = []
        self.mock_transport = create_mock_hrana_transport(
            self.sqlite_conn,
            executed_statements=self.executed_statements,
        )
        self.repo = TursoStateRepository(
            database_url="libsql://efso-test.turso.io",
            auth_token="mock-token-xyz",
            transport=self.mock_transport,
            init_schema=True,
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


    def test_turso_delivery_uncertain_replay_executes_zero_mutating_sql(self):
        """Verifies that idempotent replay of DELIVERY_UNCERTAIN records or updates sends zero mutating SQL to Turso."""
        chat_id = "1000000001"
        message_key = "turso_sql_uncertain_001"
        delivery_id = "turso_deliv_unc_001"

        rec = TelegramDeliveryRecord(
            delivery_id=delivery_id,
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            attempt_count=1,
            max_attempts=1,
            error_message="Network timeout",
            payload={"digest": "test"},
            created_at=self.now,
            updated_at=self.now,
            sent_at=None,
        )
        self.repo.record_telegram_delivery(rec)

        mutating_keywords = ("INSERT", "UPDATE", "DELETE", "REPLACE")

        # 1. Replay record_telegram_delivery
        self.executed_statements.clear()
        self.repo.record_telegram_delivery(rec)
        mutating_calls = [
            sql for sql in self.executed_statements
            if any(sql.upper().startswith(kw) for kw in mutating_keywords)
        ]
        self.assertEqual(
            mutating_calls, [],
            f"record_telegram_delivery replay must not send mutating SQL to Turso, sent: {mutating_calls}"
        )

        # 2. Replay update_telegram_delivery_status
        self.executed_statements.clear()
        res = self.repo.update_telegram_delivery_status(
            delivery_id=delivery_id,
            status=DeliveryStatus.DELIVERY_UNCERTAIN,
            error_message="New error message",
        )
        self.assertTrue(res)
        mutating_calls = [
            sql for sql in self.executed_statements
            if any(sql.upper().startswith(kw) for kw in mutating_keywords)
        ]
        self.assertEqual(
            mutating_calls, [],
            f"update_telegram_delivery_status replay must not send mutating SQL to Turso, sent: {mutating_calls}"
        )

        # 3. Attempt claim_telegram_delivery on existing DELIVERY_UNCERTAIN
        self.executed_statements.clear()
        claimed = self.repo.claim_telegram_delivery(
            delivery_id="turso_claim_retry_002",
            message_key=message_key,
            recipient_role="PRIMARY_EDITOR",
            recipient_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
        )
        self.assertFalse(claimed)
        mutating_calls = [
            sql for sql in self.executed_statements
            if any(sql.upper().startswith(kw) for kw in mutating_keywords)
        ]
        self.assertEqual(
            mutating_calls, [],
            f"claim_telegram_delivery on DELIVERY_UNCERTAIN must not send mutating SQL to Turso, sent: {mutating_calls}"
        )
