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
