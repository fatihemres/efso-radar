"""Adversarial tests for the exact mock-Hrana outbound SQL boundary."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import TursoHTTPClient
from efso_radar.staging.hrana_outbound_guard import (
    AuthorizedMockIntent,
    GuardedHermeticHranaTransport,
    MockHranaDenied,
)
from efso_radar.staging.manifest_seal import OfflineManifestSeal
from efso_radar.staging.offline_operator import _INSERT_ARTICLE, _INSERT_STORY
from efso_radar.staging.runner import (
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
    PerRunOwnershipManifest,
    create_default_manifest_entities,
)


RUN = "run_20261027T090000Z_012345abcdef"


class TestGuardedHermeticHranaTransport(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        for stmt in get_all_init_statements():
            self.conn.execute(stmt)
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-10-27T09:00:00Z', 'schema-v6')"
        )
        self.conn.commit()
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = PerRunOwnershipManifest(
            run_id=RUN,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=create_default_manifest_entities(RUN),
        ).save_atomic(Path(temp.name))
        self.seal = OfflineManifestSeal.from_persisted(self.path, RUN)
        stamp = "2026-10-25T15:00:00Z"
        self.article_id = f"synth_art_{RUN}_01"
        self.story_id = f"synth_story_{RUN}_01"
        self.article = AuthorizedMockIntent(
            _INSERT_ARTICLE,
            (
                self.article_id,
                f"https://example.com/synth/{RUN}/01",
                f"SYNTH_ARTICLE_{RUN}_01",
                "staging_synth_test",
                stamp,
                stamp,
            ),
        )
        self.story = AuthorizedMockIntent(
            _INSERT_STORY,
            (
                self.story_id,
                f"SYNTH_STORY_{RUN}_01",
                "synth_test",
                stamp,
                stamp,
                json.dumps([self.article_id]),
                1,
                1,
            ),
        )
        self.read = AuthorizedMockIntent(
            "SELECT 1 AS found FROM processed_articles WHERE article_id = ?",
            (self.article_id,),
        )
        self.mock = HermeticStagingTransport(self.conn)
        self.budget = BudgetTrackingTransport(self.mock)
        self.transport = self.make_transport()
        self.client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-no-real-credential",
            transport=self.transport,
        )
        self.addCleanup(self.client.close)

    def make_transport(self, *, mock=None, intents=None):
        return GuardedHermeticHranaTransport(
            inner=BudgetTrackingTransport(mock or self.mock),
            seal=self.seal,
            approved_intents=intents if intents is not None else (
                self.article, self.story, self.read
            ),
        )

    def count(self, table):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_direct_client_transaction_uses_mock_boundary_including_commit(self):
        with patch.object(
            httpx.HTTPTransport, "handle_request",
            side_effect=AssertionError("No real network allowed"),
        ):
            results = self.client.execute_transaction([
                (self.article.sql, list(self.article.args)),
                (self.story.sql, list(self.story.args)),
            ])
            self.assertEqual(len(results), 2)
            read = self.client.execute(self.read.sql, list(self.read.args))
        self.assertEqual(read.fetchone()["found"], 1)
        self.assertEqual(self.count("processed_articles"), 1)
        self.assertEqual(self.count("stories"), 1)
        self.assertEqual(self.budget.request_count, 0)
        self.assertEqual(self.transport._requests, 3)
        self.assertFalse(self.transport.halted)
        self.assertFalse(self.transport.unknown)
        self.assertTrue(self.path.exists(), "Mock wrapper cannot clean up manifest")

    def test_direct_execute_cannot_bypass_sql_table_allowlist(self):
        with self.assertRaisesRegex(MockHranaDenied, "SQL_OR_ARGS_NOT_ALLOWED"):
            self.client.execute("DELETE FROM notifications WHERE id = ?", ["someone"])
        self.assertTrue(self.transport.halted)
        self.assertTrue(self.transport.unknown)
        self.assertEqual(self.transport._requests, 0)
        self.assertEqual(self.count("processed_articles"), 0)

    def test_direct_executescript_does_not_bypass(self):
        with self.assertRaises(MockHranaDenied):
            self.client.executescript("CREATE TABLE pwn (id TEXT); DROP TABLE stories")
        self.assertTrue(self.transport.halted)
        self.assertEqual(self.transport._requests, 0)

    def test_wrong_args_and_forged_record_fail_before_dispatch(self):
        for candidate in (
            (self.article.sql, [f"synth_art_{RUN}_99", *self.article.args[1:]]),
            (self.article.sql, [self.article_id, "https://evil.invalid", *self.article.args[2:]]),
            (self.article.sql, [self.article_id, *self.article.args[1:-1], "2099-01-01"]),
        ):
            with self.subTest(candidate=candidate[1][0]):
                transport = self.make_transport()
                client = TursoHTTPClient(
                    database_url=EXACT_STAGING_DESTINATION,
                    transport=transport,
                )
                self.addCleanup(client.close)
                with self.assertRaisesRegex(MockHranaDenied, "SQL_OR_ARGS_NOT_ALLOWED"):
                    client.execute(candidate[0], candidate[1])
                self.assertEqual(transport._requests, 0)

    def test_disallowed_second_transaction_statement_blocks_entire_batch(self):
        with self.assertRaisesRegex(MockHranaDenied, "SQL_OR_ARGS_NOT_ALLOWED"):
            self.client.execute_transaction([
                (self.article.sql, list(self.article.args)),
                ("DELETE FROM notifications WHERE id = ?", ["bad"]),
            ])
        self.assertEqual(self.count("processed_articles"), 0)
        self.assertEqual(self.transport._requests, 0)
        self.assertTrue(self.transport.unknown)

    def test_repeat_after_success_is_denied_without_second_insert(self):
        self.client.execute(self.article.sql, list(self.article.args))
        self.assertEqual(self.count("processed_articles"), 1)
        with self.assertRaisesRegex(MockHranaDenied, "SQL_OR_ARGS_NOT_ALLOWED"):
            self.client.execute(self.article.sql, list(self.article.args))
        self.assertTrue(self.transport.halted)
        self.assertEqual(self.count("processed_articles"), 1)
        self.assertTrue(self.path.exists())

    def test_unknown_on_commit_timeout_prevents_blind_retry(self):
        mock = HermeticStagingTransport(
            self.conn, fail_on_commit_timeout=True
        )
        guard = self.make_transport(mock=mock)
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION, transport=guard
        )
        self.addCleanup(client.close)
        with self.assertRaisesRegex(MockHranaDenied, "TRANSPORT_OR_PROTOCOL_UNKNOWN"):
            client.execute_transaction([
                (self.article.sql, list(self.article.args)),
                (self.story.sql, list(self.story.args)),
            ])
        self.assertTrue(guard.unknown)
        self.assertTrue(guard.halted)
        with self.assertRaisesRegex(MockHranaDenied, "SESSION_HALTED"):
            client.execute(self.read.sql, list(self.read.args))
        self.assertTrue(self.path.exists())

    def test_truncated_mock_hrana_response_is_unknown_and_never_retried(self):
        # An HTTP 200 with only one result for execute+close is not success.
        malformed = httpx.Response(
            200, json={"baton": None, "results": [{"type": "ok"}]}
        )
        with patch.object(
            self.transport._inner, "handle_request", return_value=malformed
        ) as backend:
            with self.assertRaisesRegex(MockHranaDenied, "RESPONSE_ERROR"):
                self.client.execute(self.article.sql, list(self.article.args))
            self.assertEqual(backend.call_count, 1)
            with self.assertRaisesRegex(MockHranaDenied, "SESSION_HALTED"):
                self.client.execute(self.article.sql, list(self.article.args))
            self.assertEqual(backend.call_count, 1)
        self.assertTrue(self.transport.unknown)
        self.assertTrue(self.path.exists())
        self.assertEqual(self.count("processed_articles"), 0)

    def test_manifest_tampering_fails_before_mock_dispatch(self):
        data = json.loads(self.path.read_text(encoding="utf-8"))
        data["created_at"] = "2099-12-31T23:59:59Z"
        self.path.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaisesRegex(MockHranaDenied, "PRE_DISPATCH"):
            self.client.execute(self.article.sql, list(self.article.args))
        self.assertTrue(self.transport.unknown)
        self.assertEqual(self.transport._requests, 0)
        self.assertTrue(self.path.exists())

    def test_wrong_destination_and_duplicate_json_keys_halt(self):
        for payload in (
            b'{"requests":[],"requests":[]}',
            b'{"requests":[{"type":"execute","stmt":{"sql":"COMMIT"}}]}',
            b'{"requests":[{"type":"close"}]}',
        ):
            with self.subTest(payload=payload):
                guard = self.make_transport()
                req = httpx.Request(
                    "POST",
                    "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline",
                    content=payload,
                )
                with self.assertRaises(MockHranaDenied):
                    guard.handle_request(req)
                self.assertTrue(guard.unknown)
        guard = self.make_transport()
        with self.assertRaisesRegex(MockHranaDenied, "DESTINATION"):
            guard.handle_request(httpx.Request(
                "POST", "https://example.com/v2/pipeline", content=b'{}'
            ))
        self.assertTrue(guard.halted)

    def test_cannot_wrap_real_transport_or_file_backed_sqlite(self):
        with self.assertRaises(PermissionError):
            GuardedHermeticHranaTransport(
                inner=httpx.HTTPTransport(retries=0),
                seal=self.seal,
                approved_intents=(self.article,),
            )
        with tempfile.TemporaryDirectory() as folder:
            disk = sqlite3.connect(str(Path(folder) / "offline-test.db"))
            try:
                with self.assertRaises(PermissionError):
                    GuardedHermeticHranaTransport(
                        inner=BudgetTrackingTransport(HermeticStagingTransport(disk)),
                        seal=self.seal, approved_intents=(self.article,),
                    )
            finally:
                disk.close()

    def test_unregistered_or_unsafe_intents_rejected_at_construction(self):
        invalid = (
            AuthorizedMockIntent("DROP TABLE stories", (self.article_id,)),
            AuthorizedMockIntent(_INSERT_ARTICLE, ("some-other-run", *self.article.args[1:])),
            AuthorizedMockIntent(_INSERT_ARTICLE, (
                self.article_id, self.article.args[1], "wrong-owner",
                *self.article.args[3:],
            )),
            AuthorizedMockIntent("SELECT * FROM notifications", ("private-id",)),
        )
        for item in invalid:
            with self.subTest(sql=item.sql), self.assertRaises(ValueError):
                self.make_transport(intents=(item,))

    def test_duplicate_intent_does_not_expand_retry_capability(self):
        with self.assertRaises(ValueError):
            self.make_transport(intents=(self.article, self.article))

    def test_error_strings_never_contain_sql_or_auth_token(self):
        secret = "token-MUST-NOT-APPEAR"
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token=secret,
            transport=self.make_transport(),
        )
        self.addCleanup(client.close)
        with self.assertRaises(MockHranaDenied) as context:
            client.execute("DROP TABLE private_data", [])
        self.assertNotIn(secret, str(context.exception))
        self.assertNotIn("private_data", str(context.exception))


if __name__ == "__main__":
    unittest.main()
