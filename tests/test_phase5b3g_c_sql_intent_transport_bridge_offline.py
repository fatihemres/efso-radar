"""PR32 SQL intents -> PR33 guarded Hrana wire: hermetic integration contracts.

NO live Turso client, network transport, billing, credentials or operational
files. Readbacks confirm existence, NOT remote payload ownership/cleanup.
"""
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
from efso_radar.staging.offline_session_gate import OfflineSessionGate
from efso_radar.staging.offline_write_intent import OfflineWriteIntentBuilder
from efso_radar.staging.runner import (
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
    PerRunOwnershipManifest,
    create_default_manifest_entities,
)


RUN_ID = "run_20261027T115000Z_abcdef987654"


class TestOfflineIntentTransportBridge(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        for stmt in get_all_init_statements():
            self.conn.execute(stmt)
        self.conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-10-27T11:50:00Z', 'schema-v6')"
        )
        self.conn.commit()
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        manifest = PerRunOwnershipManifest(
            run_id=RUN_ID,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=create_default_manifest_entities(RUN_ID),
        )
        self.path = manifest.save_atomic(Path(folder.name))
        self.seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        self.builder = OfflineWriteIntentBuilder(self.seal)
        self.pair = self.builder.live02_atomic_pair()
        self.cleanup = self.builder.conditional_cleanup()

    def make_client(self, *, transport=None):
        # This explicit set excludes every DELETE; cleanup needs separate
        # row-attribute reads, verified rowcounts and 3-state audit first.
        article_id = self.pair[0].args[0]
        story_id = self.pair[1].args[0]
        reads = (
            AuthorizedMockIntent(
                "SELECT 1 AS found FROM processed_articles WHERE article_id = ?",
                (article_id,),
            ),
            AuthorizedMockIntent(
                "SELECT 1 AS found FROM stories WHERE story_id = ?",
                (story_id,),
            ),
        )
        intents = tuple(AuthorizedMockIntent(x.sql, x.args) for x in self.pair) + reads
        backend = transport if transport is not None else HermeticStagingTransport(self.conn)
        budget = BudgetTrackingTransport(backend)
        guard = GuardedHermeticHranaTransport(
            inner=budget, seal=self.seal, approved_intents=intents,
        )
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="offline-fixture-only",
            transport=guard,
        )
        self.addCleanup(client.close)
        return client, guard, budget

    def dispatch_live02(self, client):
        return client.execute_transaction([
            (intent.sql, list(intent.args)) for intent in self.pair
        ])

    def count(self, table):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_prepared_sql_pair_is_accepted_at_mock_wire_and_read_back(self):
        client, guard, budget = self.make_client()
        with patch.object(
            httpx.HTTPTransport, "handle_request",
            side_effect=AssertionError("Real HTTP must never occur"),
        ):
            results = self.dispatch_live02(client)
            article = client.execute(
                "SELECT 1 AS found FROM processed_articles WHERE article_id = ?",
                [self.pair[0].args[0]],
            ).fetchone()
            story = client.execute(
                "SELECT 1 AS found FROM stories WHERE story_id = ?",
                [self.pair[1].args[0]],
            ).fetchone()
        self.assertEqual(len(results), 2)
        self.assertEqual(article["found"], 1)
        self.assertEqual(story["found"], 1)
        self.assertEqual(self.count("processed_articles"), 1)
        self.assertEqual(self.count("stories"), 1)
        self.assertEqual(guard._requests, 4)
        self.assertEqual(budget.request_count, 4)
        self.assertFalse(guard.halted)
        self.assertTrue(self.path.exists())

        # Mock stage accounting is observational, never a real operator grant.
        session = OfflineSessionGate(
            expected_manifest_sha256=self.seal.sha256,
            clock=lambda: 0.0,
        )
        session.record_simulated_preflight(
            manifest_sha256=self.seal.sha256,
            schema_version=6,
            tables_checked=15,
            columns_checked=129,
            collision_checks=5,
            simulated_block_writes_on=True,
            request_count=47,
        )
        session.record_simulated_human_window(marker="MOCK_ONLY")
        session.record_simulated_stage(
            stage="LIVE-02", manifest_sha256=self.seal.sha256,
            request_count=budget.request_count,
        )
        self.assertEqual(session.snapshot().next_stage, "LIVE-03")
        self.assertEqual(session.snapshot().requests, 51)

    def test_cleanup_plan_is_not_implicitly_approved_for_dispatch(self):
        client, guard, budget = self.make_client()
        self.assertEqual(len(self.cleanup), 5)
        self.dispatch_live02(client)
        self.assertEqual(guard._requests, 2)
        self.assertEqual(budget.request_count, 2)
        # Caller-supplied canonical SQL alone does not authorize DELETE.
        planned = self.cleanup[0]
        with self.assertRaisesRegex(MockHranaDenied, "SQL_OR_ARGS_NOT_ALLOWED"):
            client.execute(planned.sql, list(planned.args))
        self.assertTrue(guard.halted)
        self.assertTrue(guard.unknown)
        self.assertEqual(guard._requests, 2)
        self.assertEqual(budget.request_count, 2)
        self.assertEqual(self.count("processed_articles"), 1)
        self.assertEqual(self.count("stories"), 1)
        self.assertTrue(self.path.exists())

    def test_manifest_swap_after_plan_is_denied_before_any_sql(self):
        client, guard, budget = self.make_client()
        data = json.loads(self.path.read_text(encoding="utf-8"))
        data["created_at"] = "2099-01-01T00:00:00Z"
        self.path.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaisesRegex(MockHranaDenied, "PRE_DISPATCH"):
            self.dispatch_live02(client)
        self.assertTrue(guard.unknown)
        self.assertTrue(guard.halted)
        self.assertEqual(budget.request_count, 0)
        self.assertEqual(self.count("processed_articles"), 0)
        self.assertEqual(self.count("stories"), 0)
        self.assertTrue(self.path.exists())

    def test_ambiguous_commit_does_not_retry_or_delete_the_manifest(self):
        mock = HermeticStagingTransport(
            self.conn, fail_on_commit_timeout=True,
        )
        client, guard, budget = self.make_client(transport=mock)
        with self.assertRaisesRegex(MockHranaDenied, "TRANSPORT_OR_PROTOCOL_UNKNOWN"):
            self.dispatch_live02(client)
        self.assertTrue(guard.unknown)
        self.assertTrue(guard.halted)
        self.assertEqual(budget.request_count, 2)
        with self.assertRaisesRegex(MockHranaDenied, "SESSION_HALTED"):
            self.dispatch_live02(client)
        self.assertEqual(budget.request_count, 2)
        # A COMMIT timeout cannot prove zero rows; never infer CLEAN here.
        self.assertTrue(self.path.exists())


if __name__ == "__main__":
    unittest.main()
