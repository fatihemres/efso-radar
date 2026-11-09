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
from efso_radar.staging.offline_operator import (
    _INSERT_ARTICLE,
    _INSERT_LOCK,
    _INSERT_RECOVERY,
    _INSERT_STORY,
)
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
        self.assertEqual(self.transport._inner.request_count, 3)
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
        self.assertEqual(self.transport._inner.request_count, 0)
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

    def test_untrusted_intents_require_exact_values_for_all_mutation_columns(self):
        """A caller cannot whitelist its own altered URL, JSON, time or lease."""
        stamp = "2026-10-25T15:00:00Z"
        inventory = create_default_manifest_entities(RUN)
        lock = next(ent for ent in inventory if ent.table == "run_locks")
        recovery = next(
            ent for ent in inventory if ent.table == "durable_recovery_state"
        )
        lock_intent = AuthorizedMockIntent(
            _INSERT_LOCK,
            (
                lock.pk_value, lock.expected_attributes["worker_ids"][0],
                stamp, "2026-10-25T15:05:00Z", stamp,
            ),
        )
        recovery_intent = AuthorizedMockIntent(
            _INSERT_RECOVERY,
            (
                recovery.pk_value, recovery.expected_attributes["stage"],
                recovery.expected_attributes["worker_id"],
                json.dumps({"cursor": 100}), stamp, stamp,
            ),
        )
        inputs = (
            (
                self.article,
                {
                    1: "https://unexpected.invalid/",  # URL was not checked
                    2: "FAKE_ARTICLE",
                    3: "unknown-source",
                    4: "2099-01-01T00:00:00Z",
                    5: "2099-01-01T00:00:00Z",
                },
            ),
            (
                self.story,
                {
                    1: "FAKE_STORY",
                    2: "other-category",
                    3: "2099-01-01T00:00:00Z",
                    4: "2099-01-01T00:00:00Z",
                    5: json.dumps(["other-article-id"]),
                    6: 2,
                    7: 0,
                },
            ),
            (
                lock_intent,
                {
                    1: "foreign-worker",
                    2: "2099-01-01T00:00:00Z",
                    3: "2099-01-01T00:00:00Z",
                    4: "2099-01-01T00:00:00Z",
                },
            ),
            (
                recovery_intent,
                {
                    1: "other-stage",
                    2: "other-worker",
                    3: json.dumps({"cursor": 101}),
                    4: "2099-01-01T00:00:00Z",
                    5: "2099-01-01T00:00:00Z",
                },
            ),
        )
        for baseline, replacements in inputs:
            # These baseline intents must be accepted without dispatch.
            valid = self.make_transport(intents=(baseline,))
            self.assertEqual(valid._requests, 0)
            self.assertEqual(valid._inner.request_count, 0)
            for index, mutated in replacements.items():
                with self.subTest(sql=baseline.sql, arg=index):
                    args = list(baseline.args)
                    args[index] = mutated
                    forged = AuthorizedMockIntent(baseline.sql, tuple(args))
                    with self.assertRaisesRegex(ValueError, "Noncanonical"):
                        self.make_transport(intents=(forged,))
                    self.assertEqual(self.count("processed_articles"), 0)
                    self.assertEqual(self.count("stories"), 0)
        self.assertEqual(self.transport._inner.request_count, 0)

    def test_rollback_article_urls_follow_only_canonical_mock_fixture(self):
        """Preserve known rollback/duplicate fixture URLs, deny arbitrary host."""
        entity = next(
            ent for ent in create_default_manifest_entities(RUN)
            if ent.pk_value == f"synth_art_{RUN}_02_rollback"
        )
        for suffix in ("02", "02_duplicate"):
            valid = AuthorizedMockIntent(
                _INSERT_ARTICLE,
                (
                    entity.pk_value,
                    f"https://example.com/synth/{RUN}/{suffix}",
                    entity.expected_attributes["title"],
                    entity.expected_attributes["source"],
                    "2026-10-25T15:00:00Z",
                    "2026-10-25T15:00:00Z",
                ),
            )
            self.assertEqual(self.make_transport(intents=(valid,))._requests, 0)
        invalid = AuthorizedMockIntent(
            _INSERT_ARTICLE,
            (
                entity.pk_value, "https://example.com/synth/malicious",
                entity.expected_attributes["title"],
                entity.expected_attributes["source"],
                "2026-10-25T15:00:00Z",
                "2026-10-25T15:00:00Z",
            ),
        )
        with self.assertRaisesRegex(ValueError, "Noncanonical"):
            self.make_transport(intents=(invalid,))

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


    def test_response_base_url_hardening_rejects_unsafe_destinations_and_halts(self):
        """Responses with foreign, attacker, IP, loopback, or malformed base_url fail closed and halt."""
        unsafe_cases = [
            ("foreign Turso host", "https://foreign-db.turso.io/v2/pipeline"),
            ("foreign AWS region Turso host", "https://efso-radar-staging-fatihemres.aws-us-east-1.turso.io/v2/pipeline"),
            ("attacker controlled HTTPS host", "https://attacker.evil.com/v2/pipeline"),
            ("loopback plaintext HTTP", "http://127.0.0.1:8080/v2/pipeline"),
            ("loopback HTTPS with IP", "https://127.0.0.1/v2/pipeline"),
            ("localhost plaintext HTTP", "http://localhost/v2/pipeline"),
            ("embedded credentials", "https://user:pass@efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline"),
            ("non-standard port", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io:8080/v2/pipeline"),
            ("unexpected path", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/admin/drop"),
            ("query string", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline?token=leak"),
            ("fragment", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline#anchor"),
            ("path params semicolon unexpected", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline;unexpected"),
            ("CRLF injected into allowlisted URL", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io\r\n/v2/pipeline"),
            ("TAB injected into allowlisted URL", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io\t/v2/pipeline"),
            ("CR only injected into allowlisted URL", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io\r/v2/pipeline"),
            ("LF only injected into allowlisted URL", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io\n/v2/pipeline"),
            ("leading space in allowlisted URL", " https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline"),
            ("trailing space in allowlisted URL", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline "),
            ("embedded space in allowlisted URL", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io /v2/pipeline"),
            ("control character null byte in allowlisted URL", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io\x00/v2/pipeline"),
            ("empty string", ""),
            ("whitespace only", "   "),
            ("integer type", 12345),
            ("dict type", {"url": "https://evil.com"}),
            ("list type", ["https://evil.com"]),
        ]

        for label, unsafe_val in unsafe_cases:
            with self.subTest(label=label):
                guard = self.make_transport()
                client = TursoHTTPClient(
                    database_url=EXACT_STAGING_DESTINATION,
                    transport=guard,
                )
                self.addCleanup(client.close)

                # Return response containing unsafe base_url
                fake_response = httpx.Response(
                    200,
                    json={
                        "baton": "unsafe-baton-xyz",
                        "base_url": unsafe_val,
                        "results": [
                            {"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}},
                            {"type": "ok"},
                        ],
                    },
                )
                with patch.object(guard._inner, "handle_request", return_value=fake_response) as backend:
                    with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_UNSAFE_BASE_URL"):
                        client.execute(self.article.sql, list(self.article.args))

                    self.assertTrue(guard.halted, f"Guard must be halted for {label}")
                    self.assertTrue(guard.unknown, f"Guard outcome must be unknown for {label}")
                    self.assertEqual(backend.call_count, 1)

                    # Ensure any subsequent baton-bearing dispatch is permanently blocked before network dispatch
                    with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_SESSION_HALTED"):
                        client.execute(self.read.sql, list(self.read.args))

                    # Backend was not called again
                    self.assertEqual(backend.call_count, 1)

    def test_response_base_url_exact_staging_endpoint_is_accepted(self):
        """Responses with None or exact allowlisted staging base_url succeed without altering endpoint."""
        for label, safe_val in [
            ("none (standard null)", None),
            ("exact pipeline endpoint", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline"),
            ("exact pipeline endpoint with slash", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/v2/pipeline/"),
            ("exact host root", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io"),
            ("exact host root with slash", "https://efso-radar-staging-fatihemres.aws-eu-north-1.turso.io/"),
        ]:
            with self.subTest(label=label):
                guard = self.make_transport()
                client = TursoHTTPClient(
                    database_url=EXACT_STAGING_DESTINATION,
                    transport=guard,
                )
                self.addCleanup(client.close)

                fake_response = httpx.Response(
                    200,
                    json={
                        "baton": None,
                        "base_url": safe_val,
                        "results": [
                            {"type": "ok", "response": {"type": "execute", "result": {"cols": [{"name": "found"}], "rows": [[{"type": "integer", "value": 1}]]}}},
                            {"type": "ok"},
                        ],
                    },
                )
                with patch.object(guard._inner, "handle_request", return_value=fake_response):
                    res = client.execute(self.read.sql, list(self.read.args))
                    self.assertEqual(res.fetchone()["found"], 1)
                    self.assertFalse(guard.halted)
                    self.assertFalse(guard.unknown)

    def test_http_401_and_403_responses_halt_fail_closed_and_block_subsequent_dispatch(self):
        """HTTP 401 Unauthorized and 403 Forbidden trigger fail-closed halt without leaking token or SQL."""
        secret_token = "secret-token-test-xyz"
        for status_code in (401, 403):
            with self.subTest(status_code=status_code):
                guard = self.make_transport()
                client = TursoHTTPClient(
                    database_url=EXACT_STAGING_DESTINATION,
                    auth_token=secret_token,
                    transport=guard,
                )
                self.addCleanup(client.close)

                error_resp = httpx.Response(
                    status_code,
                    json={"message": f"HTTP {status_code} authentication/authorization error"},
                )
                with patch.object(guard._inner, "handle_request", return_value=error_resp) as backend:
                    with self.assertRaises(MockHranaDenied) as ctx:
                        client.execute(self.article.sql, list(self.article.args))

                    self.assertIn("MOCK_HRANA_DENIED_NON_200_UNKNOWN", str(ctx.exception))
                    self.assertNotIn(secret_token, str(ctx.exception))
                    self.assertNotIn(self.article_id, str(ctx.exception))
                    self.assertTrue(guard.halted)
                    self.assertTrue(guard.unknown)
                    self.assertEqual(backend.call_count, 1)

                    # No further SQL dispatch permitted
                    with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_SESSION_HALTED"):
                        client.execute(self.read.sql, list(self.read.args))
                    self.assertEqual(backend.call_count, 1)

    def test_non_transactional_pipeline_step_error_does_not_rollback_and_never_treated_as_success(self):
        """In a non-transactional pipeline, statement 1 failure does not prevent statement 2 execution.

        Upstream Hrana v2 HTTP semantics: individual pipeline statements execute sequentially without
        implicit transaction encapsulation. A statement error produces an error result in the results
        array, but subsequent statements in the batch still execute and autocommit.

        Verifies:
        - Application never interprets the pipeline as successful merely because HTTP status is 200.
        - Application does not assume automatic rollback occurred (statement 2 persists in SQLite).
        - Guard rejects the non-ok response, marks UNKNOWN, latches HALT, and prohibits retries.
        """
        test_conn = sqlite3.connect(":memory:")
        self.addCleanup(test_conn.close)
        for stmt in get_all_init_statements():
            test_conn.execute(stmt)
        test_conn.commit()

        # HermeticStagingTransport configured to fail on statement 1 (fail_on_stmt_idx=1)
        raw_mock = HermeticStagingTransport(test_conn, fail_on_stmt_idx=1)
        budget = BudgetTrackingTransport(raw_mock)
        guard = GuardedHermeticHranaTransport(
            inner=budget,
            seal=self.seal,
            approved_intents=(self.article, self.story),
        )
        client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            transport=guard,
        )
        self.addCleanup(client.close)

        # Dispatch non-transactional pipeline with statement 0 (article) and statement 1 (story)
        with self.assertRaises(MockHranaDenied) as ctx:
            client.execute_pipeline([
                (self.article.sql, list(self.article.args)),
                (self.story.sql, list(self.story.args)),
            ])

        # 1. Transport boundary never interpreted the pipeline as successful (raised MockHranaDenied)
        self.assertIn("MOCK_HRANA_DENIED_RESPONSE_ERROR", str(ctx.exception))
        self.assertTrue(guard.halted)
        self.assertTrue(guard.unknown)

        # 2. Server executed statement 2 despite statement 1 failure (no automatic rollback)
        story_count = test_conn.execute("SELECT COUNT(*) FROM stories").fetchone()[0]
        self.assertEqual(story_count, 1, "Non-transactional pipeline executed subsequent statement without rollback")
        art_count = test_conn.execute("SELECT COUNT(*) FROM processed_articles").fetchone()[0]
        self.assertEqual(art_count, 0, "First statement failed with error in mock transport")

        # 3. Guard permanently halted: blocks retry or subsequent queries
        with self.assertRaisesRegex(MockHranaDenied, "MOCK_HRANA_DENIED_SESSION_HALTED"):
            client.execute(self.read.sql, list(self.read.args))

        # 4. Manifest preserved on disk: cannot be deleted or treated as clean
        self.assertTrue(self.path.exists())


if __name__ == "__main__":
    unittest.main()
