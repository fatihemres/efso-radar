"""Hermetic proof for the standalone, non-live G-C sealed operator candidate."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import httpx

from efso_radar.repositories.schema import get_all_init_statements
from efso_radar.repositories.turso_client import TursoHTTPClient
from efso_radar.staging.manifest_seal import OfflineManifestSeal
from efso_radar.staging.offline_operator import (
    SealedHermeticStagingRunner,
    _INSERT_ARTICLE,
    _INSERT_STORY,
    _INSERT_LOCK,
    _INSERT_RECOVERY,
)
from efso_radar.staging.runner import (
    AuditOutcomeState,
    BudgetTrackingTransport,
    EXACT_STAGING_DESTINATION,
    HermeticStagingTransport,
    SessionStatus,
)


class TestSealedHermeticOperator(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        for sql in get_all_init_statements():
            self.db.execute(sql)
        self.db.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, applied_at, description) "
            "VALUES (6, '2026-10-26T16:00:00Z', 'Schema v6')"
        )
        self.db.commit()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.transport = HermeticStagingTransport(self.db)
        self.budget = BudgetTrackingTransport(self.transport)
        self.client = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="synthetic-offline-only",
            transport=self.budget,
        )
        self.addCleanup(self.client.close)
        self.runner = SealedHermeticStagingRunner(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=Path(self.temp.name),
        )

    def _prepared(self):
        self.runner._create_and_persist_manifest()
        self.runner._run_live_01()
        self.assertTrue(self.runner._preflight_accepted)
        self.assertIsNotNone(self.runner._seal)

    def _article_params(self):
        run_id = self.runner.run_id
        return [
            f"synth_art_{run_id}_01",
            f"https://example.com/synth/{run_id}/01",
            f"SYNTH_ARTICLE_{run_id}_01",
            "staging_synth_test",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
        ]

    def _assert_no_new_requests(self, action):
        count = self.budget.request_count
        with patch.object(
            self.transport, "handle_request", wraps=self.transport.handle_request
        ) as spy:
            with self.assertRaises(PermissionError):
                action()
            self.assertEqual(spy.call_count, 0)
        self.assertEqual(self.budget.request_count, count)
        self.assertTrue(self.runner.halt_before_cleanup)
        self.assertEqual(self.runner.audit_state, AuditOutcomeState.UNKNOWN)

    def test_nominal_eight_stages_all_run_and_clean_without_network(self):
        with patch.object(
            httpx.HTTPTransport, "handle_request",
            side_effect=AssertionError("Real HTTP prohibited"),
        ):
            report = self.runner.run()
        self.assertEqual(report.status, SessionStatus.PASSED, report.errors)
        self.assertEqual(report.audit_state, AuditOutcomeState.CLEAN)
        self.assertEqual(len(report.stages), 8)
        self.assertEqual(report.http_requests_total, 78)
        self.assertLess(report.http_requests_total, 100)
        self.assertFalse(report.manifest_preserved)
        self.assertFalse(Path(report.manifest_path).exists())
        for table in ("processed_articles", "stories", "run_locks", "durable_recovery_state"):
            self.assertEqual(self.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)

    def test_preflight_is_mandatory_for_mutations(self):
        self._assert_no_new_requests(
            lambda: self.runner._execute(_INSERT_ARTICLE, self._article_params())
        )
        self.assertEqual(self.budget.request_count, 0)

    def test_unlisted_sql_and_read_only_table_mutations_denied(self):
        # Exercise every invalid template through the pure pre-dispatch guard.
        # Public _execute permanently halts on the first denial (tested below).
        self._prepared()
        statements = [
            ("DELETE FROM notifications WHERE id = ?", ["synthetic"]),
            ("DROP TABLE stories", []),
            ("UPDATE stories SET is_active = 0", []),
            (_INSERT_ARTICLE + "; DROP TABLE stories", self._article_params()),
        ]
        for sql, params in statements:
            with self.subTest(sql=sql):
                self._assert_no_new_requests(
                    lambda sql=sql, params=params: self.runner._guard_mutation(sql, params)
                )

    def test_unregistered_synthetic_primary_key_rejected(self):
        self._prepared()
        params = self._article_params()
        params[0] += "_forged"
        self._assert_no_new_requests(lambda: self.runner._execute(_INSERT_ARTICLE, params))

    def test_marker_mismatch_rejected_without_dispatch(self):
        self._prepared()
        params = self._article_params()
        params[3] = "real_source"
        self._assert_no_new_requests(lambda: self.runner._execute(_INSERT_ARTICLE, params))

    def test_missing_or_invalid_parameters_rejected(self):
        self._prepared()
        for params in (None, [], self._article_params()[:-1], tuple(self._article_params())):
            with self.subTest(params_type=type(params).__name__):
                self._assert_no_new_requests(
                    lambda params=params: self.runner._guard_mutation(_INSERT_ARTICLE, params)
                )

    def test_non_scalar_parameters_fail_closed_with_unknown(self):
        self._prepared()
        for bad in ([], {"unexpected": "nested"}, None, object()):
            with self.subTest(type=type(bad).__name__):
                params = self._article_params()
                params[1] = bad
                self._assert_no_new_requests(
                    lambda params=params: self.runner._guard_mutation(_INSERT_ARTICLE, params)
                )

    def test_ambiguous_reconciliation_preserves_manifest(self):
        other = SealedHermeticStagingRunner(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=Path(self.temp.name),
            simulate_ambiguous_inspection_failure=True,
        )
        report = other.run()
        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertTrue(other.halt_before_cleanup)
        self.assertEqual(len([s for s in report.stages if s.stage_id == "LIVE-08"]), 0)

    def test_ownership_mismatch_aborts_cleanup_preserving_manifest(self):
        other = SealedHermeticStagingRunner(
            client=self.client,
            destination_url=EXACT_STAGING_DESTINATION,
            manifest_dir=Path(self.temp.name),
            simulate_ownership_mismatch=True,
        )
        report = other.run()
        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.OWNERSHIP_MISMATCH_HALT)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())

    def test_only_registered_run_scoped_read_templates_are_accepted(self):
        self._prepared()
        run_id = self.runner.run_id
        invalid = (
            ("SELECT * FROM notifications WHERE id = ?", ["private"]),
            ("SELECT * FROM stories WHERE story_id = ?", ["another-run"]),
            ("SELECT 1 AS found FROM stories WHERE story_id = ? AND 1 = 1",
             [f"synth_story_{run_id}_01"]),
            ("SELECT 1; DELETE FROM stories", [f"synth_story_{run_id}_01"]),
            ("SELECT * FROM sqlite_master", []),
            ("SELECT 1 AS found FROM processed_articles WHERE article_id = ?", None),
        )
        for sql, args in invalid:
            with self.subTest(sql=sql):
                self._assert_no_new_requests(
                    lambda sql=sql, args=args: self.runner._guard_read(sql, args)
                )

    def test_article_pk_url_pair_and_all_timestamp_fields_bound(self):
        self._prepared()
        examples = []
        crossed = self._article_params()
        crossed[1] = f"https://example.com/synth/{self.runner.run_id}/02"
        examples.append(crossed)
        changed_stamp = self._article_params()
        changed_stamp[5] = "2099-01-01T00:00:00Z"
        examples.append(changed_stamp)
        huge = self._article_params()
        huge[1] = "x" * 1000
        examples.append(huge)
        for args in examples:
            with self.subTest(args=args[1]):
                self._assert_no_new_requests(
                    lambda args=args: self.runner._guard_mutation(_INSERT_ARTICLE, args)
                )

    def test_story_dates_counts_and_payload_bound(self):
        self._prepared()
        run_id = self.runner.run_id
        original = [
            f"synth_story_{run_id}_01",
            f"SYNTH_STORY_{run_id}_01",
            "synth_test",
            "2026-10-25T15:00:00Z",
            "2026-10-25T15:00:00Z",
            json.dumps([f"synth_art_{run_id}_01"]),
            1, 1,
        ]
        for index, bad in ((3, "2099-01-01T00:00:00Z"), (6, 999),
                           (7, True), (5, json.dumps(["wrong-article"]))):
            with self.subTest(index=index):
                args = original[:]
                args[index] = bad
                self._assert_no_new_requests(
                    lambda args=args: self.runner._guard_mutation(_INSERT_STORY, args)
                )

    def test_lock_cas_and_recovery_timestamps_and_payload_bound(self):
        self._prepared()
        run_id = self.runner.run_id
        lock_args = [
            f"synth_lock_{run_id}", f"runner_alpha_{run_id}",
            "2026-10-25T15:00:00Z", "2026-10-25T15:05:00Z",
            "2026-10-25T15:00:00Z",
        ]
        lock_args[3] = "2099-01-01T00:00:00Z"
        self._assert_no_new_requests(
            lambda: self.runner._execute(_INSERT_LOCK, lock_args)
        )
        recovery_args = [
            f"synth_stage_{run_id}::current",
            f"synth_stage_{run_id}", f"synth_worker_{run_id}",
            json.dumps({"cursor": 1000}),
            "2026-10-25T15:00:00Z", "2026-10-25T15:00:00Z",
        ]
        self._assert_no_new_requests(
            lambda: self.runner._execute(_INSERT_RECOVERY, recovery_args)
        )

    def test_halted_session_rejects_later_valid_reads_and_writes(self):
        self._prepared()
        invalid = self._article_params()
        invalid[3] = "wrong_owner"
        with self.assertRaisesRegex(PermissionError, "OWNERSHIP_MISMATCH"):
            self.runner._execute(_INSERT_ARTICLE, invalid)
        self.assertTrue(self.runner.halt_before_cleanup)
        before = self.budget.request_count
        with self.assertRaisesRegex(PermissionError, "SESSION_HALTED"):
            self.runner._execute(_INSERT_ARTICLE, self._article_params())
        with self.assertRaisesRegex(PermissionError, "SESSION_HALTED"):
            self.runner._execute(
                "SELECT title, source FROM processed_articles WHERE article_id = ?",
                [self._article_params()[0]],
            )
        with self.assertRaisesRegex(PermissionError, "SESSION_HALTED"):
            self.runner._execute_transaction(
                [(_INSERT_ARTICLE, self._article_params())]
            )
        self.assertEqual(self.budget.request_count, before)
        self.assertEqual(self.runner.audit_state, AuditOutcomeState.UNKNOWN)

    def test_unexpected_manifest_error_fails_closed_without_leak(self):
        self._prepared()
        assert self.runner._seal is not None
        before = self.budget.request_count
        with patch.object(
            OfflineManifestSeal,
            "require_registered_identity",
            side_effect=RuntimeError("SENSITIVE_RAW_MESSAGE"),
        ):
            with self.assertRaisesRegex(
                PermissionError, "OFFLINE_SEALED_GATE_MANIFEST_INVALID"
            ) as ctx:
                self.runner._execute(_INSERT_ARTICLE, self._article_params())
        self.assertNotIn("SENSITIVE_RAW_MESSAGE", str(ctx.exception))
        self.assertEqual(self.budget.request_count, before)
        self.assertTrue(self.runner.halt_before_cleanup)
        self.assertEqual(self.runner.audit_state, AuditOutcomeState.UNKNOWN)

    def test_registered_read_still_allowed_after_successful_preflight(self):
        self._prepared()
        before = self.budget.request_count
        rows = self.runner._execute(
            "SELECT title, source FROM processed_articles WHERE article_id = ?",
            [self._article_params()[0]],
        ).fetchall()
        self.assertEqual(rows, [])
        self.assertEqual(self.budget.request_count, before + 1)
        self.assertFalse(self.runner.halt_before_cleanup)

    def test_whole_transaction_validated_before_any_dispatch(self):
        self._prepared()
        run_id = self.runner.run_id
        article = (_INSERT_ARTICLE, self._article_params())
        story = (
            _INSERT_STORY,
            [
                f"synth_story_{run_id}_01",
                f"SYNTH_STORY_{run_id}_01",
                "other_category",   # tampered second statement
                "2026-10-25T15:00:00Z",
                "2026-10-25T15:00:00Z",
                json.dumps([f"synth_art_{run_id}_01"]),
                1, 1,
            ],
        )
        self._assert_no_new_requests(
            lambda: self.runner._execute_transaction([article, story])
        )
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM processed_articles").fetchone()[0], 0)

    def test_manifest_tampering_after_preflight_retains_file_and_refuses_cleanup(self):
        original = self.runner._run_live_02

        def corrupt_then_dispatch():
            path = self.runner.manifest_path
            assert path is not None
            data = json.loads(path.read_text(encoding="utf-8"))
            data["created_at"] = "2099-12-31T00:00:00Z"
            path.write_text(json.dumps(data), encoding="utf-8")
            return original()

        with patch.object(self.runner, "_run_live_02", side_effect=corrupt_then_dispatch):
            report = self.runner.run()
        self.assertEqual(report.status, SessionStatus.FAILED)
        self.assertEqual(report.audit_state, AuditOutcomeState.UNKNOWN)
        self.assertTrue(report.manifest_preserved)
        self.assertTrue(Path(report.manifest_path).exists())
        self.assertIn("MANIFEST_INVALID", " ".join(report.errors))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM processed_articles").fetchone()[0], 0)
        self.assertEqual([s.stage_id for s in report.stages], ["LIVE-01"])

    def test_manifest_missing_after_preflight_fails_closed(self):
        self._prepared()
        assert self.runner.manifest_path is not None
        self.runner.manifest_path.unlink()
        self._assert_no_new_requests(
            lambda: self.runner._execute(_INSERT_ARTICLE, self._article_params())
        )

    def test_live_http_transport_still_rejected_without_dispatch(self):
        network_transport = httpx.HTTPTransport(retries=0)
        live = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="not-a-real-secret",
            transport=network_transport,
        )
        try:
            with patch.object(
                network_transport, "handle_request",
                side_effect=AssertionError("network access forbidden"),
            ):
                with self.assertRaises(PermissionError):
                    SealedHermeticStagingRunner(client=live)
        finally:
            live.close()


if __name__ == "__main__":
    unittest.main()
