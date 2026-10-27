"""Hermetic contract tests for reusable but non-dispatching SQL intent data."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from efso_radar.staging.manifest_seal import (
    ManifestSealViolation,
    OfflineManifestSeal,
)
from efso_radar.staging.offline_operator import _MUTATING_TEMPLATES, _norm
from efso_radar.staging.offline_write_intent import (
    OfflineWriteIntentBuilder,
    PlannedSql,
)
from efso_radar.staging.runner import (
    PerRunOwnershipManifest,
    create_default_manifest_entities,
)


RUN_ID = "run_20261026T230000Z_aaaaaaaaaaaa"


class TestOfflineWriteIntent(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.manifest = PerRunOwnershipManifest(
            run_id=RUN_ID,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=create_default_manifest_entities(RUN_ID),
        )
        self.path = self.manifest.save_atomic(Path(self.temp.name))
        self.seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        self.builder = OfflineWriteIntentBuilder(self.seal)

    def test_stage02_prepares_exact_known_parameterized_pair_without_dispatch(self):
        with patch.object(
            httpx.HTTPTransport, "handle_request",
            side_effect=AssertionError("Real HTTP is prohibited"),
        ):
            pair = self.builder.live02_atomic_pair()
        self.assertEqual(len(pair), 2)
        self.assertEqual(
            [p.args[0] for p in pair],
            [f"synth_art_{RUN_ID}_01", f"synth_story_{RUN_ID}_01"],
        )
        self.assertEqual(pair[0].args[1], f"https://example.com/synth/{RUN_ID}/01")
        self.assertEqual(pair[0].args[3], "staging_synth_test")
        self.assertEqual(json.loads(pair[1].args[5]), [pair[0].args[0]])
        self.assertEqual(pair[1].args[6:], (1, 1))
        for item in pair:
            table, operation, expected_count = _MUTATING_TEMPLATES[_norm(item.sql)]
            self.assertEqual(operation, "insert")
            self.assertEqual(len(item.args), expected_count)
            self.assertIn(table, ("processed_articles", "stories"))
            self.assertEqual(item.sql.count("?"), expected_count)

    def test_cleanup_uses_all_five_registered_identities_and_conditional_markers(self):
        items = self.builder.conditional_cleanup()
        self.assertEqual(len(items), 5)
        self.assertEqual(
            {item.args[0] for item in items},
            {ent.pk_value for ent in create_default_manifest_entities(RUN_ID)},
        )
        for item in items:
            table, operation, expected_count = _MUTATING_TEMPLATES[_norm(item.sql)]
            self.assertEqual(operation, "delete")
            self.assertEqual(len(item.args), expected_count)
            self.assertEqual(item.sql.count("?"), expected_count)
            self.assertIn("WHERE", item.sql)
            self.assertNotIn("DELETE FROM notifications", item.sql)
        self.assertTrue(self.path.exists(), "Preparing cleanup never unlinks manifest")
        self.assertEqual(
            [item.sql.split(" ")[2] for item in items][-3:],
            ["stories", "run_locks", "durable_recovery_state"],
        )

    def test_mutation_of_file_after_sealing_refuses_all_new_intents(self):
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        raw["created_at"] = "2099-01-01T00:00:00Z"
        self.path.write_text(json.dumps(raw), encoding="utf-8")
        for action in (self.builder.live02_atomic_pair, self.builder.conditional_cleanup):
            with self.subTest(action=action.__name__), self.assertRaises(
                ManifestSealViolation
            ):
                action()

    def test_removed_manifest_refuses_to_prepare_any_new_sql(self):
        self.path.unlink()
        with self.assertRaises(ManifestSealViolation):
            self.builder.live02_atomic_pair()
        with self.assertRaises(ManifestSealViolation):
            self.builder.conditional_cleanup()

    def test_unregistered_record_or_table_cannot_be_generated(self):
        with self.assertRaisesRegex(PermissionError, "UNREGISTERED_TARGET"):
            self.builder._registered("notifications", "private")
        with self.assertRaisesRegex(PermissionError, "UNREGISTERED_TARGET"):
            self.builder._registered("processed_articles", f"synth_art_{RUN_ID}_999")

    def test_result_is_immutable_and_not_an_executable_client(self):
        first = self.builder.live02_atomic_pair()[0]
        self.assertIs(type(first), PlannedSql)
        with self.assertRaises(FrozenInstanceError):
            first.sql = "DELETE FROM notifications"
        self.assertFalse(hasattr(first, "execute"))
        self.assertFalse(hasattr(self.builder, "execute"))
        self.assertFalse(hasattr(self.builder, "connect"))
        self.assertFalse(hasattr(self.builder, "token"))

    def test_builder_rejects_unsealed_objects(self):
        for invalid in (None, object(), "a" * 64):
            with self.subTest(type=type(invalid).__name__), self.assertRaises(
                TypeError
            ):
                OfflineWriteIntentBuilder(invalid)


if __name__ == "__main__":
    unittest.main()
