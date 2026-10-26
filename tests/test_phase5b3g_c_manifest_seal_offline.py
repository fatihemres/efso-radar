"""Offline-only regression tests for future G-C per-write manifest ownership checks."""
from __future__ import annotations

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
    MAX_MANIFEST_BYTES,
)
from efso_radar.staging.runner import (
    PerRunOwnershipManifest,
    create_default_manifest_entities,
)


RUN_ID = "run_20261026T182000Z_a1b2c3d4e5f6"


class TestOfflineManifestSeal(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = PerRunOwnershipManifest(
            run_id=RUN_ID,
            created_at=datetime.now(timezone.utc).isoformat(),
            registered_entities=create_default_manifest_entities(RUN_ID),
        )
        self.path = self.manifest.save_atomic(self.root)

    def test_seal_checks_exact_registered_entities_without_network(self):
        with patch.object(
            httpx.HTTPTransport,
            "handle_request",
            side_effect=AssertionError("No network requests permitted"),
        ):
            seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
            seal.assert_unchanged()
            for entity in create_default_manifest_entities(RUN_ID):
                seal.require_registered_identity(
                    entity.table, entity.pk_column, entity.pk_value
                )
        self.assertEqual(seal.run_id, RUN_ID)
        self.assertEqual(len(seal.sha256), 64)

    def test_rejects_unregistered_table_and_other_run_id(self):
        seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        for table, pk_column, value in (
            ("telegram_delivery_records", "id", "not-allowed"),
            ("processed_articles", "article_id", "another-run-id"),
            ("stories", "id", f"synth_story_{RUN_ID}_01"),
        ):
            with self.subTest(table=table, value=value):
                with self.assertRaises(ManifestSealViolation):
                    seal.require_registered_identity(table, pk_column, value)

    def test_rejects_modified_metadata_even_if_canonical_inventory_still_valid(self):
        seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        data = json.loads(self.path.read_text(encoding="utf-8"))
        data["created_at"] = "2099-01-01T00:00:00Z"
        self.path.write_text(json.dumps(data), encoding="utf-8")
        # The canonical entity inventory still matches, but the seal must fail.
        with self.assertRaisesRegex(ManifestSealViolation, "differs"):
            seal.assert_unchanged()

    def test_rejects_tampered_ownership_marker(self):
        seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        data = json.loads(self.path.read_text(encoding="utf-8"))
        data["registered_entities"][0]["expected_attributes"]["source"] = "not-owned"
        self.path.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaises(ManifestSealViolation):
            seal.require_registered_identity(
                "processed_articles", "article_id", f"synth_art_{RUN_ID}_01"
            )

    def test_missing_or_replaced_manifest_fails_closed(self):
        seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        self.path.unlink()
        with self.assertRaises(ManifestSealViolation):
            seal.assert_unchanged()
        with self.assertRaises(ManifestSealViolation):
            OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        self.path.write_text("not a manifest", encoding="utf-8")
        with self.assertRaises(ManifestSealViolation):
            seal.assert_unchanged()

    def test_no_arbitrary_file_read_or_unexpected_filename(self):
        unrelated_file = self.root / "credentials.env"
        unrelated_file.write_text("PRIVATE-NO-READ", encoding="utf-8")
        with patch.object(Path, "read_bytes", side_effect=AssertionError("Must not read")):
            with self.assertRaises(ManifestSealViolation):
                OfflineManifestSeal.from_persisted(unrelated_file, RUN_ID)
        with self.assertRaises(ManifestSealViolation):
            OfflineManifestSeal.from_persisted(self.path, "run_wrong")

    def test_rejects_oversized_manifest_without_inspecting_contents(self):
        seal = OfflineManifestSeal.from_persisted(self.path, RUN_ID)
        with open(self.path, "wb") as f:
            f.write(b"x" * (MAX_MANIFEST_BYTES + 1))
        with self.assertRaises(ManifestSealViolation):
            seal.assert_unchanged()

    def test_new_file_does_not_implicitly_authorize_or_forge_manifest(self):
        # A pure local manifest-checking utility cannot grant write permission;
        # the existing offline runner still guards real HTTP transports.
        from efso_radar.staging.runner import StagingSessionRunner, EXACT_STAGING_DESTINATION
        from efso_radar.repositories.turso_client import TursoHTTPClient
        live = TursoHTTPClient(
            database_url=EXACT_STAGING_DESTINATION,
            auth_token="mock",
            transport=httpx.HTTPTransport(retries=0),
        )
        try:
            with self.assertRaises(PermissionError):
                StagingSessionRunner(client=live)
        finally:
            live.close()


if __name__ == "__main__":
    unittest.main()
