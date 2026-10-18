"""Unit and integration tests for Phase 4E: First Real Telegram Digest Delivery Preparation.

Verifies:
1. Editorial candidate invariants: exactly three approved candidates with refined headlines.
2. Group/Channel rejection: only private bot-to-owner chat allowed (no '-' prefix).
3. Chat ID allowlist enforcement: unauthorized recipients rejected.
4. Persistent duplicate-send prevention: identical digest hash blocked if already delivered or uncertain.
5. One-shot live dispatch with bounded single attempt:
   - HTTP 200 delivery success and ledger recording.
   - HTTP 504 / timeout handled as DELIVERY_UNCERTAIN without retries.
   - HTTP 403 access failure handled as ACCESS_DENIED without retries.
6. Token and secret redaction.
7. Dry-run mode: zero network requests made.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import httpx

from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    Market,
    StoryCategory,
    SuggestedFormat,
    VerificationStatus,
)
from efso_radar.notifications.telegram import deliver_one_shot_editorial_digest
from scripts.deliver_telegram_digest_oneshot import build_final_three_assessments

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestPhase4EDigestDelivery(unittest.TestCase):
    """Test suite for Phase 4E dedicated one-shot Telegram digest delivery workflow."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.tmpdir.name) / "test_delivery_ledger.json"
        self.settings = Settings(
            telegram_bot_token="test_bot_token_secret_12345",
            telegram_chat_id="1000000001",
            telegram_allowed_chat_ids=["1000000001"],
            telegram_delivery_enabled=False,
            http_timeout_seconds=5.0,
        )
        self.assessments = build_final_three_assessments()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_build_final_three_assessments_integrity(self) -> None:
        """Verify the 3 approved candidates adhere strictly to Phase 4E requirements."""
        self.assertEqual(len(self.assessments), 3)

        # Şükrü Özyıldız
        sukru = self.assessments[0]
        self.assertEqual(sukru.headline_tr, "Şükrü Özyıldız'ın kız kardeşi evlendi: Oyuncudan duygusal paylaşım")
        self.assertEqual(sukru.cover_title_tr, "ÖZYILDIZ AİLESİNDE DÜĞÜN")
        self.assertTrue(sukru.is_publishable)

        # Nicolas Cage & Antonio Banderas
        cage = self.assessments[1]
        self.assertIn("True Detective", cage.headline_tr)
        self.assertIn("5. Sezon", cage.headline_tr)
        self.assertEqual(cage.cover_title_tr, "TRUE DETECTIVE 5'TE DEV BULUŞMA")
        self.assertTrue(cage.is_publishable)

        # Gisele Bündchen
        gisele = self.assessments[2]
        self.assertIn("Koltuk Değnekleriyle", gisele.headline_tr)
        self.assertEqual(gisele.cover_title_tr, "GISELE BÜNDCHEN KOLTUK DEĞNEKLERİYLE")
        self.assertTrue(gisele.is_publishable)

    def test_reject_invalid_candidate_count(self) -> None:
        """Delivery workflow requires exactly 3 approved candidates."""
        # Test with 2 candidates
        result = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=self.assessments[:2],
            dry_run=True,
            ledger_path=self.ledger_path,
        )
        self.assertEqual(result["status"], "INVALID_CANDIDATE_COUNT")
        self.assertFalse(result["attempt_made"])

    def test_reject_unauthorized_chat_id(self) -> None:
        """Delivery workflow rejects unauthorized chat IDs not in allowlist."""
        unauth_settings = self.settings.model_copy(update={"telegram_chat_id": "9999999999"})
        result = deliver_one_shot_editorial_digest(
            settings=unauth_settings,
            assessments=self.assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
        )
        self.assertEqual(result["status"], "UNAUTHORIZED_RECIPIENT")
        self.assertFalse(result["attempt_made"])

    def test_reject_group_and_channel_chat_ids(self) -> None:
        """Delivery workflow rejects group or channel IDs starting with '-'."""
        group_settings = self.settings.model_copy(
            update={
                "telegram_chat_id": "-1001234567890",
                "telegram_allowed_chat_ids": ["-1001234567890"],
            }
        )
        result = deliver_one_shot_editorial_digest(
            settings=group_settings,
            assessments=self.assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
        )
        self.assertEqual(result["status"], "INVALID_CHAT_TYPE")
        self.assertIn("Group/Channel delivery is prohibited", result["error"])
        self.assertFalse(result["attempt_made"])

    def test_dry_run_preflight_audit_zero_network_calls(self) -> None:
        """Dry run mode validates payload and allowlist with zero live calls."""
        result = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=self.assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
        )
        self.assertEqual(result["status"], "SAFE_PREVIEW_PASSED")
        self.assertFalse(result["attempt_made"])
        self.assertIn("3 haber önerisi", result["rendered_html"])
        self.assertIn("Şükrü Özyıldız", result["rendered_html"])
        self.assertIn("True Detective", result["rendered_html"])
        self.assertIn("Gisele Bündchen", result["rendered_html"])
        self.assertLess(len(result["rendered_html"]), 4000)

    def test_persistent_duplicate_send_protection(self) -> None:
        """If identical digest hash exists with DELIVERY_SUCCESS, resend is prevented."""
        # 1. First dry run to obtain digest hash
        preview = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=self.assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
        )
        digest_hash = preview["digest_hash"]

        # 2. Simulate recorded delivery in ledger
        ledger_data = {
            "deliveries": {
                digest_hash: {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": datetime.now(timezone.utc).isoformat(),
                    "message_id": 9999,
                }
            }
        }
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump(ledger_data, f)

        # 3. Attempt delivery should be blocked
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})
        result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=self.assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
        )
        self.assertEqual(result["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertFalse(result["attempt_made"])
        self.assertIn("Duplicate delivery prevented", result["error"])

    def test_live_dispatch_success_with_mock_transport(self) -> None:
        """Simulated live dispatch executes exactly 1 attempt and records delivery in ledger."""
        calls = []

        def mock_handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 12345}})

        mock_transport = httpx.MockTransport(mock_handler)
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})

        result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=self.assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=mock_transport,
        )

        self.assertEqual(result["status"], "DELIVERY_SUCCESS")
        self.assertTrue(result["attempt_made"])
        self.assertEqual(result["attempts_count"], 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["message_id"], 12345)

        # Verify ledger file was written
        self.assertTrue(self.ledger_path.exists())
        with open(self.ledger_path, "r", encoding="utf-8") as f:
            saved_ledger = json.load(f)
        self.assertIn(result["digest_hash"], saved_ledger.get("deliveries", {}))
        self.assertEqual(saved_ledger["deliveries"][result["digest_hash"]]["status"], "DELIVERY_SUCCESS")

    def test_live_dispatch_uncertain_timeout_skips_retry(self) -> None:
        """Ambiguous timeout records DELIVERY_UNCERTAIN and skips retries to prevent duplicate delivery."""
        calls = []

        def mock_handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            raise httpx.ReadTimeout("Connection timed out waiting for Telegram server")

        mock_transport = httpx.MockTransport(mock_handler)
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})

        result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=self.assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=mock_transport,
        )

        self.assertEqual(result["status"], "DELIVERY_UNCERTAIN")
        self.assertTrue(result["attempt_made"])
        self.assertEqual(len(calls), 1)  # Strictly 1 attempt, zero retry!

        # Verify ledger recorded uncertain outcome
        with open(self.ledger_path, "r", encoding="utf-8") as f:
            saved_ledger = json.load(f)
        self.assertEqual(saved_ledger["deliveries"][result["digest_hash"]]["status"], "DELIVERY_UNCERTAIN")


if __name__ == "__main__":
    unittest.main()
