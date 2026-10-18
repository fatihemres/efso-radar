"""
Unit tests for the Phase 4G Primary Editor Welcome Test workflow.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import httpx

from efso_radar.config import Settings
from scripts.send_primary_editor_welcome_test import (
    WELCOME_MESSAGE_HTML,
    execute_primary_editor_welcome_test,
)


class TestPrimaryEditorWelcomeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.temp_dir.name) / "test_ledger.json"
        self.admin_chat = "1000000001"
        self.editor_chat = "2000000002"
        self.settings = Settings(
            telegram_bot_token="1234567890:AAFakeMockTokenForTestingPurposes12345",
            telegram_chat_id=self.admin_chat,
            telegram_primary_editor_chat_id=self.editor_chat,
            telegram_allowed_chat_ids=[self.admin_chat, self.editor_chat],
            telegram_delivery_enabled=False,
            http_timeout_seconds=5.0,
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_dry_run_zero_network(self) -> None:
        """Verify dry-run mode passes audit without network calls."""
        res = execute_primary_editor_welcome_test(
            settings=self.settings,
            dry_run=True,
            ledger_path=self.ledger_path,
        )
        self.assertEqual(res["status"], "SAFE_PREVIEW_PASSED")
        self.assertFalse(res["attempt_made"])
        self.assertEqual(res["attempts_count"], 0)
        self.assertTrue(res["admin_excluded"])

    def test_mock_live_send_success_and_duplicate_suppression(self) -> None:
        """Verify live send to Primary Editor only and duplicate suppression on retry."""
        dispatched_chats = []

        def mock_handler(request: httpx.Request) -> httpx.Response:
            data = json.loads(request.content.decode("utf-8"))
            cid = str(data.get("chat_id"))
            dispatched_chats.append(cid)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 888}})

        transport = httpx.MockTransport(mock_handler)
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})

        # 1. First send
        res = execute_primary_editor_welcome_test(
            settings=live_settings,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=transport,
        )
        self.assertEqual(res["status"], "DELIVERY_SUCCESS")
        self.assertEqual(res["message_id"], 888)
        self.assertTrue(res["attempt_made"])
        self.assertEqual(res["attempts_count"], 1)
        self.assertEqual(dispatched_chats, [self.editor_chat])  # Exactly Primary Editor, Admin excluded!

        # Verify ledger file
        with open(self.ledger_path, "r", encoding="utf-8") as f:
            ledger_data = json.load(f)
        key = f"{self.editor_chat}:{res['digest_hash']}"
        self.assertIn(key, ledger_data["deliveries"])
        self.assertEqual(ledger_data["deliveries"][key]["recipient_role"], "PRIMARY_EDITOR")
        self.assertEqual(ledger_data["deliveries"][key]["message_id"], 888)

        # 2. Duplicate send prevention
        res2 = execute_primary_editor_welcome_test(
            settings=live_settings,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=transport,
        )
        self.assertEqual(res2["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertFalse(res2["attempt_made"])
        self.assertEqual(len(dispatched_chats), 1)  # No second dispatch made


if __name__ == "__main__":
    unittest.main()
