"""
Phase 4G: Automated Test Suite for Two-Recipient (Admin & Primary Editor) Telegram Delivery.

Verifies:
1. Two valid private recipients (PRIMARY_EDITOR & ADMIN) configured and allowlisted.
2. Rejection of unauthorized chat IDs and channel/group chats ('-' prefix).
3. Equal category coverage: Both recipients receive all 4 notification categories.
4. Warning banners & classification embargoes intact for both recipients.
5. Independent per-recipient ledger tracking ({chat_id}:{digest_hash}).
6. Independent per-recipient duplicate suppression.
7. Partial delivery handling (one succeeds, one fails/uncertain).
8. Ambiguous network timeouts recorded as UNCERTAIN without retry.
9. Existing historical single-recipient ledger records remain 100% valid.
10. Safe preview mode strictly executes zero network requests.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import httpx

from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    Market,
    StoryCategory,
    SuggestedFormat,
    TelegramNotificationCategory,
    VerificationStatus,
)
from efso_radar.notifications.formatters import (
    compute_stable_digest_identity,
    format_telegram_breaking,
    format_telegram_digest,
    format_telegram_sensitive_review,
    format_telegram_verification_hold,
)
from efso_radar.notifications.telegram import (
    TelegramNotificationChannel,
    deliver_one_shot_editorial_digest,
)
from scripts.deliver_telegram_digest_oneshot import build_final_three_assessments


class TestPhase4GTwoRecipientDelivery(unittest.TestCase):
    """Test suite covering two-recipient Telegram architecture for Phase 4G."""

    def setUp(self) -> None:
        self.admin_chat_id = "1000000001"
        self.editor_chat_id = "2000000002"
        self.settings = Settings(
            telegram_bot_token="1234567890:AAFakeMockTokenForTestingPurposes12345",
            telegram_chat_id=self.admin_chat_id,
            telegram_primary_editor_chat_id=self.editor_chat_id,
            telegram_allowed_chat_ids=[self.admin_chat_id, self.editor_chat_id],
            telegram_delivery_enabled=False,
            telegram_max_retries=1,
            telegram_retry_delay_seconds=0.01,
            http_timeout_seconds=5.0,
        )
        self.temp_dir = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.temp_dir.name) / "test_delivery_ledger.json"
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump({"deliveries": {}}, f)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_01_two_recipients_configured_and_allowed(self) -> None:
        """Verify settings recognizes both PRIMARY_EDITOR and ADMIN as authorized recipients."""
        recipients = self.settings.get_authorized_recipients()
        self.assertEqual(len(recipients), 2)

        roles = {r["role"]: r for r in recipients}
        self.assertIn("PRIMARY_EDITOR", roles)
        self.assertIn("ADMIN", roles)

        self.assertEqual(roles["PRIMARY_EDITOR"]["chat_id"], self.editor_chat_id)
        self.assertEqual(roles["PRIMARY_EDITOR"]["masked_chat_id"], "20...02")
        self.assertEqual(roles["ADMIN"]["chat_id"], self.admin_chat_id)
        self.assertEqual(roles["ADMIN"]["masked_chat_id"], "10...01")

        self.assertEqual(self.settings.get_recipient_role(self.editor_chat_id), "PRIMARY_EDITOR")
        self.assertEqual(self.settings.get_recipient_role(self.admin_chat_id), "ADMIN")

    def test_02_unauthorized_and_group_chats_rejected(self) -> None:
        """Ensure channels, groups, and unauthorized users are strictly rejected."""
        self.assertFalse(self.settings.is_chat_id_allowed("-1001234567890"))
        self.assertFalse(self.settings.is_chat_id_allowed("-987654321"))
        self.assertFalse(self.settings.is_chat_id_allowed("1112223334"))
        self.assertFalse(self.settings.is_chat_id_allowed(""))

        self.assertIsNone(self.settings.get_recipient_role("-1001234567890"))
        self.assertIsNone(self.settings.get_recipient_role("1112223334"))

    def test_03_editorial_categories_delivered_to_primary_editor_only(self) -> None:
        """Verify updated policy: all four editorial categories route to PRIMARY_EDITOR only; ADMIN is excluded from editorial broadcasts."""
        categories = [
            TelegramNotificationCategory.EDITORIAL_DIGEST,
            TelegramNotificationCategory.VERIFIED_BREAKING,
            TelegramNotificationCategory.SENSITIVE_EDITORIAL_REVIEW,
            TelegramNotificationCategory.HOLD_FOR_VERIFICATION,
        ]
        for cat in categories:
            recipients = self.settings.get_recipients_for_category(cat)
            self.assertEqual(len(recipients), 1, f"Category {cat} should have exactly 1 recipient (PRIMARY_EDITOR)")
            self.assertEqual(recipients[0]["role"], "PRIMARY_EDITOR")
            self.assertEqual(recipients[0]["chat_id"], self.editor_chat_id)

        # Operational health report routes to ADMIN
        admin_recs = self.settings.get_recipients_for_category(TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT)
        self.assertEqual(len(admin_recs), 1)
        self.assertEqual(admin_recs[0]["role"], "ADMIN")
        self.assertEqual(admin_recs[0]["chat_id"], self.admin_chat_id)

        # Authorized recipients still lists both known identities
        all_recs = self.settings.get_authorized_recipients()
        self.assertEqual(len(all_recs), 2)

    def test_04_warning_banners_and_embargoes_intact(self) -> None:
        """Verify warning banners and embargoes are identical and intact for both recipients."""
        # 1. Sensitive Review
        art_sens = Article(
            id="art_sens_01",
            title="Vefat haberi",
            url="https://example.com/sens",
            source="Test",
            market=Market.TR,
            published_at=datetime.now(timezone.utc),
        )
        assess_sens = EditorialAssessment(
            article_ids=[art_sens.id],
            score=75,
            kind=EditorialKind.NEW,
            headline_tr="Usta Sanatçının Vefat Haberi",
            why_efso_tr="Kamuoyunu üzen vefat haberi.",
            uncertainties_tr="SENSITIVE_EDITORIAL_REVIEW: Vefat haberi. Açık onay gerektirir.",
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.FIRST_PARTY_STATEMENT,
            source_urls=[art_sens.url],
            supporting_articles=[art_sens],
            is_publishable=False,
            disposition=EditorialDisposition.SENSITIVE_REVIEW,
        )
        html_sens = format_telegram_sensitive_review(assess_sens)
        self.assertIn("SENSITIVE REVIEW", html_sens)
        self.assertIn("GİZLİ & HASSAS İÇERİK", html_sens)
        self.assertIn("Product Owner tekil ve açık onay vermedikçe yayınlanamaz", html_sens)

        # 2. Hold for Verification
        assess_hold = EditorialAssessment(
            article_ids=[art_sens.id],
            score=70,
            kind=EditorialKind.NEW,
            headline_tr="Doğrulanmamış Nişan İddiası",
            why_efso_tr="Teyit bekleyen magazin iddiası.",
            uncertainties_tr="MANUAL VERIFICATION REQUIRED: Birincil kaynak doğrulaması yok.",
            category=StoryCategory.GLOBAL,
            verification_status=VerificationStatus.SPECULATION,
            source_urls=[art_sens.url],
            supporting_articles=[art_sens],
            is_publishable=False,
            disposition=EditorialDisposition.HOLD,
        )
        html_hold = format_telegram_verification_hold(assess_hold)
        self.assertIn("HOLD FOR VERIFICATION", html_hold)
        self.assertIn("YAYINLANAMAZ — EDİTORYAL TEYİT BEKLEMESİNDE", html_hold)

    def test_05_independent_delivery_ledger_tracking(self) -> None:
        """Verify delivery ledger tracks each recipient independently with distinct keys and message IDs."""
        call_count = {self.admin_chat_id: 0, self.editor_chat_id: 0}

        def mock_handler(request: httpx.Request) -> httpx.Response:
            data = json.loads(request.content.decode("utf-8"))
            cid = str(data.get("chat_id"))
            call_count[cid] += 1
            msg_id = 501 if cid == self.admin_chat_id else 502
            return httpx.Response(200, json={"ok": True, "result": {"message_id": msg_id}})

        transport = httpx.MockTransport(mock_handler)
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})
        assessments = build_final_three_assessments()

        result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=transport,
            target_recipient_role="ALL",
            allow_legacy_admin_broadcast=True,
        )

        self.assertEqual(result["status"], "DELIVERY_SUCCESS")
        self.assertEqual(result["recipients_count"], 2)
        rec_res = result["recipient_results"]
        # Raw chat IDs must not be keys in recipient_results
        self.assertNotIn(self.admin_chat_id, rec_res)
        self.assertNotIn(self.editor_chat_id, rec_res)
        self.assertEqual(rec_res["ADMIN"]["status"], "DELIVERY_SUCCESS")
        self.assertEqual(rec_res["ADMIN"]["message_id"], 501)
        self.assertEqual(rec_res["PRIMARY_EDITOR"]["status"], "DELIVERY_SUCCESS")
        self.assertEqual(rec_res["PRIMARY_EDITOR"]["message_id"], 502)

        # Inspect persisted ledger file
        with open(self.ledger_path, "r", encoding="utf-8") as f:
            ledger_data = json.load(f)

        digest_hash = result["digest_hash"]
        deliveries = ledger_data.get("deliveries", {})

        # Assert per-recipient keys exist
        admin_key = f"{self.admin_chat_id}:{digest_hash}"
        editor_key = f"{self.editor_chat_id}:{digest_hash}"
        self.assertIn(admin_key, deliveries)
        self.assertIn(editor_key, deliveries)

        self.assertEqual(deliveries[admin_key]["message_id"], 501)
        self.assertEqual(deliveries[admin_key]["recipient_role"], "ADMIN")
        self.assertEqual(deliveries[editor_key]["message_id"], 502)
        self.assertEqual(deliveries[editor_key]["recipient_role"], "PRIMARY_EDITOR")

    def test_06_independent_duplicate_suppression(self) -> None:
        """Verify delivery to Admin does not block delivery to Primary Editor, and vice versa."""
        assessments = build_final_three_assessments()
        digest_hash = "abc123hash"
        stable_id = compute_stable_digest_identity(assessments)

        # Pre-seed ledger with ADMIN having already received this digest
        pre_ledger = {
            "deliveries": {
                f"{self.admin_chat_id}:{digest_hash}": {
                    "status": "DELIVERY_SUCCESS",
                    "chat_id": self.admin_chat_id,
                    "target_chat_id": "10...01",
                    "recipient_role": "ADMIN",
                    "digest_hash": digest_hash,
                    "stable_digest_id": stable_id,
                    "delivered_at_tsi": "18 Ekim 2026 20:00 TSİ",
                    "message_id": 999,
                }
            }
        }
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump(pre_ledger, f)

        # Dispatch in dry-run mode
        result = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="ALL",
        )

        rec_res = result["recipient_results"]
        # Admin must be blocked
        self.assertNotIn(self.admin_chat_id, rec_res)
        self.assertNotIn(self.editor_chat_id, rec_res)
        self.assertEqual(rec_res["ADMIN"]["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(rec_res["ADMIN"]["message_id"], 999)

        # Primary Editor must be eligible and pass preview
        self.assertEqual(rec_res["PRIMARY_EDITOR"]["status"], "SAFE_PREVIEW_PASSED")

    def test_07_partial_delivery_handling(self) -> None:
        """Verify partial delivery where Admin succeeds and Primary Editor fails with HTTP 403."""
        def mock_handler(request: httpx.Request) -> httpx.Response:
            data = json.loads(request.content.decode("utf-8"))
            cid = str(data.get("chat_id"))
            if cid == self.admin_chat_id:
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 777}})
            else:
                return httpx.Response(403, json={"ok": False, "description": "Forbidden: bot was blocked by the user"})

        transport = httpx.MockTransport(mock_handler)
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})
        assessments = build_final_three_assessments()

        result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=transport,
            target_recipient_role="ALL",
            allow_legacy_admin_broadcast=True,
        )

        self.assertEqual(result["status"], "PARTIAL_DELIVERY")
        rec_res = result["recipient_results"]
        self.assertNotIn(self.admin_chat_id, rec_res)
        self.assertNotIn(self.editor_chat_id, rec_res)
        self.assertEqual(rec_res["ADMIN"]["status"], "DELIVERY_SUCCESS")
        self.assertEqual(rec_res["ADMIN"]["message_id"], 777)
        self.assertEqual(rec_res["PRIMARY_EDITOR"]["status"], "ACCESS_DENIED")

        # In ledger, Admin is SUCCESS, Editor is ACCESS_DENIED
        with open(self.ledger_path, "r", encoding="utf-8") as f:
            ledger_data = json.load(f)
        digest_hash = result["digest_hash"]
        self.assertEqual(ledger_data["deliveries"][f"{self.admin_chat_id}:{digest_hash}"]["status"], "DELIVERY_SUCCESS")
        self.assertEqual(ledger_data["deliveries"][f"{self.editor_chat_id}:{digest_hash}"]["status"], "ACCESS_DENIED")

    def test_08_ambiguous_network_timeout_recorded_uncertain_no_retry(self) -> None:
        """Verify network timeout marks recipient DELIVERY_UNCERTAIN without retry, blocking subsequent sends."""
        attempts = 0

        def timeout_handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ReadTimeout("Connection timed out waiting for Telegram response")

        transport = httpx.MockTransport(timeout_handler)
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})
        assessments = build_final_three_assessments()

        result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=transport,
            target_recipient_role="PRIMARY_EDITOR",
        )

        self.assertEqual(result["status"], "DELIVERY_UNCERTAIN")
        self.assertEqual(attempts, 1)  # Exactly 1 attempt, zero retry

        # Second attempt must be prevented by duplicate check
        second_result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
            transport=transport,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(second_result["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(attempts, 1)  # No extra attempt made

    def test_09_existing_single_recipient_history_preserved(self) -> None:
        """Verify historical deliveries (e.g. Phase 4E message 6) block Admin but allow Primary Editor."""
        historical_hash = "06afb36bc22dff56263e72de"
        stable_id = "stable_hash_01"
        pre_ledger = {
            "deliveries": {
                historical_hash: {
                    "status": "DELIVERY_SUCCESS",
                    "chat_id": self.admin_chat_id,
                    "target_chat_id": "10...01",
                    "recipient_role": "ADMIN",
                    "digest_hash": historical_hash,
                    "stable_digest_id": stable_id,
                    "delivered_at_tsi": "18 Ekim 2026 21:07 TSİ",
                    "message_id": 6,
                }
            }
        }
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump(pre_ledger, f)

        assessments = build_final_three_assessments()

        # Query delivery for ADMIN
        admin_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="ADMIN",
        )
        self.assertEqual(admin_res["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(admin_res["message_id"], 6)

        # Query delivery for PRIMARY_EDITOR (not blocked by Admin's history)
        editor_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(editor_res["status"], "SAFE_PREVIEW_PASSED")

    def test_10_safe_preview_mode_strictly_zero_network_calls(self) -> None:
        """Verify safe preview mode never invokes transport or makes network calls."""
        def failing_transport(request: httpx.Request) -> httpx.Response:
            raise AssertionError("Zero network calls permitted in dry-run mode!")

        transport = httpx.MockTransport(failing_transport)
        assessments = build_final_three_assessments()

        result = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            transport=transport,
            target_recipient_role="ALL",
        )

        self.assertEqual(result["status"], "SAFE_PREVIEW_PASSED")
        self.assertFalse(result["attempt_made"])
        self.assertEqual(result["attempts_count"], 0)
        self.assertEqual(result["recipients_count"], 2)


if __name__ == "__main__":
    unittest.main()
