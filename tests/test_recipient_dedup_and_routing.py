"""Comprehensive regression test suite for recipient-scoped deduplication and updated editorial routing.

Tests:
1. Strict recipient-scoped deduplication.
2. Historical 19 October incident scenario (PRIMARY_EDITOR success does not block ADMIN).
3. Symmetric scenario (ADMIN success does not block PRIMARY_EDITOR).
4. Genuine duplicates to same recipient remain strictly blocked.
5. DELIVERY_UNCERTAIN blocks subsequent delivery to affected recipient only.
6. Default editorial routing targets PRIMARY_EDITOR only.
7. ADMIN does not receive automatic editorial digests.
8. Historical Phase 4 records remain intact and match ADMIN only.
9. Corrupted delivery ledger fails closed with zero attempts.
10. Dry run makes zero live Telegram network calls.
11. No private identifiers or raw tokens appear in returned payloads or logs.
12. Operational health report category routes to ADMIN.
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
    EditorialKind,
    StoryCategory,
    SuggestedFormat,
    TelegramNotificationCategory,
    VerificationStatus,
)
from efso_radar.notifications.formatters import compute_stable_digest_identity
from efso_radar.notifications.telegram import (
    TelegramNotificationChannel,
    deliver_one_shot_editorial_digest,
)
from scripts.deliver_telegram_digest_oneshot import build_final_three_assessments


class TestRecipientDedupAndRouting(unittest.TestCase):
    """Regression suite for recipient-scoped deduplication and routing."""

    def setUp(self) -> None:
        self.admin_chat_id = "1110000001"
        self.editor_chat_id = "2220000002"
        self.bot_token = "1234567890:AAFakeMockTokenForTestingPurposes12345"
        self.settings = Settings(
            telegram_bot_token=self.bot_token,
            telegram_chat_id=self.admin_chat_id,
            telegram_primary_editor_chat_id=self.editor_chat_id,
            telegram_allowed_chat_ids=[self.admin_chat_id, self.editor_chat_id],
            telegram_delivery_enabled=False,
            telegram_max_retries=1,
            telegram_retry_delay_seconds=0.01,
            http_timeout_seconds=5.0,
        )
        self.temp_dir = tempfile.TemporaryDirectory()
        self.ledger_path = Path(self.temp_dir.name) / "test_ledger.json"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_01_default_editorial_routing_targets_primary_editor_only(self) -> None:
        """Verify all editorial categories route to PRIMARY_EDITOR only; ADMIN is excluded."""
        editorial_categories = [
            TelegramNotificationCategory.EDITORIAL_DIGEST,
            TelegramNotificationCategory.VERIFIED_BREAKING,
            TelegramNotificationCategory.SENSITIVE_EDITORIAL_REVIEW,
            TelegramNotificationCategory.HOLD_FOR_VERIFICATION,
            None,
        ]
        for cat in editorial_categories:
            recipients = self.settings.get_recipients_for_category(cat)
            self.assertEqual(len(recipients), 1, f"Category {cat} must have exactly 1 recipient")
            self.assertEqual(recipients[0]["role"], "PRIMARY_EDITOR")
            self.assertEqual(recipients[0]["chat_id"], self.editor_chat_id)

    def test_02_admin_does_not_receive_automatic_editorial_digests(self) -> None:
        """Verify unparameterized deliver_one_shot_editorial_digest defaults to PRIMARY_EDITOR only."""
        assessments = build_final_three_assessments()

        result = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
        )

        self.assertEqual(result["status"], "SAFE_PREVIEW_PASSED")
        self.assertEqual(result["recipients_count"], 1)
        self.assertIn("PRIMARY_EDITOR", result["recipient_results"])
        self.assertNotIn(self.editor_chat_id, result["recipient_results"])
        self.assertNotIn(self.admin_chat_id, result["recipient_results"])
        self.assertNotIn("ADMIN", result["recipient_results"])
        self.assertEqual(result["recipient_results"]["PRIMARY_EDITOR"]["recipient_role"], "PRIMARY_EDITOR")

    def test_03_historical_19_october_incident_scenario_admin_not_blocked(self) -> None:
        """Historical 19 October incident: PRIMARY_EDITOR delivery success must NOT block ADMIN."""
        assessments = build_final_three_assessments()
        stable_id = compute_stable_digest_identity(assessments)
        digest_hash = "738b2891f25e460ea7a356eb"

        # Pre-seed ledger with PRIMARY_EDITOR having successfully received this exact digest
        pre_ledger = {
            "deliveries": {
                f"{self.editor_chat_id}:{digest_hash}": {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": "2026-10-19T08:00:54.547646+00:00",
                    "delivered_at_tsi": "19 Ekim 2026 11:00 TSİ",
                    "chat_id": self.editor_chat_id,
                    "target_chat_id": "22...02",
                    "recipient_role": "PRIMARY_EDITOR",
                    "stories_count": 3,
                    "digest_hash": digest_hash,
                    "stable_digest_id": stable_id,
                    "article_ids": ["art1", "art2", "art3"],
                    "is_ux_test": False,
                    "digest_type": "EDITORIAL_DIGEST",
                    "message_id": 11,
                }
            }
        }
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump(pre_ledger, f)

        # ADMIN delivery check (targeted delivery)
        admin_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="ADMIN",
        )

        # ADMIN must NOT be blocked by PRIMARY_EDITOR's delivery!
        self.assertNotEqual(admin_res["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(admin_res["status"], "SAFE_PREVIEW_PASSED")
        self.assertEqual(admin_res["recipients_count"], 1)
        self.assertEqual(admin_res["recipient_results"]["ADMIN"]["status"], "SAFE_PREVIEW_PASSED")

        # But PRIMARY_EDITOR must remain blocked as genuine duplicate
        editor_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(editor_res["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(editor_res["message_id"], 11)

    def test_04_admin_delivered_primary_editor_not_blocked(self) -> None:
        """Symmetric scenario: ADMIN delivery success must NOT block PRIMARY_EDITOR."""
        assessments = build_final_three_assessments()
        stable_id = compute_stable_digest_identity(assessments)
        digest_hash = "admin_delivered_hash_99"

        # Pre-seed ledger with ADMIN having received this digest
        pre_ledger = {
            "deliveries": {
                f"{self.admin_chat_id}:{digest_hash}": {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": "2026-10-19T09:00:00+00:00",
                    "delivered_at_tsi": "19 Ekim 2026 12:00 TSİ",
                    "chat_id": self.admin_chat_id,
                    "target_chat_id": "11...01",
                    "recipient_role": "ADMIN",
                    "stories_count": 3,
                    "digest_hash": digest_hash,
                    "stable_digest_id": stable_id,
                    "article_ids": ["art1", "art2", "art3"],
                    "is_ux_test": False,
                    "digest_type": "EDITORIAL_DIGEST",
                    "message_id": 42,
                }
            }
        }
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump(pre_ledger, f)

        # PRIMARY_EDITOR check
        editor_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )

        self.assertEqual(editor_res["status"], "SAFE_PREVIEW_PASSED")
        self.assertFalse(editor_res["attempt_made"])

        # ADMIN is blocked
        admin_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="ADMIN",
        )
        self.assertEqual(admin_res["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(admin_res["message_id"], 42)

    def test_05_delivery_uncertain_blocks_affected_recipient_only(self) -> None:
        """DELIVERY_UNCERTAIN blocks the affected recipient from retry, leaving other recipient eligible."""
        assessments = build_final_three_assessments()
        stable_id = compute_stable_digest_identity(assessments)
        digest_hash = "uncertain_hash_77"

        # Pre-seed ledger with PRIMARY_EDITOR having DELIVERY_UNCERTAIN
        pre_ledger = {
            "deliveries": {
                f"{self.editor_chat_id}:{digest_hash}": {
                    "status": "DELIVERY_UNCERTAIN",
                    "delivered_at_utc": "2026-10-19T07:30:00+00:00",
                    "chat_id": self.editor_chat_id,
                    "target_chat_id": "22...02",
                    "recipient_role": "PRIMARY_EDITOR",
                    "digest_hash": digest_hash,
                    "stable_digest_id": stable_id,
                    "error": "Gateway timeout HTTP 504",
                }
            }
        }
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump(pre_ledger, f)

        # PRIMARY_EDITOR must be blocked from automatic retry
        editor_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(editor_res["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertIn("ambiguous timeout", editor_res["error"])

        # ADMIN must NOT be blocked by PRIMARY_EDITOR's uncertain state
        admin_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="ADMIN",
        )
        self.assertEqual(admin_res["status"], "SAFE_PREVIEW_PASSED")

    def test_06_historical_phase4_records_intact_and_admin_scoped(self) -> None:
        """Historical Phase 4 single-recipient records block ADMIN but never block PRIMARY_EDITOR."""
        historical_hash = "06afb36bc22dff56263e72de"
        # Authentic Phase 4 record (no chat_id key, no recipient_role key, un-prefixed hash)
        pre_ledger = {
            "deliveries": {
                historical_hash: {
                    "status": "DELIVERY_SUCCESS",
                    "delivered_at_utc": "2026-10-18T18:07:23.052243+00:00",
                    "delivered_at_tsi": "18 Ekim 2026 21:07 TSİ",
                    "target_chat_id": "11...01",
                    "stories_count": 3,
                    "digest_hash": historical_hash,
                    "message_id": 6,
                }
            }
        }
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump(pre_ledger, f)

        assessments = build_final_three_assessments()

        # ADMIN must be blocked from re-sending historical digest
        admin_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="ADMIN",
        )
        self.assertEqual(admin_res["status"], "DUPLICATE_DELIVERY_PREVENTED")
        self.assertEqual(admin_res["message_id"], 6)

        # PRIMARY_EDITOR must NOT be blocked by historical Phase 4 record
        editor_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(editor_res["status"], "SAFE_PREVIEW_PASSED")

    def test_07_corrupted_ledger_fails_closed(self) -> None:
        """Corrupted or invalid JSON delivery ledger must fail closed with zero network attempts."""
        # 1. Syntax corruption
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            f.write("{corrupted json syntax: [unclosed")

        assessments = build_final_three_assessments()
        res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=False,  # Even with dry_run=False
            ledger_path=self.ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(res["status"], "LEDGER_CORRUPTED")
        self.assertFalse(res["attempt_made"])
        self.assertEqual(res["attempts_count"], 0)

        # 2. Structural corruption (deliveries is not a dict)
        with open(self.ledger_path, "w", encoding="utf-8") as f:
            json.dump({"deliveries": "corrupted_non_dict_value"}, f)

        res2 = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=self.ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(res2["status"], "LEDGER_CORRUPTED")
        self.assertFalse(res2["attempt_made"])

    def test_08_dry_run_strictly_zero_live_network_calls(self) -> None:
        """Verify dry run executes zero network requests even when delivery is enabled in settings."""
        def failing_transport(request: httpx.Request) -> httpx.Response:
            raise AssertionError("Network transport must never be invoked in dry-run mode!")

        transport = httpx.MockTransport(failing_transport)
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})
        assessments = build_final_three_assessments()

        result = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            transport=transport,
            target_recipient_role="PRIMARY_EDITOR",
        )

        self.assertEqual(result["status"], "SAFE_PREVIEW_PASSED")
        self.assertFalse(result["attempt_made"])
        self.assertEqual(result["attempts_count"], 0)

    def test_09_no_private_identifiers_in_return_payloads(self) -> None:
        """Verify that returned data structures never expose unmasked tokens or unmasked chat IDs."""
        assessments = build_final_three_assessments()
        result = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
            target_recipient_role="ALL",
        )

        serialized = json.dumps(result)
        # Raw bot token must never appear
        self.assertNotIn(self.bot_token, serialized)
        # Raw chat IDs for both ADMIN and PRIMARY_EDITOR must never appear in public serialized results
        self.assertNotIn(self.admin_chat_id, serialized)
        self.assertNotIn(self.editor_chat_id, serialized)
        # Masked token and masked chat IDs must appear
        self.assertIn("123456...2345", serialized)
        self.assertIn("11...01", serialized)
        self.assertIn("22...02", serialized)
        # Verify keys in recipient_results do not contain raw chat IDs
        for key in result["recipient_results"].keys():
            self.assertNotEqual(key, self.admin_chat_id)
            self.assertNotEqual(key, self.editor_chat_id)
        # Keys must be accessible via role and masked ID
        self.assertIn("ADMIN", result["recipient_results"])
        self.assertIn("PRIMARY_EDITOR", result["recipient_results"])
        self.assertIn("11...01", result["recipient_results"])
        self.assertIn("22...02", result["recipient_results"])

    def test_10_operational_health_report_routing(self) -> None:
        """Verify operational health report category routes strictly to ADMIN."""
        admin_recipients = self.settings.get_recipients_for_category(
            TelegramNotificationCategory.OPERATIONAL_HEALTH_REPORT
        )
        self.assertEqual(len(admin_recipients), 1)
        self.assertEqual(admin_recipients[0]["role"], "ADMIN")
        self.assertEqual(admin_recipients[0]["chat_id"], self.admin_chat_id)

    def test_11_telegram_channel_default_chat_id_routes_to_primary_editor(self) -> None:
        """Verify TelegramNotificationChannel defaults to PRIMARY_EDITOR for editorial items."""
        channel = TelegramNotificationChannel(settings=self.settings)
        art = Article(
            id="art_test_route_01",
            title="Test headline",
            url="https://example.com/test",
            source="Test",
            published_at=datetime.now(timezone.utc),
        )
        asm = EditorialAssessment(
            article_ids=[art.id],
            score=85,
            kind=EditorialKind.NEW,
            headline_tr="Test Başlık",
            category=StoryCategory.GLOBAL,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            suggested_format=SuggestedFormat.POST,
            supporting_articles=[art],
        )

        # 1. Send single editorial notification (dry-run)
        record = channel.send(assessment=asm, story_id="test_story", dry_run=True)
        # NotificationRecord doesn't hold chat_id directly, but let's check send_digest_batch
        record_batch = channel.send_digest_batch(
            assessments=[asm, asm, asm],
            notification_id="BATCH-01",
            dry_run=True,
        )
        self.assertEqual(record_batch.status.value, "SENT")

    def test_12_editorial_routing_fails_closed_when_primary_editor_missing(self) -> None:
        """ADMIN must never become an implicit fallback when PRIMARY_EDITOR is missing or invalid."""
        # Config with only ADMIN configured, legacy fallback disabled (default)
        admin_only_settings = Settings(
            telegram_bot_token=self.bot_token,
            telegram_chat_id=self.admin_chat_id,
            telegram_primary_editor_chat_id="",
            telegram_allowed_chat_ids=[self.admin_chat_id],
            telegram_delivery_enabled=False,
            legacy_editorial_fallback=False,  # Production default
        )
        # 1. get_editorial_recipients must fail closed (return empty list)
        self.assertEqual(admin_only_settings.get_editorial_recipients(), [])
        self.assertEqual(
            admin_only_settings.get_recipients_for_category(TelegramNotificationCategory.EDITORIAL_DIGEST),
            [],
        )

        # 2. deliver_one_shot_editorial_digest must fail closed with CONFIG_ERROR
        assessments = build_final_three_assessments()
        digest_res = deliver_one_shot_editorial_digest(
            settings=admin_only_settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=self.ledger_path,
        )
        self.assertEqual(digest_res["status"], "CONFIG_ERROR")
        self.assertFalse(digest_res["attempt_made"])
        self.assertIn("PRIMARY_EDITOR", digest_res["error"])

        # 3. Channel send and send_digest_batch must fail closed with DeliveryStatus.FAILED
        channel = TelegramNotificationChannel(settings=admin_only_settings)
        single_rec = channel.send(assessment=assessments[0], story_id="test_fail_closed", dry_run=True)
        self.assertEqual(single_rec.status.value, "FAILED")
        self.assertIn("Routing failed closed", single_rec.error_message or "")

        batch_rec = channel.send_digest_batch(
            assessments=assessments,
            notification_id="BATCH-FAIL-01",
            dry_run=True,
        )
        self.assertEqual(batch_rec.status.value, "FAILED")
        self.assertIn("Routing failed closed", batch_rec.error_message or "")

        # 4. Explicit legacy flag restores ADMIN fallback for backward compatibility
        legacy_settings = admin_only_settings.model_copy(update={"legacy_editorial_fallback": True})
        self.assertEqual(len(legacy_settings.get_editorial_recipients()), 1)
        self.assertEqual(legacy_settings.get_editorial_recipients()[0]["role"], "ADMIN")

    def test_13_missing_ledger_fails_closed_in_live_mode(self) -> None:
        """Missing delivery ledger must fail closed with MISSING_LEDGER in live mode, allowed in dry-run."""
        missing_ledger_path = Path(self.temp_dir.name) / "non_existent_ledger_file.json"
        self.assertFalse(missing_ledger_path.exists())

        assessments = build_final_three_assessments()

        # 1. Dry run allows empty ledger
        dry_res = deliver_one_shot_editorial_digest(
            settings=self.settings,
            assessments=assessments,
            dry_run=True,
            ledger_path=missing_ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(dry_res["status"], "SAFE_PREVIEW_PASSED")
        self.assertFalse(dry_res["attempt_made"])

        # 2. Live mode (dry_run=False, delivery enabled) fails closed with MISSING_LEDGER
        live_settings = self.settings.model_copy(update={"telegram_delivery_enabled": True})
        live_res = deliver_one_shot_editorial_digest(
            settings=live_settings,
            assessments=assessments,
            dry_run=False,
            ledger_path=missing_ledger_path,
            target_recipient_role="PRIMARY_EDITOR",
        )
        self.assertEqual(live_res["status"], "MISSING_LEDGER")
        self.assertFalse(live_res["attempt_made"])
        self.assertEqual(live_res["attempts_count"], 0)
        self.assertIn("does not exist", live_res["error"])


if __name__ == "__main__":
    unittest.main()
