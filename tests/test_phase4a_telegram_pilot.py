"""Tests for Efso Radar Phase 4A: Supervised Telegram Pilot Preparation.

Verifies:
1. Single private bot-to-owner architecture.
2. Explicit chat ID allowlist enforcement and unauthorized recipient rejection.
3. Four distinct message categories:
   - EDITORIAL_DIGEST
   - VERIFIED_BREAKING
   - SENSITIVE_EDITORIAL_REVIEW
   - HOLD_FOR_VERIFICATION
4. Safe preview / delivery disabled mode (zero live HTTP calls).
5. Simulated network transport failures, HTTP 403 fail-fast, HTTP 500 retries, and timeout exhaustion.
6. Token and secret redaction.
7. Restart/resume outbox state and duplicate prevention.
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
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    NotificationRecord,
    StoryCategory,
    SuggestedFormat,
    TelegramNotificationCategory,
    VerificationStatus,
)
from efso_radar.notifications.formatters import (
    format_telegram_alert,
    format_telegram_breaking,
    format_telegram_digest,
    format_telegram_sensitive_review,
    format_telegram_verification_hold,
)
from efso_radar.notifications.telegram import (
    TelegramNotificationChannel,
    derive_telegram_category,
    fetch_bot_updates_for_chat_identification,
    run_one_shot_connectivity_test,
    sanitize_telegram_text,
)
from efso_radar.repositories.sqlite import SqliteStateRepository


class TestPhase4ATelegramPilot(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 18, 12, 0, 0, tzinfo=timezone.utc)
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_pilot.db"
        self.repo = SqliteStateRepository(self.db_path)

        self.authorized_chat_id = "123456789"
        self.unauthorized_chat_id = "987654321"
        self.bot_token = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"

        self.settings = Settings(
            telegram_bot_token=self.bot_token,
            telegram_chat_id=self.authorized_chat_id,
            telegram_allowed_chat_ids=[self.authorized_chat_id],
            telegram_delivery_enabled=True,
            telegram_max_retries=3,
            telegram_retry_backoff_base=0.0,  # Fast tests without sleeping
            legacy_editorial_fallback=True,
            min_score=75,
            breaking_alert_min_score=90,
        )

        self.article = Article.create(
            "Çağatay Ulusoy Köy Ziyareti Yaptı",
            "https://example.com/cagatay-koy",
            "Hürriyet Kelebek",
            self.now,
            snippet="Çağatay Ulusoy memleketi Bulgaristan'daki akrabalarını ziyaret etti.",
        )
        self.assessment = EditorialAssessment(
            article_ids=[self.article.id],
            score=94,
            kind=EditorialKind.NEW,
            parent_story_id=None,
            development_tr=None,
            headline_tr="Çağatay Ulusoy Köy Ziyareti Yaptı",
            cover_title_tr="ÇAĞATAY ULUSOY KÖYÜNDE",
            caption_idea_tr="Ünlü oyuncudan samimi görüntüler.",
            relevance_reason_tr="Yüksek hayran ilgisi.",
            why_efso_tr="Geniş hayran kitlesi ve organik ilgi.",
            uncertainties_tr=None,
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[self.article.url],
            supporting_articles=[self.article],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )

    def tearDown(self):
        try:
            self.tmpdir.cleanup()
        except PermissionError:
            pass

    def test_unauthorized_telegram_recipient_rejection(self):
        """Live delivery to a non-allowlisted chat ID must be strictly rejected."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(
            self.assessment,
            "STORY-001",
            target_chat_id=self.unauthorized_chat_id,
        )

        self.assertEqual(record.status, DeliveryStatus.FAILED)
        self.assertIn("Unauthorized recipient", record.error_message)
        self.assertEqual(len(calls), 0, "No network request must be made to an unauthorized chat ID")

    def test_authorized_recipient_live_dispatch_success(self):
        """Authorized chat ID successfully receives dispatched notification via mock transport."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            body = json.loads(request.content.decode("utf-8"))
            self.assertEqual(body["chat_id"], self.authorized_chat_id)
            self.assertEqual(body["parse_mode"], "HTML")
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(
            self.assessment,
            "STORY-001",
            category=TelegramNotificationCategory.VERIFIED_BREAKING,
        )

        self.assertEqual(record.status, DeliveryStatus.SENT)
        self.assertEqual(len(calls), 1)
        self.assertEqual(record.category, TelegramNotificationCategory.VERIFIED_BREAKING)

    def test_safe_preview_mode_when_delivery_disabled(self):
        """When telegram_delivery_enabled=False, channel operates in zero-network preview mode."""
        safe_settings = self.settings.model_copy(update={"telegram_delivery_enabled": False})
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(safe_settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.SENT)
        self.assertEqual(len(calls), 0, "Zero network calls must be made in safe preview mode")

    def test_four_notification_categories_formatting(self):
        """Verify the Turkish formatting, headers, and safety disclaimers for all 4 categories."""
        # 1. EDITORIAL_DIGEST
        digest_text = format_telegram_digest([self.assessment], window_label="6 Saatlik Editoryal Özet")
        self.assertIn("EDİTORYAL BÜLTEN", digest_text)
        self.assertIn("6 SAATLİK EDİTORYAL ÖZET", digest_text)
        self.assertIn("Instagram'da otomatik paylaşım yapılmaz", digest_text)

        # 2. VERIFIED_BREAKING
        breaking_text = format_telegram_breaking(self.assessment)
        self.assertIn("🚨 EFSO RADAR | DOĞRULANMIŞ SICAK GELİŞME", breaking_text)
        self.assertIn("94/100", breaking_text)
        self.assertIn("Saygın Yayıncı Teyitli", breaking_text)
        self.assertIn("Instagram'da otomatik yayınlanmaz", breaking_text)

        # 3. SENSITIVE_EDITORIAL_REVIEW
        sens_asm = self.assessment.model_copy(update={
            "disposition": EditorialDisposition.SENSITIVE_REVIEW,
            "headline_tr": "Usta Sanatçının Vefat Haberi",
            "uncertainties_tr": "[NOT_PUBLISHABLE - SENSITIVE EDITORIAL REVIEW REQUIRED] Vefat haberi Product Owner onayı olmadan yayınlanamaz.",
        })
        sens_text = format_telegram_sensitive_review(sens_asm)
        self.assertIn("⚠️ EFSO RADAR | ÖZEL EDİTORYAL İNCELEME (SENSITIVE REVIEW)", sens_text)
        self.assertIn("GİZLİ & HASSAS İÇERİK — YALNIZCA EDİTÖR İNCELEME TAVSİYESİDİR", sens_text)
        self.assertIn("KESİN KURAL", sens_text)
        self.assertIn("Product Owner tekil ve açık onay vermedikçe yayınlanamaz", sens_text)

        # 4. HOLD_FOR_VERIFICATION
        hold_asm = self.assessment.model_copy(update={
            "disposition": EditorialDisposition.HOLD,
            "is_publishable": False,
            "verification_status": VerificationStatus.UNVERIFIED_RUMOR,
            "headline_tr": "Yeni Albüm İddiası",
            "uncertainties_tr": "[NOT_PUBLISHABLE - MANUAL VERIFICATION REQUIRED] Doğrulama bekletmesinde: Teyit gerektiren iddia.",
        })
        hold_text = format_telegram_verification_hold(hold_asm)
        self.assertIn("🔍 EFSO RADAR | DOĞRULAMA BEKLEYEN İDDİA (HOLD FOR VERIFICATION)", hold_text)
        self.assertIn("YAYINLANAMAZ — EDİTORYAL TEYİT BEKLEMESİNDE", hold_text)
        self.assertIn("Teyitsiz Söylenti / Magazin Dedikodusu", hold_text)
        self.assertIn("Instagram'da paylaşılamaz", hold_text)

    def test_simulated_delivery_error_403_fail_fast(self):
        """Permanent 403 Forbidden errors must fail fast without wasting retries."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(403, json={"ok": False, "description": "Forbidden: bot was blocked by the user"})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.FAILED)
        self.assertEqual(len(calls), 1, "Must fail fast on 403 without retrying")
        self.assertIn("403", record.error_message)

    def test_simulated_delivery_error_500_with_bounded_retries(self):
        """Transient 500 server errors are retried up to telegram_max_retries and succeed on recovery."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return httpx.Response(500, json={"ok": False, "description": "Internal Server Error"})
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99}})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.SENT)
        self.assertEqual(call_count, 3, "Must have retried twice and succeeded on 3rd attempt")

    def test_simulated_delivery_network_timeout_exhaustion(self):
        """Ambiguous network timeout must NOT automatically retry to prevent duplicate deliveries.
        
        Persists DELIVERY_UNCERTAIN state for safe reconciliation.
        """
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ConnectTimeout("Connection to api.telegram.org timed out")

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.DELIVERY_UNCERTAIN)
        self.assertEqual(attempts, 1, "Must NOT automatically retry ambiguous timeouts")
        self.assertIn("Ambiguous delivery timeout", record.error_message)
        # Token must not leak into error message
        self.assertNotIn("ABC-DEF1234ghIkl", record.error_message)

    def test_simulated_delivery_transport_connection_error_retries(self):
        """Non-timeout transport connection errors are retried up to telegram_max_retries."""
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ConnectError("Failed to establish new connection to api.telegram.org")

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.FAILED)
        self.assertEqual(attempts, 3, "Non-timeout transport errors retry up to max_retries")
        self.assertIn("Network transport error", record.error_message)
        self.assertNotIn("ABC-DEF1234ghIkl", record.error_message)

    def test_token_and_chat_id_redaction(self):
        """Secrets must be safely masked in utility helpers and representations."""
        masked_tok = self.settings.get_masked_telegram_token()
        self.assertEqual(masked_tok, "123456...ew11")
        self.assertNotIn("ABC-DEF1234ghIkl", masked_tok)

        masked_chat = self.settings.get_masked_chat_id()
        self.assertEqual(masked_chat, "12...89")

    def test_digest_batch_dispatch_mock(self):
        """Grouped editorial digest dispatches as EDITORIAL_DIGEST with multiple items."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 77}})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send_digest_batch(
            [self.assessment, self.assessment],
            notification_id="DIGEST-TEST-001",
            window_label="6 Saatlik Editoryal Özet",
        )
        self.assertEqual(record.status, DeliveryStatus.SENT)
        self.assertEqual(record.category, TelegramNotificationCategory.EDITORIAL_DIGEST)
        self.assertEqual(len(calls), 1)

    def test_restart_and_idempotency_behavior(self):
        """Notifications recorded in SQLite survive restart and prevent duplicate dispatches."""
        record = NotificationRecord(
            notification_id="NOTIF-001",
            story_id="STORY-TEST-IDEMP",
            kind=EditorialKind.NEW,
            channel="TELEGRAM",
            sent_at=self.now,
            status=DeliveryStatus.SENT,
            message_text="Alert text",
            headline_tr="Başlık",
            score=92,
            category=TelegramNotificationCategory.VERIFIED_BREAKING,
        )
        self.repo.record_notification(record)

        # Simulate fresh application instance/restart by re-opening repository
        fresh_repo = SqliteStateRepository(self.db_path)
        self.assertTrue(fresh_repo.has_story_been_alerted("STORY-TEST-IDEMP", kind=EditorialKind.NEW))

    def test_simulated_delivery_error_400_validation_distinction(self):
        """HTTP 400 Bad Request must be clearly distinguished from unauthorized recipients and fail fast."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(400, json={"ok": False, "description": "Bad Request: can't parse entities: unclosed tag"})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.FAILED)
        self.assertEqual(len(calls), 1, "Must fail fast on HTTP 400 without retrying")
        self.assertIn("Telegram request validation error (HTTP 400)", record.error_message)
        self.assertNotIn("Unauthorized recipient", record.error_message)

    def test_simulated_delivery_error_504_gateway_timeout_uncertain(self):
        """HTTP 504 Gateway Timeout must be treated as ambiguous and not retried automatically."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(504, text="<html>504 Gateway Time-out</html>")

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.DELIVERY_UNCERTAIN)
        self.assertEqual(len(calls), 1, "Must not retry ambiguous 504 Gateway Timeout")
        self.assertIn("Ambiguous Telegram server outcome (HTTP 504 Gateway Timeout)", record.error_message)

    def test_simulated_delivery_error_429_rate_limit(self):
        """HTTP 429 Rate limit exhausts retries and returns FAILED with rate limit info."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(429, headers={"Retry-After": "0.01"}, json={"ok": False, "description": "Too Many Requests"})

        transport = httpx.MockTransport(handler)
        channel = TelegramNotificationChannel(self.settings, transport=transport)

        record = channel.send(self.assessment, "STORY-001")
        self.assertEqual(record.status, DeliveryStatus.FAILED)
        self.assertEqual(len(calls), 3)
        self.assertIn("Telegram rate limit exceeded", record.error_message)

    def test_run_one_shot_connectivity_test_rejects_missing_chat_id(self):
        """Connectivity test rejects execution if chat ID is not explicitly configured."""
        bad_settings = self.settings.model_copy(update={"telegram_chat_id": ""})
        res = run_one_shot_connectivity_test(bad_settings)
        self.assertEqual(res["status"], "CONFIG_ERROR")
        self.assertFalse(res["attempt_made"])
        self.assertIn("explicitly configured private owner chat ID is required", res["error"])

    def test_run_one_shot_connectivity_test_rejects_unauthorized_chat_id(self):
        """Connectivity test strictly enforces allowlist and rejects unlisted chat ID without network call."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        bad_settings = self.settings.model_copy(update={
            "telegram_chat_id": self.unauthorized_chat_id,
            "telegram_allowed_chat_ids": [self.authorized_chat_id],
        })
        res = run_one_shot_connectivity_test(bad_settings, transport=transport)
        self.assertEqual(res["status"], "UNAUTHORIZED_RECIPIENT")
        self.assertFalse(res["attempt_made"])
        self.assertEqual(len(calls), 0, "Zero network calls made for unauthorized recipient")

    def test_run_one_shot_connectivity_test_dry_run_zero_network(self):
        """Pre-flight dry-run verifies configuration and generates preview without network calls."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True})

        transport = httpx.MockTransport(handler)
        res = run_one_shot_connectivity_test(self.settings, transport=transport, dry_run=True)
        self.assertEqual(res["status"], "SAFE_PREVIEW_PASSED")
        self.assertFalse(res["attempt_made"])
        self.assertEqual(len(calls), 0)
        self.assertIn("EFSO RADAR | TELEGRAM BAĞLANTI VE GÜVENLİK TESTİ", res["rendered_html"])

    def test_run_one_shot_connectivity_test_live_success_exactly_one_attempt(self):
        """Authorized live connectivity test makes exactly ONE attempt and returns message_id."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 4321}})

        transport = httpx.MockTransport(handler)
        res = run_one_shot_connectivity_test(self.settings, transport=transport, dry_run=False)
        self.assertEqual(res["status"], "DELIVERY_SUCCESS")
        self.assertTrue(res["attempt_made"])
        self.assertEqual(res["attempts_count"], 1)
        self.assertEqual(res["message_id"], 4321)
        self.assertEqual(len(calls), 1)

    def test_run_one_shot_connectivity_test_live_failure_no_retries(self):
        """Live connectivity test allows exactly ONE attempt and does not perform retry on failure."""
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(500, json={"ok": False, "description": "Server Error"})

        transport = httpx.MockTransport(handler)
        res = run_one_shot_connectivity_test(self.settings, transport=transport, dry_run=False)
        self.assertEqual(res["status"], "SERVER_ERROR")
        self.assertTrue(res["attempt_made"])
        self.assertEqual(res["attempts_count"], 1)
        self.assertEqual(len(calls), 1, "Must NOT retry during one-shot connectivity test")

    def test_fetch_bot_updates_for_chat_identification_safe_parsing(self):
        """Bot updates parsed safely without leaking tokens and without auto-trusting arbitrary senders."""
        raw_updates = {
            "ok": True,
            "result": [
                {
                    "update_id": 1001,
                    "message": {
                        "message_id": 501,
                        "from": {"id": 123456789, "username": "efso_owner", "first_name": "Emre"},
                        "chat": {"id": 123456789, "type": "private"},
                        "text": "/start",
                    },
                },
                {
                    "update_id": 1002,
                    "message": {
                        "message_id": 502,
                        "from": {"id": 999888777, "username": "stranger", "first_name": "Unknown"},
                        "chat": {"id": 999888777, "type": "private"},
                        "text": "Hello bot",
                    },
                },
            ],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=raw_updates)

        transport = httpx.MockTransport(handler)
        res = fetch_bot_updates_for_chat_identification(
            self.settings,
            transport=transport,
            expected_username="efso_owner",
        )
        self.assertEqual(res["status"], "SUCCESS")
        self.assertEqual(res["updates_count"], 2)
        candidates = res["candidates"]
        self.assertEqual(len(candidates), 2)
        self.assertTrue(candidates[0]["is_verified_match"])
        self.assertFalse(candidates[1]["is_verified_match"])
        # Token must not leak into result
        self.assertNotIn("ABC-DEF1234ghIkl", str(res))

    def test_token_masking_in_urls_and_exceptions(self):
        """sanitize_telegram_text cleanly strips bot tokens in raw strings, URLs, and exception text."""
        raw_token = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
        url = f"https://api.telegram.org/bot{raw_token}/sendMessage"
        sanitized = sanitize_telegram_text(url, raw_token)
        self.assertNotIn(raw_token, sanitized)
        self.assertIn("https://api.telegram.org/bot[REDACTED_BOT_TOKEN]/sendMessage", sanitized)

        exc_text = f"ConnectError: Failed to connect to https://api.telegram.org/bot{raw_token}/getUpdates: timed out"
        sanitized_exc = sanitize_telegram_text(exc_text, raw_token)
        self.assertNotIn(raw_token, sanitized_exc)
        self.assertIn("[REDACTED_BOT_TOKEN]", sanitized_exc)


if __name__ == "__main__":
    unittest.main()

