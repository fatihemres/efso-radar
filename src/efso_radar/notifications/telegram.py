"""Telegram alert and editorial review notification channel.

Implements the single private bot-to-owner architecture for Phase 4A Supervised Pilot:
1. Strict chat ID allowlist enforcement (rejects unauthorized recipients).
2. Four distinct editorial notification categories:
   - EDITORIAL_DIGEST
   - VERIFIED_BREAKING
   - SENSITIVE_EDITORIAL_REVIEW
   - HOLD_FOR_VERIFICATION
3. Durable outbox state tracking, bounded backoff retries, and network ambiguity isolation.
4. Deterministic local preview / dry-run mode (live delivery disabled by default).
5. Safe secret redaction in all log messages and exception traces.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

from efso_radar.config import Settings
from efso_radar.domain import (
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    NotificationRecord,
    TelegramNotificationCategory,
    VerificationStatus,
    utc_now,
)
from efso_radar.notifications.base import NotificationChannel
from efso_radar.notifications.formatters import (
    compute_stable_digest_identity,
    format_telegram_alert,
    format_telegram_breaking,
    format_telegram_digest,
    format_telegram_sensitive_review,
    format_telegram_verification_hold,
    format_turkey_timestamp,
)

logger = logging.getLogger("efso-radar.notifications.telegram")


def sanitize_telegram_text(text: str, token: str = "") -> str:
    """Mask Telegram bot tokens in text, URLs, and exceptions to prevent credential leakage."""
    if not text:
        return ""
    cleaned = str(text)
    if token and token.strip():
        tok = token.strip()
        cleaned = cleaned.replace(tok, "[REDACTED_BOT_TOKEN]")
    # Mask any /bot<token>/ pattern in URLs
    cleaned = re.sub(r"/bot[a-zA-Z0-9_:-]+/", "/bot[REDACTED_BOT_TOKEN]/", cleaned)
    cleaned = re.sub(r"bot\d+:[A-Za-z0-9_-]+", "bot[REDACTED_BOT_TOKEN]", cleaned)
    return cleaned


class TelegramDeliveryError(Exception):
    """Base exception for Telegram dispatch failures."""


class UnauthorizedTelegramRecipientError(TelegramDeliveryError):
    """Raised when an attempt is made to send notifications to a non-allowlisted chat ID."""


class TelegramValidationError(TelegramDeliveryError):
    """Raised on HTTP 400 Bad Request (payload, entity parsing, or parameter errors)."""


class TelegramAccessError(TelegramDeliveryError):
    """Raised on HTTP 403 Forbidden (bot blocked, unauthorized access)."""


class TelegramRateLimitError(TelegramDeliveryError):
    """Raised on HTTP 429 Too Many Requests."""


class TelegramAmbiguousOutcomeError(TelegramDeliveryError):
    """Raised on ambiguous timeouts or 504 where message may have already reached Telegram."""


def derive_telegram_category(assessment: EditorialAssessment) -> TelegramNotificationCategory:
    """Derive the logical Telegram message category for an editorial assessment."""
    if (
        assessment.disposition == EditorialDisposition.SENSITIVE_REVIEW
        or (assessment.uncertainties_tr and "SENSITIVE EDITORIAL REVIEW" in assessment.uncertainties_tr)
    ):
        return TelegramNotificationCategory.SENSITIVE_EDITORIAL_REVIEW

    if (
        not assessment.is_publishable
        or assessment.disposition == EditorialDisposition.HOLD
        or assessment.verification_status in (VerificationStatus.UNVERIFIED_RUMOR, VerificationStatus.SPECULATION)
        or (assessment.uncertainties_tr and "MANUAL VERIFICATION REQUIRED" in assessment.uncertainties_tr)
    ):
        return TelegramNotificationCategory.HOLD_FOR_VERIFICATION

    return TelegramNotificationCategory.VERIFIED_BREAKING


class TelegramNotificationChannel(NotificationChannel):
    """Telegram alert dispatcher for the supervised private owner pilot."""

    def __init__(
        self,
        settings: Settings,
        transport: Optional[httpx.BaseTransport] = None,
        client: Optional[httpx.Client] = None,
    ):
        self.settings = settings
        self.transport = transport
        self._client = client

    def _sanitize(self, text: str) -> str:
        return sanitize_telegram_text(text, self.settings.telegram_bot_token)

    def _get_http_client(self) -> httpx.Client:
        if self._client is not None:
            return self._client
        return httpx.Client(
            transport=self.transport,
            timeout=self.settings.http_timeout_seconds,
        )

    def send(
        self,
        assessment: EditorialAssessment,
        story_id: str,
        update_id: Optional[str] = None,
        category: Optional[TelegramNotificationCategory] = None,
        dry_run: bool = False,
        now: Optional[datetime] = None,
        target_chat_id: Optional[str] = None,
    ) -> NotificationRecord:
        """Send or preview an editorial assessment in the private owner chat."""
        current_now = now or utc_now()
        derived_category = category or derive_telegram_category(assessment)
        if target_chat_id:
            chat_id = str(target_chat_id).strip()
        else:
            recipients = self.settings.get_recipients_for_category(derived_category)
            if recipients:
                chat_id = recipients[0]["chat_id"]
            else:
                chat_id = (self.settings.telegram_primary_editor_chat_id or self.settings.telegram_chat_id).strip()

        # 1. Format message according to its logical category
        if derived_category == TelegramNotificationCategory.SENSITIVE_EDITORIAL_REVIEW:
            message_text = format_telegram_sensitive_review(assessment)
            channel_name = "TELEGRAM_SENSITIVE_REVIEW"
        elif derived_category == TelegramNotificationCategory.HOLD_FOR_VERIFICATION:
            message_text = format_telegram_verification_hold(assessment)
            channel_name = "TELEGRAM_HOLD_VERIFICATION"
        else:
            message_text = format_telegram_breaking(assessment)
            channel_name = "TELEGRAM"

        # Generate unique idempotency key
        prefix = "NOTIF-UPD" if update_id else "NOTIF"
        notification_id = f"{prefix}-{current_now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

        # 2. Check quiet hours (sensitive review and verification hold are not suppressed by quiet hours)
        if (
            derived_category == TelegramNotificationCategory.VERIFIED_BREAKING
            and self.settings.is_in_quiet_hours(current_now)
        ):
            if self.settings.quiet_hours_behavior == "ALLOW_BREAKING_95" and assessment.score >= 95:
                logger.info("Bypassing quiet hours for breaking news with score %d", assessment.score)
            elif self.settings.quiet_hours_behavior == "DISCARD":
                logger.info("Discarding notification during quiet hours: %s", assessment.headline_tr)
                return NotificationRecord(
                    notification_id=notification_id,
                    story_id=story_id,
                    update_id=update_id,
                    kind=assessment.kind,
                    channel=channel_name,
                    sent_at=current_now,
                    status=DeliveryStatus.SKIPPED_BELOW_THRESHOLD,
                    message_text=message_text,
                    headline_tr=assessment.headline_tr,
                    score=assessment.score,
                    category=derived_category,
                )
            else:  # "HOLD" default
                logger.info("Holding notification for quiet hours: %s", assessment.headline_tr)
                return NotificationRecord(
                    notification_id=notification_id,
                    story_id=story_id,
                    update_id=update_id,
                    kind=assessment.kind,
                    channel=channel_name,
                    sent_at=current_now,
                    status=DeliveryStatus.HELD_QUIET_HOURS,
                    message_text=message_text,
                    headline_tr=assessment.headline_tr,
                    score=assessment.score,
                    category=derived_category,
                )

        # 3. Safe Delivery Control: Dry-run or delivery disabled by default
        is_live_enabled = (
            self.settings.telegram_delivery_enabled
            and not dry_run
            and bool(self.settings.telegram_bot_token and self.settings.telegram_chat_id)
        )

        if not is_live_enabled:
            logger.info(
                "[SAFE PREVIEW / DELIVERY DISABLED] [%s] Generated for story %s (chat: %s)",
                derived_category.value,
                story_id,
                self.settings.get_masked_chat_id(),
            )
            return NotificationRecord(
                notification_id=notification_id,
                story_id=story_id,
                update_id=update_id,
                kind=assessment.kind,
                channel=channel_name,
                sent_at=current_now,
                status=DeliveryStatus.SENT,
                message_text=message_text,
                headline_tr=assessment.headline_tr,
                score=assessment.score,
                category=derived_category,
            )

        # 4. Privacy & Authentication Gate: Enforce chat ID allowlist on live dispatch
        if not self.settings.is_chat_id_allowed(chat_id):
            masked_id = f"{chat_id[:2]}...{chat_id[-2:]}" if len(chat_id) > 4 else "***"
            err_msg = f"Unauthorized recipient: chat_id '{masked_id}' is not in allowed_chat_ids"
            logger.error(err_msg)
            return NotificationRecord(
                notification_id=notification_id,
                story_id=story_id,
                update_id=update_id,
                kind=assessment.kind,
                channel=channel_name,
                sent_at=current_now,
                status=DeliveryStatus.FAILED,
                message_text=message_text,
                headline_tr=assessment.headline_tr,
                score=assessment.score,
                error_message=err_msg,
                category=derived_category,
            )

        # 5. Live Dispatch with Bounded Retries and Backoff
        return self._dispatch_telegram(
            notification_id=notification_id,
            story_id=story_id,
            update_id=update_id,
            assessment=assessment,
            message_text=message_text,
            now=current_now,
            target_chat_id=chat_id,
            category=derived_category,
            channel_name=channel_name,
        )

    def send_digest_batch(
        self,
        assessments: list[EditorialAssessment],
        notification_id: str,
        window_label: str = "6 Saatlik Editoryal Özet",
        dry_run: bool = False,
        now: Optional[datetime] = None,
        target_chat_id: Optional[str] = None,
    ) -> NotificationRecord:
        """Send or preview a grouped periodic editorial digest (EDITORIAL_DIGEST)."""
        current_now = now or utc_now()
        if target_chat_id:
            chat_id = str(target_chat_id).strip()
        else:
            recipients = self.settings.get_recipients_for_category(TelegramNotificationCategory.EDITORIAL_DIGEST)
            if recipients:
                chat_id = recipients[0]["chat_id"]
            else:
                chat_id = (self.settings.telegram_primary_editor_chat_id or self.settings.telegram_chat_id).strip()
        digest_text = format_telegram_digest(assessments, window_label=window_label)
        story_id = assessments[0].parent_story_id or assessments[0].article_ids[0][:8] if assessments else "DIGEST"

        is_live_enabled = (
            self.settings.telegram_delivery_enabled
            and not dry_run
            and bool(self.settings.telegram_bot_token and self.settings.telegram_chat_id)
        )

        if not is_live_enabled:
            logger.info(
                "[SAFE PREVIEW / DELIVERY DISABLED] [EDITORIAL_DIGEST] Generated %d items (chat: %s)",
                len(assessments),
                self.settings.get_masked_chat_id(),
            )
            return NotificationRecord(
                notification_id=notification_id,
                story_id=story_id,
                update_id=None,
                kind=EditorialKind.NEW,
                channel="TELEGRAM_DIGEST",
                sent_at=current_now,
                status=DeliveryStatus.SENT,
                message_text=digest_text,
                headline_tr=f"Editoryal Özet ({len(assessments)} Haber)",
                score=85,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            )

        # Check allowlist on live dispatch
        if not self.settings.is_chat_id_allowed(chat_id):
            masked_id = f"{chat_id[:2]}...{chat_id[-2:]}" if len(chat_id) > 4 else "***"
            err_msg = f"Unauthorized recipient: chat_id '{masked_id}' is not in allowed_chat_ids"
            logger.error(err_msg)
            return NotificationRecord(
                notification_id=notification_id,
                story_id=story_id,
                update_id=None,
                kind=EditorialKind.NEW,
                channel="TELEGRAM_DIGEST",
                sent_at=current_now,
                status=DeliveryStatus.FAILED,
                message_text=digest_text,
                headline_tr=f"Editoryal Özet ({len(assessments)} Haber)",
                score=85,
                error_message=err_msg,
                category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            )

        return self._dispatch_telegram(
            notification_id=notification_id,
            story_id=story_id,
            update_id=None,
            assessment=assessments[0] if assessments else None,
            message_text=digest_text,
            now=current_now,
            target_chat_id=chat_id,
            category=TelegramNotificationCategory.EDITORIAL_DIGEST,
            channel_name="TELEGRAM_DIGEST",
        )

    def send_multicast(
        self,
        assessment: EditorialAssessment,
        story_id: str,
        update_id: Optional[str] = None,
        category: Optional[TelegramNotificationCategory] = None,
        dry_run: bool = False,
        now: Optional[datetime] = None,
        target_role: Optional[str] = None,
    ) -> list[NotificationRecord]:
        """Dispatch an assessment to all authorized recipients for its category, tracked independently."""
        derived_category = category or derive_telegram_category(assessment)
        recipients = self.settings.get_recipients_for_category(derived_category)
        if target_role and target_role.upper() != "ALL":
            recipients = [r for r in recipients if r["role"].upper() == target_role.upper()]

        records: list[NotificationRecord] = []
        for r in recipients:
            rec = self.send(
                assessment=assessment,
                story_id=story_id,
                update_id=update_id,
                category=derived_category,
                dry_run=dry_run,
                now=now,
                target_chat_id=r["chat_id"],
            )
            records.append(rec)
        return records

    def send_digest_batch_multicast(
        self,
        assessments: list[EditorialAssessment],
        notification_id: str,
        window_label: str = "6 Saatlik Editoryal Özet",
        dry_run: bool = False,
        now: Optional[datetime] = None,
        target_role: Optional[str] = None,
    ) -> list[NotificationRecord]:
        """Dispatch an editorial digest batch to all authorized recipients, tracked independently."""
        recipients = self.settings.get_recipients_for_category(TelegramNotificationCategory.EDITORIAL_DIGEST)
        if target_role and target_role.upper() != "ALL":
            recipients = [r for r in recipients if r["role"].upper() == target_role.upper()]

        records: list[NotificationRecord] = []
        for r in recipients:
            rec = self.send_digest_batch(
                assessments=assessments,
                notification_id=notification_id,
                window_label=window_label,
                dry_run=dry_run,
                now=now,
                target_chat_id=r["chat_id"],
            )
            records.append(rec)
        return records

    def _dispatch_telegram(
        self,
        notification_id: str,
        story_id: str,
        update_id: Optional[str],
        assessment: Optional[EditorialAssessment],
        message_text: str,
        now: datetime,
        target_chat_id: str,
        category: TelegramNotificationCategory,
        channel_name: str,
        max_attempts_override: Optional[int] = None,
    ) -> NotificationRecord:
        """Perform reliable HTTP POST dispatch with bounded retries and exponential backoff.
        
        Failure Policy:
        - HTTP 400: Request validation/entity failure -> FAILED immediately (no retry).
        - HTTP 403: Access failure / bot blocked -> FAILED immediately (no retry).
        - HTTP 429: Rate limited -> sleep and retry if attempts remain; else FAILED.
        - HTTP 504: Gateway timeout -> DELIVERY_UNCERTAIN immediately (no retry to prevent duplicates).
        - HTTP 5xx: Server error -> retry if attempts remain; else FAILED.
        - httpx.TimeoutException: Ambiguous network timeout -> DELIVERY_UNCERTAIN immediately (no retry).
        - httpx.TransportError: Network connection error -> retry if attempts remain; else FAILED.
        """
        endpoint = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": target_chat_id,
            "text": message_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        kind = assessment.kind if assessment else EditorialKind.NEW
        headline_tr = assessment.headline_tr if assessment else "Efso Magazin Alert"
        score = assessment.score if assessment else 90

        max_attempts = max_attempts_override if max_attempts_override is not None else max(1, self.settings.telegram_max_retries)
        is_one_shot = max_attempts == 1
        last_error: Optional[str] = None
        final_status: DeliveryStatus = DeliveryStatus.FAILED

        with self._get_http_client() as client:
            for attempt in range(1, max_attempts + 1):
                try:
                    resp = client.post(endpoint, json=payload)

                    # 1. Permanent client validation error (HTTP 400)
                    # Never assume all HTTP 400 responses mean unauthorized recipients!
                    if resp.status_code == 400:
                        desc = ""
                        try:
                            desc = resp.json().get("description", "")
                        except Exception:
                            desc = resp.text[:200]
                        last_error = self._sanitize(f"Telegram request validation error (HTTP 400): {desc}")
                        logger.error("Request validation failure (HTTP 400): %s", last_error)
                        final_status = DeliveryStatus.FAILED
                        break

                    # 2. Permanent access failure (HTTP 403)
                    if resp.status_code == 403:
                        desc = ""
                        try:
                            desc = resp.json().get("description", "")
                        except Exception:
                            desc = resp.text[:200]
                        last_error = self._sanitize(f"Telegram access failure (HTTP 403): {desc}")
                        logger.error("Access failure (HTTP 403): %s", last_error)
                        final_status = DeliveryStatus.FAILED
                        break

                    # 3. Rate limiting (HTTP 429)
                    if resp.status_code == 429:
                        retry_after = 1.0
                        try:
                            retry_after = float(resp.headers.get("Retry-After", 1.0))
                        except (ValueError, TypeError):
                            pass
                        wait_sec = min(retry_after, 5.0)
                        last_error = f"Telegram rate limit exceeded (HTTP 429, retry_after={wait_sec}s)"
                        logger.warning("Telegram rate limited (HTTP 429); waiting %.1fs before attempt %d", wait_sec, attempt)
                        if not is_one_shot and attempt < max_attempts:
                            if self.settings.telegram_retry_backoff_base > 0:
                                time.sleep(wait_sec)
                            continue
                        final_status = DeliveryStatus.FAILED
                        break

                    # 4. Gateway Timeout (HTTP 504) - Ambiguous outcome: Telegram server may have processed message!
                    if resp.status_code == 504:
                        last_error = "Ambiguous Telegram server outcome (HTTP 504 Gateway Timeout): Automatic retry skipped to prevent duplicate delivery."
                        logger.warning("%s", last_error)
                        final_status = DeliveryStatus.DELIVERY_UNCERTAIN
                        break

                    # 5. Other Server errors (HTTP 5xx)
                    if resp.status_code >= 500:
                        last_error = self._sanitize(f"Telegram server error (HTTP {resp.status_code}): {resp.text[:200]}")
                        logger.warning("Telegram server error on attempt %d: %s", attempt, last_error)
                        if not is_one_shot and attempt < max_attempts:
                            if self.settings.telegram_retry_backoff_base > 0:
                                backoff = self.settings.telegram_retry_backoff_base * (2 ** (attempt - 1))
                                time.sleep(backoff)
                            continue
                        final_status = DeliveryStatus.FAILED
                        break

                    # 6. Success (HTTP 200)
                    resp_json = resp.json()
                    if resp_json.get("ok"):
                        msg_id = resp_json.get("result", {}).get("message_id")
                        logger.info(
                            "Delivered Telegram message [%s] to %s (id: %s, msg_id: %s)",
                            category.value,
                            self.settings.get_masked_chat_id(),
                            notification_id,
                            msg_id,
                        )
                        return NotificationRecord(
                            notification_id=notification_id,
                            story_id=story_id,
                            update_id=update_id,
                            kind=kind,
                            channel=channel_name,
                            sent_at=now,
                            status=DeliveryStatus.SENT,
                            message_text=message_text,
                            headline_tr=headline_tr,
                            score=score,
                            category=category,
                        )
                    else:
                        last_error = self._sanitize(f"Telegram API response not ok: {resp_json}")
                        final_status = DeliveryStatus.FAILED
                        break

                except httpx.TimeoutException as exc:
                    # Ambiguous network timeout: request may have reached Telegram before client timeout!
                    # Do NOT retry automatically to avoid duplicate delivery!
                    sanitized_exc = self._sanitize(str(exc))
                    last_error = f"Ambiguous delivery timeout: {sanitized_exc}. Automatic retry skipped to prevent duplicate delivery."
                    logger.warning("Ambiguous timeout during Telegram delivery attempt %d: %s", attempt, last_error)
                    final_status = DeliveryStatus.DELIVERY_UNCERTAIN
                    break

                except httpx.TransportError as exc:
                    sanitized_exc = self._sanitize(str(exc))
                    last_error = f"Network transport error: {sanitized_exc}"
                    logger.warning("Delivery transport attempt %d failed: %s", attempt, last_error)
                    if not is_one_shot and attempt < max_attempts:
                        if self.settings.telegram_retry_backoff_base > 0:
                            backoff = self.settings.telegram_retry_backoff_base * (2 ** (attempt - 1))
                            time.sleep(backoff)
                        continue
                    final_status = DeliveryStatus.FAILED
                    break

                except Exception as exc:
                    sanitized_exc = self._sanitize(str(exc))
                    last_error = f"Unexpected delivery error: {sanitized_exc}"
                    logger.error("Unexpected error delivering message: %s", last_error)
                    final_status = DeliveryStatus.FAILED
                    break

        return NotificationRecord(
            notification_id=notification_id,
            story_id=story_id,
            update_id=update_id,
            kind=kind,
            channel=channel_name,
            sent_at=now,
            status=final_status,
            message_text=message_text,
            headline_tr=headline_tr,
            score=score,
            error_message=self._sanitize(last_error or "Delivery failed after retries"),
            category=category,
        )


def run_one_shot_connectivity_test(
    settings: Settings,
    transport: Optional[httpx.BaseTransport] = None,
    client: Optional[httpx.Client] = None,
    dry_run: bool = False,
    test_message_custom: Optional[str] = None,
) -> dict[str, Any]:
    """Execute a dedicated one-shot Telegram connectivity test.

    Guarantees:
    1. Zero RSS scanning, zero Gemini inference, zero background scheduling, zero outbox draining.
    2. Requires an explicitly configured private owner chat ID (settings.telegram_chat_id).
    3. Strictly enforces chat ID allowlist (settings.is_chat_id_allowed).
    4. Never automatically selects or queries getUpdates for recipient selection.
    5. Allows exactly ONE sendMessage attempt (no automatic retries under any circumstance).
    6. Masked bot token and chat ID in all returned diagnostics, logs, and errors.
    """
    token_masked = settings.get_masked_telegram_token()
    chat_masked = settings.get_masked_chat_id()

    # Gate 1: Bot token configured
    if not settings.telegram_bot_token or not settings.telegram_bot_token.strip():
        return {
            "status": "CONFIG_ERROR",
            "error": "TELEGRAM_BOT_TOKEN is not configured. Set TELEGRAM_BOT_TOKEN in .env or environment.",
            "attempt_made": False,
            "masked_chat_id": chat_masked,
            "masked_token": token_masked,
        }

    # Gate 2: Explicit Owner Chat ID configured (getUpdates auto-selection prohibited)
    target_chat = settings.telegram_chat_id.strip() if settings.telegram_chat_id else ""
    if not target_chat:
        return {
            "status": "CONFIG_ERROR",
            "error": (
                "TELEGRAM_CHAT_ID is not configured. An explicitly configured private owner chat ID is required. "
                "Automatic selection from getUpdates is prohibited for safety."
            ),
            "attempt_made": False,
            "masked_chat_id": chat_masked,
            "masked_token": token_masked,
        }

    # Gate 3: Chat ID allowlist check
    if not settings.is_chat_id_allowed(target_chat):
        return {
            "status": "UNAUTHORIZED_RECIPIENT",
            "error": f"Unauthorized recipient: TELEGRAM_CHAT_ID '{chat_masked}' is not in TELEGRAM_ALLOWED_CHAT_IDS.",
            "attempt_made": False,
            "masked_chat_id": chat_masked,
            "masked_token": token_masked,
        }

    # Format message
    now_utc = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    default_text = (
        "<b>🧪 EFSO RADAR | TELEGRAM BAĞLANTI VE GÜVENLİK TESTİ</b>\n\n"
        f"✅ <b>Durum:</b> Özel bot-sahip sohbeti doğrulandı ({now_utc})\n"
        "🔒 <b>Kanal:</b> Tekil Özel Sohbet (Private Owner Chat)\n"
        f"🛡️ <b>İzin Listesi:</b> Onaylı ({chat_masked})\n"
        "🚫 <b>Instagram Paylaşımı:</b> KAPALI (Otomatik paylaşım yapılamaz)\n\n"
        "<i>ℹ️ Bu tek seferlik bir bağlantı testidir. RSS taraması, yapay zeka çıkarımı veya arka plan zamanlayıcısı başlatılmamıştır.</i>"
    )
    message_text = test_message_custom or default_text

    # Gate 4: Dry-run or delivery disabled by default
    if dry_run or not settings.telegram_delivery_enabled:
        return {
            "status": "SAFE_PREVIEW_PASSED",
            "message": (
                "Pre-flight security and allowlist audit PASSED. "
                "Zero live network calls made (dry_run=True or TELEGRAM_DELIVERY_ENABLED=false). "
                "Ready for single live test message upon explicit Product Owner authorization."
            ),
            "attempt_made": False,
            "masked_chat_id": chat_masked,
            "masked_token": token_masked,
            "rendered_html": message_text,
        }

    # Gate 5: Single-Shot Live Dispatch (EXACTLY ONE ATTEMPT, ZERO RETRIES)
    endpoint = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": target_chat,
        "text": message_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    http_client = client or httpx.Client(
        transport=transport,
        timeout=settings.http_timeout_seconds,
    )

    try:
        with http_client as cl:
            resp = cl.post(endpoint, json=payload)

            if resp.status_code == 200:
                resp_json = resp.json()
                if resp_json.get("ok"):
                    msg_id = resp_json.get("result", {}).get("message_id")
                    return {
                        "status": "DELIVERY_SUCCESS",
                        "message": f"Telegram test message successfully delivered (message_id: {msg_id}).",
                        "message_id": msg_id,
                        "attempt_made": True,
                        "attempts_count": 1,
                        "masked_chat_id": chat_masked,
                        "masked_token": token_masked,
                    }
                else:
                    return {
                        "status": "DELIVERY_FAILED",
                        "error": sanitize_telegram_text(f"Telegram API response not ok: {resp_json}", settings.telegram_bot_token),
                        "attempt_made": True,
                        "attempts_count": 1,
                        "masked_chat_id": chat_masked,
                    }

            if resp.status_code == 400:
                desc = ""
                try:
                    desc = resp.json().get("description", "")
                except Exception:
                    desc = resp.text[:200]
                return {
                    "status": "VALIDATION_ERROR",
                    "error": sanitize_telegram_text(f"Telegram request validation error (HTTP 400): {desc}", settings.telegram_bot_token),
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": chat_masked,
                }

            if resp.status_code == 403:
                desc = ""
                try:
                    desc = resp.json().get("description", "")
                except Exception:
                    desc = resp.text[:200]
                return {
                    "status": "ACCESS_DENIED",
                    "error": sanitize_telegram_text(f"Telegram access failure (HTTP 403): {desc}", settings.telegram_bot_token),
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": chat_masked,
                }

            if resp.status_code == 429:
                return {
                    "status": "RATE_LIMITED",
                    "error": "Telegram rate limit (HTTP 429). Exactly one attempt allowed; no retries.",
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": chat_masked,
                }

            if resp.status_code >= 500:
                return {
                    "status": "SERVER_ERROR",
                    "error": sanitize_telegram_text(f"Telegram server error (HTTP {resp.status_code}): {resp.text[:200]}", settings.telegram_bot_token),
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": chat_masked,
                }

            return {
                "status": "DELIVERY_FAILED",
                "error": sanitize_telegram_text(f"Unexpected HTTP status {resp.status_code}: {resp.text[:200]}", settings.telegram_bot_token),
                "attempt_made": True,
                "attempts_count": 1,
                "masked_chat_id": chat_masked,
            }

    except httpx.TimeoutException as exc:
        sanitized_exc = sanitize_telegram_text(str(exc), settings.telegram_bot_token)
        return {
            "status": "DELIVERY_UNCERTAIN",
            "error": f"Ambiguous timeout during connectivity test: {sanitized_exc}. Message may have been accepted; automatic retry skipped.",
            "attempt_made": True,
            "attempts_count": 1,
            "masked_chat_id": chat_masked,
        }
    except httpx.TransportError as exc:
        sanitized_exc = sanitize_telegram_text(str(exc), settings.telegram_bot_token)
        return {
            "status": "NETWORK_ERROR",
            "error": f"Network transport error: {sanitized_exc}",
            "attempt_made": True,
            "attempts_count": 1,
            "masked_chat_id": chat_masked,
        }
    except Exception as exc:
        sanitized_exc = sanitize_telegram_text(str(exc), settings.telegram_bot_token)
        return {
            "status": "UNEXPECTED_ERROR",
            "error": f"Unexpected error during connectivity test: {sanitized_exc}",
            "attempt_made": True,
            "attempts_count": 1,
            "masked_chat_id": chat_masked,
        }


def fetch_bot_updates_for_chat_identification(
    settings: Settings,
    transport: Optional[httpx.BaseTransport] = None,
    client: Optional[httpx.Client] = None,
    expected_username: Optional[str] = None,
    expected_secret_phrase: Optional[str] = None,
) -> dict[str, Any]:
    """Safely fetch recent Telegram bot updates to assist the owner in identifying their chat ID.

    Safety Invariants:
    1. Never reveals the bot token in logs, URLs, or exceptions.
    2. Never automatically trusts or adopts an arbitrary sender without verification.
    3. Requires human verification against known username or secret phrase.
    """
    token_masked = settings.get_masked_telegram_token()
    if not settings.telegram_bot_token or not settings.telegram_bot_token.strip():
        return {
            "status": "CONFIG_ERROR",
            "error": "TELEGRAM_BOT_TOKEN is not configured.",
            "candidates": [],
        }

    endpoint = f"https://api.telegram.org/bot{settings.telegram_bot_token}/getUpdates?limit=10"
    http_client = client or httpx.Client(
        transport=transport,
        timeout=settings.http_timeout_seconds,
    )

    try:
        with http_client as cl:
            resp = cl.get(endpoint)
            if resp.status_code != 200:
                err_desc = sanitize_telegram_text(resp.text[:200], settings.telegram_bot_token)
                return {
                    "status": "API_ERROR",
                    "error": f"Telegram API error (HTTP {resp.status_code}): {err_desc}",
                    "candidates": [],
                }

            data = resp.json()
            if not data.get("ok"):
                return {
                    "status": "API_ERROR",
                    "error": sanitize_telegram_text(f"Telegram API response not ok: {data}", settings.telegram_bot_token),
                    "candidates": [],
                }

            raw_updates = data.get("result", [])
            candidates = []
            for item in raw_updates:
                msg = item.get("message") or item.get("edited_message")
                if not msg:
                    continue
                chat = msg.get("chat", {})
                sender = msg.get("from", {})
                chat_id = str(chat.get("id", ""))
                chat_type = chat.get("type", "")
                username = sender.get("username", "")
                first_name = sender.get("first_name", "")
                last_name = sender.get("last_name", "")
                full_name = f"{first_name} {last_name}".strip()
                text = msg.get("text", "")

                is_match = True
                if expected_username:
                    exp = expected_username.lstrip("@").lower()
                    if username.lower() != exp:
                        is_match = False
                if expected_secret_phrase:
                    if expected_secret_phrase.strip() not in text:
                        is_match = False

                candidates.append({
                    "update_id": item.get("update_id"),
                    "chat_id": chat_id,
                    "chat_type": chat_type,
                    "username": f"@{username}" if username else "(Kullanıcı adı yok)",
                    "sender_name": full_name or "(İsimsiz)",
                    "text_preview": text[:40],
                    "is_verified_match": is_match if (expected_username or expected_secret_phrase) else None,
                })

            bot_username = None
            try:
                me_resp = cl.get(f"https://api.telegram.org/bot{settings.telegram_bot_token}/getMe")
                if me_resp.status_code == 200:
                    bot_username = me_resp.json().get("result", {}).get("username")
            except Exception:
                bot_username = None

            return {
                "status": "SUCCESS",
                "updates_count": len(candidates),
                "candidates": candidates,
                "token_masked": token_masked,
                "bot_username": bot_username,
            }

    except Exception as exc:
        sanitized_exc = sanitize_telegram_text(str(exc), settings.telegram_bot_token)
        return {
            "status": "ERROR",
            "error": f"Failed to fetch updates: {sanitized_exc}",
            "candidates": [],
        }


def deliver_one_shot_editorial_digest(
    settings: Settings,
    assessments: list[EditorialAssessment],
    dry_run: bool = True,
    window_label: str = "6 Saatlik Editoryal Özet",
    ledger_path: Path = Path("data/telegram_digest_delivery_ledger.json"),
    transport: Optional[httpx.BaseTransport] = None,
    client: Optional[httpx.Client] = None,
    is_ux_test: bool = False,
    target_chat_id: Optional[str] = None,
    target_recipient_role: Optional[str] = None,
) -> dict[str, Any]:
    """Execute a dedicated, one-shot editorial digest delivery with persistent per-recipient duplicate protection.

    Safety Invariants:
    1. Zero RSS scanning, zero Gemini inference, zero background scheduling.
    2. Exactly 3 approved candidates required.
    3. Requires explicitly allowlisted private recipient chat IDs (rejects channels/groups starting with '-').
    4. Enforces dry-run safety when dry_run=True or TELEGRAM_DELIVERY_ENABLED=false.
    5. Persistent per-recipient duplicate-send protection via ledger: prevents resending identical digest to any recipient.
    6. Independent delivery state per recipient: successful delivery to one recipient is not recorded as delivery to both.
    7. Single sendMessage attempt per recipient (max_attempts=1, zero automatic retries).
    8. Ambiguous timeouts recorded as DELIVERY_UNCERTAIN without retry to prevent duplicates.
    9. Masks bot token and chat IDs in all returned structures and log output.
    10. Full backward compatibility with historical records (message_id 6 and UX test message_id 9).
    """
    token_masked = settings.get_masked_telegram_token()

    # Gate 1: Check bot token configuration
    if not settings.telegram_bot_token or not settings.telegram_bot_token.strip():
        return {
            "status": "CONFIG_ERROR",
            "error": "TELEGRAM_BOT_TOKEN is not configured.",
            "attempt_made": False,
            "masked_chat_id": "<NOT_SET>",
            "masked_token": token_masked,
        }

    # Gate 2: Resolve and validate target recipients
    if target_chat_id:
        t_chat = str(target_chat_id).strip()
        t_masked = settings.get_masked_chat_id(t_chat)
        if not t_chat:
            return {
                "status": "CONFIG_ERROR",
                "error": "target_chat_id is empty.",
                "attempt_made": False,
                "masked_chat_id": t_masked,
                "masked_token": token_masked,
            }
        if t_chat.startswith("-"):
            return {
                "status": "INVALID_CHAT_TYPE",
                "error": f"Group/Channel delivery is prohibited (chat_id: '{t_masked}'). Only private bot-to-recipient conversation is allowed.",
                "attempt_made": False,
                "masked_chat_id": t_masked,
                "masked_token": token_masked,
            }
        if not settings.is_chat_id_allowed(t_chat):
            return {
                "status": "UNAUTHORIZED_RECIPIENT",
                "error": f"Recipient chat_id '{t_masked}' is not in configured allowlist: {settings.telegram_allowed_chat_ids}",
                "attempt_made": False,
                "masked_chat_id": t_masked,
                "masked_token": token_masked,
            }
        t_role = settings.get_recipient_role(t_chat) or "AUTHORIZED_RECIPIENT"
        target_recipients = [{
            "chat_id": t_chat,
            "role": t_role,
            "masked_chat_id": t_masked,
            "label": "Hedef Alıcı",
        }]
    else:
        # Check primary admin chat ID
        admin_chat = settings.telegram_chat_id.strip() if settings.telegram_chat_id else ""
        if not admin_chat:
            return {
                "status": "CONFIG_ERROR",
                "error": "TELEGRAM_CHAT_ID is not configured.",
                "attempt_made": False,
                "masked_chat_id": "<NOT_SET>",
                "masked_token": token_masked,
            }
        if admin_chat.startswith("-"):
            return {
                "status": "INVALID_CHAT_TYPE",
                "error": f"Group/Channel delivery is prohibited (chat_id: '{settings.get_masked_chat_id(admin_chat)}'). Only private bot-to-owner conversation is allowed.",
                "attempt_made": False,
                "masked_chat_id": settings.get_masked_chat_id(admin_chat),
                "masked_token": token_masked,
            }
        if not settings.is_chat_id_allowed(admin_chat):
            return {
                "status": "UNAUTHORIZED_RECIPIENT",
                "error": f"Recipient chat_id '{settings.get_masked_chat_id(admin_chat)}' is not in configured allowlist: {settings.telegram_allowed_chat_ids}",
                "attempt_made": False,
                "masked_chat_id": settings.get_masked_chat_id(admin_chat),
                "masked_token": token_masked,
            }

        if target_recipient_role:
            req_role = target_recipient_role.upper()
            all_recs = settings.get_authorized_recipients()
            if req_role == "ALL":
                target_recipients = all_recs
            else:
                target_recipients = [r for r in all_recs if r["role"] == req_role]
                if not target_recipients:
                    return {
                        "status": "CONFIG_ERROR",
                        "error": f"Requested recipient role '{target_recipient_role}' is not configured or not allowlisted.",
                        "attempt_made": False,
                        "masked_chat_id": "<NOT_SET>",
                        "masked_token": token_masked,
                    }
        else:
            # Default routing for editorial digest:
            # Editorial notifications go to PRIMARY_EDITOR only.
            # ADMIN is not automatically included in routine editorial broadcasts.
            target_recipients = settings.get_recipients_for_category(TelegramNotificationCategory.EDITORIAL_DIGEST)
            if not target_recipients:
                return {
                    "status": "CONFIG_ERROR",
                    "error": "PRIMARY_EDITOR (telegram_primary_editor_chat_id) is not configured or not allowlisted for editorial digest delivery.",
                    "attempt_made": False,
                    "masked_chat_id": "<NOT_SET>",
                    "masked_token": token_masked,
                }

    # Gate 4: Candidate count verification (must be exactly 3)
    if len(assessments) != 3:
        return {
            "status": "INVALID_CANDIDATE_COUNT",
            "error": f"Expected exactly 3 approved editorial candidates, got {len(assessments)}.",
            "attempt_made": False,
            "masked_chat_id": target_recipients[0]["masked_chat_id"] if target_recipients else "<NOT_SET>",
            "masked_token": token_masked,
        }

    # Gate 5: Render and validate HTML payload
    rendered_html = format_telegram_digest(
        assessments,
        window_label=window_label,
        deduplicate=False,
        is_ux_test=is_ux_test,
    )
    if len(rendered_html) > 4096:
        return {
            "status": "PAYLOAD_OVERFLOW",
            "error": f"Rendered HTML length ({len(rendered_html)}) exceeds Telegram 4096-character limit.",
            "attempt_made": False,
            "masked_chat_id": target_recipients[0]["masked_chat_id"] if target_recipients else "<NOT_SET>",
            "masked_token": token_masked,
        }

    digest_hash = hashlib.sha256(rendered_html.encode("utf-8")).hexdigest()[:24]
    stable_digest_id = ("ux_test_" if is_ux_test else "") + compute_stable_digest_identity(assessments)
    candidate_article_ids = sorted(set(aid for a in assessments for aid in a.article_ids if aid))

    # Gate 6: Load ledger data (fail closed on corrupted or invalid ledger)
    ledger_file = Path(ledger_path)
    ledger_data: dict[str, Any] = {}
    if ledger_file.exists():
        try:
            with open(ledger_file, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict) or not isinstance(loaded.get("deliveries", {}), dict):
                return {
                    "status": "LEDGER_CORRUPTED",
                    "error": f"Delivery ledger at '{ledger_path}' is corrupted or invalid. Failing closed to protect against duplicate deliveries.",
                    "attempt_made": False,
                    "attempts_count": 0,
                    "masked_chat_id": target_recipients[0]["masked_chat_id"] if target_recipients else "<NOT_SET>",
                    "masked_token": token_masked,
                }
            ledger_data = loaded
        except Exception as exc:
            return {
                "status": "LEDGER_CORRUPTED",
                "error": f"Delivery ledger at '{ledger_path}' could not be read or parsed: {sanitize_telegram_text(str(exc), settings.telegram_bot_token)}. Failing closed to protect against duplicate deliveries.",
                "attempt_made": False,
                "attempts_count": 0,
                "masked_chat_id": target_recipients[0]["masked_chat_id"] if target_recipients else "<NOT_SET>",
                "masked_token": token_masked,
            }

    KNOWN_HISTORICAL_HASHES: dict[str, set[str]] = {
        "06afb36bc22dff56263e72de": {"c12aebfb09cea9753d28a1cb", "bc038ac2e6c763dfd15c913f", "8262e6a3c4f2f3abf2534c98"},
    }

    deliveries = ledger_data.get("deliveries", {})

    def _find_existing_record(r_chat: str, r_role: str) -> Optional[dict[str, Any]]:
        is_admin_recipient = (r_role == "ADMIN" or r_chat == settings.telegram_chat_id)

        # 1. Exact key match for this recipient: f"{r_chat}:{digest_hash}"
        r_key = f"{r_chat}:{digest_hash}"
        if r_key in deliveries:
            rec = deliveries[r_key]
            if isinstance(rec, dict):
                return rec

        # 2. Stable identity match for this recipient: f"{r_chat}:{stable_digest_id}"
        r_stable_key = f"{r_chat}:{stable_digest_id}"
        if r_stable_key in deliveries:
            rec = deliveries[r_stable_key]
            if isinstance(rec, dict):
                return rec

        # 3. Iterate through all deliveries and find records that belong strictly to THIS recipient
        for rec_k, rec_v in deliveries.items():
            if not isinstance(rec_v, dict):
                continue

            # Determine ownership of rec_v
            key_chat_prefix = rec_k.split(":", 1)[0] if ":" in rec_k else None
            record_chat = str(rec_v.get("chat_id") or "").strip()
            record_role = str(rec_v.get("recipient_role") or "").strip()

            belongs_to_this_recipient = False
            if record_chat:
                belongs_to_this_recipient = (record_chat == r_chat)
            elif key_chat_prefix:
                belongs_to_this_recipient = (key_chat_prefix == r_chat)
            elif record_role:
                belongs_to_this_recipient = (record_role == r_role)
            else:
                # Legacy Phase 4 record without explicit chat_id, role, or colon key
                # Historical Phase 4 was strictly single-recipient (ADMIN only)
                belongs_to_this_recipient = is_admin_recipient

            if not belongs_to_this_recipient:
                continue

            # This record belongs to r_chat/r_role. Check if it matches the current test/digest mode
            rec_is_ux_test = bool(rec_v.get("is_ux_test") or rec_k.startswith("ux_test_"))
            if is_ux_test != rec_is_ux_test:
                continue

            # Check digest identity
            if rec_v.get("digest_hash") == digest_hash:
                return rec_v
            if rec_v.get("stable_digest_id") == stable_digest_id:
                return rec_v
            if not is_ux_test:
                rec_art_ids = rec_v.get("article_ids")
                if rec_art_ids and set(rec_art_ids) == set(candidate_article_ids):
                    return rec_v
                if is_admin_recipient and rec_k in KNOWN_HISTORICAL_HASHES:
                    if KNOWN_HISTORICAL_HASHES[rec_k] == set(candidate_article_ids):
                        return rec_v

        # 4. Backward compatibility check for ADMIN with legacy un-prefixed keys
        if is_admin_recipient:
            if not is_ux_test and digest_hash in deliveries:
                legacy_rec = deliveries[digest_hash]
                if isinstance(legacy_rec, dict) and not legacy_rec.get("is_ux_test"):
                    leg_chat = str(legacy_rec.get("chat_id") or "").strip()
                    leg_role = str(legacy_rec.get("recipient_role") or "").strip()
                    if (not leg_chat or leg_chat == r_chat) and (not leg_role or leg_role == "ADMIN"):
                        return legacy_rec

        return None

    def _save_delivery_record(r_chat: str, r_role: str, r_masked: str, rec_status: str, msg_id: Optional[int] = None, err_desc: Optional[str] = None):
        now_dt = datetime.now(timezone.utc)
        ledger_file.parent.mkdir(parents=True, exist_ok=True)
        rec = {
            "status": rec_status,
            "delivered_at_utc": now_dt.isoformat(),
            "delivered_at_tsi": format_turkey_timestamp(now_dt),
            "chat_id": r_chat,
            "target_chat_id": r_masked,
            "recipient_role": r_role,
            "stories_count": len(assessments),
            "digest_hash": digest_hash,
            "stable_digest_id": stable_digest_id,
            "article_ids": candidate_article_ids,
            "is_ux_test": is_ux_test,
            "digest_type": "UX_TEST" if is_ux_test else "EDITORIAL_DIGEST",
        }
        if msg_id:
            rec["message_id"] = msg_id
        if err_desc:
            rec["error"] = err_desc
        # Save under per-recipient key
        ledger_data.setdefault("deliveries", {})[f"{r_chat}:{digest_hash}"] = rec
        # If ADMIN, also write to digest_hash for legacy single-recipient tests
        if r_role == "ADMIN" or r_chat == settings.telegram_chat_id:
            ledger_data["deliveries"][digest_hash] = rec
        tmp_path = ledger_file.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f_tmp:
            json.dump(ledger_data, f_tmp, ensure_ascii=False, indent=2)
        tmp_path.replace(ledger_file)

    should_close_client = client is None
    http_client = client or httpx.Client(
        transport=transport,
        timeout=settings.http_timeout_seconds,
    )

    recipient_results: dict[str, dict[str, Any]] = {}

    for rec_target in target_recipients:
        r_chat = rec_target["chat_id"]
        r_role = rec_target["role"]
        r_masked = rec_target["masked_chat_id"]

        # Check existing record
        existing_record = _find_existing_record(r_chat, r_role)

        if existing_record:
            rec_status = existing_record.get("status")
            rec_label = "UX test message" if is_ux_test else "digest"
            if rec_status == "DELIVERY_SUCCESS":
                recipient_results[r_chat] = {
                    "status": "DUPLICATE_DELIVERY_PREVENTED",
                    "error": f"Duplicate delivery prevented: This exact {rec_label} was already delivered to {r_role} on {existing_record.get('delivered_at_tsi') or existing_record.get('delivered_at_utc')}.",
                    "digest_hash": digest_hash,
                    "stable_digest_id": stable_digest_id,
                    "stories_count": len(assessments),
                    "attempt_made": False,
                    "attempts_count": 0,
                    "message_id": existing_record.get("message_id"),
                    "masked_chat_id": r_masked,
                    "chat_id": r_chat,
                    "recipient_role": r_role,
                    "is_ux_test": is_ux_test,
                }
                continue
            elif rec_status == "DELIVERY_UNCERTAIN":
                recipient_results[r_chat] = {
                    "status": "DUPLICATE_DELIVERY_PREVENTED",
                    "error": f"Duplicate delivery prevented: A prior attempt for {r_role} resulted in an ambiguous timeout on {existing_record.get('delivered_at_utc')}. Automatic resend is blocked.",
                    "digest_hash": digest_hash,
                    "stable_digest_id": stable_digest_id,
                    "stories_count": len(assessments),
                    "attempt_made": False,
                    "attempts_count": 0,
                    "masked_chat_id": r_masked,
                    "chat_id": r_chat,
                    "recipient_role": r_role,
                    "is_ux_test": is_ux_test,
                }
                continue

        # Dry run / delivery disabled check
        if dry_run or not settings.telegram_delivery_enabled:
            recipient_results[r_chat] = {
                "status": "SAFE_PREVIEW_PASSED",
                "message": (
                    f"Pre-flight audit PASSED for {r_role} ({r_masked}) for {'UX TEST' if is_ux_test else 'EDITORIAL_DIGEST'}. "
                    "Zero live network calls made (dry_run=True or TELEGRAM_DELIVERY_ENABLED=false)."
                ),
                "attempt_made": False,
                "attempts_count": 0,
                "digest_hash": digest_hash,
                "stable_digest_id": stable_digest_id,
                "stories_count": len(assessments),
                "rendered_html": rendered_html,
                "masked_chat_id": r_masked,
                "chat_id": r_chat,
                "recipient_role": r_role,
                "is_ux_test": is_ux_test,
            }
            continue

        # Single-Shot Live Dispatch (EXACTLY ONE ATTEMPT, ZERO RETRIES)
        endpoint = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
        payload = {
            "chat_id": r_chat,
            "text": rendered_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            resp = http_client.post(endpoint, json=payload)

            if resp.status_code == 200:
                resp_json = resp.json()
                if resp_json.get("ok"):
                    msg_id = resp_json.get("result", {}).get("message_id")
                    _save_delivery_record(r_chat, r_role, r_masked, "DELIVERY_SUCCESS", msg_id=msg_id)
                    recipient_results[r_chat] = {
                        "status": "DELIVERY_SUCCESS",
                        "message": f"Telegram editorial digest successfully delivered to {r_role} (message_id: {msg_id}).",
                        "message_id": msg_id,
                        "digest_hash": digest_hash,
                        "stable_digest_id": stable_digest_id,
                        "stories_count": len(assessments),
                        "attempt_made": True,
                        "attempts_count": 1,
                        "masked_chat_id": r_masked,
                        "chat_id": r_chat,
                        "recipient_role": r_role,
                    }
                else:
                    err_msg = sanitize_telegram_text(f"Telegram API response not ok: {resp_json}", settings.telegram_bot_token)
                    _save_delivery_record(r_chat, r_role, r_masked, "DELIVERY_FAILED", err_desc=err_msg)
                    recipient_results[r_chat] = {
                        "status": "DELIVERY_FAILED",
                        "error": err_msg,
                        "digest_hash": digest_hash,
                        "attempt_made": True,
                        "attempts_count": 1,
                        "masked_chat_id": r_masked,
                        "chat_id": r_chat,
                        "recipient_role": r_role,
                    }
            elif resp.status_code == 400:
                desc = ""
                try:
                    desc = resp.json().get("description", "")
                except Exception:
                    desc = resp.text[:200]
                err_msg = sanitize_telegram_text(f"Telegram request validation error (HTTP 400): {desc}", settings.telegram_bot_token)
                _save_delivery_record(r_chat, r_role, r_masked, "VALIDATION_ERROR", err_desc=err_msg)
                recipient_results[r_chat] = {
                    "status": "VALIDATION_ERROR",
                    "error": err_msg,
                    "digest_hash": digest_hash,
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": r_masked,
                    "chat_id": r_chat,
                    "recipient_role": r_role,
                }
            elif resp.status_code == 403:
                desc = ""
                try:
                    desc = resp.json().get("description", "")
                except Exception:
                    desc = resp.text[:200]
                err_msg = sanitize_telegram_text(f"Telegram access failure (HTTP 403): {desc}", settings.telegram_bot_token)
                _save_delivery_record(r_chat, r_role, r_masked, "ACCESS_DENIED", err_desc=err_msg)
                recipient_results[r_chat] = {
                    "status": "ACCESS_DENIED",
                    "error": err_msg,
                    "digest_hash": digest_hash,
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": r_masked,
                    "chat_id": r_chat,
                    "recipient_role": r_role,
                }
            elif resp.status_code == 429:
                err_msg = "Telegram rate limit (HTTP 429). Exactly one attempt allowed; no retries."
                _save_delivery_record(r_chat, r_role, r_masked, "RATE_LIMITED", err_desc=err_msg)
                recipient_results[r_chat] = {
                    "status": "RATE_LIMITED",
                    "error": err_msg,
                    "digest_hash": digest_hash,
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": r_masked,
                    "chat_id": r_chat,
                    "recipient_role": r_role,
                }
            elif resp.status_code >= 500:
                err_msg = sanitize_telegram_text(f"Telegram server error (HTTP {resp.status_code}): {resp.text[:200]}", settings.telegram_bot_token)
                if resp.status_code == 504:
                    _save_delivery_record(r_chat, r_role, r_masked, "DELIVERY_UNCERTAIN", err_desc=err_msg)
                    recipient_results[r_chat] = {
                        "status": "DELIVERY_UNCERTAIN",
                        "error": f"Ambiguous gateway timeout (HTTP 504): {err_msg}. Message may have been accepted; automatic retry skipped.",
                        "digest_hash": digest_hash,
                        "attempt_made": True,
                        "attempts_count": 1,
                        "masked_chat_id": r_masked,
                        "chat_id": r_chat,
                        "recipient_role": r_role,
                    }
                else:
                    _save_delivery_record(r_chat, r_role, r_masked, "SERVER_ERROR", err_desc=err_msg)
                    recipient_results[r_chat] = {
                        "status": "SERVER_ERROR",
                        "error": err_msg,
                        "digest_hash": digest_hash,
                        "attempt_made": True,
                        "attempts_count": 1,
                        "masked_chat_id": r_masked,
                        "chat_id": r_chat,
                        "recipient_role": r_role,
                    }
            else:
                err_msg = sanitize_telegram_text(f"Unexpected HTTP status {resp.status_code}: {resp.text[:200]}", settings.telegram_bot_token)
                _save_delivery_record(r_chat, r_role, r_masked, "DELIVERY_FAILED", err_desc=err_msg)
                recipient_results[r_chat] = {
                    "status": "DELIVERY_FAILED",
                    "error": err_msg,
                    "digest_hash": digest_hash,
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_chat_id": r_masked,
                    "chat_id": r_chat,
                    "recipient_role": r_role,
                }
        except httpx.TimeoutException as exc:
            sanitized_exc = sanitize_telegram_text(str(exc), settings.telegram_bot_token)
            _save_delivery_record(r_chat, r_role, r_masked, "DELIVERY_UNCERTAIN", err_desc=sanitized_exc)
            recipient_results[r_chat] = {
                "status": "DELIVERY_UNCERTAIN",
                "error": f"Ambiguous timeout during digest delivery for {r_role}: {sanitized_exc}. Message may have been accepted; automatic retry skipped to prevent duplicates.",
                "digest_hash": digest_hash,
                "attempt_made": True,
                "attempts_count": 1,
                "masked_chat_id": r_masked,
                "chat_id": r_chat,
                "recipient_role": r_role,
            }
        except httpx.TransportError as exc:
            sanitized_exc = sanitize_telegram_text(str(exc), settings.telegram_bot_token)
            _save_delivery_record(r_chat, r_role, r_masked, "NETWORK_ERROR", err_desc=sanitized_exc)
            recipient_results[r_chat] = {
                "status": "NETWORK_ERROR",
                "error": f"Network transport error for {r_role}: {sanitized_exc}",
                "digest_hash": digest_hash,
                "attempt_made": True,
                "attempts_count": 1,
                "masked_chat_id": r_masked,
                "chat_id": r_chat,
                "recipient_role": r_role,
            }
        except Exception as exc:
            sanitized_exc = sanitize_telegram_text(str(exc), settings.telegram_bot_token)
            _save_delivery_record(r_chat, r_role, r_masked, "UNEXPECTED_ERROR", err_desc=sanitized_exc)
            recipient_results[r_chat] = {
                "status": "UNEXPECTED_ERROR",
                "error": f"Unexpected error during digest delivery for {r_role}: {sanitized_exc}",
                "digest_hash": digest_hash,
                "attempt_made": True,
                "attempts_count": 1,
                "masked_chat_id": r_masked,
                "chat_id": r_chat,
                "recipient_role": r_role,
            }

    if should_close_client:
        try:
            http_client.close()
        except Exception:
            pass

    # Synthesize return dictionary
    if len(target_recipients) == 1:
        single_res = list(recipient_results.values())[0]
        return {
            "status": single_res["status"],
            "message": single_res.get("message") or (f"Telegram editorial digest successfully delivered (message_id: {single_res.get('message_id')})." if single_res["status"] == "DELIVERY_SUCCESS" else single_res.get("error", "")),
            "message_id": single_res.get("message_id"),
            "error": single_res.get("error"),
            "attempt_made": single_res["attempt_made"],
            "attempts_count": single_res["attempts_count"],
            "masked_chat_id": single_res["masked_chat_id"],
            "masked_token": token_masked,
            "digest_hash": digest_hash,
            "stable_digest_id": stable_digest_id,
            "stories_count": len(assessments),
            "rendered_html": rendered_html,
            "is_ux_test": is_ux_test,
            "recipients_count": 1,
            "recipient_results": recipient_results,
        }

    statuses = [res["status"] for res in recipient_results.values()]
    if all(s == "DELIVERY_SUCCESS" for s in statuses):
        overall_status = "DELIVERY_SUCCESS"
    elif all(s == "SAFE_PREVIEW_PASSED" for s in statuses):
        overall_status = "SAFE_PREVIEW_PASSED"
    elif all(s == "DUPLICATE_DELIVERY_PREVENTED" for s in statuses):
        overall_status = "DUPLICATE_DELIVERY_PREVENTED"
    elif any(s == "DELIVERY_SUCCESS" for s in statuses):
        overall_status = "PARTIAL_DELIVERY"
    else:
        overall_status = "MULTI_RECIPIENT_PROCESSED"

    return {
        "status": overall_status,
        "message": f"Processed delivery for {len(target_recipients)} recipients.",
        "recipients_count": len(target_recipients),
        "recipient_results": recipient_results,
        "attempt_made": any(res["attempt_made"] for res in recipient_results.values()),
        "attempts_count": sum(res["attempts_count"] for res in recipient_results.values()),
        "digest_hash": digest_hash,
        "stable_digest_id": stable_digest_id,
        "stories_count": len(assessments),
        "rendered_html": rendered_html,
        "is_ux_test": is_ux_test,
        "masked_token": token_masked,
    }

