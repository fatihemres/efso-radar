"""Telegram alert notification channel.

Handles quiet hours suppression, HTML message rendering, retry logic,
and deterministic dry-run/preview execution.
"""
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Optional

from efso_radar.config import Settings
from efso_radar.domain import DeliveryStatus, EditorialAssessment, NotificationRecord, utc_now
from efso_radar.notifications.base import NotificationChannel
from efso_radar.notifications.formatters import format_telegram_alert

logger = logging.getLogger("efso-radar.notifications.telegram")


class TelegramNotificationChannel(NotificationChannel):
    """Telegram Bot alert dispatcher."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def send(
        self,
        assessment: EditorialAssessment,
        story_id: str,
        update_id: Optional[str] = None,
        dry_run: bool = False,
        now: Optional[datetime] = None,
    ) -> NotificationRecord:
        current_now = now or utc_now()
        message_text = format_telegram_alert(assessment)
        if update_id:
            notification_id = f"NOTIF-UPD-{current_now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        else:
            notification_id = f"NOTIF-{current_now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

        # Check quiet hours
        if self.settings.is_in_quiet_hours(current_now):
            if self.settings.quiet_hours_behavior == "ALLOW_BREAKING_95" and assessment.score >= 95:
                logger.info("Bypassing quiet hours for breaking news with score %d", assessment.score)
            elif self.settings.quiet_hours_behavior == "DISCARD":
                logger.info("Discarding notification during quiet hours: %s", assessment.headline_tr)
                return NotificationRecord(
                    notification_id=notification_id,
                    story_id=story_id,
                    update_id=update_id,
                    kind=assessment.kind,
                    sent_at=current_now,
                    status=DeliveryStatus.SKIPPED_BELOW_THRESHOLD,
                    message_text=message_text,
                    headline_tr=assessment.headline_tr,
                    score=assessment.score,
                )
            else:  # "HOLD" default
                logger.info("Holding notification for quiet hours: %s", assessment.headline_tr)
                return NotificationRecord(
                    notification_id=notification_id,
                    story_id=story_id,
                    update_id=update_id,
                    kind=assessment.kind,
                    sent_at=current_now,
                    status=DeliveryStatus.HELD_QUIET_HOURS,
                    message_text=message_text,
                    headline_tr=assessment.headline_tr,
                    score=assessment.score,
                )

        if dry_run or not (self.settings.telegram_bot_token and self.settings.telegram_chat_id):
            logger.info("[DRY-RUN / LOCAL PREVIEW] Notification generated for story %s (update: %s)", story_id, update_id)
            return NotificationRecord(
                notification_id=notification_id,
                story_id=story_id,
                update_id=update_id,
                kind=assessment.kind,
                sent_at=current_now,
                status=DeliveryStatus.SENT,
                message_text=message_text,
                headline_tr=assessment.headline_tr,
                score=assessment.score,
            )

        # Real dispatch (only when configured and not in dry-run)
        return self._dispatch_telegram(notification_id, story_id, update_id, assessment, message_text, current_now)

    def _dispatch_telegram(
        self,
        notification_id: str,
        story_id: str,
        update_id: Optional[str],
        assessment: Optional[EditorialAssessment],
        message_text: str,
        now: datetime,
    ) -> NotificationRecord:
        endpoint = f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage"
        params = {
            "chat_id": self.settings.telegram_chat_id,
            "text": message_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
        payload = urllib.parse.urlencode(params).encode("utf-8")
        request = urllib.request.Request(endpoint, data=payload, method="POST")

        kind = assessment.kind if assessment else EditorialKind.NEW
        headline_tr = assessment.headline_tr if assessment else "Efso Magazin Alert"
        score = assessment.score if assessment else 90

        try:
            with urllib.request.urlopen(request, timeout=self.settings.http_timeout_seconds) as response:
                result = json.load(response)
                if not result.get("ok"):
                    raise RuntimeError(f"Telegram rejected message: {result}")

            logger.info("Successfully delivered Telegram alert for story %s", story_id)
            return NotificationRecord(
                notification_id=notification_id,
                story_id=story_id,
                update_id=update_id,
                kind=kind,
                sent_at=now,
                status=DeliveryStatus.SENT,
                message_text=message_text,
                headline_tr=headline_tr,
                score=score,
            )
        except Exception as exc:
            logger.error("Failed to deliver Telegram notification: %s", exc)
            return NotificationRecord(
                notification_id=notification_id,
                story_id=story_id,
                update_id=update_id,
                kind=kind,
                sent_at=now,
                status=DeliveryStatus.FAILED,
                message_text=message_text,
                headline_tr=headline_tr,
                score=score,
                error_message=str(exc),
            )
