"""Notifications package for Efso Radar."""
from efso_radar.notifications.base import NotificationChannel
from efso_radar.notifications.formatters import (
    compute_stable_digest_identity,
    deduplicate_assessments,
    extract_factual_summary,
    format_telegram_alert,
    format_telegram_digest,
    format_telegram_digest_legacy,
)
from efso_radar.notifications.telegram import TelegramNotificationChannel

__all__ = [
    "NotificationChannel",
    "TelegramNotificationChannel",
    "compute_stable_digest_identity",
    "deduplicate_assessments",
    "extract_factual_summary",
    "format_telegram_alert",
    "format_telegram_digest",
    "format_telegram_digest_legacy",
]
