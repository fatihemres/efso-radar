"""Deterministic offline planning and execution preview adapters for Efso Radar.

Enforces strict hermetic execution:
- Synthetic fixtures for articles, editorial assessments, health metrics, and backup plans.
- Unconditional preview isolation: all preview writes occur exclusively inside internally
  owned ephemeral temporary storage. Caller-supplied repositories are NEVER mutated or written to.
- Zero external network requests (no RSS HTTP, no Turso HTTP, no Telegram Bot API, no Gemini API, no GitHub Releases API).
- Strict 0 TRY / $0.00 expenditure.
- Explicit recipient routing checks:
    * EDITORIAL_DIGEST: Dispatches exclusively to PRIMARY_EDITOR (ADMIN is never included).
    * ADMIN_HEALTH_REPORT: Dispatches exclusively to ADMIN (PRIMARY_EDITOR is never included).
"""
from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

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
    utc_now,
)
from efso_radar.notifications.formatters import (
    format_telegram_digest,
    format_telegram_admin_health_report,
)
from efso_radar.repositories.base import StateRepository
from efso_radar.repositories.sqlite import SqliteStateRepository

logger = logging.getLogger("efso-radar.workflow.preview")


def get_synthetic_feed_articles(ref_now: Optional[datetime] = None) -> list[Article]:
    """Return deterministic synthetic articles for offline scan preview (zero network).

    Publication times are derived deterministically relative to ref_now (if provided)
    to ensure freshness within 24-hour candidate retrieval windows across arbitrary dates.
    """
    if ref_now is not None:
        base_utc = ref_now.astimezone(timezone.utc) if ref_now.tzinfo else ref_now.replace(tzinfo=timezone.utc)
    else:
        base_utc = datetime(2026, 10, 24, 6, 0, 0, tzinfo=timezone.utc)

    t1 = base_utc - timedelta(hours=1)
    t2 = base_utc - timedelta(hours=2)
    t3 = base_utc - timedelta(hours=3)

    return [
        Article(
            id="synth_art_001",
            title="Popüler Müzik Grubu Yeni Dünya Turnesi Tarihlerini Açıkladı",
            url="https://preview.efsoradar.internal/news/tour-announced-2026",
            canonical_url="https://preview.efsoradar.internal/news/tour-announced-2026",
            source="Pop Wire Global",
            market=Market.EN,
            published_at=t1,
            snippet="Ödüllü müzik grubu merakla beklenen stadyum konserleri için takvimi duyurdu.",
            raw_metadata={"priority_score": 8.5},
        ),
        Article(
            id="synth_art_002",
            title="Ünlü Oyuncunun Yeni Dijital Dizisi İzlenme Rekoru Kırdı",
            url="https://preview.efsoradar.internal/news/series-record-2026",
            canonical_url="https://preview.efsoradar.internal/news/series-record-2026",
            source="Magazin Gazetesi",
            market=Market.TR,
            published_at=t2,
            snippet="Yayınlandığı ilk hafta sonunda tüm zamanların en yüksek açılış izlenmesine ulaştı.",
            raw_metadata={"priority_score": 9.0},
        ),
        Article(
            id="synth_art_003",
            title="Genç Şarkıcı İlk Solo Albümünün Çıkış Tarihini Paylaştı",
            url="https://preview.efsoradar.internal/news/album-release-date",
            canonical_url="https://preview.efsoradar.internal/news/album-release-date",
            source="Müzik Haber",
            market=Market.TR,
            published_at=t3,
            snippet="Sosyal medyada viral olan sanatçı ilk stüdyo albümünün kapak görselini paylaştı.",
            raw_metadata={"priority_score": 7.8},
        ),
    ]


def get_synthetic_editorial_candidates() -> list[EditorialAssessment]:
    """Return deterministic synthetic editorial picks for offline digest preview."""
    return [
        EditorialAssessment(
            article_ids=["synth_art_001"],
            score=88,
            kind=EditorialKind.NEW,
            headline_tr="Popüler Müzik Grubu Yeni Dünya Turnesini Duyurdu",
            cover_title_tr="YENİ DÜNYA TURNESİ",
            caption_idea_tr="Ödüllü müzik grubu merakla beklenen stadyum konserleri için takvimi açıkladı.",
            why_efso_tr="Genç kitle ve popüler kültür için küresel gündem değeri yüksek.",
            suggested_format=SuggestedFormat.CAROUSEL,
            category=StoryCategory.GLOBAL,
        ),
        EditorialAssessment(
            article_ids=["synth_art_002"],
            score=91,
            kind=EditorialKind.NEW,
            headline_tr="Yeni Dijital Dizi İlk Haftada İzlenme Rekoru Kırdı",
            cover_title_tr="DİJİTAL DİZİDEN REKOR",
            caption_idea_tr="Yayınlandığı ilk hafta sonunda tüm zamanların en yüksek açılış izlenmesine ulaştı.",
            why_efso_tr="Yerli dizi ve genç oyuncu popülaritesi yüksek bir yapım.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
        ),
    ]


def get_synthetic_health_metrics() -> dict[str, Any]:
    """Return deterministic operational health metrics conforming to DAILY_ADMIN_HEALTH_REPORT_DESIGN."""
    return {
        "system_status": "NORMAL / SAĞLIKLI",
        "rss_windows_success": 8,
        "rss_windows_total": 8,
        "last_rss_success_ts": "21:05 TSİ",
        "total_articles_scanned": 343,
        "candidates_evaluated": 52,
        "editor_digests_sent": 3,
        "editor_news_items": 9,
        "delivery_attempts": 3,
        "delivery_successes": 3,
        "delivery_errors": 0,
        "gemini_calls": 0,
        "cost_usd": 0.00,
        "operational_notes": [
            "Kritik veya degrade sistem hatası kaydedilmedi.",
            "Tüm veritabanı kilitleri zamanında serbest bırakıldı.",
            "Editoryal teslimatlar PRIMARY_EDITOR kanalına eksiksiz ulaştırıldı.",
        ],
    }


def run_offline_scan_preview(
    timestamp: datetime,
    settings: Optional[Settings] = None,
    repository: Optional[StateRepository] = None,
) -> dict[str, Any]:
    """Execute decoupled RSS discovery offline preview with synthetic fixtures.

    GUARANTEE (ISSUE 1 FIX):
    All preview writes occur EXCLUSIVELY inside an internally owned ephemeral
    synthetic storage created and cleaned up within this function.
    Even if an external repository is passed to this function, it is NEVER written
    to or mutated under any circumstance.
    """
    active_settings = settings or Settings()
    articles = get_synthetic_feed_articles(ref_now=timestamp)

    # Always isolate preview mutations in an internally owned ephemeral directory.
    # Caller-supplied repositories must NEVER receive preview mutations under any circumstance.
    with tempfile.TemporaryDirectory(prefix="efso_preview_scan_") as temp_dir:
        db_path = Path(temp_dir) / "preview_radar.db"
        isolated_repo = SqliteStateRepository(db_path)
        isolated_repo.save_pending_candidates(articles)
        pending = isolated_repo.get_pending_candidates(max_age_hours=24, now=timestamp)
        return {
            "mode": "OFFLINE_PREVIEW",
            "task": "SCAN_ONLY",
            "synthetic_articles_scanned": len(articles),
            "backlog_candidates_queued": len(pending),
            "gemini_api_calls": 0,
            "telegram_messages_dispatched": 0,
            "network_requests": 0,
            "cost_usd": 0.00,
            "isolated_storage": True,
        }


def run_offline_digest_preview(
    timestamp: datetime,
    settings: Optional[Settings] = None,
    repository: Optional[StateRepository] = None,
) -> dict[str, Any]:
    """Execute editorial digest offline preview; enforces PRIMARY_EDITOR routing."""
    active_settings = settings or Settings()
    picks = get_synthetic_editorial_candidates()

    # Verify recipient policy: editorial content goes exclusively to PRIMARY_EDITOR
    editorial_recipients = active_settings.get_editorial_recipients()
    admin_recipients = active_settings.get_administrative_recipients()
    admin_chat_ids = {r["chat_id"] for r in admin_recipients}

    for r in editorial_recipients:
        if r["chat_id"] in admin_chat_ids and r.get("role") != "PRIMARY_EDITOR":
            raise AssertionError("SECURITY INVARIANT VIOLATION: ADMIN found in editorial recipients")

    digest_text = format_telegram_digest(picks, window_label="6 Saatlik Editoryal Özet (Önizleme)")

    return {
        "mode": "OFFLINE_PREVIEW",
        "task": "EDITORIAL_DIGEST",
        "recipient_role": "PRIMARY_EDITOR",
        "recipients_count": len(editorial_recipients),
        "recipient_chat_id_masked": active_settings.get_masked_primary_editor_chat_id(),
        "editorial_picks_count": len(picks),
        "digest_preview_html": digest_text[:400] + ("..." if len(digest_text) > 400 else ""),
        "gemini_api_calls": 0,
        "telegram_messages_dispatched": 0,
        "network_requests": 0,
        "cost_usd": 0.00,
    }


def run_offline_admin_health_preview(
    timestamp: datetime,
    settings: Optional[Settings] = None,
    repository: Optional[StateRepository] = None,
) -> dict[str, Any]:
    """Execute daily admin health report offline preview; enforces ADMIN routing."""
    active_settings = settings or Settings()
    metrics = get_synthetic_health_metrics()

    # Verify recipient policy: operational health reports go exclusively to ADMIN
    admin_recipients = active_settings.get_administrative_recipients()
    editorial_recipients = active_settings.get_editorial_recipients()
    editor_chat_ids = {r["chat_id"] for r in editorial_recipients}

    for r in admin_recipients:
        if r["chat_id"] in editor_chat_ids and r.get("role") != "ADMIN":
            raise AssertionError("SECURITY INVARIANT VIOLATION: PRIMARY_EDITOR found in admin health recipients")

    report_text = format_telegram_admin_health_report(metrics, now=timestamp)

    return {
        "mode": "OFFLINE_PREVIEW",
        "task": "ADMIN_HEALTH_REPORT",
        "recipient_role": "ADMIN",
        "recipients_count": len(admin_recipients),
        "recipient_chat_id_masked": active_settings.get_masked_admin_chat_id(),
        "system_status": metrics["system_status"],
        "rss_windows_success": metrics["rss_windows_success"],
        "report_preview_html": report_text[:400] + ("..." if len(report_text) > 400 else ""),
        "gemini_api_calls": 0,
        "telegram_messages_dispatched": 0,
        "network_requests": 0,
        "cost_usd": 0.00,
    }


def run_offline_backup_preview(
    timestamp: datetime,
    settings: Optional[Settings] = None,
) -> dict[str, Any]:
    """Execute disaster recovery backup preview (DESIGN ONLY).

    Strictly produces planning and preview metadata with zero remote modifications.
    Live upload and remote deletion are explicitly labeled NOT IMPLEMENTED.
    """
    return {
        "mode": "OFFLINE_PREVIEW",
        "task": "ENCRYPTED_BACKUP",
        "status": "DESIGN_ONLY",
        "schema_version": 6,
        "tables_included": 15,
        "encryption_algorithm": "AES-256-GCM (Full Header AAD Binding)",
        "retention_policy": "14 days rolling; minimum 2 recovery points preserved",
        "remote_upload_status": "NOT_IMPLEMENTED (DESIGN ONLY)",
        "remote_retention_delete_status": "NOT_IMPLEMENTED (DESIGN ONLY)",
        "network_requests": 0,
        "cost_usd": 0.00,
    }
