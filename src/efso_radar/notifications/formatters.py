"""Telegram message templates and HTML formatting utilities.

Guarantees strict HTML character escaping to prevent Telegram API 400 Bad Request errors.
"""
from __future__ import annotations

import html
from efso_radar.domain import EditorialAssessment, EditorialKind


def format_telegram_alert(assessment: EditorialAssessment) -> str:
    """Format an approved editorial assessment into an HTML Telegram alert message."""
    e = html.escape
    kind_badge = "🔄 ÖNEMLİ GELİŞME" if assessment.kind == EditorialKind.UPDATE else "🚨 YENİ HABER"

    sources_html = "\n".join(
        f'• <a href="{e(a.url, quote=True)}">{e(a.source)} — kaynak</a>'
        for a in assessment.supporting_articles[:3]
    )

    development_block = ""
    if assessment.kind == EditorialKind.UPDATE and assessment.development_tr:
        development_block = f"\n<b>Ne değişti?</b> {e(assessment.development_tr)}\n"

    return (
        f"<b>💗 EFSO RADAR | {kind_badge}</b>\n"
        f"<b>{e(assessment.headline_tr)}</b>\n\n"
        f"⭐ Efso skoru: {assessment.score}/100   |   📱 {e(assessment.suggested_format.value)}"
        f"   |   {e(assessment.category.value)}\n"
        f"<b>Neden değerli?</b> {e(assessment.why_efso_tr)}\n"
        f"{development_block}\n"
        f"<b>Kapak önerisi</b>\n{e(assessment.cover_title_tr)}\n\n"
        f"<b>Caption fikri</b>\n{e(assessment.caption_idea_tr)}\n\n"
        f"<b>Kaynaklar</b>\n{sources_html}\n\n"
        "<i>Ön eleme: haberin tam metni ve iddialar yayın öncesi doğrulanmalıdır.</i>"
    )
