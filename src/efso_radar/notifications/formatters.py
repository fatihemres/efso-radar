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


def format_telegram_digest(
    assessments: list[EditorialAssessment],
    window_label: str = "6 Saatlik Editoryal Özet",
) -> str:
    """Format a group of editorially approved stories into a single periodic Telegram digest."""
    e = html.escape
    tr_upper = window_label.replace("i", "İ").replace("ı", "I").upper()
    header = (
        f"<b>📋 EFSO RADAR | {e(tr_upper)} ({len(assessments)} Haber)</b>\n"
        "<i>Acil kırılım gerektirmeyen ancak içerik kütüphanesi için öne çıkan editoryal adaylar:</i>\n"
    )
    blocks: list[str] = [header]
    for idx, item in enumerate(assessments, 1):
        kind_tag = "🔄 GÜNCELLEME" if item.kind == EditorialKind.UPDATE else "✨ ADAY"
        caution_line = ""
        if item.uncertainties_tr:
            caution_line = f"\n   ⚠️ <i>Teyit notu: {e(item.uncertainties_tr)}</i>"
        first_source = ""
        if item.supporting_articles:
            art = item.supporting_articles[0]
            first_source = f' — <a href="{e(art.url, quote=True)}">{e(art.source)}</a>'
        blocks.append(
            f"<b>{idx}. [{kind_tag} | {item.score}/100 | {e(item.suggested_format.value)}] {e(item.headline_tr)}</b>\n"
            f"   💡 {e(item.why_efso_tr)}{first_source}\n"
            f"   🎯 Kapak: {e(item.cover_title_tr)}{caution_line}"
        )
    blocks.append("\n<i>Not: Yayın öncesi birincil kaynak ve iddia doğrulaması yapılmalıdır.</i>")
    return "\n\n".join(blocks)

