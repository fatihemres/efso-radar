"""Telegram message templates and HTML formatting utilities.

Guarantees strict HTML character escaping to prevent Telegram API 400 Bad Request errors.
Supports four logically separated message categories within a single private owner chat:
- EDITORIAL_DIGEST
- VERIFIED_BREAKING
- SENSITIVE_EDITORIAL_REVIEW
- HOLD_FOR_VERIFICATION
"""
from __future__ import annotations

import hashlib
import html
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from difflib import SequenceMatcher
from efso_radar.deduplication.engine import (
    calculate_token_overlap,
    extract_key_tokens,
    find_matched_affinity_entities,
    normalize_text,
)
from efso_radar.domain import (
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    TelegramNotificationCategory,
    VerificationStatus,
)

TURKEY_TZ = timezone(timedelta(hours=3))

TR_MONTH_NAMES = {
    1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan", 5: "Mayıs", 6: "Haziran",
    7: "Temmuz", 8: "Ağustos", 9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık"
}


def format_turkey_timestamp(dt: Optional[datetime] = None, with_middle_dot: bool = False) -> str:
    """Format datetime in Türkiye local time (TSİ / UTC+3)."""
    ref_dt = (dt or datetime.now(timezone.utc)).astimezone(TURKEY_TZ)
    m = TR_MONTH_NAMES.get(ref_dt.month, "")
    sep = " · " if with_middle_dot else " "
    return f"{ref_dt.day} {m} {ref_dt.year}{sep}{ref_dt.strftime('%H:%M')} TSİ"


def truncate_word_boundary(text: str, max_length: int = 80) -> str:
    """Truncate text cleanly at word boundary without slicing words in half or leaving broken punctuation."""
    if not text or len(text) <= max_length:
        return text.strip() if text else ""
    truncated = text[:max_length]
    last_space = truncated.rfind(" ")
    if last_space > 0:
        truncated = truncated[:last_space]
    return truncated.rstrip(" ,;:-/&…")


def is_redundant_cover_title(headline: str, cover_title: str) -> bool:
    """Check if cover suggestion merely repeats or mirrors the headline."""
    if not cover_title or not headline:
        return True
    h_norm = normalize_text(headline)
    c_norm = normalize_text(cover_title)
    if h_norm == c_norm:
        return True
    if h_norm in c_norm:
        return True
    # A cover that is a direct prefix truncation of the headline (at least 50% of it)
    if h_norm.startswith(c_norm) and len(c_norm) >= 0.5 * len(h_norm):
        return True
    # Only flag containment as redundant if cover title is essentially the entire headline
    if len(c_norm) >= 0.85 * len(h_norm) and c_norm in h_norm:
        return True
    return SequenceMatcher(None, h_norm, c_norm).ratio() >= 0.80


HYPERBOLIC_PHRASES = [
    r"(?i)\s*sosyal\s+medyayı\s+(salladı|kasıp\s+kavurdu|yıktı\s+geçti|ikiye\s+böldü)\b",
    r"(?i)\s*(bomba|olay|şok)\s+(iddia|açıklama|itiraf|gelişme)\b",
    r"(?i)\s*viral\s+(oldu|olan|videoları|dansıyla)\b",
    r"(?i)\s*görenler\s+tanıyamadı\b",
    r"(?i)\s*rekor\s+(kırdı|izlenme)\b",
    r"(?i)\s*yer\s+yerinden\s+oynadı\b",
    r"(?i)\s*nefes\s+kesen\b",
]


def clean_factual_relevance(text: str) -> str:
    """Clean unsupported buzzwords and sensationalist claims, preserving factual entertainment relevance."""
    if not text:
        return ""
    cleaned = text
    for pat in HYPERBOLIC_PHRASES:
        cleaned = re.sub(pat, "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or text.strip()


VERIFICATION_STATUS_TR: dict[VerificationStatus, str] = {
    VerificationStatus.OFFICIAL_CONFIRMATION: "Resmî Kurum / Menajer Onaylı",
    VerificationStatus.FIRST_PARTY_STATEMENT: "Birinci Ağız Beyanı",
    VerificationStatus.REPUTABLE_REPORTING: "Saygın Yayıncı Teyitli",
    VerificationStatus.ATTRIBUTED_CLAIM: "Belirli Kaynağa Dayandırılan İddia",
    VerificationStatus.UNVERIFIED_RUMOR: "Teyitsiz Söylenti / Magazin Dedikodusu",
    VerificationStatus.SPECULATION: "Spekülasyon / Sosyal Medya İddiası",
}


def format_sources_html(assessment: EditorialAssessment, limit: int = 3) -> str:
    """Format supporting article sources safely into HTML bullet links."""
    e = html.escape
    if not assessment.supporting_articles:
        if assessment.source_urls:
            return "\n".join(f'• <a href="{e(u, quote=True)}">Haber Kaynağı</a>' for u in assessment.source_urls[:limit])
        return "• <i>Kaynak bağlantısı belirtilmemiş</i>"

    return "\n".join(
        f'• <a href="{e(a.url, quote=True)}">{e(a.source)}</a>'
        for a in assessment.supporting_articles[:limit]
    )


UPDATE_SIGNAL_KEYWORDS = {
    "açıkladı", "duyurdu", "karar", "tarih", "taburcu", "dava", "anlaşma",
    "resmen", "itiraf", "başladı", "sonuçlandı", "ertelendi", "açıklama",
    "netleşti", "yeni", "ifade", "beraat", "tahliye", "boşandı", "ayrıldı",
    "evlendi", "nişanlandı", "doğurdu", "ameliyat", "tedavi", "belgesel",
    "proje", "ceza", "hapis", "tazminat", "sözleşme", "imza", "gözaltı",
}


def is_meaningful_update(item: EditorialAssessment, base_item: Optional[EditorialAssessment] = None) -> bool:
    """Check if an assessment represents a legitimate update containing meaningful new information.

    Guarantees:
    1. Must have EditorialKind.UPDATE.
    2. Must have non-empty development text.
    3. Length alone is NOT sufficient: Must contain concrete factual development markers.
    4. Must not merely repeat or paraphrase base item's headline or existing text.
    5. Ambiguous claims without concrete new facts are rejected as updates.
    """
    if item.kind != EditorialKind.UPDATE:
        return False
    dev = (item.development_tr or "").strip()
    if len(dev) < 15:
        return False

    dev_norm = normalize_text(dev)
    dev_words = set(dev_norm.split())

    # Must contain at least one verifiable new development signal token
    has_update_signal = any(kw in dev_words or kw in dev_norm for kw in UPDATE_SIGNAL_KEYWORDS)
    if not has_update_signal:
        return False

    if base_item:
        base_norm = normalize_text(base_item.headline_tr)
        if base_norm in dev_norm or dev_norm in base_norm:
            return False
        base_tokens = extract_key_tokens(base_item.headline_tr)
        dev_tokens = extract_key_tokens(dev)
        overlap = calculate_token_overlap(base_tokens, dev_tokens)
        if overlap >= 0.70:
            return False

    return True


def deduplicate_assessments(
    assessments: list[EditorialAssessment],
) -> tuple[list[EditorialAssessment], list[tuple[EditorialAssessment, str]]]:
    """Deduplicate a list of editorial assessments with strict cross-story semantics.

    Semantics:
    1. True duplicates (same story/event without new development) are merged into the
       primary representative, consolidating supporting articles and source URLs.
    2. Legitimate UPDATE events containing meaningful new information remain eligible
       and are NOT dropped merely because a higher-scoring earlier article exists.
    3. Pseudo-updates lacking substantive development (<15 chars or repeating the headline)
       are collapsed into the primary story.

    Returns:
        tuple of (selected_assessments, dropped_assessments_with_reasons)
    """
    sorted_items = sorted(assessments, key=lambda a: (a.score, a.headline_tr), reverse=True)
    selected: list[EditorialAssessment] = []
    dropped: list[tuple[EditorialAssessment, str]] = []

    for item in sorted_items:
        item_article_ids = set(item.article_ids)
        for art in item.supporting_articles:
            if hasattr(art, "raw_metadata") and isinstance(art.raw_metadata, dict):
                cluster_ids = art.raw_metadata.get("cluster_article_ids")
                if cluster_ids:
                    item_article_ids.update(cluster_ids)

        item_tokens = extract_key_tokens(item.headline_tr)
        item_ents = set(find_matched_affinity_entities(item.headline_tr))

        is_dup = False
        dup_reason = ""

        for kept in selected:
            kept_article_ids = set(kept.article_ids)
            for art in kept.supporting_articles:
                if hasattr(art, "raw_metadata") and isinstance(art.raw_metadata, dict):
                    cluster_ids = art.raw_metadata.get("cluster_article_ids")
                    if cluster_ids:
                        kept_article_ids.update(cluster_ids)

            # 1. Exact article ID or cluster ID overlap
            shared_ids = item_article_ids & kept_article_ids
            if shared_ids:
                is_dup = True
                dup_reason = f"Paylaşılan makale / küme ID ({list(shared_ids)[0]}) — '{kept.headline_tr}' ile mükerrer."
                # Consolidate sources into kept
                for art in item.supporting_articles:
                    if art.id not in {a.id for a in kept.supporting_articles}:
                        kept.supporting_articles.append(art)
                for u in item.source_urls:
                    if u not in kept.source_urls:
                        kept.source_urls.append(u)
                for aid in item.article_ids:
                    if aid not in kept.article_ids:
                        kept.article_ids.append(aid)
                break

            # 2. Text and celebrity entity overlap
            kept_tokens = extract_key_tokens(kept.headline_tr)
            kept_ents = set(find_matched_affinity_entities(kept.headline_tr))
            shared_ents = item_ents & kept_ents

            token_sim = calculate_token_overlap(item_tokens, kept_tokens)
            char_sim = SequenceMatcher(
                None, normalize_text(item.headline_tr), normalize_text(kept.headline_tr)
            ).ratio()

            is_same_event = (shared_ents and (token_sim >= 0.25 or char_sim >= 0.50)) or char_sim >= 0.72

            if is_same_event:
                # Check whether item or kept represents a legitimate UPDATE with meaningful new development
                item_is_update = is_meaningful_update(item, kept)
                kept_is_update = is_meaningful_update(kept, item)

                if item_is_update or kept_is_update:
                    # Legitimate UPDATE contains meaningful new information -> keep both eligible
                    continue

                # True duplicate without meaningful development -> merge into higher-scoring representative
                is_dup = True
                ent_desc = f" ({', '.join(sorted(shared_ents))})" if shared_ents else ""
                dup_reason = (
                    f"Mükerrer haber birleştirildi{ent_desc}: '{kept.headline_tr}' "
                    f"ile konsolide edildi (benzerlik: token={token_sim:.2f}, char={char_sim:.2f}). "
                    "Kaynaklar ve makale ID'leri birleştirildi."
                )

                # Merge supporting articles, source URLs, and article IDs into kept
                for art in item.supporting_articles:
                    if art.id not in {a.id for a in kept.supporting_articles}:
                        kept.supporting_articles.append(art)
                for u in item.source_urls:
                    if u not in kept.source_urls:
                        kept.source_urls.append(u)
                for aid in item.article_ids:
                    if aid not in kept.article_ids:
                        kept.article_ids.append(aid)
                break

        if is_dup:
            dropped.append((item, dup_reason))
        else:
            selected.append(item)

    return selected, dropped


def extract_factual_summary(assessment: EditorialAssessment) -> Optional[str]:
    """Extract a concise, factually grounded sentence describing what happened.

    Rules:
    1. If UPDATE and development_tr exists, return that development statement.
    2. Check supporting_articles for genuine RSS snippets.
    3. Ensure every factual summary is a complete, properly punctuated Turkish sentence.
    4. If source metadata is insufficient, return None rather than inventing facts.
    """
    if assessment.kind == EditorialKind.UPDATE and assessment.development_tr:
        clean = assessment.development_tr.strip()
        if len(clean) >= 15:
            if not clean.endswith((".", "!", "?", "…")):
                clean += "."
            return clean

    for art in assessment.supporting_articles:
        if art.snippet and art.snippet.strip():
            snippet = art.snippet.strip().strip('"\' ')
            if len(snippet) > 280:
                snippet = truncate_word_boundary(snippet, max_length=280)
            if not snippet.endswith((".", "!", "?", "…")):
                snippet += "."
            return snippet

    return None


def compute_stable_digest_identity(assessments: list[EditorialAssessment]) -> str:
    """Compute a presentation-independent hash for an editorial digest based on its story identities.

    Guarantees that styling, whitespace, timestamps, and formatting changes do not alter
    the canonical identity of the delivered editorial digest.
    """
    canonical_keys: list[str] = []
    for a in assessments:
        if a.article_ids:
            canonical_keys.extend(sorted(a.article_ids))
        elif a.supporting_articles:
            canonical_keys.extend(sorted(art.id for art in a.supporting_articles))
        else:
            canonical_keys.append(normalize_text(a.headline_tr))
    unique_sorted = sorted(set(canonical_keys))
    key_str = ":".join(unique_sorted)
    return hashlib.sha256(key_str.encode("utf-8")).hexdigest()[:24]


def compute_editorial_digest_revision_id(
    assessments: list[EditorialAssessment],
    is_ux_test: bool = False,
    window_label: Optional[str] = None,
) -> str:
    """Compute a presentation-independent, editorial-revision-scoped canonical hash for an editorial digest.

    Guarantees:
    1. Stable across display timestamps, whitespace, and formatting variations.
    2. Captures all editorial revisions: kinds (NEW vs UPDATE), headlines, developments,
       and story article identities.
    3. Distinguishes UX test mode from live editorial digests.
    4. Distinguishes different window intervals (if specified).
    5. 100% deterministic across Python processes and platforms (SHA-256).
    """
    story_reprs: list[str] = []
    for a in assessments:
        canonical_arts = (
            sorted(a.article_ids)
            if a.article_ids
            else (sorted(art.id for art in a.supporting_articles) if a.supporting_articles else [])
        )
        art_str = ",".join(canonical_arts)
        kind_str = a.kind.value if hasattr(a.kind, "value") else str(a.kind)
        parent_id = a.parent_story_id or ""
        headline_clean = normalize_text(a.headline_tr)
        dev_clean = normalize_text(a.development_tr or "")
        why_clean = normalize_text(a.why_efso_tr or "")
        cover_clean = normalize_text(a.cover_title_tr or "")

        story_repr = (
            f"arts={art_str};k={kind_str};p={parent_id};"
            f"h={headline_clean};dev={dev_clean};why={why_clean};c={cover_clean}"
        )
        story_reprs.append(story_repr)

    sorted_stories = sorted(story_reprs)
    stories_block = "|".join(sorted_stories)
    ux_tag = "UX" if is_ux_test else "PROD"
    win_tag = normalize_text(window_label or "")
    canonical_seed = f"mode={ux_tag};win={win_tag};items={stories_block}"
    return hashlib.sha256(canonical_seed.encode("utf-8")).hexdigest()[:24]


def format_telegram_digest_legacy(
    assessments: list[EditorialAssessment],
    window_label: str = "6 Saatlik Editoryal Özet",
    deduplicate: bool = True,
) -> str:
    """Legacy Phase 4E digest formatter (retained for backward compatibility and comparison)."""
    e = html.escape
    clean_assessments = assessments
    if deduplicate and len(assessments) > 1:
        clean_assessments, _ = deduplicate_assessments(assessments)

    tr_upper = window_label.replace("i", "İ").replace("ı", "I").upper()
    header = (
        f"<b>📋 EFSO RADAR | EDİTORYAL ÖZET</b>\n"
        f"<b>{e(tr_upper)} ({len(clean_assessments)} Haber)</b>\n\n"
        "<i>Acil kırılım gerektirmeyen, içerik havuzu için öne çıkan editoryal bülten seçkisi:</i>\n"
    )
    blocks: list[str] = [header]
    for idx, item in enumerate(clean_assessments, 1):
        kind_tag = "🔄 GÜNCELLEME" if item.kind == EditorialKind.UPDATE else "✨ ADAY"
        caution_line = ""
        if item.uncertainties_tr:
            caution_line = f"\n   ⚠️ <i>Teyit notu: {e(item.uncertainties_tr)}</i>"

        first_source = ""
        if item.supporting_articles:
            art = item.supporting_articles[0]
            first_source = f' — <a href="{e(art.url, quote=True)}">{e(art.source)}</a>'
        elif item.source_urls:
            first_source = f' — <a href="{e(item.source_urls[0], quote=True)}">Kaynak</a>'

        relevance = clean_factual_relevance(item.why_efso_tr or item.relevance_reason_tr or "Editoryal seçki.")

        cover_line = ""
        clean_cover = truncate_word_boundary(item.cover_title_tr or "", max_length=70)
        if clean_cover and not is_redundant_cover_title(item.headline_tr, clean_cover):
            cover_line = f"\n   🎯 <b>Kapak fikri:</b> {e(clean_cover)}"

        development_line = ""
        if item.kind == EditorialKind.UPDATE and item.development_tr:
            development_line = f"\n   🆕 <b>Yeni Gelişme:</b> {e(item.development_tr)}"

        blocks.append(
            f"<b>{idx}. [{kind_tag} | Skor: {item.score}/100 | Format: {e(item.suggested_format.value)}] {e(item.headline_tr)}</b>\n"
            f"   💡 <b>Neden değerli?</b> {e(relevance)}{first_source}"
            f"{development_line}"
            f"{cover_line}"
            f"{caution_line}"
        )

    blocks.append(
        "\n<i>ℹ️ Bu bülten tek bir özel sohbete iletilen editoryal özettir. "
        "Instagram'da otomatik paylaşım izni vermez.</i>"
    )
    result = "\n\n".join(blocks)
    return result


def format_telegram_digest(
    assessments: list[EditorialAssessment],
    window_label: Optional[str] = None,
    deduplicate: bool = True,
    timestamp: Optional[datetime] = None,
    validate_length: bool = True,
    is_ux_test: bool = False,
) -> str:
    """Format a group of editorially approved stories into the redesigned Phase 4F Telegram digest (EDITORIAL_DIGEST).

    Guarantees:
    1. Modern, visually engaging, and mobile-scannable layout using supported Telegram HTML.
    2. Strict factual groundness: summaries are derived only from authentic source snippets/development.
    3. Missing optional metadata is omitted cleanly rather than inventing facts.
    4. Non-redundant cover suggestions (omitted if identical/redundant to headline).
    5. Clean Turkish TSİ timestamp and exact story counter.
    6. Exceeding 4096-character limit raises ValueError; stories are never silently discarded.
    7. Supports separately identified, explicitly labeled UX TEST mode.
    """
    e = html.escape
    clean_assessments = assessments
    if deduplicate and len(assessments) > 1:
        clean_assessments, _ = deduplicate_assessments(assessments)

    ts_str = format_turkey_timestamp(timestamp, with_middle_dot=True)
    count = len(clean_assessments)
    count_line = f"✨ {count} haber önerisi"
    if is_ux_test:
        count_line += " · TEST"
    elif window_label and "Saatlik" in window_label:
        tr_upper = window_label.replace("i", "İ").replace("ı", "I").upper()
        count_line += f" · {tr_upper}"

    if is_ux_test:
        header = (
            "🧪 <b>EFSO RADAR · MOBİL UX TESTİ</b>\n"
            "📰 <b>EDİTORYAL BÜLTEN (TASARIM ÖNİZLEME)</b>\n\n"
            f"{ts_str}\n"
            f"{count_line}"
        )
    else:
        header = (
            "📡 <b>EFSO RADAR</b>\n"
            "📰 <b>EDİTORYAL BÜLTEN</b>\n\n"
            f"{ts_str}\n"
            f"{count_line}"
        )

    separator = "━━━━━━━━━━━━━━"
    story_blocks: list[str] = []

    for idx, item in enumerate(clean_assessments, 1):
        num_str = f"{idx:02d}"
        circle = "🔄" if item.kind == EditorialKind.UPDATE else "🟢"
        headline_line = f"{circle} <b>{num_str} · {e(item.headline_tr)}</b>"
        score_format_line = f"⭐ <b>{item.score}/100</b> · 📱 <b>{e(item.suggested_format.value)}</b>"

        parts = [headline_line, score_format_line]

        # 1. Concise factual summary (omitted if insufficient source metadata)
        summary = extract_factual_summary(item)
        if summary:
            parts.append(f"📝 <b>Özet:</b>\n{e(summary)}")

        # 2. Editorial relevance note (clean factual explanation)
        relevance = clean_factual_relevance(item.why_efso_tr or item.relevance_reason_tr or "")
        if relevance:
            parts.append(f"💡 <b>Editör notu:</b>\n{e(relevance)}")

        # 3. Cover suggestion (omitted if redundant with headline or empty)
        clean_cover = truncate_word_boundary(item.cover_title_tr or "", max_length=70)
        if clean_cover and not is_redundant_cover_title(item.headline_tr, clean_cover):
            parts.append(f"🎯 <b>Kapak önerisi:</b>\n{e(clean_cover)}")

        # 4. Clickable source link
        if item.supporting_articles:
            art = item.supporting_articles[0]
            parts.append(f'🔗 <a href="{e(art.url, quote=True)}">{e(art.source)}</a>')
        elif item.source_urls:
            parts.append(f'🔗 <a href="{e(item.source_urls[0], quote=True)}">Haber Kaynağı</a>')
        else:
            parts.append('🔗 <i>Kaynak belirtilmemiş</i>')

        story_blocks.append("\n\n".join(parts))

    if is_ux_test:
        footer = (
            "🧪 <b>Yalnızca mobil görünüm ve UX doğrulama testidir.</b>\n"
            "<i>Instagram'da otomatik paylaşım yapılmaz.</i>"
        )
    else:
        footer = (
            "🔒 <b>Yalnızca editoryal değerlendirme.</b>\n"
            "<i>Instagram'da otomatik paylaşım yapılmaz.</i>"
        )

    full_elements = [header]
    for sb in story_blocks:
        full_elements.append(separator)
        full_elements.append(sb)
    full_elements.append(separator)
    full_elements.append(footer)

    result = "\n\n".join(full_elements)

    if validate_length and len(result) > 4096:
        raise ValueError(
            f"Rendered Telegram digest HTML length ({len(result)}) exceeds "
            f"Telegram 4096-character limit. Automatic discarding of stories is prohibited."
        )

    return result


def format_telegram_breaking(assessment: EditorialAssessment) -> str:
    """Format an exceptionally important verified breaking news alert (VERIFIED_BREAKING)."""
    e = html.escape
    kind_badge = "🔄 ÖNEMLİ GELİŞME" if assessment.kind == EditorialKind.UPDATE else "🚨 YENİ HABER"
    v_label = VERIFICATION_STATUS_TR.get(assessment.verification_status, assessment.verification_status.value)
    sources_html = format_sources_html(assessment)

    development_block = ""
    if assessment.kind == EditorialKind.UPDATE and assessment.development_tr:
        development_block = f"\n<b>Ne değişti?</b> {e(assessment.development_tr)}\n"

    return (
        f"<b>🚨 EFSO RADAR | DOĞRULANMIŞ SICAK GELİŞME ({kind_badge})</b>\n"
        f"<b>{e(assessment.headline_tr)}</b>\n\n"
        f"⭐ <b>Efso Skoru:</b> {assessment.score}/100   |   📱 <b>Format:</b> {e(assessment.suggested_format.value)}"
        f"   |   🏷️ <b>Kategori:</b> {e(assessment.category.value)}\n"
        f"✅ <b>Doğrulama Durumu:</b> {e(v_label)}\n\n"
        f"<b>Neden Kritik?</b> {e(assessment.why_efso_tr)}\n"
        f"{development_block}\n"
        f"<b>Kapak Önerisi:</b>\n{e(assessment.cover_title_tr)}\n\n"
        f"<b>Caption Fikri:</b>\n{e(assessment.caption_idea_tr)}\n\n"
        f"<b>Kaynaklar:</b>\n{sources_html}\n\n"
        "<i>⚠️ Bu bildirim acil editoryal inceleme içindir; Instagram'da otomatik yayınlanmaz.</i>"
    )


def format_telegram_sensitive_review(assessment: EditorialAssessment) -> str:
    """Format a sensitive tragedy, death, or bereavement review notification (SENSITIVE_EDITORIAL_REVIEW)."""
    e = html.escape
    v_label = VERIFICATION_STATUS_TR.get(assessment.verification_status, assessment.verification_status.value)
    sources_html = format_sources_html(assessment)
    rationale = assessment.why_efso_tr or assessment.relevance_reason_tr or "Önemli kamuoyu ilgisi bulunan hassas gelişme."
    safety_note = assessment.uncertainties_tr or "Hassas içerik Product Owner açık onayı olmadan yayınlanamaz."

    return (
        "<b>⚠️ EFSO RADAR | ÖZEL EDİTORYAL İNCELEME (SENSITIVE REVIEW)</b>\n"
        "🔒 <b>GİZLİ & HASSAS İÇERİK — YALNIZCA EDİTÖR İNCELEME TAVSİYESİDİR</b>\n\n"
        f"<b>{e(assessment.headline_tr)}</b>\n\n"
        f"⭐ <b>Efso Skoru:</b> {assessment.score}/100   |   🏷️ <b>Kategori:</b> {e(assessment.category.value)}\n"
        "🕊️ <b>Hassasiyet Türü:</b> Vefat / Cenaze / Taziye / Trajik Gelişme\n"
        f"🔍 <b>Olgusal Teyit:</b> {e(v_label)}\n\n"
        f"<b>Editoryal Gerekçe:</b>\n{e(rationale)}\n\n"
        f"<b>Güvenlik & Kilit Durumu:</b>\n<i>{e(safety_note)}</i>\n\n"
        f"<b>Kaynaklar:</b>\n{sources_html}\n\n"
        "⛔ <b>KESİN KURAL:</b> <i>Bu haber hiçbir genel kanala veya Instagram'a otomatik olarak gönderilmez. "
        "Product Owner tekil ve açık onay vermedikçe yayınlanamaz.</i>"
    )


def format_telegram_verification_hold(assessment: EditorialAssessment) -> str:
    """Format an unverified rumor or speculation needing verification (HOLD_FOR_VERIFICATION)."""
    e = html.escape
    v_label = VERIFICATION_STATUS_TR.get(assessment.verification_status, assessment.verification_status.value)
    sources_html = format_sources_html(assessment)
    rationale = assessment.why_efso_tr or assessment.relevance_reason_tr or "Teyit gerektiren editoryal iddia."
    uncertainties = assessment.uncertainties_tr or "Resmi kurumlardan doğrulama bulunmuyor; iddia niteliğindedir."

    return (
        "<b>🔍 EFSO RADAR | DOĞRULAMA BEKLEYEN İDDİA (HOLD FOR VERIFICATION)</b>\n"
        "⏳ <b>YAYINLANAMAZ — EDİTORYAL TEYİT BEKLEMESİNDE</b>\n\n"
        f"<b>{e(assessment.headline_tr)}</b>\n\n"
        f"⭐ <b>Efso Skoru:</b> {assessment.score}/100   |   🏷️ <b>Kategori:</b> {e(assessment.category.value)}\n"
        f"⚠️ <b>İddia Niteliği:</b> {e(v_label)}\n\n"
        f"<b>Editoryal İlgi:</b>\n{e(rationale)}\n\n"
        f"<b>Teyit Notu:</b>\n<i>{e(uncertainties)}</i>\n\n"
        f"<b>Kaynaklar:</b>\n{sources_html}\n\n"
        "🛑 <i>Olgusal teyit ve birincil kaynak doğrulaması tamamlanmadan yayın kuyruğuna alınamaz. "
        "Instagram'da paylaşılamaz.</i>"
    )


def format_telegram_alert(assessment: EditorialAssessment) -> str:
    """Format an approved editorial assessment into an HTML Telegram message according to its safety state."""
    # 1. Sensitive Tragedy / Bereavement Review
    if (
        assessment.disposition == EditorialDisposition.SENSITIVE_REVIEW
        or (assessment.uncertainties_tr and "SENSITIVE EDITORIAL REVIEW" in assessment.uncertainties_tr)
    ):
        return format_telegram_sensitive_review(assessment)

    # 2. Manual Verification Hold
    if (
        not assessment.is_publishable
        or assessment.disposition == EditorialDisposition.HOLD
        or assessment.verification_status in (VerificationStatus.UNVERIFIED_RUMOR, VerificationStatus.SPECULATION)
        or (assessment.uncertainties_tr and "MANUAL VERIFICATION REQUIRED" in assessment.uncertainties_tr)
    ):
        return format_telegram_verification_hold(assessment)

    # 3. Verified Breaking / Routine Alert
    return format_telegram_breaking(assessment)



def _is_valid_nonnegative_int(val: Any) -> bool:
    """Check if value is a valid non-negative integer (not bool, not float, not string)."""
    return isinstance(val, int) and not isinstance(val, bool) and val >= 0


def _is_valid_positive_float(val: Any) -> bool:
    """Check if value is a valid finite positive float/int (rejects bool, non-finite, <= 0)."""
    if val is None or isinstance(val, bool):
        return False
    if not isinstance(val, (int, float)):
        return False
    import math
    return math.isfinite(val) and val > 0.0


def _is_valid_nonnegative_cost(val: Any) -> bool:
    """Check if value is a valid finite non-negative float/int (rejects bool, non-finite, < 0)."""
    if val is None or isinstance(val, bool):
        return False
    if not isinstance(val, (int, float)):
        return False
    import math
    return math.isfinite(val) and val >= 0.0


def format_telegram_admin_health_report(
    metrics: dict[str, Any],
    now: Optional[datetime] = None,
) -> str:
    """Format daily operational health report into Telegram HTML for the ADMIN recipient.

    Strictly contains zero editorial gossip or article news items; reports operational
    system metrics, scan reliability, delivery health, and zero-spend accounting.
    Complies with docs/DAILY_ADMIN_HEALTH_REPORT_DESIGN.md.

    Guarantees (Issue 1 Acceptance Fixes):
    - Never derives delivery success percentage from error count alone.
      When total attempts / successes are unavailable, reports error count with percentage UNKNOWN.
    - Rejects negative, fractional, or inconsistent operational counters.
    - Rejects rss_windows_success greater than rss_windows_total.
    - Rejects non-finite USD costs or exchange rates (NaN, Infinity).
    - Rejects negative costs; never interprets them as valid expenditure.
    - Missing one member of a metric pair does not fabricate zero for that missing value.
    - Preserves correct 0,00 TRY / $0.00 formatting for explicitly verified zero-cost fixtures.
    - Handles None, zero denominators, malformed types, and HTML escaping safely.
    """
    e = html.escape
    timestamp_str = format_turkey_timestamp(now, with_middle_dot=True)

    # 1. System Status
    status_raw = metrics.get("system_status") if isinstance(metrics, dict) else None
    if status_raw is None or not isinstance(status_raw, str) or not status_raw.strip():
        status = "BİLİNMİYOR (VERİ YOK)"
        status_icon = "⚪"
    else:
        status = status_raw.strip()
        status_icon = "🟢" if ("NORMAL" in status or "SAĞLIKLI" in status) else ("🟡" if "DEGRADE" in status else "🔴")

    # 2. RSS Scan Windows
    rss_success_raw = metrics.get("rss_windows_success") if isinstance(metrics, dict) else None
    rss_total_raw = metrics.get("rss_windows_total") if isinstance(metrics, dict) else None
    valid_rss_success = _is_valid_nonnegative_int(rss_success_raw)
    valid_rss_total = _is_valid_nonnegative_int(rss_total_raw)

    if not valid_rss_success or not valid_rss_total:
        rss_line = "• Başarılı: <i>BİLİNMİYOR (Veri Kaydı Yok)</i>"
    elif rss_success_raw > rss_total_raw:
        rss_line = "• Başarılı: <i>BİLİNMİYOR (Tutarsız Veri: Başarılı > Toplam)</i>"
    elif rss_total_raw == 0:
        rss_line = "• Başarılı: 0 / 0 pencere (Tarama Penceresi Yok)"
    else:
        rss_pct = int((rss_success_raw / rss_total_raw) * 100)
        rss_line = f"• Başarılı: {rss_success_raw} / {rss_total_raw} pencere (%{rss_pct})"

    last_rss_raw = metrics.get("last_rss_success_ts") if isinstance(metrics, dict) else None
    if last_rss_raw and isinstance(last_rss_raw, str) and last_rss_raw.strip():
        last_rss_line = f"• Son Başarılı Tarama: {e(last_rss_raw.strip())}"
    else:
        last_rss_line = "• Son Başarılı Tarama: <i>BİLİNMİYOR</i>"

    scanned_raw = metrics.get("total_articles_scanned") if isinstance(metrics, dict) else None
    if _is_valid_nonnegative_int(scanned_raw):
        scanned_line = f"• Toplam Taranan Makale: {scanned_raw}"
    else:
        scanned_line = "• Toplam Taranan Makale: <i>BİLİNMİYOR</i>"

    # 3. Editorial Metrics
    evaluated_raw = metrics.get("candidates_evaluated") if isinstance(metrics, dict) else None
    if _is_valid_nonnegative_int(evaluated_raw):
        evaluated_line = f"• Değerlendirilen / Havuzdaki Aday: {evaluated_raw}"
    else:
        evaluated_line = "• Değerlendirilen / Havuzdaki Aday: <i>BİLİNMİYOR</i>"

    digests_sent_raw = metrics.get("editor_digests_sent") if isinstance(metrics, dict) else None
    news_items_raw = metrics.get("editor_news_items") if isinstance(metrics, dict) else None
    valid_digests = _is_valid_nonnegative_int(digests_sent_raw)
    valid_items = _is_valid_nonnegative_int(news_items_raw)

    if valid_digests and valid_items:
        if digests_sent_raw > 0 or news_items_raw > 0:
            editor_delivery_line = f"• PRIMARY_EDITOR Teslimatı: {digests_sent_raw} bülten ({news_items_raw} haber)"
        else:
            editor_delivery_line = "• PRIMARY_EDITOR Teslimatı: 0 bülten, 0 haber (sakin haber günü)"
    elif valid_digests and not valid_items:
        editor_delivery_line = f"• PRIMARY_EDITOR Teslimatı: {digests_sent_raw} bülten (Haber Sayısı Bilinmiyor)"
    elif not valid_digests and valid_items:
        editor_delivery_line = f"• PRIMARY_EDITOR Teslimatı: Bülten Sayısı Bilinmiyor ({news_items_raw} haber)"
    else:
        editor_delivery_line = "• PRIMARY_EDITOR Teslimatı: <i>BİLİNMİYOR (Veri Kaydı Yok)</i>"

    # Delivery Success Rate: Never derive percentage from errors alone
    delivery_errs_raw = metrics.get("delivery_errors") if isinstance(metrics, dict) else None
    attempts_raw = metrics.get("delivery_attempts") if isinstance(metrics, dict) else None
    successes_raw = metrics.get("delivery_successes") if isinstance(metrics, dict) else None

    valid_errs = _is_valid_nonnegative_int(delivery_errs_raw)
    valid_attempts = _is_valid_nonnegative_int(attempts_raw)
    valid_successes = _is_valid_nonnegative_int(successes_raw)

    if valid_errs:
        if valid_attempts:
            if attempts_raw == 0:
                delivery_line = "• Teslimat Başarısı: 0 / 0 teslimat (Teslimat Denemesi Yok)"
            elif valid_successes:
                if successes_raw > attempts_raw:
                    delivery_line = "• Teslimat Başarısı: <i>BİLİNMİYOR (Tutarsız Veri: Başarılı > Toplam)</i>"
                else:
                    succ_pct = int((successes_raw / attempts_raw) * 100)
                    delivery_line = f"• Teslimat Başarısı: %{succ_pct} (Toplam: {attempts_raw}, Hata: {delivery_errs_raw})"
            elif delivery_errs_raw > attempts_raw:
                delivery_line = "• Teslimat Başarısı: <i>BİLİNMİYOR (Tutarsız Veri: Hata > Toplam)</i>"
            else:
                succ = attempts_raw - delivery_errs_raw
                succ_pct = int((succ / attempts_raw) * 100)
                delivery_line = f"• Teslimat Başarısı: %{succ_pct} (Toplam: {attempts_raw}, Hata: {delivery_errs_raw})"
        else:
            # Attempts unavailable: report error count and mark success percentage UNKNOWN
            delivery_line = f"• Teslimat Başarısı: <i>BİLİNMİYOR</i> (Hata: {delivery_errs_raw})"
    else:
        delivery_line = "• Teslimat Başarısı: <i>BİLİNMİYOR (Veri Kaydı Yok)</i>"

    # 4. Budget & Cost Accounting
    gemini_calls_raw = metrics.get("gemini_calls") if isinstance(metrics, dict) else None
    if _is_valid_nonnegative_int(gemini_calls_raw):
        gemini_calls_str = str(gemini_calls_raw)
    else:
        gemini_calls_str = "<i>BİLİNMİYOR</i>"

    cost_usd_raw = metrics.get("cost_usd") if isinstance(metrics, dict) else None
    if not _is_valid_nonnegative_cost(cost_usd_raw):
        cost_line = "• Dahili Hesaplanan Harcama: <i>BİLİNMİYOR (Harcama Kanıtı Yok)</i>"
    else:
        cost_usd = float(cost_usd_raw)
        if cost_usd == 0.0:
            cost_line = "• Dahili Hesaplanan Harcama: <b>0,00 TRY / $0.00</b>"
        else:
            rate_raw = metrics.get("usd_to_try_rate") if isinstance(metrics, dict) else None
            if _is_valid_positive_float(rate_raw):
                rate = float(rate_raw)
                cost_try = cost_usd * rate
                cost_try_str = f"{cost_try:.2f}".replace(".", ",")
                cost_line = f"• Dahili Hesaplanan Harcama: <b>{cost_try_str} TRY / ${cost_usd:.2f}</b> (Kur: {rate:.2f})"
            else:
                cost_line = f"• Dahili Hesaplanan Harcama: <b>${cost_usd:.2f}</b> <i>(TRY Karşılığı Bilinmiyor — Kur Doğrulanmadı)</i>"

    # 5. Operational Notes
    notes_raw = metrics.get("operational_notes") if isinstance(metrics, dict) else None
    if not notes_raw or not isinstance(notes_raw, list):
        notes_html = "• <i>Kayıtlı operasyonel not bulunmuyor.</i>"
    else:
        valid_notes = [e(str(n).strip()) for n in notes_raw if n and str(n).strip()]
        notes_html = "\n".join(f"• {n}" for n in valid_notes) if valid_notes else "• <i>Kayıtlı operasyonel not bulunmuyor.</i>"

    return (
        f"🩺 <b>EFSO RADAR · GÜNLÜK SAĞLIK RAPORU</b>\n"
        f"📅 <b>{timestamp_str}</b>\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"{status_icon} <b>SİSTEM DURUMU: {e(status)}</b>\n"
        f"━━━━━━━━━━━━━━\n\n"
        f"📡 <b>RSS Taramaları:</b>\n"
        f"{rss_line}\n"
        f"{last_rss_line}\n"
        f"{scanned_line}\n\n"
        f"🎯 <b>Editoryal Süreç:</b>\n"
        f"{evaluated_line}\n"
        f"{editor_delivery_line}\n"
        f"{delivery_line}\n"
        f"<i>(Not: Sakin günlerde 0 bülten olağan editoryal filtreleme sonucudur.)</i>\n\n"
        f"💰 <b>Bütçe & Harcama:</b>\n"
        f"• Gemini Free-Tier Çağrısı: {gemini_calls_str}\n"
        f"{cost_line}\n"
        f"• Sağlayıcı Fatura Durumu: <i>Doğrulanmamış (Konsoldan teyit gerekir)</i>\n\n"
        f"⚠️ <b>Operasyonel Notlar:</b>\n"
        f"{notes_html}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"🔒 <i>Bu rapor yalnızca sistem yöneticisine (ADMIN) iletilen operasyonel durum özetidir.</i>"
    )
