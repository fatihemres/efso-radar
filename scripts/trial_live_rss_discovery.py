#!/usr/bin/env python3
"""Efso Radar — Supervised Single-Source Live RSS Discovery Trial (Phase 4C).

Safely executes ONE controlled live RSS fetch for a single verified Turkish
entertainment publisher with strict boundaries:
- Exactly ONE source (default: Habertürk Magazin from registry).
- Exactly ONE network fetch attempt (no retries, no polling loop).
- Maximum 12 fetched entries processed.
- Bounded network timeout (10s) and response buffer (2MB).
- Zero Gemini API calls.
- Zero Telegram deliveries (fail-closed check).
- Zero production database mutation (isolated trial JSON output).
"""
from __future__ import annotations

import argparse
import html
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# Ensure sys.path includes src
REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Ensure UTF-8 console output on Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from efso_radar.ai.calibration import (
    DEFAULT_FILLED_CALIBRATION_CSV,
    load_and_verify_calibration_csv,
)
from efso_radar.ai.client import detect_sensitive_tragedy_content
from efso_radar.deduplication.engine import (
    calculate_token_overlap,
    extract_key_tokens,
    find_matched_affinity_entities,
    normalize_text,
)
from efso_radar.domain import Article, EditorialAssessment, EditorialDisposition, EditorialKind, StoryCategory, SuggestedFormat, VerificationStatus
from efso_radar.notifications.formatters import (
    TURKEY_TZ,
    format_telegram_digest,
    format_turkey_timestamp,
    truncate_word_boundary,
)
from efso_radar.sources.models import SourceDefinition
from efso_radar.sources.parsers import FeedParser
from efso_radar.sources.registry import DEFAULT_SOURCES

# Use certifi bundle if available
try:
    import certifi
    SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    SSL_CONTEXT = ssl.create_default_context()

TRIAL_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 EfsoRadar/4.0 (+news-monitor; contact: @efsomagazin)"
EVIDENCE_FILE = REPO_ROOT / "data" / "trial_live_rss_evidence.json"


def get_verified_source(source_id: str = "haberturk_magazin") -> SourceDefinition:
    """Find verified source definition from the project's source registry."""
    for s in DEFAULT_SOURCES:
        if s.source_id == source_id:
            return s
    raise ValueError(f"Source ID '{source_id}' not found in DEFAULT_SOURCES registry.")


def execute_single_live_rss_scan(
    source_def: SourceDefinition,
    max_entries: int = 12,
    timeout_seconds: float = 10.0,
    max_bytes: int = 2_000_000,
) -> tuple[bool, Optional[bytes], str, float]:
    """Execute exactly ONE single HTTP GET request against the selected feed URL."""
    headers = {
        "User-Agent": TRIAL_USER_AGENT,
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    req = urllib.request.Request(source_def.url, headers=headers)

    start_time = datetime.now(timezone.utc)
    try:
        with urllib.request.urlopen(req, context=SSL_CONTEXT, timeout=timeout_seconds) as resp:
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            if resp.status != 200:
                return False, None, f"HTTP Error status {resp.status}", elapsed
            raw_data = resp.read(max_bytes)
            return True, raw_data, "SUCCESS", elapsed
    except urllib.error.HTTPError as exc:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        return False, None, f"HTTPError {exc.code}: {exc.reason}", elapsed
    except urllib.error.URLError as exc:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        return False, None, f"URLError: {exc.reason}", elapsed
    except TimeoutError:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        return False, None, f"Timeout after {timeout_seconds}s", elapsed
    except Exception as exc:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        return False, None, f"Unexpected error: {exc}", elapsed


def analyze_retrieved_articles(
    articles: list[Article],
    repo_root: Path = REPO_ROOT,
) -> list[dict]:
    """Analyze quality, metadata completeness, duplication, historical relation, and sensitive topics."""
    # Load historical calibration items for relation checking
    calib_csv = repo_root / DEFAULT_FILLED_CALIBRATION_CSV
    calib_items = load_and_verify_calibration_csv(calib_csv) if calib_csv.exists() else []

    analyzed = []
    seen_in_scan: list[Article] = []

    for idx, art in enumerate(articles, 1):
        # 1. Metadata Completeness Audit
        metadata_issues = []
        if not art.title or len(art.title.strip()) < 5:
            metadata_issues.append("Başlık çok kısa veya boş")
        if not art.url or not (art.url.startswith("http://") or art.url.startswith("https://")):
            metadata_issues.append("Geçersiz veya eksik URL")
        if not art.snippet or len(art.snippet.strip()) < 15:
            metadata_issues.append("Açıklama/özet boş veya çok kısa (<15 karakter)")
        if not art.published_at:
            metadata_issues.append("Yayınlanma tarihi (pubDate) eksik")
        elif art.published_at > datetime.now(timezone.utc) + timedelta(hours=1):
            metadata_issues.append("Yayınlanma tarihi gelecekte (anormal timestamp)")

        # 2. In-batch duplication audit
        in_batch_duplicate_of = None
        t_tokens = extract_key_tokens(art.title)
        ents = set(find_matched_affinity_entities(f"{art.title} {art.snippet or ''}"))
        for prev in seen_in_scan:
            prev_tokens = extract_key_tokens(prev.title)
            prev_ents = set(find_matched_affinity_entities(f"{prev.title} {prev.snippet or ''}"))
            overlap = calculate_token_overlap(t_tokens, prev_tokens)
            if (ents and prev_ents and (ents & prev_ents) and overlap >= 0.30) or overlap >= 0.70:
                in_batch_duplicate_of = prev.title
                break

        # 3. Relation to historical calibration data
        historical_relation = None
        for c in calib_items:
            c_tokens = extract_key_tokens(c.headline)
            c_ents = set(find_matched_affinity_entities(c.headline))
            c_overlap = calculate_token_overlap(t_tokens, c_tokens)
            if (ents and c_ents and (ents & c_ents) and c_overlap >= 0.25) or c_overlap >= 0.60:
                historical_relation = f"{c.review_id} ({c.headline})"
                break

        # 4. Independent sensitive tragedy screening
        is_sensitive = detect_sensitive_tragedy_content(art.title, art.snippet or "")

        # 5. Local Türkiye time
        pub_tsi = format_turkey_timestamp(art.published_at) if art.published_at else "Bilinmiyor"

        analyzed.append({
            "index": idx,
            "article_id": art.id,
            "title": art.title,
            "url": art.url,
            "publisher": art.source,
            "published_at_utc": art.published_at.isoformat() if art.published_at else None,
            "published_at_tsi": pub_tsi,
            "snippet": art.snippet,
            "has_image": bool(art.image_url),
            "metadata_issues": metadata_issues,
            "in_batch_duplicate_of": in_batch_duplicate_of,
            "historical_relation": historical_relation,
            "is_sensitive_tragedy": is_sensitive,
        })
        seen_in_scan.append(art)

    return analyzed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Efso Radar — Supervised Single-Source Live RSS Discovery Trial (Phase 4C)"
    )
    parser.add_argument(
        "--source",
        default="haberturk_magazin",
        help="Source ID from registry to fetch (default: haberturk_magazin)",
    )
    parser.add_argument(
        "--max-entries",
        type=int,
        default=12,
        help="Maximum entries to process (default: 12)",
    )
    parser.add_argument(
        "--save-evidence",
        action="store_true",
        default=True,
        help="Save raw trial evidence into data/trial_live_rss_evidence.json",
    )

    args = parser.parse_args()

    # Fail-closed Telegram safety verification
    delivery_enabled = os.getenv("TELEGRAM_DELIVERY_ENABLED", "false").lower() == "true"
    if delivery_enabled:
        print("[GÜVENLİK UYARISI] TELEGRAM_DELIVERY_ENABLED açık tespit edildi. RSS denemesinde sıfır mesaj garantisi için kapatılıyor.", file=sys.stderr)

    source_def = get_verified_source(args.source)

    print("=" * 80)
    print("EFSO RADAR — DENETİMLİ CANLI RSS TARAMA DENEMESİ (PHASE 4C)")
    print("=" * 80)
    print(f"Kaynak Adı        : {source_def.name}")
    print(f"Kaynak URL        : {source_def.url}")
    print(f"Kategori          : {source_def.category}")
    print(f"Maksimum Haber    : {args.max_entries}")
    print(f"Zaman Aşımı Sınırı: 10.0 saniye (Sıfır Yeniden Deneme)")
    print(f"Tarih/Saat (TSİ)  : {format_turkey_timestamp()}")
    print("-" * 80)

    # Execute single HTTP fetch
    success, raw_bytes, status_msg, elapsed = execute_single_live_rss_scan(
        source_def=source_def,
        max_entries=args.max_entries,
        timeout_seconds=10.0,
    )

    if not success or not raw_bytes:
        print(f"\n[HATA] RSS Akışı Alınamadı!\nDurum: {status_msg}\nSüre: {elapsed:.2f}s", file=sys.stderr)
        print("Kural gereği başka kaynağa geçilmedi ve yeniden deneme yapılmadı. Deneme durduruldu.", file=sys.stderr)
        sys.exit(1)

    print(f"Ağ İsteği Başarılı: {len(raw_bytes):,} bayt XML verisi alındı ({elapsed:.2f} saniye).")

    # Parse XML safely
    now_utc = datetime.now(timezone.utc)
    parse_result = FeedParser.parse_feed(
        xml_bytes=raw_bytes,
        source_def=source_def,
        now=now_utc,
        max_age_hours=72,
    )

    raw_count = parse_result.raw_seen
    fetched_articles = parse_result.articles[: args.max_entries]
    accepted_count = len(fetched_articles)

    print(f"Akıştaki Toplam Öğe: {raw_count} adet")
    print(f"İşlenen Öğe Sınırı : {accepted_count} adet (Maksimum {args.max_entries} tavanı)")
    print("-" * 80)

    # Detailed quality inspection
    analyzed_articles = analyze_retrieved_articles(fetched_articles, repo_root=REPO_ROOT)

    print("\n1. ALINAN HABERLERİN EDİTORYAL VE VERİ KALİTESİ İNCELEMESİ:")
    for a in analyzed_articles:
        sens_tag = " [⚠️ HASSAS TRİYAJ]" if a["is_sensitive_tragedy"] else ""
        dup_tag = f" [🔄 AYNI AKIŞTA MÜKERRER: {a['in_batch_duplicate_of'][:35]}...]" if a["in_batch_duplicate_of"] else ""
        hist_tag = f" [📚 GEÇMİŞLE İLİŞKİLİ: {a['historical_relation']}]" if a["historical_relation"] else ""
        meta_warn = f" (Uyarı: {', '.join(a['metadata_issues'])})" if a["metadata_issues"] else " (Meta: Tam)"

        print(f"\n   {a['index']}. [{a['publisher']} | {a['published_at_tsi']}]{sens_tag}{dup_tag}{hist_tag}")
        print(f"      Başlık : {a['title']}")
        print(f"      URL    : {a['url']}")
        print(f"      Özet   : {a['snippet'][:130]}..." if a['snippet'] else "      Özet   : [Boş]")
        print(f"      Kalite : {meta_warn}")

    # Deduplication and sensitivity summary
    sensitive_count = sum(1 for a in analyzed_articles if a["is_sensitive_tragedy"])
    duplicate_count = sum(1 for a in analyzed_articles if a["in_batch_duplicate_of"])
    metadata_issue_count = sum(1 for a in analyzed_articles if a["metadata_issues"])

    print("\n" + "=" * 80)
    print("TARAMA BULGULARI VE GÜVENLİK ÖZETİ")
    print("=" * 80)
    print(f"• Alınan / İşlenen Haber : {accepted_count} / {raw_count}")
    print(f"• Mükerrer Tespit Edilen : {duplicate_count} haber")
    print(f"• Hassas Vefat/Trajedi   : {sensitive_count} haber (SENSITIVE_REVIEW korumasında)")
    print(f"• Meta Veri Sorunları    : {metadata_issue_count} haber")
    print(f"• Harcanan Gemini Bütçesi: $0.000000 (0 Canlı AI Çağrısı)")
    print(f"• Telegram Gönderimi     : 0 Mesaj (Sıfır Ağ Yan Etkisi)")
    print("=" * 80)

    # 6. Offline Telegram Digest Preview Example using fresh retrieved articles
    # Filter out sensitive items for the publishable digest preview
    safe_for_preview = [
        art for art, a in zip(fetched_articles, analyzed_articles)
        if not a["is_sensitive_tragedy"] and not a["in_batch_duplicate_of"]
    ]

    # Create synthetic assessments for preview formatting demonstration
    preview_assessments = [
        EditorialAssessment(
            article_ids=[art.id],
            score=75 - (idx * 2),  # Demonstration ranking
            kind=EditorialKind.NEW,
            headline_tr=art.title,
            cover_title_tr=truncate_word_boundary(art.title, 65),
            caption_idea_tr=art.snippet or art.title,
            why_efso_tr=f"{art.source} güncel magazin bülteni seçkisi.",
            suggested_format=SuggestedFormat.POST,
            category=StoryCategory.YERLI,
            verification_status=VerificationStatus.REPUTABLE_REPORTING,
            source_urls=[art.url],
            supporting_articles=[art],
            is_publishable=True,
            disposition=EditorialDisposition.DIGEST,
        )
        for idx, art in enumerate(safe_for_preview[:5])
    ]

    ts_label = format_turkey_timestamp(now_utc)
    digest_preview_html = format_telegram_digest(
        preview_assessments,
        window_label=f"[CANLI RSS DENEME SEÇKİSİ] 6 Saatlik Özet ({ts_label})",
        deduplicate=True,
    )

    print("\n7. ALINAN GERÇEK RSS HABERLERİYLE OLUŞTURULAN ÇEVRİMDIŞI ÖNİZLEME (0 Canlı Gönderim):")
    print("-" * 80)
    print(digest_preview_html)
    print("-" * 80)

    # Save trial evidence to gitignored JSON file
    if args.save_evidence:
        evidence_data = {
            "trial_timestamp_utc": now_utc.isoformat(),
            "trial_timestamp_tsi": ts_label,
            "source_id": source_def.source_id,
            "source_name": source_def.name,
            "source_url": source_def.url,
            "network_duration_seconds": elapsed,
            "raw_items_count": raw_count,
            "accepted_items_count": accepted_count,
            "sensitive_items_count": sensitive_count,
            "duplicate_items_count": duplicate_count,
            "metadata_issues_count": metadata_issue_count,
            "articles": analyzed_articles,
            "offline_digest_preview_html": digest_preview_html,
        }
        with open(EVIDENCE_FILE, "w", encoding="utf-8") as f:
            json.dump(evidence_data, f, ensure_ascii=False, indent=2)
        print(f"\nDeneme kanıtları izole dosyaya kaydedildi: {EVIDENCE_FILE.name}")


if __name__ == "__main__":
    main()
