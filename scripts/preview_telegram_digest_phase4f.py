"""
Phase 4F: Telegram Editorial Digest UX Redesign Offline Preview Script.

Performs offline preview and visual comparison of the redesigned Telegram digest layout.
Constraints:
- ZERO network requests (dry-run only).
- ZERO Gemini AI inference calls.
- ZERO RSS feeds fetched.
- Strictly authentic saved Phase 4E evidence.
"""
from __future__ import annotations

import html
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add src to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from efso_radar.notifications.formatters import (
    compute_stable_digest_identity,
    format_telegram_digest,
    format_telegram_digest_legacy,
    format_turkey_timestamp,
)
from scripts.deliver_telegram_digest_oneshot import build_final_three_assessments


def html_to_plain_text(rendered_html: str) -> str:
    """Convert Telegram HTML markup into clean readable console text for demonstration."""
    # Convert <a href="URL">TEXT</a> to TEXT (URL)
    text = re.sub(r'<a\s+href="([^"]+)">([^<]+)</a>', r'\2 (\1)', rendered_html)
    # Strip basic tags
    text = re.sub(r'</?(?:b|i|u|s|code|pre|blockquote)>', '', text)
    # Unescape HTML entities
    text = html.unescape(text)
    return text


def run_preview() -> dict[str, any]:
    assessments = build_final_three_assessments()
    from datetime import timedelta
    fixed_time = datetime(2026, 10, 18, 20, 53, tzinfo=timezone(timedelta(hours=3)))

    # 1. Previous format (Phase 4E legacy)
    prev_html = format_telegram_digest_legacy(
        assessments,
        window_label="Canlı RSS Seçkisi (18 Ekim 2026 21:07 TSİ)",
        deduplicate=False,
    )

    # 2. Redesigned format (Phase 4F Standard)
    new_html = format_telegram_digest(
        assessments,
        window_label=None,
        deduplicate=False,
        timestamp=fixed_time,
        validate_length=True,
        is_ux_test=False,
    )

    # 3. Redesigned format (Phase 4F Mobile UX Test Mode)
    ux_test_html = format_telegram_digest(
        assessments,
        window_label=None,
        deduplicate=False,
        timestamp=fixed_time,
        validate_length=True,
        is_ux_test=True,
    )

    stable_id = compute_stable_digest_identity(assessments)
    ux_test_stable_id = f"ux_test_{stable_id}"
    readable_preview = html_to_plain_text(new_html)
    ux_test_readable_preview = html_to_plain_text(ux_test_html)

    return {
        "story_count": len(assessments),
        "previous_html": prev_html,
        "previous_char_count": len(prev_html),
        "new_html": new_html,
        "new_char_count": len(new_html),
        "readable_preview": readable_preview,
        "ux_test_html": ux_test_html,
        "ux_test_char_count": len(ux_test_html),
        "ux_test_readable_preview": ux_test_readable_preview,
        "stable_digest_id": stable_id,
        "ux_test_stable_id": ux_test_stable_id,
        "timestamp_tsi": format_turkey_timestamp(fixed_time, with_middle_dot=True),
    }


def main() -> int:
    preview = run_preview()

    print("=" * 75)
    print("  EFSO RADAR — PHASE 4F TELEGRAM EDITORIAL DIGEST UX REDESIGN PREVIEW")
    print("=" * 75)
    print(f"• Hikaye Sayısı              : {preview['story_count']}")
    print(f"• Referans Zaman Damgası     : {preview['timestamp_tsi']}")
    print(f"• Kararlı Normal Kimlik      : {preview['stable_digest_id']}")
    print(f"• Kararlı UX Testi Kimliği   : {preview['ux_test_stable_id']}")
    print(f"• Eski Karakter Sayısı       : {preview['previous_char_count']} karakter")
    print(f"• Yeni Normal Karakter       : {preview['new_char_count']} karakter (Limit: 4096)")
    print(f"• UX Testi Karakter          : {preview['ux_test_char_count']} karakter (Limit: 4096)")
    print(f"• Güvenlik Doğrulaması       : SIFIR Ağ Çağrısı, SIFIR Gemini Çıkarımı, SIFIR RSS Taraması")
    print("-" * 75)

    print("\n[1] ÖNCEKİ FORMAT (Phase 4E):")
    print("-" * 50)
    print(preview["previous_html"])

    print("\n[2] YENİLENMİŞ FORMAT HTML (Phase 4F - Normal Bülten):")
    print("-" * 50)
    print(preview["new_html"])

    print("\n[3] OKUNABİLİR METİN ÖNİZLEMESİ (Mobil Görünüm Simülasyonu - Normal Bülten):")
    print("━" * 50)
    print(preview["readable_preview"])
    print("━" * 50)

    print("\n[4] MOBİL UX TESTİ HTML (Phase 4F - Özel Etiketli Test Bülteni):")
    print("-" * 50)
    print(preview["ux_test_html"])

    print("\n[5] OKUNABİLİR METİN ÖNİZLEMESİ (Mobil Görünüm Simülasyonu - UX Testi):")
    print("━" * 50)
    print(preview["ux_test_readable_preview"])
    print("━" * 50)

    print("\n[BİLGİ] Çevrimdışı görsel önizleme tamamlandı. Canlı mesaj gönderilmemiştir.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
