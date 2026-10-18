"""Dedicated One-Shot Telegram Connectivity Test Script for Efso Radar.

Guarantees & Constraints:
1. Zero RSS scanning.
2. Zero Gemini inference.
3. Zero background scheduling.
4. Zero production outbox queue draining.
5. Requires explicitly configured, allowlisted private owner chat ID.
6. Never automatically selects a chat from getUpdates.
7. Performs exactly ONE sendMessage attempt (no automatic retries).
8. Masks all tokens and secrets in console output.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Add src to sys.path if not already present
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from efso_radar.config import Settings
from efso_radar.notifications.telegram import run_one_shot_connectivity_test


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dedicated one-shot Telegram connectivity test for Efso Radar."
    )
    parser.add_argument(
        "--live",
        "--execute-live-send",
        dest="execute_live",
        action="store_true",
        help="Execute exactly ONE live test message to the configured allowlisted private owner chat ID.",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Perform pre-flight safety audit and preview message without live network dispatch.",
    )
    parser.add_argument(
        "--custom-text",
        dest="custom_text",
        default=None,
        help="Optional custom message text (HTML).",
    )
    args = parser.parse_args()

    settings = Settings.from_env()

    print("=" * 65)
    print("  EFSO RADAR — DEDICATED ONE-SHOT TELEGRAM CONNECTIVITY TEST")
    print("=" * 65)
    print(f"• Maskeli Bot Tokeni : {settings.get_masked_telegram_token()}")
    print(f"• Maskeli Chat ID    : {settings.get_masked_chat_id()}")
    print(f"• İzin Listesi Adedi : {len(settings.telegram_allowed_chat_ids)}")
    print(f"• Canlı Gönderim Kilidi : {'AÇIK (TELEGRAM_DELIVERY_ENABLED=true)' if settings.telegram_delivery_enabled else 'KAPALI (TELEGRAM_DELIVERY_ENABLED=false)'}")
    print("• RSS Taraması       : DEVRE DIŞI (0 çağrı)")
    print("• Gemini AI Çıkarımı : DEVRE DIŞI (0 çağrı)")
    print("• Arka Plan Zamanlama: DEVRE DIŞI (0 işlem)")
    print("• Outbox Kuyruk Boşaltma: DEVRE DIŞI (0 işlem)")
    print("-" * 65)

    is_dry_run = args.dry_run or (not args.execute_live)

    if not args.execute_live and not args.dry_run:
        print("\n[BİLGİ] --live veya --execute-live-send bayrağı belirtilmediği için")
        print("güvenli ön denetim (dry-run) modunda çalıştırılıyor.")
        print("Canlı tek bir test mesajı göndermek için: python scripts/test_telegram_connectivity.py --live\n")

    # If user explicitly requested live send, ensure delivery setting is enabled for this test
    test_settings = settings
    if args.execute_live:
        test_settings = settings.model_copy(update={"telegram_delivery_enabled": True})

    result = run_one_shot_connectivity_test(
        settings=test_settings,
        dry_run=is_dry_run,
        test_message_custom=args.custom_text,
    )

    print("\n--- TEST SONUCU ---")
    status = result.get("status")
    print(f"Durum Kodu     : {status}")
    print(f"İstek Yapıldı mı: {'EVET (1 Deneme)' if result.get('attempt_made') else 'HAYIR (0 Deneme)'}")

    if status == "DELIVERY_SUCCESS":
        print(f"\n[BAŞARILI] {result.get('message')}")
        print("Tekil özel bot-sahip sohbeti doğrulandı. Tam 1 deneme yapıldı, 0 mükerrer risk.")
        return 0

    if status == "SAFE_PREVIEW_PASSED":
        print(f"\n[ÖN DENETİM BAŞARILI] {result.get('message')}")
        print("\nRender Edilen HTML Mesaj Önizlemesi:")
        print("-" * 40)
        print(result.get("rendered_html", ""))
        print("-" * 40)
        print("\nCanlı tekil test mesajı göndermek için komut:")
        print("  python scripts/test_telegram_connectivity.py --live")
        return 0

    if status == "DELIVERY_UNCERTAIN":
        print(f"\n[BELİRSİZ DURUM] {result.get('error')}")
        print("Ağ veya sunucu zaman aşımı yaşandı. Telegram mesajı almış olabileceğinden")
        print("mükerrer gönderimi önlemek için otomatik yeniden deneme (retry) YAPILMADI.")
        return 1

    # Error cases
    print(f"\n[HATA] {result.get('error')}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
