"""Safe procedure to identify Telegram chat ID via configured bot (Admin & Primary Editor Onboarding).

Safety Invariants:
1. Never displays or logs the Telegram bot token.
2. Never automatically trusts or adopts an arbitrary sender without human verification.
3. Guides the owner to verify their username/code and manually configure .env.
4. Strictly flags and rejects channel/group chat IDs.
5. Clearly distinguishes existing Admin from new Primary Editor candidates.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add src to sys.path if not already present
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from efso_radar.config import Settings
from efso_radar.notifications.telegram import fetch_bot_updates_for_chat_identification


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Safe chat ID identification via configured Telegram bot (Admin or Primary Editor)."
    )
    parser.add_argument(
        "--primary-editor",
        dest="primary_editor_mode",
        action="store_true",
        default=False,
        help="Run in Primary Editor onboarding mode to discover and verify the girlfriend's chat ID.",
    )
    parser.add_argument(
        "--username",
        dest="expected_username",
        default=None,
        help="Telegram username (without @) to highlight matching incoming messages.",
    )
    parser.add_argument(
        "--secret",
        dest="expected_secret",
        default=None,
        help="A unique verification word or phrase sent to the bot (e.g. 'efso123').",
    )
    args = parser.parse_args()

    settings = Settings.from_env()

    title = "BAŞ EDİTÖR (PRIMARY EDITOR) ONBOARDING TESPİTİ" if args.primary_editor_mode else "GÜVENLİ TELEGRAM SAHİP CHAT ID TESPİTİ"
    print("=" * 70)
    print(f"  EFSO RADAR — {title}")
    print("=" * 70)
    print(f"• Maskeli Bot Tokeni : {settings.get_masked_telegram_token()}")
    print(f"• Mevcut Yönetici ID : {settings.get_masked_admin_chat_id()}")
    if settings.telegram_primary_editor_chat_id:
        print(f"• Mevcut Baş Editör  : {settings.get_masked_primary_editor_chat_id()}")
    print("• Gizlilik Koruması  : Token ve özel anahtarlar asla açık yazdırılmaz.")
    print("• Güvenlik Kuralı    : Yabancı gönderenler ASLA otomatik yetkilendirilmez.")
    print("• Grup/Kanal Kuralı  : Grup ve kanallar (-) kesinlikle reddedilir.")
    print("-" * 70)

    if not settings.telegram_bot_token:
        print("\n[HATA] TELEGRAM_BOT_TOKEN yapılandırılmamış.")
        print("Lütfen önce BotFather'dan aldığınız tokenı .env dosyanıza ekleyin:")
        print("TELEGRAM_BOT_TOKEN=\"your_bot_token\"")
        return 1

    print("\nTelegram botundan gelen son mesajlar alınıyor...")
    result = fetch_bot_updates_for_chat_identification(
        settings=settings,
        expected_username=args.expected_username,
        expected_secret_phrase=args.expected_secret,
    )

    if result.get("status") != "SUCCESS":
        print(f"\n[HATA] {result.get('error')}")
        return 1

    bot_username = result.get("bot_username")
    if bot_username:
        print(f"• Bot Kullanıcı Adı  : @{bot_username}")
        print(f"• Bot Bağlantısı     : https://t.me/{bot_username}")

    candidates = result.get("candidates", [])
    if not candidates:
        print("\n[BİLGİ] Botunuza henüz herhangi bir mesaj gelmemiş.")
        print("\nYapılması Gereken Adımlar:")
        if args.primary_editor_mode:
            bot_tag = f"@{bot_username}" if bot_username else "Telegram botunuzun"
            print(f"1. Kız arkadaşınıza {bot_tag} bağlantısını iletin.")
            print("2. Kendisinden botu başlatmasını ('/start') ve kısa bir mesaj (ör. 'merhaba') atmasını rica edin.")
            print("3. Mesaj gönderildikten sonra bu komutu tekrar çalıştırın:")
            print("   python scripts/identify_telegram_chat_id.py --primary-editor")
        else:
            print("1. Telegram uygulamanızda botunuzu bulun ve bir mesaj gönderin (ör. /start veya gizli bir kelime).")
            print("2. Ardından bu komutu tekrar çalıştırın:")
            print("   python scripts/identify_telegram_chat_id.py")
        return 0

    print(f"\nToplam {len(candidates)} adet gelen mesaj bulundu:")
    print("-" * 70)

    existing_admin_id = settings.telegram_chat_id.strip() if settings.telegram_chat_id else ""
    existing_editor_id = settings.telegram_primary_editor_chat_id.strip() if settings.telegram_primary_editor_chat_id else ""

    for i, c in enumerate(candidates, 1):
        cid = c["chat_id"]
        ctype = c["chat_type"]
        is_group = cid.startswith("-") or ctype in ("group", "supergroup", "channel")

        # Distinguish roles
        role_label = ""
        if cid == existing_admin_id:
            role_label = "  [👑 MEVCUT YÖNETİCİ (ADMIN) - ZATEN KAYITLI]"
        elif existing_editor_id and cid == existing_editor_id:
            role_label = "  [✨ MEVCUT BAŞ EDİTÖR (PRIMARY_EDITOR) - ZATEN KAYITLI]"
        elif is_group:
            role_label = "  [⛔ GRUP/KANAL - GEÇERSİZ VE DESTEKLENMEZ]"
        else:
            if args.primary_editor_mode:
                role_label = "  [👉 ADAY BAŞ EDİTÖR (PRIMARY_EDITOR)]"
            else:
                role_label = "  [ADAY GÖNDEREN]"

        match_tag = ""
        if c.get("is_verified_match") is True:
            match_tag = "  <-- [✅ BELİRTTİĞİNİZ BİLGİLERLE EŞLEŞTİ!]"
        elif c.get("is_verified_match") is False:
            match_tag = "  (Belirttiğiniz filtre ile eşleşmiyor)"

        print(f"[{i}] Gönderen       : {c['sender_name']} ({c['username']}){role_label}{match_tag}")
        print(f"    Chat ID        : {cid}")
        print(f"    Sohbet Türü    : {ctype}" + (" (REDDEDİLDİ: Özel sohbet olmalı)" if is_group else " (Özel Bireysel Sohbet)"))
        print(f"    Mesaj Özeti    : \"{c['text_preview']}\"")
        print()

    print("=" * 70)
    print("  ONBOARDING VE GÜVENLİK ADIMLARI")
    print("=" * 70)
    if args.primary_editor_mode:
        print("1. Yukarıdaki listeden kız arkadaşınıza ait mesajı, ismi ve kullanıcı adını doğrulayın.")
        print("2. Kendisinin Chat ID'sini kopyalayın (kesinlikle eksi '-' ile başlamamalıdır).")
        print("3. .env dosyanıza şu satırı ekleyin:")
        print()
        print("   TELEGRAM_PRIMARY_EDITOR_CHAT_ID=\"<KIZ_ARKADASININ_CHAT_ID'SI>\"")
        print()
        print("4. Efso Radar izin listesi otomatik olarak her iki yetkili kullanıcıyı kapsayacaktır.")
        print("5. Ekledikten sonra kontrol etmek için şu komutu çalıştırabilirsiniz:")
        print("   python scripts/preview_two_recipient_delivery.py")
    else:
        print("1. Yukarıdaki listeden KENDİ gönderdiğiniz mesajı ve Telegram kullanıcı adınızı teyit edin.")
        print("2. Kendinize ait Chat ID'yi kopyalayın.")
        print("3. .env dosyanıza manuel olarak şu satırı ekleyin:")
        print()
        print("   TELEGRAM_CHAT_ID=\"<KENDI_CHAT_ID'NIZ>\"")
        print()
    print("⚠️ KESİNLİKLE tanımadığınız veya doğrulamadığınız bir Chat ID'yi izin listesine eklemeyiniz.")
    print("⚠️ Grup veya kanal sohbetleri editoryal güvenlik gereği kabul edilmez.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
