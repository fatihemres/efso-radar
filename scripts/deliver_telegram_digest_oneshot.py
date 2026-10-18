"""
Phase 4E: Dedicated One-Shot Telegram Digest Delivery Script for Efso Radar.

Guarantees & Constraints:
1. Zero RSS scanning, zero Gemini inference, zero background scheduling.
2. Uses exact 3 approved candidates from Phase 4D editorial review.
3. Requires allowlisted private owner chat ID only (rejects groups/channels).
4. Dedicated dry-run mode (default) and explicitly gated live mode (--live).
5. Masks all tokens and secrets in console output and return structures.
6. Persistent duplicate-send protection via ledger: blocks resending identical digest.
7. Single sendMessage attempt (max_attempts=1, zero automatic retries).
8. Safe handling of ambiguous network timeouts without retry.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add src to sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

# Reconfigure stdout/stderr for UTF-8 safety on Windows console
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

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
)
from efso_radar.notifications.formatters import format_turkey_timestamp
from efso_radar.notifications.telegram import deliver_one_shot_editorial_digest

DIGEST_LEDGER_PATH = REPO_ROOT / "data" / "telegram_digest_delivery_ledger.json"


def build_final_three_assessments() -> list[EditorialAssessment]:
    """Build the exact three approved candidates adhering strictly to Phase 4F UX polish."""
    now_utc = datetime.now(timezone.utc)

    # 1. Şükrü Özyıldız
    art_sukru = Article(
        id="c12aebfb09cea9753d28a1cb",
        title="Kardeşini evlendirdi",
        url="https://www.haberturk.com/magazin/foto/sukru-ozyildiz-in-kiz-kardesi-burcu-ozyildiz-onur-eksioglu-ile-evlendi-3920594",
        source="Habertürk Magazin",
        source_category="MAGAZIN_TR",
        market=Market.TR,
        published_at=datetime.fromisoformat("2026-10-18T07:50:28+00:00"),
        snippet="Şükrü Özyıldız'ın kız kardeşi Burcu Özyıldız, Onur Ekşioğlu ile evlendi. Oyuncu, kardeşinin düğününe dair fotoğrafları duygusal bir mesaj ile paylaştı.",
    )
    assess_sukru = EditorialAssessment(
        article_ids=[art_sukru.id],
        score=85,
        kind=EditorialKind.NEW,
        headline_tr="Şükrü Özyıldız'ın kız kardeşi evlendi: Oyuncudan duygusal paylaşım",
        cover_title_tr="ÖZYILDIZ AİLESİNDE DÜĞÜN",
        caption_idea_tr="Şükrü Özyıldız'ın kız kardeşinin düğününe dair paylaştığı duygusal mesaj.",
        relevance_reason_tr="Sevilen oyuncunun samimi aile kutlaması ve kişisel paylaşımı, yerli magazin akışında sıcak ve yüksek etkileşimli bir içerik değeri sunuyor.",
        why_efso_tr="Sevilen oyuncunun samimi aile kutlaması ve kişisel paylaşımı, yerli magazin akışında sıcak ve yüksek etkileşimli bir içerik değeri sunuyor.",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.YERLI,
        verification_status=VerificationStatus.FIRST_PARTY_STATEMENT,
        source_urls=[art_sukru.url],
        supporting_articles=[art_sukru],
        is_publishable=True,
        disposition=EditorialDisposition.DIGEST,
    )

    # 2. Nicolas Cage & Antonio Banderas
    art_cage = Article(
        id="bc038ac2e6c763dfd15c913f",
        title="Dev buluşma",
        url="https://www.haberturk.com/magazin/antonio-banderas-ve-nicolas-cage-true-detective-dizisinde-bulusuyor-3920600",
        source="Habertürk Magazin",
        source_category="MAGAZIN_TR",
        market=Market.TR,
        published_at=datetime.fromisoformat("2026-10-18T09:30:47+00:00"),
        snippet="Nicolas Cage'in başrol için anlaştığı 'True Detective' dizisinin yeni sezon kadrosuna Antonio Banderas'ın da katıldığı açıklandı.",
    )
    assess_cage = EditorialAssessment(
        article_ids=[art_cage.id],
        score=82,
        kind=EditorialKind.NEW,
        headline_tr="True Detective'in 5. Sezonunda Dev Buluşma: Nicolas Cage ile Antonio Banderas Bir Arada",
        cover_title_tr="TRUE DETECTIVE 5'TE DEV BULUŞMA",
        caption_idea_tr="Nicolas Cage ve Antonio Banderas True Detective dizisinde buluşuyor.",
        relevance_reason_tr="Dünya çapında iki usta aktörün HBO'nun prestijli serisinde ilk kez bir araya gelmesi, küresel dizi ve popüler kültür takipçileri için birinci sınıf bir editoryal değer sunuyor.",
        why_efso_tr="Dünya çapında iki usta aktörün HBO'nun prestijli serisinde ilk kez bir araya gelmesi, küresel dizi ve popüler kültür takipçileri için birinci sınıf bir editoryal değer sunuyor.",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.GLOBAL,
        verification_status=VerificationStatus.REPUTABLE_REPORTING,
        source_urls=[art_cage.url],
        supporting_articles=[art_cage],
        is_publishable=True,
        disposition=EditorialDisposition.DIGEST,
    )

    # 3. Gisele Bündchen
    art_gisele = Article(
        id="8262e6a3c4f2f3abf2534c98",
        title="Süper model sakatlandı",
        url="https://www.haberturk.com/magazin/foto/unlu-model-gisele-bundchen-sakatlandi-3920648",
        source="Habertürk Magazin",
        source_category="MAGAZIN_TR",
        market=Market.TR,
        published_at=datetime.fromisoformat("2026-10-18T13:49:36+00:00"),
        snippet="Eski Victoria's Secret meleği olan dünyaca ünlü Brezilyalı model Gisele Bündchen, koltuk değnekleriyle görüntülendi.",
    )
    assess_gisele = EditorialAssessment(
        article_ids=[art_gisele.id],
        score=75,
        kind=EditorialKind.NEW,
        headline_tr="Dünyaca Ünlü Süper Model Gisele Bündchen Koltuk Değnekleriyle Görüntülendi",
        cover_title_tr="GISELE BÜNDCHEN KOLTUK DEĞNEKLERİYLE",
        caption_idea_tr="Gisele Bündchen ortopedik bot ve koltuk değnekleriyle görüntülendi.",
        relevance_reason_tr="Moda dünyasının ikonik isminin sokak stilindeki somut fiziksel durumu, asılsız sağlık spekülasyonlarına girmeden küresel magazin ilgisi uyandıran net bir görsel gelişmedir.",
        why_efso_tr="Moda dünyasının ikonik isminin sokak stilindeki somut fiziksel durumu, asılsız sağlık spekülasyonlarına girmeden küresel magazin ilgisi uyandıran net bir görsel gelişmedir.",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.GLOBAL,
        verification_status=VerificationStatus.REPUTABLE_REPORTING,
        source_urls=[art_gisele.url],
        supporting_articles=[art_gisele],
        is_publishable=True,
        disposition=EditorialDisposition.DIGEST,
    )

    return [assess_sukru, assess_cage, assess_gisele]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dedicated one-shot Telegram editorial digest delivery workflow (Phase 4F Polish)."
    )
    parser.add_argument(
        "--live",
        dest="execute_live",
        action="store_true",
        help="Execute exactly ONE live digest message dispatch to authorized recipients.",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Perform safety, allowlist, duplicate-prevention, and formatting audit without network dispatch.",
    )
    parser.add_argument(
        "--ux-test",
        dest="is_ux_test",
        action="store_true",
        default=False,
        help="Prepare or send explicitly labeled mobile UX test message (preserves production digest idempotency).",
    )
    parser.add_argument(
        "--recipient",
        dest="recipient",
        choices=["all", "admin", "primary_editor"],
        default="all",
        help="Target recipient selection: 'all' (both recipients), 'admin' (only owner), or 'primary_editor' (only primary editor).",
    )
    args = parser.parse_args()

    settings = Settings.from_env()

    print("=" * 70)
    print(f"  EFSO RADAR — DEDICATED ONE-SHOT TELEGRAM DIGEST DELIVERY ({'UX TEST' if args.is_ux_test else 'PHASE 4G'})")
    print("=" * 70)
    print(f"• Maskeli Bot Tokeni : {settings.get_masked_telegram_token()}")
    print(f"• Yönetici (ADMIN)   : {settings.get_masked_admin_chat_id()}")
    print(f"• Baş Editör (EDITOR): {settings.get_masked_primary_editor_chat_id() if settings.telegram_primary_editor_chat_id else 'YAPILANDIRILMAMIŞ'}")
    print(f"• Hedef Alıcı Seçimi : {args.recipient.upper()}")
    print(f"• İzin Listesi Adedi : {len(settings.telegram_allowed_chat_ids)}")
    print(f"• Canlı Gönderim İzni: {'AÇIK (TELEGRAM_DELIVERY_ENABLED=true)' if settings.telegram_delivery_enabled else 'KAPALI (TELEGRAM_DELIVERY_ENABLED=false)'}")
    print(f"• Mesaj Türü         : {'MOBİL UX TESTİ (Özel Etiketli)' if args.is_ux_test else 'NORMAL EDİTORYAL BÜLTEN'}")
    print("• RSS Taraması       : DEVRE DIŞI (0 çağrı)")
    print("• Gemini AI Çıkarımı : DEVRE DIŞI (0 çağrı)")
    print("• Arka Plan Zamanlama: DEVRE DIŞI (0 işlem)")
    print("• Mükerrer Koruma    : data/telegram_digest_delivery_ledger.json")
    print("-" * 70)

    is_dry_run = args.dry_run or (not args.execute_live)

    if not args.execute_live and not args.dry_run:
        print("\n[BİLGİ] --live bayrağı belirtilmediği için güvenli ön denetim (dry-run) modunda çalıştırılıyor.")
        print("Gerçek gönderim için Product Owner'ın açık onayı ve yetkilendirilmiş komut gereklidir.\n")

    # If live dispatch was explicitly requested, ensure delivery setting is enabled for this process
    delivery_settings = settings
    if args.execute_live:
        delivery_settings = settings.model_copy(update={"telegram_delivery_enabled": True})

    assessments = build_final_three_assessments()

    window_label = f"Canlı RSS Seçkisi ({format_turkey_timestamp()})"
    result = deliver_one_shot_editorial_digest(
        settings=delivery_settings,
        assessments=assessments,
        dry_run=is_dry_run,
        window_label=window_label,
        ledger_path=DIGEST_LEDGER_PATH,
        is_ux_test=args.is_ux_test,
        target_recipient_role=args.recipient.upper(),
    )

    status = result.get("status")
    print("\n--- ÇALIŞTIRMA SONUCU ---")
    print(f"Genel Durum Kodu    : {status}")
    print(f"Ağ İsteği Yapıldı mı: {'EVET' if result.get('attempt_made') else 'HAYIR (0 Deneme)'}")
    print(f"Toplam Deneme Sayısı: {result.get('attempts_count', 0)}")
    print(f"Digest İçerik Hash  : {result.get('digest_hash')}")
    print(f"Haber Sayısı        : {result.get('stories_count', len(assessments))}")
    print(f"Hedeflenen Alıcı    : {result.get('recipients_count', 1)} kişi")

    # Per-recipient breakdown
    recipient_results = result.get("recipient_results", {})
    if recipient_results:
        print("\n--- ALICI BAZLI DURUM DAĞILIMI ---")
        for chat_id, r_info in recipient_results.items():
            r_role = r_info.get("recipient_role", "UNKNOWN")
            r_masked = r_info.get("masked_chat_id", "***")
            r_stat = r_info.get("status", "UNKNOWN")
            r_msg_id = r_info.get("message_id")
            print(f"• [{r_role}] ({r_masked}): {r_stat}" + (f" (message_id: {r_msg_id})" if r_msg_id else ""))
            if r_info.get("error"):
                print(f"    Uyarı/Hata: {r_info.get('error')}")
            if r_info.get("message"):
                print(f"    Bilgi: {r_info.get('message')}")

    if status in ("DELIVERY_SUCCESS", "PARTIAL_DELIVERY"):
        print(f"\n[İŞLEM TAMAMLANDI] {result.get('message')}")
        return 0

    if status == "SAFE_PREVIEW_PASSED":
        print(f"\n[ÖN DENETİM BAŞARILI] {result.get('message')}")
        print("\nRender Edilen Telegram HTML Bülten Metni (Her İki Alıcı İçin Birebir Aynı):")
        print("-" * 50)
        print(result.get("rendered_html", ""))
        print("-" * 50)
        return 0

    if status == "DUPLICATE_DELIVERY_PREVENTED":
        print(f"\n[MÜKERRER KORUMA ENGELİ] {result.get('error') or result.get('message')}")
        print("Hedeflenen tüm alıcılar için bu bülten zaten daha önce iletilmiş.")
        return 1

    if status == "DELIVERY_UNCERTAIN":
        print(f"\n[BELİRSİZ DURUM] {result.get('error') or result.get('message')}")
        print("Ağ veya sunucu zaman aşımı yaşandı. Telegram mesajı almış olabileceğinden")
        print("mükerrer gönderimi önlemek için otomatik yeniden deneme (retry) YAPILMADI.")
        return 1

    if result.get("error"):
        print(f"\n[HATA] {result.get('error')}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
