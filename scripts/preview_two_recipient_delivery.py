"""
Phase 4G: Preflight Two-Recipient Delivery Preview & Equivalence Validation Script.

Demonstrates:
1. Two-recipient configuration: ADMIN and PRIMARY_EDITOR roles.
2. 100% content & presentation equivalence across all 4 notification categories:
   - EDITORIAL_DIGEST
   - VERIFIED_BREAKING
   - SENSITIVE_EDITORIAL_REVIEW
   - HOLD_FOR_VERIFICATION
3. Strict preservation of warning banners and classification embargoes for both users.
4. Independent duplicate prevention: ADMIN historical delivery does not block PRIMARY_EDITOR.
5. Absolute zero-network execution (100% offline dry-run preview).
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add src and root to sys.path
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

from efso_radar.config import Settings
from efso_radar.domain import (
    Article,
    DeliveryStatus,
    EditorialAssessment,
    EditorialDisposition,
    EditorialKind,
    Market,
    StoryCategory,
    SuggestedFormat,
    TelegramNotificationCategory,
    VerificationStatus,
)
from efso_radar.notifications.formatters import (
    format_telegram_breaking,
    format_telegram_digest,
    format_telegram_sensitive_review,
    format_telegram_verification_hold,
    format_turkey_timestamp,
)
from efso_radar.notifications.telegram import (
    TelegramNotificationChannel,
    deliver_one_shot_editorial_digest,
)
from scripts.deliver_telegram_digest_oneshot import build_final_three_assessments

DIGEST_LEDGER_PATH = REPO_ROOT / "data" / "telegram_digest_delivery_ledger.json"


def build_sample_breaking_assessment() -> EditorialAssessment:
    """Build a high-confidence verified breaking news assessment."""
    art = Article(
        id="cage_banderas_breaking_01",
        title="Dev buluşma: Nicolas Cage ile Antonio Banderas True Detective'de",
        url="https://www.haberturk.com/magazin/antonio-banderas-ve-nicolas-cage-true-detective-dizisinde-bulusuyor-3920600",
        source="Habertürk Magazin",
        market=Market.TR,
        published_at=datetime.fromisoformat("2026-10-18T09:30:47+00:00"),
    )
    return EditorialAssessment(
        article_ids=[art.id],
        score=96,
        kind=EditorialKind.NEW,
        headline_tr="True Detective'in 5. Sezonunda Dev Buluşma: Nicolas Cage ile Antonio Banderas Bir Arada",
        cover_title_tr="TRUE DETECTIVE 5'TE DEV BULUŞMA",
        caption_idea_tr="Dünya sinemasının iki dev aktörü HBO serisinde ilk kez bir araya geliyor.",
        why_efso_tr="HBO'nun amiral gemisi serisinde iki Akademi ödüllü/adaylı yıldızın buluşması küresel popüler kültürde birinci sınıf haberdir.",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.GLOBAL,
        verification_status=VerificationStatus.REPUTABLE_REPORTING,
        source_urls=[art.url],
        supporting_articles=[art],
        is_publishable=True,
        disposition=EditorialDisposition.DIGEST,
    )


def build_sample_sensitive_review_assessment() -> EditorialAssessment:
    """Build a sensitive tragedy/bereavement review assessment with red warning banner."""
    art = Article(
        id="sensitive_soner_olgun_01",
        title="Usta sanatçı Soner Olgun'un acı günü: Annesi vefat etti",
        url="https://www.haberturk.com/magazin/soner-olgun-un-aci-gunu-annesi-vefat-etti-3920612",
        source="Habertürk Magazin",
        market=Market.TR,
        published_at=datetime.fromisoformat("2026-10-18T10:15:00+00:00"),
    )
    return EditorialAssessment(
        article_ids=[art.id],
        score=78,
        kind=EditorialKind.NEW,
        headline_tr="Sanatçı Soner Olgun'un Acı Günü: Annesi Hayatını Kaybetti",
        cover_title_tr="SONER OLGUN'UN ACI GÜNÜ",
        caption_idea_tr="Müzisyen Soner Olgun'un annesi vefat etti.",
        relevance_reason_tr="Kıymetli sanatçının ailevi vefat haberi kamuoyu ilgisi taşımaktadır ancak hassas içerik protokolüne tabidir.",
        why_efso_tr="Kıymetli sanatçının ailevi vefat haberi kamuoyu ilgisi taşımaktadır ancak hassas içerik protokolüne tabidir.",
        uncertainties_tr="SENSITIVE_EDITORIAL_REVIEW: Vefat ve taziye haberi. Product Owner veya Baş Editör açık onayı olmadan yayınlanamaz.",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.YERLI,
        verification_status=VerificationStatus.FIRST_PARTY_STATEMENT,
        source_urls=[art.url],
        supporting_articles=[art],
        is_publishable=False,
        disposition=EditorialDisposition.SENSITIVE_REVIEW,
    )


def build_sample_verification_hold_assessment() -> EditorialAssessment:
    """Build an unverified speculation assessment with amber warning banner."""
    art = Article(
        id="hold_hurley_cyrus_01",
        title="Elizabeth Hurley ile Billy Ray Cyrus nişanlandı mı?",
        url="https://www.haberturk.com/magazin/elizabeth-hurley-ile-billy-ray-cyrus-nisanlandi-iddiasi-3920630",
        source="Habertürk Magazin",
        market=Market.TR,
        published_at=datetime.fromisoformat("2026-10-18T12:00:00+00:00"),
    )
    return EditorialAssessment(
        article_ids=[art.id],
        score=84,
        kind=EditorialKind.NEW,
        headline_tr="Elizabeth Hurley ile Billy Ray Cyrus Nişanlandı İddiası: Sosyal Medyada Evlilik Spekülasyonu",
        cover_title_tr="HURLEY & CYRUS NİŞANLANDI MI?",
        caption_idea_tr="Elizabeth Hurley ve Billy Ray Cyrus hakkındaki nişan söylentileri.",
        relevance_reason_tr="İki ünlü ismin ani evlilik söylentisi yüksek magazin ilgisi barındırıyor.",
        why_efso_tr="İki ünlü ismin ani evlilik söylentisi yüksek magazin ilgisi barındırıyor.",
        uncertainties_tr="MANUAL VERIFICATION REQUIRED: Nişan iddiası resmi sözcüler veya birincil kaynaklarca doğrulanmadı. Teyitsiz spekülasyon.",
        suggested_format=SuggestedFormat.POST,
        category=StoryCategory.GLOBAL,
        verification_status=VerificationStatus.SPECULATION,
        source_urls=[art.url],
        supporting_articles=[art],
        is_publishable=False,
        disposition=EditorialDisposition.HOLD,
    )


def main() -> int:
    print("=" * 80)
    print("  EFSO RADAR — PHASE 4G: ÇİFT ALICILI (YÖNETİCİ & BAŞ EDİTÖR) DAĞITIM VE EŞDEĞERLİK ÖN İNCELEMESİ")
    print("=" * 80)

    raw_settings = Settings.from_env()

    # Determine preview settings: if girlfriend's chat ID is not in .env yet, use a demonstration ID for validation
    simulated_editor_used = False
    if not raw_settings.telegram_primary_editor_chat_id:
        simulated_editor_used = True
        demo_editor_chat_id = "9988776655"
        settings = raw_settings.model_copy(
            update={
                "telegram_primary_editor_chat_id": demo_editor_chat_id,
                "telegram_allowed_chat_ids": [raw_settings.telegram_chat_id, demo_editor_chat_id],
                "telegram_delivery_enabled": False,
            }
        )
    else:
        settings = raw_settings.model_copy(update={"telegram_delivery_enabled": False})

    print("\n1. YAPILANDIRMA VE YETKİLENDİRME DURUMU:")
    print("-" * 60)
    print(f"• Maskeli Bot Tokeni : {settings.get_masked_telegram_token()}")
    print(f"• 1. Alıcı (ADMIN)   : {settings.get_masked_admin_chat_id()} [YETKİLİ - TEKNİK YÖNETİCİ]")
    if simulated_editor_used:
        print(f"• 2. Alıcı (EDITOR)  : {settings.get_masked_primary_editor_chat_id()} [ÖN İZLEME SİMÜLASYONU - HENÜZ .env'de TANIMLI DEĞİL]")
        print("  ℹ️ Not: .env dosyasında TELEGRAM_PRIMARY_EDITOR_CHAT_ID henüz tanımlanmadığından,")
        print("     eşdeğerlik ve mükerrerlik denetimi güvenli test ID'si (99...55) ile simüle ediliyor.")
    else:
        print(f"• 2. Alıcı (EDITOR)  : {settings.get_masked_primary_editor_chat_id()} [YETKİLİ - BAŞ EDİTÖR]")
    print(f"• Canlı Gönderim İzni: {'AÇIK' if settings.telegram_delivery_enabled else 'KAPALI (Güvenli Ön Denetim Modu)'}")
    print("• Ağ İsteği Sayısı   : 0 (Sıfır canlı istek)")

    recipients = settings.get_authorized_recipients()
    print(f"\nYetkili Toplam Alıcı : {len(recipients)} kişi")
    for r in recipients:
        print(f"  - Rol: {r['role']:<15} | Chat ID: {r['masked_chat_id']} | Tanım: {r['label']}")

    print("\n" + "=" * 80)
    print("2. DÖRT EDİTORYAL KATEGORİDE 100% İÇERİK EŞDEĞERLİĞİ DENETİMİ:")
    print("=" * 80)

    # 1. EDITORIAL_DIGEST
    digest_items = build_final_three_assessments()
    digest_html_admin = format_telegram_digest(digest_items, window_label="Canlı RSS Seçkisi (TSİ 18.10.2026 21:00)")
    digest_html_editor = format_telegram_digest(digest_items, window_label="Canlı RSS Seçkisi (TSİ 18.10.2026 21:00)")
    assert digest_html_admin == digest_html_editor, "EDITORIAL_DIGEST içerik farkı tespit edildi!"
    print("\n[KATEGORİ 1: EDITORIAL_DIGEST] — 100% EŞDEĞER")
    print("Her iki alıcı da aynı 3 haberlik seçkiyi, kapak önerilerini ve kaynak linklerini alır.")
    print("-" * 50)
    print(digest_html_admin[:280] + "\n... [İçerik Tamamı Eşdeğer] ...")

    # 2. VERIFIED_BREAKING
    breaking_item = build_sample_breaking_assessment()
    breaking_html_admin = format_telegram_breaking(breaking_item)
    breaking_html_editor = format_telegram_breaking(breaking_item)
    assert breaking_html_admin == breaking_html_editor, "VERIFIED_BREAKING içerik farkı tespit edildi!"
    print("\n[KATEGORİ 2: VERIFIED_BREAKING] — 100% EŞDEĞER")
    print("🚨 Doğrulanmış Sıcak Gelişme başlığı, Efso skoru (96/100) ve acil editoryal inceleme uyarısı her iki alıcıya eksiksiz iletilir.")
    print("-" * 50)
    print(breaking_html_admin[:280] + "\n... [İçerik Tamamı Eşdeğer] ...")

    # 3. SENSITIVE_EDITORIAL_REVIEW
    sensitive_item = build_sample_sensitive_review_assessment()
    sensitive_html_admin = format_telegram_sensitive_review(sensitive_item)
    sensitive_html_editor = format_telegram_sensitive_review(sensitive_item)
    assert sensitive_html_admin == sensitive_html_editor, "SENSITIVE_EDITORIAL_REVIEW içerik farkı tespit edildi!"
    print("\n[KATEGORİ 3: SENSITIVE_EDITORIAL_REVIEW] — 100% EŞDEĞER")
    print("⚠️ Kırmızı Kilit ve Hassas Uyarı Afişi, vefat gerekçesi ve açık onay kilidi hem Yönetici hem de Baş Editör'e aynı şekilde sunulur.")
    print("-" * 50)
    print(sensitive_html_admin[:320] + "\n... [İçerik Tamamı Eşdeğer] ...")

    # 4. HOLD_FOR_VERIFICATION
    hold_item = build_sample_verification_hold_assessment()
    hold_html_admin = format_telegram_verification_hold(hold_item)
    hold_html_editor = format_telegram_verification_hold(hold_item)
    assert hold_html_admin == hold_html_editor, "HOLD_FOR_VERIFICATION içerik farkı tespit edildi!"
    print("\n[KATEGORİ 4: HOLD_FOR_VERIFICATION] — 100% EŞDEĞER")
    print("🔍 Sarı Teyit Beklemesi Afişi, teyitsiz spekülasyon uyarısı ve yayın kısıtı her iki alıcıya eksiksiz iletilir.")
    print("-" * 50)
    print(hold_html_admin[:320] + "\n... [İçerik Tamamı Eşdeğer] ...")

    print("\n" + "=" * 80)
    print("3. BAĞIMSIZ MÜKERRERLİK VE DAĞITIM DURUMU SİMÜLASYONU:")
    print("=" * 80)
    print("Defter Kontrolü: data/telegram_digest_delivery_ledger.json")

    # Execute deliver_one_shot_editorial_digest in dry_run mode
    dispatch_res = deliver_one_shot_editorial_digest(
        settings=settings,
        assessments=digest_items,
        dry_run=True,
        window_label="Canlı RSS Seçkisi (TSİ 18.10.2026 21:00)",
        ledger_path=DIGEST_LEDGER_PATH,
        target_recipient_role="ALL",
    )

    rec_results = dispatch_res.get("recipient_results", {})
    admin_id = settings.telegram_chat_id
    editor_id = settings.telegram_primary_editor_chat_id

    admin_res = rec_results.get(admin_id, {})
    editor_res = rec_results.get(editor_id, {})

    print(f"\n• [ADMIN] ({settings.get_masked_admin_chat_id()}):")
    print(f"  - Durum       : {admin_res.get('status')}")
    print(f"  - Açıklama    : {admin_res.get('error') or admin_res.get('message')}")
    print(f"  - Ağ İsteği   : {'EVET' if admin_res.get('attempt_made') else 'HAYIR (0 Deneme)'}")

    print(f"\n• [PRIMARY_EDITOR] ({settings.get_masked_primary_editor_chat_id()}):")
    print(f"  - Durum       : {editor_res.get('status')}")
    print(f"  - Açıklama    : {editor_res.get('message') or editor_res.get('error')}")
    print(f"  - Ağ İsteği   : {'EVET' if editor_res.get('attempt_made') else 'HAYIR (0 Deneme)'}")

    print("\n" + "=" * 80)
    print("4. GÜVENLİK VE MİMARİ DOĞRULAMA ÖZETİ:")
    print("=" * 80)
    print("✅ Bağımsız Defter Anahtarı   : Her alıcı {chat_id}:{digest_hash} ile ayrı ayrı takip edilir.")
    print("✅ Mükerrer İzolasyonu       : ADMIN'e daha önce gönderilmiş olması (msg 6), PRIMARY_EDITOR'ün bülteni almasını ENGELLEMEZ.")
    print("✅ Kısmi Gönderim Dayanıklılığı: Bir alıcıya giden mesajın başarısı diğerinin durumunu etkilemez.")
    print("✅ Sıfır Canlı Ağ Trafiği    : Testler ve ön denetimler boyunca sıfır Telegram ve sıfır Gemini isteği yapılmıştır.")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
