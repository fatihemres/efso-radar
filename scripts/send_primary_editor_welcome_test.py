"""
Phase 4G: Dedicated Primary Editor Telegram Connectivity Test Script for Efso Radar.

Authorized live test constraints:
1. Destination: Strictly PRIMARY_EDITOR only (ADMIN is completely excluded).
2. Content: Warm welcome and category introduction message (not an editorial digest).
3. Attempt count: Exactly ONE sendMessage request, ZERO automatic retries.
4. Ambiguity handling: Timeouts marked as DELIVERY_UNCERTAIN without retry.
5. Idempotency: Ledger-backed duplicate suppression prevents duplicate welcome sends.
6. Privacy: Masks all Chat IDs and tokens in logs and console output.
7. Persistence: Leaves .env TELEGRAM_DELIVERY_ENABLED=false untouched.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

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
from efso_radar.notifications.formatters import format_turkey_timestamp
from efso_radar.notifications.telegram import sanitize_telegram_text

LEDGER_PATH = REPO_ROOT / "data" / "telegram_digest_delivery_ledger.json"

WELCOME_TEST_STABLE_ID = "welcome_test_primary_editor_v1"
WELCOME_MESSAGE_HTML = (
    "🐱 <b>EFSO RADAR'A HOŞ GELDİN!</b>\n\n"
    "💖 <b>Merhaba Baş Editör!</b>\n\n"
    "Magazin dünyasındaki gelişmeleri senin için takip etmeye hazırlanıyorum.\n\n"
    "📰 <b>Editoryal haber bültenleri</b>\n"
    "🚨 <b>Önemli gelişmeler</b>\n"
    "🔍 <b>Doğrulama bekleyen haberler</b>\n"
    "❤️ <b>Hassas editoryal incelemeler</b>\n\n"
    "🧪 <i>Bu mesaj tek seferlik bir bağlantı testidir. Otomatik bildirimler henüz etkin değildir.</i>"
)


def compute_welcome_test_hash(chat_id: str) -> str:
    """Compute deterministic hash for welcome test delivery ledger."""
    raw = f"{WELCOME_TEST_STABLE_ID}:{chat_id.strip()}:{WELCOME_MESSAGE_HTML}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


def execute_primary_editor_welcome_test(
    settings: Settings,
    dry_run: bool = True,
    ledger_path: Path = LEDGER_PATH,
    transport: Optional[httpx.BaseTransport] = None,
    client: Optional[httpx.Client] = None,
) -> dict[str, Any]:
    """Execute preflight and single-shot welcome message delivery to PRIMARY_EDITOR only."""
    admin_chat = settings.telegram_chat_id.strip() if settings.telegram_chat_id else ""
    editor_chat = settings.telegram_primary_editor_chat_id.strip() if settings.telegram_primary_editor_chat_id else ""

    token_masked = settings.get_masked_telegram_token()
    admin_masked = settings.get_masked_admin_chat_id()
    editor_masked = settings.get_masked_primary_editor_chat_id()

    # --- Preflight Safety Audits ---
    if not settings.telegram_bot_token:
        return {
            "status": "CONFIG_ERROR",
            "error": "TELEGRAM_BOT_TOKEN is not configured.",
            "attempt_made": False,
        }

    if not editor_chat:
        return {
            "status": "CONFIG_ERROR",
            "error": "TELEGRAM_PRIMARY_EDITOR_CHAT_ID is not configured in .env. STOP before network send.",
            "attempt_made": False,
        }

    if editor_chat.startswith("-"):
        return {
            "status": "INVALID_CHAT_TYPE",
            "error": f"PRIMARY_EDITOR chat ID '{editor_masked}' starts with '-'. Group/channel destinations are prohibited.",
            "attempt_made": False,
        }

    if admin_chat and editor_chat == admin_chat:
        return {
            "status": "CONFIG_ERROR",
            "error": f"PRIMARY_EDITOR ({editor_masked}) and ADMIN ({admin_masked}) cannot be identical for this dedicated test.",
            "attempt_made": False,
        }

    if not settings.is_chat_id_allowed(editor_chat):
        return {
            "status": "UNAUTHORIZED_RECIPIENT",
            "error": f"PRIMARY_EDITOR '{editor_masked}' is not in configured allowlist.",
            "attempt_made": False,
        }

    test_hash = compute_welcome_test_hash(editor_chat)
    ledger_key = f"{editor_chat}:{test_hash}"

    # Load ledger
    ledger_data: dict[str, Any] = {"deliveries": {}}
    if ledger_path.exists():
        try:
            with open(ledger_path, "r", encoding="utf-8") as f:
                ledger_data = json.load(f)
        except Exception as e:
            logger_err = sanitize_telegram_text(str(e), settings.telegram_bot_token)
            return {
                "status": "LEDGER_ERROR",
                "error": f"Failed to read delivery ledger: {logger_err}",
                "attempt_made": False,
            }

    deliveries = ledger_data.get("deliveries", {})

    # Check duplicate prevention
    if ledger_key in deliveries:
        rec = deliveries[ledger_key]
        return {
            "status": "DUPLICATE_DELIVERY_PREVENTED",
            "error": f"Welcome test already delivered or attempted for PRIMARY_EDITOR on {rec.get('delivered_at_tsi') or rec.get('delivered_at_utc')}.",
            "message_id": rec.get("message_id"),
            "attempt_made": False,
            "attempts_count": 0,
            "masked_primary_editor": editor_masked,
            "masked_admin": admin_masked,
            "admin_excluded": True,
            "target_role": "PRIMARY_EDITOR",
            "digest_hash": test_hash,
        }

    # Verify existing Admin historical records remain intact
    admin_legacy_records = [
        k for k, v in deliveries.items()
        if (isinstance(v, dict) and v.get("recipient_role") == "ADMIN") or k in ("06afb36bc22dff56263e72de", "7f2ff61c50b9cb66aebf4ecb")
    ]

    # Offline Dry-Run Check
    if dry_run or not settings.telegram_delivery_enabled:
        return {
            "status": "SAFE_PREVIEW_PASSED",
            "message": (
                f"Preflight audit PASSED for PRIMARY_EDITOR ({editor_masked}). "
                "ADMIN is completely excluded. Zero network calls made."
            ),
            "attempt_made": False,
            "attempts_count": 0,
            "masked_primary_editor": editor_masked,
            "masked_admin": admin_masked,
            "admin_excluded": True,
            "digest_hash": test_hash,
            "rendered_html": WELCOME_MESSAGE_HTML,
            "admin_historical_records_count": len(admin_legacy_records),
        }

    # --- Live Single-Shot Dispatch ---
    endpoint = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": editor_chat,
        "text": WELCOME_MESSAGE_HTML,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    def _save_record(rec_status: str, msg_id: Optional[int] = None, err_desc: Optional[str] = None):
        now_dt = datetime.now(timezone.utc)
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        rec = {
            "status": rec_status,
            "delivered_at_utc": now_dt.isoformat(),
            "delivered_at_tsi": format_turkey_timestamp(now_dt),
            "chat_id": editor_chat,
            "target_chat_id": editor_masked,
            "recipient_role": "PRIMARY_EDITOR",
            "digest_hash": test_hash,
            "stable_digest_id": WELCOME_TEST_STABLE_ID,
            "digest_type": "PRIMARY_EDITOR_WELCOME_TEST",
        }
        if msg_id:
            rec["message_id"] = msg_id
        if err_desc:
            rec["error"] = err_desc
        ledger_data.setdefault("deliveries", {})[ledger_key] = rec
        tmp_path = ledger_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f_tmp:
            json.dump(ledger_data, f_tmp, ensure_ascii=False, indent=2)
        tmp_path.replace(ledger_path)

    should_close_client = client is None
    http_client = client or httpx.Client(
        transport=transport,
        timeout=settings.http_timeout_seconds,
    )

    try:
        resp = http_client.post(endpoint, json=payload)
        if resp.status_code == 200:
            resp_json = resp.json()
            if resp_json.get("ok"):
                msg_id = resp_json.get("result", {}).get("message_id")
                _save_record("DELIVERY_SUCCESS", msg_id=msg_id)
                return {
                    "status": "DELIVERY_SUCCESS",
                    "message": f"Welcome test message successfully delivered to PRIMARY_EDITOR ({editor_masked}).",
                    "message_id": msg_id,
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_primary_editor": editor_masked,
                    "masked_admin": admin_masked,
                    "admin_excluded": True,
                    "digest_hash": test_hash,
                    "api_status_code": 200,
                }
            else:
                err_msg = sanitize_telegram_text(f"Telegram API response not ok: {resp_json}", settings.telegram_bot_token)
                _save_record("DELIVERY_FAILED", err_desc=err_msg)
                return {
                    "status": "DELIVERY_FAILED",
                    "error": err_msg,
                    "attempt_made": True,
                    "attempts_count": 1,
                    "masked_primary_editor": editor_masked,
                    "admin_excluded": True,
                    "digest_hash": test_hash,
                    "api_status_code": 200,
                }
        elif resp.status_code == 504:
            err_msg = "Ambiguous gateway timeout (HTTP 504). Zero automatic retries."
            _save_record("DELIVERY_UNCERTAIN", err_desc=err_msg)
            return {
                "status": "DELIVERY_UNCERTAIN",
                "error": err_msg,
                "attempt_made": True,
                "attempts_count": 1,
                "masked_primary_editor": editor_masked,
                "admin_excluded": True,
                "digest_hash": test_hash,
                "api_status_code": 504,
            }
        else:
            err_msg = sanitize_telegram_text(f"Telegram error (HTTP {resp.status_code}): {resp.text[:200]}", settings.telegram_bot_token)
            _save_record("DELIVERY_FAILED", err_desc=err_msg)
            return {
                "status": "DELIVERY_FAILED",
                "error": err_msg,
                "attempt_made": True,
                "attempts_count": 1,
                "masked_primary_editor": editor_masked,
                "admin_excluded": True,
                "digest_hash": test_hash,
                "api_status_code": resp.status_code,
            }
    except (httpx.TimeoutException, httpx.ReadTimeout) as exc:
        err_msg = sanitize_telegram_text(f"Network timeout: {exc}", settings.telegram_bot_token)
        _save_record("DELIVERY_UNCERTAIN", err_desc=err_msg)
        return {
            "status": "DELIVERY_UNCERTAIN",
            "error": f"Ambiguous timeout: {err_msg}. Message may have been accepted; automatic retry skipped.",
            "attempt_made": True,
            "attempts_count": 1,
            "masked_primary_editor": editor_masked,
            "admin_excluded": True,
            "digest_hash": test_hash,
        }
    except Exception as exc:
        err_msg = sanitize_telegram_text(f"Transport/Dispatch error: {exc}", settings.telegram_bot_token)
        _save_record("DELIVERY_FAILED", err_desc=err_msg)
        return {
            "status": "DELIVERY_FAILED",
            "error": err_msg,
            "attempt_made": True,
            "attempts_count": 1,
            "masked_primary_editor": editor_masked,
            "admin_excluded": True,
            "digest_hash": test_hash,
        }
    finally:
        if should_close_client:
            try:
                http_client.close()
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dedicated primary editor Telegram connectivity test workflow (Phase 4G)."
    )
    parser.add_argument(
        "--live",
        dest="execute_live",
        action="store_true",
        default=False,
        help="Execute exactly ONE live connectivity test dispatch to PRIMARY_EDITOR.",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Perform offline preflight audit without network dispatch.",
    )
    args = parser.parse_args()

    settings = Settings.from_env()

    print("=" * 70)
    print("  EFSO RADAR — BAŞ EDİTÖR (PRIMARY_EDITOR) TELEGRAM BAĞLANTI TESTİ")
    print("=" * 70)
    print(f"• Maskeli Bot Tokeni : {settings.get_masked_telegram_token()}")
    print(f"• Hedef Alıcı        : PRIMARY_EDITOR ({settings.get_masked_primary_editor_chat_id()})")
    print(f"• Hariç Tutulan      : ADMIN ({settings.get_masked_admin_chat_id()}) [MESAJ GÖNDERİLMEZ]")
    print(f"• İzin Listesi Adedi : {len(settings.telegram_allowed_chat_ids)}")
    print(f"• Canlı Gönderim İzni: {'AÇIK (--live)' if args.execute_live else 'KAPALI (Güvenli Ön Denetim)'}")
    print("• RSS Taraması       : DEVRE DIŞI (0 çağrı)")
    print("• Gemini AI Çıkarımı : DEVRE DIŞI (0 çağrı)")
    print("• Defter Takibi      : data/telegram_digest_delivery_ledger.json")
    print("-" * 70)

    is_dry_run = args.dry_run or (not args.execute_live)

    # Temporary process-level activation only if --live explicitly requested
    active_settings = settings
    if args.execute_live:
        active_settings = settings.model_copy(update={"telegram_delivery_enabled": True})

    result = execute_primary_editor_welcome_test(
        settings=active_settings,
        dry_run=is_dry_run,
        ledger_path=LEDGER_PATH,
    )

    status = result.get("status")
    print("\n--- ÇALIŞTIRMA SONUCU ---")
    print(f"Durum Kodu           : {status}")
    print(f"Hedef                : PRIMARY_EDITOR ({result.get('masked_primary_editor', '***')})")
    print(f"Admin Hariç Tutuldu mu: {'EVET (Yöneticiye mesaj iletilmedi)' if result.get('admin_excluded') else 'HAYIR'}")
    print(f"Ağ İsteği Yapıldı mı : {'EVET' if result.get('attempt_made') else 'HAYIR (0 Deneme)'}")
    print(f"Toplam Deneme Sayısı : {result.get('attempts_count', 0)}")
    print(f"Test İçerik Hash     : {result.get('digest_hash')}")

    if status == "DELIVERY_SUCCESS":
        print(f"\n[BAŞARILI] {result.get('message')}")
        print(f"Telegram Message ID  : {result.get('message_id')}")
        print("Müstakil kayıt deftere yazıldı. Yönetici teslimat kayıtları aynen korundu.")
        return 0

    if status == "SAFE_PREVIEW_PASSED":
        print(f"\n[ÖN DENETİM BAŞARILI] {result.get('message')}")
        print("\nRender Edilecek Mesaj Metni:")
        print("-" * 50)
        print(result.get("rendered_html", ""))
        print("-" * 50)
        print("\nCanlı gönderim için Product Owner yetkili komutu:")
        print("  python scripts/send_primary_editor_welcome_test.py --live")
        return 0

    if status == "DUPLICATE_DELIVERY_PREVENTED":
        print(f"\n[MÜKERRER KORUMA ENGELİ] {result.get('error')}")
        return 1

    if status == "DELIVERY_UNCERTAIN":
        print(f"\n[BELİRSİZ DURUM] {result.get('error')}")
        print("Ağ veya sunucu zaman aşımı yaşandı. Otomatik yeniden deneme yapılmadı.")
        return 1

    print(f"\n[HATA] {result.get('error')}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
