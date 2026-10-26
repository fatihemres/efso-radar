# 💗 Efso Radar — Popüler Kültür & Magazin Haber Radarı

@efsomagazin için yerli ve global popüler kültür gündemini takip eden, Google News RSS üzerinden haberleri toplayan, kural tabanlı ve yapay zekâ destekli editoryal ön eleme sunan ve **yalnızca seçici ve doğrulanabilir gelişmeleri** Telegram üzerinden bildiren editoryal radar sistemi.

> **ÖNEMLİ GÜVENLİK VE MİMARİ BİLGİLENDİRMESİ:**
> - **Herkese Açık Kod / Public Depo (Tüm Hakları Saklıdır):** Bu depo GitHub üzerinde genel erişime açıktır (`fatihemres/efso-radar`). Hiçbir özel anahtar, bot token'ı veya özel chat ID depoda yer almaz.
> - **Üretim Otomasyonu Kapalıdır:** GitHub Actions zamanlanmış cron iş akışları güvenlik nedeniyle **tamamen devre dışıdır**. Arka planda çalışan canlı veya gözetimsiz bir üretim servisi bulunmamaktadır.
> - **Git Commit ile Durum Saklanmaz:** Sistem durumu Git commitleri ile depoya yazılmaz. Durum yönetimi Schema v6 (15 tablo) ile yerel SQLite ve Turso libSQL bulut veritabanı altyapısıyla sağlanır.
> - **Varsayılan Güvenlik Korumaları:** Yapay zekâ değerlendirmesi (`AI_EVALUATION_ENABLED=False`) ve Telegram bildirim iletimi (`TELEGRAM_DELIVERY_ENABLED=False`) varsayılan olarak **kapalıdır**.
> - **Harcama Politikası:** Kesin 0 TRY / $0.00 operasyonel harcama kuralı geçerlidir.
> - **Instagram Entegrasyonu:** Instagram otomatik paylaşımı kesinlikle kapalıdır; sistem yalnızca Telegram üzerinden editöre taslak öneri sunar.
> - **Üretim Aktivasyonu:** Canlı sistem geçişi ve zamanlanmış iş akışlarının etkinleştirilmesi Ürün Sahibi'nin (Product Owner) ayrı ve açık onayına tabidir.

> **Lisans notu:** Bu deponun sahibine ait özgün çalışmalar için tüm haklar saklıdır ve yeniden kullanım izni verilmemektedir. Ayrıntılar için [LICENSE](LICENSE) dosyasına bakın. Üçüncü taraf haber verileri ve bağımlılıkların hakları ayrıca saklıdır.

---

## 👥 Alıcı Politikası (Recipient Policy)

19 Ekim 2026 tarihli Ürün Sahibi kararı uyarınca bildirim rolleri kesin sınırlarla ayrılmıştır:
- **PRIMARY_EDITOR (Baş Editör):** Efso Radar'ın ana editoryal kullanıcısıdır. Tüm editoryal bildirimleri (`EDITORIAL_DIGEST`, `VERIFIED_BREAKING`, `SENSITIVE_EDITORIAL_REVIEW`, `HOLD_FOR_VERIFICATION`) özel Telegram kanalından alır.
- **ADMIN (Yönetici / Ürün Sahibi):** Günlük rutin editoryal bültenlerle meşgul edilmez. Yalnızca sistemin operasyonel durumunu, tarama metriklerini ve harcama durumunu özetleyen **günde en fazla 1 adet Operasyonel Sağlık Raporu** alır.
- **Editoryal Ayrışma:** Rutin editoryal haberler ADMIN'e asla iletilmez; ADMIN sağlık raporları PRIMARY_EDITOR'e asla gönderilmez.

---

## ⏱️ Zamanlama ve Çalışma Modeli (Design Baseline)

Gelecekteki operasyonel zamanlama (Europe/Istanbul — TSİ / UTC+3):
1. **RSS Keşif Taramaları (SCAN_ONLY):** 00:17, 03:17, 06:17, 09:17, 12:17, 15:17, 18:17, 21:17 (3 saatte bir, günde 8 kez).
2. **Konsolide Editoryal Bülten (EDITORIAL_DIGEST):** 09:30, 15:30, 21:30 (Sakin saatler dışında, günde 3 kez).
3. **Yönetici Sağlık Raporu (ADMIN_HEALTH_REPORT):** 22:00 (Günde en fazla 1 kez).
4. **Şifreli Yedekleme (ENCRYPTED_BACKUP):** 02:15 (Tasarım aşamasında / Design Only).
5. **Sakin Saatler (Quiet Hours):** 00:00–08:30 TSİ (Rutin editoryal iletim engellenir).

---

## 🛠️ Çalışma Modları ve CLI Kullanımı

| Mod / Komut | Ağ Erişimi (RSS) | Yapay Zekâ (Gemini) | Telegram İletimi | Veritabanı Etkisi | Amaç |
| :--- | :---: | :---: | :---: | :---: | :--- |
| `python src/radar.py --demo` | ❌ Yok | ❌ Yok | ❌ Yok | ❌ Yok | Sentetik örnek kart çıktısını gösterir. |
| `python src/radar.py --legacy --dry-run` | ❌ Yok | ❌ Yok | ❌ Yok | ❌ Yok | Deterministik çevrimdışı fikstür önizlemesi. |
| `python -m efso_radar.workflow --preview` | ❌ Yok | ❌ Yok | ❌ Yok | ❌ Geçici DB | Zamanlama ve iş akışı sözleşmelerini çevrimdışı test eder. |
| `python src/radar.py --scan-only --dry-run` | 🌐 RSS Var | ❌ Yok | ❌ Yok | ❌ Geçici DB | İzole geçici veritabanında RSS keşfi simülasyonu. |
| `python src/radar.py --dry-run` | 🌐 RSS Var | 🌐 API Var* | ❌ Yok | ❌ Geçici DB | İzole geçici veritabanında AI değerlendirme simülasyonu. |
| `python src/radar.py --scan-only` | 🌐 RSS Var | ❌ Yok | ❌ Yok | 💾 Kalıcı DB | RSS keşfi ve aday havuzu güncellemesi (0 Gemini, 0 Telegram). |
| `python src/radar.py` (Canlı) | 🌐 RSS Var | 🌐 API Var* | 📱 Telegram* | 💾 Kalıcı DB | Tam canlı pipeline (*Ayrı onay ve ayar gerektirir). |

*\* Not: `AI_EVALUATION_ENABLED` ve `TELEGRAM_DELIVERY_ENABLED` ortam değişkenleri `true` yapılmadıkça canlı Gemini çağrısı ve Telegram iletimi gerçekleşmez; sistem güvenlik nedeniyle fail-closed kapanır.*

### Çevrimdışı İş Akışı Önizleme CLI

Phase 5B.3D kapsamında eklenen orkestrasyon CLI aracı ile tüm iş akışları çevrimdışı test edilebilir:

```bash
# Zamanlama tablosunu ve geçerli UTC zamanı için uygunluğu göster:
python -m efso_radar.workflow --verify-schedule

# Belirli bir UTC zaman damgası için görev uygunluğunu ve önizlemesini çalıştır:
python -m efso_radar.workflow --task editorial-digest --timestamp 2026-10-24T06:30:00Z

# Sakin saatler kontrolü (04:00 TSİ = 01:00 UTC):
python -m efso_radar.workflow --task editorial-digest --timestamp 2026-10-24T01:00:00Z
```

---

## 🔒 Güvenlik İlkeleri

1. **Katı 0 TRY / $0.00 Harcama:** Operasyonel AI para limitleri artık varsayılan olarak `0.00`; gerçek Gemini API çağrıları, sağlayıcı Free Tier/faturalandırma güvenliği bağımsız doğrulanıp ayrıca PO tarafından yetkilendirilene kadar **kod seviyesinde engellenir** (Issue #21 adayı). Paid-tier fiyat tahminleri gerçek ücretsiz katman ücretleri değildir. Uygulama bütçesi veya Google spend cap tek başına sıfır fatura garantisi vermez. Eski pozitif `.env` bütçeleri yüklemede hata verir; testlerdeki pozitif değerler yalnızca hermetik tarihsel fiyat/ledger senaryolarına aittir.
2. **Fail-Closed Tasarım:** Eksik kimlik bilgisi, zaman aşımı veya belirsiz teslimat durumunda sistem beklemeye geçer; mükerrer gönderim yapmaz.
3. **Uçucu Runner İzolasyonu:** GitHub Actions üzerinde çalışan işler geçicidir; kalıcı durum Turso/SQLite üzerinde korunur.
4. **Token ve Kimlik Maskeleme:** Loglarda veya konsol çıktılarında gizli anahtarlar ve sohbet kimlikleri asla açık metin olarak yer almaz.

---

## 📋 Product & Engineering Yönetimi

GitHub kanonik proje kaydıdır; ChatGPT Project karar alma/çalışma alanıdır. Güncel belge bağlantıları:

- [AI ajanları ve iş yetkisi — AGENTS.md](AGENTS.md)
- [Son doğrulanmış durum — PROJECT_STATUS.md](PROJECT_STATUS.md)
- [PO kararları ve açık kararlar — DECISIONS.md](DECISIONS.md)
- [Milestone / Roadmap — ROADMAP.md](ROADMAP.md)
- [Test, CI ve doğrulama kanıtları — VERIFICATION.md](VERIFICATION.md)
- [Mimari — docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [Git / PR / kod inceleme yönetişimi — docs/ENGINEERING_GOVERNANCE.md](docs/ENGINEERING_GOVERNANCE.md)
- [Sürüm ve release ilkeleri — docs/RELEASE_POLICY.md](docs/RELEASE_POLICY.md)
- [Ürün fikirleri — docs/IDEA_REGISTER.md](docs/IDEA_REGISTER.md)
- [Kontrollü Turso staging test planı — Phase 5B.3G](docs/PHASE5B3G_LIVE_STAGING_RELIABILITY_PLAN.md)

Önemli: Dokümandaki planlar veya geçmiş canlı pilotlar, bugün Turso erişimi, üretim/cron etkinleştirmesi, Gemini/Telegram çağrısı ya da release izni vermez. Bunlar ayrı ve açık PO onayına bağlıdır. GitHub PR/CI kanıtları ile tarihlendirilmiş durum kayıtlarını birbirinden ayırın.

---

## 💻 Geliştirici kurulumu ve güvenli test

Desteklenen CI Python sürümleri: **3.11 ve 3.12**. Aşağıdaki komutlar Windows PowerShell içindir; geliştirme ortamı `C:\Dev\EfsoRadar` olarak bilinir ancak kişisel yolun değişebilir.

```powershell
git clone https://github.com/fatihemres/efso-radar.git
cd efso-radar
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
$env:PYTHONPATH = 'src'
.\.venv\Scripts\python.exe -m pytest tests -v
```

Geliştirme ortamında `python -m efso_radar.workflow --verify-schedule` ve `python -W error -m efso_radar.staging.runner --help` yalnızca komut/sözleşme denetimi içindir. **Dikkat:** README'deki bazı `--dry-run` komutları bile RSS veya Gemini ağına erişebilir. Harici bağlantı ve API çağrıları mevcut işletim kısıtları uyarınca ayrı PO izni gerektirir. Testler için hermetik fikstür ve mock HTTP kullanın. Gerçek `.env`, `data/` ve kurtarma manifestlerini okumayın/değiştirmeyin.

**Proje dizinleri:** `src/efso_radar/` uygulama modülleri; `src/radar.py` CLI; `tests/` otomatik testler; `scripts/` yardımcı komutlar (çalıştırmadan önce yan etkilerini inceleyin); `.github/workflows/` CI ve kapalı üretim şablonları; `docs/` mimari ve faz kanıtları; `data/` operasyonel/deneysel kayıt alanı (korunan içerik). İlgili yönetim ve teknik kaynak bağlantıları yukarıdaki bölümde.
