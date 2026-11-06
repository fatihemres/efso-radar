# Efso Radar — Mimari ve Tasarım Dokümantasyonu (Faz 1 & Faz 1.1)

## Güncel mimari harita (26 Ekim 2026, main: d1660653)

Bu bölüm güncel dizin yapısına dayanan **üst düzey bir haritadır**; aşağıdaki erken Faz 1/2 anlatımı tarihsel tasarım bağlamı olarak korunmuştur. Ayrıntılı ve sonraki faza özgü kayıtları yeniden kopyalamak yerine onlara bağlantı verir.

```text
RSS / Google News / çoklu yayıncı adaptörleri (sources/)
            |
      kaynak doğrulama ve deduplication/
            |
          domain.py + ai/ değerlendirme sözleşmeleri
            |
           orchestrator.py
            |
 StateRepository (repositories/base.py)
     +-------------------------+
     | SQLite/WAL              | Turso/libSQL Hrana v2 |
     | repositories/sqlite.py  | repositories/turso.py |
     +-------------------------+
            |
 notifications/ (PRIMARY_EDITOR editoryal, ADMIN yalnız sağlık)
            |
          Telegram (gerçek gönderim KAPALI)

workflow/  -> çevrimdışı zamanlama/iş akışı önizlemesi
backup/    -> AES-256-GCM yedek, doğrulanmış geri yükleme sözleşmeleri
staging/   -> Turso bağlantı/preflight, kurtarma, offline hermetic runner
```

**Sınırlar:** `src/radar.py` CLI girişidir; `config.py` güvenlik/bütçe/çalışma ayarları, `domain.py` olay ve editoryal veri modelleri, `repositories/schema.py` Schema v6 (15 tablo) temelidir. Bulut staging için kodun mevcut olması canlı cloud validation'ın veya üretim aktivasyonunun tamamlandığını göstermez. Önce hermetik test, sonra bağımsız PO gate esastır.

**Derin teknik kayıtlar:** [Cloud Safety](PHASE5B3_CLOUD_SAFETY_ARCHITECTURE.md), [persistence parity](PHASE5B3C_CLOUD_PERSISTENCE_INTEGRATION.md), [offline orchestration](PHASE5B3D_OFFLINE_WORKFLOW_ORCHESTRATION.md), [Turso readiness](PHASE5B3E_TURSO_STAGING_READINESS.md), [hermetic reliability](PHASE5B3F_HERMETIC_RELIABILITY_VALIDATION.md), [live staging plan (authorization NOT granted)](PHASE5B3G_LIVE_STAGING_RELIABILITY_PLAN.md), [encrypted backup](PHASE5B2_BACKUP_ARCHITECTURE.md), [roadmap](../ROADMAP.md). Yaşam döngüsü ve sınırlar için [Engineering Governance](ENGINEERING_GOVERNANCE.md).

---


## 1. Genel Bakış ve Modül Sınırları

Efso Radar, katmanlı ve pragmatik bir **Clean / Ports & Adapters** mimarisine sahiptir. Temel iş mantığı harici altyapılardan (veritabanı sürücüsü, bulut sağlayıcısı, bildirim kanalı) tamamen soyutlanmıştır.

### Modül Hiyerarşisi

```
src/efso_radar/
├── config.py                 # Tip güvenli konfigürasyon (Settings, Bütçe, Sessiz Saatler)
├── domain.py                 # Çekirdek modeller (Article, Story, StoryUpdate, Assessment, Notification)
├── repositories/             # Kalıcılık arayüzü ve yerel SQLite adaptörü
│   ├── base.py               # StateRepository (Protocol/ABC)
│   └── sqlite.py             # WAL modu, eşzamanlılık kilidi (run_lock), geriye uyumlu JSON migrasyonu
├── sources/                  # Haber kaynağı arayüzü ve Google News RSS adaptörü
│   ├── base.py               # NewsSource (Protocol/ABC)
│   └── google_news.py        # Hata yalıtımlı RSS çekici ve XML ayrıştırıcı
├── deduplication/            # Çok kademeli tekilleştirme ve kümeleme
│   └── engine.py             # URL hash kontrolü + Kapsama (containment) ve Türkçe kök analizi
├── ai/                       # Gemini LLM entegrasyonu, Structured Outputs ve doğrulama
│   ├── client.py             # GeminiEditorialEngine (Pydantic şema + iş kuralı doğrulama)
│   ├── cost.py               # Resmî token fiyatlandırması ve çağrı başı maliyet tahmini
│   ├── prompts.py            # Enjeksiyon korumalı prompt şablonu (untrusted payload isolation)
│   └── schemas.py            # Pydantic Structured Outputs şemaları
├── notifications/            # Bildirim dağıtım katmanı
│   ├── base.py               # NotificationChannel (Protocol/ABC)
│   ├── formatters.py         # Güvenli HTML karakter escaping ve mesaj şablonu
│   └── telegram.py           # Sessiz saat kontrolü (HOLD/DISCARD) ve Telegram istemcisi
└── orchestrator.py           # Pipeline orkestratörü (Ingest -> Dedup -> AI -> Notify -> Persist)
```

---

## 2. Temel Tasarım Kararları

### 2.1. Alan Modellerinin Ayrıştırılması (Domain Concepts)
* `Article`: Bir yayıncıdan gelen ham haber birimi (URL, kaynak, başlık, yayınlanma zamanı).
* `Story`: Gerçek dünyadaki bir olayı temsil eden küme. Birden fazla yayıncının haberi tek bir `Story`ye bağlanabilir.
* `StoryUpdate`: Daha önce bildirilmiş bir `Story` için gerçekleşen somut, yeni gelişme.
* `EditorialAssessment`: Yapay zekâ tarafından önerilen ve iş kurallarıyla onaylanan editoryal karar.
* `NotificationRecord`: Gönderim kanalı, zamanı, durumu (`SENT`, `HELD_QUIET_HOURS`, `DISPATCHING`, `FAILED`) ve hata kaydı.

### 2.2. Eşzamanlılık ve İdempotens (Concurrency & Idempotency)
* **Run Lock ve Lease Renewal (Heartbeat):** `SqliteStateRepository` içinde 15 dakikalık TTL'e sahip `run_locks` tablosu bulunur. Çakışan veya üst üste binen GitHub Actions / Cron koşuları birbirini ezmez; kilitli olan koşu sessizce çekilir. Uzun süren AI üretimlerinde çalışan worker `renew_run_lock` ile kilidini uzatır.
* **Bildirim İdempotensi:** `has_story_been_alerted` metodu ile aynı hikayenin mükerrer bildirilmesi engellenir.

### 2.3. Gemini API Resmî Maliyet ve Bütçe Güvenliği (2026 Güncel)
Resmî Google AI Studio tarifeleri baz alınmıştır:
* **gemini-3.8-flash:** Input: $0.75 / 1M token, Output: $3.75 / 1M token (Akıl yürütme / thinking tokenları dahil).
* **gemini-3.5-flash-lite (Önerilen Çekirdek Model):** Input: $0.30 / 1M token, Output: $2.50 / 1M token.
* **gemini-3.1-flash-lite:** Input: $0.25 / 1M token, Output: $1.50 / 1M token.
* **gemini-2.5-flash:** Input: $0.30 / 1M token, Output: $2.50 / 1M token (Eski sürüm, kısıtlı erişim).
* *Not:* `gemini-2.0-flash-lite` 1 Haziran 2026 tarihinde kapatılmıştır ve otomatik olarak `gemini-3.5-flash-lite` modeline yönlendirilmektedir.
* **Google Search Grounding:** Opsiyoneldir ($35 / 1.000 sorgu) ve gereksiz maliyetleri önlemek için varsayılan olarak kapalıdır (`ENABLE_SEARCH_GROUNDING=false`).
* **Devre Kesici (Circuit Breaker):** `max_estimated_run_cost_usd` eşiğini aşan sorgularda sistem AI çağrısını iptal eder.

### 2.4. Sessiz Saatler Kuyruğu (Quiet Hours Queue & Drain)
24 saat kesintisiz izleme yapılırken, Telegram bildirimleri için sessiz saatler bağımsız yönetilir:
* `QUIET_HOURS_ENABLED=true`
* `QUIET_HOURS_START_UTC="21:00"` (TR 00:00)
* `QUIET_HOURS_END_UTC="05:30"` (TR 08:30)
* Gece saatlerinde onaylanan haberler `HELD_QUIET_HOURS` statüsüyle veritabanında kalıcı olarak kuyruğa alınır. Sabah sessiz saatler bittiğinde orkestratör kuyruktaki haberleri otomatik olarak gönderir ve mükerrer gönderimi engeller.

### 2.5. Çapraz Yayıncı Kümeleme & Kapsama Benzerliği (Containment Similarity)
Asimetrik başlıklarda (biri kısa: *"Hadise sevgilisinden ayrıldı"*, diğeri detaylı: *"Ünlü şarkıcı Hadise ile sevgilisi ayrılık kararı aldı"*) salt Jaccard benzerliği ekstra sıfatlar nedeniyle düşük kalabilir. Bu nedenle Jaccard (%40) ve Kapsama Benzerliği (%60) ağırlıklı hibrit benzerlik metriği uygulanmıştır. Aynı ünlünün farklı olayları (albüm vs ayrılık) bu sayede birbirinden kesin olarak ayrılır.

### 2.6. Windows ve Linux Uyumluluğu
* Windows konsolunda Türkçe kod sayfası (`cp1254`) kaynaklı emoji hataları `sys.stdout.reconfigure(encoding="utf-8")` ile çözülmüştür.
* Yerel SQLite motoru standart Python kütüphanesiyle çalıştığından hem Windows'ta hem de Linux CI ortamında sıfır derleme sorunuyla çalışır.

---

## 3. Faz 2: Turso (libSQL HTTP Hrana v2) & Dayanıklı Kalıcılık

### 3.1. Protokol ve Taşınabilirlik (Zero Native C-Dependencies)
* Turso entegrasyonu platform bağımlı C-kütüphaneleri veya soket kütüphaneleri yerine resmî libSQL HTTP Hrana v2 protokolü (`/v2/pipeline`) üzerinden `httpx` ile gerçekleştirilmiştir.
* Parametreli sorgular (`?`) ve tip serileştirme (`serialize_hrana_arg`) ile tam enjeksiyon güvenliği sağlanmıştır.
* `SqliteStateRepository` ve `TursoStateRepository` aynı `StateRepository` sözleşmesini uygular; çalışma zamanında tek bir ortam değişkeni (`TURSO_DATABASE_URL`) ile yerel SQLite ve bulut Turso arasında kod değişikliği gerekmeksizin geçiş yapılır.

### 3.2. Dayanıklı Bildirim Talepleri (Claim / Lease Mekanizması)
* Dış Telegram API'si ile veritabanı işlemleri tek bir dağıtık ACID işlem oluşturamaz.
* Sistem at-least-once yerine güvenli at-most-once yaklaşımı benimser:
  1. Worker göndermeden önce `claim_notification_for_dispatch` ile bildirimi `DISPATCHING` statüsüne ve süreli lease'e (örneğin 180 saniye) alır.
  2. Başarılı Telegram yanıtında statü `SENT` yapılır ve lease temizlenir.
  3. Çöken veya yanıt vermeyen worker'ların kilitleri `recover_expired_dispatch_claims` ile güvenli şekilde `FAILED` statüsüne çekilir.

### 3.3. Sessiz Saatler Tazelik Eşiği (`EXPIRED_STALE`)
* Magazin haberleri hızla eskir; gece kuyruğa alınan bir haberin sabah 8 saat sonra gönderilmesi editoryal değerini yitirmiş olabilir.
* `expire_stale_held_notifications(max_age_hours)` metodu ile yapılandırılabilir tazelik sınırını (varsayılan 8 saat) aşan bildirimler `EXPIRED_STALE` olarak işaretlenir ve sabah bildirim kuyruğundan sessizce elenir.

### 3.4. Olay Seviyesi Zekâ (Story Intelligence & Distinct Records)
Beş ayrı kavram veritabanında ayrı tablolarda ve modellerde saklanır:
1. `processed_articles`: Kaynak yayıncılardan gelen ham makaleler.
2. `stories`: Gerçek dünya popüler kültür olayları (birden çok yayıncı aynı olaya bağlanır).
3. `story_updates`: Mevcut bir hikayedeki somut, yeni gelişmeler (`kind == UPDATE`).
4. `editorial_assessments`: AI tarafından üretilen editoryal kararlar ve format önerileri.
5. `notifications`: Telegram gönderim denemeleri, lease'ler ve durumlar.
* Mevcut bir hikayeye (`parent_story_id`) yeni ve anlamlı bir gelişme geldiğinde bildirim engellenmez; yeni bir bildirim tetiklenir ve `stories.update_count` artırılır. Aynı gelişmenin mükerrer tekrarı ise idempotency kontrolüyle bastırılır.

---

## 4. Launch G3: Minimal Strict-$0 Gemini Activation Safety Contract & Integration Plan (Offline Readiness)

Bu bölüm, GitHub Issue [#88](https://github.com/fatihemres/efso-radar/issues/88) ve Üst Görev [#82](https://github.com/fatihemres/efso-radar/issues/82) (G1/G3/G6) kapsamında PO onaylı mimari yönü belgeler. Tamamen **ÇEVRİMDIŞI (OFFLINE-ONLY)** hazırlıktır; canlı Gemini çağrılarını, bulut staging bağlantılarını veya abonelik yükseltmelerini yetkilendirmez.

### 4.1. Temel Güvenlik Değişmezleri ve Kesin $0.00 / 0 TRY İlkesi
* **Sıfır Harcama:** Aylık ve koşu başı işletme harcaması kesin olarak **$0.00 / 0 TRY**'dir. Ücretli planlara otomatik yükseltme, fatura bağlantısı (`billing_enabled=True`) veya ücretli yedek sağlayıcı (paid fallback) kesinlikle yasaktır.
* **Koşulsuz Canlı İstek Bariyeri:** `require_verified_free_tier()` fonksiyonu, gerçek Gemini API uç noktasına gidecek tüm canlı çağrıları çalışma zamanında koşulsuz olarak bloke eder (`RuntimeError`). Bu koruma hiçbir ortam değişkeni, `.env` bayrağı veya API anahtarı ile devre dışı bırakılamaz.
* **Sahte Yetkilendirme Yasağı:** Çağrıcı tarafından sağlanan sözlükler, JSON kayıtları, ekran görüntüleri veya istemci içi sayaçlar bağımsız sağlayıcı kanıtı sayılamaz. Sağlayıcı tarafı doğrulanmış fatura kanıtı bulunmadıkça gerçek faturalandırılan tutar (`actual_billed_usd`) daima `None` (UNKNOWN) kalır.

### 4.2. Ön Dağıtım Karar Tablosu ve Durum Makinesi (Pre-Dispatch State Machine)
Gemini çağrısı öncesinde her istek `evaluate_g3_activation_preflight()` fonksiyonu ile denetlenir. Tüm makine kontrolleri ve insan PO onayı sağlanmadıkça istek reddedilir (`allowed=False`).

| Kapı (Gate) | Denetlenen Koşul | Geçersizlik Durum Kodu | Reddin Gerekçesi (Fail-Closed) |
| :--- | :--- | :--- | :--- |
| **1. Model Adayı** | Model `gemini-3.5-flash-lite` mi? | `DENY_UNASSESSED_MODEL` | Yalnızca `gemini-3.5-flash-lite` değerlendirilmiştir (`ASSESSED_LAUNCH_MODELS`). `gemini-3.8-flash` ve diğer modeller fırlatma için değerlendirilmemiştir. |
| **2. Arama Genişletme** | `enable_search_grounding == False` mu? | `DENY_SEARCH_GROUNDING` | Google Search Grounding Free Tier kapsamında ücretsiz değildir ($35 / 1.000 sorgu). Kesin $0 kuralı gereği kapalı olmalıdır. |
| **3. Sıfır Bütçe** | `max_run_cost_usd == 0.0` ve `max_monthly_cost_usd == 0.0` mı? | `DENY_POSITIVE_BUDGET` | Pozitif dolar tavanı Free Tier ile çelişir; operasyonel tavanlar kesin $0.00 olmalıdır. |
| **4. Fatura Kanıtı** | `billing_enabled == False` ve sağlayıcı kanıtı geçerli mi? | `DENY_PAID_PROJECT` veya `DENY_UNVERIFIED_BILLING` | `billing_enabled=True` doğrudan `DISALLOWED_PAID` verir. İstemci beyanları doğrulanamaz ise durum `UNKNOWN` sayılır. |
| **5. Kota Kanıtı** | RPM, TPM, Input TPM, RPD ve Aylık Çağrı gözlemlendi mi? | `DENY_QUOTA_UNVERIFIED` veya `DENY_QUOTA_EXHAUSTED` | Kota gözlemi eksikse veya limit (`OBSERVED_PROJECT_*`) aşılmışsa derhal durulur. |
| **6. İnsan PO Onayı** | İlgili oturum için `is_po_live_go_granted == True` mu? | `DENY_NO_PO_GO` | Makine kapılarının geçmesi zorunlu bir ön koşuldur ancak ASLA kendi başına canlı çağrıyı yetkilendirmez. |
| **7. Çevrimdışı Sınır** | `require_verified_free_tier()` canlı çağrıyı bloke eder | `ALLOW_PREFLIGHT_READY` (Yalnızca Sentetik Test) | Sentetik ön hazırlık tamamlansa dahi kod tabanındaki sert bariyer canlı ağ erişimini engeller. |

### 4.3. Makine Kapıları ile İnsan Ürün Sahibi (PO) Onayının Kesin Ayrımı
* **Makine Geçişi ≠ Canlı Yetki:** Güvenlik kapılarının tümünden "PASS" almak dağıtım için **gerekli ama yetersizdir**. Hiçbir yazılım bileşeni kendi kendine canlı API izni üretemez.
* **Oturuma Özgü İnsan Onayı:** Canlı çağrı yapılabilmesi için Product Owner tarafından ilgili çalıştırma oturumuna özel, açık ve bağımsız bir "PO Live GO" yetkisi (`is_po_live_go_granted=True`) verilmiş olmalıdır.
* **Bypass Edilemezlik:** Hiçbir `.env` değeri, CLI parametresi veya test konfigürasyonu insan PO onayını simüle edemez veya atlayamaz.

### 4.4. Kota Yönetimi ve Günlük Pasifik Gece Yarısı Sıfırlaması
Google AI Studio / Gemini API kotaları proje bazlıdır (API anahtarına veya çalıştırıcıya özel değildir):
* **Gözlemlenen Proje Temel Limitleri:**
  - **RPM:** 15 İstek / Dakika (`OBSERVED_PROJECT_RPM_LIMIT`)
  - **Input TPM:** 250.000 Girdi Tokenı / Dakika (`OBSERVED_PROJECT_INPUT_TPM_LIMIT`)
  - **RPD:** 500 İstek / Gün (`OBSERVED_PROJECT_RPD_LIMIT`)
  - **Dahili Aylık Tavan:** 300 Çağrı / Ay (`MAX_MONTHLY_AI_CALLS`)
* **Kalıcı Hak Değildir:** Bu değerler Efso Radar projesinin AI Studio konsolunda gözlemlenen mevcut kota sınırlarıdır; kalıcı hesap hakkı veya evrensel sabit değildir. Google tarafından değiştirilebilir.
* **Pasifik Gece Yarısı (PT Midnight) Sıfırlaması:** Google RPD kotası her gün ABD Pasifik Saatiyle (PT) 00:00:00'da sıfırlanır:
  - Standart Saat (PST, UTC-8): Türkiye saatiyle 11:00 (08:00 UTC).
  - Yaz Saati (PDT, UTC-7): Türkiye saatiyle 10:00 (07:00 UTC).
  - Sistem, `calendar` tabanlı saf Python algoritmasıyla (`is_pacific_daylight_saving_time`) harici `tzdata` bağımlılığı olmadan Pasifik takvim gününü (`get_pacific_calendar_date`) hesaplar ve RPD kotasını bu takvime göre izler.

### 4.5. Dış İstek Sınırları ve Yük Sanitizasyonu
Canlı dağıtım düşünüldüğünde uygulanacak istek profili:
* **Model:** Yalnızca `gemini-3.5-flash-lite`.
* **Araçlar ve Akıl Yürütme:** `enable_search_grounding=False`, fonksiyon çağırma kapalı, akıl yürütme seviyesi `minimal` (`thinking_level='minimal'`).
* **Zaman Aşımı ve Deneme:** Tek deneme (`attempts=1`), sıfır kör yeniden deneme (blind-retry), 30 saniye katı zaman aşımı.
* **Veri Koruma:** İstek gövdesinde yalnızca doğrulanmış kamuya açık RSS haber metinleri yer alır; kullanıcı kimlik bilgisi, şifre, API anahtarı veya PII (Kişisel Veri) promptlara kesinlikle dahil edilmez.
* **Acil Durdurma (Kill-Switch):** `AI_EVALUATION_ENABLED=false` yapıldığında veya bütçe tavanı sıfırın üzerine çıktığında sistem derhal değerlendirmeyi durdurur.

### 4.6. Schema v6 Kalıcılık Yeteneği vs. Boşluk Matrisi (G2 / ERD-017 Bağımlılığı)
Mevcut Schema v6 tablolarının G3 gereksinimlerini karşılama durumu:

| Alt Sistem / İhtiyaç | Schema v6 Mevcut Yetenek | Kalan Boşluk / Risk | Çözüm Yolu |
| :--- | :--- | :--- | :--- |
| **Tekil Çalıştırıcı Sıralaması** | `run_locks` (15 dk lease + heartbeat) eşzamanlı çakışmaları engeller. | Yok (tekil denetimli çalıştırma için yeterli). | Schema v6 `run_locks` korunur. |
| **Belirsiz Hata Takibi** | `ai_usage_ledger` `FAILED_AMBIGUOUS` ve `RESERVED_IN_FLIGHT` durumlarını saklar. | Ağ zaman aşımında sağlayıcı durumunun kesinliği bilinemez (UNKNOWN). | İkircikli durumlarda HALT; kör yeniden deneme yok. |
| **Aylık Çağrı Sayacı** | `ai_usage_ledger` üzerinden aylık başarılı/belirsiz çağrılar toplanır. | Dağıtık runner'larda SQLite kilitlenme riski. | Tekil koşu izolasyonu. |
| **Çoklu Runner Kota Rezervasyonu** | `ai_usage_ledger` `INSERT OR REPLACE` kullanır; atomik CAS veya rolling-window desteği yoktur. | GitHub Actions geçici (ephemeral) sanal makineleri arasında 1 dakikalık RPM/TPM pencereleri paylaşılamaz. Harici projelerin kota tüketimi yerel veritabanında görülemez. | **G2 / ERD-017 Bağımlılığı:** Otomasyona geçilmeden önce Seçenek B (sağlayıcı-kalıcı sahiplik günlüğü) entegre edilmelidir. Erken Schema v7 yapılmaz. |

### 4.7. Entegrasyon ve Aktivasyon Hazırlık Sırası
Gelecekte olası bir canlı aktivasyon öncesinde tamamlanması gereken aşamalar:
1. **G1 (Fatura Doğrulama):** Google Cloud / AI Studio tarafında projenin faturalandırmaya bağlı olmadığının (`billing_enabled=False`) bağımsız, kanıtlanabilir kriptografik veya API doğrulaması.
2. **G2 (ERD-017 Kota Günlüğü):** Çoklu runner ortamında kota aşımını önleyecek atomik sahiplik ve kota rezervasyon günlüğü.
3. **G3 (Sözleşme & Preflight):** Mevcut `evaluate_g3_activation_preflight()` ve karar durum makinesinin devreye alınması (tamamlandı).
4. **G6 (Bağımsız PO GO):** Her canlı oturum için Product Owner'ın açık, yazılı ve kayıtlı canlı çalıştırma onayı.
