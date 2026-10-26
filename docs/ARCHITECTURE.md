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
