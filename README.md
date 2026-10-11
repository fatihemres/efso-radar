# 💗 Efso Radar — 30 dakikalık magazin haber radarı

@efsomagazin için yerli ve global popüler kültür gündemini tarar, Gemini'ye seçici bir
editör gibi ön değerlendirme yaptırır ve **yalnızca önemli haberleri** Telegram'dan
bildirir. Haberi paylaşmaz, Instagram hesabına giriş yapmaz.

## İşleyiş

1. Google News RSS üzerinden TR + global haberleri tarar (11 arama).
2. Önceki taramalarda görülen linkleri çıkarır.
3. Gemini önemli ve Efso'ya uygun başlıkları seçer; benzer olayları birleştirir.
4. Önceki bildirimleri hatırlar, **önemli bir gelişme olduğunda** `UPDATE` gönderir.
5. Sonuç yoksa **mesaj göndermez**.
6. Telegram'da başlık, kısa caption önerisi, içerik formatı, kaynak ve puan sunar.

### Önemli sınırlar

- RSS, Instagram/TikTok paylaşımlarının tamamını tarayamaz. Viral bir olay habere dönüşmeden
  yakalanmayabilir. İleride izinli/yasal veri sağlayıcıları bağlanabilir.
- Model yalnızca başlık/özet/kaynak metadata'sını görür; iddiaları bağımsız olarak
  doğrulamaz. **Yayınlamadan önce mutlaka asıl kaynağı kontrol et.**
- GitHub Actions taraması **tam dakikasında garanti edilmez**; zaman zaman gecikebilir veya
  bir koşu atlanabilir.
- Bu paket tek başına açıldığında arka planda çalışmaz. GitHub ve API bilgileriyle
  kurulması gerekir.
- Gemini API, Google AI Pro/Ultra aboneliğinden farklı ücretlendirme ve limitlere sahip
  olabilir. Kullanımı ve maliyeti AI Studio / Cloud üzerinden kontrol et.

## Kurulum (GitHub Actions + Telegram)

**1. GitHub'da özel (private) yeni repo oluştur:** örneğin `efso-radar`. Bu paketteki
*dosyaların tamamını*, `.github/workflows/radar.yml` dahil yükle. Default branch `main`
olsun; workflow bu branch üzerinde bulunmalı.

**2. Telegram botunu hazırla:** Telegram'da `@BotFather` ile `/newbot` kullan.
Bot token'ını **kimseyle paylaşma**. Oluşan bota `/start` mesajı gönder. Sonra tarayıcıda
`https://api.telegram.org/bot<TOKEN>/getUpdates` adresine bakıp mesajın `chat.id`
değerini öğren; token'ı sayfadaki yer tutucuyla değiştir, gördüğün token'ı kimseyle
paylaşma. Grup kullanıyorsan botu gruba eklemeli ve gerekirse chat ID'sini grup
mesajından almalısın.

**3. Gemini API key oluştur:** [Google AI Studio](https://aistudio.google.com/apikey).
Model default `gemini-2.5-flash`. API key kullanım koşullarını, kota ve fiyatları kontrol et.

**4. GitHub repo → Settings → Secrets and variables → Actions → New repository secret**:

| Secret | Değer |
|---|---|
| `GEMINI_API_KEY` | Google AI Studio anahtarı |
| `TELEGRAM_BOT_TOKEN` | Telegram BotFather token'ı |
| `TELEGRAM_CHAT_ID` | Telegram sohbet/chat ID |

**5. GitHub repo → Settings → Actions → General → Workflow permissions:**
`Read and write permissions` etkin olmalı (veya varsayılan politika yazmaya izin vermeli);
workflow zaten `contents: write` ister. Böylece eski bildirimler `data/state.json`
dosyasına kalıcı olarak kaydedilir.

**6. İlk test:** `Actions → Efso Magazin Radar → Run workflow`. GitHub'daki logu incele.
Kayda değer haber varsa Telegram'a mesaj gelir, yoksa gelmez.

**7. Zamanlama:** `17,47 6-20 * * *` UTC = Türkiye saati her gün
**09:17, 09:47, ... 23:17, 23:47**. İstersen dosyadaki cron'u değiştir.
Gün sonrasında önemli gelişmeler bir sonraki sabah hâlâ son 16 saatin içindeyse yeniden
aday olabilir.

> Not: GitHub Actions'ın `schedule` etkinliği bazen gecikir/atlanır. 30 dakikayı
> kesin garanti eden altyapı istenirse sürekli çalışan bir VPS/cron tercih et.
> Halka açık depolarda uzun süre etkinlik olmazsa zamanlanmış workflow kapanabilir;
> private repo ve arada durum commitleriyle de platform koşullarını izlemek gerekir.

## Lokal kullanım

Python 3.11+:

```bash
pip install -r requirements.txt
export GEMINI_API_KEY='...'
export TELEGRAM_BOT_TOKEN='...'
export TELEGRAM_CHAT_ID='...'
python src/radar.py --demo       # uydurma örnek, API/Telegram kullanılmaz
python src/radar.py --dry-run    # gerçek RSS + Gemini, bildirim göndermez
python src/radar.py              # gerçek tarama; seçilirse Telegram gönderir
python -m unittest discover -s tests -v
```

## Yapılandırma

| Parametre | Varsayılan | Açıklama |
|---|---|---|
| `MIN_SCORE` | 82 | Daha yüksek = daha az bildirim |
| `MAX_PER_RUN` | 3 | Tarama başına azami mesaj |
| `MAX_AGE_HOURS` | 16 | Ne kadar eski haberlere bakılacağı |
| `MAX_CANDIDATES` | 55 | Gemini'ye gönderilecek azami yeni haber |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Model değiştirilebilir |

### Güvenlik

- `.env` ve secret'ları GitHub'a commit etme; secret'ları README/issue/log'a yazma.
- Bot yetkilerini yalnızca gerekli sohbetle sınırlı tut.
- RSS başlıkları güvenilmeyen veri kabul edilir; Gemini promptu kaynak başlıklarından
  komut alma riskini azaltır ama mutlak güvenlik garantisi değildir.
- Başlık ve caption AI tarafından üretilmiş **taslaktır**. Özellikle iddia/ölüm/sağlık/
  mahremiyet/ilişki haberlerini orijinal kaynakla doğrula.

