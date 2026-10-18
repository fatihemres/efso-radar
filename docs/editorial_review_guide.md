# Efso Radar — Editoryal Kalibrasyon ve İnceleme Rehberi

Bu rehber, **@efsomagazin** (~15K takipçi) yayınının editoryal standartlarını, marka sesini ve içerik önceliklerini Efso Radar yapay zeka modeline öğretmek (kalibre etmek) amacıyla hazırlanmıştır.

`data/calibration_dataset_review.csv` dosyasında yer alan 50 adet gerçek, canlı kaynaklardan toplanmış aday haberi değerlendirirken bu rehberdeki kriterleri referans alabilirsiniz.

---

## 1. Değerlendirme Amacı

Efso Radar’ın amacı **"her eğlence haberini özetlemek" değildir.**

Temel misyonumuz:
> **Deneyimli bir Efso Magazin editörünün gerçekten Instagram sayfasına taşıyacağı, takipçilerin etkileşime gireceği, arkadaşlarına DM ile göndereceği o nadir ve kaliteli 3-5 hikayeyi yakalamaktır.**

Sessizlik, düşük kaliteli veya alakasız bildirim göndermekten her zaman evladır. Bu çalışma yapay zekanın "gürültü" ile "gerçek Efso içeriği" arasındaki farkı anlamasını sağlayacaktır.

---

## 2. Karar Seçenekleri (`owner_decision`)

CSV dosyasındaki her haber için `owner_decision` sütununa aşağıdaki 5 değerden **tam olarak birini** yazınız:

| Karar Kodu | Anlamı ve Kriteri | Örnek |
| :--- | :--- | :--- |
| **`APPROVE`** | **Yayınlanmaya Değer:** Efso Magazin tonuna, kitlesine ve görsel diline tam uygun. Takipçilerde merak, eğlence veya tartışma yaratacak güçlü popüler kültür içeriği. | *Lvbel C5'e edebiyat cezası, Çağatay Ulusoy'un köy ziyareti, Harry Styles'ın sokak dansı videosu.* |
| **`REJECT`** | **Red / Alakasız / Uygunsuz:** Efso'nun ilgi alanı dışındaki konular (sert siyaset, spor maç rehberleri, din, genel cinayet/kaza) veya markaya yakışmayan tık tuzakları. | *Papa ameliyatı, Amerikan futbolu yayın rehberi, iklim değişikliği raporu, hayvan saldırısı.* |
| **`TOO_WEAK`** | **Çok Zayıf / Rutin Dolgu:** Eğlence veya magazin kategorisinde olsa da paylaşmaya değmeyecek kadar sıradan, sıkıcı, zamansız veya rutin PR bülteni. | *Genel saç kesim modelleri, sıradan dizi set arkası bülteni, nostaljik arşiv fotoğrafları.* |
| **`DUPLICATE`** | **Mükerrer / Aynı Olay:** Listede daha önce yer alan veya sayfada zaten paylaşılmış bir olayın başka bir kaynak tarafından tekrar edilmesi. | *Aynı köy ziyaretinin farklı gazetelerdeki kopyası (örn. Sabah vs Habertürk).* |
| **`IMPORTANT_UPDATE`** | **Kritik Güncelleme:** Daha önce paylaşılan veya bilinen bir gündem hikayesinde hikayenin seyrini değiştiren resmi ifade, adli karar veya yeni teyitli gelişme. | *Trajik bir vefatın ardından eşin/ailenin ilk resmi açıklaması veya adli tahliye kararı.* |

---

## 3. Efso Magazin Editoryal Filtreleri

Haberleri değerlendirirken şu 4 ana ekseni göz önünde bulundurunuz:

### A. Hedef Kitle ve Popüler Kültür Nabzı (18-35 Yaş Türk Gençliği)
- Takipçiler bu olayı Instagram Keşfet'te, Twitter/X'te veya TikTok'ta konuşuyor mu ya da konuşur mu?
- Olaydaki figür (sanatçı, rapçi, dizi oyuncusu, küresel ikon) hedef kitlemizin tanıdığı ve duygusal/mizahi bağ kurduğu biri mi?

### B. Görsel Hikaye Anlatımı ve Format Potansiyeli
- **Hikaye (Story):** Hızlı tüketilen, anket/soru çıkartması konabilecek, anlık reaksiyon çekecek durumlar.
- **Akış Postu (Feed Carousel / Single):** Güçlü fotoğraf, dikkat çekici alıntı veya estetik kırmızı halı/konser anları.
- **Reels:** Viral sokak videosu, canlı yayın gafları, şov anları.
- *Eğer haber sadece metinden ibaretse ve güçlü bir görsel/video unsuru yoksa veya görsel üretilemiyorsa değer kaybeder.*

### C. Doğruluk ve Güvenilirlik Eşiği
- Birinci ağızdan röportajlar, resmi basın bültenleri ve saygın ajans teyitleri önceliklidir.
- Dedikodular, "iddia edildi" veya "sosyal medyada öne sürüldü" kalıplı haberler temkinle karşılanmalıdır. Asılsız karalama veya komplo teorileri (örn. kara büyü, sahte sağlık iddiaları) kesinlikle **`REJECT`** edilmelidir.
- Ölüm ve kaza gibi son derece hassas olaylarda saygılı ve dengeli bir dil şarttır.

### D. Tık Tuzağı ve Sektörel B2B Filtresi
- "Estetiksiz halini görenler şaştı kaldı" benzeri içi boş nostalji içerikleri `TOO_WEAK` olarak etiketlenmelidir.
- Yapım şirketleri arasındaki dağıtım anlaşmaları, gişe ticaret anlaşmaları (B2B) genç kitle için sıkıcıdır; elenmelidir.

---

## 4. Öncelik Puanı ve Eşik Analizi (`enters_ai_cutoff_yn`)

CSV dosyasında iki teknik sütun göreceksiniz:
- `discovery_priority`: Sistemin kural bazlı belirlediği öncelik puanı (1.00 ile 13.00 arası).
- `enters_ai_cutoff_yn`: Haberin ilk 55 aday içerisine girip yapay zekaya sunulup sunulmadığı (`Y` = Girdi, `N` = Backlog'da kaldı).

### Önemli Gözlem ve Göreviniz:
- Sistemimiz kaynak çeşitliliğini korumak ve haber kirliliğini önlemek adına tek seferde en fazla **55 adayı** yapay zeka değerlendirmesine alır.
- Ancak kural bazlı önceliklendirme bazı durumlarda (örneğin Türkçe anahtar kelime içermeyen uluslararası viral haberler) çok popüler bir haberi **backlog'a (N)** itebilmektedir.
- **Örnek:** Harry Styles'ın New York sokaklarında tek başına dans ettiği viral video (Rank 160, Puan 4.00, Cutoff: N) veya Shane Gillis'in canlı yayın küfür özrü (Rank 58, Puan 6.30, Cutoff: N).
- Bu nedenle `enters_ai_cutoff_yn = N` olan haberleri dikkatle inceleyiniz. Eğer "Bu haber kesinlikle kaçırılmamalıydı, ilk 55'e girmeliydi" dediğiniz bir haber varsa, bunu `APPROVE` seçerek `owner_notes` sütununda belirtiniz. Bu geri bildirim, önceliklendirme algoritmamızı iyileştirmek için kullanılacaktır.

---

## 5. Notlar Sütunu (`owner_notes`) Nasıl Doldurulmalı?

Zorunlu olmamakla birlikte, özellikle sınırda kaldığınız veya algoritmanın iyileştirilmesini istediğiniz durumlarda kısa gerekçeler yazmanız çok değerlidir:
- *Örnek:* "Story'de anket açılır, çok etkileşim alır."
- *Örnek:* "Çok niş bir sinema haberi, Türkiye'de karşılığı yok."
- *Örnek:* "Başlık yanıltıcı, içerik bomboş tık tuzağı."
- *Örnek:* "Backlog'da kalması büyük hata, Harry Styles Türk gençliğinde çok trend."
- *Örnek:* "Önceki haberle aynı gün içinde verildi, mükerrer."

---

## 6. Sonraki Adım

CSV dosyasındaki 50 satırı doldurduktan sonra dosyayı kaydedip bildirmeniz yeterlidir. Bu etiketler, Faz 3C-2'de yapay zekanın editorial karar motorunu prompt ve kural seviyesinde kalibre etmek için altın referans veri seti (ground truth) olarak kullanılacaktır.
