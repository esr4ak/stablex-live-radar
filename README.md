# Stablex Kripto Piyasa Canlı Radarı (prototip)

FastAPI + WebSocket tabanlı canlı akış prototipi. `../poller/`
(RSS+Gemini+Firestore) ile bağımsız, ayrı bir mimari denemesi.

**Kalıcılık: yerel SQLite (`radar.db`), dış bağımlılık yok.** Firestore/
Firebase gerekmiyor — proje kökünde otomatik oluşan tek bir dosya. Sunucu
yeniden başlasa bile daha önce işlenmiş haberler tekrar Gemini'ye gitmez;
bir istemci bağlanır bağlanmaz son 50 haber `{"tip": "gecmis_haberler"}`
WebSocket mesajıyla anında gönderilir (sayfa yenilendiğinde akış boş
başlamaz). `radar.db`'yi silersen geçmiş sıfırlanır, sistem bozulmaz.

- **Haberler**: GERÇEK tarama — `sources.py`'deki BEYAZ LİSTE'den (11 kaynak,
  aşağıda) 60 saniyede bir çekilir, sadece son 10 gün içindeki ve daha önce
  yayınlanmamış haberler işlenir, Gemini API'ye (`../poller/` ile aynı
  sağlayıcı) gönderilip Türkçe başlık/özet/etiket/pazarlama aksiyonu
  JSON'ına dönüştürülür, en fazla 15 saniyede bir istemcilere yayınlanır.
  Akışın üstünde coin/haber bazlı arama kutusu var (client-side, mevcut
  kartları filtreler).

  > Not: Bunu bir ara yerel LLM'e (Ollama) bağlamıştık ama bu kişisel/tek
  > kullanıcılı bir makine olduğu için Ollama'nın avantajları (gizlilik,
  > paylaşımlı kota olmaması) burada bir fayda sağlamıyor, sadece kurulum
  > yükü getiriyordu — Gemini'ye geri döndük. `GEMINI_MODEL` sabit
  > (`poller/poll.py`'deki gibi) `-latest` alias'lardan biri; bazı alias'lar
  > (`gemini-flash-latest`) çok düşük günlük kotalı (20/gün) bir modele
  > yönlenebiliyor, `gemini-flash-lite-latest` bu oturumda daha güvenilir
  > çıktı — kota hatası görürsen `main.py`'de bu değeri değiştir.
- **Fiyatlar**: GERÇEK veri — CoinGecko'dan Stablex'te listeli 61 varlığın
  TAMAMI tek istekte 90 saniyede bir çekilir (1-2 dk bandı, API kotasını
  zorlamamak için), istemciye 3 saniyede bir yayınlanır. Şeritte önce "top"
  coinler (BTC/ETH/SOL/AVAX/XRP/ADA/DOGE/LINK/DOT/LTC), ardından kalan 51
  coin alfabetik sırayla — panel kaydırılabilir.
- **Düzenleyici Kaynaklar** paneli: SPK/SEC/ESMA(MiCA) ana sayfalarına hızlı
  erişim linkleri (haber akışındaki duyurulardan ayrı, sağ sütunda sabit).

## Beyaz Liste kaynaklar

| Kaynak | Tür | Not |
|---|---|---|
| The Block | RSS | doğrulandı |
| CoinDesk | RSS | doğrulandı |
| Cointelegraph | RSS | doğrulandı |
| Decrypt | RSS | doğrulandı |
| CryptoSlate | RSS | doğrulandı |
| Reuters | Google News vekil | gerçek RSS'i yok/401 — site-scoped arama RSS'i kullanılıyor, gürültü riski RSS'e göre daha yüksek |
| Bloomberg HT | RSS | doğrulandı |
| Uzmancoin | RSS | doğrulandı |
| Foreks Haber | Google News vekil | doğrudan erişim 403 — site-scoped arama RSS'i kullanılıyor |
| SPK | BeautifulSoup (HTML) | RSS yok, resmi basın duyuruları sayfası taranıyor |
| SEC | RSS | doğrulandı |

Yeni bir kaynak eklemek/çıkarmak istersen sadece `sources.py`daki
`WHITELIST_SOURCES` listesini düzenle.

## Çalıştırma

```bash
cd live-radar
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export COINGECKO_API_KEY=...   # https://www.coingecko.com/en/developers/dashboard , ücretsiz
export GEMINI_API_KEY=...      # https://aistudio.google.com/apikey , ücretsiz

uvicorn main:app --reload
# http://127.0.0.1:8000
```

Gemini geçici olarak hata verirse (kota/yoğunluk) sistem çökmez — o haber
radar.db'ye yazılmadığı için "görülmemiş" sayılmaya devam eder ve doğrudan
kuyrukta kalıp tekrar denenir (en fazla `EMIT_MAX_RETRIES` kez); kalıcı
hata varsayılırsa vazgeçilip loglanır.

## Not: her zaman açık sunucu gerektirir

SQLite kalıcılığı çözdüğü şey "restart'ta geçmiş kaybolmasın" — ama bu
mimari hâlâ, `../poller/`'daki GitHub Actions cron modelinden farklı olarak
**sürekli çalışan bir process** ister (WebSocket bağlantısını canlı tutmak
için). Ücretsiz kalıcı barındırma seçenekleri var (Render/Railway/Fly.io
free tier) ama çoğu belirli bir süre trafik gelmeyince uyur — bu da "canlı"
sürekliliği bozar; ayrıca SQLite dosyası genelde bu tür free tier'larda
kalıcı disk olmadığı için deploy'lar arasında sıfırlanabilir (yerel
makinede bu sorun yok). Firestore + GitHub Actions kurgusu bu sorunları
yaşamaz çünkü hiç sürekli açık sunucuya ihtiyaç duymaz.
