# Stablex Canlı Radar — Beyaz Liste kaynak konfigürasyonu.
#
# Asılsız/güvenilmez piyasa haberlerinden korunmak için sistem SADECE burada
# tanımlı kaynaklardan veri çeker. Yeni bir kaynak eklemek istersen buraya
# bir satır eklemen yeterli — main.py bu listeyi olduğu gibi kullanır.
#
# type:
#   "rss"          — kaynağın kendi gerçek RSS'i (temiz sinyal, doğrulandı).
#   "google_news"  — kaynağın gerçek RSS'i yok ya da bot korumasından direkt
#                     erişim engelleniyor (Reuters 401, Foreks 403); Google
#                     News'in ücretsiz arama RSS'i üzerinden site-scoped
#                     vekil bir akış. Gürültü riski RSS'e göre daha yüksek.
#   "spk_html"     — SPK'nın RSS'i yok, resmi duyuru sayfası BeautifulSoup
#                     ile taranıyor (özel parser main.py'de fetch_spk()).
WHITELIST_SOURCES = [
    # --- Küresel ---
    {"name": "The Block", "type": "rss", "url": "https://www.theblock.co/rss.xml", "kategori": "Küresel"},
    {"name": "CoinDesk", "type": "rss", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/", "kategori": "Küresel"},
    {"name": "Cointelegraph", "type": "rss", "url": "https://cointelegraph.com/rss", "kategori": "Küresel"},
    {"name": "Decrypt", "type": "rss", "url": "https://decrypt.co/feed", "kategori": "Küresel"},
    {"name": "CryptoSlate", "type": "rss", "url": "https://cryptoslate.com/feed/", "kategori": "Küresel"},
    {"name": "Reuters", "type": "google_news", "query": "site:reuters.com finance markets", "kategori": "Küresel"},
    # --- Yerel (Türkiye) ---
    {"name": "Bloomberg HT", "type": "rss", "url": "https://www.bloomberght.com/rss", "kategori": "Yerel"},
    {"name": "Uzmancoin", "type": "rss", "url": "https://uzmancoin.com/feed/", "kategori": "Yerel"},
    {"name": "Foreks Haber", "type": "google_news", "query": "Foreks piyasa haberleri", "kategori": "Yerel"},
    # --- Regülasyon kurumları ---
    {"name": "SPK", "type": "spk_html", "kategori": "Regülasyon"},
    {"name": "SEC", "type": "rss", "url": "https://www.sec.gov/news/pressreleases.rss", "kategori": "Regülasyon"},
]

FETCH_INTERVAL_SECONDS = 60  # kaynakları tarama sıklığı (siteleri yormamak için)
ENTRIES_PER_SOURCE = 8  # her taramada kaynak başına en fazla kaç girdiye bakılacak
FRESHNESS_WINDOW_DAYS = 10  # bundan eski haberler asla işlenmez (backlog seli engellenir)

# Haber akışında haberin kendisi dışında, pazarlama ekibinin hızlıca
# erişebileceği düzenleyici kurum referansları (frontend'de statik olarak
# gösterilir — SPK/SEC/ESMA duyuru sayfaları zaten haber olarak akıyor ama
# kurumların ana sayfalarına hızlı erişim akışta yer almıyordu).
REGULATORY_LINKS = [
    {"name": "SPK", "url": "https://spk.gov.tr/duyurular"},
    {"name": "SEC", "url": "https://www.sec.gov/newsroom/press-releases"},
    {"name": "ESMA (MiCA)", "url": "https://www.esma.europa.eu/esmas-activities/digital-finance-and-innovation/markets-crypto-assets-regulation-mica"},
]

# Stablex'te listeli 61 varlık — Gemini'nin ürettiği "ilgili_varliklar" bu
# listeyle kesiştirilip filtrelenir.
STABLEX_COINS = [
    "AAVE", "ADA", "ALGO", "ANKR", "APE", "ARB", "ATOM", "AUDIO", "AVAX", "AXS",
    "BAT", "BONK", "BTC", "CHZ", "COMP", "CRV", "DOGE", "DOT", "EIGEN", "ENS",
    "ETHFI", "ETH", "FET", "FLOKI", "GALA", "GRT", "IMX", "IO", "JASMY", "JTO",
    "JUP", "LDO", "LINK", "LPT", "LTC", "MANA", "ONDO", "PAXG", "PENDLE", "PENGU",
    "PEPE", "POL", "PYTH", "RENDER", "SAND", "SHIB", "SOL", "STORJ", "STRK", "STX",
    "TRX", "UNI", "USDT", "WIF", "WLD", "W", "XLM", "XRP", "XTZ", "ZRO", "ZRX",
]

# Stablex sembolü -> CoinGecko coin id (canlı fiyat şeridi için gerekli).
STABLEX_COIN_IDS = {
    "AAVE": "aave", "ADA": "cardano", "ALGO": "algorand", "ANKR": "ankr",
    "APE": "apecoin", "ARB": "arbitrum", "ATOM": "cosmos", "AUDIO": "audius",
    "AVAX": "avalanche-2", "AXS": "axie-infinity", "BAT": "basic-attention-token",
    "BONK": "bonk", "BTC": "bitcoin", "CHZ": "chiliz",
    "COMP": "compound-governance-token", "CRV": "curve-dao-token", "DOGE": "dogecoin",
    "DOT": "polkadot", "EIGEN": "eigenlayer", "ENS": "ethereum-name-service",
    "ETHFI": "ether-fi", "ETH": "ethereum", "FET": "fetch-ai", "FLOKI": "floki",
    "GALA": "gala", "GRT": "the-graph", "IMX": "immutable-x", "IO": "io",
    "JASMY": "jasmycoin", "JTO": "jito-governance-token",
    "JUP": "jupiter-exchange-solana", "LDO": "lido-dao", "LINK": "chainlink",
    "LPT": "livepeer", "LTC": "litecoin", "MANA": "decentraland",
    "ONDO": "ondo-finance", "PAXG": "pax-gold", "PENDLE": "pendle",
    "PENGU": "pudgy-penguins", "PEPE": "pepe", "POL": "polygon-ecosystem-token",
    "PYTH": "pyth-network", "RENDER": "render-token", "SAND": "the-sandbox",
    "SHIB": "shiba-inu", "SOL": "solana", "STORJ": "storj", "STRK": "starknet",
    "STX": "blockstack", "TRX": "tron", "UNI": "uniswap", "USDT": "tether",
    "WIF": "dogwifcoin", "WLD": "worldcoin-wld", "W": "wormhole", "XLM": "stellar",
    "XRP": "ripple", "XTZ": "tezos", "ZRO": "layerzero", "ZRX": "0x",
}

# Şeritte en üstte sabit gösterilecek "top" coinler — geri kalan 51 coin
# bunların ardından alfabetik sırayla akar (bkz. main.py price_stream_loop).
TOP_COIN_SYMBOLS = ["BTC", "ETH", "SOL", "AVAX", "XRP", "ADA", "DOGE", "LINK", "DOT", "LTC"]

PRICE_FETCH_INTERVAL_SECONDS = 90  # CoinGecko'ya gerçek istek sıklığı (1-2 dk bandı)
PRICE_STREAM_INTERVAL_SECONDS = 3  # istemciye yayın sıklığı (UI ritmi, cache'ten okunur)

# Rakip borsa coin listeleri günde birkaç kez bile değişmez — 6 saatte bir
# tarama, hem Paribu'yu gereksiz yormaz hem de "yeni listelendi" sinyalini
# makul bir gecikmeyle yakalar.
COMPETITOR_CHECK_INTERVAL_SECONDS = 6 * 60 * 60

CHANNELS = {
    "push": "Push Bildirimi",
    "email": "E-posta Bülteni",
    "blog": "Blog İçeriği",
    "sosyal": "Sosyal Medya",
}

# Rakip Kapsam Farkı — "rakipte var, Stablex'te yok" coin tespiti.
#
# type:
#   "api"    — herkese açık, kimlik doğrulamasız bir uç nokta var (Paribu),
#              tam otomatik taranır (bkz. main.py fetch_paribu_coins()).
#   "manual" — herkese açık/güvenilir bir API bulunamayan rakipler için
#              (ör. Cloudflare bot korumalı siteler); coin listesi elle
#              güncellenir. Şu an tanımlı rakip yok — Midas Kripto
#              kapsam dışı bırakıldı (bkz. proje notları: Cloudflare
#              korumasını kırmak maliyet/hukuki risk açısından
#              gerekçelendirilmedi).
COMPETITOR_SOURCES = [
    {"name": "Paribu", "type": "api", "url": "https://www.paribu.com/ticker"},
]

# X Gönderi Akışı + Keşif — X (Twitter) üzerinden Grok API (xAI) ile.
# Reddit denenmişti ama self-servis API kaydı 2026'da platform seviyesinde
# kapatıldığı için (bkz. proje notları) tamamen X'e geçildi. Grok'un
# x_search aracı bizim yerimize X'i arayıp özetliyor.
#
# Gönderi Akışı ZAMANLANMIŞ bir döngü DEĞİL — kullanıcı bir coin
# seçtiğinde talep üzerine main.py:/api/x-feed üzerinden tetiklenir (bkz.
# main.py notu: saatlik/toplu bir tasarım denenmişti ama "kaynak linki
# zorunlu" kuralı büyük batch'lerde dakikalarca asılı kalmaya sebep oldu,
# tek-coin talep-üzerine modeline geçildi).
XAI_MODEL = "grok-4.3"  # grok-4.6'dan ~%50-60 daha ucuz (2026-08), aynı ailede x_search destekli
                         # olması beklenir; model isimleri değişebilir, güncel liste console.x.ai'de

# X Keşif — günlük, AYRI bir sorgu: (a) STABLEX_COINS'te olmayan ama X'te
# trend olan ticker'ları bulur, (b) rakip borsa(lar) hakkındaki X
# sentiment'ini özetler. Saatlik sentiment taramasından bağımsız çünkü
# açık uçlu keşif sorgusu muhtemelen daha çok arama gerektirir — günlük
# yeterli, yeni coin/rakip sinyali saatlik olacak kadar acil değil.
X_DISCOVERY_INTERVAL_SECONDS = 24 * 60 * 60
X_DISCOVERY_COMPETITOR_NAMES = ["Paribu"]  # COMPETITOR_SOURCES'taki isimlerle tutarlı tutulmalı

# On-Chain Olaylar — Whale Alert kullanılmadı (ücretsiz tier yok, $29.95/ay'lık
# planı "personal use only" lisanslı — Stablex bir şirket olduğu için uygun
# değil). Bunun yerine DefiLlama'nın tamamen ücretsiz, auth gerektirmeyen
# stablecoin arz API'siyle USDT'nin küresel dolaşımdaki arzındaki (mint/burn)
# anlamlı değişimler bir on-chain sinyali olarak izleniyor.
ONCHAIN_STABLECOIN_SYMBOL = "USDT"
ONCHAIN_CHECK_INTERVAL_SECONDS = 2 * 60 * 60  # 2 saat — arz verisi günlük bazda anlamlı değişir
ONCHAIN_CHANGE_THRESHOLD_PCT = 0.5  # bu yüzdenin altındaki 24s değişim "olay" sayılmaz, gürültüdür

# Zincir TVL (Total Value Locked) genişletmesi — DefiLlama'nın kendi "chain"
# adlandırması -> Stablex'te listeli ilgili L1/L2 native coin sembolü.
# TVL, borsalara giren/çıkan genel likiditenin dolaylı bir göstergesi:
# TVL artışı o ekosistemde büyüyen aktiviteye, düşüşü daralan aktiviteye
# işaret eder (tekil bir "balina transferi" değil, daha geniş bir sinyal).
ONCHAIN_TRACKED_CHAINS = {
    "Ethereum": "ETH",
    "Solana": "SOL",
    "Avalanche": "AVAX",
    "Arbitrum": "ARB",
    "Polygon": "POL",
}
ONCHAIN_TVL_CHANGE_THRESHOLD_PCT = 3.0  # TVL, stablecoin arzından daha oynak — eşik daha yüksek tutuldu

# Piyasa Nabzı — Kriz Anı Sentezi. Frontend'deki breadth bar'ı (index.html)
# ile AYNI eşik burada da kullanılıyor (MARKET_PULSE_RISK_RATIO) — biri
# görsel rozeti tetikler, diğeri Gemini sentezini; iki ayrı katman ama
# tutarlı bir eşiğe bağlı olmaları gerekiyor.
MARKET_PULSE_CHECK_INTERVAL_SECONDS = 5 * 60  # 5 dk — hafif bir kontrol, price_fetch_loop'tan bağımsız
# Histerezis: GİRİŞ ve ÇIKIŞ eşikleri kasıtlı olarak farklı. Tek bir eşik
# kullanılsaydı, piyasa oranın hemen etrafında dalgalandığında (ör. %74-76
# arası) bar her 5 dakikada bir kriz<->normal arasında "titreyebilirdi".
# Krize GİRMEK için %75, krizden ÇIKMAK için %65'in altına düşmek gerekir —
# ikisi arasındaki bant mevcut durumu korur.
MARKET_PULSE_RISK_RATIO = 0.75  # Stablex coinlerinin bu oranından fazlası düşüşteyse krize GİRİLİR
MARKET_PULSE_RECOVERY_RATIO = 0.65  # oran bunun ALTINA inmeden kriz durumu bitmez
MARKET_PULSE_SYNTHESIS_COOLDOWN_SECONDS = 2 * 60 * 60  # kriz sürse bile Gemini'yi bu süreden sık çağırma
MARKET_PULSE_NEWS_CONTEXT_LIMIT = 15  # sentez için Gemini'ye bağlam olarak verilecek son haber sayısı
