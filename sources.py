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

CHANNELS = {
    "push": "Push Bildirimi",
    "email": "E-posta Bülteni",
    "blog": "Blog İçeriği",
    "sosyal": "Sosyal Medya",
}
