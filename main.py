"""
Stablex Kripto Piyasa Canlı Radarı — WebSocket backend (FastAPI)

Kalıcılık: dış bağımlılık YOK — proje kökünde `radar.db` adlı bir SQLite
dosyasında (`market_intel` tablosu) saklanır. Sunucu yeniden başlasa bile
daha önce işlenmiş haberler tekrar Gemini'ye gitmez; bir istemci WebSocket'e
bağlandığı an son 50 haber `{"tip": "gecmis_haberler", "veri": [...]}`
mesajıyla doğrudan o istemciye gönderilir. Dört bağımsız arka plan görevi var:

  1. fetch_loop          — her 60 saniyede bir sources.py'deki BEYAZ LİSTE
     kaynakları (RSS/Google News vekil/BeautifulSoup) tarar. Sadece son
     FRESHNESS_WINDOW_DAYS (10) gün içindeki VE radar.db'de henüz kaydı
     olmayan haberler bir kuyruğa (_pending_queue) eklenir.
  2. emit_loop            — kuyruktan sırayla haber çekip Gemini API'ye
     gönderir, sonucu radar.db'ye yazıp "haber" tipinde yayınlar. Emisyon
     hızı en fazla NEWS_INTERVAL_SECONDS'ta bir (kuyruk boşsa bekler).
  3. price_fetch_loop     — her 90 saniyede (1-2 dk bandı, API kotasını
     zorlamamak için) CoinGecko'dan GERÇEK $ fiyat ve gerçek 24s değişim
     çeker — Stablex'te listeli 61 varlığın TAMAMI için (tek istekte),
     cache'i günceller.
  4. price_stream_loop    — her 3 saniyede bir cache'teki (gerçek) fiyatları
     "piyasa_verisi" tipinde istemcilere yayınlar (top coinler önce,
     kalan 51 coin alfabetik sırayla arkasından).
  5. competitor_coverage_loop — her 6 saatte bir rakip borsalardaki
     ("Rakip Kapsam Farkı") coin listelerini STABLEX_COINS ile karşılaştırır;
     kaynaklar sources.py:COMPETITOR_SOURCES'ta tanımlı (şu an: Paribu,
     herkese açık ticker API'si üzerinden). Yeni tespit varsa
     "rakip_kapsami" tipinde yayınlanır.
  6. (arka plan döngüsü değil) /api/x-feed — kullanıcı bir coin seçtiğinde
     Grok API'ye (xAI, x_search aracıyla) TEK coin için gerçek X
     gönderilerini (yazar/metin/link/etkileşim) getiren talep-üzerine bir
     uç nokta. Saatlik toplu bir tarama modeli denenmişti ama "kaynak
     linki zorunlu" kuralı büyük batch'lerde dakikalarca asılı kalmaya
     sebep oldu (bkz. proje notları) — tek-coin/talep-üzerine modeline
     geçildi. Reddit de denenmişti ama self-servis API kaydı platform
     seviyesinde kapandığı için X/Grok'a geçilmişti.
  7. x_discovery_loop       — günde bir Grok'a ayrı bir keşif sorgusu
     gönderir: Stablex'te olmayan ama X'te trend olan ticker'ları
     (x_discovery tablosu) ve rakip borsa(lar)ın X sentiment'ini
     "x_kesif" tipinde yayınlar.
  8. onchain_loop           — her 2 saatte bir DefiLlama'dan USDT'nin küresel
     dolaşımdaki arzını çeker, 24s/7g değişimini hesaplayıp "onchain"
     tipinde yayınlar (Whale Alert kullanılmadı — ücretsiz tier'ı yok,
     $29.95/ay'lık planı "personal use only" lisanslı, bkz. proje notları).
  9. market_pulse_loop      — her 5 dakikada bir piyasa genişliğini
     (_price_cache'ten kaç coin yükseliş/düşüşte) kontrol eder. Coinlerin
     %75'inden fazlası düşüşteyse (ve son sentezden 2 saatten fazla
     geçtiyse) son haberleri + duyarlılık/on-chain verisini Gemini'ye
     sentezletip "piyasa_krizi" tipinde yayınlar — frontend'deki Piyasa
     Nabzı bar'ının içeriğini YERİNDE değiştirir, yeni bir kart eklemez.

Gerekli ortam değişkenleri:
  COINGECKO_API_KEY — ücretsiz, coingecko.com/en/developers/dashboard
  GEMINI_API_KEY     — ücretsiz, aistudio.google.com/apikey
  XAI_API_KEY        — ücretli (pay-per-use), console.x.ai

Çalıştırma:
  pip install -r requirements.txt
  export COINGECKO_API_KEY=...
  export GEMINI_API_KEY=...
  uvicorn main:app --reload
  # http://127.0.0.1:8000
"""

import asyncio
import calendar
import hashlib
import json
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response
from xml.sax.saxutils import escape as xml_escape
from google import genai
from google.genai import types as genai_types
from openai import OpenAI

from sources import (
    CHANNELS,
    COMPETITOR_CHECK_INTERVAL_SECONDS,
    COMPETITOR_SOURCES,
    ENTRIES_PER_SOURCE,
    FETCH_INTERVAL_SECONDS,
    FRESHNESS_WINDOW_DAYS,
    PRICE_FETCH_INTERVAL_SECONDS,
    PRICE_STREAM_INTERVAL_SECONDS,
    MARKET_PULSE_CHECK_INTERVAL_SECONDS,
    MARKET_PULSE_NEWS_CONTEXT_LIMIT,
    MARKET_PULSE_RECOVERY_RATIO,
    MARKET_PULSE_RISK_RATIO,
    MARKET_PULSE_SYNTHESIS_COOLDOWN_SECONDS,
    ONCHAIN_CHANGE_THRESHOLD_PCT,
    ONCHAIN_CHECK_INTERVAL_SECONDS,
    ONCHAIN_STABLECOIN_SYMBOL,
    ONCHAIN_TRACKED_CHAINS,
    ONCHAIN_TVL_CHANGE_THRESHOLD_PCT,
    REGULATORY_LINKS,
    STABLEX_COIN_IDS,
    STABLEX_COINS,
    TOP_COIN_SYMBOLS,
    WHITELIST_SOURCES,
    XAI_MODEL,
    X_DISCOVERY_COMPETITOR_NAMES,
    X_DISCOVERY_INTERVAL_SECONDS,
)

STATIC_DIR = Path(__file__).parent / "static"

NEWS_INTERVAL_SECONDS = 15  # emisyon ritmi (kuyrukta bekleyen varsa en fazla bu sıklıkta)

STABLEX_COINS_SET = set(STABLEX_COINS)
CHANNEL_KEYS = list(CHANNELS.keys())

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

# Şeritte gösterilecek sıra: önce TOP_COIN_SYMBOLS, sonra kalan tüm Stablex
# coinleri alfabetik. Uydurma "yedek fiyat" YOK — cache boş başlar, ilk
# CoinGecko yanıtı gelene kadar ilgili satırlar istemcide "—" gösterir.
PRICE_DISPLAY_ORDER = TOP_COIN_SYMBOLS + sorted(
    s for s in STABLEX_COIN_IDS if s not in TOP_COIN_SYMBOLS
)

GEMINI_MODEL = "gemini-flash-lite-latest"  # bkz. ../poller/poll.py notu: sabit sürüm adları
                                            # yeni hesaplar için kapatılabiliyor, -latest alias kullan.
                                            # "gemini-flash-latest" bazen çok düşük günlük kotalı
                                            # (20/gün) bir modele denk geliyor, lite daha güvenilir.
GEMINI_MAX_RETRIES = 4
GEMINI_BASE_DELAY_SECONDS = 5
_gemini_client = None


def get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY tanımlı değil.")
        _gemini_client = genai.Client(api_key=api_key)
    return _gemini_client


_xai_client = None


def get_xai_client():
    """xAI'nin API'si OpenAI SDK ile uyumlu — sadece base_url'i değiştirip
    aynı istemciyi kullanıyoruz, ayrı bir SDK'ya gerek yok. timeout=45:
    x_search + kaynak linki isteyen sorgularda bir istek dakikalarca
    (gözlemlenen bir vakada 3+ dakika) asılı kalabiliyor — bu, çağrının
    süresiz beklemek yerine öngörülebilir şekilde hata vermesini sağlar."""
    global _xai_client
    if _xai_client is None:
        api_key = os.environ.get("XAI_API_KEY")
        if not api_key:
            raise RuntimeError("XAI_API_KEY tanımlı değil.")
        _xai_client = OpenAI(api_key=api_key, base_url="https://api.x.ai/v1", timeout=45.0)
    return _xai_client


def stable_id(*parts) -> str:
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


# --------------------------------------------------------------------------
# WebSocket bağlantı yönetimi
# --------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active:
            self.active.remove(websocket)

    async def broadcast(self, message: dict) -> None:
        dead = []
        # list(self.active): await sırasında yeni bir bağlantı/kopuş
        # self.active'i mutasyona uğratabilir; orijinal liste üzerinde
        # dönmek eleman atlanmasına yol açabilirdi.
        for websocket in list(self.active):
            try:
                await websocket.send_json(message)
            except Exception:
                dead.append(websocket)
        for websocket in dead:
            self.disconnect(websocket)


manager = ConnectionManager()


# --------------------------------------------------------------------------
# SQLite kalıcılığı — dış bağımlılık yok, radar.db proje kökünde
# --------------------------------------------------------------------------
DB_PATH = Path(__file__).parent / "radar.db"

_TITLE_NORM_RE = re.compile(r"[^\w\s]", re.UNICODE)


def normalize_title(title: str) -> str:
    """Çapraz-kaynak mükerrer tespiti için başlığı kabaca normalize eder
    (küçük harf, noktalama yok, tek boşluk). Kesin bir çözüm değil —
    farklı kelimelerle yazılmış aynı olayı yakalamaz, ama aynı/çok benzer
    başlıkla birden fazla kaynaktan gelen (ör. wire haberleri) klasik
    mükerrerleri ucuza yakalar."""
    text = (title or "").casefold().strip()
    text = _TITLE_NORM_RE.sub("", text)
    return re.sub(r"\s+", " ", text)


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        # WAL modu, dosyada kalıcıdır (tek sefer yeterli) — fetch_loop (okuma)
        # ve emit_loop (yazma) farklı thread'lerden eşzamanlı DB'ye dokunuyor;
        # varsayılan journal modunda nadiren "database is locked" görülebilirdi.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_intel (
                post_id      TEXT PRIMARY KEY,
                published_at TEXT,
                source       TEXT,
                title_norm   TEXT,
                json_data    TEXT NOT NULL
            )
        """)
        # Daha önce title_norm sütunu olmadan oluşturulmuş bir radar.db varsa
        # (bu güncellemeden önceki bir çalıştırmadan kalmışsa) sütunu ekle.
        cols = {row[1] for row in conn.execute("PRAGMA table_info(market_intel)")}
        if "title_norm" not in cols:
            conn.execute("ALTER TABLE market_intel ADD COLUMN title_norm TEXT")

        # Rakip Kapsam Farkı — bir coin bir rakipte İLK kez görüldüğünde
        # (rakip, coin) çifti tek satır olarak eklenir; ilk_gorulme o andan
        # sonra değişmez, bu yüzden "yeni listelendi" sinyali kalıcıdır.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS competitor_coverage (
                rakip       TEXT NOT NULL,
                coin        TEXT NOT NULL,
                ilk_gorulme TEXT NOT NULL,
                PRIMARY KEY (rakip, coin)
            )
        """)

        # Basit key-value kalıcılık — şu an sadece piyasa krizi durumunu
        # (aktif mi, son sentez ne zaman yapıldı) sunucu yeniden başlasa
        # bile hatırlamak için kullanılıyor (bkz. market_pulse_loop).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # X Keşif — competitor_coverage ile aynı mantık: bir ticker X'te
        # trend olarak İLK kez tespit edildiğinde eklenir, ilk_gorulme
        # bir daha değişmez.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS x_discovery (
                ticker      TEXT PRIMARY KEY,
                ilk_gorulme TEXT NOT NULL
            )
        """)
        conn.commit()
    finally:
        conn.close()


def db_check_batch(post_ids: list[str], freshness_cutoff_iso: str) -> tuple[set, set]:
    """Bir tarama turunda toplanan TÜM adayları TEK seferde kontrol eder
    (kaynak başına ayrı ayrı sorgu atmak yerine):
      - seen_ids: post_id'si zaten radar.db'de olan haberler (tüm geçmiş).
      - seen_titles: tazelik penceresi içindeki (normalize edilmiş) başlıklar
        — çapraz-kaynak mükerrer tespiti için.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        seen_ids = set()
        if post_ids:
            placeholders = ",".join("?" * len(post_ids))
            rows = conn.execute(
                f"SELECT post_id FROM market_intel WHERE post_id IN ({placeholders})",
                post_ids,
            ).fetchall()
            seen_ids = {row[0] for row in rows}

        title_rows = conn.execute(
            "SELECT title_norm FROM market_intel WHERE published_at >= ? AND title_norm IS NOT NULL AND title_norm != ''",
            (freshness_cutoff_iso,),
        ).fetchall()
        seen_titles = {row[0] for row in title_rows}
        return seen_ids, seen_titles
    finally:
        conn.close()


def db_save_news(message: dict) -> None:
    """Yayınlanacak haberi market_intel'e yazar (zengin JSON tek sütunda).
    post_id PRIMARY KEY olduğu için aynı haber iki kez yazılmaya çalışılırsa
    (teorik olarak) sessizce üzerine yazar, hata vermez."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO market_intel (post_id, published_at, source, title_norm, json_data) "
            "VALUES (?, ?, ?, ?, ?)",
            (message["id"], message["yayin_zamani"], message["kaynak"],
             normalize_title(message.get("title", "")),
             json.dumps(message, ensure_ascii=False)),
        )
        conn.commit()
    finally:
        conn.close()


def db_load_recent(limit: int) -> list[dict]:
    """published_at'e göre azalan (en yeni önce) sırayla son `limit` haberi
    döner — WebSocket bağlantısı kurulur kurulmaz istemciye gönderilir."""
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT json_data FROM market_intel ORDER BY published_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [json.loads(row[0]) for row in rows]
    finally:
        conn.close()


def db_upsert_competitor_coverage(rakip: str, coins: list[str]) -> list[str]:
    """Bir rakipte tespit edilen, Stablex'te olmayan coinleri kaydeder.
    (rakip, coin) zaten varsa INSERT OR IGNORE sessizce atlar — yani
    ilk_gorulme tarihi bir daha DEĞİŞMEZ, gerçekten yeni olanları döner."""
    conn = sqlite3.connect(DB_PATH)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        new_coins = []
        for coin in coins:
            cur = conn.execute(
                "INSERT OR IGNORE INTO competitor_coverage (rakip, coin, ilk_gorulme) VALUES (?, ?, ?)",
                (rakip, coin, now_iso),
            )
            if cur.rowcount:
                new_coins.append(coin)
        conn.commit()
        return new_coins
    finally:
        conn.close()


def db_load_competitor_coverage() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT rakip, coin, ilk_gorulme FROM competitor_coverage ORDER BY ilk_gorulme DESC"
        ).fetchall()
        return [{"rakip": r[0], "coin": r[1], "ilk_gorulme": r[2]} for r in rows]
    finally:
        conn.close()


def db_get_state(key: str) -> str | None:
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def db_set_state(key: str, value: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO app_state (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def db_upsert_x_discovery(tickers: list[str]) -> list[str]:
    """competitor_coverage ile aynı desen: (INSERT OR IGNORE) zaten var
    olan ticker'ların ilk_gorulme'sini değiştirmez, sadece gerçekten
    yeni olanları döner."""
    conn = sqlite3.connect(DB_PATH)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        new_tickers = []
        for ticker in tickers:
            cur = conn.execute(
                "INSERT OR IGNORE INTO x_discovery (ticker, ilk_gorulme) VALUES (?, ?)",
                (ticker, now_iso),
            )
            if cur.rowcount:
                new_tickers.append(ticker)
        conn.commit()
        return new_tickers
    finally:
        conn.close()


def db_load_x_discovery() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT ticker, ilk_gorulme FROM x_discovery ORDER BY ilk_gorulme DESC"
        ).fetchall()
        return [{"ticker": r[0], "ilk_gorulme": r[1]} for r in rows]
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Beyaz liste kaynak tarayıcıları — sources.py:WHITELIST_SOURCES
# --------------------------------------------------------------------------
def _entry_published_at(entry):
    """feedparser'ın published_parsed/updated_parsed alanları HER ZAMAN UTC
    struct_time'dır. time.mktime() bunu YEREL saat sanıp yanlış epoch üretir
    (sunucu UTC+3'teyse haberler 3 saat kayar) — calendar.timegm() struct'ı
    doğrudan UTC olarak epoch'a çevirir, tek doğru yöntem budur."""
    struct = entry.get("published_parsed") or entry.get("updated_parsed")
    return datetime.fromtimestamp(calendar.timegm(struct), tz=timezone.utc) if struct else None


def fetch_rss_source(source: dict) -> list[dict]:
    parsed = feedparser.parse(source["url"])
    items = []
    for entry in parsed.entries[:ENTRIES_PER_SOURCE]:
        guid = entry.get("id") or entry.get("guid") or entry.get("link")
        items.append({
            "post_id": stable_id("rss", source["name"], guid),
            "title": entry.get("title", "").strip(),
            "source": source["name"],
            "kategori": source["kategori"],
            "url": entry.get("link"),
            "published_at": _entry_published_at(entry),
        })
    return items


def fetch_google_news_source(source: dict) -> list[dict]:
    """Kaynağın kendi RSS'i olmadığında (Reuters 401, Foreks 403) Google
    News'in ücretsiz arama RSS'i üzerinden vekil bir akış — gürültü riski
    doğrudan RSS'e göre daha yüksektir."""
    query = source["query"].replace(" ", "+")
    url = f"https://news.google.com/rss/search?q={query}&hl=tr&gl=TR&ceid=TR:tr"
    parsed = feedparser.parse(url)
    items = []
    for entry in parsed.entries[:ENTRIES_PER_SOURCE]:
        items.append({
            "post_id": stable_id("google_news", source["name"], entry.get("link")),
            "title": entry.get("title", "").strip(),
            "source": source["name"],
            "kategori": source["kategori"],
            "url": entry.get("link"),
            "published_at": _entry_published_at(entry),
        })
    return items


_SPK_DATE_RE = re.compile(r"^(\d{2})\s+(\w{3})\s+(\d{4})")
_TR_MONTHS = {
    "Oca": 1, "Şub": 2, "Mar": 3, "Nis": 4, "May": 5, "Haz": 6,
    "Tem": 7, "Ağu": 8, "Eyl": 9, "Eki": 10, "Kas": 11, "Ara": 12,
}


def fetch_spk_source(source: dict) -> list[dict]:
    """SPK'nın RSS'i yok — resmi basın duyuruları sayfasını BeautifulSoup
    ile tarıyoruz. Sayfa 'DD Ay YYYY' ile başlayan duyuru satırları listeler;
    bu önekten hem tarihi hem başlığı çıkarıyoruz."""
    year = datetime.now(timezone.utc).year
    url = f"https://spk.gov.tr/duyurular/basin-duyurulari/{year}"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        response.raise_for_status()
    except Exception as exc:
        print(f"  ⚠ SPK sayfası okunamadı: {exc}")
        return []

    soup = BeautifulSoup(response.text, "lxml")
    seen_urls = set()
    items = []
    for a in soup.select(f'a[href*="basin-duyurulari/{year}/"]'):
        text = a.get_text(strip=True)
        href = a.get("href")
        match = _SPK_DATE_RE.match(text)
        if not match or not href:
            continue  # tarih ile başlamayan linkler (nav/kategori) atlanır

        full_url = href if href.startswith("http") else f"https://spk.gov.tr{href}"
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        day, mon_abbr, year_str = match.groups()
        month = _TR_MONTHS.get(mon_abbr)
        published_at = (
            datetime(int(year_str), month, int(day), tzinfo=timezone.utc) if month else None
        )
        title = text[match.end():].strip() or text
        title = re.sub(r"^Yeni(?=Basın Duyurusu)", "", title)  # sayfadaki "Yeni" rozeti karışıyor

        items.append({
            "post_id": stable_id("spk", full_url),
            "title": title,
            "source": "SPK",
            "kategori": source["kategori"],
            "url": full_url,
            "published_at": published_at,
        })
    return items[:ENTRIES_PER_SOURCE]


FETCHERS = {
    "rss": fetch_rss_source,
    "google_news": fetch_google_news_source,
    "spk_html": fetch_spk_source,
}


def _is_fresh(published_at) -> bool:
    if published_at is None:
        return True  # tarih bilgisi olmayan nadir girdilerde ihtiyatlı davranıp dışlamıyoruz
    cutoff = datetime.now(timezone.utc) - timedelta(days=FRESHNESS_WINDOW_DAYS)
    return published_at >= cutoff


_queued_ids: set[str] = set()  # şu an kuyrukta bekleyen/analiz sürecindeki haberler (bu çalıştırma için)
_queued_titles: set[str] = set()  # aynı turda/kuyrukta normalize başlık çakışmasını yakalamak için
_retry_counts: dict = {}  # post_id -> kaç kez Gemini analizi başarısız oldu
_pending_queue: "asyncio.Queue[dict]" = asyncio.Queue()
EMIT_MAX_RETRIES = 5  # bu kadar denemeden sonra bir haberden vazgeçilir (kalıcı hata varsayımı)


async def fetch_loop() -> None:
    """Beyaz liste kaynaklarını periyodik tarar (HEPSİ AYNI ANDA, sıralı
    değil); sadece son FRESHNESS_WINDOW_DAYS gün içindeki VE radar.db'de
    kaydı olmayan haberleri kuyruğa ekler — sunucu yeniden başlasa bile
    aynı haber tekrar Gemini'ye gitmez. Ayrıca normalize edilmiş başlık
    üzerinden ÇAPRAZ KAYNAK mükerrer kontrolü yapar (aynı haberi farklı
    kaynaklardan iki kez işlememek için — kesin bir çözüm değil, aynen ya
    da çok benzer başlıkları yakalar).

    Gemini analizi başarısız olan bir haber radar.db'ye yazılmadığı için
    "görülmemiş" sayılmaya devam eder, ama kuyruktan da düşmez — emit_loop
    kendi içinde tekrar dener (bkz. emit_loop, EMIT_MAX_RETRIES); bu tarama
    o haberi ikinci kez kuyruğa eklemekle uğraşmaz (_queued_ids/_queued_titles
    zaten işaretli tutuyor)."""
    while True:
        results = await asyncio.gather(
            *(asyncio.to_thread(FETCHERS[source["type"]], source) for source in WHITELIST_SOURCES),
            return_exceptions=True,
        )

        all_items = []
        for source, result in zip(WHITELIST_SOURCES, results):
            if isinstance(result, Exception):
                print(f"  ⚠ {source['name']} taranamadı: {result}")
                continue
            all_items.extend(result)

        fresh_items = [i for i in all_items if _is_fresh(i["published_at"])]
        candidates = [i for i in fresh_items if i["post_id"] not in _queued_ids]

        if candidates:
            cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=FRESHNESS_WINDOW_DAYS)).isoformat()
            seen_ids, seen_titles = await asyncio.to_thread(
                db_check_batch, [i["post_id"] for i in candidates], cutoff_iso
            )
            for item in candidates:
                post_id = item["post_id"]
                if post_id in seen_ids:
                    continue
                title_norm = normalize_title(item["title"])
                if title_norm and (title_norm in seen_titles or title_norm in _queued_titles):
                    continue

                item["_title_norm"] = title_norm
                _queued_ids.add(post_id)
                if title_norm:
                    _queued_titles.add(title_norm)
                await _pending_queue.put(item)

        await asyncio.sleep(FETCH_INTERVAL_SECONDS)


# --------------------------------------------------------------------------
# Gemini analizi
# --------------------------------------------------------------------------
SYSTEM_PROMPT = f"""Sen Stablex'te (Akbank/Ak Yatırım kripto borsası) çalışan, hem piyasa
analizi yapan hem de SPK'nın kripto varlık hizmet sağlayıcılarına yönelik
pazarlama/reklam kurallarına hakim kıdemli bir içerik editörüsün. Sana gelen
kripto/finans haberini analiz et ve SADECE şu JSON formatında yanıt ver:

{{
  "baslik_tr": "Haber başlığının doğal, akıcı Türkçe çevirisi (zaten Türkçeyse aynen bırak)",
  "ozet_tr": "2 cümlelik Türkçe özet",
  "stablex_etiketi": "kampanya_firsati" | "risk_uyarisi" | "regulasyon"
                      | "genel_farkindalik" | "onemsiz",
  "ilgili_varliklar": ["BTC", "ETH", ...],
  "onerilen_kanallar": ["push", "email", "blog", "sosyal"],
  "icerik_onerisi": {{
    "push": "O kanal önerildiyse: tek cümlelik, bilgilendirici (asla 'al/sat/kazan'
             çağrısı yapmayan) örnek push bildirimi metni",
    "email": "O kanal önerildiyse: tek cümlelik örnek e-posta bülteni konusu/özeti",
    "blog": "O kanal önerildiyse: bir editörün taslak yazmak için kullanabileceği
             profesyonel bir brief. 'Başlık:', 'Açı:', 'Değinilecek noktalar:',
             'Uyumluluk notu:', 'CTA:' etiketleriyle başlayan 5 satır, aralarında
             \\n olacak şekilde (bkz. BLOG BRIEF TALİMATLARI aşağıda)",
    "sosyal": "O kanal önerildiyse: tek paragraflık, bilgilendirici örnek sosyal
               medya paylaşım taslağı (emoji kullanmadan, abartısız)"
  }}
}}

BLOG BRIEF TALİMATLARI ("blog" alanı için — aşağıdakiler SANA yazma talimatı
veriyor, bunları kelimesi kelimesine kopyalayıp yanıtına yapıştırma, her
haber için özgün ve o habere özel içerik üret):
1. "Başlık:" — SEO'ya uygun, abartısız, net bir blog başlığı yaz.
2. "Açı:" — bu haber hangi zaviyeden ele alınmalı ve Stablex kullanıcısı için
   neden önemli olduğunu TEK CÜMLEDE, bu habere özgü şekilde anlat.
3. "Değinilecek noktalar:" — bu habere özgü, virgülle ayrılmış 3-4 somut alt
   başlık/madde yaz; her biri gerçek bilgi/bağlam versin, jenerik ifade kullanma.
4. "Uyumluluk notu:" — SADECE BU HABERE özgü, yazarken dikkat edilmesi gereken
   somut bir SPK/reklam mevzuatı noktası yaz (genel bir kural listesi değil,
   bu konunun kendine özgü riski ne ise onu belirt).
5. "CTA:" — yazının sonunda kullanıcıyı Stablex'te hangi bilgilendirici eyleme
   yönlendireceğini yaz (ör. "X hakkında daha fazla bilgi edinin"), asla
   doğrudan al/sat çağrısı olmasın.

Dil ve kalite kuralları:
- Tüm Türkçe metinler düzgün, akıcı, YAZIM VE DİLBİLGİSİ HATASI OLMAYAN
  profesyonel Türkçeyle yazılmalı. Yanıtı vermeden önce kendi yazdığın metni
  sessizce tekrar oku, uydurma/bozuk kelime varsa düzelt.
- Klişe, abartılı pazarlama dili kullanma; sakin, güvenilir, bilgilendirici
  bir kurumsal ton kullan.

Türkiye kripto reklam/pazarlama uyumluluk kuralları (TÜM içerik önerileri için
geçerli — push/email/blog/sosyal fark etmez):
- Asla fiyat tahmini, hedef fiyat veya getiri vaadi verme.
- CTA'lar "öğren", "incele", "takip et" gibi bilgilendirici eylemler önermeli,
  yatırım tavsiyesi izlenimi vermemeli.
- FOMO yaratacak aciliyet dili kullanma.
- "risk_uyarisi" etiketli haberlerde içerik önerisi riski gizlememeli.

Etiketleme kuralları:
- "ilgili_varliklar" SADECE şu listeden seçilebilir (Stablex'te listeli olanlar):
  {", ".join(STABLEX_COINS)}. Haberde geçen varlık bu listede yoksa listeye ekleme.
- "kampanya_firsati": fiyat yükselişi, olumlu kurumsal haber, yeni listeleme
  potansiyeli gibi kullanıcıyı harekete geçirebilecek haberler.
- "risk_uyarisi": ani düşüş, güvenlik ihlali, regülasyon baskısı.
- "onemsiz" etiketini gördüğünde "onerilen_kanallar" listesini boş bırak.
- "onerilen_kanallar" için sadece haberin gerçekten uygun olduğu kanalları seç
  (genelde 0-2 kanal yeterli); seçilmeyen kanalları "icerik_onerisi" içine hiç
  ekleme. Bu taslaklar doğrudan yayınlanacak metinler değil, ilham amaçlı
  örneklerdir."""


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[len("json"):]
    return json.loads(text.strip())


def _call_gemini_with_prompt(text: str, system_prompt: str) -> str:
    client = get_gemini_client()
    last_error = None
    for attempt in range(GEMINI_MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=text,
                config=genai_types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    response_mime_type="application/json",
                ),
            )
            return response.text
        except Exception as exc:  # Gemini rate limit / geçici hata
            last_error = exc
            delay = GEMINI_BASE_DELAY_SECONDS * (2 ** attempt)
            print(f"  Gemini hatası (deneme {attempt + 1}/{GEMINI_MAX_RETRIES}): {exc}. "
                  f"{delay}s bekleniyor...")
            time.sleep(delay)
    raise RuntimeError(f"Gemini {GEMINI_MAX_RETRIES} denemeden sonra başarısız: {last_error}")


def _call_gemini(news_text: str) -> str:
    return _call_gemini_with_prompt(news_text, SYSTEM_PROMPT)


def _normalize_analysis(analysis: dict) -> dict:
    assets = analysis.get("ilgili_varliklar") or []
    analysis["ilgili_varliklar"] = [a for a in assets if a in STABLEX_COINS_SET]

    channels = [c for c in (analysis.get("onerilen_kanallar") or []) if c in CHANNEL_KEYS]
    content = analysis.get("icerik_onerisi") or {}
    analysis["icerik_onerisi"] = {c: content[c] for c in channels if content.get(c)}
    analysis["onerilen_kanallar"] = [c for c in channels if c in analysis["icerik_onerisi"]]
    return analysis


def analyze_with_gemini(item: dict) -> dict:
    news_text = f"Title: {item['title']}\nSource: {item['source']}\nURL: {item['url']}"
    raw = _call_gemini(news_text)
    return _normalize_analysis(_extract_json(raw))


EMIT_BURST_QUEUE_THRESHOLD = 5  # kuyrukta bundan fazla haber varsa "toparlanma" moduna geç
EMIT_BURST_INTERVAL_SECONDS = 3  # toparlanma modunda emisyon sıklığı
EMIT_STEADY_INTERVAL_SECONDS = NEWS_INTERVAL_SECONDS  # normal (kuyruk kısa) sıklık


async def emit_loop() -> None:
    """Kuyruktan sırayla haber alır, Gemini ile analiz eder, sonucu yayınlar.
    Emisyon hızı ADAPTİF: kuyrukta EMIT_BURST_QUEUE_THRESHOLD'dan fazla haber
    birikmişse (ör. soğuk başlangıçta 11 kaynaktan gelen ilk dalga) daha sık
    emisyon yaparak hızla toparlanır; kuyruk kısaldığında normal (15sn) hıza
    döner — Gemini'yi sürekli hızlı yakmaz ama uzun bir "hiçbir şey gelmiyor"
    bekleyişini de önler.

    Analiz başarısız olursa haber KAYNAĞIN RSS'ine tekrar görünmesine bağlı
    kalmadan doğrudan kuyruğa geri konur (fetch_loop'un ilgili kaynağı
    tekrar taraması gerekmez) — aksi halde yoğun haber trafiğinde (kaynak
    başına taramada en fazla ENTRIES_PER_SOURCE girdiye bakılıyor) başarısız
    bir haber RSS'in "üst N" penceresinden düşüp kalıcı olarak kaybolabilirdi.
    EMIT_MAX_RETRIES denemeden sonra kalıcı hata varsayılıp vazgeçilir."""
    while True:
        item = await _pending_queue.get()
        _queued_ids.discard(item["post_id"])
        _queued_titles.discard(item.get("_title_norm", ""))
        try:
            analysis = await asyncio.to_thread(analyze_with_gemini, item)
        except Exception as exc:
            retries = _retry_counts.get(item["post_id"], 0) + 1
            _retry_counts[item["post_id"]] = retries
            if retries >= EMIT_MAX_RETRIES:
                print(f"  ✗ Vazgeçildi ({EMIT_MAX_RETRIES} deneme, {item['title'][:60]}): {exc}")
                _retry_counts.pop(item["post_id"], None)
            else:
                print(f"  ⚠ Gemini analizi başarısız, {retries}/{EMIT_MAX_RETRIES} "
                      f"({item['title'][:60]}): {exc}")
                # RSS'te tekrar görünmesini beklemeden doğrudan kuyruğa geri koy.
                _queued_ids.add(item["post_id"])
                if item.get("_title_norm"):
                    _queued_titles.add(item["_title_norm"])
                await _pending_queue.put(item)
            # Kısa bir bekleme ile kuyruğu/API'yi hızlı hata döngüsüyle boşa
            # yakmamaya çalışıyoruz.
            await asyncio.sleep(5)
            continue

        _retry_counts.pop(item["post_id"], None)
        message = {
            "tip": "haber",
            "id": item["post_id"],
            "kategori": item["kategori"],
            "kaynak": item["source"],
            "kaynak_url": item["url"],
            "yayin_zamani": (item["published_at"] or datetime.now(timezone.utc)).isoformat(),
            "title": item["title"],
            **analysis,
        }
        # radar.db'ye, WebSocket'e yayınlamadan HEMEN ÖNCE yazılır — sunucu
        # çöküp yeniden başlasa bile bu haber "görülmüş" sayılır.
        await asyncio.to_thread(db_save_news, message)
        await manager.broadcast(message)

        delay = (
            EMIT_BURST_INTERVAL_SECONDS
            if _pending_queue.qsize() > EMIT_BURST_QUEUE_THRESHOLD
            else EMIT_STEADY_INTERVAL_SECONDS
        )
        await asyncio.sleep(delay)


# --------------------------------------------------------------------------
# Rakip Kapsam Farkı — "rakipte var, Stablex'te yok" coin tespiti
# --------------------------------------------------------------------------
def fetch_competitor_coins_api(source: dict) -> set[str]:
    """Paribu gibi herkese açık, kimlik doğrulamasız bir ticker JSON'u olan
    rakipler için: pair anahtarlarını ("AAVE_TL", "ADA_USDT" gibi) tabana
    indirger ("AAVE", "ADA") — quote para birimini (TL/USDT/...) atar."""
    response = requests.get(source["url"], headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    response.raise_for_status()
    data = response.json()
    symbols = set()
    for pair in data.keys():
        base = pair.split("_")[0].strip().upper()
        if base:
            symbols.add(base)
    return symbols


async def competitor_coverage_loop() -> None:
    """sources.py:COMPETITOR_SOURCES'taki her rakip için ("api" tipinde
    canlı taranan ya da "manual" tipinde elle tutulan) coin listesini
    STABLEX_COINS ile karşılaştırır, sadece rakipte olup bizde olmayanları
    competitor_coverage'a yazar.

    Bilinen sınırlama: bir coin rakipten kaldırılırsa burada otomatik
    silinmiyor (yalnızca ekleme yapılıyor) — bu bir "keşif" akışı, "şu an
    tam olarak neyin listeli olduğu" akışı değil."""
    while True:
        any_new = False
        for source in COMPETITOR_SOURCES:
            try:
                if source["type"] == "api":
                    coins = await asyncio.to_thread(fetch_competitor_coins_api, source)
                elif source["type"] == "manual":
                    coins = set(source.get("coins", []))
                else:
                    continue
            except Exception as exc:
                print(f"  ⚠ {source['name']} rakip kapsamı taranamadı: {exc}")
                continue

            missing = sorted(coins - STABLEX_COINS_SET)
            if not missing:
                continue
            new_coins = await asyncio.to_thread(db_upsert_competitor_coverage, source["name"], missing)
            if new_coins:
                any_new = True
                print(f"  + {source['name']}: {len(new_coins)} yeni rakip-only coin — {', '.join(new_coins)}")

        if any_new:
            snapshot = await asyncio.to_thread(db_load_competitor_coverage)
            await manager.broadcast({"tip": "rakip_kapsami", "veri": snapshot})

        await asyncio.sleep(COMPETITOR_CHECK_INTERVAL_SECONDS)


# --------------------------------------------------------------------------
# X Gönderi Akışı + Keşif — X (Twitter) üzerinden Grok API (xAI)
# --------------------------------------------------------------------------
# Grok'un x_search aracı bizim yerimize X'i arayıp özetliyor — biz X'e
# hiç dokunmuyoruz, sadece Grok'a "şunu araştır, şu formatta rapor et"
# diyoruz (bkz. proje notları).
#
# TASARIM NOTU (önceki turdan): ilk tasarım saatlik, toplu (20 coin/tarama),
# "hype skoru + kaynak linki" özetleyen bir sistemdi — ama "kaynak linki
# ZORUNLU" kuralı modeli aşırı temkinli/yavaş yaptı (büyük batch'lerde
# dakikalarca asılı kaldı, küçük testlerde boş sonuç döndü). Bunun yerine
# TALEP ÜZERİNE (kullanıcı bir coin seçtiğinde), TEK coin için GERÇEK
# gönderileri (özet/skor değil, ham gönderi + link) döndüren bir modele
# geçildi — hem daha güvenilir hem her sonuç zaten kendi kaynağını taşıyor.
X_POST_FEED_SYSTEM_PROMPT = """Sen Stablex'te çalışan bir sosyal medya analistisin. Sana
verilen TEK bir kripto para ticker'ı için X (Twitter) üzerinde TAM OLARAK 1 kez x_search
aracını kullanarak GERÇEK gönderileri bul. SADECE aşağıdaki JSON formatında yanıtla:

{"gonderiler": [{"yazar": "@kullanici", "metin": "...", "url": "https://x.com/...", "begeni": 0, "yanit": 0, "repost": 0, "tur": "yeni"}]}

Kurallar:
- En fazla 5 gönderi döndür — ÇEŞİTLİLİK ÖNEMLİ, aşağıdaki 3 türden bir karışım yap
  (hepsi aynı türden olmasın). ZAMAN PENCERESİ türe göre değişir:
  - "yeni": SADECE son 1 SAAT içinde paylaşılmış gönderi(ler) — bu pencerenin dışında
    bir şeyi "yeni" olarak etiketleme.
  - "etkilesimli": son 24 SAAT içinden, toplam beğeni/repost/yanıt sayısı en yüksek
    olan gönderi(ler) — "yeni"den farklı olarak daha geniş bir pencerede arayabilirsin,
    zaman değil toplam popülerlik kriteri.
  - "yukseliste": son 24 SAAT içinden, kısa sürede hızla etkileşim toplayan/momentum
    kazanan bir gönderi — "etkilesimli"den farkı: toplam sayısı en yüksek olmayabilir
    ama yayınlandığı andan bu yana hızla büyüyor/viral olmaya başlıyor. Böyle bir
    gönderi bulamazsan bu türü atla, uydurma.
- "tur": SADECE "yeni", "etkilesimli" ya da "yukseliste" — her gönderi için hangi
  sebeple seçtiğini belirt.
- "metin": gönderinin TAMAMI değil, en fazla 200 karakterlik bir alıntı.
- "url": GERÇEKTEN bulduğun gönderinin tam X linki — ZORUNLU. Gerçek bir link
  bulamadığın bir gönderiyi listeye HİÇ EKLEME, uydurma link kesinlikle yasak.
- "begeni"/"yanit"/"repost": bildiğin gerçek sayılar; bilmiyorsan 0 yaz, uydurma.
- Bu ticker hakkında gerçek/ilgili gönderi bulamazsan boş liste döndür."""

X_DISCOVERY_SYSTEM_PROMPT = """Sen Stablex'te çalışan bir piyasa istihbaratı analistisin.
X (Twitter) üzerinde en fazla 2 kez x_search aracı kullanarak şunları araştır:
1) Son 24 saatte X'te gerçekten popüler/gündemde olan kripto para ticker'ları (herhangi
   bir listeyle sınırlı değil).
2) Sana verilen rakip borsa(lar) hakkında X'teki genel sentiment ve kısa bir gerekçe.

SADECE aşağıdaki JSON formatında yanıtla:
{
  "trend_ticker_lar": ["XYZ", "ABC"],
  "rakip_sentiment": {"Paribu": {"yon": "olumlu", "ozet": "...", "kaynak_url": "https://x.com/..."}}
}

Kurallar:
- "trend_ticker_lar": en fazla 10 tane, SADECE gerçekten popüler ve şüpheli/spam olmayan
  ticker'lar — emin olmadığın bir şeyi ekleme, boş liste döndürebilirsin.
- "yon" alanı SADECE "olumlu", "olumsuz" ya da "notr" olabilir.
- "ozet" en fazla 20 kelime, nesnel bir dille.
- "kaynak_url": rakip_sentiment değerlendirmene dayanak olan, GERÇEKTEN bulduğun bir
  gönderinin tam X linki. Gerçek bir link bulamadıysan bu alanı hiç ekleme — ASLA link
  uydurma, bu en kritik kural."""


def _call_xai_x_search(user_content: str, system_prompt: str) -> str:
    """xAI'nin Responses API'sini x_search aracıyla çağırır, ham metni
    (JSON olması beklenir) döner. Gemini çağrılarımızdan farklı olarak
    agresif retry yapmıyoruz — x_search çağrıları nispeten pahalı, hızlı
    hata döngüsü maliyeti gereksiz katlar; bir sonraki periyodik taramada
    zaten tekrar denenecek."""
    client = get_xai_client()
    response = client.responses.create(
        model=XAI_MODEL,
        instructions=system_prompt,
        input=user_content,
        tools=[{"type": "x_search"}],
    )
    return response.output_text


def _clean_source_url(url) -> str | None:
    """Gemini/Grok'un döndürdüğü kaynak URL'sini kaba bir şekilde doğrular
    — gerçek bir XSS savunması DEĞİL (o iş frontend'deki safeUrl()'ün),
    sadece "N/A", boş metin gibi belirgin şekilde link olmayan değerleri
    en baştan eleyip veritabanına/WS'e taşımamak için."""
    url = str(url or "").strip()
    return url if url.startswith("http://") or url.startswith("https://") else None


def fetch_x_post_feed(symbol: str, region: str = "global") -> list[dict]:
    """Tek bir coin için Grok'a gerçek X gönderilerini getirtir. "url"
    olmayan (Grok'un talimata rağmen link vermediği) her gönderi baştan
    elenir — özet/skor değil, doğrudan kaynak gösteren ham veri döner.

    region="tr": ayrı, açıkça Türkçe/Türkiye odaklı bir arama — global
    aramayla AYNI çağrıda birleştirilmiyor çünkü kripto X'i ezici
    çoğunlukla İngilizce/global; tek aramada "bölge etiketi" istesek bile
    Türkçe içerik muhtemelen cılız kalırdı. Kullanıcı gerçekten TR
    görünümü istediğinde ayrı bir ücretli çağrı yapılır (bkz. proje
    notları — otomatik ikisini birden çekmiyoruz, maliyeti katlar)."""
    region_hint = (
        "SADECE Türkçe yazılmış, Türkiye'deki kullanıcılardan/hesaplardan gönderiler ara."
        if region == "tr" else
        "Global (herhangi bir dilde, dünya genelinden) gönderiler ara."
    )
    raw = _call_xai_x_search(f"Ticker: {symbol}\n{region_hint}", X_POST_FEED_SYSTEM_PROMPT)
    data = _extract_json(raw)
    posts = []
    for item in (data.get("gonderiler") or [])[:5]:
        url = _clean_source_url(item.get("url"))
        if not url:
            continue
        tur = item.get("tur")
        posts.append({
            "yazar": str(item.get("yazar", ""))[:50],
            "metin": str(item.get("metin", ""))[:200],
            "url": url,
            "begeni": item.get("begeni", 0),
            "yanit": item.get("yanit", 0),
            "repost": item.get("repost", 0),
            "tur": tur if tur in ("yeni", "etkilesimli", "yukseliste") else "yeni",
        })
    return posts


def fetch_x_discovery() -> dict:
    """Stablex'te olmayan ama X'te trend olan ticker'ları ve rakip
    borsa(lar)ın X'teki sentiment'ini TEK bir Grok çağrısında toplar.
    rakip_sentiment içindeki "kaynak_url" — şeffaflık için, dayanak
    gösterilen gerçek gönderi (varsa)."""
    user_content = f"Rakip borsa(lar): {', '.join(X_DISCOVERY_COMPETITOR_NAMES)}"
    raw = _call_xai_x_search(user_content, X_DISCOVERY_SYSTEM_PROMPT)
    data = _extract_json(raw)
    trend_tickers = [str(t).strip().upper() for t in (data.get("trend_ticker_lar") or []) if str(t).strip()]
    rakip_sentiment = {}
    for rakip, info in (data.get("rakip_sentiment") or {}).items():
        if not isinstance(info, dict):
            continue
        rakip_sentiment[rakip] = {
            "yon": info.get("yon") if info.get("yon") in ("olumlu", "olumsuz", "notr") else "notr",
            "ozet": info.get("ozet", ""),
            "kaynak_url": _clean_source_url(info.get("kaynak_url")),
        }
    return {
        "trend_ticker_lar": trend_tickers[:10],
        "rakip_sentiment": rakip_sentiment,
    }


_latest_x_rakip_sentiment: dict = {}


async def x_discovery_loop() -> None:
    """Her X_DISCOVERY_INTERVAL_SECONDS'ta (günlük) bir Grok'a ayrı bir
    keşif sorgusu gönderir: (a) Stablex'te olmayan trend ticker'ları
    x_discovery tablosuna (competitor_coverage ile aynı "ilk görülme"
    mantığı) kaydeder, (b) rakip borsa(lar)ın X sentiment'ini yayınlar.
    Saatlik sentiment taramasından bağımsız — açık uçlu keşif sorgusu
    muhtemelen daha çok arama gerektirir, günlük yeterli."""
    global _latest_x_rakip_sentiment
    while True:
        try:
            result = await asyncio.to_thread(fetch_x_discovery)
        except Exception as exc:
            print(f"  ⚠ X keşif taraması başarısız: {exc}")
            await asyncio.sleep(X_DISCOVERY_INTERVAL_SECONDS)
            continue

        unlisted = [t for t in result["trend_ticker_lar"] if t not in STABLEX_COINS_SET]
        if unlisted:
            new_tickers = await asyncio.to_thread(db_upsert_x_discovery, unlisted)
            if new_tickers:
                print(f"  + X'te trend, Stablex'te yok: {', '.join(new_tickers)}")

        _latest_x_rakip_sentiment = result["rakip_sentiment"]
        snapshot = await asyncio.to_thread(db_load_x_discovery)
        await manager.broadcast({
            "tip": "x_kesif",
            "veri": {"trend_coinler": snapshot, "rakip_sentiment": _latest_x_rakip_sentiment},
        })

        await asyncio.sleep(X_DISCOVERY_INTERVAL_SECONDS)


# --------------------------------------------------------------------------
# On-Chain Olaylar — DefiLlama'dan stablecoin arz (mint/burn) + zincir TVL
# --------------------------------------------------------------------------
DEFILLAMA_STABLECOINS_URL = "https://stablecoins.llama.fi/stablecoins"
DEFILLAMA_CHAIN_TVL_URL = "https://api.llama.fi/v2/historicalChainTvl/{chain}"

_latest_onchain_snapshot: dict | None = None


def fetch_stablecoin_supply_change() -> dict | None:
    """DefiLlama'dan ONCHAIN_STABLECOIN_SYMBOL'ün (USDT) küresel dolaşımdaki
    arzını çeker, 24 saatlik ve 7 günlük değişim yüzdesini hesaplar. Auth
    gerektirmez, tamamen ücretsiz. Büyük bir arz ARTIŞI (mint) borsalara yeni
    likidite girişine işaret eder (genelde olumlu bir piyasa sinyali); büyük
    bir AZALIŞ (burn) likidite çıkışına işaret eder (dikkat sinyali)."""
    response = requests.get(DEFILLAMA_STABLECOINS_URL, params={"includePrices": "true"}, timeout=20)
    response.raise_for_status()
    for asset in response.json().get("peggedAssets", []):
        if asset.get("symbol") != ONCHAIN_STABLECOIN_SYMBOL:
            continue
        current = asset.get("circulating", {}).get("peggedUSD")
        prev_day = asset.get("circulatingPrevDay", {}).get("peggedUSD")
        prev_week = asset.get("circulatingPrevWeek", {}).get("peggedUSD")
        if not current or not prev_day:
            return None
        change_24s_pct = (current - prev_day) / prev_day * 100
        change_7g_pct = (current - prev_week) / prev_week * 100 if prev_week else 0.0
        yon = "mint" if change_24s_pct >= ONCHAIN_CHANGE_THRESHOLD_PCT else (
            "burn" if change_24s_pct <= -ONCHAIN_CHANGE_THRESHOLD_PCT else "sabit"
        )
        return {
            "sembol": ONCHAIN_STABLECOIN_SYMBOL,
            "dolasimdaki_arz_usd": current,
            "degisim_24s_yuzde": round(change_24s_pct, 3),
            "degisim_7g_yuzde": round(change_7g_pct, 3),
            "yon": yon,
        }
    return None


def _closest_series_entry(series: list[dict], target_date: float, tolerance_seconds: float) -> dict | None:
    """`series` (DefiLlama'nın kronolojik artan {date, tvl} listesi) içinde
    `target_date`'e en yakın kaydı bulur. `tolerance_seconds`'tan daha uzak
    bir kayıt bulunursa None döner — yani "en yakın olan neyse onu kullan"
    değil, "yeterince yakın bir kayıt yoksa güvenme" mantığı. Bu, DefiLlama
    verisinde bir gün eksik olduğunda (nadir ama olur) sabit bir index'in
    (ör. series[-8]) sessizce yanlış bir tarihi "7 gün önce" sanmasını
    önler."""
    best, best_diff = None, None
    for entry in reversed(series):
        diff = abs(entry["date"] - target_date)
        if best_diff is None or diff < best_diff:
            best, best_diff = entry, diff
        if entry["date"] < target_date - tolerance_seconds:
            break  # seri kronolojik artan, daha geriye gitmenin faydası yok
    return best if best is not None and best_diff <= tolerance_seconds else None


def fetch_chain_tvl_change(chain: str, symbol: str) -> dict | None:
    """DefiLlama'nın günlük TVL zaman serisinden (historicalChainTvl) 1 gün
    ve 7 gün öncesine en yakın kayıtları TARİHE GÖRE bulur (sabit index
    offset'i DEĞİL — bkz. _closest_series_entry) ve değişim yüzdesini
    hesaplar."""
    url = DEFILLAMA_CHAIN_TVL_URL.format(chain=chain)
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    series = response.json()
    if len(series) < 2:
        return None

    current_entry = series[-1]
    current = current_entry.get("tvl")
    current_date = current_entry.get("date")
    if not current or not current_date:
        return None

    prev_day_entry = _closest_series_entry(series, current_date - 86400, tolerance_seconds=12 * 60 * 60)
    if not prev_day_entry or not prev_day_entry.get("tvl"):
        return None  # 1 günlük referans için yeterince yakın bir kayıt yok, güvenilir değişim hesaplanamaz

    prev_day = prev_day_entry["tvl"]
    change_24s_pct = (current - prev_day) / prev_day * 100

    prev_week_entry = _closest_series_entry(series, current_date - 7 * 86400, tolerance_seconds=36 * 60 * 60)
    change_7g_pct = 0.0
    if prev_week_entry and prev_week_entry.get("tvl"):
        change_7g_pct = (current - prev_week_entry["tvl"]) / prev_week_entry["tvl"] * 100

    yon = "artis" if change_24s_pct >= ONCHAIN_TVL_CHANGE_THRESHOLD_PCT else (
        "azalis" if change_24s_pct <= -ONCHAIN_TVL_CHANGE_THRESHOLD_PCT else "sabit"
    )
    return {
        "sembol": symbol,
        "zincir": chain,
        "tvl_usd": current,
        "degisim_24s_yuzde": round(change_24s_pct, 3),
        "degisim_7g_yuzde": round(change_7g_pct, 3),
        "yon": yon,
    }


def fetch_onchain_snapshot() -> dict:
    """Stablecoin arzı + izlenen zincirlerin TVL değişimini tek bir anlık
    görüntüde toplar. Sıralı (senkron) HTTP istekleri kullanır — 2 saatte
    bir çalıştığı için paralelleştirmeye gerek yok, basitlik tercih edildi."""
    try:
        stablecoin = fetch_stablecoin_supply_change()
    except Exception as exc:
        print(f"  ⚠ Stablecoin arz verisi alınamadı: {exc}")
        stablecoin = None

    chains = []
    for chain_name, symbol in ONCHAIN_TRACKED_CHAINS.items():
        try:
            tvl_data = fetch_chain_tvl_change(chain_name, symbol)
        except Exception as exc:
            print(f"  ⚠ {chain_name} TVL verisi alınamadı: {exc}")
            continue
        if tvl_data:
            chains.append(tvl_data)

    return {"stablecoin": stablecoin, "zincirler": chains}


async def onchain_loop() -> None:
    """Her ONCHAIN_CHECK_INTERVAL_SECONDS'ta (2 saat) bir DefiLlama'yı
    kontrol eder (stablecoin arzı + izlenen zincirlerin TVL'i). Yeni bağlanan
    istemciye anında gösterebilmek için son anlık görüntü modül seviyesinde
    tutulur."""
    global _latest_onchain_snapshot
    while True:
        snapshot = await asyncio.to_thread(fetch_onchain_snapshot)

        if snapshot["stablecoin"] or snapshot["zincirler"]:
            snapshot["zaman"] = datetime.now(timezone.utc).isoformat()
            _latest_onchain_snapshot = snapshot
            await manager.broadcast({"tip": "onchain", "veri": snapshot})

        await asyncio.sleep(ONCHAIN_CHECK_INTERVAL_SECONDS)


# --------------------------------------------------------------------------
# Canlı borsa akışı — CoinGecko'dan gerçek fiyat + gerçek 24s değişim
# --------------------------------------------------------------------------
# Son başarılı CoinGecko yanıtı burada tutulur. Uydurma "yedek fiyat" YOK —
# cache boş başlar; ilk yanıt gelene kadar istemci ilgili satırlarda "—"
# gösterir. API başarısız olursa son bilinen gerçek değer korunur.
_price_cache: dict = {}


def _fetch_coingecko_prices() -> dict:
    api_key = os.environ.get("COINGECKO_API_KEY")
    if not api_key:
        raise RuntimeError("COINGECKO_API_KEY tanımlı değil.")

    params = {
        "vs_currency": "usd",
        "ids": ",".join(STABLEX_COIN_IDS.values()),
        "price_change_percentage": "24h",
        "x_cg_demo_api_key": api_key,
    }
    response = requests.get(COINGECKO_URL, params=params, timeout=20)
    response.raise_for_status()

    id_to_symbol = {v: k for k, v in STABLEX_COIN_IDS.items()}
    result = {}
    for coin in response.json():
        symbol = id_to_symbol.get(coin["id"])
        if not symbol:
            continue
        price = coin["current_price"]
        result[symbol] = {
            "fiyat_usd": round(price, 2 if price >= 10 else 4 if price >= 0.01 else 8),
            "degisim_24s": round(coin.get("price_change_percentage_24h") or 0.0, 2),
        }
    return result


async def price_fetch_loop() -> None:
    """CoinGecko'ya gerçek istek atan döngü — Stablex'te listeli 61 coinin
    TAMAMINI TEK istekte çeker (coin sayısı arttıkça istek sayısı artmaz).
    Ücretsiz kotayı zorlamamak için yayın sıklığından (3sn) daha seyrek
    çalışır (varsayılan 90sn — 1-2 dk bandı)."""
    while True:
        try:
            fresh = await asyncio.to_thread(_fetch_coingecko_prices)
            _price_cache.update(fresh)
        except Exception as exc:
            print(f"  ⚠ CoinGecko'dan fiyat alınamadı, son bilinen değerler korunuyor: {exc}")
        await asyncio.sleep(PRICE_FETCH_INTERVAL_SECONDS)


async def price_stream_loop() -> None:
    """İstemcilere sabit ritimde (3sn) yayın yapan döngü — değerler
    price_fetch_loop'un güncellediği cache'ten okunur, burada üretilmez.
    Sıralama: top coinler önce, kalan Stablex coinleri alfabetik. Henüz
    fiyatı gelmemiş coinler için "fiyat_usd": null gönderilir (istemci "—"
    gösterir) — uydurma değer yayınlanmaz."""
    while True:
        await asyncio.sleep(PRICE_STREAM_INTERVAL_SECONDS)
        payload = [
            {"sembol": symbol, **_price_cache.get(symbol, {"fiyat_usd": None, "degisim_24s": None})}
            for symbol in PRICE_DISPLAY_ORDER
        ]
        await manager.broadcast({
            "tip": "piyasa_verisi",
            "zaman": datetime.now(timezone.utc).isoformat(),
            "veri": payload,
        })


# --------------------------------------------------------------------------
# Piyasa Nabzı — Kriz Anı Sentezi
# --------------------------------------------------------------------------
# Frontend'deki breadth bar'ı zaten "Piyasa Nabzı" görselini gösteriyor
# (bkz. index.html:renderMarketPulse). Burada YENİ bir kart/mesaj tipi
# eklemiyoruz — aynı bar'ın içeriğini, piyasa genelinde geniş çaplı bir
# düşüş tespit edildiğinde Gemini'nin ürettiği "neden düşüyoruz" özetiyle
# YERİNDE değiştiriyoruz (bkz. "piyasa_krizi" mesajı, frontend'de
# handleMarketCrisis()).
MARKET_CRISIS_SYSTEM_PROMPT = """Sen Stablex'te çalışan kıdemli bir piyasa analisti VE
içerik editörüsün. Sana son birkaç saatteki haber başlıkları ve (varsa) sosyal
duyarlılık/on-chain sinyalleri veriliyor. Stablex'te listeli coinlerin büyük çoğunluğunda
piyasa genelinde bir düşüş yaşanıyor. SADECE aşağıdaki JSON formatında yanıtla:

{
  "maddeler": ["...", "...", "..."],
  "icerik_onerisi": {
    "push": "...",
    "email": "..."
  }
}

"maddeler" için kurallar:
- En fazla 3 madde, her biri en fazla 20 kelime, Türkçe, nesnel ve sakin bir dille.
- Her madde somut bir kategoriye bağlı olsun: makroekonomik gelişme, on-chain/likidite
  hareketi, regülasyon/hukuki gelişme, ya da spesifik bir borsa/proje olayı.
- SPEKÜLASYON YAPMA — sadece sana verilen bağlamdaki gerçek haber/veri noktalarına dayan.
  Bağlamda düşüşü açıklayacak somut bir şey yoksa o kategoriyi atla; hiçbir şey
  bulamazsan "maddeler" için boş liste ([]) döndür VE "icerik_onerisi"ni de boş obje
  ({}) olarak döndür — somut bir neden yoksa iletişim taslağı da üretme.

"icerik_onerisi" için kurallar (SADECE "maddeler" doluysa doldur, aksi halde {} bırak):
- "push": Tek cümlelik, bilgilendirici bir push bildirimi metni (ör. "Bitcoin'deki
  dalgalanmanın ardındaki zincir üstü verileri ve makro gelişmeleri sizin için
  özetledik. Hemen inceleyin." tarzında) — asla "panik yapmayın", "hemen alım
  yapın" gibi ifadeler kullanma.
- "email": "Konu:" ve "Gövde:" etiketleriyle başlayan, aralarında \\n olan iki satır.
  Konu başlığı veri odaklı olsun (ör. "Piyasadaki Geri Çekilmenin Arkasındaki 3 Veri").
  Gövde, "maddeler" listesini doğal bir dille özetleyen, yatırım tavsiyesi İÇERMEYEN
  kısa bir paragraf olsun.

Dil ve uyumluluk kuralları (TÜM içerik için):
- Asla fiyat tahmini, hedef fiyat veya getiri vaadi verme.
- CTA'lar "incele", "detayları öğren" gibi bilgilendirici eylemler önermeli, yatırım
  tavsiyesi izlenimi vermemeli.
- FOMO/panik yaratacak aciliyet dili kullanma — sakin, güven verici, kurumsal bir ton."""


def synthesize_market_crisis(news_items: list[dict], sentiment: list, onchain: dict | None) -> dict:
    """Son N haberi + (varsa) sosyal duyarlılık/on-chain anlık görüntüsünü
    TEK bir Gemini çağrısında sentezleyip hem piyasa genelindeki düşüşün
    somut nedenlerini (en fazla 3 madde) hem de bu sentezden türetilen
    push/e-posta taslaklarını üretir — ayrı bir ikinci çağrıya gerek yok.
    Gemini bağlamda somut bir şey bulamazsa ikisi de boş döner."""
    lines = ["SON HABERLER:"]
    for item in news_items:
        lines.append(f"- [{item.get('kategori', '')}] {item.get('baslik_tr') or item.get('title', '')}")

    if sentiment:
        top = sorted(sentiment, key=lambda s: s.get("mention_sayisi", 0), reverse=True)[:5]
        lines.append("SOSYAL DUYARLILIK (en çok bahsedilen coinler):")
        for s in top:
            lines.append(f"- {s['sembol']}: {s['mention_sayisi']} gönderi, trend={s['trend']}")

    if onchain and onchain.get("stablecoin"):
        sc = onchain["stablecoin"]
        lines.append(
            f"ON-CHAIN: {sc['sembol']} küresel arzı son 24s'te %{sc['degisim_24s_yuzde']:.2f} değişti ({sc['yon']})."
        )

    raw = _call_gemini_with_prompt("\n".join(lines), MARKET_CRISIS_SYSTEM_PROMPT)
    data = _extract_json(raw)
    maddeler = [str(m).strip() for m in (data.get("maddeler") or []) if str(m).strip()][:3]
    icerik_onerisi = data.get("icerik_onerisi") or {}
    icerik_onerisi = {k: v for k, v in icerik_onerisi.items() if k in ("push", "email") and str(v).strip()}
    if not maddeler:
        icerik_onerisi = {}  # somut neden yoksa iletişim taslağı da anlamsız
    return {"maddeler": maddeler, "icerik_onerisi": icerik_onerisi}


def _compute_market_breadth() -> tuple[int, int] | None:
    """_price_cache'teki (server-side, price_fetch_loop tarafından
    doldurulan) gerçek 24s değişim verisinden kaç coinin yükselişte/
    düşüşte olduğunu sayar. Henüz hiç fiyat gelmediyse None döner."""
    up = down = 0
    for data in _price_cache.values():
        change = data.get("degisim_24s")
        if change is None:
            continue
        if change >= 0:
            up += 1
        else:
            down += 1
    return (up, down) if (up + down) else None


_market_crisis_active = False
_market_crisis_last_synthesis_at: datetime | None = None
_latest_market_crisis_payload: dict | None = None


async def market_pulse_loop() -> None:
    """Her MARKET_PULSE_CHECK_INTERVAL_SECONDS'ta (5 dk) bir piyasa
    genişliğini kontrol eder.

    HİSTEREZİS: düşenlerin oranı MARKET_PULSE_RISK_RATIO'yu (%75) aşınca
    krize girilir/sentez YENİLENİR (cooldown'a tabi); oran sadece
    MARKET_PULSE_RECOVERY_RATIO'nun (%65) ALTINA inince kriz biter. İkisi
    arasındaki bantta mevcut durum korunur — tek bir eşik kullanılsaydı
    oran sınırda dalgalandığında bar her kontrolde kriz<->normal arasında
    "titreyebilirdi".

    KALICILIK: aktif/son-sentez durumu radar.db:app_state'e yazılır —
    sunucu kriz sürerken yeniden başlarsa cooldown'u sıfırlamaz."""
    global _market_crisis_active, _market_crisis_last_synthesis_at, _latest_market_crisis_payload

    persisted_active = await asyncio.to_thread(db_get_state, "market_crisis_active")
    persisted_last_synthesis = await asyncio.to_thread(db_get_state, "market_crisis_last_synthesis_at")
    _market_crisis_active = persisted_active == "1"
    if persisted_last_synthesis:
        _market_crisis_last_synthesis_at = datetime.fromisoformat(persisted_last_synthesis)

    while True:
        breadth = await asyncio.to_thread(_compute_market_breadth)
        if breadth:
            up, down = breadth
            declining_ratio = down / (up + down)

            if declining_ratio >= MARKET_PULSE_RISK_RATIO:
                now = datetime.now(timezone.utc)
                cooldown_ok = (
                    _market_crisis_last_synthesis_at is None
                    or (now - _market_crisis_last_synthesis_at).total_seconds() >= MARKET_PULSE_SYNTHESIS_COOLDOWN_SECONDS
                )
                if cooldown_ok:
                    try:
                        news_items = await asyncio.to_thread(db_load_recent, MARKET_PULSE_NEWS_CONTEXT_LIMIT)
                        # Toplu X duyarlılık taraması kaldırıldığı için (bkz. proje
                        # notları) sentiment bağlamı artık yok — sadece haber + on-chain.
                        synthesis = await asyncio.to_thread(
                            synthesize_market_crisis, news_items, [], _latest_onchain_snapshot
                        )
                    except Exception as exc:
                        print(f"  ⚠ Piyasa krizi sentezi başarısız: {exc}")
                        synthesis = {"maddeler": [], "icerik_onerisi": {}}
                    _market_crisis_active = True
                    _market_crisis_last_synthesis_at = now
                    await asyncio.to_thread(db_set_state, "market_crisis_active", "1")
                    await asyncio.to_thread(db_set_state, "market_crisis_last_synthesis_at", now.isoformat())
                    _latest_market_crisis_payload = {
                        "aktif": True,
                        "maddeler": synthesis["maddeler"],
                        "icerik_onerisi": synthesis["icerik_onerisi"],
                        "dusen_oran": round(declining_ratio, 3),
                        "zaman": now.isoformat(),
                    }
                    await manager.broadcast({"tip": "piyasa_krizi", "veri": _latest_market_crisis_payload})
            elif declining_ratio <= MARKET_PULSE_RECOVERY_RATIO and _market_crisis_active:
                _market_crisis_active = False
                await asyncio.to_thread(db_set_state, "market_crisis_active", "0")
                _latest_market_crisis_payload = {"aktif": False}
                await manager.broadcast({"tip": "piyasa_krizi", "veri": _latest_market_crisis_payload})
            # Aradaki bant (%65-%75): mevcut durum kasıtlı olarak değiştirilmez.

        await asyncio.sleep(MARKET_PULSE_CHECK_INTERVAL_SECONDS)


# --------------------------------------------------------------------------
# FastAPI app
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    fetch_task = asyncio.create_task(fetch_loop())
    emit_task = asyncio.create_task(emit_loop())
    price_fetch_task = asyncio.create_task(price_fetch_loop())
    price_stream_task = asyncio.create_task(price_stream_loop())
    competitor_task = asyncio.create_task(competitor_coverage_loop())
    x_discovery_task = asyncio.create_task(x_discovery_loop())
    onchain_task = asyncio.create_task(onchain_loop())
    market_pulse_task = asyncio.create_task(market_pulse_loop())
    try:
        yield
    finally:
        fetch_task.cancel()
        emit_task.cancel()
        price_fetch_task.cancel()
        price_stream_task.cancel()
        competitor_task.cancel()
        x_discovery_task.cancel()
        onchain_task.cancel()
        market_pulse_task.cancel()


app = FastAPI(title="Stablex Kripto Piyasa Canlı Radarı", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return html.replace("/*__REGULATORY_LINKS__*/", json.dumps(REGULATORY_LINKS, ensure_ascii=False))


@app.get("/feed/coins.xml")
async def coins_feed() -> Response:
    """Coin fiyat/değişim verisini XML formatında dışa açar — İstihbarat
    panelinden ve WebSocket'ten TAMAMEN bağımsız, düz bir HTTP GET
    entegrasyon noktası. Her istekte _price_cache'ten (price_fetch_loop'un
    doldurduğu, gerçek CoinGecko verisi) ANLIK üretilir — diske yazılmaz,
    önbelleğe alınmaz. Prototip alan seti: sembol/fiyat/24s değişim
    (netleştirme sonrası genişletilebilir)."""
    now_iso = datetime.now(timezone.utc).isoformat()
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', f'<coinFeed guncelleme="{now_iso}">']
    for symbol in PRICE_DISPLAY_ORDER:
        data = _price_cache.get(symbol)
        if not data:
            continue
        lines.append(
            f'  <coin sembol="{xml_escape(symbol)}">'
            f'<fiyatUsd>{data["fiyat_usd"]}</fiyatUsd>'
            f'<degisim24s>{data["degisim_24s"]}</degisim24s>'
            f'</coin>'
        )
    lines.append('</coinFeed>')
    return Response(content="\n".join(lines), media_type="application/xml")


X_FEED_CACHE_TTL_SECONDS = 15 * 60  # 15 dk — sosyal sinyal bu sürede büyük değişmez;
                                     # aynı coin'e art arda/birden fazla kişi bakarsa
                                     # gereksiz tekrar ücretli arama yapılmasın diye.
_x_feed_cache: dict = {}  # (sembol, bölge) -> {"veri": [...], "zaman": datetime}
_x_feed_request_count = 0  # sunucu başladığından beri kaç GERÇEK (cache'siz) çağrı yapıldı


@app.get("/api/x-feed")
async def x_feed(sembol: str, bolge: str = "global") -> dict:
    """Talep üzerine (kullanıcı bir coin seçtiğinde) TEK bir coin için
    gerçek X gönderilerini getirir — arka planda çalışan zamanlanmış bir
    döngü DEĞİL, sadece bu istek geldiğinde Grok'a tek bir sorgu gider.
    Bu tasarım, önceki saatlik toplu tarama modelinin yavaşlık/güvenilirlik
    sorununu ortadan kaldırır (bkz. main.py:fetch_x_post_feed notu).

    15 dakikalık bir önbellek var (aynı sembol+bölge için tekrar arama
    yapmaz) ve her GERÇEK (önbellekten dönmeyen) çağrı sunucu logunda
    numaralandırılır — maliyeti takip etmek için console.x.ai'ye gitmeden
    kabaca kaç çağrı yapıldığını görebilesin diye."""
    global _x_feed_request_count
    sembol = sembol.strip().upper()
    bolge = bolge if bolge in ("tr", "global") else "global"
    if sembol not in STABLEX_COINS_SET:
        return {"sembol": sembol, "bolge": bolge, "gonderiler": [], "hata": "Bilinmeyen sembol"}

    cache_key = (sembol, bolge)
    cached = _x_feed_cache.get(cache_key)
    now = datetime.now(timezone.utc)
    if cached and (now - cached["zaman"]).total_seconds() < X_FEED_CACHE_TTL_SECONDS:
        return {"sembol": sembol, "bolge": bolge, "gonderiler": cached["veri"], "onbellek": True}

    try:
        posts = await asyncio.to_thread(fetch_x_post_feed, sembol, bolge)
    except Exception as exc:
        print(f"  ⚠ X gönderi akışı ({sembol}/{bolge}) alınamadı: {exc}")
        return {"sembol": sembol, "bolge": bolge, "gonderiler": [], "hata": "İstek başarısız oldu"}

    _x_feed_cache[cache_key] = {"veri": posts, "zaman": now}
    _x_feed_request_count += 1
    print(f"  ℹ X gönderi akışı çağrısı #{_x_feed_request_count} ({sembol}/{bolge}, {len(posts)} gönderi)")
    return {"sembol": sembol, "bolge": bolge, "gonderiler": posts, "onbellek": False}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        # Bağlantı kurulur kurulmaz, sadece BU istemciye (broadcast değil)
        # radar.db'deki son 50 haberi gönder — sayfa yenilendiğinde akış
        # boş başlamaz.
        history = await asyncio.to_thread(db_load_recent, 50)
        if history:
            await websocket.send_json({"tip": "gecmis_haberler", "veri": history})

        competitor_snapshot = await asyncio.to_thread(db_load_competitor_coverage)
        if competitor_snapshot:
            await websocket.send_json({"tip": "rakip_kapsami", "veri": competitor_snapshot})

        x_discovery_snapshot = await asyncio.to_thread(db_load_x_discovery)
        if x_discovery_snapshot or _latest_x_rakip_sentiment:
            await websocket.send_json({
                "tip": "x_kesif",
                "veri": {"trend_coinler": x_discovery_snapshot, "rakip_sentiment": _latest_x_rakip_sentiment},
            })

        if _latest_onchain_snapshot:
            await websocket.send_json({"tip": "onchain", "veri": _latest_onchain_snapshot})

        if _latest_market_crisis_payload and _latest_market_crisis_payload.get("aktif"):
            await websocket.send_json({"tip": "piyasa_krizi", "veri": _latest_market_crisis_payload})

        while True:
            # İstemciden mesaj beklemiyoruz; bağlantının canlı kalmasını
            # sağlamak ve kopuşu WebSocketDisconnect ile yakalamak için
            # gelen her şeyi (varsa ping/pong) yutuyoruz.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        # WebSocketDisconnect DIŞINDA bir hata (ör. history gönderilirken)
        # yakalanmazsa finally'ye hiç düşmeden soket manager.active'te askıda
        # kalırdı — bu yüzden geniş except + finally birlikte kullanılıyor.
        print(f"  ⚠ WebSocket bağlantısında beklenmeyen hata: {exc}")
    finally:
        manager.disconnect(websocket)
