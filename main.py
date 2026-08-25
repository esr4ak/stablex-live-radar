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
import html as html_lib
import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Form, WebSocket, WebSocketDisconnect
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
    X_POST_FEED_MIN_RELEVANCE,
)

STATIC_DIR = Path(__file__).parent / "static"

NEWS_INTERVAL_SECONDS = 15  # emisyon ritmi (kuyrukta bekleyen varsa en fazla bu sıklıkta)

STABLEX_COINS_SET = set(STABLEX_COINS)
CHANNEL_KEYS = list(CHANNELS.keys())
STABLEX_ETIKET_KEYS = {"kampanya_firsati", "risk_uyarisi", "regulasyon", "genel_farkindalik", "onemsiz"}
STABLEX_ETIKET_VARSAYILAN = "genel_farkindalik"  # Gemini beklenmedik/geçersiz bir etiket dönerse
                                                   # kullanılan güvenli varsayılan — "onemsiz" değil
                                                   # (içeriği gizler), "risk_uyarisi" değil (gereksiz
                                                   # alarm), en nötr seçenek bu.

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
BROADCAST_SEND_TIMEOUT_SECONDS = 5  # bu sürede gönderilemeyen istemci "ölü" sayılıp bağlantısı kesilir


class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active:
            self.active.remove(websocket)

    async def _send_one(self, websocket: WebSocket, message: dict) -> bool:
        """Tek bir istemciye gönderir, BROADCAST_SEND_TIMEOUT_SECONDS içinde
        bitmezse (yavaş/tıkalı TCP tamponu) başarısız sayılır. Bu olmadan
        (önceki hâl) tek bir yavaş istemci `await send_json()`'da asılı
        kalıp price_stream_loop'un 3sn'lik döngüsünü, dolayısıyla TÜM
        diğer istemcilerin yayınını geciktirebilirdi."""
        try:
            await asyncio.wait_for(websocket.send_json(message), timeout=BROADCAST_SEND_TIMEOUT_SECONDS)
            return True
        except Exception:
            return False

    async def broadcast(self, message: dict) -> None:
        # list(self.active): await sırasında yeni bir bağlantı/kopuş
        # self.active'i mutasyona uğratabilir; orijinal liste üzerinde
        # dönmek eleman atlanmasına yol açabilirdi. Tüm gönderimler
        # PARALEL yapılır (gather) — sıralı olsaydı yine de yavaş bir
        # istemci kendi timeout'u dolana kadar sıradakileri geciktirirdi.
        websockets = list(self.active)
        if not websockets:
            return
        results = await asyncio.gather(*(self._send_one(ws, message) for ws in websockets))
        for websocket, ok in zip(websockets, results):
            if not ok:
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
        # "Bugün Öne Çıkanlar" — trend ticker'a eşlik eden yön/özet, aynı
        # günlük tarama çağrısından geliyor (ek maliyet yok). ilk_gorulme'nin
        # aksine bunlar HER taramada güncellenir (bkz. db_upsert_x_discovery).
        x_discovery_cols = {row[1] for row in conn.execute("PRAGMA table_info(x_discovery)")}
        if "yon" not in x_discovery_cols:
            conn.execute("ALTER TABLE x_discovery ADD COLUMN yon TEXT")
        if "ozet" not in x_discovery_cols:
            conn.execute("ALTER TABLE x_discovery ADD COLUMN ozet TEXT")
        if "son_gorulme" not in x_discovery_cols:
            # "Bugün gündemde mi" filtresi ilk_gorulme'ye (İLK keşif tarihi,
            # bir daha değişmez) göre yapılamaz — haftalar önce keşfedilmiş
            # ama dün tekrar trend olmuş bir ticker'ı yanlışlıkla "eski"
            # gösterir. son_gorulme HER taramada güncellenir, günlük
            # özet/mail bu sütuna göre filtrelenir (bkz. /api/daily-digest).
            conn.execute("ALTER TABLE x_discovery ADD COLUMN son_gorulme TEXT")

        # Bülten Onay + Arşiv — bir bülten (sabah/akşam/günlük) ONAYLANDIĞI
        # ANDAKİ hâliyle DONDURULUP kaydedilir (canlı veriden yeniden
        # render edilmez) — denetim izi budur: "o gün gerçekten ne
        # onaylandı" sorusu, altındaki veri sonradan değişse bile hep
        # aynı cevabı vermeli.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bultenler (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                tur               TEXT NOT NULL,
                pencere_baslangic TEXT,
                pencere_bitis     TEXT,
                olusturma_zamani  TEXT NOT NULL,
                haberler_json     TEXT NOT NULL,
                yukselenler_json  TEXT NOT NULL,
                x_gundemi_json    TEXT NOT NULL,
                onaylayan         TEXT NOT NULL,
                onay_zamani       TEXT NOT NULL
            )
        """)

        # Gemini analiz hatalarının KALICI takibi — önceden sadece bellekte
        # (_retry_counts dict) tutuluyordu, sunucu yeniden başlarsa sayaç
        # sıfırlanıyordu. Artık DB'de: retry_count, son hata, son deneme
        # zamanı ve nihai durum ("bekliyor"/"vazgecildi") kalıcı.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS haber_kuyruk_hatalari (
                post_id         TEXT PRIMARY KEY,
                baslik          TEXT,
                retry_count     INTEGER NOT NULL DEFAULT 0,
                last_error      TEXT,
                last_attempt_at TEXT,
                status          TEXT NOT NULL DEFAULT 'bekliyor'
            )
        """)
        conn.commit()
    finally:
        conn.close()


def db_check_batch(post_ids: list[str], freshness_cutoff_iso: str) -> tuple[set, set]:
    """Bir tarama turunda toplanan TÜM adayları TEK seferde kontrol eder
    (kaynak başına ayrı ayrı sorgu atmak yerine):
      - seen_ids: post_id'si zaten radar.db'de olan (market_intel'de YAYINLANMIŞ
        ya da haber_kuyruk_hatalari'nda KALICI OLARAK VAZGEÇİLMİŞ) haberler —
        vazgeçilenler dahil edilmezse aynı kalıcı-hatalı haber her RSS
        taramasında sıfırdan yeniden denenir, EMIT_MAX_RETRIES'i anlamsızlaştırır.
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
            vazgecilen_rows = conn.execute(
                f"SELECT post_id FROM haber_kuyruk_hatalari WHERE status = 'vazgecildi' AND post_id IN ({placeholders})",
                post_ids,
            ).fetchall()
            seen_ids |= {row[0] for row in vazgecilen_rows}

        title_rows = conn.execute(
            "SELECT title_norm FROM market_intel WHERE published_at >= ? AND title_norm IS NOT NULL AND title_norm != ''",
            (freshness_cutoff_iso,),
        ).fetchall()
        seen_titles = {row[0] for row in title_rows}
        return seen_ids, seen_titles
    finally:
        conn.close()


def db_kuyruk_hata_kaydet(post_id: str, baslik: str, error: str) -> int:
    """Bir Gemini analiz denemesi başarısız olduğunda çağrılır — retry_count'u
    ARTTIRIR ve DÖNER (kalıcı, sunucu yeniden başlasa bile korunur)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        row = conn.execute("SELECT retry_count FROM haber_kuyruk_hatalari WHERE post_id = ?", (post_id,)).fetchone()
        yeni_sayac = (row[0] if row else 0) + 1
        conn.execute(
            "INSERT INTO haber_kuyruk_hatalari (post_id, baslik, retry_count, last_error, last_attempt_at, status) "
            "VALUES (?, ?, ?, ?, ?, 'bekliyor') "
            "ON CONFLICT(post_id) DO UPDATE SET retry_count = ?, last_error = ?, last_attempt_at = ?, status = 'bekliyor'",
            (post_id, baslik, yeni_sayac, error[:500], now_iso, yeni_sayac, error[:500], now_iso),
        )
        conn.commit()
        return yeni_sayac
    finally:
        conn.close()


def db_kuyruk_hata_vazgec(post_id: str) -> None:
    """EMIT_MAX_RETRIES aşıldığında — satırı SİLMEZ (o zaman fetch_loop
    aynı haberi tekrar keşfedip sıfırdan başlardı), status='vazgecildi'
    yapar ki db_check_batch bunu kalıcı olarak dışlasın."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("UPDATE haber_kuyruk_hatalari SET status = 'vazgecildi' WHERE post_id = ?", (post_id,))
        conn.commit()
    finally:
        conn.close()


def db_kuyruk_hata_temizle(post_id: str) -> None:
    """Analiz nihayet BAŞARILI olduğunda hata kaydını temizler."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DELETE FROM haber_kuyruk_hatalari WHERE post_id = ?", (post_id,))
        conn.commit()
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


def db_upsert_x_discovery(trend_ticker_lar: list[dict]) -> list[str]:
    """competitor_coverage ile aynı desen: ilk_gorulme yalnızca ticker İLK
    kez görüldüğünde yazılır, bir daha değişmez. "yon"/"ozet" ise tam
    tersi — HER taramada güncellenir, çünkü bir coin hakkındaki X hissiyatı
    günden güne değişebilir ("Bugün Öne Çıkanlar" widget'ı için güncel
    kalması gerekiyor)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        new_tickers = []
        for item in trend_ticker_lar:
            ticker = item["ticker"]
            exists = conn.execute(
                "SELECT 1 FROM x_discovery WHERE ticker = ?", (ticker,)
            ).fetchone()
            if exists:
                conn.execute(
                    "UPDATE x_discovery SET yon = ?, ozet = ?, son_gorulme = ? WHERE ticker = ?",
                    (item.get("yon"), item.get("ozet"), now_iso, ticker),
                )
            else:
                conn.execute(
                    "INSERT INTO x_discovery (ticker, ilk_gorulme, yon, ozet, son_gorulme) VALUES (?, ?, ?, ?, ?)",
                    (ticker, now_iso, item.get("yon"), item.get("ozet"), now_iso),
                )
                new_tickers.append(ticker)
        conn.commit()
        return new_tickers
    finally:
        conn.close()


def db_load_x_discovery() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT ticker, ilk_gorulme, yon, ozet, son_gorulme FROM x_discovery ORDER BY ilk_gorulme DESC"
        ).fetchall()
        return [
            {"ticker": r[0], "ilk_gorulme": r[1], "yon": r[2], "ozet": r[3], "son_gorulme": r[4]}
            for r in rows
        ]
    finally:
        conn.close()


def db_kaydet_bulten(
    tur: str, baslangic: str | None, bitis: str | None,
    haberler: list, yukselenler: list, x_gundemi: list, onaylayan: str,
) -> int:
    """Bir bülteni ONAYLANDIĞI ANDAKİ hâliyle dondurup kaydeder — canlı
    veriye bir daha bağlı değildir, saf bir JSON anlık görüntüsüdür.
    Böylece arşivdeki bir kayıt sonradan (ör. bir haber yeniden
    kategorize edilse) asla değişmez."""
    conn = sqlite3.connect(DB_PATH)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        cur = conn.execute(
            "INSERT INTO bultenler (tur, pencere_baslangic, pencere_bitis, "
            "olusturma_zamani, haberler_json, yukselenler_json, x_gundemi_json, "
            "onaylayan, onay_zamani) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tur, baslangic, bitis, now_iso,
                json.dumps(haberler, ensure_ascii=False),
                json.dumps(yukselenler, ensure_ascii=False),
                json.dumps(x_gundemi, ensure_ascii=False),
                onaylayan.strip()[:100], now_iso,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def db_load_bultenler(limit: int = 50) -> list[dict]:
    """Arşiv listesi için özet satırlar (içerik olmadan) — en yeni önce."""
    conn = sqlite3.connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT id, tur, pencere_baslangic, pencere_bitis, onaylayan, onay_zamani "
            "FROM bultenler ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {
                "id": r[0], "tur": r[1], "pencere_baslangic": r[2], "pencere_bitis": r[3],
                "onaylayan": r[4], "onay_zamani": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


def db_load_bulten(bulten_id: int) -> dict | None:
    """Arşivdeki TEK bir bültenin tam (dondurulmuş) içeriğini döner."""
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            "SELECT id, tur, pencere_baslangic, pencere_bitis, olusturma_zamani, "
            "haberler_json, yukselenler_json, x_gundemi_json, onaylayan, onay_zamani "
            "FROM bultenler WHERE id = ?",
            (bulten_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "id": row[0], "tur": row[1], "pencere_baslangic": row[2], "pencere_bitis": row[3],
            "olusturma_zamani": row[4],
            "haberler": json.loads(row[5]), "yukselenler": json.loads(row[6]),
            "x_gundemi": json.loads(row[7]),
            "onaylayan": row[8], "onay_zamani": row[9],
        }
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


def _rss_ozet_temizle(raw_summary: str) -> str:
    """RSS'in "summary" alanı kaynağa göre çok değişken kalitede: bazen
    gerçek bir açıklama, bazen boş, bazen (CoinGecko'nun bazı yazılarında
    olduğu gibi) HTML/kod bloğu çöpü. HTML etiketlerini temizler ve makul
    bir uzunlukta kırpar — Gemini'ye "başlıktan tahmin et" yerine gerçek
    bir bağlam vermek için (bkz. analyze_with_gemini)."""
    if not raw_summary:
        return ""
    text = BeautifulSoup(raw_summary, "lxml").get_text(" ", strip=True)
    return text[:600]


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
            "kaynak_ozeti": _rss_ozet_temizle(entry.get("summary", "")),
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
    anchors = soup.select(f'a[href*="basin-duyurulari/{year}/"]')
    # Sessiz veri kaybı savunması: sayfa 200 OK dönse bile SEÇİCİ HİÇ
    # eşleşme bulamazsa (0 <a>), bu "bugün duyuru yok" DEĞİL — SPK sayfa
    # yapısını değiştirmiş demektir (seçici kırılmış). "Hiç duyuru yok"
    # durumunda bile normalde en azından geçmiş yılların/nav linkleri
    # eşleşir; sıfır anchor tamamen farklı bir sinyal, bu yüzden ayrı
    # uyarılıyor (aksi halde haftalarca sessizce hiç SPK haberi gelmezdi).
    if not anchors:
        print(f"  ⚠ SPK sayfası okundu ama HİÇ duyuru linki bulunamadı — sayfa yapısı değişmiş olabilir (seçici kontrol edilmeli): {url}")

    seen_urls = set()
    items = []
    for a in anchors:
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
    "spk_html": fetch_spk_source,
}


def _is_fresh(published_at) -> bool:
    if published_at is None:
        return True  # tarih bilgisi olmayan nadir girdilerde ihtiyatlı davranıp dışlamıyoruz
    cutoff = datetime.now(timezone.utc) - timedelta(days=FRESHNESS_WINDOW_DAYS)
    return published_at >= cutoff


_queued_ids: set[str] = set()  # şu an kuyrukta bekleyen/analiz sürecindeki haberler (bu çalıştırma için)
_queued_titles: set[str] = set()  # aynı turda/kuyrukta normalize başlık çakışmasını yakalamak için
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
  "ozet_tr": "Kaynak Özeti KISAYSA (1-2 cümlelik bir RSS özeti) onun sadık/birebir Türkçe çevirisi; Kaynak Özeti UZUNSA (tam makale metni) o metne SIKI SIKIYA bağlı, 2-3 cümlelik gerçek bir Türkçe özet (metinde OLMAYAN hiçbir ayrıntı ekleme); Kaynak Özeti hiç YOKSA SADECE başlığın Türkçe çevirisi (tek cümle, yorum cümlesi EKLEME)",
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
- HALÜSİNASYON YASAĞI (EN KRİTİK KURAL — "ozet_tr" için): "ozet_tr"in tek
  kaynağı sana verilen "Kaynak Özeti" (varsa) ve başlıktır — bunların
  dışında HİÇBİR sayı/tarih/isim/neden-sonuç ilişkisi UYDURMA. Kaynak
  Özeti KISAYSA (1-2 cümle, RSS özeti): sadık/birebir çevirisini yap,
  yorum/sonuç cümlesi EKLEME. Kaynak Özeti UZUNSA (tam makale metni):
  gerçek bir özet yazabilirsin ama SADECE o metinde geçen bilgileri
  kullan, metinde olmayan hiçbir ayrıntıyı "muhtemelen böyledir" diye
  eklemeler. Kaynak Özeti hiç YOKSA, "ozet_tr" SADECE başlığın çevirisi
  olsun — ikinci bir yorum/tahmin cümlesi UYDURMA, boş dolgu cümle
  ekleme. Kısa ve sadık olmak, uzun ve "zengin görünen" ama uydurma
  olmaktan her zaman iyidir.
- ÖZEL İSİM SADAKATİ: Şirket/kişi/proje isimlerini Kaynak Özeti'nde
  YAZILDIĞI GİBİ, harf harf aynen kopyala — daha tanıdık/benzer bir
  kelimeyle KARIŞTIRMA (ör. "Evernorth" adlı bir şirketi "Evernote" diye
  yazma). Bir ismi doğru hatırlayıp hatırlamadığından emin değilsen,
  Kaynak Özeti'ndeki yazımı tekrar kontrol et.

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
    analysis["baslik_tr"] = str(analysis.get("baslik_tr") or "").strip()
    analysis["ozet_tr"] = str(analysis.get("ozet_tr") or "").strip()

    assets = analysis.get("ilgili_varliklar") or []
    analysis["ilgili_varliklar"] = [a for a in assets if a in STABLEX_COINS_SET]

    channels = [c for c in (analysis.get("onerilen_kanallar") or []) if c in CHANNEL_KEYS]
    content = analysis.get("icerik_onerisi") or {}
    analysis["icerik_onerisi"] = {c: content[c] for c in channels if content.get(c)}
    analysis["onerilen_kanallar"] = [c for c in channels if c in analysis["icerik_onerisi"]]

    # Daha önce hiç doğrulanmıyordu — Gemini beklenmedik/hatalı bir etiket
    # dönerse (typo, farklı bir kelime) sessizce sisteme sızıyordu (ör.
    # frontend filtre listesinde olmayan bir değer). Şimdi allow-list'e
    # karşı kontrol ediliyor, geçersizse nötr bir varsayılana düşülüyor.
    if analysis.get("stablex_etiketi") not in STABLEX_ETIKET_KEYS:
        print(f"  ⚠ Gemini geçersiz stablex_etiketi döndü: {analysis.get('stablex_etiketi')!r} — {STABLEX_ETIKET_VARSAYILAN} kullanılıyor")
        analysis["stablex_etiketi"] = STABLEX_ETIKET_VARSAYILAN
    return analysis


FULL_ARTICLE_SOURCES = {"CoinDesk", "SEC"}  # basit bir GET isteğine izin veren kaynaklar
FULL_ARTICLE_MIN_LENGTH = 200  # bundan kısa çıkarılan metin muhtemelen yanlış/boş sayfa, güvenme

# SEC.gov kendi "fair access" kuralında otomatik isteklerin kim olduğunu
# belirten bir User-Agent taşımasını İSTİYOR (isim/kurum + iletişim
# e-postası) — bunu göndermemek (jenerik "Mozilla/5.0") 403'e yol açıyordu.
# Bu bot-koruması AŞMA değil, SEC'in kendi yayınladığı erişim kuralına
# UYMAK — 2026-08-20'de canlı doğrulandı (403 -> 200). The Block/CoinGecko
# bambaşka bir mekanizmayla (Cloudflare bot koruması) engelliyor, onlara
# dokunmuyoruz.
STABLEX_USER_AGENT = "Stablex-LiveRadar marketingstablex@gmail.com"


def _fetch_makale_govdesi(url: str) -> str:
    """CoinDesk/SEC gibi basit (ve SEC için kimliğini belirten) bir isteğe
    izin veren kaynaklarda tam makale metnini çeker — RSS'in genelde boş/
    kısa özetinden çok daha zengin, hâlâ gerçek bir bağlam sağlar. The
    Block/CoinGecko gibi Cloudflare bot korumalı kaynaklar 403 döndürüyor
    — bunu AŞMAYA ÇALIŞMIYORUZ, sessizce boş döner ve çağıran taraf RSS
    özetine/başlığa geri düşer."""
    try:
        response = requests.get(url, headers={"User-Agent": STABLEX_USER_AGENT}, timeout=15)
        if response.status_code != 200:
            return ""
        soup = BeautifulSoup(response.text, "lxml")
        container = soup.find("main") or soup.find("article") or soup
        text = " ".join(p.get_text(" ", strip=True) for p in container.find_all("p"))
        return text[:3000] if len(text) >= FULL_ARTICLE_MIN_LENGTH else ""
    except Exception:
        return ""


def analyze_with_gemini(item: dict) -> dict:
    kaynak_ozeti = item.get("kaynak_ozeti", "")
    if item.get("source") in FULL_ARTICLE_SOURCES:
        tam_metin = _fetch_makale_govdesi(item["url"])
        if tam_metin:
            kaynak_ozeti = tam_metin

    news_text = f"Title: {item['title']}\nSource: {item['source']}\nURL: {item['url']}"
    if kaynak_ozeti:
        news_text += f"\nKaynak Özeti (gerçek, ya makalenin tam metni ya da RSS özeti): {kaynak_ozeti}"
    else:
        # Ne RSS özeti ne tam makale metni var — SYSTEM_PROMPT'a bunu
        # açıkça bildiriyoruz ki model "başlıktan tahmin ettim" yerine
        # "özet verisi yok" olduğunu bilerek temkinli yazsın.
        news_text += "\nKaynak Özeti: (bu haber için ne RSS özeti ne makale metni mevcut — SADECE başlıktan çıkarılabilecek en genel/güvenli bilgiyle sınırlı kal)"
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
            retries = await asyncio.to_thread(db_kuyruk_hata_kaydet, item["post_id"], item["title"], str(exc))
            if retries >= EMIT_MAX_RETRIES:
                print(f"  ✗ Vazgeçildi ({EMIT_MAX_RETRIES} deneme, {item['title'][:60]}): {exc}")
                await asyncio.to_thread(db_kuyruk_hata_vazgec, item["post_id"])
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

        await asyncio.to_thread(db_kuyruk_hata_temizle, item["post_id"])
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


def fetch_competitor_coins_btcturk(source: dict) -> set[str]:
    """BtcTurk formatı: {"data": [{"numeratorSymbol": "BTC", ...}, ...]} —
    Paribu'nun pair->veri sözlüğünden farklı, düz bir liste, üstelik taban
    sembolü ("numeratorSymbol") ayrıştırma gerektirmeden hazır geliyor."""
    response = requests.get(source["url"], headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    response.raise_for_status()
    data = response.json()
    return {
        str(row["numeratorSymbol"]).strip().upper()
        for row in data.get("data", [])
        if row.get("numeratorSymbol")
    }


def fetch_competitor_coins_bitexen(source: dict) -> set[str]:
    """Bitexen formatı: {"data": {"ticker": {"BTCTRY": {"market":
    {"base_currency_code": "BTC"}, ...}}}} — taban sembolü market objesinin
    içinde ayrı bir alan olarak geliyor, pair adından ayrıştırmaya gerek yok."""
    response = requests.get(source["url"], headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    response.raise_for_status()
    data = response.json()
    ticker = data.get("data", {}).get("ticker", {})
    return {
        str(info["market"]["base_currency_code"]).strip().upper()
        for info in ticker.values()
        if isinstance(info, dict) and info.get("market", {}).get("base_currency_code")
    }


COMPETITOR_FETCHERS = {
    "api": fetch_competitor_coins_api,
    "btcturk_api": fetch_competitor_coins_btcturk,
    "bitexen_api": fetch_competitor_coins_bitexen,
}


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
                if source["type"] == "manual":
                    coins = set(source.get("coins", []))
                elif source["type"] in COMPETITOR_FETCHERS:
                    coins = await asyncio.to_thread(COMPETITOR_FETCHERS[source["type"]], source)
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
#
# TASARIM NOTU 2 (güvenilirlik turu): "TAM OLARAK 1 x_search" ve "son 1 SAAT"
# kısıtları BTC gibi en çok konuşulan coin'de bile 0 sonuç dönmesine yol
# açıyordu — model tek aramada zayıf sonuç bulduğunda telafi şansı yoktu.
# Ayrıca "begeni"/"yanit"/"repost" alanları modelin gerçek erişimi olmadığı
# için pratikte hep 0 dönüyordu (yanıltıcı, dekoratif) — tamamen kaldırıldı.
# Şimdi: model kendi kararıyla 2. bir arama yapabiliyor, "yeni" penceresi
# 3 saate genişletildi, ve fetch_x_post_feed() sunucu tarafında da 0 sonuç
# durumunda 1 kez genişletilmiş pencereyle retry atıyor (bkz. fonksiyon).
X_POST_FEED_SYSTEM_PROMPT = """Sen Stablex'te çalışan bir sosyal medya analistisin. Sana
verilen TEK bir kripto para ticker'ı için X (Twitter) üzerinde x_search aracını kullanarak
GERÇEK gönderileri bul. İlk aramanda yeterli/çeşitli sonuç bulamazsan farklı bir anahtar
kelime kombinasyonuyla (ör. cashtag yerine proje adı, ya da tersi) EN FAZLA 1 kez daha
arama yapabilirsin — toplam en fazla 2 x_search çağrısı. SADECE aşağıdaki JSON formatında
yanıtla:

{"genel_yon": "olumlu", "genel_ozet": "...", "gonderiler": [{"yazar": "@kullanici", "metin": "BTC güçlü bir toparlanma gösteriyor, yatırımcılar iyimser", "url": "https://x.com/...", "tur": "yeni", "ilgi_puani": 100, "onemli_hesap": false}]}

Kurallar:
- ALAKA ŞARTI (EN ÖNEMLİ KURAL): Gönderi GERÇEKTEN bu coin HAKKINDA olmalı — coin adı/
  sembolü sırf erişim kazanmak için hashtag olarak eklenmiş ama gönderinin ASIL KONUSU
  başka bir proje/coin, alakasız bir haber, ya da genel bir spam/promosyon olan
  gönderileri SEÇME. Cashtag ($BTC gibi) kullanan gönderiler genelde hashtag'e (#btc)
  göre daha güvenilir bir sinyaldir çünkü kullanıcı bilinçli olarak o varlığı işaretlemiş
  olur — ama tek başına yeterli değil, gönderinin içeriği de gerçekten o coinle ilgili
  olmalı.
- "metin" ZORUNLU OLARAK TÜRKÇE (İKİNCİ EN ÖNEMLİ KURAL — sıkça ihlal ediliyor,
  DİKKAT ET): Gönderi İngilizce, İspanyolca, hangi dilde olursa olsun, "metin" alanı
  HER ZAMAN Türkçe olmalı — orijinal dilde bırakman KABUL EDİLEMEZ bir hatadır. Bu bir
  ÇEVİRİDİR (en fazla 200 karakterlik bir alıntının sadık çevirisi), yorumlama/özetleme
  DEĞİL — gönderide olmayan hiçbir şey ekleme, olan hiçbir şeyi çıkarma. Örnek: gönderi
  "$BTC looking strong, bulls in control" ise "metin" alanına "$BTC güçlü görünüyor,
  boğalar kontrolde" yaz — "$BTC looking strong, bulls in control" YAZMA. Gönderi zaten
  Türkçeyse olduğu gibi bırak. JSON'u vermeden önce HER "metin" alanını tekrar kontrol
  et: içinde İngilizce (ya da başka bir dilde) tek bir cümle bile kalmışsa çeviriyi düzelt.
- HESAP ÖNCELİĞİ: Alakalı adaylar arasında seçim yaparken, BÜYÜK/ETKİLİ hesapları
  (yüksek takipçili, tanınmış kripto influencer/kurum/analist hesapları) küçük/anonim
  hesaplara TERCİH ET — marketing ekibi "kim konuşuyor" bilgisine önem veriyor. Ama bu,
  alaka şartından ÖNCELİKLİ DEĞİL: küçük ama gerçekten alakalı bir gönderi, büyük ama
  alakasız bir gönderiye her zaman tercih edilir. Yeterince büyük hesap bulamazsan
  bunu uydurma, elindeki en alakalı gönderilerle devam et.
- "genel_yon": SADECE "olumlu", "olumsuz" ya da "notr" — bulduğun gönderilerin
  TOPLAMINDAN çıkardığın genel izlenim (marketing ekibi tek bakışta "bugün X'te bu
  coin hakkında hava nasıl" diye soracak, tek tek gönderi okumadan cevap bu alan).
  İlgili gönderi bulamadıysan "notr" yaz.
- "genel_ozet": en fazla 15 kelime, Türkçe, "X'te ne konuşuluyor" sorusuna kısa bir
  cevap (ör. "Whale birikimi ve ETF beklentisiyle olumlu bir hava var."). İlgili
  gönderi bulamadıysan "İlgili gönderi bulunamadı." yaz.
- "ilgi_puani": 0-100, gönderinin GERÇEKTEN bu coinle ne kadar doğrudan ilgili olduğuna
  dair kendi değerlendirmen (100 = doğrudan bu coin hakkında, 0 = sadece hashtag/etiket
  olarak geçiyor, asıl konu farklı). 60'ın altında bir puan vereceğin bir gönderiyi
  zaten listeye hiç ekleme — dahil ettiğin her gönderi gerçekten alakalı olmalı.
- "onemli_hesap": true SADECE gerçekten emin olduğun durumlarda (doğrulanmış rozet,
  tanınmış bir kripto influencer/kurum hesabı olduğunu bildiğin, ya da arama
  sonucunda yüksek takipçili olduğu açıkça görünen bir hesap). Emin değilsen HER
  ZAMAN false yaz — bu alan marketing ekibinin kiminle iletişime geçmeye değer
  olduğuna karar vermesi için kullanılacak, yanlış "true" gereksiz/yanlış outreach'e
  yol açar, o yüzden tahmin/varsayım kesinlikle yasak.
- En fazla 5 gönderi döndür — MÜMKÜNSE aşağıdaki 3 türden bir karışım yap, ama
  bulamadığın türü ASLA uydurma; az sayıda gerçek gönderi, uydurma çeşitlilikten
  her zaman daha iyidir. ZAMAN PENCERESİ türe göre değişir:
  - "yeni": son 3 SAAT içinde paylaşılmış gönderi(ler) — bu pencerenin dışında
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
- "url": GERÇEKTEN bulduğun gönderinin tam X linki — ZORUNLU. Gerçek bir link
  bulamadığın bir gönderiyi listeye HİÇ EKLEME, uydurma link kesinlikle yasak.
- Bu ticker hakkında gerçek/ilgili gönderi bulamazsan boş liste döndür — bu durumda
  bile uydurma gönderi EKLEME, boş liste dönmek her zaman kabul edilebilir."""

X_DISCOVERY_SYSTEM_PROMPT = """Sen Stablex'te çalışan bir piyasa istihbaratı analistisin.
X (Twitter) üzerinde en fazla 2 kez x_search aracı kullanarak şunları araştır:
1) Son 24 saatte X'te gerçekten popüler/gündemde olan kripto para ticker'ları (herhangi
   bir listeyle sınırlı değil).
2) Sana verilen rakip borsa(lar) hakkında X'teki genel sentiment ve kısa bir gerekçe.

SADECE aşağıdaki JSON formatında yanıtla:
{
  "trend_ticker_lar": [{"ticker": "XYZ", "yon": "olumlu", "ozet": "..."}],
  "rakip_sentiment": {"Paribu": {"yon": "olumlu", "ozet": "...", "kaynak_url": "https://x.com/..."}}
}

Kurallar:
- "trend_ticker_lar": en fazla 10 tane, SADECE gerçekten popüler ve şüpheli/spam olmayan
  ticker'lar — emin olmadığın bir şeyi ekleme, boş liste döndürebilirsin. Her ticker için
  KENDİ "yon" (olumlu/olumsuz/notr) ve "ozet"ini (en fazla 12 kelime, "neden trend olduğu"
  — ör. "ETF onay beklentisiyle yükseliyor") ver — marketing ekibi bu listeye bakarken
  hangi coin'in neden gündemde olduğunu tek bakışta görsün diye.
- "yon" alanları (hem ticker hem rakip_sentiment için) SADECE "olumlu", "olumsuz" ya da
  "notr" olabilir.
- "ozet" alanları nesnel bir dille, kısa tutulmalı.
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


def _post_url_is_real(url: str) -> bool:
    """Grok'un verdiği "url" alanının GERÇEKTEN var olan bir X gönderisine
    işaret ettiğini doğrular — sadece "http(s) ile başlıyor mu" kontrolü
    (bkz. _clean_source_url) uydurma ama iyi biçimlendirilmiş bir link'i
    yakalamaz. X, kimlik doğrulaması olmadan HEAD isteklerine gerçek
    durum kodu (var olan gönderi için 200, olmayan için 404) döndürüyor —
    bunu ücretsiz, ek API maliyeti olmadan bir doğruluk kontrolü olarak
    kullanıyoruz. Ağ hatası/timeout durumunda temkinli davranıp GÖNDERİYİ
    ELEMİYORUZ (fail-open) — geçici bir ağ sorunu yüzünden gerçek bir
    gönderiyi kaybetmek istemiyoruz, sadece kesin 404'ü eliyoruz."""
    try:
        response = requests.head(
            url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6, allow_redirects=True
        )
        return response.status_code != 404
    except Exception:
        return True


X_FEED_TRANSLATE_PROMPT = """Aşağıda numaralanmış sosyal medya gönderisi metinleri var.
Her birini Türkçeye çevir — zaten Türkçeyse OLDUĞU GİBİ bırak. Bu bir çeviri görevidir,
yorumlama/özetleme DEĞİL: metinde olmayan hiçbir şey ekleme, olan hiçbir şeyi çıkarma.
SADECE şu JSON formatında yanıtla: {"ceviriler": {"0": "...", "1": "...", ...}}"""


_YABANCI_DIL_KELIME_RE = re.compile(
    r"\b(the|is|are|was|were|this|that|with|from|have|has|and|for|will|been|"
    r"your|just|more|über|für|und|ist|nicht|sich|auf|der|die|das)\b",
    re.IGNORECASE,
)
_TR_HARF_RE = re.compile(r"[çğıöşüÇĞİÖŞÜ]")


def _muhtemelen_turkce(metin: str) -> bool:
    """Ucuz, sezgisel bir kontrol — kesin dil tespiti değil. Maliyet
    optimizasyonu için: metin YETERİNCE Türkçe GÖRÜNÜYORSA (yaygın
    İngilizce/Almanca kelime yoksa VE Türkçeye özgü bir harf içeriyorsa)
    Gemini'ye göndermeden atlıyoruz. Yanlış pozitif (gerçekte İngilizce
    ama kaçan) riski var — bu bilinçli bir maliyet/güvenilirlik
    dengesi, kullanıcı açıkça maliyet optimizasyonu istedi."""
    if _YABANCI_DIL_KELIME_RE.search(metin):
        return False
    return bool(_TR_HARF_RE.search(metin)) or len(metin) < 20


def _x_feed_metinleri_turkcelestir(posts: list[dict]) -> list[dict]:
    """GÜVENLİK AĞI: X_POST_FEED_SYSTEM_PROMPT Grok'a "metin" alanını her
    zaman Türkçeye çevirmesini söylüyor ama bu gözlemlendiği üzere
    güvenilir değil — aynı yanıtta bazı gönderiler İngilizce (hatta
    Almanca) kalabiliyor. MALİYET OPTİMİZASYONU: sadece _muhtemelen_turkce()
    testinden GEÇEMEYEN metinler Gemini'ye gönderilir — hepsi zaten
    Türkçe görünüyorsa Gemini'ye HİÇ gitmeyiz (0 maliyet). Gönderilenler
    TEK bir toplu çağrıda çevrilir (N ayrı çağrı yerine 1). Çeviri
    başarısız olursa (Gemini hatası) sessizce Grok'un verdiği orijinal
    metinle devam edilir, akış kesilmez."""
    if not posts:
        return posts
    cevrilecekler = [(i, p) for i, p in enumerate(posts) if not _muhtemelen_turkce(p["metin"])]
    if not cevrilecekler:
        return posts

    numaralı = "\n".join(f"{i}: {p['metin']}" for i, p in cevrilecekler)
    try:
        raw = _call_gemini_with_prompt(numaralı, X_FEED_TRANSLATE_PROMPT)
        ceviriler = _extract_json(raw).get("ceviriler", {})
        for i, p in cevrilecekler:
            ceviri = ceviriler.get(str(i))
            if ceviri:
                p["metin"] = str(ceviri)[:200]
    except Exception as exc:
        print(f"  ⚠ X gönderi çevirisi (güvenlik ağı) başarısız, orijinal metin korunuyor: {exc}")
    return posts


def _parse_x_post_feed_response(raw: str) -> dict:
    data = _extract_json(raw)
    posts = []
    for item in (data.get("gonderiler") or [])[:5]:
        url = _clean_source_url(item.get("url"))
        if not url:
            continue
        # "Hashtag hijacking" savunması: Grok'un kendi ilgi puanına rağmen
        # eşiğin altındaki (ya da hiç puan vermediği) gönderileri sunucu
        # tarafında da eliyoruz — modelin kendi filtresine tam güvenmiyoruz.
        ilgi_puani = item.get("ilgi_puani")
        if not isinstance(ilgi_puani, (int, float)) or ilgi_puani < X_POST_FEED_MIN_RELEVANCE:
            continue
        tur = item.get("tur")
        posts.append({
            "yazar": str(item.get("yazar", ""))[:50],
            "metin": str(item.get("metin", ""))[:200],
            "url": url,
            "tur": tur if tur in ("yeni", "etkilesimli", "yukseliste") else "yeni",
            # Sadece modelin GERÇEKTEN emin olduğu durumlarda true — bkz.
            # sistem promptundaki "onemli_hesap" kuralı. Marketing ekibi
            # bunu "kiminle iletişime geçmeye değer" filtresi olarak kullanır.
            "onemli_hesap": item.get("onemli_hesap") is True,
        })
    genel_yon = data.get("genel_yon")
    return {
        "gonderiler": posts,
        "genel_yon": genel_yon if genel_yon in ("olumlu", "olumsuz", "notr") else "notr",
        "genel_ozet": str(data.get("genel_ozet") or "")[:200],
    }


def fetch_x_post_feed(symbol: str, region: str = "global") -> dict:
    """Tek bir coin için Grok'a gerçek X gönderilerini getirtir. "url"
    olmayan (Grok'un talimata rağmen link vermediği) her gönderi baştan
    elenir — özet/skor değil, doğrudan kaynak gösteren ham veri döner.

    region="tr": ayrı, açıkça Türkçe/Türkiye odaklı bir arama — global
    aramayla AYNI çağrıda birleştirilmiyor çünkü kripto X'i ezici
    çoğunlukla İngilizce/global; tek aramada "bölge etiketi" istesek bile
    Türkçe içerik muhtemelen cılız kalırdı. Kullanıcı gerçekten TR
    görünümü istediğinde ayrı bir ücretli çağrı yapılır (bkz. proje
    notları — otomatik ikisini birden çekmiyoruz, maliyeti katlar).

    GÜVENİLİRLİK: BTC gibi en çok konuşulan coin'lerde bile tek denemede
    0 sonuç dönebiliyordu (model o seferinde zayıf arama yapmış olabilir).
    İlk deneme boş dönerse, sunucu tarafında AÇIKÇA genişletilmiş bir
    pencereyle (ve modele "ilk aramanda bir şey bulamadın" bilgisiyle)
    1 kez daha deneriz — AMA SADECE TOP_COIN_SYMBOLS için (maliyet
    optimizasyonu): niş bir altcoin'de 0 sonuç genelde gerçekten
    "konuşulmuyor" demektir, popüler coinlerde ise muhtemelen modelin o
    seferki zayıf aramasıdır. İkinci deneme de boşsa bu artık gerçek bir
    "ilgili gönderi yok" sonucudur — daha fazla denemek maliyeti
    katlamaya değmez."""
    region_hint = (
        "SADECE Türkçe yazılmış, Türkiye'deki kullanıcılardan/hesaplardan gönderiler ara."
        if region == "tr" else
        "Global (herhangi bir dilde, dünya genelinden) gönderiler ara."
    )
    raw = _call_xai_x_search(f"Ticker: {symbol}\n{region_hint}", X_POST_FEED_SYSTEM_PROMPT)
    result = _parse_x_post_feed_response(raw)
    # Maliyet optimizasyonu: retry sadece TOP_COIN_SYMBOLS için — bu coinler
    # gerçekten popüler olduğu için 0 sonuç muhtemelen modelin zayıf aramasından
    # kaynaklanır. Niş bir altcoin'de 0 sonuç genelde gerçekten "konuşulmuyor"
    # demektir, ikinci bir ücretli çağrıyı hak etmez.
    if not result["gonderiler"] and symbol in TOP_COIN_SYMBOLS:
        print(f"  ↻ X gönderi akışı ({symbol}/{region}) ilk denemede 0 sonuç — genişletilmiş pencereyle tekrar deneniyor")
        retry_content = (
            f"Ticker: {symbol}\n{region_hint}\n"
            "NOT: Bir önceki aramada bu ticker için hiç sonuç bulamadın. Zaman "
            "penceresini biraz daha geniş tut (\"yeni\" için son 3 saat yerine "
            "son 12 saate kadar bakabilirsin) ve farklı anahtar kelime/cashtag "
            "kombinasyonları dene. Yine de gerçekten alakalı bir şey bulamazsan "
            "boş liste döndürmen tamamen kabul edilebilir, uydurma yapma."
        )
        raw_retry = _call_xai_x_search(retry_content, X_POST_FEED_SYSTEM_PROMPT)
        result = _parse_x_post_feed_response(raw_retry)

    if result["gonderiler"]:
        with ThreadPoolExecutor(max_workers=5) as pool:
            real_flags = list(pool.map(lambda p: _post_url_is_real(p["url"]), result["gonderiler"]))
        dropped = [p for p, real in zip(result["gonderiler"], real_flags) if not real]
        if dropped:
            print(f"  ✗ X gönderi akışı ({symbol}/{region}): {len(dropped)} gönderi 404 (uydurma link) — elendi")
        result["gonderiler"] = [p for p, real in zip(result["gonderiler"], real_flags) if real]

    result["gonderiler"] = _x_feed_metinleri_turkcelestir(result["gonderiler"])

    # "Buzz" seviyesi: gerçek X hacmini iddia etmiyoruz — sadece bizim
    # arama denemelerimizde kaç alakalı gönderi bulduğumuzun kaba bir
    # göstergesi (marketing için "bugün konuşuluyor mu" sinyali).
    post_count = len(result["gonderiler"])
    if post_count >= 4:
        buzz = "yuksek"
    elif post_count >= 2:
        buzz = "orta"
    elif post_count >= 1:
        buzz = "dusuk"
    else:
        buzz = "yok"
    result["buzz"] = buzz
    return result


def fetch_x_discovery() -> dict:
    """Stablex'te olmayan ama X'te trend olan ticker'ları ve rakip
    borsa(lar)ın X'teki sentiment'ini TEK bir Grok çağrısında toplar.
    rakip_sentiment içindeki "kaynak_url" — şeffaflık için, dayanak
    gösterilen gerçek gönderi (varsa)."""
    user_content = f"Rakip borsa(lar): {', '.join(X_DISCOVERY_COMPETITOR_NAMES)}"
    raw = _call_xai_x_search(user_content, X_DISCOVERY_SYSTEM_PROMPT)
    data = _extract_json(raw)
    trend_tickers = []
    for item in (data.get("trend_ticker_lar") or []):
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker", "")).strip().upper()
        if not ticker:
            continue
        yon = item.get("yon")
        trend_tickers.append({
            "ticker": ticker,
            "yon": yon if yon in ("olumlu", "olumsuz", "notr") else "notr",
            "ozet": str(item.get("ozet") or "")[:200],
        })
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

        unlisted = [t for t in result["trend_ticker_lar"] if t["ticker"] not in STABLEX_COINS_SET]
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


X_FEED_CACHE_TTL_SECONDS = 30 * 60  # 30 dk (önceden 15) — sosyal sinyal bu sürede
                                     # büyük değişmez; aynı coin'e art arda/birden
                                     # fazla kişi bakarsa gereksiz tekrar ücretli
                                     # arama yapılmasın diye maliyet optimizasyonu.
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
        return {"sembol": sembol, "bolge": bolge, "onbellek": True, **cached["veri"]}

    try:
        result = await asyncio.to_thread(fetch_x_post_feed, sembol, bolge)
    except Exception as exc:
        print(f"  ⚠ X gönderi akışı ({sembol}/{bolge}) alınamadı: {exc}")
        return {"sembol": sembol, "bolge": bolge, "gonderiler": [], "hata": "İstek başarısız oldu"}

    _x_feed_cache[cache_key] = {"veri": result, "zaman": now}
    _x_feed_request_count += 1
    print(f"  ℹ X gönderi akışı çağrısı #{_x_feed_request_count} ({sembol}/{bolge}, {len(result['gonderiler'])} gönderi, buzz={result['buzz']})")
    return {"sembol": sembol, "bolge": bolge, "onbellek": False, **result}


DIGEST_HABER_PENCERE_SAAT = 24  # "günün gündemi" maili son 24s'lik haberleri kapsar
DIGEST_X_PENCERE_SAAT = 48  # X Keşif günde 1 kez çalışır — 48s, bir sonraki
                             # tarama gecikirse "dün bulunan" veriyi de kapsasın diye


def _haberler_araliginda(baslangic: "datetime", bitis: "datetime", limit: int) -> list[dict]:
    """[baslangic, bitis] UTC aralığındaki, "onemsiz" ETİKETLİ OLMAYAN
    haberleri döner (en yeniden eskiye, en fazla `limit` tane). Hem
    "son N saat" tabanlı /api/daily-digest'in hem de sabit saat
    dilimlerine (00:00-12:00 / 12:00-24:00 TR) bağlı sabah/akşam
    bültenlerinin ortak filtresi. Her haberin zaten kendi Gemini
    analizinde bir "email" içerik önerisi var (bkz. SYSTEM_PROMPT —
    icerik_onerisi.email) — bunu ayrıca uydurmuyoruz, varsa aynen taşıyoruz."""
    sonuc = []
    for haber in db_load_recent(80):
        if len(sonuc) >= limit:
            break
        if haber.get("stablex_etiketi") == "onemsiz":
            continue
        try:
            yayin_zamani = datetime.fromisoformat(haber.get("yayin_zamani", ""))
        except ValueError:
            continue
        if not (baslangic <= yayin_zamani <= bitis):
            continue
        kanallar = haber.get("onerilen_kanallar") or []
        email_onerisi = (haber.get("icerik_onerisi") or {}).get("email") if "email" in kanallar else None
        sonuc.append({
            "baslik": haber.get("baslik_tr", ""),
            "ozet": haber.get("ozet_tr", ""),
            "etiket": haber.get("stablex_etiketi"),
            "ilgili_varliklar": haber.get("ilgili_varliklar") or [],
            "kaynak": haber.get("kaynak"),
            "kaynak_url": haber.get("kaynak_url"),
            "yayin_zamani": haber.get("yayin_zamani"),
            "email_onerisi": email_onerisi,
        })
    return sonuc


def _digest_haberler(simdi: "datetime", limit: int = 10) -> list[dict]:
    """Marketing'in "haberler + yükselenler" mail taslağı için son
    DIGEST_HABER_PENCERE_SAAT içindeki haberleri döner (24s'lik pencerede
    onlarca haber birikebiliyor, bir mail için makul bir sayıya kesiyoruz)."""
    return _haberler_araliginda(simdi - timedelta(hours=DIGEST_HABER_PENCERE_SAAT), simdi, limit)


TR_TZ = timezone(timedelta(hours=3))  # Türkiye DST uygulamıyor, sabit UTC+3


def _bulten_araligi(tur: str, simdi_utc: "datetime") -> tuple["datetime", "datetime"]:
    """"sabah" (00:00-12:00 TR) ya da "aksam" (12:00-24:00 TR) baskısının
    zaman aralığını UTC olarak döner. İstek o baskının penceresi İÇİNDE
    gelirse bitiş "şu an" olur (baskı devam ediyor, büyümeye devam eder);
    pencere henüz BAŞLAMADIYSA (ör. saat 09:00'da akşam bülteni istenirse)
    bir önceki TAMAMLANMIŞ baskı gösterilir — boş sayfa yerine "bir
    önceki baskı" her zaman daha kullanışlı."""
    simdi_tr = simdi_utc.astimezone(TR_TZ)
    bugun_00 = simdi_tr.replace(hour=0, minute=0, second=0, microsecond=0)
    bugun_12 = bugun_00 + timedelta(hours=12)
    if tur == "sabah":
        baslangic, bitis = bugun_00, min(bugun_12, simdi_tr)
    else:
        if simdi_tr < bugun_12:
            baslangic, bitis = bugun_12 - timedelta(days=1), bugun_00
        else:
            baslangic, bitis = bugun_12, simdi_tr
    return baslangic.astimezone(timezone.utc), bitis.astimezone(timezone.utc)


def _digest_yukselenler(limit: int = 5) -> list[dict]:
    """_price_cache'ten (server-side, price_fetch_loop tarafından
    güncellenen aynı kaynak) en çok yükselen coinleri döner — frontend'deki
    renderTopMovers() ile aynı sıralama mantığı, burada mail taslağı için
    tekrarlanıyor."""
    entries = [
        {"sembol": sembol, **veri}
        for sembol, veri in _price_cache.items()
        if isinstance(veri.get("degisim_24s"), (int, float))
    ]
    entries.sort(key=lambda e: e["degisim_24s"], reverse=True)
    return [e for e in entries if e["degisim_24s"] > 0][:limit]


def _digest_x_gundemi(simdi: "datetime", limit: int = 5) -> list[dict]:
    """X Keşif'in "Bugün Öne Çıkanlar" verisinden son DIGEST_X_PENCERE_SAAT
    içinde GÜNCELLENMİŞ (son_gorulme) ticker'ları döner — ilk_gorulme'ye göre
    filtrelemiyoruz çünkü o "ilk keşif" tarihidir, haftalar önce keşfedilip
    dün tekrar trend olmuş bir ticker'ı yanlışlıkla "eski" gösterirdi."""
    kesim = simdi - timedelta(hours=DIGEST_X_PENCERE_SAAT)
    sonuc = []
    for item in db_load_x_discovery():
        son_gorulme = item.get("son_gorulme")
        if not son_gorulme:
            continue
        try:
            son_gorulme_dt = datetime.fromisoformat(son_gorulme)
        except ValueError:
            continue
        if son_gorulme_dt < kesim:
            continue
        sonuc.append(item)
    sonuc.sort(key=lambda i: i["son_gorulme"], reverse=True)
    return sonuc[:limit]


@app.get("/api/daily-digest")
async def daily_digest() -> dict:
    """Marketing ekibinin "haberler + yükselenler" temalı kullanıcı
    maili için tek çağrıda ihtiyaç duyacağı VERİYİ hazırlar — mail'i
    KENDİSİ GÖNDERMEZ, sadece içerik/veri döner (gönderim ayrı, daha
    büyük bir iş — kimlik bilgisi ve gönderim onayı gerektirir).

    Üç bölüm de zaten çalışan, ücretsiz/önbellekli kaynaklardan geliyor
    (market_intel, _price_cache, x_discovery) — bu endpoint için YENİ
    bir Gemini/Grok çağrısı yapılmıyor, ek maliyeti yok."""
    simdi = datetime.now(timezone.utc)
    return {
        "olusturma_zamani": simdi.isoformat(),
        "one_cikan_haberler": _digest_haberler(simdi),
        "yukselenler": _digest_yukselenler(),
        "x_gundemi": _digest_x_gundemi(simdi),
    }


DIGEST_ETIKET_LABELS = {
    "kampanya_firsati": "Kampanya Fırsatı",
    "risk_uyarisi": "Risk Uyarısı",
    "regulasyon": "Regülasyon",
    "rakip_hareketi": "Rakip Hareketi",
    "genel_farkindalik": "Genel Farkındalık",
}
_TR_MONTH_NAMES = {v: k for k, v in _TR_MONTHS.items()}


def _digest_fmt_zaman(iso_str: str | None) -> str:
    """TR kullanıcılarına gösterilecek her zaman TR saatiyle (UTC+3)
    formatlanır — DB'deki yayin_zamani değerleri UTC olarak saklanıyor,
    burada çevrilmezse (ör. sabah/akşam bülteni saat sınırları gibi)
    kullanıcıya yanlış/kafa karıştırıcı bir saat gösterilirdi."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
    except ValueError:
        return ""
    dt = dt.astimezone(TR_TZ)
    return f"{dt.day} {_TR_MONTH_NAMES.get(dt.month, '')} {dt.strftime('%H:%M')}"


def _digest_haber_card_html(haber: dict) -> str:
    etiket_label = DIGEST_ETIKET_LABELS.get(haber["etiket"], haber["etiket"] or "")
    varliklar = " · ".join(html_lib.escape(v) for v in haber["ilgili_varliklar"])
    kaynak_link = (
        f'<a href="{html_lib.escape(haber["kaynak_url"])}" target="_blank" rel="noopener" '
        f'style="color:#dc0005;font-weight:700;text-decoration:none;">{html_lib.escape(haber["kaynak"] or "")} — Kaynağı gör →</a>'
        if haber.get("kaynak_url") else html_lib.escape(haber.get("kaynak") or "")
    )
    return f"""
      <div style="background:#fff;border-radius:16px;padding:20px;display:flex;flex-direction:column;gap:8px;box-shadow:0 1px 2px rgba(16,24,40,0.04),0 4px 16px -4px rgba(16,24,40,0.06);">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;">
          <span style="font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.04em;color:#6a7282;background:#f9fafb;padding:4px 10px;border-radius:999px;">{html_lib.escape(etiket_label)}</span>
          <span style="font-size:11px;font-weight:600;color:#6a7282;">{html_lib.escape(_digest_fmt_zaman(haber["yayin_zamani"]))}</span>
        </div>
        <h3 style="font-size:15px;font-weight:800;color:#000;margin:0;">{html_lib.escape(haber["baslik"])}</h3>
        <p style="font-size:13px;color:#364153;line-height:1.5;margin:0;">{html_lib.escape(haber["ozet"])}</p>
        {f'<p style="font-size:11px;font-weight:700;color:#6a7282;margin:0;">{varliklar}</p>' if varliklar else ""}
        <p style="font-size:12px;margin:4px 0 0 0;">{kaynak_link}</p>
      </div>"""


def _digest_gainer_row_html(g: dict) -> str:
    return f"""
      <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 14px;background:#f9fafb;border-radius:12px;">
        <span style="font-size:13px;font-weight:800;color:#000;">{html_lib.escape(g["sembol"])}</span>
        <span style="font-size:13px;font-weight:700;color:#00c758;">▲ {g["degisim_24s"]:.2f}%</span>
      </div>"""


_DIGEST_YON_LABEL = {"olumlu": "Olumlu", "olumsuz": "Olumsuz", "notr": "Nötr"}


def _digest_x_row_html(item: dict) -> str:
    yon = item.get("yon")
    renk = "#00c758" if yon == "olumlu" else "#fb2c36" if yon == "olumsuz" else "#6a7282"
    ozet = html_lib.escape(item.get("ozet") or "")
    return f"""
      <div style="background:#f9fafb;border-radius:12px;padding:12px 14px;display:flex;flex-direction:column;gap:4px;">
        <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;">
          <span style="font-size:13px;font-weight:800;color:#000;">{html_lib.escape(item["ticker"])}</span>
          <span style="font-size:11px;font-weight:700;color:{renk};">{_DIGEST_YON_LABEL.get(yon, "")}</span>
        </div>
        {f'<p style="font-size:12px;color:#364153;margin:0;">{ozet}</p>' if ozet else ""}
      </div>"""


def _bulten_onay_bloku_html(tur: str, pencere_baslangic: str | None, pencere_bitis: str | None, onaylandi: bool) -> str:
    """Onay formu — bu SADECE bir kayıt/denetim izi mekanizmasıdır, gerçek
    bir yetkilendirme/kimlik doğrulama sistemi DEĞİLDİR (kim isim yazarsa
    o "onaylayan" olarak kaydedilir). Sayfanın kendisi hâlâ hiçbir onay
    olmadan görüntülenebilir — onay sadece bultenler tablosuna dondurulmuş
    bir anlık görüntü kaydeder, yayın/gönderim yapmaz."""
    onay_mesaji = (
        '<p style="color:#00c758;font-size:12px;font-weight:700;margin:0 0 8px 0;">✓ Onaylandı ve arşive kaydedildi.</p>'
        if onaylandi else ""
    )
    return f"""
    <section style="background:#f9fafb;border-radius:16px;padding:16px 20px;">
      {onay_mesaji}
      <form method="post" action="/api/bulten-onayla" style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
        <input type="hidden" name="tur" value="{html_lib.escape(tur)}">
        <input type="hidden" name="pencere_baslangic" value="{html_lib.escape(pencere_baslangic or '')}">
        <input type="hidden" name="pencere_bitis" value="{html_lib.escape(pencere_bitis or '')}">
        <input type="text" name="onaylayan" required placeholder="Adın (onaylayan)" style="flex:1;min-width:160px;padding:8px 12px;border-radius:10px;border:1px solid #e4e4e4;font-size:13px;">
        <button type="submit" style="background:#000;color:#fff;border:none;border-radius:10px;padding:8px 16px;font-size:13px;font-weight:700;cursor:pointer;">Onayla ve Arşivle</button>
      </form>
      <p style="font-size:11px;color:#6a7282;margin:8px 0 0 0;">Onaylamak bu bülteni <a href="/bulten-arsivi" style="color:#dc0005;">arşive</a> o anki hâliyle kaydeder — otomatik gönderim/yayın yapmaz.</p>
    </section>"""


def _bulten_sayfasi_html(
    baslik: str, alt_baslik: str, haberler: list[dict], yukselenler: list[dict], x_gundemi: list[dict],
    bos_haber_metni: str, tur: str | None = None, pencere_baslangic: str | None = None,
    pencere_bitis: str | None = None, onaylandi: bool = False,
) -> str:
    """Üç bülten sayfasının (günlük, sabah, akşam) ortak render'ı — sadece
    başlık/alt başlık ve haber listesi değişir, Yükselenler/X Gündemi
    her zaman "şu anki" (zaman penceresiz) anlık görüntüyü gösterir.
    "tur" verilirse altta bir Onay Bloku da eklenir (arşiv sayfasında
    geçmiş bir kaydı salt-okunur göstermek için tur=None geçilir)."""
    haber_html = (
        "\n".join(_digest_haber_card_html(h) for h in haberler)
        if haberler else f'<p style="color:#6a7282;font-size:13px;">{html_lib.escape(bos_haber_metni)}</p>'
    )
    gainer_html = (
        "\n".join(_digest_gainer_row_html(g) for g in yukselenler)
        if yukselenler else '<p style="color:#6a7282;font-size:13px;">Şu an yükselen coin yok.</p>'
    )
    x_html = (
        "\n".join(_digest_x_row_html(i) for i in x_gundemi)
        if x_gundemi else '<p style="color:#6a7282;font-size:13px;">Şu an öne çıkan X gündemi yok.</p>'
    )
    onay_bloku = _bulten_onay_bloku_html(tur, pencere_baslangic, pencere_bitis, onaylandi) if tur else ""
    return f"""<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html_lib.escape(baslik)}</title>
</head>
<body style="margin:0;background:#f9fafb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="background:#000;padding:24px 20px;">
    <h1 style="color:#fff;font-size:20px;font-weight:800;margin:0;">STABLEX <span style="color:#dc0005;">●</span> {html_lib.escape(baslik)}</h1>
    <p style="color:#e4e4e4;font-size:12px;margin:6px 0 0 0;">{html_lib.escape(alt_baslik)}</p>
  </div>
  <div style="background:#f9fafb;border-bottom:1px solid #e4e4e4;padding:8px 20px;text-align:center;">
    <p style="font-size:11px;font-weight:600;color:#6a7282;margin:0;">
      Bu içerikler yapay zeka tarafından otomatik üretilir ve <strong style="color:#000;">yatırım tavsiyesi değildir</strong> — yayınlanmadan önce insan onayından geçirilmelidir.
    </p>
  </div>
  <div style="max-width:640px;margin:0 auto;padding:24px 20px;display:flex;flex-direction:column;gap:28px;">
    {onay_bloku}
    <section style="display:flex;flex-direction:column;gap:12px;">
      <h2 style="font-size:16px;font-weight:800;color:#000;margin:0;">Öne Çıkan Haberler</h2>
      {haber_html}
    </section>
    <section style="display:flex;flex-direction:column;gap:8px;">
      <h2 style="font-size:16px;font-weight:800;color:#000;margin:0;">Yükselenler</h2>
      {gainer_html}
    </section>
    <section style="display:flex;flex-direction:column;gap:8px;">
      <h2 style="font-size:16px;font-weight:800;color:#000;margin:0;">X Gündemi</h2>
      {x_html}
    </section>
    <p style="font-size:11px;color:#6a7282;text-align:center;">Bu sayfa otomatik oluşturuldu — ham veri için <a href="/api/daily-digest" style="color:#dc0005;">/api/daily-digest</a></p>
  </div>
</body>
</html>"""


@app.get("/daily-digest", response_class=HTMLResponse)
async def daily_digest_html() -> str:
    """/api/daily-digest'in aynı verisini, marketing ekibinin doğrudan
    ekranda görüp değerlendirebileceği (kopyala-yapıştır yerine) render
    edilmiş bir sayfa olarak sunar — her haberin kaynağına giden link
    dahil. Ayrı bir AI çağrısı yapmıyor, /api/daily-digest ile aynı
    veriyi kullanıyor."""
    simdi = datetime.now(timezone.utc)
    return _bulten_sayfasi_html(
        "Günlük Bülten",
        f"Oluşturulma: {_digest_fmt_zaman(simdi.isoformat())} (TR)",
        _digest_haberler(simdi),
        _digest_yukselenler(),
        _digest_x_gundemi(simdi),
        "Son 24 saatte önemli bir haber yok.",
        tur="gunluk",
    )


@app.get("/sabah-bulteni", response_class=HTMLResponse)
async def sabah_bulteni_html() -> str:
    """00:00-12:00 (TR saati) penceresindeki haberleri gösterir — istek
    bu pencere içindeyken "şu ana kadar", dışındayken (öğleden sonra/akşam)
    o günün TAMAMLANMIŞ sabah baskısını gösterir (bkz. _bulten_araligi)."""
    simdi = datetime.now(timezone.utc)
    baslangic, bitis = _bulten_araligi("sabah", simdi)
    return _bulten_sayfasi_html(
        "Sabah Bülteni",
        f"{_digest_fmt_zaman(baslangic.isoformat())} – {_digest_fmt_zaman(bitis.isoformat())} (TR)",
        _haberler_araliginda(baslangic, bitis, limit=15),
        _digest_yukselenler(),
        _digest_x_gundemi(simdi),
        "Bu sabah henüz önemli bir haber yok.",
        tur="sabah", pencere_baslangic=baslangic.isoformat(), pencere_bitis=bitis.isoformat(),
    )


@app.get("/aksam-bulteni", response_class=HTMLResponse)
async def aksam_bulteni_html() -> str:
    """12:00-24:00 (TR saati) penceresindeki haberleri gösterir — istek
    bu pencere BAŞLAMADAN (saat 12:00'dan önce) gelirse dünün TAMAMLANMIŞ
    akşam baskısını gösterir, boş sayfa yerine (bkz. _bulten_araligi)."""
    simdi = datetime.now(timezone.utc)
    baslangic, bitis = _bulten_araligi("aksam", simdi)
    return _bulten_sayfasi_html(
        "Akşam Bülteni",
        f"{_digest_fmt_zaman(baslangic.isoformat())} – {_digest_fmt_zaman(bitis.isoformat())} (TR)",
        _haberler_araliginda(baslangic, bitis, limit=15),
        _digest_yukselenler(),
        _digest_x_gundemi(simdi),
        "Bu akşam henüz önemli bir haber yok.",
        tur="aksam", pencere_baslangic=baslangic.isoformat(), pencere_bitis=bitis.isoformat(),
    )


@app.post("/api/bulten-onayla", response_class=HTMLResponse)
async def bulten_onayla(
    tur: str = Form(...),
    pencere_baslangic: str = Form(""),
    pencere_bitis: str = Form(""),
    onaylayan: str = Form(...),
) -> str:
    """Bir bülteni ONAYLANDIĞI ANDAKİ hâliyle bultenler tablosuna
    dondurup kaydeder. Bu SADECE bir kayıt/denetim izi — gerçek bir
    yetkilendirme sistemi değil (kim isim yazarsa o "onaylayan" olur),
    ve HİÇBİR yayın/gönderim (mail, push) tetiklemez. İçerik istemciden
    GÜVENİLMEZ — güvenlik/tutarlılık için sunucu tarafında aynı window
    ile YENİDEN üretilir (kullanıcı forma dokunup içerik değiştiremez)."""
    if not onaylayan.strip():
        return HTMLResponse("Onaylayan adı zorunlu.", status_code=400)

    simdi = datetime.now(timezone.utc)
    if tur == "sabah" or tur == "aksam":
        try:
            baslangic_dt = datetime.fromisoformat(pencere_baslangic)
            bitis_dt = datetime.fromisoformat(pencere_bitis)
        except ValueError:
            baslangic_dt, bitis_dt = _bulten_araligi(tur, simdi)
        haberler = _haberler_araliginda(baslangic_dt, bitis_dt, limit=15)
    else:
        tur = "gunluk"
        baslangic_dt, bitis_dt = None, None
        haberler = _digest_haberler(simdi)

    yukselenler = _digest_yukselenler()
    x_gundemi = _digest_x_gundemi(simdi)
    bulten_id = await asyncio.to_thread(
        db_kaydet_bulten, tur,
        baslangic_dt.isoformat() if baslangic_dt else None,
        bitis_dt.isoformat() if bitis_dt else None,
        haberler, yukselenler, x_gundemi, onaylayan,
    )
    print(f"  ✓ Bülten onaylandı: #{bulten_id} ({tur}, onaylayan: {onaylayan.strip()[:50]})")

    baslik = {"sabah": "Sabah Bülteni", "aksam": "Akşam Bülteni", "gunluk": "Günlük Bülten"}[tur]
    alt_baslik = (
        f"{_digest_fmt_zaman(baslangic_dt.isoformat())} – {_digest_fmt_zaman(bitis_dt.isoformat())} (TR)"
        if baslangic_dt else f"Oluşturulma: {_digest_fmt_zaman(simdi.isoformat())} (TR)"
    )
    return _bulten_sayfasi_html(
        baslik, alt_baslik, haberler, yukselenler, x_gundemi,
        "İçerik yok.", tur=tur,
        pencere_baslangic=baslangic_dt.isoformat() if baslangic_dt else None,
        pencere_bitis=bitis_dt.isoformat() if bitis_dt else None,
        onaylandi=True,
    )


def _bulten_arsiv_satir_html(kayit: dict) -> str:
    tur_label = {"sabah": "Sabah Bülteni", "aksam": "Akşam Bülteni", "gunluk": "Günlük Bülten"}.get(kayit["tur"], kayit["tur"])
    pencere = (
        f"{_digest_fmt_zaman(kayit['pencere_baslangic'])} – {_digest_fmt_zaman(kayit['pencere_bitis'])}"
        if kayit.get("pencere_baslangic") else "—"
    )
    return f"""
      <a href="/bulten-arsivi/{kayit['id']}" style="text-decoration:none;color:inherit;">
        <div style="background:#fff;border-radius:14px;padding:14px 18px;display:flex;align-items:center;justify-content:space-between;gap:12px;box-shadow:0 1px 2px rgba(16,24,40,0.04);">
          <div style="display:flex;flex-direction:column;gap:2px;">
            <span style="font-size:13px;font-weight:800;color:#000;">{html_lib.escape(tur_label)}</span>
            <span style="font-size:11px;color:#6a7282;">{html_lib.escape(pencere)}</span>
          </div>
          <div style="text-align:right;display:flex;flex-direction:column;gap:2px;">
            <span style="font-size:12px;font-weight:700;color:#00c758;">✓ {html_lib.escape(kayit['onaylayan'])}</span>
            <span style="font-size:11px;color:#6a7282;">{html_lib.escape(_digest_fmt_zaman(kayit['onay_zamani']))}</span>
          </div>
        </div>
      </a>"""


@app.get("/bulten-arsivi", response_class=HTMLResponse)
async def bulten_arsivi_html() -> str:
    """Onaylanmış tüm bültenlerin listesi — denetim izi/geçmiş kayıt.
    Her satır kendi dondurulmuş anlık görüntüsüne (/bulten-arsivi/{id})
    gider, canlı veriyle yeniden hesaplanmaz."""
    kayitlar = await asyncio.to_thread(db_load_bultenler, 50)
    satirlar = (
        "\n".join(_bulten_arsiv_satir_html(k) for k in kayitlar)
        if kayitlar else '<p style="color:#6a7282;font-size:13px;">Henüz onaylanmış bir bülten yok.</p>'
    )
    return f"""<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bülten Arşivi</title>
</head>
<body style="margin:0;background:#f9fafb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
  <div style="background:#000;padding:24px 20px;">
    <h1 style="color:#fff;font-size:20px;font-weight:800;margin:0;">STABLEX <span style="color:#dc0005;">●</span> Bülten Arşivi</h1>
    <p style="color:#e4e4e4;font-size:12px;margin:6px 0 0 0;">Onaylanmış geçmiş bültenler — denetim izi</p>
  </div>
  <div style="max-width:640px;margin:0 auto;padding:24px 20px;display:flex;flex-direction:column;gap:10px;">
    {satirlar}
  </div>
</body>
</html>"""


@app.get("/bulten-arsivi/{bulten_id}", response_class=HTMLResponse)
async def bulten_arsivi_detay_html(bulten_id: int) -> str:
    """Arşivdeki TEK bir bültenin dondurulmuş hâli — salt okunur, onay
    formu YOK (zaten onaylanmış), canlı veriyle yeniden render edilmez."""
    kayit = await asyncio.to_thread(db_load_bulten, bulten_id)
    if not kayit:
        return HTMLResponse("Bülten bulunamadı.", status_code=404)
    baslik = {"sabah": "Sabah Bülteni", "aksam": "Akşam Bülteni", "gunluk": "Günlük Bülten"}.get(kayit["tur"], kayit["tur"])
    pencere = (
        f"{_digest_fmt_zaman(kayit['pencere_baslangic'])} – {_digest_fmt_zaman(kayit['pencere_bitis'])} (TR)"
        if kayit.get("pencere_baslangic") else f"Oluşturulma: {_digest_fmt_zaman(kayit['olusturma_zamani'])} (TR)"
    )
    alt_baslik = f"{pencere} · ✓ {kayit['onaylayan']} tarafından {_digest_fmt_zaman(kayit['onay_zamani'])} tarihinde onaylandı"
    return _bulten_sayfasi_html(
        f"{baslik} (Arşiv #{kayit['id']})", alt_baslik,
        kayit["haberler"], kayit["yukselenler"], kayit["x_gundemi"],
        "İçerik yok.",
    )


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
