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

Gerekli ortam değişkenleri:
  COINGECKO_API_KEY — ücretsiz, coingecko.com/en/developers/dashboard
  GEMINI_API_KEY     — ücretsiz, aistudio.google.com/apikey

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
from fastapi.responses import HTMLResponse
from google import genai
from google.genai import types as genai_types

from sources import (
    CHANNELS,
    ENTRIES_PER_SOURCE,
    FETCH_INTERVAL_SECONDS,
    FRESHNESS_WINDOW_DAYS,
    PRICE_FETCH_INTERVAL_SECONDS,
    PRICE_STREAM_INTERVAL_SECONDS,
    REGULATORY_LINKS,
    STABLEX_COIN_IDS,
    STABLEX_COINS,
    TOP_COIN_SYMBOLS,
    WHITELIST_SOURCES,
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


def _call_gemini(news_text: str) -> str:
    client = get_gemini_client()
    last_error = None
    for attempt in range(GEMINI_MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=news_text,
                config=genai_types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
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
# FastAPI app
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    fetch_task = asyncio.create_task(fetch_loop())
    emit_task = asyncio.create_task(emit_loop())
    price_fetch_task = asyncio.create_task(price_fetch_loop())
    price_stream_task = asyncio.create_task(price_stream_loop())
    try:
        yield
    finally:
        fetch_task.cancel()
        emit_task.cancel()
        price_fetch_task.cancel()
        price_stream_task.cancel()


app = FastAPI(title="Stablex Kripto Piyasa Canlı Radarı", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    return html.replace("/*__REGULATORY_LINKS__*/", json.dumps(REGULATORY_LINKS, ensure_ascii=False))


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
