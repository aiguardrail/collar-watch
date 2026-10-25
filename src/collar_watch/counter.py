"""
Aşama 0 — Robinhood Chain'de Uniswap V4 havuz sıklığı ölçümü.
Hiçbir Collar çağrısı yapmaz; sadece zincirden okur.

Neler yapar:
  * Initialize olaylarını eth_getLogs ile (blok aralığı, parça parça) okur.
    Robinhood Chain eth_subscribe desteklemediği için WSS yok; polling tek yoldur.
  * Her olayı çözer: poolId, currency0/1, fee, tickSpacing, hooks, sqrtPriceX96, tick.
  * İsteğe bağlı: ModifyLiquidity ile havuzun İLK likiditesinin ne zaman eklendiğini ölçer
    (Aşama 1'de T+0 değerlendirmesinde likidite var mı sorusunu cevaplar).
  * Kalıcıdır: events.jsonl + checkpoint.json. Çökerse/yeniden başlatılırsa kaldığı
    bloktan devam eder, aradaki boşluğu geriye dönük doldurur.
  * Geriye dönük ölçüm: BACKFILL_HOURS ile geçmiş günleri dakikalar içinde ölçer.

eth_getLogs hataları (HTTP 400 veya JSON-RPC range error) otomatik olarak
max_span'i küçültür. Başarılı çağrılar arttıkça aralık yavaşça tekrar büyütülür
(MAX_BLOCK_RANGE tavanına kadar), böylece geçici bir hata kalıcı yavaşlığa yol açmaz.

Bu sürümde (geriye dönük uyumlu) eklenenler:
  * Sonlu modda (DURATION_HOURS=0 / süre dolmuş) poll hatası olursa hemen çıkmak yerine
    MAX_CONSECUTIVE_ERRORS kadar tekrar dener; hâlâ bitmediyse çıkış kodu 3 döner
    (eskiden sessizce 0 dönüyordu → CI'da "başarılı ama boş veri" riski).
  * Rate-limit (429 / JSON-RPC "rate limit") span'i küçültmez; Retry-After'a uyar.
  * Backoff'a jitter, tüm taşıma hataları (httpx.TransportError) için retry.
  * Initialize blok zaman damgaları paralel (sınırlı) önceden çekilir → backfill hızlanır.
  * JSONL yazımı parça başına tek fsync (checkpoint'ten ÖNCE) → aynı dayanıklılık, çok daha hızlı.
  * Yarım kalmış son JSONL satırı bir sonraki yazımı bozmaz.
  * Periyodik kısmi özet (PROGRESS_INTERVAL_SEC) + heartbeat.json (izleme için).
  * Özete yalnızca EKLEME yapıldı: "daily", "liquidity.pools_without_liquidity", "health".
    Mevcut tüm alanlar ve dosya formatları aynıdır.
  * --selftest: ağsız çözümleyici testi.

Kullanım:
    python -m collar_watch.counter                                  # 24 saat canlı
    BACKFILL_HOURS=168 DURATION_HOURS=0 python -m collar_watch.counter   # son 7 günü tara, çık
    BACKFILL_HOURS=48 DURATION_HOURS=24 python -m collar_watch.counter   # 48s geçmiş + 24s canlı
    START_BLOCK=1234567 DURATION_HOURS=0 python -m collar_watch.counter  # belirli bloktan
    POOL_MANAGER=0x... python -m collar_watch.counter                    # tek kontrat filtresi
    DATA_DIR=run2 python -m collar_watch.counter                         # yeni, temiz ölçüm
    python -m collar_watch.counter --selftest                            # çevrimdışı test

Ortam değişkenleri:
    ROBINHOOD_RPC, POOL_MANAGER, DURATION_HOURS, BACKFILL_HOURS, START_BLOCK,
    POLL_INTERVAL_SEC (15), CONFIRMATIONS (2), MAX_BLOCK_RANGE (5000),
    TRACK_LIQUIDITY (1), EXPECTED_CHAIN_ID (4663), DATA_DIR (data),
    COUNTER_LOG (counter.log), COUNTER_SUMMARY (counter-summary.json),
    PROGRESS_INTERVAL_SEC (900),
    MAX_CONSECUTIVE_ERRORS (10)  # yalnızca süre dolduktan sonra çıkış için geçerli
    TS_PREFETCH_CONCURRENCY (8)

Çıkış kodları: 0 = tamam, 2 = yapılandırma/zincir uyuşmazlığı, 3 = hata yüzünden bitirilemedi.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import httpx
from eth_utils import keccak

# --- Config ---
RPC_URL = os.getenv("ROBINHOOD_RPC", "https://rpc.mainnet.chain.robinhood.com")
POOL_MANAGER = os.getenv("POOL_MANAGER", "").strip().lower()
DURATION_HOURS = float(os.getenv("DURATION_HOURS", "24"))
BACKFILL_HOURS = float(os.getenv("BACKFILL_HOURS", "0"))
START_BLOCK = os.getenv("START_BLOCK", "").strip()
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "15"))
CONFIRMATIONS = int(os.getenv("CONFIRMATIONS", "2"))
MAX_BLOCK_RANGE = int(os.getenv("MAX_BLOCK_RANGE", "5000"))
TRACK_LIQUIDITY = os.getenv("TRACK_LIQUIDITY", "1") == "1"
EXPECTED_CHAIN_ID = int(os.getenv("EXPECTED_CHAIN_ID", "4663"))
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
LOG_FILE = os.getenv("COUNTER_LOG", "counter.log")
SUMMARY_FILE = os.getenv("COUNTER_SUMMARY", "counter-summary.json")
PROGRESS_INTERVAL_SEC = float(os.getenv("PROGRESS_INTERVAL_SEC", "900"))
MAX_CONSECUTIVE_ERRORS = int(os.getenv("MAX_CONSECUTIVE_ERRORS", "10"))
TS_PREFETCH_CONCURRENCY = int(os.getenv("TS_PREFETCH_CONCURRENCY", "8"))

# Uniswap V4 PoolManager olayları (kanonik imzalar: PoolId=bytes32, Currency=address)
INITIALIZE_SIG = "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
MODIFY_LIQUIDITY_SIG = "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)"
INITIALIZE_TOPIC = "0x" + keccak(text=INITIALIZE_SIG).hex()
MODIFY_LIQUIDITY_TOPIC = "0x" + keccak(text=MODIFY_LIQUIDITY_SIG).hex()

ZERO_ADDR = "0x" + "0" * 40
DYNAMIC_FEE_FLAG = 0x800000  # V4 LPFeeLibrary.DYNAMIC_FEE_FLAG
LIQ_THRESHOLDS_SEC = (0, 60, 600, 3600, 86400)

# span büyütme/küçültme ayarları
_GROW_AFTER_START = 10     # bu kadar ardışık başarıdan sonra span büyütülür
_GROW_AFTER_CAP = 320      # her küçültmeden sonra bekleme ikiye katlanır, bu değere kadar

# --- Logging ---
log = logging.getLogger("counter")


def setup_logging():
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
    sh = logging.StreamHandler()
    for h in (fh, sh):
        h.setFormatter(fmt)
    log.setLevel(logging.INFO)
    log.handlers[:] = [fh, sh]


# --- ABI çözümleme ---
def _words(data: str) -> list[str]:
    h = data[2:] if data.startswith("0x") else data
    return [h[i:i + 64] for i in range(0, len(h) - len(h) % 64, 64)]


def _s256(word_hex: str) -> int:
    v = int(word_hex, 16)
    return v - (1 << 256) if v >= (1 << 255) else v


def _addr_word(word_hex: str) -> str:
    return "0x" + word_hex[-40:].lower()


def _topic_addr(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def decode_initialize(lg: dict) -> dict | None:
    topics, w = lg.get("topics", []), _words(lg.get("data", "0x"))
    if len(topics) < 4 or len(w) < 5:
        return None
    return {
        "pool_id": topics[1].lower(),
        "currency0": _topic_addr(topics[2]),
        "currency1": _topic_addr(topics[3]),
        "fee": int(w[0], 16),
        "tick_spacing": _s256(w[1]),
        "hooks": _addr_word(w[2]),
        "sqrt_price_x96": str(int(w[3], 16)),  # string: JS tüketicilerde hassasiyet kaybı olmasın
        "tick": _s256(w[4]),
    }


def decode_modify_liquidity(lg: dict) -> dict | None:
    topics, w = lg.get("topics", []), _words(lg.get("data", "0x"))
    if len(topics) < 3 or len(w) < 4:
        return None
    return {
        "pool_id": topics[1].lower(),
        "sender": _topic_addr(topics[2]),
        "tick_lower": _s256(w[0]),
        "tick_upper": _s256(w[1]),
        "liquidity_delta": _s256(w[2]),
    }


# --- RPC istemcisi ---
class RPCError(Exception):
    def __init__(self, err):
        super().__init__(str(err))
        self.err = err


def _is_rate_limit(e: RPCError) -> bool:
    m = str(e).lower()
    return "rate limit" in m or "rate-limit" in m or "too many requests" in m or "throttl" in m


def _is_range_error(e: RPCError) -> bool:
    if _is_rate_limit(e):
        return False  # rate limit span'i küçültmekle çözülmez; bekleyip tekrar denenir
    m = str(e).lower()
    return any(k in m for k in (
        "range", "limit", "too many", "exceed", "too large", "10000",
        "response size", "more than", "block range", "query returned",
    ))


def _retry_after_sec(r: httpx.Response, default: float) -> float:
    try:
        v = float(r.headers.get("retry-after", ""))
        return min(max(v, 0.5), 60.0)
    except (ValueError, TypeError):
        return default


def _backoff(attempt: int) -> float:
    return 2 ** attempt + random.random()  # jitter: aynı anda tekrar deneme fırtınasını önler


class RPC:
    def __init__(self, url: str, timeout: float = 30):
        self.url = url
        self.client = httpx.AsyncClient(timeout=timeout)
        self._id = 0
        self.max_span = MAX_BLOCK_RANGE
        self._ts: dict[int, int] = {}
        self._alchemy_400_logged = False
        self._ok_streak = 0
        self._grow_after = _GROW_AFTER_START

    async def aclose(self):
        await self.client.aclose()

    # --- span yönetimi ---
    def shrink_span(self, span: int) -> int:
        self.max_span = max(1, span // 2)
        self._ok_streak = 0
        self._grow_after = min(self._grow_after * 2, _GROW_AFTER_CAP)
        return self.max_span

    def note_success(self, span_used: int):
        """Ardışık başarılardan sonra span'i yavaşça büyüt (MAX_BLOCK_RANGE tavanına kadar)."""
        # Yalnızca gerçekten tam span kullanıldıysa say (kısa kuyruk parçaları kanıt değildir).
        if span_used < self.max_span:
            return
        self._ok_streak += 1
        if self._ok_streak >= self._grow_after and self.max_span < MAX_BLOCK_RANGE:
            old = self.max_span
            self.max_span = min(MAX_BLOCK_RANGE, int(old * 1.5) + 1)
            self._ok_streak = 0
            log.info("getLogs aralığı büyütüldü: %d → %d blok", old, self.max_span)

    async def call(self, method: str, params: list, retries: int = 4):
        """JSON-RPC çağrısı.

        Retry politikası:
          * Taşıma hataları (httpx.TransportError: bağlantı/zaman aşımı/okuma vb.) → retry
          * HTTP 429 (rate limit) → retry (Retry-After'a uyar)
          * HTTP 5xx → retry
          * HTTP 4xx (429 hariç) → KALICI hata, hemen çık (HTTPStatusError raise)
          * JSON parse hatası → retry
          * JSON-RPC error → RPCError raise (rate-limit türü olanlar retry'lanır)
        """
        for attempt in range(retries + 1):
            # 1) HTTP isteği (ağ hataları retry'lanır)
            try:
                self._id += 1
                r = await self.client.post(self.url, json={
                    "jsonrpc": "2.0", "id": self._id, "method": method, "params": params,
                })
            except httpx.TransportError as e:
                if attempt == retries:
                    raise
                wait = _backoff(attempt)
                log.warning("%s ağ hatası: %s — %.1fs sonra tekrar", method, type(e).__name__, wait)
                await asyncio.sleep(wait)
                continue

            # 2) HTTP durum kodu kontrolü
            if r.status_code >= 400:
                snippet = (r.text or "").strip().replace("\n", " ")[:300]
                # 429 veya 5xx → retry
                if r.status_code == 429 or r.status_code >= 500:
                    if attempt < retries:
                        wait = _retry_after_sec(r, _backoff(attempt)) if r.status_code == 429 else _backoff(attempt)
                        log.warning("%s HTTP %d → %.1fs sonra tekrar | %s",
                                    method, r.status_code, wait, snippet)
                        await asyncio.sleep(wait)
                        continue
                # 4xx (429 hariç) veya retry tükendi → kalıcı hata
                log.warning("%s HTTP %d (kalıcı): %s", method, r.status_code, snippet)
                r.raise_for_status()

            # 3) JSON parse
            try:
                data = r.json()
            except ValueError as e:
                if attempt == retries:
                    raise
                wait = _backoff(attempt)
                log.warning("%s JSON parse hatası: %s — %.1fs sonra tekrar", method, e, wait)
                await asyncio.sleep(wait)
                continue

            # 4) JSON-RPC error
            if "error" in data:
                err = RPCError(data["error"])
                if _is_rate_limit(err) and attempt < retries:
                    wait = _backoff(attempt)
                    log.warning("%s JSON-RPC rate limit → %.1fs sonra tekrar", method, wait)
                    await asyncio.sleep(wait)
                    continue
                raise err
            return data["result"]
        raise RuntimeError(f"{method}: retry döngüsü beklenmedik şekilde bitti")  # erişilmemeli

    async def block_number(self) -> int:
        return int(await self.call("eth_blockNumber", []), 16)

    async def block_ts(self, number: int) -> int:
        if number in self._ts:
            return self._ts[number]
        blk = await self.call("eth_getBlockByNumber", [hex(number), False])
        if not blk:
            raise RuntimeError(f"blok bulunamadı: {number}")
        ts = int(blk["timestamp"], 16)
        if len(self._ts) > 20000:
            self._ts.clear()
        self._ts[number] = ts
        return ts

    async def prefetch_ts(self, blocks: set[int]):
        """Eksik blok zaman damgalarını sınırlı paralellikle önceden çek (backfill hızlandırır)."""
        missing = [b for b in blocks if b not in self._ts]
        if len(missing) < 2:
            return  # tek blok için paralellik kazanç sağlamaz; block_ts tembel çeker
        sem = asyncio.Semaphore(max(1, TS_PREFETCH_CONCURRENCY))

        async def one(b: int):
            async with sem:
                await self.block_ts(b)

        await asyncio.gather(*(one(b) for b in missing))


async def iter_log_chunks(rpc: RPC, start: int, end: int):
    """[start, end] aralığını parça parça getirir.

    RPC sağlayıcı 'aralık çok büyük' derse (JSON-RPC error veya HTTP 400)
    parçayı küçültür. Alchemy gibi sağlayıcılar 400 Bad Request döndürebilir;
    her iki durumu da ele alıyoruz. Başarılı çağrılar span'i yavaşça geri büyütür.
    """
    topics = [[INITIALIZE_TOPIC, MODIFY_LIQUIDITY_TOPIC] if TRACK_LIQUIDITY else INITIALIZE_TOPIC]
    cur = start
    while cur <= end:
        span = max(1, rpc.max_span)
        hi = min(cur + span - 1, end)
        flt = {"fromBlock": hex(cur), "toBlock": hex(hi), "topics": topics}
        if POOL_MANAGER:
            flt["address"] = POOL_MANAGER
        try:
            res = await rpc.call("eth_getLogs", [flt])
        except RPCError as e:
            # JSON-RPC seviyesinde range hatası
            if span > 1 and _is_range_error(e):
                new = rpc.shrink_span(span)
                log.info("getLogs aralığı küçültüldü (JSON-RPC): %d → %d blok (%s)", span, new, e)
                continue
            raise
        except httpx.HTTPStatusError as e:
            # HTTP 400 Bad Request → Alchemy blok aralığını çok büyük buldu
            status = e.response.status_code if e.response is not None else 0
            body = ""
            if e.response is not None:
                body = (e.response.text or "").strip().replace("\n", " ")[:200]
            if status == 400 and span > 1:
                if not rpc._alchemy_400_logged:
                    log.warning("HTTP 400 alındı, blok aralığı küçültülüyor. Sunucu yanıtı: %s", body)
                    rpc._alchemy_400_logged = True
                new = rpc.shrink_span(span)
                log.info("getLogs aralığı küçültüldü (HTTP 400): %d → %d blok", span, new)
                continue
            # span=1'de bile 400 → bu sağlayıcı bu çağrıyı desteklemiyor
            if status == 400 and span == 1:
                log.error("HTTP 400, span=1'de bile. Bu RPC sağlayıcısı eth_getLogs "
                          "desteklemiyor olabilir. Yanıt: %s", body)
            raise
        rpc.note_success(hi - cur + 1)
        yield cur, hi, res
        cur = hi + 1


async def estimate_start_block(rpc: RPC, head: int, hours: float) -> int:
    """Son ~50k bloktan ortalama blok süresini tahmin edip 'hours' saat öncesini bulur."""
    sample = min(head, 50_000)
    if sample < 100:
        return 0
    t_head, t_old = await rpc.block_ts(head), await rpc.block_ts(head - sample)
    spb = (t_head - t_old) / sample
    if spb <= 0:
        raise RuntimeError("blok süresi tahmin edilemedi")
    return max(0, head - int(hours * 3600 / spb))


# --- Kalıcı durum ---
def _atomic_write_json(path: Path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _read_jsonl(path: Path):
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue  # çökme anında yarım kalmış son satır


def _ensure_trailing_newline(path: Path):
    """Çökmeden yarım kalmış son satır varsa, sonraki kaydın onunla birleşmemesi için satırı kapat."""
    try:
        if path.exists() and path.stat().st_size > 0:
            with open(path, "rb+") as f:
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b"\n":
                    f.write(b"\n")
                    f.flush()
                    os.fsync(f.fileno())
    except OSError as e:
        log.warning("JSONL sonu kontrol edilemedi (%s): %s", path, e)


class Store:
    def __init__(self, d: Path):
        d.mkdir(parents=True, exist_ok=True)
        self.dir = d
        self.events_path = d / "events.jsonl"
        self.liq_path = d / "liquidity.jsonl"
        self.ckpt_path = d / "checkpoint.json"
        self.heartbeat_path = d / "heartbeat.json"
        self.pools: dict[str, dict] = {}
        self.first_liq: dict[str, dict] = {}
        self.keys: set[str] = set()
        for rec in _read_jsonl(self.events_path):
            self.pools[rec["pool_id"]] = rec
            self.keys.add(f'{rec["tx"]}:{rec["log_index"]}')
        for rec in _read_jsonl(self.liq_path):
            self.first_liq[rec["pool_id"]] = rec
        self.ckpt: dict | None = None
        if self.ckpt_path.exists():
            self.ckpt = json.loads(self.ckpt_path.read_text())
        for p in (self.events_path, self.liq_path):
            _ensure_trailing_newline(p)
        self._fh: dict[Path, object] = {}
        self._dirty = False

    def _append(self, path: Path, rec: dict):
        fh = self._fh.get(path)
        if fh is None:
            fh = self._fh[path] = open(path, "a")
        fh.write(json.dumps(rec) + "\n")
        self._dirty = True  # fsync commit() içinde, checkpoint'ten hemen önce

    def commit(self):
        """Bekleyen JSONL kayıtlarını diske garantile. Checkpoint ilerlemeden ÖNCE çağrılmalı."""
        if not self._dirty:
            return
        for fh in self._fh.values():
            fh.flush()
            os.fsync(fh.fileno())
        self._dirty = False

    def close(self):
        try:
            self.commit()
        finally:
            for fh in self._fh.values():
                try:
                    fh.close()
                except OSError:
                    pass
            self._fh.clear()

    def add_init(self, rec: dict):
        self._append(self.events_path, rec)
        self.pools[rec["pool_id"]] = rec
        self.keys.add(f'{rec["tx"]}:{rec["log_index"]}')

    def add_first_liq(self, rec: dict):
        self._append(self.liq_path, rec)
        self.first_liq[rec["pool_id"]] = rec

    def save_ckpt(self):
        self.commit()  # veri, checkpoint'ten önce kalıcı olmalı (aksi halde olay kaybı olurdu)
        _atomic_write_json(self.ckpt_path, self.ckpt)

    def write_heartbeat(self, head: int, safe: int):
        ck = self.ckpt or {}
        nb = ck.get("next_block") or 0
        try:
            _atomic_write_json(self.heartbeat_path, {
                "ts": int(time.time()),
                "head": head,
                "safe": safe,
                "next_block": nb,
                "lag_blocks": max(0, safe - nb + 1),
                "pools": len(self.pools),
                "errors": ck.get("counters", {}).get("errors", 0),
            })
        except OSError as e:
            log.warning("heartbeat yazılamadı: %s", e)


async def process_logs(rpc: RPC, store: Store, logs: list[dict]):
    logs.sort(key=lambda lg: (int(lg["blockNumber"], 16), int(lg["logIndex"], 16)))
    c = store.ckpt["counters"]

    # Initialize bloklarının zaman damgalarını paralel çek (sıralı işleme sonuçları değiştirmez)
    init_blocks = {
        int(lg["blockNumber"], 16) for lg in logs
        if not lg.get("removed") and lg["topics"][0].lower() == INITIALIZE_TOPIC
    }
    if init_blocks:
        await rpc.prefetch_ts(init_blocks)

    for lg in logs:
        if lg.get("removed"):
            continue
        topic0 = lg["topics"][0].lower()
        block = int(lg["blockNumber"], 16)
        log_index = int(lg["logIndex"], 16)
        tx = lg["transactionHash"]

        if topic0 == INITIALIZE_TOPIC:
            if f"{tx}:{log_index}" in store.keys:
                continue  # tekrar (yeniden başlatma / örtüşen aralık)
            dec = decode_initialize(lg)
            if not dec:
                log.warning("Initialize çözülemedi: tx=%s idx=%d", tx[:12], log_index)
                continue
            ts = await rpc.block_ts(block)
            rec = {
                "ts": ts, "block": block, "tx": tx, "log_index": log_index,
                "contract": lg["address"].lower(), **dec,
                "dynamic_fee": dec["fee"] == DYNAMIC_FEE_FLAG,
                "has_hook": dec["hooks"] != ZERO_ADDR,
                "native_pair": ZERO_ADDR in (dec["currency0"], dec["currency1"]),
            }
            store.add_init(rec)
            lvl = logging.INFO if time.time() - ts < 600 else logging.DEBUG
            log.log(lvl, "HAVUZ #%d | blok=%d | %s / %s | fee=%s hook=%s",
                    len(store.pools), block, rec["currency0"][:10], rec["currency1"][:10],
                    "dyn" if rec["dynamic_fee"] else rec["fee"],
                    rec["hooks"][:10] if rec["has_hook"] else "-")

        elif topic0 == MODIFY_LIQUIDITY_TOPIC and TRACK_LIQUIDITY:
            dec = decode_modify_liquidity(lg)
            if not dec or dec["liquidity_delta"] <= 0:
                continue
            c["liq_adds_total"] += 1
            pid = dec["pool_id"]
            if pid not in store.pools:
                c["liq_adds_unknown_pool"] += 1  # ölçüm penceresinden önce açılmış havuz
                continue
            if pid in store.first_liq:
                continue
            store.add_first_liq({
                "pool_id": pid, "ts": await rpc.block_ts(block), "block": block, "tx": tx,
                "same_tx_as_init": tx == store.pools[pid]["tx"],
                "tick_lower": dec["tick_lower"], "tick_upper": dec["tick_upper"],
                "liquidity_delta": str(dec["liquidity_delta"]),
            })


async def poll_once(rpc: RPC, store: Store):
    head = await rpc.block_number()
    safe = head - CONFIRMATIONS
    ck = store.ckpt
    if safe < ck["next_block"]:
        store.write_heartbeat(head, safe)
        return
    async for lo, hi, logs in iter_log_chunks(rpc, ck["next_block"], safe):
        await process_logs(rpc, store, logs)
        ck["next_block"] = hi + 1
        ck["window_end_ts"] = await rpc.block_ts(hi)
        store.save_ckpt()  # önce JSONL fsync, sonra checkpoint: yarıda kalan backfill kaldığı yerden devam eder
        if logs or hi - lo >= 1000:
            log.info("blok %d→%d | %d log | toplam havuz=%d (span=%d)",
                     lo, hi, len(logs), len(store.pools), rpc.max_span)
    ck["counters"]["polls"] += 1
    store.save_ckpt()
    store.write_heartbeat(head, safe)


# --- Özet ---
def _pct(sorted_vals: list, p: float):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(round(p * (len(sorted_vals) - 1))))]


def _utc_hour(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:00Z")


def _utc_day(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def compute_summary(store: Store) -> dict:
    ck = store.ckpt or {}
    pools = sorted(store.pools.values(), key=lambda r: (r["ts"], r["log_index"]))
    n = len(pools)
    w0, w1 = ck.get("window_start_ts"), ck.get("window_end_ts")
    hours = (w1 - w0) / 3600 if (w0 and w1 and w1 > w0) else 0.0

    # Saatlik dağılım (boş saatler dahil) + günün saati
    per_hour = Counter(_utc_hour(r["ts"]) for r in pools)
    hourly = {}
    if hours > 0:
        t = (w0 // 3600) * 3600
        while t <= w1:
            hourly[_utc_hour(t)] = per_hour.get(_utc_hour(t), 0)
            t += 3600
    hour_of_day = Counter(datetime.fromtimestamp(r["ts"], timezone.utc).hour for r in pools)
    # Günlük dağılım (kısmi ilk/son gün ayrı işaretlenmez; 'hourly' ile birlikte yorumlanmalı)
    daily = dict(sorted(Counter(_utc_day(r["ts"]) for r in pools).items()))

    gaps = sorted(b["ts"] - a["ts"] for a, b in zip(pools, pools[1:]))

    tokens = Counter()
    for r in pools:
        for cur in (r["currency0"], r["currency1"]):
            if cur != ZERO_ADDR:
                tokens[cur] += 1
    launch_like = sum(
        1 for r in pools
        if any(cur != ZERO_ADDR and tokens[cur] == 1 for cur in (r["currency0"], r["currency1"]))
    )

    liq = {}
    if TRACK_LIQUIDITY:
        deltas = sorted(
            store.first_liq[p["pool_id"]]["ts"] - p["ts"]
            for p in pools if p["pool_id"] in store.first_liq
        )
        within = {}
        for T in LIQ_THRESHOLDS_SEC:
            # Sağdan kırpma: pencere sonuna T saniyeden az kalmış havuzlar henüz değerlendirilemez
            eligible = [p for p in pools if w1 and p["ts"] <= w1 - T]
            hit = sum(
                1 for p in eligible
                if p["pool_id"] in store.first_liq
                and store.first_liq[p["pool_id"]]["ts"] - p["ts"] <= T
            )
            within[str(T)] = {
                "eligible_pools": len(eligible), "with_liquidity": hit,
                "share": round(hit / len(eligible), 3) if eligible else None,
            }
        liq = {
            "pools_with_liquidity": len(deltas),
            "pools_without_liquidity": n - len(deltas),  # EKLENDİ: pencere sonuna kadar hiç likidite görmeyenler
            "same_tx_as_init": sum(1 for p in pools if store.first_liq.get(p["pool_id"], {}).get("same_tx_as_init")),
            "median_sec_to_first_liquidity": _pct(deltas, 0.5),
            "p90_sec_to_first_liquidity": _pct(deltas, 0.9),
            "added_within_sec": within,
            "liquidity_adds_total": ck.get("counters", {}).get("liq_adds_total", 0),
            "liquidity_adds_unknown_pool": ck.get("counters", {}).get("liq_adds_unknown_pool", 0),
        }

    return {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "window": {
            "start": datetime.fromtimestamp(w0, timezone.utc).isoformat() if w0 else None,
            "end": datetime.fromtimestamp(w1, timezone.utc).isoformat() if w1 else None,
            "hours": round(hours, 3),
            "blocks": [ck.get("start_block"), (ck.get("next_block") or 0) - 1],
        },
        "contract_filter": ck.get("contract_filter"),
        "total_initialize": n,
        "pools_per_hour": round(n / hours, 3) if hours else None,
        "pools_per_day": round(n / hours * 24, 2) if hours else None,
        "extrapolation_reliable": hours >= 24,
        "inter_arrival_sec": {
            "median": _pct(gaps, 0.5), "p90": _pct(gaps, 0.9), "max": gaps[-1] if gaps else None,
        },
        "hourly": hourly,
        "daily": daily,
        "hour_of_day_utc": {str(h): hour_of_day.get(h, 0) for h in range(24)},
        "by_contract": dict(Counter(r["contract"] for r in pools)),
        "block_range_seen": {
            "min": min((r["block"] for r in pools), default=None),
            "max": max((r["block"] for r in pools), default=None),
        },
        "pool_shape": {
            "with_hook": sum(1 for r in pools if r["has_hook"]),
            "dynamic_fee": sum(1 for r in pools if r["dynamic_fee"]),
            "native_pair": sum(1 for r in pools if r["native_pair"]),
            "top_hooks": Counter(r["hooks"] for r in pools if r["has_hook"]).most_common(10),
            "fees": dict(Counter(str(r["fee"]) for r in pools).most_common(10)),
        },
        "tokens": {
            "distinct": len(tokens),
            "top10_by_pool_count": tokens.most_common(10),
            "tokens_in_single_pool": sum(1 for v in tokens.values() if v == 1),
            # Vekil ölçü: penceredeki tek havuzu olan bir token içeren havuz. Pencere kısaysa şişer.
            "launch_like_pools_proxy": launch_like,
        },
        "liquidity": liq,
        "runtime": ck.get("counters", {}),
        "health": {"completed": None},  # write_summary doldurur
        "config": {
            "rpc": RPC_URL, "initialize_topic": INITIALIZE_TOPIC,
            "modify_liquidity_topic": MODIFY_LIQUIDITY_TOPIC,
            "confirmations": CONFIRMATIONS, "poll_interval_sec": POLL_INTERVAL_SEC,
            "track_liquidity": TRACK_LIQUIDITY,
            "final_max_span": None,  # write_summary doldurur
        },
    }


def write_summary(store: Store, rpc: RPC | None = None, *, quiet: bool = False,
                  completed: bool | None = None):
    if not store.ckpt:
        return
    summary = compute_summary(store)
    if rpc is not None:
        summary["config"]["final_max_span"] = rpc.max_span
    summary["health"]["completed"] = completed  # True: temiz bitti, False: hata/erken çıkış, None: ara özet
    _atomic_write_json(Path(SUMMARY_FILE), summary)
    if quiet:
        return
    log.info("=" * 60)
    log.info("ÖZET | pencere=%.2fsa | havuz=%d | %s/gün | güvenilir=%s",
             summary["window"]["hours"], summary["total_initialize"],
             summary["pools_per_day"], summary["extrapolation_reliable"])
    liq = summary["liquidity"]
    if liq:
        log.info("ÖZET | likidite eklenen=%d | aynı tx=%d | medyan=%ss",
                 liq["pools_with_liquidity"], liq["same_tx_as_init"], liq["median_sec_to_first_liquidity"])
    log.info("Tam özet: %s", SUMMARY_FILE)
    log.info("=" * 60)


# --- Ana akış ---
def _install_signals(stop: asyncio.Event):
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass


async def run() -> int:
    stop = asyncio.Event()
    _install_signals(stop)
    store = Store(DATA_DIR)
    rpc = RPC(RPC_URL)
    exit_code = 0
    completed: bool | None = None
    try:
        chain_id = int(await rpc.call("eth_chainId", []), 16)
        if chain_id != EXPECTED_CHAIN_ID:
            log.error("Yanlış zincir: %d (beklenen %d). RPC'yi kontrol et.", chain_id, EXPECTED_CHAIN_ID)
            return 2
        head = await rpc.block_number()

        if store.ckpt:
            if (store.ckpt.get("contract_filter") or "") != POOL_MANAGER:
                log.error("POOL_MANAGER filtresi checkpoint ile uyuşmuyor. Temiz ölçüm için DATA_DIR değiştir.")
                return 2
            log.info("Checkpoint bulundu → blok %d'den devam (%d havuz kayıtlı)",
                     store.ckpt["next_block"], len(store.pools))
        else:
            if START_BLOCK:
                start_block = int(START_BLOCK)
            elif BACKFILL_HOURS > 0:
                start_block = await estimate_start_block(rpc, head, BACKFILL_HOURS)
            else:
                start_block = head - CONFIRMATIONS + 1
            store.ckpt = {
                "started_at": time.time(),
                "start_block": start_block,
                "next_block": start_block,
                "window_start_ts": await rpc.block_ts(start_block),
                "window_end_ts": await rpc.block_ts(start_block),
                "contract_filter": POOL_MANAGER,
                "counters": {"polls": 0, "errors": 0, "liq_adds_total": 0, "liq_adds_unknown_pool": 0},
            }
            store.save_ckpt()

        log.info("=" * 60)
        log.info("Aşama 0 — havuz sıklığı ölçümü")
        log.info("Kontrat:    %s", POOL_MANAGER or "(KEŞİF — adres filtresi yok)")
        log.info("RPC:        %s (chain %d)", RPC_URL, chain_id)
        log.info("Başlangıç:  blok %d | head %d | geriye dönük ≈ %d blok",
                 store.ckpt["next_block"], head, max(0, head - store.ckpt["next_block"]))
        log.info("Likidite:   %s | confirmations=%d", "izleniyor" if TRACK_LIQUIDITY else "kapalı", CONFIRMATIONS)
        log.info("Initialize: %s", INITIALIZE_TOPIC)
        log.info("Süre:       %.4f saat canlı", DURATION_HOURS)
        log.info("=" * 60)

        end_at = store.ckpt["started_at"] + DURATION_HOURS * 3600
        last_progress = time.time()
        consecutive_errors = 0

        while True:
            poll_ok = False
            try:
                await poll_once(rpc, store)
                consecutive_errors = 0
                poll_ok = True
            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_errors += 1
                store.ckpt["counters"]["errors"] += 1
                log.warning("poll hatası: %s (%s) [üst üste %d]",
                            str(e).splitlines()[0][:120], type(e).__name__, consecutive_errors)

            if stop.is_set():
                completed = poll_ok
                break
            if time.time() >= end_at:
                # Süre doldu. Son poll başarılıysa temiz çık; hata varsa (sonlu/backfill modda
                # veri eksik kalmasın diye) MAX_CONSECUTIVE_ERRORS'a kadar tekrar dene.
                if poll_ok:
                    completed = True
                    break
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    log.error("Süre doldu ama %d üst üste hata: ölçüm tamamlanamadı. "
                              "Aynı komutu tekrar çalıştır; checkpoint'ten devam eder.", consecutive_errors)
                    completed = False
                    exit_code = 3
                    break
            if time.time() - last_progress > PROGRESS_INTERVAL_SEC:
                ck = store.ckpt
                hrs = max((ck["window_end_ts"] - ck["window_start_ts"]) / 3600, 1e-9)
                log.info("--- PROGRESS | pencere %.2fsa | havuz=%d | %.1f/gün | likiditeli=%d | "
                         "span=%d | hata=%d ---",
                         hrs, len(store.pools), len(store.pools) / hrs * 24,
                         len(store.first_liq), rpc.max_span, ck["counters"]["errors"])
                try:
                    write_summary(store, rpc, quiet=True)  # kısmi özet: süreç öldürülse bile elde bir şey kalır
                except Exception as e:  # özet hatası ölçümü durdurmamalı
                    log.warning("ara özet yazılamadı: %s", e)
                last_progress = time.time()

            wait = min(120.0, POLL_INTERVAL_SEC * (2 ** min(consecutive_errors, 4)))
            try:
                await asyncio.wait_for(stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass
        return exit_code
    finally:
        try:
            if store.ckpt:
                store.save_ckpt()
            write_summary(store, rpc, completed=completed if completed is not None else False)
        finally:
            try:
                store.close()
            finally:
                await rpc.aclose()


# --- Çevrimdışı self-test (ağ yok) ---
def selftest() -> int:
    def pad_addr(a: str) -> str:
        return "0x" + a[2:].rjust(64, "0")

    def w(v: int) -> str:
        return format(v & ((1 << 256) - 1), "064x")

    c0, c1 = "0x" + "00" * 20, "0x" + "ab" * 20
    hook = "0x" + "cd" * 20
    init_log = {
        "topics": [INITIALIZE_TOPIC, "0x" + "11" * 32, pad_addr(c0), pad_addr(c1)],
        "data": "0x" + w(3000) + w(60) + w(int(hook, 16)) + w(79228162514264337593543950336) + w(-5),
    }
    d = decode_initialize(init_log)
    assert d and d["fee"] == 3000 and d["tick_spacing"] == 60 and d["tick"] == -5, d
    assert d["hooks"] == hook and d["currency1"] == c1 and d["currency0"] == c0, d

    liq_log = {
        "topics": [MODIFY_LIQUIDITY_TOPIC, "0x" + "11" * 32, pad_addr(c1)],
        "data": "0x" + w(-120) + w(120) + w(10**18) + w(0),
    }
    m = decode_modify_liquidity(liq_log)
    assert m and m["tick_lower"] == -120 and m["tick_upper"] == 120 and m["liquidity_delta"] == 10**18, m

    assert decode_initialize({"topics": [], "data": "0x"}) is None
    assert INITIALIZE_TOPIC != MODIFY_LIQUIDITY_TOPIC and len(INITIALIZE_TOPIC) == 66

    # hata sınıflandırma
    assert _is_range_error(RPCError({"code": -32005, "message": "query returned more than 10000 results"}))
    assert not _is_range_error(RPCError({"code": 429, "message": "rate limit exceeded"}))
    assert _is_rate_limit(RPCError({"message": "Too Many Requests"}))

    # span küçültme / büyütme
    class _R(RPC):
        def __init__(self):
            self.max_span, self._ok_streak, self._grow_after = 1000, 0, _GROW_AFTER_START

    r = _R()
    assert r.shrink_span(1000) == 500 and r._grow_after == 2 * _GROW_AFTER_START
    for _ in range(r._grow_after):
        r.note_success(r.max_span)
    assert r.max_span > 500
    print("selftest OK")
    return 0


def main():
    if "--selftest" in sys.argv[1:]:
        sys.exit(selftest())
    setup_logging()
    try:
        sys.exit(asyncio.run(run()))
    except KeyboardInterrupt:
        log.info("Kullanıcı durdurdu")


if __name__ == "__main__":
    main()
