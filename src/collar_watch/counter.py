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

Kullanım:
    python -m collar_watch.counter                                  # 24 saat canlı
    BACKFILL_HOURS=168 DURATION_HOURS=0 python -m collar_watch.counter   # son 7 günü tara, çık
    BACKFILL_HOURS=48 DURATION_HOURS=24 python -m collar_watch.counter   # 48s geçmiş + 24s canlı
    START_BLOCK=1234567 DURATION_HOURS=0 python -m collar_watch.counter  # belirli bloktan
    POOL_MANAGER=0x... python -m collar_watch.counter                    # tek kontrat filtresi
    DATA_DIR=run2 python -m collar_watch.counter                         # yeni, temiz ölçüm

Ortam değişkenleri:
    ROBINHOOD_RPC, POOL_MANAGER, DURATION_HOURS, BACKFILL_HOURS, START_BLOCK,
    POLL_INTERVAL_SEC (15), CONFIRMATIONS (2), MAX_BLOCK_RANGE (5000),
    TRACK_LIQUIDITY (1), EXPECTED_CHAIN_ID (4663), DATA_DIR (data),
    COUNTER_LOG (counter.log), COUNTER_SUMMARY (counter-summary.json),
    PROGRESS_INTERVAL_SEC (900)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
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

# Uniswap V4 PoolManager olayları (kanonik imzalar: PoolId=bytes32, Currency=address)
INITIALIZE_SIG = "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
MODIFY_LIQUIDITY_SIG = "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)"
INITIALIZE_TOPIC = "0x" + keccak(text=INITIALIZE_SIG).hex()
MODIFY_LIQUIDITY_TOPIC = "0x" + keccak(text=MODIFY_LIQUIDITY_SIG).hex()

ZERO_ADDR = "0x" + "0" * 40
DYNAMIC_FEE_FLAG = 0x800000  # V4 LPFeeLibrary.DYNAMIC_FEE_FLAG
LIQ_THRESHOLDS_SEC = (0, 60, 600, 3600, 86400)

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


def _is_range_error(e: RPCError) -> bool:
    m = str(e).lower()
    return any(k in m for k in (
        "range", "limit", "too many", "exceed", "too large", "10000", "response size", "more than",
    ))


class RPC:
    def __init__(self, url: str, timeout: float = 30):
        self.url = url
        self.client = httpx.AsyncClient(timeout=timeout)  # tek client, bağlantı yeniden kullanılır
        self._id = 0
        self.max_span = MAX_BLOCK_RANGE
        self._ts: dict[int, int] = {}

    async def aclose(self):
        await self.client.aclose()

    async def call(self, method: str, params: list, retries: int = 4):
        for attempt in range(retries + 1):
            try:
                self._id += 1
                r = await self.client.post(self.url, json={
                    "jsonrpc": "2.0", "id": self._id, "method": method, "params": params,
                })
                r.raise_for_status()
                data = r.json()
            except (httpx.HTTPError, ValueError) as e:  # 429/5xx/ağ/JSON dışı yanıt
                if attempt == retries:
                    raise
                wait = 2 ** attempt
                log.warning("%s geçici hata: %s (%s) — %ds sonra tekrar", method, e, type(e).__name__, wait)
                await asyncio.sleep(wait)
                continue
            if "error" in data:
                raise RPCError(data["error"])
            return data["result"]

    async def block_number(self) -> int:
        return int(await self.call("eth_blockNumber", []), 16)

    async def block_ts(self, number: int) -> int:
        if number in self._ts:
            return self._ts[number]
        blk = await self.call("eth_getBlockByNumber", [hex(number), False])
        if not blk:
            raise RuntimeError(f"blok bulunamadı: {number}")
        ts = int(blk["timestamp"], 16)
        if len(self._ts) > 5000:
            self._ts.clear()
        self._ts[number] = ts
        return ts


async def iter_log_chunks(rpc: RPC, start: int, end: int):
    """[start, end] aralığını parça parça getirir; RPC 'aralık çok büyük' derse parçayı küçültür."""
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
            if span > 1 and _is_range_error(e):
                rpc.max_span = max(1, span // 2)
                log.info("getLogs aralığı küçültüldü: %d → %d blok (%s)", span, rpc.max_span, e)
                continue
            raise
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


class Store:
    def __init__(self, d: Path):
        d.mkdir(parents=True, exist_ok=True)
        self.events_path = d / "events.jsonl"
        self.liq_path = d / "liquidity.jsonl"
        self.ckpt_path = d / "checkpoint.json"
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

    def _append(self, path: Path, rec: dict):
        with open(path, "a") as f:
            f.write(json.dumps(rec) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def add_init(self, rec: dict):
        self._append(self.events_path, rec)
        self.pools[rec["pool_id"]] = rec
        self.keys.add(f'{rec["tx"]}:{rec["log_index"]}')

    def add_first_liq(self, rec: dict):
        self._append(self.liq_path, rec)
        self.first_liq[rec["pool_id"]] = rec

    def save_ckpt(self):
        _atomic_write_json(self.ckpt_path, self.ckpt)


async def process_logs(rpc: RPC, store: Store, logs: list[dict]):
    logs.sort(key=lambda lg: (int(lg["blockNumber"], 16), int(lg["logIndex"], 16)))
    c = store.ckpt["counters"]
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
        return
    async for lo, hi, logs in iter_log_chunks(rpc, ck["next_block"], safe):
        await process_logs(rpc, store, logs)
        ck["next_block"] = hi + 1
        ck["window_end_ts"] = await rpc.block_ts(hi)
        store.save_ckpt()  # her parçadan sonra: uzun backfill yarıda kalırsa kaldığı yerden devam
        if logs or hi - lo >= 1000:
            log.info("blok %d→%d | %d log | toplam havuz=%d", lo, hi, len(logs), len(store.pools))
    ck["counters"]["polls"] += 1
    store.save_ckpt()


# --- Özet ---
def _pct(sorted_vals: list, p: float):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(round(p * (len(sorted_vals) - 1))))]


def _utc_hour(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:00Z")


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
        "config": {
            "rpc": RPC_URL, "initialize_topic": INITIALIZE_TOPIC,
            "modify_liquidity_topic": MODIFY_LIQUIDITY_TOPIC,
            "confirmations": CONFIRMATIONS, "poll_interval_sec": POLL_INTERVAL_SEC,
            "track_liquidity": TRACK_LIQUIDITY,
        },
    }


def write_summary(store: Store):
    if not store.ckpt:
        return
    summary = compute_summary(store)
    _atomic_write_json(Path(SUMMARY_FILE), summary)
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
            try:
                await poll_once(rpc, store)
                consecutive_errors = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_errors += 1
                store.ckpt["counters"]["errors"] += 1
                log.warning("poll hatası: %s (%s) [üst üste %d]", e, type(e).__name__, consecutive_errors)

            if stop.is_set() or time.time() >= end_at:
                break
            if time.time() - last_progress > PROGRESS_INTERVAL_SEC:
                ck = store.ckpt
                hrs = max((ck["window_end_ts"] - ck["window_start_ts"]) / 3600, 1e-9)
                log.info("--- PROGRESS | pencere %.2fsa | havuz=%d | %.1f/gün | likiditeli=%d | hata=%d ---",
                         hrs, len(store.pools), len(store.pools) / hrs * 24,
                         len(store.first_liq), ck["counters"]["errors"])
                last_progress = time.time()

            wait = min(120.0, POLL_INTERVAL_SEC * (2 ** min(consecutive_errors, 4)))
            try:
                await asyncio.wait_for(stop.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass
        return 0
    finally:
        try:
            if store.ckpt:
                store.save_ckpt()
            write_summary(store)
        finally:
            await rpc.aclose()


def main():
    setup_logging()
    try:
        sys.exit(asyncio.run(run()))
    except KeyboardInterrupt:
        log.info("Kullanıcı durdurdu")


if __name__ == "__main__":
    main()
