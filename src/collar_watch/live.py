"""live.py - canlı Initialize sampler (Robinhood Chain, Uniswap V4).

Akış (her yeni havuz için):
  init kaydı -> T0 hemen -> T3m (T0 çağrısından LIVE_DELAY_SEC sonra)

T24h ve outcomes ayrı cron'da çalışır; indeks olarak live-events.jsonl'deki
"init" satırları kullanılır.

Append-only JSONL (EVENTS). Satır tipleri (stage): init | skip | t0 | t3m
Restart güvenli: açılışta EVENTS okunur, bitmemiş T0/T3m işleri devam eder.

Gerekli env:
  POOL_MANAGER   Uniswap V4 PoolManager adresi
  QUOTE_ADDRS    virgülle ayrılmış quote token adresleri (USDG, WETH, native 0x00..)
Opsiyonel:
  RPC_URL, INIT_TOPIC0, HOOK_LABELS ("0xabc:pons_v2,0xdef:x"), POLL_SEC,
  REORG_DEPTH, MAX_BLOCK_RANGE, LIVE_DELAY_SEC, EVENTS, STATE

Kullanım:
    python -u -m collar_watch.live
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from collar_watch import collar as C   # _do_call, rate_limiter, preflight, mcp_initialize

RPC_URL = os.getenv("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
POOL_MANAGER = os.getenv("POOL_MANAGER", "").strip().lower()
# Uniswap V4 Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)
# counter.py'deki sabitle karşılaştır; farklıysa INIT_TOPIC0 env'ini ver.
INIT_TOPIC0 = os.getenv(
    "INIT_TOPIC0",
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438",
).strip().lower()
QUOTES = {a.strip().lower() for a in os.getenv("QUOTE_ADDRS", "").split(",") if a.strip()}
HOOK_LABELS = {}
for pair in os.getenv("HOOK_LABELS", "").split(","):
    if ":" in pair:
        a, name = pair.split(":", 1)
        HOOK_LABELS[a.strip().lower()] = name.strip()

POLL_SEC = float(os.getenv("POLL_SEC", "15"))
REORG_DEPTH = int(os.getenv("REORG_DEPTH", "5"))
MAX_BLOCK_RANGE = int(os.getenv("MAX_BLOCK_RANGE", "2000"))
LIVE_DELAY_SEC = float(os.getenv("LIVE_DELAY_SEC", "180"))
# 0 = sonsuz (systemd). >0 = bu kadar saniye sonra poll'u durdur, bekleyen T3m'leri bitir, çık (Actions).
MAX_RUNTIME_SEC = float(os.getenv("MAX_RUNTIME_SEC", "0"))
DRAIN_SEC = LIVE_DELAY_SEC + 90
EVENTS = Path(os.getenv("EVENTS", "live-events.jsonl"))
STATE = Path(os.getenv("STATE", "live-state.json"))

_write_lock = asyncio.Lock()
_block_ts_cache: dict[int, int] = {}


def log(msg: str):
    print(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {msg}", flush=True)


# ---------- RPC ----------

async def rpc(client: httpx.AsyncClient, method: str, params: list, retries: int = 4):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last = None
    for a in range(retries + 1):
        try:
            r = await client.post(RPC_URL, json=payload)
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                await asyncio.sleep(2.0 * (a + 1))
                continue
            d = r.json()
            if "error" in d:
                raise RuntimeError(f"{method}: {d['error']}")
            return d["result"]
        except (httpx.TransportError, ValueError) as e:
            last = f"{type(e).__name__}: {e}"
            await asyncio.sleep(2.0 * (a + 1))
    raise RuntimeError(f"rpc {method} başarısız: {last}")


async def block_ts(client: httpx.AsyncClient, number: int) -> int | None:
    if number in _block_ts_cache:
        return _block_ts_cache[number]
    try:
        b = await rpc(client, "eth_getBlockByNumber", [hex(number), False])
        ts = int(b["timestamp"], 16)
        _block_ts_cache[number] = ts
        if len(_block_ts_cache) > 2000:
            _block_ts_cache.pop(next(iter(_block_ts_cache)))
        return ts
    except Exception as e:
        log(f"block_ts hata {number}: {e}")
        return None


# ---------- decode ----------

def decode_init(lg: dict) -> dict:
    t = lg["topics"]
    data = lg["data"][2:]
    word = lambda i: data[i * 64:(i + 1) * 64]
    return {
        "pool_id": t[1].lower(),
        "c0": ("0x" + t[2][-40:]).lower(),
        "c1": ("0x" + t[3][-40:]).lower(),
        "fee": int(word(0), 16),
        "hook": ("0x" + word(2)[-40:]).lower(),
        "block": int(lg["blockNumber"], 16),
        "tx": lg["transactionHash"],
    }


def split_pair(c0: str, c1: str):
    """(token, quote) döner; çift quote/token ayrımı yapılamıyorsa None."""
    if c0 in QUOTES and c1 not in QUOTES:
        return c1, c0
    if c1 in QUOTES and c0 not in QUOTES:
        return c0, c1
    return None


# ---------- storage ----------

async def emit(rec: dict):
    rec["written_at"] = datetime.now(timezone.utc).isoformat()
    async with _write_lock:
        with open(EVENTS, "a") as f:
            f.write(json.dumps(rec) + "\n")
            f.flush()


def load_events() -> dict[str, dict]:
    """pool_id -> {"init":..., "t0":..., "t3m":..., "skip":...}"""
    pools: dict[str, dict] = {}
    if not EVENTS.exists():
        return pools
    with open(EVENTS) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            pid = r.get("pool_id")
            if pid and r.get("stage"):
                pools.setdefault(pid, {})[r["stage"]] = r
    return pools


def load_state() -> dict:
    try:
        return json.loads(STATE.read_text())
    except Exception:
        return {}


def save_state(state: dict):
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE)


# ---------- per-pool pipeline ----------

async def run_pool(client, session_id, init: dict, t0_rec: dict | None = None):
    pid, token = init["pool_id"], init["token"]

    if t0_rec is None:
        s0 = {}
        res0 = await C._do_call(client, token, session_id, {}, s0)
        called0 = s0.get("t", time.time())
        bts = init.get("block_ts")
        t0_rec = {
            "stage": "t0", "pool_id": pid, "called_at": called0,
            "age_at_t0_sec": int(called0 - bts) if bts else None,
            "detect_lag_sec": int(called0 - init["detected_at"]),
            "result": res0,
        }
        await emit(t0_rec)
        log(f"T0 {token[:10]}... {res0['decision']} (age {t0_rec['age_at_t0_sec']}s)")

    wait = t0_rec["called_at"] + LIVE_DELAY_SEC - time.time()
    if wait > 0:
        await asyncio.sleep(wait)

    s3 = {}
    res3 = await C._do_call(client, token, session_id, {}, s3)
    called3 = s3.get("t", time.time())
    d0, d3 = t0_rec["result"]["decision"], res3["decision"]
    ok = d0 in C.OK_DECISIONS and d3 in C.OK_DECISIONS
    await emit({
        "stage": "t3m", "pool_id": pid, "called_at": called3,
        "gap_sec": int(called3 - t0_rec["called_at"]),
        "changed": bool(ok and d0 != d3),
        "result": res3,
    })
    log(f"T3m {token[:10]}... {d3} (gap {int(called3 - t0_rec['called_at'])}s)")


def _spawn(tasks: set, coro):
    t = asyncio.create_task(coro)
    tasks.add(t)

    def _done(task):
        tasks.discard(task)
        if not task.cancelled() and task.exception():
            log(f"pool task hata: {task.exception()!r}")

    t.add_done_callback(_done)


# ---------- poll ----------

async def poll_once(client, session_id, seen: set, tasks: set, state: dict):
    latest = int(await rpc(client, "eth_blockNumber", []), 16)
    last = state.get("last_block")
    if last is None:
        last = latest            # canlı mod: geçmişi alma
    start = max(0, last - REORG_DEPTH + 1)

    frm = start
    while frm <= latest:
        to = min(frm + MAX_BLOCK_RANGE - 1, latest)
        logs = await rpc(client, "eth_getLogs", [{
            "address": POOL_MANAGER, "topics": [INIT_TOPIC0],
            "fromBlock": hex(frm), "toBlock": hex(to),
        }])
        for lg in logs:
            d = decode_init(lg)
            pid = d["pool_id"]
            if pid in seen:
                continue
            seen.add(pid)
            detected = time.time()
            pair = split_pair(d["c0"], d["c1"])
            if pair is None:
                await emit({"stage": "skip", "pool_id": pid, "reason": "no_single_quote",
                            "c0": d["c0"], "c1": d["c1"], "block": d["block"],
                            "hook": d["hook"]})
                continue
            token, quote = pair
            init = {
                "stage": "init", "pool_id": pid, "token": token, "quote": quote,
                "hook": HOOK_LABELS.get(d["hook"], d["hook"]), "hook_addr": d["hook"],
                "fee": d["fee"], "block": d["block"], "tx": d["tx"],
                "block_ts": await block_ts(client, d["block"]),
                "detected_at": detected,
            }
            await emit(init)
            log(f"yeni havuz {pid[:10]}... token={token[:10]}... hook={init['hook']}")
            _spawn(tasks, run_pool(client, session_id, init))
        frm = to + 1

    state["last_block"] = latest     # sadece tüm aralık başarılıysa ilerler
    save_state(state)
    return latest


async def main():
    if not POOL_MANAGER or not QUOTES:
        print("HATA: POOL_MANAGER ve QUOTE_ADDRS env'leri gerekli.", file=sys.stderr)
        sys.exit(1)

    log(f"live sampler: rpc={RPC_URL} pm={POOL_MANAGER[:10]}... "
        f"quotes={len(QUOTES)} delay={LIVE_DELAY_SEC}s poll={POLL_SEC}s")

    async with httpx.AsyncClient(timeout=C.TIMEOUT_SEC) as client:
        schema, err = await C.preflight(client, None)
        if err:
            print(f"HATA: preflight: {err}", file=sys.stderr)
            sys.exit(2)
        session_id, init_err = await C.mcp_initialize(client)
        if init_err:
            print(f"HATA: MCP initialize: {init_err}", file=sys.stderr)
            sys.exit(2)

        existing = load_events()
        seen = set(existing.keys())
        tasks: set = set()
        state = load_state()

        # resume: bitmemiş T0/T3m işleri
        resumed = 0
        for pid, st in existing.items():
            init = st.get("init")
            if init and "t3m" not in st:
                _spawn(tasks, run_pool(client, session_id, init, st.get("t0")))
                resumed += 1
        if resumed:
            log(f"resume: {resumed} havuz devam ediyor")

        deadline = time.time() + MAX_RUNTIME_SEC if MAX_RUNTIME_SEC > 0 else None
        last_hb = 0.0
        while deadline is None or time.time() < deadline:
            try:
                latest = await poll_once(client, session_id, seen, tasks, state)
                if time.time() - last_hb > 300:
                    log(f"heartbeat block={latest} seen={len(seen)} pending={len(tasks)}")
                    last_hb = time.time()
            except Exception as e:
                log(f"poll hata: {type(e).__name__}: {e}")
            await asyncio.sleep(POLL_SEC)

        # süre doldu: yeni havuz alma, uçuştaki T0/T3m'leri bitir
        log(f"süre doldu, {len(tasks)} bekleyen iş bitiriliyor (<= {DRAIN_SEC:.0f}s)")
        if tasks:
            await asyncio.wait(set(tasks), timeout=DRAIN_SEC)
        log("çıkış")


if __name__ == "__main__":
    asyncio.run(main())
