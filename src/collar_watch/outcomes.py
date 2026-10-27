"""outcomes.py - v0.4.2 (Robinhood Chain USDG Odaklı, Gelişmiş Adli Analiz - RPC Fix)"""
import asyncio
import json
import math
import os
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import httpx

# --- Yapılandırma ---
RPC_URL = os.getenv("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
POOL_MANAGER = os.getenv("POOL_MANAGER", "").strip().lower()
SOURCES = [s.strip() for s in os.getenv("SOURCES", "backfill,live").split(",") if s.strip()]
LIVE_EVENTS = Path(os.getenv("LIVE_EVENTS", "live-events.jsonl"))
BACKFILL_EVENTS = Path(os.getenv("BACKFILL_EVENTS", "data/events.jsonl"))
EVALUATIONS = Path(os.getenv("EVALUATIONS", "evaluations.jsonl"))
OUT = Path(os.getenv("OUTCOMES_OUT", "outcomes.json"))
OUT_JSONL = Path(os.getenv("OUTCOMES_OUT_JSONL", "outcomes.jsonl"))

LP_PULL_THRESHOLD = float(os.getenv("LP_PULL_THRESHOLD", "0.90"))
PRICE_CRASH_THRESHOLD = float(os.getenv("PRICE_CRASH_THRESHOLD", "0.95"))
LP_WINDOW_SEC = float(os.getenv("LP_WINDOW_SEC", "86400"))
START_RANGE = int(os.getenv("START_RANGE", "10000"))
MIN_RANGE = int(os.getenv("MIN_RANGE", "500"))
RPC_RPS = float(os.getenv("RPC_RPS", "2"))  # Düşürüldü (5 -> 2)

# Uniswap V4 Event Topic'leri
MODIFY_LIQ_TOPIC = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
SWAP_TOPIC = "0x40e9cecbdb1f4b7d12a5d6bb2f4c35fd8e1c5f7bdfbd89c2590bf31bc92bd6d8"

# USDG (6 decimals) - Robinhood Chain resmi kontrat adresi
USDG_ADDRESS = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"

DEFAULT_QUOTES = (
    f"{USDG_ADDRESS},"
    "0x0000000000000000000000000000000000000000"
)
QUOTES = {a.strip().lower() for a in (os.getenv("QUOTE_ADDRS") or DEFAULT_QUOTES).split(",") if a.strip()}

HOOK_LABELS: dict[str, str] = {}
for _pair in os.getenv("HOOK_LABELS", "").split(","):
    if ":" in _pair:
        _a, _n = _pair.split(":", 1)
        HOOK_LABELS[_a.strip().lower()] = _n.strip()

class RangeError(RuntimeError):
    pass

_rate = {"next": 0.0}
_rate_lock = asyncio.Lock()

async def _throttle():
    if RPC_RPS <= 0: return
    async with _rate_lock:
        now = time.monotonic()
        wait = _rate["next"] - now
        _rate["next"] = max(now, _rate["next"]) + 1.0 / RPC_RPS
    if wait > 0: await asyncio.sleep(wait)

async def rpc(client: httpx.AsyncClient, method: str, params: list, retries: int = 6):
    last = None
    for a in range(retries + 1):
        try:
            await _throttle()
            r = await client.post(RPC_URL, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                try:
                    wait = float(r.headers.get("retry-after", 0))
                except (ValueError, TypeError):
                    wait = 0
                if wait <= 0:
                    wait = (2 ** a) + random.uniform(0, 1)
                wait = min(wait, 60)
                print(f"  RPC {method} HTTP {r.status_code} → {wait:.1f}s bekleniyor (deneme {a+1}/{retries})", file=sys.stderr)
                await asyncio.sleep(wait)
                continue
            d = r.json()
            if "error" in d:
                raise RangeError(f"{method}: {str(d['error'])[:200]}")
            return d["result"]
        except (httpx.TransportError, ValueError) as e:
            last = f"{type(e).__name__}: {e}"
            await asyncio.sleep(2.0 * (a + 1))
    raise RuntimeError(f"rpc {method} başarısız: {last}")

def _jsonl(path: Path):
    if not path.exists(): return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try: yield json.loads(line)
                except json.JSONDecodeError: continue

def hook_label(addr: str | None) -> str:
    a = (addr or "").lower()
    return HOOK_LABELS.get(a, a or "none")

def load_backfill_pools() -> list[dict]:
    out = []
    for r in _jsonl(BACKFILL_EVENTS):
        c0 = (r.get("currency0") or "").lower()
        c1 = (r.get("currency1") or "").lower()
        token_is_c0 = False
        if c0 in QUOTES and c1 not in QUOTES: token = c1
        elif c1 in QUOTES and c0 not in QUOTES: token = c0; token_is_c0 = True
        else: continue
        pid = (r.get("pool_id") or "").lower()
        if not pid or r.get("block") is None: continue
        hook_addr = (r.get("hooks") or "").lower()
        out.append({"pool_id": pid, "token": token, "token_is_c0": token_is_c0,
                    "hook_addr": hook_addr, "hook": hook_label(hook_addr), 
                    "block": int(r["block"]), "source": "backfill"})
    return out

def load_live_pools() -> list[dict]:
    pools: dict[str, dict] = {}
    for r in _jsonl(LIVE_EVENTS):
        pid = (r.get("pool_id") or "").lower()
        if not pid: continue
        st = pools.setdefault(pid, {})
        if r.get("stage") == "init":
            hook_addr = (r.get("hook_addr") or "").lower()
            st.update({"pool_id": pid, "token": r.get("token"), "token_is_c0": r.get("token_is_c0", False),
                       "hook_addr": hook_addr, "hook": r.get("hook") or hook_label(hook_addr), 
                       "block": r.get("block"), "source": "live"})
        elif r.get("stage") == "t0":
            st["collar_t0"] = (r.get("result") or {}).get("decision")
            st["collar_sell_sim_ok"] = (r.get("result") or {}).get("sell_simulation_ok")
    return [p for p in pools.values() if p.get("token") and p.get("block") is not None]

def load_pools() -> list[dict]:
    merged: dict[str, dict] = {}
    for src in SOURCES:
        if src == "backfill": batch = load_backfill_pools()
        elif src == "live": batch = load_live_pools()
        else: continue
        for p in batch: merged[p["pool_id"]] = p
    return list(merged.values())

def attach_collar_sell_sim(pools: list[dict]):
    ev = {}
    for r in _jsonl(EVALUATIONS):
        pid = (r.get("pool_id") or "").lower()
        if pid: ev[pid] = (r.get("t0") or {}).get("sell_simulation_ok")
    for p in pools:
        if "collar_sell_sim_ok" not in p and p["pool_id"] in ev: p["collar_sell_sim_ok"] = ev[p["pool_id"]]

def decode_modify_liquidity(lg: dict) -> tuple[str, int, str] | None:
    topics = lg.get("topics", [])
    if len(topics) < 3: return None
    data = lg["data"][2:]
    if len(data) < 192: return None
    delta_hex = data[128:192]
    v = int(delta_hex, 16)
    delta = v - (1 << 256) if v >= (1 << 255) else v
    sender = "0x" + topics[2][-40:].lower()
    return topics[1].lower(), delta, sender

def decode_swap(lg: dict) -> tuple[str, str, int, int, float] | None:
    topics = lg.get("topics", [])
    if len(topics) < 3: return None
    data = lg["data"][2:]
    if len(data) < 192: return None
    amt0 = int(data[0:64], 16)
    if amt0 >= (1 << 255): amt0 -= (1 << 256)
    amt1 = int(data[64:128], 16)
    if amt1 >= (1 << 255): amt1 -= (1 << 256)
    sqrt_price = int(data[128:192], 16)
    sender = "0x" + topics[2][-40:].lower()
    return topics[1].lower(), sender, amt0, amt1, float(sqrt_price)

async def scan_chain(client: httpx.AsyncClient, from_block: int, to_block: int):
    liq_series = defaultdict(list)
    swap_series = defaultdict(list)
    size, cap, frm, ok_streak, calls = START_RANGE, START_RANGE, from_block, 0, 0
    t0 = time.time()
    while frm <= to_block:
        to = min(frm + size - 1, to_block)
        try:
            logs = await rpc(client, "eth_getLogs", [{
                "address": POOL_MANAGER, 
                "topics": [[MODIFY_LIQ_TOPIC, SWAP_TOPIC]],
                "fromBlock": hex(frm), "toBlock": hex(to)}])
        except RangeError as e:
            if size <= MIN_RANGE: raise RuntimeError(f"getLogs {frm}->{to} en küçük aralıkta başarısız: {e}")
            size = max(MIN_RANGE, size // 2)
            cap, ok_streak = size, 0
            print(f"  aralık {size}'e düşürüldü ({str(e)[:90]})", file=sys.stderr)
            continue
        except RuntimeError as e:
            if "429" in str(e):
                print(f"  {frm}->{to} için RPC 429, 30s bekleyip tekrar deniyorum", file=sys.stderr)
                await asyncio.sleep(30)
                continue
            raise
        calls += 1
        for lg in logs:
            topic0 = lg["topics"][0].lower()
            if topic0 == MODIFY_LIQ_TOPIC:
                d = decode_modify_liquidity(lg)
                if d: liq_series[d[0]].append((int(lg["blockNumber"], 16), d[1], d[2]))
            elif topic0 == SWAP_TOPIC:
                d = decode_swap(lg)
                if d: swap_series[d[0]].append((int(lg["blockNumber"], 16), d[1], d[2], d[3], d[4]))
        frm = to + 1
        ok_streak += 1
        if ok_streak >= 15 and cap < START_RANGE:
            cap = min(START_RANGE, int(cap * 1.5))
            size, ok_streak = cap, 0
        if calls % 20 == 0:
            print(f"  tarama %{(frm - from_block) / max(1, to_block - from_block + 1) * 100:.0f} | çağrı={calls} | likidite={len(liq_series)} | swap={len(swap_series)}", flush=True)
    for pid in liq_series: liq_series[pid].sort(key=lambda x: x[0])
    for pid in swap_series: swap_series[pid].sort(key=lambda x: x[0])
    print(f"tarama bitti: {calls} çağrı, likidite={len(liq_series)}, swap={len(swap_series)}, {int(time.time() - t0)}s")
    return liq_series, swap_series

async def blocks_per_sec(client: httpx.AsyncClient, from_block: int, head: int) -> float:
    a = await rpc(client, "eth_getBlockByNumber", [hex(from_block), False])
    b = await rpc(client, "eth_getBlockByNumber", [hex(head), False])
    dt = int(b["timestamp"], 16) - int(a["timestamp"], 16)
    if dt <= 0 or head <= from_block: raise RuntimeError("blok hızı hesaplanamadı")
    return (head - from_block) / dt

def analyze_pool(liq_events: list, swap_events: list, init_block: int, window_blocks: int, token_is_c0: bool) -> dict:
    end = init_block + window_blocks
    cum = peak_liq = 0
    max_dd_liq = 0.0
    n_add = n_rem = 0
    for blk, delta, sender in liq_events:
        if blk < init_block: continue
        if blk > end: break
        cum += delta
        if delta > 0: n_add += 1
        elif delta < 0: n_rem += 1
        if cum > peak_liq: peak_liq = cum
        elif peak_liq > 0: max_dd_liq = max(max_dd_liq, min(1.0, 1.0 - cum / peak_liq))
        
    swap_count = 0
    buy_count = 0
    sell_count = 0
    unique_traders = set()
    buyer_wallets = set()
    seller_wallets = set()
    peak_price = 0.0
    max_price_dd = 0.0
    early_snipers = 0
    sells_by_block = Counter()
    trade_amounts = Counter()
    
    for i, (blk, sender, amt0, amt1, sqrt_price) in enumerate(swap_events):
        if blk < init_block: continue
        if blk > end: break
        swap_count += 1
        unique_traders.add(sender)
        if blk <= init_block + 2: early_snipers += 1
        target_amt = amt0 if token_is_c0 else amt1
        trade_size = abs(target_amt)
        if target_amt > 0: 
            sell_count += 1
            seller_wallets.add(sender)
            sells_by_block[blk] += 1
        elif target_amt < 0: 
            buy_count += 1
            buyer_wallets.add(sender)
        if trade_size > 0: trade_amounts[trade_size] += 1
        
        # Fiyat hesabı
        price_ratio_c0 = (sqrt_price / (2**96)) ** 2
        if token_is_c0:
            current_price = price_ratio_c0
        else:
            current_price = (1.0 / price_ratio_c0) if price_ratio_c0 > 0 else 0.0

        if current_price > peak_price: peak_price = current_price
        elif peak_price > 0:
            dd = 1.0 - (current_price / peak_price)
            if dd > max_price_dd: max_price_dd = dd

    price_crash = max_price_dd >= PRICE_CRASH_THRESHOLD
    is_honeypot = swap_count > 5 and sell_count == 0
    is_wash_trade = swap_count > 10 and len(unique_traders) <= 3
    max_sells = max(sells_by_block.values()) if sells_by_block else 0
    is_synchronized_dump = max_sells >= 5
    is_bot_activity = False
    if swap_count > 10 and trade_amounts:
        most_common_amt, count = trade_amounts.most_common(1)[0]
        if count / swap_count > 0.40: is_bot_activity = True
    flippers = buyer_wallets.intersection(seller_wallets)
    is_flipping_network = len(buyer_wallets) > 5 and (len(flippers) / len(buyer_wallets)) > 0.50

    is_trusted = (
        not price_crash and 
        max_dd_liq < LP_PULL_THRESHOLD and 
        not is_honeypot and 
        not is_wash_trade and 
        not is_synchronized_dump and 
        not is_bot_activity and 
        not is_flipping_network and
        early_snipers <= 3
    )
    return {
        "peak_liq": peak_liq, "max_drawdown_liq": max_dd_liq, "n_add": n_add, "n_remove": n_rem,
        "swap_count": swap_count, "buy_count": buy_count, "sell_count": sell_count,
        "unique_traders": len(unique_traders), "price_crash": price_crash, "max_price_dd_pct": round(max_price_dd * 100, 1),
        "is_honeypot": is_honeypot, "is_wash_trade": is_wash_trade, "is_synchronized_dump": is_synchronized_dump, 
        "is_bot_activity": is_bot_activity, "is_flipping_network": is_flipping_network, "early_snipers": early_snipers,
        "is_trusted": is_trusted
    }

def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0: return (0.0, 1.0)
    p = k / n; denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))

def rate_block(k: int, n: int) -> dict:
    lo, hi = wilson_ci(k, n)
    return {"n": n, "rug": k, "rate": round(k / n, 4) if n else None, "ci95": [round(lo, 4), round(hi, 4)]}

async def main():
    if not POOL_MANAGER:
        print("HATA: POOL_MANAGER gerekli.", file=sys.stderr); sys.exit(1)

    pools = load_pools()
    if not pools: print("Havuz yok, çıkıyorum."); return
    attach_collar_sell_sim(pools)
    from_block = min(p["block"] for p in pools)

    async with httpx.AsyncClient(timeout=30) as client:
        head = int(await rpc(client, "eth_blockNumber", []), 16)
        bps = await blocks_per_sec(client, from_block, head)
        window_blocks = int(LP_WINDOW_SEC * bps)
        to_block = min(head, max(p["block"] for p in pools) + window_blocks)
        print(f"blok aralığı {from_block} -> {to_block} | pencere ≈ {window_blocks:,} blok")
        liq_series, swap_series = await scan_chain(client, from_block, to_block)

    per_pool, excl = [], Counter()
    for p in pools:
        pid = p["pool_id"]
        a = analyze_pool(liq_series.get(pid, []), swap_series.get(pid, []), p["block"], window_blocks, p.get("token_is_c0", False))
        mature = p["block"] + window_blocks <= head
        if not mature: status = "immature"
        elif a["n_add"] == 0: status = "no_liquidity_events"
        else: status = "included"
        excl[status] += 1

        is_lp_pull = status == "included" and a["max_drawdown_liq"] >= LP_PULL_THRESHOLD
        is_price_crash = status == "included" and a["price_crash"]
        is_dead_pool = status == "included" and a["swap_count"] == 0
        rug = is_lp_pull or is_price_crash

        per_pool.append({
            "pool_id": pid, "token": p["token"], "hook": p["hook"], "block": p["block"],
            "source": p["source"], "status": status, "peak_liq": float(a["peak_liq"]),
            "max_drawdown_pct": round(a["max_drawdown_liq"] * 100, 1),
            "swap_count": a["swap_count"], "buy_count": a["buy_count"], "sell_count": a["sell_count"],
            "unique_traders": a["unique_traders"], "price_crash": is_price_crash,
            "price_drop_pct": a["max_price_dd_pct"], "is_honeypot": a["is_honeypot"],
            "is_wash_trade": a["is_wash_trade"], "is_synchronized_dump": a["is_synchronized_dump"],
            "is_bot_activity": a["is_bot_activity"], "is_flipping_network": a["is_flipping_network"],
            "early_snipers": a["early_snipers"], "is_lp_pull": is_lp_pull,
            "is_dead_pool": is_dead_pool, "rug": rug, "is_trusted": a["is_trusted"],
            "collar_t0": p.get("collar_t0"), "collar_sell_sim_ok": p.get("collar_sell_sim_ok"),
        })

    inc = [r for r in per_pool if r["status"] == "included"]
    if not inc:
        print("HATA: payda boş.", file=sys.stderr); sys.exit(1)

    total_rug = sum(r["rug"] for r in inc)
    total_trusted = sum(1 for r in inc if r["is_trusted"])
    
    anomalies_summary = {
        "honeypots": sum(1 for r in inc if r["is_honeypot"]),
        "wash_trading": sum(1 for r in inc if r["is_wash_trade"]),
        "synchronized_dumps": sum(1 for r in inc if r["is_synchronized_dump"]),
        "bot_activity": sum(1 for r in inc if r["is_bot_activity"]),
        "flipping_networks": sum(1 for r in inc if r["is_flipping_network"]),
        "sniper_attacks": sum(1 for r in inc if r["early_snipers"] > 3)
    }

    result = {
        "schema": 4,
        "rules_version": "v0.4.2-usdg-forensics",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rug_definition": {"label": "multi_metric_rug", "window_sec": LP_WINDOW_SEC},
        "overall": rate_block(total_rug, len(inc)),
        "trusted_tokens": {"n": total_trusted, "total": len(inc)},
        "dead_pools": {"n": sum(1 for r in inc if r["is_dead_pool"]), "total": len(inc)},
        "anomalies_summary": anomalies_summary,
        "per_pool": per_pool,
    }
    
    OUT.write_text(json.dumps(result, indent=2))
    with open(OUT_JSONL, "w") as f:
        for p in per_pool:
            f.write(json.dumps({
                "token": p["token"], "rug_24h": p["rug"], "status": p["status"],
                "is_lp_pull": p["is_lp_pull"], "is_price_crash": p["price_crash"],
                "is_dead_pool": p["is_dead_pool"], "is_trusted": p["is_trusted"],
                "swap_count": p["swap_count"], "unique_traders": p["unique_traders"],
                "is_honeypot": p["is_honeypot"], "is_wash_trade": p["is_wash_trade"],
                "is_synchronized_dump": p["is_synchronized_dump"],
                "is_bot_activity": p["is_bot_activity"], "is_flipping_network": p["is_flipping_network"],
                "max_drawdown_pct": p["max_drawdown_pct"], "price_drop_pct": p["price_drop_pct"],
                "pool_id": p["pool_id"]
            }) + "\n")

    print(f"\n=== SONUÇ ===")
    print(f"Değerlendirilen: {len(inc)} | Rug: {total_rug} | Trusted: {total_trusted}")

if __name__ == "__main__":
    asyncio.run(main())