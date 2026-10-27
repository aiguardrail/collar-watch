"""outcomes.py - v0.7.1 (Cumulative + Fixed Rolling Window + Trusted/Dead Fix + Rate-limit hardening)"""
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
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "24"))
START_RANGE = int(os.getenv("START_RANGE", "2000"))
MIN_RANGE = int(os.getenv("MIN_RANGE", "200"))
RPC_RPS = float(os.getenv("RPC_RPS", "3"))
# YENİ: 429 fırtınasında nefes alma
COOLDOWN_EVERY = int(os.getenv("COOLDOWN_EVERY", "50"))
COOLDOWN_SEC = float(os.getenv("COOLDOWN_SEC", "20"))

MODIFY_LIQ_TOPIC = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
SWAP_TOPIC = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
USDG_ADDRESS = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"

try:
    from .constants import QUOTE_ASSETS as _QA
    DEFAULT_QUOTES = ",".join(_QA.keys()) + ",0x0000000000000000000000000000000000000000"
except ImportError:
    DEFAULT_QUOTES = f"{USDG_ADDRESS},0x0000000000000000000000000000000000000000"
QUOTES = {a.strip().lower() for a in (os.getenv("QUOTE_ADDRS") or DEFAULT_QUOTES).split(",") if a.strip()}

HOOK_LABELS: dict[str, str] = {}
for _pair in os.getenv("HOOK_LABELS", "").split(","):
    if ":" in _pair:
        _a, _n = _pair.split(":", 1)
        HOOK_LABELS[_a.strip().lower()] = _n.strip()

class RangeError(RuntimeError): pass
_rate = {"next": 0.0}
_rate_lock = asyncio.Lock()

async def _throttle():
    if RPC_RPS <= 0: return
    async with _rate_lock:
        now = time.monotonic()
        wait = _rate["next"] - now
        _rate["next"] = max(now, _rate["next"]) + 1.0 / RPC_RPS
    if wait > 0: await asyncio.sleep(wait)

async def rpc(client, method, params, retries=20):
    """FIX: retries 10→20, max bekleme 30s→120s. Public RPC rate limit'lerine dayanıklı."""
    last = None
    for a in range(retries):
        try:
            await _throttle()
            r = await client.post(RPC_URL, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                try: wait = float(r.headers.get("retry-after", 0))
                except: wait = 0
                if wait <= 0: wait = (2 ** min(a, 6)) + random.uniform(0, 1)
                wait = min(wait, 120)
                print(f"  RPC {method} HTTP {r.status_code} → {wait:.1f}s (deneme {a+1}/{retries})", file=sys.stderr)
                await asyncio.sleep(wait)
                continue
            d = r.json()
            if "error" in d: raise RangeError(f"{method}: {str(d['error'])[:200]}")
            return d["result"]
        except (httpx.TransportError, ValueError) as e:
            last = f"{type(e).__name__}: {e}"
            await asyncio.sleep(min(2.0 * (a + 1), 30))
    raise RuntimeError(f"rpc {method} başarısız ({retries} deneme): {last}")

def _jsonl(path):
    if not path.exists(): return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try: yield json.loads(line)
                except: continue

def hook_label(addr):
    a = (addr or "").lower()
    return HOOK_LABELS.get(a, a or "none")

def load_backfill_pools():
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

def load_live_pools():
    pools = {}
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

def load_pools():
    merged = {}
    for src in SOURCES:
        if src == "backfill": batch = load_backfill_pools()
        elif src == "live": batch = load_live_pools()
        else: continue
        for p in batch: merged[p["pool_id"]] = p
    return list(merged.values())

def load_previous_outcomes():
    out = {}
    for rec in _jsonl(OUT_JSONL):
        tk = (rec.get("token") or "").lower()
        if tk: out[tk] = rec
    return out

def attach_collar_sell_sim(pools):
    ev = {}
    for r in _jsonl(EVALUATIONS):
        pid = (r.get("pool_id") or "").lower()
        if pid: ev[pid] = (r.get("t0") or {}).get("sell_simulation_ok")
    for p in pools:
        if "collar_sell_sim_ok" not in p and p["pool_id"] in ev:
            p["collar_sell_sim_ok"] = ev[p["pool_id"]]

def decode_modify_liquidity(lg):
    topics = lg.get("topics", [])
    if len(topics) < 3: return None
    data = lg["data"][2:]
    if len(data) < 192: return None
    v = int(data[128:192], 16)
    delta = v - (1 << 256) if v >= (1 << 255) else v
    return topics[1].lower(), delta, "0x" + topics[2][-40:].lower()

def decode_swap(lg):
    topics = lg.get("topics", [])
    if len(topics) < 3: return None
    data = lg["data"][2:]
    if len(data) < 192: return None
    amt0 = int(data[0:64], 16)
    if amt0 >= (1 << 255): amt0 -= (1 << 256)
    amt1 = int(data[64:128], 16)
    if amt1 >= (1 << 255): amt1 -= (1 << 256)
    sqrt_price = int(data[128:192], 16)
    return topics[1].lower(), "0x" + topics[2][-40:].lower(), amt0, amt1, float(sqrt_price)

async def scan_chain(client, from_block, to_block):
    liq_series = defaultdict(list)
    swap_series = defaultdict(list)
    size, cap, frm, ok_streak, calls = START_RANGE, START_RANGE, from_block, 0, 0
    t0 = time.time()
    total_blocks = max(1, to_block - from_block + 1)
    while frm <= to_block:
        to = min(frm + size - 1, to_block)
        try:
            logs = await rpc(client, "eth_getLogs", [{
                "address": POOL_MANAGER, "topics": [[MODIFY_LIQ_TOPIC, SWAP_TOPIC]],
                "fromBlock": hex(frm), "toBlock": hex(to)}])
        except RangeError as e:
            if size <= MIN_RANGE: raise RuntimeError(f"getLogs {frm}->{to} failed: {e}")
            size = max(MIN_RANGE, size // 2)
            cap, ok_streak = size, 0
            continue
        calls += 1
        # YENİ: her COOLDOWN_EVERY çağrıda RPC'ye nefes aldır
        if COOLDOWN_EVERY > 0 and calls % COOLDOWN_EVERY == 0:
            print(f"  cooldown {COOLDOWN_SEC:.0f}s ({calls} çağrı) — RPC nefes alsın...", flush=True)
            await asyncio.sleep(COOLDOWN_SEC)
        for lg in logs:
            t0t = lg["topics"][0].lower()
            if t0t == MODIFY_LIQ_TOPIC:
                d = decode_modify_liquidity(lg)
                if d: liq_series[d[0]].append((int(lg["blockNumber"], 16), d[1], d[2]))
            elif t0t == SWAP_TOPIC:
                d = decode_swap(lg)
                if d: swap_series[d[0]].append((int(lg["blockNumber"], 16), d[1], d[2], d[3], d[4]))
        frm = to + 1
        ok_streak += 1
        if ok_streak >= 15 and cap < START_RANGE:
            cap = min(START_RANGE, int(cap * 1.5))
            size, ok_streak = cap, 0
        if calls % 20 == 0:
            pct = (frm - from_block) / total_blocks * 100
            print(f"  tarama %{pct:.0f} | çağrı={calls} | aralık={size}", flush=True)
    for pid in liq_series: liq_series[pid].sort(key=lambda x: x[0])
    for pid in swap_series: swap_series[pid].sort(key=lambda x: x[0])
    total_liq = sum(len(v) for v in liq_series.values())
    total_swaps = sum(len(v) for v in swap_series.values())
    print(f"tarama bitti: {calls} çağrı, likidite={total_liq} ({len(liq_series)} havuz), "
          f"swap={total_swaps} ({len(swap_series)} havuz)")
    if total_liq > 0 and total_swaps == 0:
        print(f"UYARI: likidite var ama tek swap logu yok. SWAP_TOPIC yanlış olabilir ({SWAP_TOPIC}).",
              file=sys.stderr)
    return liq_series, swap_series

async def blocks_per_sec(client, from_block, head):
    a = await rpc(client, "eth_getBlockByNumber", [hex(from_block), False])
    b = await rpc(client, "eth_getBlockByNumber", [hex(head), False])
    dt = int(b["timestamp"], 16) - int(a["timestamp"], 16)
    if dt <= 0: raise RuntimeError("blok hızı hesaplanamadı")
    return (head - from_block) / dt

def analyze_pool(liq_events, swap_events, init_block, window_blocks, token_is_c0):
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

    swap_count = buy_count = sell_count = 0
    unique_traders, buyer_wallets, seller_wallets = set(), set(), set()
    peak_price = 0.0
    max_price_dd = 0.0
    early_snipers = 0
    sells_by_block = Counter()
    trade_amounts = Counter()

    for blk, sender, amt0, amt1, sqrt_price in swap_events:
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
        price_c0 = (sqrt_price / (2**96)) ** 2
        current_price = price_c0 if token_is_c0 else (1.0 / price_c0 if price_c0 > 0 else 0)
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
    is_trusted = (swap_count > 0 and not price_crash and max_dd_liq < LP_PULL_THRESHOLD and
                  not is_honeypot and not is_wash_trade and not is_synchronized_dump and
                  not is_bot_activity and not is_flipping_network and early_snipers <= 3)

    return {
        "peak_liq": peak_liq, "max_drawdown_liq": max_dd_liq, "n_add": n_add, "n_remove": n_rem,
        "swap_count": swap_count, "buy_count": buy_count, "sell_count": sell_count,
        "unique_traders": len(unique_traders), "price_crash": price_crash,
        "max_price_dd_pct": round(max_price_dd * 100, 1),
        "is_honeypot": is_honeypot, "is_wash_trade": is_wash_trade,
        "is_synchronized_dump": is_synchronized_dump, "is_bot_activity": is_bot_activity,
        "is_flipping_network": is_flipping_network, "early_snipers": early_snipers,
        "is_trusted": is_trusted
    }

def wilson_ci(k, n, z=1.96):
    if n == 0: return (0.0, 1.0)
    p = k / n; denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))

def rate_block(k, n):
    lo, hi = wilson_ci(k, n)
    return {"n": n, "rug": k, "rate": round(k / n, 4) if n else None, "ci95": [round(lo, 4), round(hi, 4)]}

async def main():
    if not POOL_MANAGER:
        print("HATA: POOL_MANAGER gerekli.", file=sys.stderr); sys.exit(1)

    pools = load_pools()
    if not pools: print("Havuz yok, çıkıyorum."); return
    attach_collar_sell_sim(pools)

    prev_outcomes = load_previous_outcomes()
    print(f"Önceki outcomes: {len(prev_outcomes)} token")

    async with httpx.AsyncClient(timeout=30) as client:
        head = int(await rpc(client, "eth_blockNumber", []), 16)
        from_block_all = min(p["block"] for p in pools)
        bps = await blocks_per_sec(client, from_block_all, head)
        window_blocks = int(LP_WINDOW_SEC * bps)
        lookback_blocks = int(LOOKBACK_HOURS * 3600 * bps)

    current_tokens = {p["token"].lower(): p for p in pools}
    reused = {}
    to_scan = []

    for p in pools:
        tk = p["token"].lower()
        prev_out = prev_outcomes.get(tk)
        is_old = p["block"] + window_blocks <= head
        has_good_prev = prev_out and prev_out.get("swap_count", 0) > 0

        if is_old and has_good_prev:
            reused[tk] = prev_out
        elif is_old:
            to_scan.append(p)
        else:
            to_scan.append(p)

    for tk, prev_out in prev_outcomes.items():
        if tk not in current_tokens:
            reused[tk] = prev_out

    print(f"Reused: {len(reused)} | To scan: {len(to_scan)} | Lookback: {LOOKBACK_HOURS}h")

    new_per_pool = []
    if to_scan:
        # FIX 1: min() — eski havuzların oluşum bloğundan başla
        min_scan_block = min(head - lookback_blocks, min(p["block"] for p in to_scan))
        # FIX 2 (YENİ): to_block'u daralt — en son "24h penceresi biten havuz"un ötesini taramaya gerek yok
        max_relevant = max(p["block"] + window_blocks for p in to_scan)
        to_block = min(head, max_relevant)
        print(f"Tarama aralığı: {min_scan_block} -> {to_block} "
              f"({to_block - min_scan_block} blok, head={head}, saved={head - to_block} blok)", flush=True)

        async with httpx.AsyncClient(timeout=30) as client:
            liq_series, swap_series = await scan_chain(client, min_scan_block, to_block)

        for p in to_scan:
            pid = p["pool_id"]
            a = analyze_pool(liq_series.get(pid, []), swap_series.get(pid, []),
                             p["block"], window_blocks, p.get("token_is_c0", False))
            mature = p["block"] + window_blocks <= head
            if not mature: status = "immature"
            elif a["n_add"] == 0: status = "no_liquidity_events"
            else: status = "included"

            is_lp_pull = status == "included" and a["max_drawdown_liq"] >= LP_PULL_THRESHOLD
            is_price_crash = status == "included" and a["price_crash"]
            is_dead_pool = status == "included" and a["swap_count"] == 0
            rug = is_lp_pull or is_price_crash

            new_per_pool.append({
                "token": p["token"], "rug_24h": rug, "status": status,
                "is_lp_pull": is_lp_pull, "is_price_crash": is_price_crash,
                "is_dead_pool": is_dead_pool, "is_trusted": a["is_trusted"],
                "swap_count": a["swap_count"], "unique_traders": a["unique_traders"],
                "is_honeypot": a["is_honeypot"], "is_wash_trade": a["is_wash_trade"],
                "is_synchronized_dump": a["is_synchronized_dump"],
                "is_bot_activity": a["is_bot_activity"], "is_flipping_network": a["is_flipping_network"],
                "max_drawdown_pct": round(a["max_drawdown_liq"] * 100, 1),
                "price_drop_pct": a["max_price_dd_pct"],
                "pool_id": pid,
                "hook_addr": p.get("hook_addr"),
            })

    all_outcomes = dict(reused)
    for r in new_per_pool:
        all_outcomes[r["token"].lower()] = r

    final_list = list(all_outcomes.values())
    print(f"Toplam outcomes: {len(final_list)} (reused: {len(reused)}, new: {len(new_per_pool)})")

    inc = [r for r in final_list if r.get("status") == "included"]
    if not inc:
        _sd = Counter(r.get("status") for r in final_list)
        print(f"UYARI: included=0, status dağılımı: {dict(_sd)}", file=sys.stderr)

    total_rug = sum(1 for r in inc if r.get("rug_24h"))
    total_trusted = sum(1 for r in inc if r.get("is_trusted"))
    dead_pools = sum(1 for r in inc if r.get("is_dead_pool"))
    unique_tokens = len({r["token"].lower() for r in inc})
    unique_rug = len({r["token"].lower() for r in inc if r.get("rug_24h")})

    anomalies = {
        "honeypots": sum(1 for r in inc if r.get("is_honeypot")),
        "wash_trading": sum(1 for r in inc if r.get("is_wash_trade")),
        "synchronized_dumps": sum(1 for r in inc if r.get("is_synchronized_dump")),
        "bot_activity": sum(1 for r in inc if r.get("is_bot_activity")),
        "flipping_networks": sum(1 for r in inc if r.get("is_flipping_network")),
        "sniper_attacks": sum(1 for r in inc if r.get("early_snipers", 0) > 3),
        "pools_without_any_swap": sum(1 for r in inc if r.get("swap_count", 0) == 0),
    }

    # Hook bazında özet — Guardrail tier-3 advisory için
    by_hook = defaultdict(lambda: {"n": 0, "rug": 0})
    for r in inc:
        h = (r.get("hook_addr") or "none").lower()
        by_hook[h]["n"] += 1
        if r.get("rug_24h"):
            by_hook[h]["rug"] += 1

    result = {
        "schema": 6,
        "rules_version": "v0.7.1-fixed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_hours": LOOKBACK_HOURS,
        "overall": rate_block(total_rug, len(inc)),
        "overall_unique": rate_block(unique_rug, unique_tokens),
        "unique_tokens": unique_tokens,
        "verdicts": len(inc),
        "trusted_tokens": {"n": total_trusted, "total": len(inc)},
        "dead_pools": {"n": dead_pools, "total": len(inc)},
        "anomalies_summary": anomalies,
        "by_hook": {
            h: {"n": v["n"], "rug": v["rug"],
                "rug_rate": round(v["rug"]/v["n"], 4) if v["n"] else None}
            for h, v in by_hook.items()
        },
    }

    OUT.write_text(json.dumps(result, indent=2))
    with open(OUT_JSONL, "w") as f:
        for p in final_list:
            f.write(json.dumps(p) + "\n")

    o = result["overall"]
    print(f"\n=== SONUÇ (Kümülatif) ===")
    print(f"verdicts={o['n']}  rug={o['rug']}  oran={o['rate']}")
    print(f"unique_tokens={unique_tokens}  unique_rug={unique_rug}")
    print(f"Trusted: {total_trusted}, Dead: {dead_pools}")
    print(f"Anomaliler: {anomalies}")
    print(f"Hook sayısı: {len(by_hook)}")

if __name__ == "__main__":
    asyncio.run(main())
