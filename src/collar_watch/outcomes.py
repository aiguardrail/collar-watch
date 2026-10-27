"""outcomes.py - v0 rug etiketi: doğumdan sonraki LP_WINDOW_SEC içinde LP çekilmesi.

RUG TANIMI (v0, yalnızca zincirden, Collar'dan BAĞIMSIZ):
  Havuzun Initialize bloğundan itibaren LP_WINDOW_SEC (varsayılan 24sa) içinde,
  toplam likidite birimi (ModifyLiquidity liquidityDelta kümülatifi) koşan tepe
  değerinden en az LP_PULL_THRESHOLD (varsayılan %90) düştü.

NEDEN sell-block etikette YOK:
  Collar'ın kendi sell_simulation_ok çıktısı etikete girerse Collar'ı kendi çıktısıyla
  ölçmüş olursun (precision şişer). Etiket Collar'dan bağımsız kalmalı. Collar'ın
  sell-sim sonucu yalnızca bilgi sütunu (collar_sell_sim_ok) olarak taşınır.

PAYDA:
  - pencere dolmamış havuzlar ("immature") dışarıda
  - pencerede hiç likidite eklenmemiş havuzlar dışarıda
  - tepe likidite < MIN_PEAK_LIQ olanlar dışarıda (varsayılan 0 = kapalı;
    peak_sensitivity bölümü eşiğe duyarlılığı gösterir)
  Dışarıda kalan sayılar "exclusions" altında raporlanır.

TEŞHİS (hook_lp_activity):
  Hook başına: kaç havuzda likidite eklendi / çekildi + hook adres bitlerinden
  beforeRemoveLiquidity izni. pons_v2'de çekilme ≈ 0 ise LP-pull sinyali bu launchpad
  için yapısal olarak ölü demektir; oran 0 çıkarsa "rug yok" diye okunmamalı.

Env:
  POOL_MANAGER (zorunlu), RPC_URL, SOURCES (backfill,live), LIVE_EVENTS,
  BACKFILL_EVENTS, EVALUATIONS (opsiyonel), OUTCOMES_OUT, OUTCOMES_OUT_JSONL,
  QUOTE_ADDRS, HOOK_LABELS, LP_PULL_THRESHOLD (0.90), LP_WINDOW_SEC (86400),
  START_RANGE (10000), MIN_RANGE (200), RPC_RPS (5), MIN_PEAK_LIQ (0), MODIFY_LIQ_TOPIC

Kullanım:
    POOL_MANAGER=0x... python -m collar_watch.outcomes
"""
import asyncio
import json
import math
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import httpx

RPC_URL = os.getenv("RPC_URL", "https://rpc.mainnet.chain.robinhood.com")
POOL_MANAGER = os.getenv("POOL_MANAGER", "").strip().lower()
SOURCES = [s.strip() for s in os.getenv("SOURCES", "backfill,live").split(",") if s.strip()]
LIVE_EVENTS = Path(os.getenv("LIVE_EVENTS", "live-events.jsonl"))
BACKFILL_EVENTS = Path(os.getenv("BACKFILL_EVENTS", "data/events.jsonl"))
EVALUATIONS = Path(os.getenv("EVALUATIONS", "evaluations.jsonl"))
OUT = Path(os.getenv("OUTCOMES_OUT", "outcomes.json"))

# YENİ: publish.py'nin okuyacağı JSONL dosyası
OUT_JSONL = Path(os.getenv("OUTCOMES_OUT_JSONL", "outcomes.jsonl"))

LP_PULL_THRESHOLD = float(os.getenv("LP_PULL_THRESHOLD", "0.90"))
LP_WINDOW_SEC = float(os.getenv("LP_WINDOW_SEC", "86400"))
START_RANGE = int(os.getenv("START_RANGE", "10000"))
MIN_RANGE = int(os.getenv("MIN_RANGE", "200"))
RPC_RPS = float(os.getenv("RPC_RPS", "5"))
MIN_PEAK_LIQ = float(os.getenv("MIN_PEAK_LIQ", "0"))

# ModifyLiquidity(bytes32 indexed id, address indexed sender, int24, int24, int256, bytes32)
MODIFY_LIQ_TOPIC = os.getenv(
    "MODIFY_LIQ_TOPIC",
    "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec",
).strip().lower()

DEFAULT_QUOTES = (
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168,"
    "0x0bd7d308f8e1639fab988df18a8011f41eacad73,"
    "0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea,"
    "0x999999990237e901c537bbd768e09562be02efa5,"
    "0x0000000000000000000000000000000000000000"
)
QUOTES = {a.strip().lower() for a in (os.getenv("QUOTE_ADDRS") or DEFAULT_QUOTES).split(",") if a.strip()}

HOOK_LABELS: dict[str, str] = {}
for _pair in os.getenv("HOOK_LABELS", "").split(","):
    if ":" in _pair:
        _a, _n = _pair.split(":", 1)
        HOOK_LABELS[_a.strip().lower()] = _n.strip()

# Uniswap V4 hook izin bitleri (hook adresinin alt 14 biti)
BEFORE_ADD_LIQ = 1 << 11
BEFORE_REMOVE_LIQ = 1 << 9
AFTER_REMOVE_LIQ_RETURNS_DELTA = 1 << 0


# ---------- RPC ----------

class RangeError(RuntimeError):
    """Node bir JSON-RPC hatası döndü (genelde aralık/sonuç sayısı sınırı)."""


_rate = {"next": 0.0}
_rate_lock = asyncio.Lock()


async def _throttle():
    if RPC_RPS <= 0:
        return
    async with _rate_lock:
        now = time.monotonic()
        wait = _rate["next"] - now
        _rate["next"] = max(now, _rate["next"]) + 1.0 / RPC_RPS
    if wait > 0:
        await asyncio.sleep(wait)


async def rpc(client: httpx.AsyncClient, method: str, params: list, retries: int = 4):
    last = None
    for a in range(retries + 1):
        try:
            await _throttle()
            r = await client.post(RPC_URL, json={
                "jsonrpc": "2.0", "id": 1, "method": method, "params": params})
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                await asyncio.sleep(2.0 * (a + 1))
                continue
            d = r.json()
            if "error" in d:
                raise RangeError(f"{method}: {str(d['error'])[:200]}")
            return d["result"]
        except (httpx.TransportError, ValueError) as e:
            last = f"{type(e).__name__}: {e}"
            await asyncio.sleep(2.0 * (a + 1))
    raise RuntimeError(f"rpc {method} başarısız: {last}")


# ---------- havuz listeleri ----------

def _jsonl(path: Path):
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def hook_label(addr: str | None) -> str:
    a = (addr or "").lower()
    return HOOK_LABELS.get(a, a or "none")


def load_backfill_pools() -> list[dict]:
    """data/events.jsonl (counter.py çıktısı): currency0/currency1/hooks/pool_id/block."""
    out = []
    for r in _jsonl(BACKFILL_EVENTS):
        c0 = (r.get("currency0") or "").lower()
        c1 = (r.get("currency1") or "").lower()
        if c0 in QUOTES and c1 not in QUOTES:
            token = c1
        elif c1 in QUOTES and c0 not in QUOTES:
            token = c0
        else:
            continue
        pid = (r.get("pool_id") or "").lower()
        if not pid or r.get("block") is None:
            continue
        hook_addr = (r.get("hooks") or "").lower()
        out.append({"pool_id": pid, "token": token, "hook_addr": hook_addr,
                    "hook": hook_label(hook_addr), "block": int(r["block"]),
                    "source": "backfill"})
    return out


def load_live_pools() -> list[dict]:
    pools: dict[str, dict] = {}
    for r in _jsonl(LIVE_EVENTS):
        pid = (r.get("pool_id") or "").lower()
        if not pid:
            continue
        st = pools.setdefault(pid, {})
        if r.get("stage") == "init":
            hook_addr = (r.get("hook_addr") or "").lower()
            st.update({"pool_id": pid, "token": r.get("token"), "hook_addr": hook_addr,
                       "hook": r.get("hook") or hook_label(hook_addr),
                       "block": r.get("block"), "source": "live"})
        elif r.get("stage") == "t0":
            st["collar_t0"] = (r.get("result") or {}).get("decision")
            st["collar_sell_sim_ok"] = (r.get("result") or {}).get("sell_simulation_ok")
    return [p for p in pools.values() if p.get("token") and p.get("block") is not None]


def load_pools() -> list[dict]:
    """Kaynakları birleştir; aynı havuz hem backfill hem live'daysa live kazanır."""
    merged: dict[str, dict] = {}
    for src in SOURCES:
        if src == "backfill":
            batch = load_backfill_pools()
        elif src == "live":
            batch = load_live_pools()
        else:
            print(f"UYARI: bilinmeyen kaynak {src!r}", file=sys.stderr)
            continue
        print(f"kaynak {src}: {len(batch)} havuz")
        for p in batch:
            merged[p["pool_id"]] = p
    return list(merged.values())


def attach_collar_sell_sim(pools: list[dict]):
    """Bilgi sütunu: evaluations.jsonl'deki T0 sell_simulation_ok (etikete GİRMEZ)."""
    ev = {}
    for r in _jsonl(EVALUATIONS):
        pid = (r.get("pool_id") or "").lower()
        if pid:
            ev[pid] = (r.get("t0") or {}).get("sell_simulation_ok")
    for p in pools:
        if "collar_sell_sim_ok" not in p and p["pool_id"] in ev:
            p["collar_sell_sim_ok"] = ev[p["pool_id"]]


# ---------- likidite taraması ----------

def decode_modify_liquidity(lg: dict) -> tuple[str, int, str] | None:
    topics = lg.get("topics", [])
    if len(topics) < 3:
        return None
    data = lg["data"][2:]
    w = [data[i * 64:(i + 1) * 64] for i in range(len(data) // 64)]
    if len(w) < 4:
        return None
    v = int(w[2], 16)
    delta = v - (1 << 256) if v >= (1 << 255) else v
    return topics[1].lower(), delta, "0x" + topics[2][-40:].lower()


async def scan_liquidity(client: httpx.AsyncClient, from_block: int, to_block: int):
    """[from, to] arasındaki tüm ModifyLiquidity log'ları: pool_id -> [(block, delta, sender)].

    Aralık hatasında yarıya indirir; en küçük aralıkta bile hata verirse DURUR
    (boşluk bırakıp devam etmek peak/drawdown'ı sessizce bozar).
    """
    series: dict[str, list[tuple[int, int, str]]] = defaultdict(list)
    size, cap, frm, ok_streak, calls = START_RANGE, START_RANGE, from_block, 0, 0
    t0 = time.time()
    while frm <= to_block:
        to = min(frm + size - 1, to_block)
        try:
            logs = await rpc(client, "eth_getLogs", [{
                "address": POOL_MANAGER, "topics": [MODIFY_LIQ_TOPIC],
                "fromBlock": hex(frm), "toBlock": hex(to)}])
        except RangeError as e:
            if size <= MIN_RANGE:
                raise RuntimeError(f"getLogs {frm}->{to} en küçük aralıkta başarısız: {e}")
            size = max(MIN_RANGE, size // 2)
            cap, ok_streak = size, 0          # başarısız tavanı hatırla, hemen tekrar büyütme
            print(f"  aralık {size}'e düşürüldü ({str(e)[:90]})", flush=True)
            continue
        calls += 1
        for lg in logs:
            d = decode_modify_liquidity(lg)
            if d:
                series[d[0]].append((int(lg["blockNumber"], 16), d[1], d[2]))
        frm = to + 1
        ok_streak += 1
        if ok_streak >= 15 and cap < START_RANGE:     # log yoğunluğu değişir: yavaşça yokla
            cap = min(START_RANGE, int(cap * 1.5))
            size, ok_streak = cap, 0
        if calls % 20 == 0:
            done = (frm - from_block) / max(1, to_block - from_block + 1)
            print(f"  tarama %{done * 100:.0f} | çağrı={calls} | aralık={size} | "
                  f"havuz={len(series)} | {int(time.time() - t0)}s", flush=True)
    for pid in series:
        series[pid].sort(key=lambda x: x[0])
    print(f"tarama bitti: {calls} çağrı, {len(series)} havuz, {int(time.time() - t0)}s")
    return series


async def blocks_per_sec(client: httpx.AsyncClient, from_block: int, head: int) -> float:
    a = await rpc(client, "eth_getBlockByNumber", [hex(from_block), False])
    b = await rpc(client, "eth_getBlockByNumber", [hex(head), False])
    dt = int(b["timestamp"], 16) - int(a["timestamp"], 16)
    if dt <= 0 or head <= from_block:
        raise RuntimeError("blok hızı hesaplanamadı")
    return (head - from_block) / dt


# ---------- analiz ----------

def analyze_pool(events: list[tuple[int, int, str]], init_block: int, window_blocks: int) -> dict:
    """Pencere [init_block, init_block+window_blocks] içinde koşan-tepeye göre maks. düşüş."""
    end = init_block + window_blocks
    cum = peak = 0
    max_dd = 0.0
    n_add = n_rem = 0
    senders: Counter = Counter()
    for blk, delta, sender in events:
        if blk < init_block:
            continue
        if blk > end:
            break
        cum += delta
        if delta > 0:
            n_add += 1
            senders[sender] += 1
        elif delta < 0:
            n_rem += 1
        if cum > peak:
            peak = cum
        elif peak > 0:
            max_dd = max(max_dd, min(1.0, 1.0 - cum / peak))
    return {"peak": peak, "final": cum, "max_drawdown": max_dd,
            "n_add": n_add, "n_remove": n_rem, "senders": senders}


def hook_flags(hook_addr: str) -> dict:
    try:
        v = int(hook_addr, 16)
    except (ValueError, TypeError):
        return {"has_hook": False}
    if v == 0:
        return {"has_hook": False}
    return {"has_hook": True,
            "before_add_liquidity": bool(v & BEFORE_ADD_LIQ),
            "before_remove_liquidity": bool(v & BEFORE_REMOVE_LIQ),
            "after_remove_returns_delta": bool(v & AFTER_REMOVE_LIQ_RETURNS_DELTA)}


def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def rate_block(k: int, n: int) -> dict:
    lo, hi = wilson_ci(k, n)
    return {"n": n, "rug": k, "rate": round(k / n, 4) if n else None,
            "ci95": [round(lo, 4), round(hi, 4)]}


def quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


# ---------- ana ----------

async def main():
    if not POOL_MANAGER:
        print("HATA: POOL_MANAGER gerekli.", file=sys.stderr)
        sys.exit(1)

    pools = load_pools()
    print(f"toplam havuz (birleşik): {len(pools)} | kaynaklar={SOURCES}")
    if not pools:
        print("Havuz yok, çıkıyorum.")
        return
    attach_collar_sell_sim(pools)

    from_block = min(p["block"] for p in pools)

    async with httpx.AsyncClient(timeout=30) as client:
        head = int(await rpc(client, "eth_blockNumber", []), 16)
        bps = await blocks_per_sec(client, from_block, head)
        window_blocks = int(LP_WINDOW_SEC * bps)
        # pencere dolmuş en yeni havuzun penceresi head'i aşmaz; taramayı buraya kadar yap
        to_block = min(head, max(p["block"] for p in pools) + window_blocks)
        print(f"blok aralığı {from_block} -> {to_block} ({to_block - from_block:,}) | "
              f"~{bps:.2f} blok/sn | pencere ≈ {window_blocks:,} blok")
        series = await scan_liquidity(client, from_block, to_block)

    per_pool, excl = [], Counter()
    hook_act = defaultdict(lambda: {"pools": 0, "with_add": 0, "with_remove": 0,
                                    "senders": Counter(), "addr": None})

    for p in pools:
        pid = p["pool_id"]
        a = analyze_pool(series.get(pid, []), p["block"], window_blocks)

        h = hook_act[p["hook"]]
        h["pools"] += 1
        h["addr"] = h["addr"] or p.get("hook_addr")
        if a["n_add"]:
            h["with_add"] += 1
            h["senders"].update(a["senders"])
        if a["n_remove"]:
            h["with_remove"] += 1

        mature = p["block"] + window_blocks <= head
        if not mature:
            status = "immature"
        elif a["n_add"] == 0:
            status = "no_liquidity_events"
        elif a["peak"] < MIN_PEAK_LIQ:
            status = "below_min_peak"
        else:
            status = "included"
        excl[status] += 1

        rug = status == "included" and a["max_drawdown"] >= LP_PULL_THRESHOLD
        per_pool.append({
            "pool_id": pid, "token": p["token"], "hook": p["hook"], "block": p["block"],
            "source": p["source"], "status": status,
            "peak_liq": float(a["peak"]), "max_drawdown_pct": round(a["max_drawdown"] * 100, 1),
            "n_add": a["n_add"], "n_remove": a["n_remove"], "rug": rug,
            "collar_t0": p.get("collar_t0"), "collar_sell_sim_ok": p.get("collar_sell_sim_ok"),
        })

    inc = [r for r in per_pool if r["status"] == "included"]
    if not inc:
        print("HATA: payda boş (olgun + likiditeli havuz yok).", file=sys.stderr)
        print(f"dışarıda kalanlar: {dict(excl)}", file=sys.stderr)
        sys.exit(1)

    by_source = {s: rate_block(sum(r["rug"] for r in rs), len(rs))
                 for s in {r["source"] for r in inc}
                 for rs in [[r for r in inc if r["source"] == s]]}
    by_hook = []
    for hook in sorted({r["hook"] for r in inc}, key=lambda h: -sum(1 for r in inc if r["hook"] == h)):
        rs = [r for r in inc if r["hook"] == hook]
        by_hook.append({"hook": hook, **rate_block(sum(r["rug"] for r in rs), len(rs))})

    peaks = sorted(r["peak_liq"] for r in inc)
    sensitivity = []
    for q in (0.0, 0.10, 0.25, 0.50):
        thr = quantile(peaks, q)
        rs = [r for r in inc if r["peak_liq"] >= thr]
        sensitivity.append({"peak_quantile": q, "min_peak_liq": thr,
                            **rate_block(sum(r["rug"] for r in rs), len(rs))})

    lp_activity = []
    for hook, h in sorted(hook_act.items(), key=lambda kv: -kv[1]["pools"]):
        lp_activity.append({
            "hook": hook, "hook_addr": h["addr"], "pools": h["pools"],
            "pools_with_add": h["with_add"], "pools_with_remove": h["with_remove"],
            "remove_share_of_funded": round(h["with_remove"] / h["with_add"], 4) if h["with_add"] else None,
            "top_add_senders": h["senders"].most_common(3),
            **hook_flags(h["addr"] or ""),
        })

    total_rug = sum(r["rug"] for r in inc)
    result = {
        "schema": 2,
        "rules_version": "v0-lp-pull-windowed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rug_definition": {
            "label": "lp_pull",
            "window_sec": LP_WINDOW_SEC,
            "threshold_pct": int(LP_PULL_THRESHOLD * 100),
            "note": ("Doğumdan sonra pencere içinde, toplam likidite biriminin koşan tepeden "
                     "maks. düşüşü eşiği aştı. Collar çıktısı etikete girmez. "
                     "Diğer rug türleri (deployer dump, tax artışı, sell-block) ölçülmedi: "
                     "oran ALT SINIRDIR."),
        },
        "overall": rate_block(total_rug, len(inc)),
        "by_source": by_source,
        "by_hook": by_hook,
        "exclusions": dict(excl),
        "peak_sensitivity": sensitivity,
        "hook_lp_activity": lp_activity,
        "caveats": [
            "Likidite birimi (L) fiyat aralığına bağlıdır; USD değeri değildir.",
            "Pencere sonu blok hızından tahmin edilir (±küçük sapma, tüm havuzlarda aynı).",
            "Sahibin likiditeyi başka havuza taşıması (migrasyon) rug gibi görünebilir.",
        ],
        "per_pool": per_pool,
    }
    
    # 1. Detaylı JSON raporunu yaz
    OUT.write_text(json.dumps(result, indent=2))
    
    # 2. YENİ: publish.py'nin okuyacağı JSONL formatını üret
    with open(OUT_JSONL, "w") as f:
        for p in per_pool:
            f.write(json.dumps({
                "token": p["token"],
                "rug_24h": p["rug"],
                "status": p["status"],
                "max_drawdown_pct": p["max_drawdown_pct"],
                "pool_id": p["pool_id"]
            }) + "\n")

    o = result["overall"]
    print("\n=== SONUÇ ===")
    print(f"payda={o['n']}  rug={o['rug']}  oran={o['rate']}  CI95={o['ci95']}")
    print(f"dışarıda: {dict(excl)}")
    print("\nHook LP aktivitesi (TEŞHİS):")
    for h in lp_activity:
        print(f"  {h['hook'][:16]:16s} havuz={h['pools']:5d} ekleme={h['pools_with_add']:5d} "
              f"çekilme={h['pools_with_remove']:5d} beforeRemoveIzni={h.get('before_remove_liquidity')}")
    print(f"\nyazıldı: {OUT} ve {OUT_JSONL}")


if __name__ == "__main__":
    asyncio.run(main())