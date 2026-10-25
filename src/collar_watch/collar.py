"""sampled-pools.jsonl'daki her havuz için Collar Guardrail MCP'sini çağırır.

Her havuz için:
  1. T+0: check_token_safety
  2. DELAY_SEC bekle
  3. T+3dk: tekrar check_token_safety
  4. Sonucu evaluations.jsonl'a ANINDA yaz (timeout olsa bile veri kalır)

Preflight: Havuz döngüsünden önce tek bir `tools/list` isteği atar.
Ödeme/auth gerektirmez, en temiz sondadır. Başarısızsa 122 çağrı yapmaz.

Kullanım:
    python -m collar_watch.collar
"""
import asyncio
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

SAMPLED = Path(os.getenv("SAMPLED", "sampled-pools.jsonl"))
OUT = Path(os.getenv("EVAL_OUT", "evaluations.jsonl"))
COLLAR_URL = os.getenv("COLLAR_MCP_URL", "https://api.collarguardrail.com/mcp-http/mcp")
COLLAR_KEY = os.getenv("COLLAR_API_KEY", "").strip()
DELAY_SEC = float(os.getenv("DELAY_SEC", "180"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
TIMEOUT_SEC = float(os.getenv("TIMEOUT_SEC", "30"))

OK_DECISIONS = {"ALLOW", "WARN", "DENY"}

# Preflight'ta ilgilendiğimiz header'lar (auth, WAF, redirect, rate-limit, x402)
PREFLIGHT_HEADERS = {
    "content-type", "server", "cf-ray", "cf-mitigated", "location",
    "retry-after", "www-authenticate", "payment-required",
    "x-ratelimit-remaining", "mcp-session-id",
}


async def preflight(client: httpx.AsyncClient) -> bool:
    """Tek istek: tools/list. Ödeme/auth gerektirmez. Her şeyi yazdır.

    follow_redirects=False bilerek: yönlendirme varsa `location` header'ında görürüz.
    Authorization header'ı asla yazdırılmaz.
    """
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if COLLAR_KEY:
        headers["Authorization"] = f"Bearer {COLLAR_KEY}"

    try:
        r = await client.post(COLLAR_URL, headers=headers, json=payload,
                              follow_redirects=False)
    except Exception as e:
        print(f"=== PREFLIGHT === ağ hatası: {type(e).__name__}: {e}")
        return False

    print("=== PREFLIGHT ===")
    print(f"url:      {COLLAR_URL}")
    print(f"auth:     {'yes' if COLLAR_KEY else 'no'}")
    print(f"status:   {r.status_code} | http: {r.http_version}")
    print(f"redirect: {r.headers.get('location')}")
    print(f"headers:  {dict((k, v) for k, v in r.headers.items() if k.lower() in PREFLIGHT_HEADERS)}")
    print(f"body:     {r.text[:600]!r}")
    print("=== END PREFLIGHT ===")
    print()
    return r.status_code == 200


async def call_collar(client: httpx.AsyncClient, token: str, chain: str = "robinhood") -> dict:
    """Collar MCP'ye check_token_safety çağrısı yap (redirect + SSE uyumlu)."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if COLLAR_KEY:
        headers["Authorization"] = f"Bearer {COLLAR_KEY}"

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "check_token_safety",
            "arguments": {"token": token, "chain": chain},
        },
    }

    try:
        r = await client.post(COLLAR_URL, headers=headers, json=payload, follow_redirects=True)

        if r.status_code != 200 or not r.content.strip():
            return {
                "decision": "ERROR", "risk": None,
                "reason": f"HTTP {r.status_code}: {r.text[:200]!r}",
                "symbol": None, "name": None,
            }

        ctype = r.headers.get("content-type", "")
        if "text/event-stream" in ctype:
            data_line = next((l for l in r.text.splitlines() if l.startswith("data:")), None)
            if not data_line:
                return {"decision": "ERROR", "risk": None, "reason": "SSE: no data line",
                        "symbol": None, "name": None}
            data = json.loads(data_line[5:].strip())
        else:
            data = r.json()

    except Exception as e:
        return {"decision": "ERROR", "risk": None,
                "reason": f"{type(e).__name__}: {str(e)[:150]}",
                "symbol": None, "name": None}

    if "error" in data:
        return {"decision": "ERROR", "risk": None, "reason": str(data["error"])[:100],
                "symbol": None, "name": None}

    content = data.get("result", {}).get("content", [])
    if content and content[0].get("type") == "text":
        raw_text = content[0]["text"]
        try:
            inner = json.loads(raw_text)
        except json.JSONDecodeError as e:
            return {"decision": "ERROR", "risk": None, "reason": f"parse: {e}",
                    "symbol": None, "name": None}

        decision = (inner.get("decision") or "").upper()
        if decision not in OK_DECISIONS:
            # Karar yoksa ham yanıtı sakla — bir sonraki run'da ne olduğunu göreceğiz
            return {
                "decision": "ERROR", "risk": None,
                "reason": f"raw: {raw_text[:200]!r}",
                "symbol": None, "name": None,
            }

        return {
            "decision": decision,
            "risk": inner.get("risk"),
            "reason": inner.get("reason") or inner.get("severity"),
            "symbol": inner.get("symbol") or inner.get("token_symbol"),
            "name": inner.get("name") or inner.get("token_name"),
        }
    return {"decision": "ERROR", "risk": None, "reason": "empty_content",
            "symbol": None, "name": None}


async def evaluate_pool(sem: asyncio.Semaphore, client: httpx.AsyncClient,
                        pool: dict, idx: int, total: int) -> dict:
    """Bir havuzu T+0 ve T+3dk değerlendir. Semaphore sadece HTTP etrafında tutulur."""
    token = pool["token"]
    t0_started = time.time()
    print(f"[{idx}/{total}] T+0 token={token[:10]}...")

    async with sem:
        t0 = await call_collar(client, token)

    if DELAY_SEC > 0:
        await asyncio.sleep(DELAY_SEC)

    print(f"[{idx}/{total}] T+3dk token={token[:10]}...")
    async with sem:
        t3m = await call_collar(client, token)

    ok = t0["decision"] in OK_DECISIONS and t3m["decision"] in OK_DECISIONS
    changed = ok and t0["decision"] != t3m["decision"]
    age_at_t0_sec = int(t0_started - pool["ts"]) if pool.get("ts") else None

    return {
        "pool_id": pool["pool_id"],
        "token": token,
        "quote": pool["quote"],
        "hook": pool.get("hook"),
        "fee": pool.get("fee"),
        "block": pool.get("block"),
        "ts": pool.get("ts"),
        "age_at_t0_sec": age_at_t0_sec,
        "symbol": t0.get("symbol") or t3m.get("symbol"),
        "name": t0.get("name") or t3m.get("name"),
        "t0": t0,
        "t3m": t3m,
        "changed": changed,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }


def load_pools(path: Path) -> list[dict]:
    if not path.exists():
        print(f"HATA: {path} yok. Önce sampler çalıştır.", file=sys.stderr)
        sys.exit(1)
    pools = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    pools.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return pools


async def main():
    pools = load_pools(SAMPLED)
    total = len(pools)
    print(f"Değerlendirilecek havuz: {total}")
    print(f"Collar: {COLLAR_URL}")
    print(f"Auth: {'API key ile' if COLLAR_KEY else 'authsuz'}")
    print(f"Concurrency: {CONCURRENCY}, delay: {DELAY_SEC}s")
    print("=" * 60)
    if total == 0:
        sys.exit(1)

    sem = asyncio.Semaphore(CONCURRENCY)
    results = []
    start = time.time()

    with open(OUT, "w") as f:
        async with httpx.AsyncClient(timeout=TIMEOUT_SEC) as client:
            # PREFLIGHT: tek istek, sorunu 122 çağrı harcamadan erken yakala
            ok = await preflight(client)
            if not ok:
                print("PREFLIGHT başarısız — 122 çağrı yapmaya gerek yok, çıkıyorum.",
                      file=sys.stderr)
                sys.exit(2)

            tasks = [
                evaluate_pool(sem, client, pool, i + 1, total)
                for i, pool in enumerate(pools)
            ]
            for coro in asyncio.as_completed(tasks):
                try:
                    rec = await coro
                except Exception as e:
                    print(f"Havuz hatası: {e}", file=sys.stderr)
                    continue
                results.append(rec)
                f.write(json.dumps(rec) + "\n")
                f.flush()

    elapsed = time.time() - start
    print("=" * 60)
    print(f"Tamamlandı: {len(results)} / {total} havuz, {elapsed:.0f}s")

    t0_decisions = Counter(r["t0"]["decision"] for r in results)
    t3m_decisions = Counter(r["t3m"]["decision"] for r in results)
    changed_count = sum(1 for r in results if r["changed"])
    symbols_found = sum(1 for r in results if r.get("symbol"))
    fresh_pools = sum(1 for r in results if r.get("age_at_t0_sec") is not None and r["age_at_t0_sec"] < 300)

    print(f"\nT+0 kararlar: {dict(t0_decisions)}")
    print(f"T+3dk kararlar: {dict(t3m_decisions)}")
    print(f"Değişen: {changed_count} / {len(results)}")
    print(f"Symbol bulundu: {symbols_found} / {len(results)}")
    print(f"Taze havuz (<5dk): {fresh_pools} / {len(results)}")
    print(f"Yazıldı: {OUT}")

    # ERROR nedenlerini özetle — bir sonraki run'da ne olduğunu göreceğiz
    if results:
        print("\n=== En sık 5 ERROR nedeni (t0) ===")
        reasons = Counter(
            r["t0"]["reason"] for r in results
            if r["t0"]["decision"] not in OK_DECISIONS
        )
        if reasons:
            for reason, count in reasons.most_common(5):
                print(f"  [{count}x] {reason}")
        else:
            print("  (ERROR yok)")

    # Hata oranı kapısı — t0 VE t3m ERROR, exception'lar da paydaya
    bad = sum(
        1 for r in results
        if r["t0"]["decision"] not in OK_DECISIONS or r["t3m"]["decision"] not in OK_DECISIONS
    )
    failed = total - len(results)
    error_rate = (bad + failed) / total if total > 0 else 1.0

    print(f"\nHata oranı: {bad + failed}/{total} (%{error_rate*100:.1f})")
    if error_rate > 0.05:
        print(f"HATA: Error oranı %5'i aştı ({error_rate*100:.1f}%). Publish iptal.",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
