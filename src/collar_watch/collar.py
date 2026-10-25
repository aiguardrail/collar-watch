"""sampled-pools.jsonl'daki her havuz için Collar Guardrail MCP'sini çağırır.

Her havuz için:
  1. T+0: check_token_safety
  2. 3 dakika bekle
  3. T+3dk: tekrar check_token_safety
  4. Sonucu evaluations.jsonl'a yaz

Çıktı satırı:
  {
    "pool_id": "...", "token": "...", "quote": "...", "hook": "...",
    "t0": {"decision": "ALLOW", "risk": 22, "reason": null},
    "t3m": {"decision": "DENY", "risk": 78, "reason": "honeypot"},
    "changed": true,
    "evaluated_at": "..."
  }

Kullanım:
    python -m collar_watch.collar
    COLLAR_API_KEY=... CONCURRENCY=5 python -m collar_watch.collar
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
SAMPLED = DATA_DIR / "sampled-pools.jsonl"
OUT = Path(os.getenv("EVAL_OUT", "evaluations.jsonl"))
COLLAR_URL = os.getenv("COLLAR_MCP_URL", "https://api.collarguardrail.com/mcp-http/mcp")
COLLAR_KEY = os.getenv("COLLAR_API_KEY", "").strip()
DELAY_SEC = float(os.getenv("DELAY_SEC", "180"))     # T+3dk bekleme
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))     # paralel havuz
TIMEOUT_SEC = float(os.getenv("TIMEOUT_SEC", "30"))


async def call_collar(client: httpx.AsyncClient, token: str, chain: str = "robinhood") -> dict:
    """Collar MCP'ye check_token_safety çağrısı yap."""
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
        r = await client.post(COLLAR_URL, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
    except httpx.HTTPStatusError as e:
        return {"decision": "ERROR", "risk": None,
                "reason": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"decision": "ERROR", "risk": None, "reason": str(e)[:100]}

    if "error" in data:
        return {"decision": "ERROR", "risk": None, "reason": str(data["error"])[:100]}

    content = data.get("result", {}).get("content", [])
    if content and content[0].get("type") == "text":
        try:
            inner = json.loads(content[0]["text"])
            return {
                "decision": (inner.get("decision") or "UNKNOWN").upper(),
                "risk": inner.get("risk"),
                "reason": inner.get("reason") or inner.get("severity"),
            }
        except (json.JSONDecodeError, KeyError) as e:
            return {"decision": "ERROR", "risk": None, "reason": f"parse: {e}"}
    return {"decision": "ERROR", "risk": None, "reason": "empty_content"}


async def evaluate_pool(sem: asyncio.Semaphore, client: httpx.AsyncClient,
                        pool: dict, idx: int, total: int) -> dict:
    """Bir havuzu T+0 ve T+3dk değerlendir."""
    async with sem:
        token = pool["token"]
        print(f"[{idx}/{total}] T+0 token={token[:10]}...")

        t0 = await call_collar(client, token)

        if DELAY_SEC > 0:
            await asyncio.sleep(DELAY_SEC)

        print(f"[{idx}/{total}] T+3dk token={token[:10]}...")
        t3m = await call_collar(client, token)

        changed = t0["decision"] != t3m["decision"]

        return {
            "pool_id": pool["pool_id"],
            "token": token,
            "quote": pool["quote"],
            "hook": pool.get("hook"),
            "fee": pool.get("fee"),
            "block": pool.get("block"),
            "ts": pool.get("ts"),
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

    # Paralel işleme (ama her havuz içinde 3 dk seri bekler)
    sem = asyncio.Semaphore(CONCURRENCY)
    results = []
    start = time.time()

    async with httpx.AsyncClient(timeout=TIMEOUT_SEC) as client:
        tasks = [
            evaluate_pool(sem, client, pool, i + 1, total)
            for i, pool in enumerate(pools)
        ]
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                results.append(result)
            except Exception as e:
                print(f"Havuz hatası: {e}", file=sys.stderr)

    # Sonuçları yaz
    with open(OUT, "w") as f:
        for rec in results:
            f.write(json.dumps(rec) + "\n")

    elapsed = time.time() - start
    print("=" * 60)
    print(f"Tamamlandı: {len(results)} / {total} havuz, {elapsed:.0f}s")

    # Özet
    from collections import Counter
    t0_decisions = Counter(r["t0"]["decision"] for r in results)
    t3m_decisions = Counter(r["t3m"]["decision"] for r in results)
    changed_count = sum(1 for r in results if r["changed"])

    print(f"\nT+0 kararlar: {dict(t0_decisions)}")
    print(f"T+3dk kararlar: {dict(t3m_decisions)}")
    print(f"Değişen: {changed_count} / {len(results)}")
    print(f"Yazıldı: {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
