"""sampled-pools.jsonl'daki her havuz için Collar Guardrail MCP'sini çağırır.

MCP Streamable HTTP akışı:
  1. initialize → session ID al (bir kez)
  2. tools/call → session ID ile (her havuz için 2 kez)

Rate limit: 30 HTTP req/dk (handshake dahil). 25/dk hedefle.

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
TIMEOUT_SEC = float(os.getenv("TIMEOUT_SEC", "30"))

# Rate limit: sunucu 30/dk diyor, biz 22/dk hedefleyelim (buffer)
REQ_PER_MIN = float(os.getenv("REQ_PER_MIN", "22"))

OK_DECISIONS = {"ALLOW", "WARN", "DENY"}


class RateLimiter:
    """Basit token bucket — saniyede REQ_PER_MIN/60 istek."""
    def __init__(self, per_minute: float):
        self.interval = 60.0 / per_minute
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = self._next - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._next = max(now, self._next) + self.interval


rate_limiter = RateLimiter(REQ_PER_MIN)


def _base_headers() -> dict:
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if COLLAR_KEY:
        h["Authorization"] = f"Bearer {COLLAR_KEY}"
    return h


def _parse_body(r: httpx.Response) -> dict:
    """JSON veya SSE yanıtını dict'e çevir. Birden fazla data: satırı olabilir."""
    ctype = r.headers.get("content-type", "")

    if "text/event-stream" in ctype:
        # SSE: tüm "data:" satırlarını topla, SON geçerli JSON-RPC yanıtını seç
        data_lines = []
        for line in r.text.splitlines():
            if line.startswith("data:"):
                data_lines.append(line[5:].strip())
        # Sondan başa dene: gerçek sonuç genelde son event'te
        for raw in reversed(data_lines):
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and ("result" in obj or "error" in obj):
                    return obj
            except json.JSONDecodeError:
                continue
        raise ValueError(f"SSE: no valid JSON-RPC in {len(data_lines)} data lines")

    # Düz JSON
    return r.json()


async def mcp_initialize(client: httpx.AsyncClient) -> tuple[str, str]:
    """MCP handshake. (session_id, error) döner."""
    payload = {
        "jsonrpc": "2.0", "id": 0, "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "collar-watch", "version": "0.1.0"},
        },
    }
    await rate_limiter.acquire()
    try:
        r = await client.post(COLLAR_URL, headers=_base_headers(), json=payload,
                              follow_redirects=True)
        if r.status_code != 200:
            return "", f"HTTP {r.status_code}: {r.text[:200]!r}"
        body = _parse_body(r)
        if "error" in body:
            return "", f"init error: {body['error']}"
        sid = r.headers.get("mcp-session-id", "")
        return sid, ""
    except Exception as e:
        return "", f"{type(e).__name__}: {e}"


async def call_collar(client: httpx.AsyncClient, token: str, session_id: str,
                      chain: str = "robinhood") -> dict:
    """tools/call — session_id ile."""
    headers = _base_headers()
    if session_id:
        headers["Mcp-Session-Id"] = session_id

    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "check_token_safety",
            "arguments": {"token": token, "chain": chain},
        },
    }

    await rate_limiter.acquire()

    try:
        r = await client.post(COLLAR_URL, headers=headers, json=payload, follow_redirects=True)

        if r.status_code != 200 or not r.content.strip():
            return {"decision": "ERROR", "risk": None,
                    "reason": f"HTTP {r.status_code}: {r.text[:200]!r}",
                    "symbol": None, "name": None}

        data = _parse_body(r)
    except Exception as e:
        return {"decision": "ERROR", "risk": None,
                "reason": f"{type(e).__name__}: {str(e)[:200]}",
                "symbol": None, "name": None}

    if "error" in data:
        return {"decision": "ERROR", "risk": None,
                "reason": f"rpc error: {str(data['error'])[:200]}",
                "symbol": None, "name": None}

    content = data.get("result", {}).get("content", [])
    if content and content[0].get("type") == "text":
        raw_text = content[0]["text"]
        try:
            inner = json.loads(raw_text)
        except json.JSONDecodeError as e:
            return {"decision": "ERROR", "risk": None,
                    "reason": f"parse: {e} | raw: {raw_text[:150]!r}",
                    "symbol": None, "name": None}

        decision = (inner.get("decision") or "").upper()
        if decision not in OK_DECISIONS:
            return {"decision": "ERROR", "risk": None,
                    "reason": f"raw: {raw_text[:200]!r}",
                    "symbol": None, "name": None}

        return {
            "decision": decision,
            "risk": inner.get("risk"),
            "reason": inner.get("reason") or inner.get("severity"),
            "symbol": inner.get("symbol") or inner.get("token_symbol"),
            "name": inner.get("name") or inner.get("token_name"),
        }
    return {"decision": "ERROR", "risk": None, "reason": "empty_content",
            "symbol": None, "name": None}


async def evaluate_pool(client: httpx.AsyncClient, session_id: str,
                        pool: dict, idx: int, total: int) -> dict:
    """Bir havuzu T+0 ve T+3dk değerlendir. Rate limit zaten her çağrıda."""
    token = pool["token"]
    t0_started = time.time()
    print(f"[{idx}/{total}] T+0 token={token[:10]}...")

    t0 = await call_collar(client, token, session_id)

    if DELAY_SEC > 0:
        await asyncio.sleep(DELAY_SEC)

    print(f"[{idx}/{total}] T+3dk token={token[:10]}...")
    t3m = await call_collar(client, token, session_id)

    ok = t0["decision"] in OK_DECISIONS and t3m["decision"] in OK_DECISIONS
    changed = ok and t0["decision"] != t3m["decision"]
    age_at_t0_sec = int(t0_started - pool["ts"]) if pool.get("ts") else None

    return {
        "pool_id": pool["pool_id"], "token": token, "quote": pool["quote"],
        "hook": pool.get("hook"), "fee": pool.get("fee"),
        "block": pool.get("block"), "ts": pool.get("ts"),
        "age_at_t0_sec": age_at_t0_sec,
        "symbol": t0.get("symbol") or t3m.get("symbol"),
        "name": t0.get("name") or t3m.get("name"),
        "t0": t0, "t3m": t3m, "changed": changed,
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
    print(f"Rate limit: {REQ_PER_MIN}/dk ({60.0/REQ_PER_MIN:.1f}s/istek)")
    print(f"Delay (T+3dk): {DELAY_SEC}s")
    print("=" * 60)
    if total == 0:
        sys.exit(1)

    results = []
    start = time.time()

    with open(OUT, "w") as f:
        async with httpx.AsyncClient(timeout=TIMEOUT_SEC) as client:
            # MCP handshake — bir kez
            print("MCP initialize...")
            session_id, err = await mcp_initialize(client)
            if err:
                print(f"HATA: MCP initialize başarısız: {err}", file=sys.stderr)
                sys.exit(2)
            print(f"MCP session: {session_id[:20] + '...' if session_id else '(none)'}")
            print()

            # Havuzları SIRAYLA işle (rate limit zaten her çağrıda bekletiyor)
            for i, pool in enumerate(pools):
                try:
                    rec = await evaluate_pool(client, session_id, pool, i + 1, total)
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

    bad = sum(
        1 for r in results
        if r["t0"]["decision"] not in OK_DECISIONS or r["t3m"]["decision"] not in OK_DECISIONS
    )
    failed = total - len(results)
    error_rate = (bad + failed) / total if total > 0 else 1.0
    print(f"\nHata oranı: {bad + failed}/{total} (%{error_rate*100:.1f})")
    if error_rate > 0.05:
        print(f"HATA: Error oranı %5'i aştı (%{error_rate*100:.1f}%). Publish iptal.",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
