"""sampled-pools.jsonl'daki her havuz için Collar Guardrail MCP'sini çağırır."""
import asyncio
import json
import os
import random
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
REQ_PER_MIN = float(os.getenv("REQ_PER_MIN", "8")) # 22 yerine 8 (Güvenli liman)
CONCURRENCY = int(os.getenv("CONCURRENCY", "1"))  # 5 yerine 1 (Kesin sıralı geçiş)
LIMIT = int(os.getenv("LIMIT", "0"))

POOL_INTERVAL_SEC = 1.2 * 2 * 60.0 / REQ_PER_MIN
OK_DECISIONS = {"ALLOW", "WARN", "DENY"}
SEVERITY_MAP = {"safe": "ALLOW", "warn": "WARN", "danger": "DENY"}

class RateLimiter:
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
    h = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    if COLLAR_KEY: h["Authorization"] = f"Bearer {COLLAR_KEY}"
    return h

def _parse_body(r: httpx.Response) -> dict:
    ctype = r.headers.get("content-type", "")
    if "text/event-stream" in ctype:
        data_lines = [l[5:].strip() for l in r.text.splitlines() if l.startswith("data:")]
        for raw in reversed(data_lines):
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and ("result" in obj or "error" in obj): return obj
            except json.JSONDecodeError: continue
        raise ValueError(f"SSE: no valid JSON-RPC in {len(data_lines)} data lines")
    return r.json()

def _extract_text(result: dict) -> str:
    content = result.get("content", [])
    if content and content[0].get("type") == "text": return content[0].get("text", "")
    return ""

def build_args(token: str) -> dict:
    return {"contract_address": token}

async def _post_with_retry(client: httpx.AsyncClient, headers: dict, payload: dict, retries: int = 4) -> httpx.Response:
    last_exc = None
    for attempt in range(retries + 1):
        try:
            r = await client.post(COLLAR_URL, headers=headers, json=payload, follow_redirects=True)
            if r.status_code == 429 or r.status_code >= 500:
                if attempt < retries:
                    # Retry-After varsa ona uy, yoksa exponential backoff + jitter
                    try: wait = float(r.headers.get("retry-after", 10 * (2 ** attempt)))
                    except (ValueError, TypeError): wait = 10.0 * (2 ** attempt)
                    wait += random.uniform(1, 5) # Jitter ekle
                    print(f"  HTTP {r.status_code} → {wait:.1f}s bekleniyor", file=sys.stderr)
                    await asyncio.sleep(wait)
                    continue
            return r
        except (httpx.TimeoutException, httpx.TransportError) as e:
            last_exc = e
            if attempt < retries:
                await asyncio.sleep(5.0 * (2 ** attempt))
                continue
            raise
    if last_exc: raise last_exc
    raise RuntimeError("retry döngüsü bitti")

async def preflight(client: httpx.AsyncClient, smoke_token: str | None = None) -> tuple[dict | None, str]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    await rate_limiter.acquire()
    try:
        r = await _post_with_retry(client, _base_headers(), payload)
    except Exception as e: return None, f"preflight ağ hatası: {e}"
    
    print(f"=== PREFLIGHT ===\nstatus: {r.status_code}")
    if r.status_code != 200: return None, f"tools/list HTTP {r.status_code}"
    
    try: data = _parse_body(r)
    except Exception as e: return None, f"tools/list parse: {e}"
    if "error" in data: return None, f"tools/list rpc error: {data['error']}"

    tools = data.get("result", {}).get("tools", [])
    print(f"tools ({len(tools)}): {[t.get('name') for t in tools]}")
    
    schema = None
    for t in tools:
        if t.get("name") == "check_token_safety":
            schema = t.get("inputSchema", {})
            print(f"\n=== check_token_safety INPUT SCHEMA ===\n{json.dumps(schema, indent=2)}\n=== END SCHEMA ===\n")
            break
    if schema is None: return None, "check_token_safety tool bulunamadı"

    if smoke_token:
        print(f"=== SMOKE CALL token={smoke_token[:10]}... ===")
        smoke = await _do_call(client, smoke_token, "", {})
        print(f"smoke decision: {smoke['decision']}")
        if smoke["decision"] not in OK_DECISIONS: return schema, f"smoke call başarısız: {smoke['reason']}"
        print("=== END SMOKE ===\n")
    print("=== END PREFLIGHT ===\n")
    return schema, ""

async def _do_call(client: httpx.AsyncClient, token: str, session_id: str, schema: dict, stamp: dict | None = None) -> dict:
    headers = _base_headers()
    if session_id: headers["Mcp-Session-Id"] = session_id
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "check_token_safety", "arguments": build_args(token)}}

    max_retries = 5
    for attempt in range(max_retries):
        await rate_limiter.acquire()
        if stamp is not None: stamp["t"] = time.time()

        try:
            r = await _post_with_retry(client, headers, payload)
            if r.status_code != 200 or not r.content.strip():
                return {"decision": "ERROR", "severity": None, "risk_factors": [], "sell_simulation_ok": None, "reason": f"HTTP {r.status_code}: {r.text[:200]!r}", "raw": r.text}
            data = _parse_body(r)
        except Exception as e:
            return {"decision": "ERROR", "severity": None, "risk_factors": [], "sell_simulation_ok": None, "reason": f"{type(e).__name__}: {str(e)[:200]}", "raw": ""}

        if "error" in data:
            return {"decision": "ERROR", "severity": None, "risk_factors": [], "sell_simulation_ok": None, "reason": f"rpc error: {str(data['error'])[:200]}", "raw": json.dumps(data)}

        result = data.get("result", {})
        raw_text = _extract_text(result)

        if result.get("isError") or not raw_text:
            return {"decision": "ERROR", "severity": None, "risk_factors": [], "sell_simulation_ok": None, "reason": "tool isError or empty_content", "raw": raw_text}

        try:
            inner = json.loads(raw_text)
        except json.JSONDecodeError as e:
            return {"decision": "ERROR", "severity": None, "risk_factors": [], "sell_simulation_ok": None, "reason": f"non-json: {e}", "raw": raw_text}

        # JSON İÇİNDEKİ GİZLİ 429'U YAKALAMA (Madde 1)
        if inner.get("error") is not None:
            err_str = str(inner.get("error")).lower()
            if "429" in err_str or "rate limit" in err_str:
                wait = (15.0 * (2 ** attempt)) + random.uniform(2, 8) # 15s, 30s, 60s + jitter
                print(f"  [JSON 429] Tool Rate Limit. {wait:.1f}s bekleniyor (Deneme {attempt+1}/{max_retries})", file=sys.stderr)
                await asyncio.sleep(wait)
                continue
            return {"decision": "ERROR", "severity": None, "risk_factors": [], "sell_simulation_ok": None, "reason": f"tool error: {inner.get('error')}", "raw": raw_text}

        severity = (inner.get("severity") or "").lower()
        decision = SEVERITY_MAP.get(severity)
        if decision is None:
            return {"decision": "ERROR", "severity": severity, "risk_factors": [], "sell_simulation_ok": None, "reason": f"unknown severity: {severity}", "raw": raw_text}

        return {
            "decision": decision,
            "severity": severity,
            "risk_factors": inner.get("risk_factors", []) or [],
            "sell_simulation_ok": inner.get("sell_simulation_ok"),
            "reason": "; ".join(inner.get("risk_factors", []) or []) if inner.get("risk_factors") else None,
            "raw": raw_text,
        }
    
    return {"decision": "ERROR", "severity": None, "risk_factors": [], "sell_simulation_ok": None, "reason": "Max retries exceeded for tool rate limit", "raw": ""}

async def mcp_initialize(client: httpx.AsyncClient) -> tuple[str, str]:
    payload = {"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "collar-watch", "version": "0.1.0"}}}
    await rate_limiter.acquire()
    try:
        r = await _post_with_retry(client, _base_headers(), payload)
        if r.status_code != 200: return "", f"HTTP {r.status_code}"
        body = _parse_body(r)
        if "error" in body: return "", f"init error: {body['error']}"
        return r.headers.get("mcp-session-id", ""), ""
    except Exception as e: return "", f"{type(e).__name__}: {e}"

async def evaluate_pool(sem: asyncio.Semaphore, client: httpx.AsyncClient, session_id: str, schema: dict, pool: dict, idx: int, total: int) -> dict:
    token = pool["token"]
    s0, s3 = {}, {}
    async with sem:
        print(f"[{idx}/{total}] T+0 token={token[:10]}...")
        t0 = await _do_call(client, token, session_id, schema, s0)
    if DELAY_SEC > 0: await asyncio.sleep(DELAY_SEC)
    async with sem:
        print(f"[{idx}/{total}] T+3dk token={token[:10]}...")
        t3m = await _do_call(client, token, session_id, schema, s3)

    ok = t0["decision"] in OK_DECISIONS and t3m["decision"] in OK_DECISIONS
    changed = ok and t0["decision"] != t3m["decision"]
    return {
        "pool_id": pool["pool_id"], "token": token, "quote": pool.get("quote"), "hook": pool.get("hook"), "fee": pool.get("fee"), "block": pool.get("block"), "ts": pool.get("ts"),
        "age_at_t0_sec": int(s0["t"] - pool["ts"]) if pool.get("ts") and "t" in s0 else None,
        "t3m_gap_sec": int(s3["t"] - s0["t"]) if "t" in s0 and "t" in s3 else None,
        "t0": t0, "t3m": t3m, "changed": changed, "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }

def load_pools(path: Path) -> list[dict]:
    if not path.exists():
        print(f"HATA: {path} yok.", file=sys.stderr)
        sys.exit(1)
    pools = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try: pools.append(json.loads(line))
                except json.JSONDecodeError: continue
    return pools

async def main():
    pools = load_pools(SAMPLED)
    
    # MADDE 4: HAVUZU DEDUPLİKE ET (Aynı token'ı tekrar sorgulama)
    unique_pools = {}
    for p in pools:
        if p["token"] not in unique_pools:
            unique_pools[p["token"]] = p
    original_count = len(pools)
    pools = list(unique_pools.values())
    print(f"Tekilleştirme: {original_count} havuzdan {len(pools)} benzersiz token kaldı.")

    if LIMIT > 0:
        pools = pools[:LIMIT]
        print(f"LIMIT={LIMIT} — ilk {len(pools)} havuz işlenecek")

    total = len(pools)
    print(f"Collar: {COLLAR_URL}")
    print(f"Rate limit: {REQ_PER_MIN}/dk ({60.0/REQ_PER_MIN:.1f}s/istek)")
    print(f"Concurrency: {CONCURRENCY}, delay: {DELAY_SEC}s")
    print("=" * 60)
    if total == 0: sys.exit(1)

    results = []
    start = time.time()

    async with httpx.AsyncClient(timeout=TIMEOUT_SEC) as client:
        smoke_token = pools[0]["token"] if pools else None
        schema, err = await preflight(client, smoke_token)
        if err:
            print(f"HATA: preflight başarısız: {err}", file=sys.stderr)
            sys.exit(2)

        print("MCP initialize...")
        session_id, init_err = await mcp_initialize(client)
        if init_err:
            print(f"HATA: MCP initialize başarısız: {init_err}", file=sys.stderr)
            sys.exit(2)
        print(f"MCP session: {session_id[:20] + '...' if session_id else '(none)'}\n")

        async def launch(i: int, pool: dict) -> dict:
            await asyncio.sleep(i * POOL_INTERVAL_SEC)
            return await evaluate_pool(sem, client, session_id, schema, pool, i + 1, total)

        with open(OUT, "w") as f:
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [launch(i, pool) for i, pool in enumerate(pools)]
            for coro in asyncio.as_completed(tasks):
                try: rec = await coro
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
    
    print(f"\nT+0 kararlar: {dict(t0_decisions)}")
    print(f"T+3dk kararlar: {dict(t3m_decisions)}")
    print(f"Değişen: {changed_count} / {len(results)}")
    print(f"Yazıldı: {OUT}")

    bad = sum(1 for r in results if r["t0"]["decision"] not in OK_DECISIONS or r["t3m"]["decision"] not in OK_DECISIONS)
    failed = total - len(results)
    error_rate = (bad + failed) / total if total > 0 else 1.0
    print(f"\nHata oranı: {bad + failed}/{total} (%{error_rate*100:.1f})")
    if error_rate > 0.05:
        print(f"HATA: Error oranı %5'i aştı (%{error_rate*100:.1f}). Publish iptal.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())