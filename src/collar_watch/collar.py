"""sampled-pools.jsonl'daki her havuz için Collar Guardrail MCP'sini çağırır.

Akış:
  1. tools/list → check_token_safety inputSchema'sını oku
  2. Smoke call → ilk havuzun token'ı ile gerçek tools/call
  3. initialize → session ID (stateless ise none)
  4. Her havuz için T+0 ve T+3dk (paralel, Semaphore(CONCURRENCY))

Kullanım:
    python -m collar_watch.collar
    LIMIT=3 DELAY_SEC=5 python -m collar_watch.collar    # smoke test
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
REQ_PER_MIN = float(os.getenv("REQ_PER_MIN", "22"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
LIMIT = int(os.getenv("LIMIT", "0"))  # 0 = hepsi, >0 = ilk N

OK_DECISIONS = {"ALLOW", "WARN", "DENY"}

# Token için kullanılabilecek alan adları
TOKEN_KEYS = {"contract_address", "token_address", "address", "token"}
# Chain için kullanılabilecek alan adları
CHAIN_KEYS = {"chain", "network", "chain_id", "network_id"}
CHAIN_INT = 4663
CHAIN_STR = "robinhood"


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
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if COLLAR_KEY:
        h["Authorization"] = f"Bearer {COLLAR_KEY}"
    return h


def _parse_body(r: httpx.Response) -> dict:
    ctype = r.headers.get("content-type", "")
    if "text/event-stream" in ctype:
        data_lines = [l[5:].strip() for l in r.text.splitlines() if l.startswith("data:")]
        for raw in reversed(data_lines):
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and ("result" in obj or "error" in obj):
                    return obj
            except json.JSONDecodeError:
                continue
        raise ValueError(f"SSE: no valid JSON-RPC in {len(data_lines)} data lines")
    return r.json()


def _extract_text(result: dict) -> str:
    """result.content[0].text — varsa. Yoksa boş string."""
    content = result.get("content", [])
    if content and content[0].get("type") == "text":
        return content[0].get("text", "")
    return ""


def build_args(schema: dict, token: str) -> dict:
    """Schema'dan argüman üret. Eksik zorunlu parametre varsa RuntimeError."""
    props = schema.get("properties", {})
    required = schema.get("required", [])
    args = {}

    for k, v in props.items():
        vtype = v.get("type", "")
        if k in TOKEN_KEYS:
            args[k] = token
        elif k in CHAIN_KEYS:
            args[k] = CHAIN_INT if vtype == "integer" else CHAIN_STR

    missing = [r for r in required if r not in args]
    if missing:
        raise RuntimeError(
            f"Karşılanamayan zorunlu parametreler: {missing}. "
            f"Schema required={required}, properties={list(props.keys())}"
        )
    return args


async def preflight(client: httpx.AsyncClient, smoke_token: str | None = None
                    ) -> tuple[dict | None, str]:
    """tools/list → schema dump. smoke_token verilirse tools/call dener."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    await rate_limiter.acquire()

    try:
        r = await client.post(COLLAR_URL, headers=_base_headers(), json=payload,
                              follow_redirects=False)
    except Exception as e:
        return None, f"preflight ağ hatası: {type(e).__name__}: {e}"

    print("=== PREFLIGHT ===")
    print(f"status: {r.status_code}")
    print(f"body[:300]: {r.text[:300]!r}")

    if r.status_code != 200:
        return None, f"tools/list HTTP {r.status_code}"

    try:
        data = _parse_body(r)
    except Exception as e:
        return None, f"tools/list parse: {e}"

    if "error" in data:
        return None, f"tools/list rpc error: {data['error']}"

    tools = data.get("result", {}).get("tools", [])
    print(f"tools ({len(tools)}): {[t.get('name') for t in tools]}")

    schema = None
    for t in tools:
        if t.get("name") == "check_token_safety":
            schema = t.get("inputSchema", {})
            print("\n=== check_token_safety INPUT SCHEMA ===")
            print(json.dumps(schema, indent=2))
            print("=== END SCHEMA ===\n")
            break

    if schema is None:
        return None, "check_token_safety tool bulunamadı"

    # Smoke call — gerçek tools/call
    if smoke_token:
        print(f"=== SMOKE CALL token={smoke_token[:10]}... ===")
        try:
            args = build_args(schema, smoke_token)
            print(f"args: {args}")
        except RuntimeError as e:
            print(f"SMOKE FAIL (args): {e}", file=sys.stderr)
            return schema, str(e)

        smoke = await _do_call(client, smoke_token, "", schema)
        print(f"smoke decision: {smoke['decision']}")
        print(f"smoke reason: {smoke['reason']}")
        print("=== END SMOKE ===\n")

        if smoke["decision"] not in OK_DECISIONS:
            return schema, f"smoke call başarısız: {smoke['reason']}"

    print("=== END PREFLIGHT ===\n")
    return schema, ""


async def _do_call(client: httpx.AsyncClient, token: str, session_id: str,
                   schema: dict, chain: str = "robinhood") -> dict:
    """Gerçek tools/call. build_args ile schema'dan argüman üretir."""
    try:
        args = build_args(schema, token)
    except RuntimeError as e:
        return {"decision": "ERROR", "risk": None, "reason": str(e),
                "symbol": None, "name": None}

    headers = _base_headers()
    if session_id:
        headers["Mcp-Session-Id"] = session_id

    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "check_token_safety", "arguments": args},
    }

    await rate_limiter.acquire()

    try:
        r = await client.post(COLLAR_URL, headers=headers, json=payload, follow_redirects=True)
        if r.status_code != 200 or not r.content.strip():
            return {"decision": "ERROR", "risk": None,
                    "reason": f"HTTP {r.status_code}: {r.text[:500]!r}",
                    "symbol": None, "name": None}
        data = _parse_body(r)
    except Exception as e:
        return {"decision": "ERROR", "risk": None,
                "reason": f"{type(e).__name__}: {str(e)[:300]}",
                "symbol": None, "name": None}

    # JSON-RPC seviyesi hata
    if "error" in data:
        return {"decision": "ERROR", "risk": None,
                "reason": f"rpc error: {str(data['error'])[:500]}",
                "symbol": None, "name": None}

    result = data.get("result", {})

    # FastMCP doğrulama hataları: result.isError=true + text içinde mesaj
    if result.get("isError"):
        raw_text = _extract_text(result)
        return {"decision": "ERROR", "risk": None,
                "reason": f"tool isError: {raw_text[:500]}",
                "symbol": None, "name": None}

    raw_text = _extract_text(result)
    if not raw_text:
        return {"decision": "ERROR", "risk": None, "reason": "empty_content",
                "symbol": None, "name": None}

    # text içindeki JSON'u parse et
    try:
        inner = json.loads(raw_text)
    except json.JSONDecodeError as e:
        # JSON değilse muhtemelen doğrulama hatası metni — tamamını göster
        return {"decision": "ERROR", "risk": None,
                "reason": f"non-json: {e} | raw: {raw_text[:500]!r}",
                "symbol": None, "name": None}

    decision = (inner.get("decision") or "").upper()
    if decision not in OK_DECISIONS:
        return {"decision": "ERROR", "risk": None,
                "reason": f"raw: {raw_text[:500]!r}",
                "symbol": None, "name": None}

    return {
        "decision": decision,
        "risk": inner.get("risk"),
        "reason": inner.get("reason") or inner.get("severity"),
        "symbol": inner.get("symbol") or inner.get("token_symbol"),
        "name": inner.get("name") or inner.get("token_name"),
    }


async def call_collar(client: httpx.AsyncClient, token: str, session_id: str,
                      schema: dict, chain: str = "robinhood") -> dict:
    """_do_call'un kolaylık sarmalayıcısı."""
    return await _do_call(client, token, session_id, schema, chain)


async def mcp_initialize(client: httpx.AsyncClient) -> tuple[str, str]:
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


async def evaluate_pool(sem: asyncio.Semaphore, client: httpx.AsyncClient,
                        session_id: str, schema: dict, pool: dict,
                        idx: int, total: int) -> dict:
    """Semaphore ile sınırlandırılmış havuz işleme. Rate limiter zaten bekletiyor."""
    async with sem:
        token = pool["token"]
        t0_started = time.time()
        print(f"[{idx}/{total}] T+0 token={token[:10]}...")

        t0 = await call_collar(client, token, session_id, schema)

        if DELAY_SEC > 0:
            await asyncio.sleep(DELAY_SEC)

        print(f"[{idx}/{total}] T+3dk token={token[:10]}...")
        t3m = await call_collar(client, token, session_id, schema)

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
    if LIMIT > 0:
        pools = pools[:LIMIT]
        print(f"LIMIT={LIMIT} — ilk {len(pools)} havuz işlenecek")

    total = len(pools)
    print(f"Değerlendirilecek havuz: {total}")
    print(f"Collar: {COLLAR_URL}")
    print(f"Auth: {'API key ile' if COLLAR_KEY else 'authsuz'}")
    print(f"Rate limit: {REQ_PER_MIN}/dk ({60.0/REQ_PER_MIN:.1f}s/istek)")
    print(f"Concurrency: {CONCURRENCY}, delay: {DELAY_SEC}s")
    print("=" * 60)
    if total == 0:
        sys.exit(1)

    results = []
    start = time.time()

    with open(OUT, "w") as f:
        async with httpx.AsyncClient(timeout=TIMEOUT_SEC) as client:
            # 1. Preflight + schema dump + smoke call (ilk havuzun token'ı)
            smoke_token = pools[0]["token"] if pools else None
            schema, err = await preflight(client, smoke_token)
            if err:
                print(f"HATA: preflight başarısız: {err}", file=sys.stderr)
                sys.exit(2)

            # 2. MCP initialize (stateless ise session none normal)
            print("MCP initialize...")
            session_id, init_err = await mcp_initialize(client)
            if init_err:
                print(f"HATA: MCP initialize başarısız: {init_err}", file=sys.stderr)
                sys.exit(2)
            print(f"MCP session: {session_id[:20] + '...' if session_id else '(none)'}")
            print()

            # 3. Paralel işleme (Semaphore ile bounded)
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [
                evaluate_pool(sem, client, session_id, schema, pool, i + 1, total)
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
        print(f"HATA: Error oranı %5'i aştı (%{error_rate*100:.1f}). Publish iptal.",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
