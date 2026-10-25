"""t24h.py - T+24h ölçümü.

live-events.jsonl'deki havuzlardan T0'dan (yoksa tespitten) 24 saat geçmiş ve
henüz t24h kaydı olmayanları Collar'a tekrar sorar; sonucu t24h-events.jsonl'e
(append-only) yazar. Saatlik cron ile çalışır, bu yüzden gerçek ölçüm
T+24h..T+25h aralığındadır; "late_sec" bunu kaydeder.

LP pull / sell-block gibi zincir sonuçları (rug etiketi) burada ölçülmez,
onlar outcomes.py'nin işi; bu dosya sadece Collar kararının 24h sonraki halini yazar.

Env: LIVE_EVENTS, T24H_EVENTS, T24H_SEC (varsayılan 86400), MAX_PER_RUN (150)
Kullanım:
    python -m collar_watch.t24h
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from collar_watch import collar as C

LIVE_EVENTS = Path(os.getenv("LIVE_EVENTS", "live-events.jsonl"))
T24H_EVENTS = Path(os.getenv("T24H_EVENTS", "t24h-events.jsonl"))
HORIZON_SEC = float(os.getenv("T24H_SEC", str(24 * 3600)))
MAX_PER_RUN = int(os.getenv("MAX_PER_RUN", "150"))


def read_jsonl(path: Path):
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def due_pools(now: float) -> list[tuple[float, dict]]:
    live: dict[str, dict] = {}
    for r in read_jsonl(LIVE_EVENTS):
        pid, st = r.get("pool_id"), r.get("stage")
        if pid and st in ("init", "t0"):
            live.setdefault(pid, {})[st] = r

    done = {r.get("pool_id") for r in read_jsonl(T24H_EVENTS) if r.get("stage") == "t24h"}

    out = []
    for pid, st in live.items():
        if pid in done or "init" not in st:
            continue
        base = st["t0"]["called_at"] if "t0" in st else st["init"]["detected_at"]
        target = base + HORIZON_SEC
        if target <= now:
            out.append((target, st))
    out.sort(key=lambda x: x[0])        # en eski vade önce
    return out[:MAX_PER_RUN]


async def main():
    due = due_pools(time.time())
    print(f"T24h vadesi gelen havuz: {len(due)} (üst sınır {MAX_PER_RUN})", flush=True)
    if not due:
        return

    n_ok = n_err = 0
    async with httpx.AsyncClient(timeout=C.TIMEOUT_SEC) as client:
        sid, err = await C.mcp_initialize(client)
        if err:
            print(f"HATA: MCP initialize: {err}", file=sys.stderr)
            sys.exit(2)

        T24H_EVENTS.parent.mkdir(parents=True, exist_ok=True)
        with open(T24H_EVENTS, "a") as f:
            for target, st in due:
                init = st["init"]
                s = {}
                res = await C._do_call(client, init["token"], sid, {}, s)
                called = s.get("t", time.time())

                d0 = ((st.get("t0") or {}).get("result") or {}).get("decision")
                d24 = res["decision"]
                both_ok = d0 in C.OK_DECISIONS and d24 in C.OK_DECISIONS
                rec = {
                    "stage": "t24h",
                    "pool_id": init["pool_id"],
                    "token": init["token"],
                    "called_at": called,
                    "late_sec": int(called - target),
                    "t0_decision": d0,
                    "changed_vs_t0": bool(both_ok and d0 != d24),
                    "result": res,
                    "written_at": datetime.now(timezone.utc).isoformat(),
                }
                f.write(json.dumps(rec) + "\n")
                f.flush()
                if d24 in C.OK_DECISIONS:
                    n_ok += 1
                else:
                    n_err += 1
                    print(f"  ERROR {init['token'][:10]}...: {res.get('reason')}", flush=True)

    print(f"Bitti: ok={n_ok} error={n_err}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
