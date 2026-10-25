"""
Aşama 0: 24-48 saatlik havuz sıklığı ölçümü.
Hiçbir Collar çağrısı yapmaz, sadece Uniswap V4 Initialize olaylarını sayar.

Önce WSS (eth_subscribe) dener. Başarısız olursa HTTP polling'e geçer.

Kullanım:
    python -m collar_watch.counter                      # 24 saat
    DURATION_HOURS=0.05 python -m collar_watch.counter  # 3 dakikalık test
    POOL_MANAGER=0x... python -m collar_watch.counter   # belirli kontrat
    POOL_MANAGER= python -m collar_watch.counter        # keşif modu
"""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx
import websockets
from eth_utils import keccak

# --- Config ---
WSS = os.getenv("ROBINHOOD_WSS", "wss://feed.mainnet.chain.robinhood.com")
RPC = os.getenv("ROBINHOOD_RPC", "https://rpc.mainnet.chain.robinhood.com")
POOL_MANAGER = os.getenv("POOL_MANAGER", "").strip().lower()
DURATION_HOURS = float(os.getenv("DURATION_HOURS", "24"))
LOG_FILE = os.getenv("COUNTER_LOG", "counter.log")
SUMMARY_FILE = os.getenv("COUNTER_SUMMARY", "counter-summary.json")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "15"))

# Uniswap V4 PoolManager.Initialize event
INITIALIZE_SIG = "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
INITIALIZE_TOPIC = "0x" + keccak(text=INITIALIZE_SIG).hex()

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
log = logging.getLogger("counter")


def process_event(ev, state):
    state["total"] += 1
    addr = ev["address"].lower()
    state["by_address"][addr] += 1
    block = int(ev["blockNumber"], 16)
    state["recent_blocks"].append(block)
    state["recent_blocks"] = state["recent_blocks"][-50:]
    tx = ev.get("transactionHash", "")
    tx = tx if isinstance(tx, str) else tx.hex()
    log.info(
        "EVENT #%d | contract=%s | block=%d | tx=%s",
        state["total"], addr[:12], block, tx[:12],
    )


def heartbeat(state, start):
    elapsed_h = (time.time() - start) / 3600
    rate = state["total"] / elapsed_h if elapsed_h > 0 else 0
    log.info(
        "--- HEARTBEAT | %.2fh | total=%d | %.1f/saat | %.1f/gün ---",
        elapsed_h, state["total"], rate, rate * 24,
    )


async def run_wss(start, end, state):
    params = {"topics": [INITIALIZE_TOPIC]}
    if POOL_MANAGER:
        params["address"] = POOL_MANAGER

    async with websockets.connect(
        WSS, ping_interval=20, ping_timeout=20, close_timeout=10
    ) as ws:
        await ws.send(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
            "params": ["logs", params],
        }))
        resp = json.loads(await ws.recv())
        if "error" in resp:
            raise RuntimeError(f"Subscribe failed: {resp['error']}")
        sub_id = resp["result"]
        log.info("WSS abone olundu: %s", sub_id)
        state["mode"] = "wss"

        last_heartbeat = start
        async for raw in ws:
            if time.time() >= end:
                return
            msg = json.loads(raw)
            if msg.get("method") != "eth_subscription":
                continue
            if msg["params"]["subscription"] != sub_id:
                continue
            process_event(msg["params"]["result"], state)
            if time.time() - last_heartbeat > 600:
                heartbeat(state, start)
                last_heartbeat = time.time()


async def run_polling(start, end, state):
    log.info("HTTP polling modu (her %.0f saniyede bir eth_getLogs)", POLL_INTERVAL_SEC)
    state["mode"] = "http-polling"

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(RPC, json={
            "jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": [],
        })
        last_block = int(r.json()["result"], 16)
        log.info("Başlangıç bloğu: %d", last_block)

    last_heartbeat = start
    while time.time() < end:
        try:
            params = {
                "fromBlock": hex(last_block + 1),
                "toBlock": "latest",
                "topics": [INITIALIZE_TOPIC],
            }
            if POOL_MANAGER:
                params["address"] = POOL_MANAGER

            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(RPC, json={
                    "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
                    "params": [params],
                })
                data = r.json()

            if "error" in data:
                log.warning("getLogs error: %s", data["error"])
            else:
                for ev in data["result"]:
                    process_event(ev, state)
                    last_block = max(last_block, int(ev["blockNumber"], 16))

            if time.time() - last_heartbeat > 600:
                heartbeat(state, start)
                last_heartbeat = time.time()

        except Exception as e:
            log.warning("polling hatası: %s", e)

        await asyncio.sleep(POLL_INTERVAL_SEC)


def write_summary(start, state):
    elapsed_h = (time.time() - start) / 3600
    summary = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "duration_hours": round(elapsed_h, 2),
        "mode": state["mode"],
        "contract_filter": POOL_MANAGER or None,
        "total_events": state["total"],
        "events_per_hour": round(state["total"] / elapsed_h, 2) if elapsed_h else 0,
        "events_per_day": round(state["total"] / elapsed_h * 24, 1) if elapsed_h else 0,
        "by_contract": dict(state["by_address"]),
        "block_range": {
            "min": min(state["recent_blocks"]) if state["recent_blocks"] else None,
            "max": max(state["recent_blocks"]) if state["recent_blocks"] else None,
        },
        "wss": WSS,
        "rpc": RPC,
        "topic": INITIALIZE_TOPIC,
    }
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    log.info("=" * 60)
    log.info("ÖLÇÜM BİTTİ")
    log.info(json.dumps(summary, indent=2))
    log.info("=" * 60)


async def run():
    start = time.time()
    end = start + DURATION_HOURS * 3600
    state = {
        "total": 0,
        "by_address": defaultdict(int),
        "recent_blocks": [],
        "mode": "unknown",
    }

    log.info("=" * 60)
    log.info("Aşama 0 — havuz sıklığı ölçümü")
    log.info("Kontrat:    %s", POOL_MANAGER or "(KEŞİF — filtre yok)")
    log.info("WSS:        %s", WSS)
    log.info("RPC:        %s", RPC)
    log.info("Topic:      %s", INITIALIZE_TOPIC)
    log.info("Süre:       %.2f saat", DURATION_HOURS)
    log.info("=" * 60)

    try:
        await run_wss(start, end, state)
    except (RuntimeError, ConnectionError, OSError) as e:
        log.warning("WSS başarısız: %s", e)
        log.info("HTTP polling'e geçiliyor")
        await run_polling(start, end, state)
    finally:
        write_summary(start, state)


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Kullanıcı durdurdu")


if __name__ == "__main__":
    main()
