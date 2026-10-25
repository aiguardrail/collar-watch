"""
Aşama 0: 24-48 saatlik havuz sıklığı ölçümü.
Hiçbir Collar çağrısı yapmaz, sadece Uniswap V4 Initialize olaylarını sayar.

Kullanım:
    python -m collar_watch.counter                    # 24 saat
    DURATION_HOURS=0.05 python -m collar_watch.counter  # 3 dakikalık test
    POOL_MANAGER=0x... python -m collar_watch.counter   # belirli kontrat
    POOL_MANAGER= python -m collar_watch.counter        # keşif modu (tüm kontratlar)
"""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import websockets
from eth_utils import keccak

# --- Config ---
WSS = os.getenv("ROBINHOOD_WSS", "wss://feed.mainnet.chain.robinhood.com")
POOL_MANAGER = os.getenv("POOL_MANAGER", "").strip().lower()
DURATION_HOURS = float(os.getenv("DURATION_HOURS", "24"))
LOG_FILE = os.getenv("COUNTER_LOG", "counter.log")
SUMMARY_FILE = os.getenv("COUNTER_SUMMARY", "counter-summary.json")

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


async def subscribe(ws):
    params = {"topics": [INITIALIZE_TOPIC]}
    if POOL_MANAGER:
        params["address"] = POOL_MANAGER
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
        "params": ["logs", params],
    }
    await ws.send(json.dumps(payload))
    resp = json.loads(await ws.recv())
    if "error" in resp:
        raise RuntimeError(f"Subscribe failed: {resp['error']}")
    return resp["result"]


async def run():
    start = time.time()
    end = start + DURATION_HOURS * 3600
    total = 0
    by_address = defaultdict(int)
    recent_blocks = []
    last_heartbeat = start
    mode = "belirli kontrat" if POOL_MANAGER else "KEŞİF (tüm kontratlar)"

    log.info("=" * 60)
    log.info("Aşama 0 — havuz sıklığı ölçümü")
    log.info("Mod:        %s", mode)
    log.info("Kontrat:    %s", POOL_MANAGER or "(filtre yok)")
    log.info("WSS:        %s", WSS)
    log.info("Topic:      %s", INITIALIZE_TOPIC)
    log.info("Süre:       %.2f saat", DURATION_HOURS)
    log.info("=" * 60)

    while time.time() < end:
        try:
            async with websockets.connect(
                WSS, ping_interval=20, ping_timeout=20, close_timeout=10
            ) as ws:
                sub_id = await subscribe(ws)
                log.info("Abone olundu: %s", sub_id)
                async for raw in ws:
                    if time.time() >= end:
                        break
                    msg = json.loads(raw)
                    if msg.get("method") != "eth_subscription":
                        continue
                    if msg["params"]["subscription"] != sub_id:
                        continue

                    ev = msg["params"]["result"]
                    total += 1
                    addr = ev["address"].lower()
                    by_address[addr] += 1
                    block = int(ev["blockNumber"], 16)
                    recent_blocks.append(block)
                    recent_blocks = recent_blocks[-50:]

                    log.info(
                        "EVENT #%d | contract=%s | block=%d | tx=%s",
                        total, addr[:12], block, ev["transactionHash"][:12],
                    )

                    if time.time() - last_heartbeat > 3600:
                        elapsed_h = (time.time() - start) / 3600
                        rate = total / elapsed_h if elapsed_h > 0 else 0
                        log.info(
                            "--- HEARTBEAT | %.1fh geçti | total=%d | %.1f/saat | %.1f/gün ---",
                            elapsed_h, total, rate, rate * 24,
                        )
                        last_heartbeat = time.time()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Bağlantı koptu: %s — 5 sn sonra tekrar", e)
            await asyncio.sleep(5)

    # --- Özet ---
    elapsed_h = (time.time() - start) / 3600
    summary = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "duration_hours": round(elapsed_h, 2),
        "mode": mode,
        "contract_filter": POOL_MANAGER or None,
        "total_events": total,
        "events_per_hour": round(total / elapsed_h, 2) if elapsed_h else 0,
        "events_per_day": round(total / elapsed_h * 24, 1) if elapsed_h else 0,
        "by_contract": dict(by_address),
        "block_range": {
            "min": min(recent_blocks) if recent_blocks else None,
            "max": max(recent_blocks) if recent_blocks else None,
        },
        "wss": WSS,
        "topic": INITIALIZE_TOPIC,
    }
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)

    log.info("=" * 60)
    log.info("ÖLÇÜM BİTTİ")
    log.info(json.dumps(summary, indent=2))
    log.info("=" * 60)
    log.info("Sonuç: %s", SUMMARY_FILE)


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Kullanıcı durdurdu")


if __name__ == "__main__":
    main()
