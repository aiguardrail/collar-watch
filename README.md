# collar-watch

Observer bot that watches new Uniswap V4 pools on Robinhood Chain and evaluates them with Collar Guardrail.

**Current stage:** Aşama 0 — measuring pool frequency. No trades, no UI. Just counting.

## How it works

1. Connects to Robinhood Chain (WSS first, HTTP polling fallback)
2. Listens for `Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)` events on Uniswap V4 PoolManager
3. Logs each event (contract, block, tx) and writes a summary at the end

## Run via GitHub Actions

1. **Actions** tab → **Counter Test (3 min)** → **Run workflow**
2. Wait ~3 minutes
3. Download the **counter-result** artifact

## Local run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env
DURATION_HOURS=0.05 POOL_MANAGER= python -m collar_watch.counter
