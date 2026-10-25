# collar-watch

Observer bot that watches new Uniswap V4 pools on Robinhood Chain and evaluates them with Collar Guardrail.

**Current stage:** Aşama 0 — measuring pool frequency for 24–48h. No trades, no user-facing UI. Just counting.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env   # fill in POOL_MANAGER after first discovery run
