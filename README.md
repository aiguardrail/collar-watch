# collar-watch 🔍🛡️

**Public scorecard & on-chain forensics for new Uniswap V4 pools on Robinhood Chain.**

> *"How many ALLOWs rugged? How many DENYs were junk? With base rates, not vibes."*

## 🚀 Current Stage: Autonomous Forensics (v0.4)
The system is fully autonomous. It runs daily, discovers new pools, consults Collar Guardrail, independently measures the tokens' fate 24 hours later (Rug, Pump & Dump, or Trusted), and publishes the results to the live website.

## 🛠️ How it works (The Pipeline)

1. **`counter.py`** — Scans Uniswap V4 `Initialize` events (new pools) on Robinhood Chain over the last 48 hours using `eth_getLogs`. It is resilient and resumes from the last checkpoint if interrupted.
2. **`sampler.py`** — Filters for USDG (the primary quote asset on Robinhood Chain) and other quote assets. Selects a deterministic sample (e.g., 3%). It **deduplicates** tokens if the same token appears in multiple pools.
3. **`collar.py`** — Evaluates the sampled tokens via the Collar Guardrail MCP (`check_token_safety`). 
   *   **API-Friendly:** Uses 8 requests per minute (Rate Limit) and sequential execution (Concurrency: 1).
   *   **Resilient:** Doesn't give up on HTTP 429 (Rate Limit) or 500 errors. Uses "Exponential Backoff" to wait and retry. It even catches hidden 429s embedded within the JSON response.
4. **`outcomes.py` (The Detective)** — Completely independent of Collar, it analyzes the on-chain state 24 hours later:
   *   **LP Pull:** Did liquidity drop by >90% from its peak?
   *   **Price Crash:** Did the price drop by >95% from its peak? (Catches Pump & Dumps even on locked-liquidity platforms like Pons).
   *   **Honeypot:** Are there transactions but absolutely zero sells? (Bal trap).
   *   **Bot/Sybil Activity:** Are there synchronized dumps from 5+ wallets in the same block? Are transactions using the exact same raw amounts?
   *   **Flipping Network:** Are >50% of buyers immediately selling to manipulate the price?
   *   **Trusted:** If there are no anomalies and the price hasn't crashed, it gets stamped as `is_trusted: true`.
5. **`publish.py`** — Transforms all this data into `scorecard.json`, `recent.json`, and `outcomes.json` formats for the UI (HTML) to consume.

## ⚙️ Automation (GitHub Actions)
The system runs on two main workflows:
*   **`collar-eval.yml`**: Runs daily at 03:00 AM New York time (cron). It fetches historical data from the `live-data` branch (for 24h outcome analysis), performs a new scan, and pushes the results back to `live-data`.
*   **`counter-backfill.yml`**: Manually triggered. Allows you to perform a massive 7-day (168 hours) historical scan and download the raw data as artifacts.

## 📂 Repo Structure
```text
.
├── .github/
│   ├── scripts/databranch.sh      # Isolated management of the live-data branch
│   └── workflows/
│       ├── collar-eval.yml        # Daily autonomous pipeline (Cron)
│       └── counter-backfill.yml   # Manual deep historical scan
├── src/collar_watch/
│   ├── collar.py                  # Collar API integration (Rate-limit protected)
│   ├── counter.py                 # Block scanner (Backfill)
│   ├── outcomes.py                # 24h Rug & Anomaly detector
│   ├── publish.py                 # UI JSON generator
│   └── sampler.py                 # Sampling and deduplication
├── pyproject.toml
└── README.md