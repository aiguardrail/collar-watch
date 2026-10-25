# collar-watch

Observer bot that watches new Uniswap V4 pools on Robinhood Chain and evaluates them with Collar Guardrail.

**Current stage:** Aşama 0 → C. Ölçüm + örneklem hazır. Collar entegrasyonu sonraki adım.

## Ne yapıyor

1. **`counter.py`** — Robinhood Chain'de Uniswap V4 `Initialize` (yeni havuz) ve `ModifyLiquidity` (likidite) olaylarını `eth_getLogs` ile okur. WSS yok (zincir `eth_subscribe` desteklemiyor). Kalıcı: `events.jsonl` + `liquidity.jsonl` + `checkpoint.json`. Çökerse kaldığı yerden devam eder. `BACKFILL_HOURS` ile geçmişi saniyeler içinde tarar.
2. **`sampler.py`** — `events.jsonl`'ı okur, quote asset'leri (USDG/WETH/…) filtreler, `SAMPLE_RATE` ile deterministik örneklem yapar, `sampled-pools.jsonl` üretir. **Collar'a çağrı yapmaz.**
3. **(sonraki adım)** — `collar.py` — `sampled-pools.jsonl`'daki havuzlara T+0 ve T+3dk'da `check_token_safety` çağrısı. Sonuç `evaluations.jsonl`'a.

## Repo yapısı
