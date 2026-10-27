# collar-watch 🔍🛡️

**Public scorecard & on-chain forensics for new Uniswap V4 pools on Robinhood Chain.**

> *"How many ALLOWs rugged? How many DENYs were junk? With base rates, not vibes."*

## 🚀 Current Stage: Autonomous Forensics (v0.4)
Sistem tamamen otonomdur. Her gün kendi kendine çalışır, yeni havuzları bulur, Collar Guardrail'e danışır, 24 saat sonra zincire dönüp tokenların kaderini (Rug mı, Pump & Dump mı, Güvenilir mi) bağımsız olarak ölçer ve sonuçları web sitesine yayınlar.

## 🛠️ Ne yapıyor? (The Pipeline)

1. **`counter.py`** — Robinhood Chain'de son 48 saat içindeki Uniswap V4 `Initialize` olaylarını (yeni havuzları) `eth_getLogs` ile tarar. Çökerse kaldığı yerden devam eder.
2. **`sampler.py`** — USDG (Robinhood Chain ana paritesi) ve diğer quote asset'leri filtreler. Deterministik bir örneklem (ör. %3) seçer. Aynı token birden fazla havuzdaysa **tekilleştirir (dedupe)**.
3. **`collar.py`** — Seçilen tokenları Collar Guardrail MCP (`check_token_safety`) üzerinden değerlendirir. 
   *   **API Dostu:** Dakikada 8 istek (Rate Limit) ve sıralı geçiş (Concurrency: 1) kullanır.
   *   **Dirençli:** HTTP 429 (Rate Limit) veya 500 hatalarında pes etmez, "Exponential Backoff" ile bekleyip tekrar dener. JSON içindeki gizli 429'ları bile yakalar.
4. **`outcomes.py` (The Detective)** — Collar'dan tamamen bağımsız olarak, 24 saat sonraki on-chain durumu analiz eder:
   *   **LP Pull:** Likidite tepe noktasından %90+ düştü mü?
   *   **Price Crash:** Fiyat tepe noktasından %95+ düştü mü? (Pons gibi kilitli likidite platformlarındaki Pump & Dump'ları yakalar).
   *   **Honeypot:** İşlem var ama hiç satış yok mu? (Bal kapanı).
   *   **Bot/Sybil:** Aynı blokta 5+ cüzdandan senkronize satış var mı? İşlemler tamamen aynı ham miktarlarla mı yapılıyor?
   *   **Flipping:** Alanların %50'si anında satıp fiyatı mı manipüle ediyor?
   *   **Trusted:** Hiçbir anomali yoksa ve fiyat çökmediyse `is_trusted: true` damgası basar.
5. **`publish.py`** — Tüm bu analizleri UI'ın (HTML) okuyabileceği `scorecard.json`, `recent.json` ve `outcomes.json` formatlarına dönüştürür.

## ⚙️ Otomasyon (GitHub Actions)
Sistem iki ana workflow ile çalışır:
*   **`collar-eval.yml`**: Her gün New York saatiyle 03:00'te (cron) çalışır. Geçmiş verileri `live-data` branch'inden çeker (24 saatlik analiz için), yeni tarama yapar ve sonuçları tekrar `live-data`'ya pushlar.
*   **`counter-backfill.yml`**: Manuel tetiklenir. İstediğinde 7 günlük (168 saat) devasa bir geçmiş tarama yapıp ham veriyi (artifact olarak) indirmeni sağlar.

## 📂 Repo Yapısı
```text
.
├── .github/
│   ├── scripts/databranch.sh      # live-data branch'ini izole yönetir
│   └── workflows/
│       ├── collar-eval.yml        # Günlük otonom pipeline (Cron)
│       └── counter-backfill.yml   # Manuel derin geçmiş tarama
├── src/collar_watch/
│   ├── collar.py                  # Collar API entegrasyonu (Rate-limit korumalı)
│   ├── counter.py                 # Blok tarayıcı (Backfill)
│   ├── outcomes.py                # 24h Rug & Anomali Dedektörü
│   ├── publish.py                 # UI JSON üretici
│   └── sampler.py                 # Örneklem ve tekilleştirme
├── pyproject.toml
└── README.md