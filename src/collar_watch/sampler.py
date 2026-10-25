"""events.jsonl'dan yeni token havuzlarını seçer, quote filtreler, örnekler.

Akış:
  1. data/events.jsonl oku (counter.py üretir)
  2. Her havuzda quote olmayan tarafı "yeni token" say
  3. İki taraf da quote ise atla (USDG/USDG, WETH/USDG, ...)
  4. SAMPLE_RATE ile deterministik örneklem seç (SEED)
  5. sampled-pools.jsonl yaz

Örneklem çıktısındaki her satır:
  {pool_id, token, quote, hook, hook_addr, fee, dynamic_fee, block, ts, tx}

Kullanım:
    python -m collar_watch.sampler
    SAMPLE_RATE=0.015 python -m collar_watch.sampler
    SEED=42 SAMPLE_RATE=0.015 python -m collar_watch.sampler

Ortam:
    DATA_DIR (data), SAMPLE_RATE (0.01), SEED (42), SAMPLE_OUT (sampled-pools.jsonl)
"""
import json
import os
import random
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .constants import is_quote, hook_name

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
SAMPLE_RATE = float(os.getenv("SAMPLE_RATE", "0.01"))
SEED = int(os.getenv("SEED", "42"))
SAMPLE_OUT = Path(os.getenv("SAMPLE_OUT", "sampled-pools.jsonl"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "5000"))


def _utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def _load_events(path: Path):
    if not path.exists():
        print(f"HATA: {path} bulunamadı.", file=sys.stderr)
        print("Önce 'python -m collar_watch.counter' çalıştır.", file=sys.stderr)
        sys.exit(1)
    size = path.stat().st_size
    if size == 0:
        print(f"HATA: {path} boş (0 byte). Backfill tamamlanmamış olabilir.", file=sys.stderr)
        sys.exit(1)
    print(f"Okunuyor: {path} ({size:,} byte)")
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def pick_new_token(rec: dict) -> tuple[str, str] | None:
    """Havuzdaki quote olmayan tarafı döndürür. (token, quote) çifti.

    İki taraf da quote ise None döner.
    İkisi de quote değilse sol tarafı "yeni" sayar (nadir durum).
    """
    c0, c1 = rec["currency0"].lower(), rec["currency1"].lower()
    c0_q, c1_q = is_quote(c0), is_quote(c1)
    if c0_q and c1_q:
        return None
    if c0_q:
        return c1, c0
    if c1_q:
        return c0, c1
    return c0, c1  # ikisi de quote değil — c0'ı "yeni" say, c1'i quote


def main():
    events_path = DATA_DIR / "events.jsonl"
    new_pools: list[dict] = []
    skipped_both_quote = 0
    skipped_bad = 0
    total = 0
    ts_min: int | None = None
    ts_max: int | None = None

    for i, rec in enumerate(_load_events(events_path), start=1):
        total = i
        # Zorunlu alanları kontrol et
        if not all(k in rec for k in ("pool_id", "currency0", "currency1", "hooks", "fee", "ts", "block", "tx")):
            skipped_bad += 1
            continue

        picked = pick_new_token(rec)
        if picked is None:
            skipped_both_quote += 1
            continue
        token, quote = picked

        ts = int(rec["ts"])
        if ts_min is None or ts < ts_min:
            ts_min = ts
        if ts_max is None or ts > ts_max:
            ts_max = ts

        new_pools.append({
            "pool_id": rec["pool_id"],
            "token": token,
            "quote": quote,
            "hook": hook_name(rec["hooks"]),
            "hook_addr": rec["hooks"],
            "fee": int(rec["fee"]),
            "dynamic_fee": bool(rec.get("dynamic_fee", False)),
            "block": int(rec["block"]),
            "ts": ts,
            "tx": rec["tx"],
        })

        if i % PROGRESS_EVERY == 0:
            print(f"  ... {i:,} event okundu | yeni token: {len(new_pools):,} | iki-quote: {skipped_both_quote:,}")

    # --- Özet ---
    print()
    print(f"Toplam event:         {total:,}")
    print(f"Yeni token havuzu:    {len(new_pools):,}")
    print(f"Atlanan (iki quote):  {skipped_both_quote:,}")
    print(f"Atlanan (bozuk):      {skipped_bad:,}")

    if ts_min and ts_max:
        hours = (ts_max - ts_min) / 3600
        print(f"Zaman aralığı:        {_utc(ts_min)} → {_utc(ts_max)} ({hours:.1f} saat)")

    if not new_pools:
        print("\nHATA: yeni token havuzu yok.", file=sys.stderr)
        print("Muhtemel sebepler: quote asset listesi çok geniş, veya events.jsonl bozuk.",
              file=sys.stderr)
        sys.exit(1)

    # Quote dağılımı
    quote_counts = Counter(p["quote"] for p in new_pools)
    print("\nQuote dağılımı:")
    for q, c in quote_counts.most_common(10):
        sym = q[:10]
        pct = 100 * c / len(new_pools)
        print(f"  {sym}...: {c:,} havuz ({pct:.1f}%)")

    # Hook dağılımı
    hook_counts = Counter(p["hook"] for p in new_pools)
    print("\nHook dağılımı:")
    for h, c in hook_counts.most_common(10):
        pct = 100 * c / len(new_pools)
        print(f"  {h:14s}: {c:,} havuz ({pct:.1f}%)")

    # Deterministik örneklem
    rng = random.Random(SEED)
    n = max(1, int(round(len(new_pools) * SAMPLE_RATE)))
    n = min(n, len(new_pools))
    sample = rng.sample(new_pools, n)
    sample.sort(key=lambda r: r["ts"])

    with open(SAMPLE_OUT, "w") as f:
        for rec in sample:
            f.write(json.dumps(rec) + "\n")

    print()
    print(f"Örneklem: {n:,} havuz ({SAMPLE_RATE:.2%} × {len(new_pools):,}, seed={SEED})")
    print(f"Yazıldı:  {SAMPLE_OUT}")

    # İlk örneklem kaydını göster (validation)
    if sample:
        print("\nİlk örneklem kaydı (format kontrolü):")
        first = sample[0]
        print(f"  token   = {first['token']}")
        print(f"  quote   = {first['quote'][:10]}...")
        print(f"  hook    = {first['hook']}")
        print(f"  fee     = {first['fee']}")
        print(f"  block   = {first['block']:,}")
        print(f"  ts      = {_utc(first['ts'])}")
        print(f"  pool_id = {first['pool_id'][:18]}...")


if __name__ == "__main__":
    main()
