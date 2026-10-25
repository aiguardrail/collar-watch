"""events.jsonl'dan yeni token havuzlarını seçer, quote filtreler, örnekler."""
import json
import os
import random
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .constants import QUOTE_ASSETS, ZERO_ADDR, is_quote, hook_name

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
SAMPLE_RATE = float(os.getenv("SAMPLE_RATE", "0.03"))
SEED = int(os.getenv("SEED", "42"))
SAMPLE_OUT = Path(os.getenv("SAMPLE_OUT", "sampled-pools.jsonl"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "5000"))


def _utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def _load_events(path: Path):
    if not path.exists():
        print(f"HATA: {path} bulunamadı.", file=sys.stderr)
        sys.exit(1)
    if path.stat().st_size == 0:
        print(f"HATA: {path} boş.", file=sys.stderr)
        sys.exit(1)
    print(f"Okunuyor: {path}")
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
    """Quote olmayan tarafı döndürür. (token, quote) veya None."""
    c0, c1 = rec["currency0"].lower(), rec["currency1"].lower()

    # Native ETH (ZERO) pair'lerini atla — hangi tarafın "yeni" olduğu belirsiz
    if c0 == ZERO_ADDR or c1 == ZERO_ADDR:
        return None

    c0_q, c1_q = is_quote(c0), is_quote(c1)
    if c0_q and c1_q:
        return None  # iki quote
    if c0_q:
        return c1, c0  # c0 quote, c1 yeni
    if c1_q:
        return c0, c1  # c1 quote, c0 yeni
    return c0, c1  # ikisi de quote değil, ZERO de değil


def main():
    events_path = DATA_DIR / "events.jsonl"
    new_pools = []
    skipped_zero = 0
    skipped_both_quote = 0
    skipped_bad = 0
    total = 0
    ts_min = ts_max = None

    for i, rec in enumerate(_load_events(events_path), start=1):
        total = i
        if not all(k in rec for k in ("pool_id", "currency0", "currency1",
                                       "hooks", "fee", "ts", "block", "tx")):
            skipped_bad += 1
            continue

        # ZERO kontrolü ayrı sayaç
        if rec["currency0"].lower() == ZERO_ADDR or rec["currency1"].lower() == ZERO_ADDR:
            skipped_zero += 1
            continue

        picked = pick_new_token(rec)
        if picked is None:
            skipped_both_quote += 1
            continue

        token, quote = picked
        ts = int(rec["ts"])
        ts_min = ts if ts_min is None or ts < ts_min else ts_min
        ts_max = ts if ts_max is None or ts > ts_max else ts_max

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
            print(f"  ... {i:,} okundu | yeni: {len(new_pools):,} | "
                  f"ZERO: {skipped_zero:,} | iki-quote: {skipped_both_quote:,}")

    print(f"\nToplam event:          {total:,}")
    print(f"Yeni token havuzu:     {len(new_pools):,}")
    print(f"Atlanan (native ETH):  {skipped_zero:,}")
    print(f"Atlanan (iki quote):   {skipped_both_quote:,}")
    print(f"Atlanan (bozuk):       {skipped_bad:,}")
    if ts_min and ts_max:
        print(f"Zaman aralığı:         {_utc(ts_min)} → {_utc(ts_max)} "
              f"({(ts_max-ts_min)/3600:.1f} saat)")

    if not new_pools:
        print("\nHATA: yeni token havuzu yok.", file=sys.stderr)
        sys.exit(1)

    # Quote dağılımı
    print("\nQuote dağılımı:")
    for q, c in Counter(p["quote"] for p in new_pools).most_common(10):
        sym = QUOTE_ASSETS.get(q, q[:10] + "...")
        print(f"  {sym:20s}: {c:,} havuz ({100*c/len(new_pools):.1f}%)")

    # Hook dağılımı
    print("\nHook dağılımı:")
    for h, c in Counter(p["hook"] for p in new_pools).most_common(10):
        print(f"  {h:15s}: {c:,} havuz ({100*c/len(new_pools):.1f}%)")

    rng = random.Random(SEED)
    n = max(1, min(int(round(len(new_pools) * SAMPLE_RATE)), len(new_pools)))
    sample = rng.sample(new_pools, n)
    sample.sort(key=lambda r: r["ts"])

    with open(SAMPLE_OUT, "w") as f:
        for rec in sample:
            f.write(json.dumps(rec) + "\n")

    print(f"\nÖrneklem: {n:,} havuz ({SAMPLE_RATE:.2%} × {len(new_pools):,}, seed={SEED})")
    print(f"Yazıldı: {SAMPLE_OUT}")

    if sample:
        first = sample[0]
        print(f"\nİlk örneklem:")
        print(f"  token   = {first['token']}")
        print(f"  quote   = {first['quote']}")
        print(f"  hook    = {first['hook']}")
        print(f"  fee     = {first['fee']}")
        print(f"  ts      = {_utc(first['ts'])}")


if __name__ == "__main__":
    main()
