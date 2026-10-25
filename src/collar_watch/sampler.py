"""events.jsonl'dan yeni token havuzlarını seçer, quote filtreler, örnekler."""
import json
import os
import random
import sys
from collections import Counter
from pathlib import Path

from .constants import is_quote, hook_name

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
SAMPLE_RATE = float(os.getenv("SAMPLE_RATE", "0.01"))
SEED = int(os.getenv("SEED", "42"))
SAMPLE_OUT = Path(os.getenv("SAMPLE_OUT", "sampled-pools.jsonl"))


def _load_events(path: Path):
    if not path.exists():
        print(f"HATA: {path} bulunamadı.", file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        for line in f:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def pick_new_token(rec: dict) -> str | None:
    c0, c1 = rec["currency0"].lower(), rec["currency1"].lower()
    c0_q, c1_q = is_quote(c0), is_quote(c1)
    if c0_q and c1_q:
        return None
    if c0_q:
        return c1
    if c1_q:
        return c0
    return c0


def main():
    events_path = DATA_DIR / "events.jsonl"
    print(f"Okunuyor: {events_path}")

    new_pools = []
    skipped = 0
    for rec in _load_events(events_path):
        token = pick_new_token(rec)
        if token is None:
            skipped += 1
            continue
        quote = rec["currency0"] if rec["currency1"].lower() == token else rec["currency1"]
        new_pools.append({
            "pool_id": rec["pool_id"],
            "token": token,
            "quote": quote,
            "hook": hook_name(rec["hooks"]),
            "hook_addr": rec["hooks"],
            "fee": rec["fee"],
            "dynamic_fee": rec["dynamic_fee"],
            "block": rec["block"],
            "ts": rec["ts"],
            "tx": rec["tx"],
        })

    print(f"Toplam havuz: {len(new_pools) + skipped}")
    print(f"Yeni token havuzu: {len(new_pools)}")
    print(f"Atlanan (iki quote): {skipped}")

    if not new_pools:
        print("HATA: yeni token yok", file=sys.stderr)
        sys.exit(1)

    rng = random.Random(SEED)
    n = max(1, int(round(len(new_pools) * SAMPLE_RATE)))
    sample = rng.sample(new_pools, n)
    sample.sort(key=lambda r: r["ts"])

    with open(SAMPLE_OUT, "w") as f:
        for rec in sample:
            f.write(json.dumps(rec) + "\n")

    print(f"\nÖrneklem: {n} havuz ({SAMPLE_RATE:.2%}, seed={SEED})")
    print(f"Yazıldı: {SAMPLE_OUT}")

    hooks = Counter(p["hook"] for p in new_pools)
    print("\nHook dağılımı:")
    for h, c in hooks.most_common():
        print(f"  {h:12s}: {c}")


if __name__ == "__main__":
    main()
