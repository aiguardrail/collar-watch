"""evaluations.jsonl → UI için iki JSON dosyası.

Çıktı:
  scorecard.json  → özet istatistikler (UI ana sayfa)
  recent.json     → son 100 değerlendirme (UI liste)

Sanity gate: eksik/bozuk veri varsa fail eder, R2'ye yüklenmez.

Kullanım:
    python -m collar_watch.publish
"""
import json
import math
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

EVAL_IN = Path(os.getenv("EVAL_IN", "evaluations.jsonl"))
SCORECARD_OUT = Path(os.getenv("SCORECARD_OUT", "scorecard.json"))
RECENT_OUT = Path(os.getenv("RECENT_OUT", "recent.json"))
SAMPLE_POOLS = Path(os.getenv("SAMPLED_POOLS", "data/sampled-pools.jsonl"))
RECENT_LIMIT = int(os.getenv("RECENT_LIMIT", "100"))
SCHEMA_VERSION = 1
RULES_VERSION = os.getenv("RULES_VERSION", "v0")

DECISIONS = ("ALLOW", "WARN", "DENY", "ERROR", "UNKNOWN")


def wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """%95 Wilson güven aralığı. n=0 ise (0,1) döner."""
    if n == 0:
        return (0.0, 1.0)
    p = successes / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def load_evals(path: Path) -> list[dict]:
    if not path.exists():
        print(f"HATA: {path} yok", file=sys.stderr)
        sys.exit(1)
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def count_sampled_pools(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path) as f:
        return sum(1 for line in f if line.strip())


def sanity_gate(evals: list[dict], total_sampled: int) -> None:
    """Bozuk veri iyi veriyi ezmesin. Fail → workflow durur → R2'deki eski dosya korunur."""
    n = len(evals)
    if n == 0:
        raise SystemExit("SANITY FAIL: 0 evaluation, publish iptal")

    # Örneklemin en az %70'i tamamlanmış olmalı
    if total_sampled and n < total_sampled * 0.7:
        raise SystemExit(
            f"SANITY FAIL: {n}/{total_sampled} tamamlandı (%{100*n/total_sampled:.0f}), "
            f"%70 altı. Publish iptal."
        )

    # t3m eksik/hata oranı %10'u geçmesin
    missing_t3m = sum(
        1 for e in evals
        if not e.get("t3m") or e["t3m"].get("decision") in (None, "ERROR")
    )
    if n > 0 and missing_t3m / n > 0.10:
        raise SystemExit(
            f"SANITY FAIL: {missing_t3m}/{n} t3m eksik/hata "
            f"(%{100*missing_t3m/n:.0f}), %10 üstü. Publish iptal."
        )

    # En az 5 ALLOW olmalı (flip metriği anlamlı olsun)
    allow_t0 = sum(
        1 for e in evals
        if e.get("t0", {}).get("decision") == "ALLOW"
    )
    if allow_t0 < 5:
        raise SystemExit(
            f"SANITY FAIL: sadece {allow_t0} ALLOW T+0, flip metriği anlamsız. Publish iptal."
        )

    print(f"✓ Sanity gate geçildi: n={n}, t3m eksik={missing_t3m}, ALLOW@T0={allow_t0}")


def build_scorecard(evals: list[dict], total_sampled: int) -> dict:
    n = len(evals)
    if n == 0:
        return {"schema": SCHEMA_VERSION, "error": "no_data"}

    t0_counts = Counter(e["t0"]["decision"] for e in evals)
    t3m_counts = Counter(e["t3m"]["decision"] for e in evals)

    transitions = Counter()
    for e in evals:
        transitions[(e["t0"]["decision"], e["t3m"]["decision"])] += 1

    flip_out = sum(c for (a, b), c in transitions.items() if a == "ALLOW" and b != "ALLOW")
    flip_in = sum(c for (a, b), c in transitions.items() if a != "ALLOW" and b == "ALLOW")
    allow_t0 = t0_counts.get("ALLOW", 0)
    allow_t3m = t3m_counts.get("ALLOW", 0)

    if allow_t0 > 0:
        ci_lo, ci_hi = wilson_ci(flip_out, allow_t0)
        flip_rate = flip_out / allow_t0
    else:
        ci_lo = ci_hi = flip_rate = None

    n_complete = sum(
        1 for e in evals
        if e.get("t3m") and e["t3m"].get("decision") not in (None, "ERROR")
    )

    return {
        "schema": SCHEMA_VERSION,
        "rules_version": RULES_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_hours": 48,
        "sample": {
            "evaluated": n,
            "n_complete": n_complete,
            "total_pools_in_window": total_sampled,
            "sample_rate": round(n / total_sampled, 4) if total_sampled else None,
        },
        "t0": {"counts": {d: t0_counts.get(d, 0) for d in DECISIONS}},
        "t3m": {"counts": {d: t3m_counts.get(d, 0) for d in DECISIONS}},
        "transitions": {
            f"{a}→{b}": c for (a, b), c in sorted(transitions.items())
        },
        "flip": {
            "allow_t0_count": allow_t0,
            "allow_t3m_count": allow_t3m,
            "flipped_out": flip_out,
            "flipped_in": flip_in,
            "flip_rate": round(flip_rate, 4) if flip_rate is not None else None,
            "flip_rate_ci95": [round(ci_lo, 4), round(ci_hi, 4)] if ci_lo is not None else None,
        },
    }


def build_recent(evals: list[dict], limit: int = 100) -> dict:
    sorted_evals = sorted(evals, key=lambda e: e.get("ts") or 0, reverse=True)
    items = []
    for e in sorted_evals[:limit]:
        items.append({
            "token": e["token"],
            "quote": e.get("quote"),
            "hook": e.get("hook"),
            "fee": e.get("fee"),
            "block": e.get("block"),
            "t0": {
                "decision": e["t0"]["decision"],
                "risk": e["t0"].get("risk"),
            },
            "t3m": {
                "decision": e["t3m"]["decision"],
                "risk": e["t3m"].get("risk"),
            },
            "changed": e.get("changed", False),
            "evaluated_at": e.get("evaluated_at"),
        })
    return {
        "schema": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items,
    }


def main():
    evals = load_evals(EVAL_IN)
    total_sampled = count_sampled_pools(SAMPLE_POOLS)

    sanity_gate(evals, total_sampled)

    scorecard = build_scorecard(evals, total_sampled)
    recent = build_recent(evals, RECENT_LIMIT)

    SCORECARD_OUT.write_text(json.dumps(scorecard, indent=2))
    RECENT_OUT.write_text(json.dumps(recent, indent=2))

    print(f"Yazıldı: {SCORECARD_OUT} (schema={SCHEMA_VERSION})")
    print(f"Yazıldı: {RECENT_OUT} ({recent['count']} kayıt)")

    if scorecard.get("error") != "no_data":
        t0 = scorecard["t0"]["counts"]
        t3m = scorecard["t3m"]["counts"]
        flip = scorecard["flip"]
        print(f"\nT+0:   {t0}")
        print(f"T+3dk: {t3m}")
        if flip["flip_rate"] is not None:
            lo, hi = flip["flip_rate_ci95"]
            print(f"Flip:  {flip['flipped_out']}/{flip['allow_t0_count']} ALLOW → farklı "
                  f"({flip['flip_rate']:.1%}, %95 CI [{lo:.1%}, {hi:.1%}])")


if __name__ == "__main__":
    main()
