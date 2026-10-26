"""evaluations.jsonl → UI için iki JSON dosyası.

Çıktı:
  scorecard.json  → özet istatistikler (UI ana sayfa)
  recent.json     → son 100 değerlendirme (UI liste)

Sanity gate: eksik/bozuk veri varsa fail eder, R2'ye yüklenmez.

Kullanım:
    python -m collar_watch.publish

Ortam değişkenleri:
    EVAL_IN         (default: evaluations.jsonl)
    SCORECARD_OUT   (default: scorecard.json)
    RECENT_OUT      (default: recent.json)
    SAMPLE_POOLS    (default: sampled-pools.jsonl)
    OUTCOMES_IN     (default: outcomes.jsonl)  — yoksa outcomes bloğu atlanır
    RECENT_LIMIT    (default: 100)
    TOP_RISK_FACTORS(default: 10)
    RULES_VERSION   (default: v0)
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
SAMPLE_POOLS = Path(os.getenv("SAMPLED_POOLS", "sampled-pools.jsonl"))
OUTCOMES_IN = Path(os.getenv("OUTCOMES_IN", "outcomes.jsonl"))  # optional
RECENT_LIMIT = int(os.getenv("RECENT_LIMIT", "100"))
TOP_RISK_FACTORS = int(os.getenv("TOP_RISK_FACTORS", "10"))
SCHEMA_VERSION = 1
RULES_VERSION = os.getenv("RULES_VERSION", "v0")

DECISIONS = ("ALLOW", "WARN", "DENY", "ERROR", "UNKNOWN")

# DENY kategorileri
CATEGORY_RULE = "rule_based"      # "asset not in registry", "unsupported chain" vb.
CATEGORY_RISK = "risk_flag"       # honeypot, high tax, LP unlocked, mint open
CATEGORY_OTHER = "other"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def wilson_ci(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """%95 Wilson güven aralığı. n=0 ise (0,1) döner."""
    if n == 0:
        return (0.0, 1.0)
    p = successes / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def categorize_deny(reason: str | None, risk: int | None) -> str:
    """DENY kararının kaynağını ayır: kural tabanlı mı, gerçek risk mi?

    Önemli: Eğer DENY'ların çoğu 'kayıtlı varlık listesi dışı' gibi bir kuraldan
    geliyorsa, bunları 'risk tespiti' gibi sunmak yanıltıcı olur.
    """
    if not reason:
        return CATEGORY_RISK if risk is not None and risk >= 50 else CATEGORY_OTHER

    r = reason.lower()

    rule_keywords = [
        "not in registry", "unsupported chain", "unknown token",
        "not supported", "allowlist", "not allowed", "whitelist",
        "registry", "not found",
    ]
    if any(k in r for k in rule_keywords):
        return CATEGORY_RULE

    risk_keywords = [
        "honeypot", "high tax", "high_transfer_fee", "lp unlocked",
        "mint open", "mint authority", "freeze", "severe", "danger",
        "ownership", "proxy", "blacklist", "rug",
    ]
    if any(k in r for k in risk_keywords):
        return CATEGORY_RISK

    return CATEGORY_OTHER


def _t3m_decision(eval_rec: dict) -> str | None:
    """t3m kararını döndür; eksik/ERROR ise None.

    Sanity gate %10'a kadar eksik t3m'e izin verdiği için, downstream kod
    t3m'i 'olabilir de olmayabilir de' gibi ele almalı.
    """
    t3m = eval_rec.get("t3m")
    if not t3m:
        return None
    d = t3m.get("decision")
    return d if d not in (None, "ERROR") else None


def _split_factors(reason_str: str | None) -> list[str]:
    """'honeypot, high_tax' → ['honeypot', 'high_tax']."""
    if not reason_str:
        return []
    return [p.strip() for p in reason_str.replace("|", ",").split(",") if p.strip()]


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


def load_outcomes(path: Path) -> dict[str, dict] | None:
    """Optional outcomes.jsonl: {token: {rug_24h: bool, ...}}.

    Dosya yoksa None döner → scorecard.json'da outcomes bloğu yazılmaz →
    HTML 'outcome data pending' mesajı gösterir.
    """
    if not path.exists():
        return None
    by_token: dict[str, dict] = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            token = rec.get("token")
            if token and "rug_24h" in rec:
                by_token[token] = rec
    return by_token or None


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

    if total_sampled and n < total_sampled * 0.7:
        raise SystemExit(
            f"SANITY FAIL: {n}/{total_sampled} tamamlandı (%{100*n/total_sampled:.0f}), "
            f"%70 altı. Publish iptal."
        )

    missing_t3m = sum(1 for e in evals if _t3m_decision(e) is None)
    if n > 0 and missing_t3m / n > 0.10:
        raise SystemExit(
            f"SANITY FAIL: {missing_t3m}/{n} t3m eksik/hata "
            f"(%{100*missing_t3m/n:.0f}), %10 üstü. Publish iptal."
        )

    allow_t0 = sum(1 for e in evals if e.get("t0", {}).get("decision") == "ALLOW")
    if allow_t0 < 5:
        raise SystemExit(
            f"SANITY FAIL: sadece {allow_t0} ALLOW T+0, flip metriği anlamsız. Publish iptal."
        )

    print(f"✓ Sanity gate geçildi: n={n}, t3m eksik={missing_t3m}, ALLOW@T0={allow_t0}")


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def build_hook_distribution(evals: list[dict]) -> list[dict]:
    """Pools grouped by V4 hook. Hook alanı olmayanlar 'unknown' altında."""
    counter: Counter[str] = Counter()
    for e in evals:
        hook = e.get("hook")
        counter[hook if hook else "unknown"] += 1
    return [{"hook": h, "n": c} for h, c in counter.most_common()]


def build_risk_factors(evals: list[dict], top_n: int = 10) -> list[dict]:
    """En sık görülen risk faktörleri (kural tabanlı olanlar hariç).

    HTML: 'Soft flags (for example, ownership not renounced) appear here but
    do not change an ALLOW verdict.'
    """
    counter: Counter[str] = Counter()
    for e in evals:
        t0 = e.get("t0") or {}
        for factor in _split_factors(t0.get("reason")):
            # Kural tabanlı olanları ('not in registry' vb.) risk faktörü sayma
            if categorize_deny(factor, None) == CATEGORY_RULE:
                continue
            counter[factor] += 1
    return [{"factor": f, "n": c} for f, c in counter.most_common(top_n)]


def build_outcomes(evals: list[dict], outcomes_by_token: dict[str, dict] | None) -> dict | None:
    """evaluations × outcomes join. Veri yoksa None → HTML pending mesajı gösterir.

    Sadece 24h rug verisi olan pool'lar sayılır. Rate = rugs / n (0–1).
    """
    if not outcomes_by_token:
        return None

    by_verdict: dict[str, dict[str, int]] = {d: {"n": 0, "rugs": 0} for d in DECISIONS}
    measured = 0

    for e in evals:
        token = e.get("token")
        outcome = outcomes_by_token.get(token) if token else None
        if not outcome:
            continue
        verdict = (e.get("t0") or {}).get("decision")
        if verdict not in by_verdict:
            continue
        by_verdict[verdict]["n"] += 1
        if outcome.get("rug_24h") is True:
            by_verdict[verdict]["rugs"] += 1
        measured += 1

    if measured == 0:
        return None

    out_by_verdict: dict[str, dict] = {}
    for d in DECISIONS:
        v = by_verdict[d]
        if v["n"] > 0:
            out_by_verdict[d] = {
                "n": v["n"],
                "rugs": v["rugs"],
                "rate": round(v["rugs"] / v["n"], 4),
            }

    return {
        "measured": measured,
        "window": "24h",
        "method": "liquidity_pulled_within_24h",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "by_verdict": out_by_verdict,
    }


def build_scorecard(
    evals: list[dict],
    total_sampled: int,
    outcomes_by_token: dict[str, dict] | None = None,
) -> dict:
    n = len(evals)
    if n == 0:
        return {"schema": SCHEMA_VERSION, "error": "no_data"}

    t0_counts = Counter(
        (e.get("t0") or {}).get("decision", "UNKNOWN") for e in evals
    )

    # t3m eksik olanları sessizce dışla (sanity gate zaten %10 üstünü engelliyor)
    t3m_counts: Counter[str] = Counter()
    transitions: Counter[tuple[str, str]] = Counter()
    for e in evals:
        t3m_d = _t3m_decision(e)
        if t3m_d is None:
            continue
        t0_d = (e.get("t0") or {}).get("decision", "UNKNOWN")
        t3m_counts[t3m_d] += 1
        transitions[(t0_d, t3m_d)] += 1

    flip_out = sum(c for (a, b), c in transitions.items() if a == "ALLOW" and b != "ALLOW")
    flip_in = sum(c for (a, b), c in transitions.items() if a != "ALLOW" and b == "ALLOW")
    allow_t0 = t0_counts.get("ALLOW", 0)
    allow_t3m = t3m_counts.get("ALLOW", 0)

    if allow_t0 > 0:
        ci_lo, ci_hi = wilson_ci(flip_out, allow_t0)
        flip_rate = flip_out / allow_t0
    else:
        ci_lo = ci_hi = flip_rate = None

    n_complete = sum(1 for e in evals if _t3m_decision(e) is not None)

    # DENY breakdown — kural tabanlı vs gerçek risk
    deny_breakdown = {CATEGORY_RULE: 0, CATEGORY_RISK: 0, CATEGORY_OTHER: 0}
    for e in evals:
        t0 = e.get("t0") or {}
        if t0.get("decision") == "DENY":
            cat = categorize_deny(t0.get("reason"), t0.get("risk"))
            deny_breakdown[cat] += 1

    # Toplam değişim (t3m'i olanlar arasında)
    total_changed = sum(
        1
        for e in evals
        if _t3m_decision(e) is not None
        and (e.get("t0") or {}).get("decision") != (e.get("t3m") or {}).get("decision")
    )

    # Hook & risk faktörleri — HTML bunları section olarak çiziyor
    hook_distribution = build_hook_distribution(evals)
    risk_factors = build_risk_factors(evals, TOP_RISK_FACTORS)

    # Outcomes — sadece veri varsa
    outcomes = build_outcomes(evals, outcomes_by_token)

    result = {
        "schema": SCHEMA_VERSION,
        "rules_version": RULES_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_hours": 48,
        "metric_type": "decision_stability",  # accuracy değil!
        "sample": {
            "evaluated": n,
            "n_complete": n_complete,
            "total_pools_in_window": total_sampled,
            "sample_rate": round(n / total_sampled, 4) if total_sampled else None,
        },
        "t0": {"counts": {d: t0_counts.get(d, 0) for d in DECISIONS}},
        "t3m": {"counts": {d: t3m_counts.get(d, 0) for d in DECISIONS}},
        "transitions": {f"{a}→{b}": c for (a, b), c in sorted(transitions.items())},
        "changes": {
            "total_changed": total_changed,
            "change_rate": round(total_changed / n, 4) if n else None,
        },
        "flip": {
            "allow_t0_count": allow_t0,
            "allow_t3m_count": allow_t3m,
            "flipped_out": flip_out,
            "flipped_in": flip_in,
            "flip_rate": round(flip_rate, 4) if flip_rate is not None else None,
            "flip_rate_ci95": [round(ci_lo, 4), round(ci_hi, 4)] if ci_lo is not None else None,
        },
        "deny_breakdown": {
            "total_deny": sum(deny_breakdown.values()),
            **deny_breakdown,
        },
        # --- HTML section'ları için yeni alanlar ---
        "hook_distribution": hook_distribution,
        "risk_factors": risk_factors,
        "notes": {
            "metric_type": "decision_stability",
            "accuracy_disclaimer": (
                "This measures T+0 vs T+3m decision consistency, not predictive accuracy. "
                "T+24h outcome data (did the token actually rug?) will be published separately."
            ),
            "conflict_disclosure": (
                "This bot does not hold or trade any token. Scoring is independent of "
                "token ownership. Collar Guardrail has no token."
            ),
        },
    }

    # outcomes varsa ekle, yoksa key hiç yazılmasın → HTML pending gösterir
    if outcomes is not None:
        result["outcomes"] = outcomes

    return result


def build_recent(evals: list[dict], limit: int = 100) -> dict:
    sorted_evals = sorted(evals, key=lambda e: e.get("ts") or 0, reverse=True)
    items = []
    for e in sorted_evals[:limit]:
        t0 = e.get("t0") or {}
        t3m = e.get("t3m") or {}
        ts = e.get("evaluated_at") or e.get("ts")

        item = {
            # Mevcut şema (back-compat)
            "token": e.get("token"),
            "symbol": e.get("symbol"),
            "name": e.get("name"),
            "quote": e.get("quote"),
            "hook": e.get("hook"),
            "fee": e.get("fee"),
            "block": e.get("block"),
            "t0": {
                "decision": t0.get("decision", "UNKNOWN"),
                "risk": t0.get("risk"),
                "reason": t0.get("reason"),
            },
            "t3m": (
                {
                    "decision": t3m.get("decision"),
                    "risk": t3m.get("risk"),
                    "reason": t3m.get("reason"),
                }
                if t3m
                else None
            ),
            "changed": e.get("changed", False),
            "evaluated_at": e.get("evaluated_at"),
            # --- scorecard.html renderRecent() için alias'lar ---
            "ts": ts,
            "verdict": t0.get("decision", "UNKNOWN"),
            "risk_score": t0.get("risk"),
        }
        items.append(item)

    return {
        "schema": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        # HTML bu key'i arıyor (evaluations || recent || array)
        "evaluations": items,
        # Eski tüketiciler için alias
        "items": items,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    evals = load_evals(EVAL_IN)
    total_sampled = count_sampled_pools(SAMPLE_POOLS)
    outcomes_by_token = load_outcomes(OUTCOMES_IN)

    sanity_gate(evals, total_sampled)

    scorecard = build_scorecard(evals, total_sampled, outcomes_by_token)
    recent = build_recent(evals, RECENT_LIMIT)

    SCORECARD_OUT.write_text(json.dumps(scorecard, indent=2))
    RECENT_OUT.write_text(json.dumps(recent, indent=2))

    print(f"Yazıldı: {SCORECARD_OUT} (schema={SCHEMA_VERSION})")
    print(f"Yazıldı: {RECENT_OUT} ({recent['count']} kayıt)")

    if outcomes_by_token is None:
        print("Not: outcomes.jsonl yok — outcomes bloğu atlandı (HTML 'pending' gösterir).")
    elif "outcomes" in scorecard:
        print(f"Outcomes: {scorecard['outcomes']['measured']} pool eşleşti.")

    if scorecard.get("error") != "no_data":
        t0 = scorecard["t0"]["counts"]
        t3m = scorecard["t3m"]["counts"]
        flip = scorecard["flip"]
        changes = scorecard["changes"]
        deny = scorecard["deny_breakdown"]
        print(f"\nT+0:     {t0}")
        print(f"T+3dk:   {t3m}")
        print(f"Changes: {changes['total_changed']}/{scorecard['sample']['evaluated']} "
              f"(%{100*changes['change_rate']:.1f})")
        if flip["flip_rate"] is not None:
            lo, hi = flip["flip_rate_ci95"]
            print(f"Flip:    {flip['flipped_out']}/{flip['allow_t0_count']} ALLOW → farklı "
                  f"({flip['flip_rate']:.1%}, %95 CI [{lo:.1%}, {hi:.1%}])")
        print(f"DENY:    rule={deny['rule_based']}, risk={deny['risk_flag']}, other={deny['other']}")
        print(f"Hooks:   {len(scorecard['hook_distribution'])} benzersiz")
        print(f"Factors: {len(scorecard['risk_factors'])} benzersiz")


if __name__ == "__main__":
    main()
