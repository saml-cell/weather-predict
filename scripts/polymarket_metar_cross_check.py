#!/usr/bin/env python3
"""Cross-check: does our METAR daily-max match Polymarket's resolved winner bucket?

The paper tracker's `paper_pnl` is already computed from Polymarket's own
outcomePrices (which reflect whatever source Wunderground/NOAA/HKO provided).
So METAR re-resolution does NOT change the paper PnL number.

What it DOES tell us:
  1. Sanity: does our METAR fetch align with Polymarket's resolution?
     If yes → the pipeline is ready to retrain MOS on METAR / fix the backtest.
     If no → investigate (wrong ICAO? different aggregation? rounding?).
  2. Per-city diagnostic: same comparison, split by city + provider.

Emits a delta report to data/polymarket_metar_delta.json and stdout.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DB_PATH = ROOT / "data" / "weather.db"
MAP_PATH = ROOT / "data" / "polymarket_resolution_sources.json"
ERAS_PATH = ROOT / "data" / "polymarket_city_eras.json"
OUT_PATH = ROOT / "data" / "polymarket_metar_delta.json"


def daily_max_c(con: sqlite3.Connection, city: str, date: str) -> float | None:
    row = con.execute(
        """SELECT MAX(temp_c) FROM observations_hourly_metar
           WHERE city_name=? AND DATE(valid_time)=?""",
        (city, date),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def resolve_bucket(max_c: float, unit: str, buckets: list[dict]) -> str | None:
    """Given METAR max in °C and the market's unit, find the winning bucket_label.

    unit ∈ {'C', 'F', 'C01'} — for 'F' we convert to whole °F then back to the
    °C midpoint, which is what the market actually measures.
    """
    if unit == "F":
        max_f = round(max_c * 9 / 5 + 32)
        effective_c = (max_f - 32) * 5 / 9
    elif unit == "C01":
        effective_c = round(max_c, 1)
    else:  # 'C'
        effective_c = round(max_c)

    # Buckets stored with bucket_lo_c / bucket_hi_c in °C. The first/last buckets
    # are "≤X" / "≥X" with one side = None.
    for b in buckets:
        lo, hi = b["lo"], b["hi"]
        if lo is None and hi is not None:
            if effective_c <= hi:
                return b["label"]
        elif hi is None and lo is not None:
            if effective_c >= lo:
                return b["label"]
        elif lo is not None and hi is not None:
            if lo <= effective_c <= hi:
                return b["label"]
    return None


def main() -> int:
    mapping = json.loads(MAP_PATH.read_text())
    # Simplify to city → (icao, unit)
    units = {}
    for city, entry in mapping.items():
        if not entry:
            continue
        rounding = (entry.get("rounding") or "").lower()
        if "fahrenheit" in rounding:
            units[city] = "F"
        elif "one decimal" in rounding:
            units[city] = "C01"
        else:
            units[city] = "C"
    # Manual
    units.setdefault("New York", "F")

    eras = json.loads(ERAS_PATH.read_text()) if ERAS_PATH.exists() else {}
    era_of = dict(eras)  # city → era_label

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # All resolved scan rows (polymarket_pnl is not None)
    rows = con.execute(
        """SELECT id, city_name, target_date, event_slug, bucket_label,
                  bucket_lo_c, bucket_hi_c, would_bet_side, market_yes,
                  resolved_yes, paper_pnl
             FROM polymarket_scan
            WHERE paper_pnl IS NOT NULL""",
    ).fetchall()
    print(f"Loaded {len(rows)} bets with Polymarket-resolved pnl")

    # Group buckets by (city, target_date, event_slug)
    buckets_by_event = defaultdict(list)
    for r in rows:
        key = (r["city_name"], r["target_date"], r["event_slug"])
        buckets_by_event[key].append({
            "label": r["bucket_label"],
            "lo": r["bucket_lo_c"],
            "hi": r["bucket_hi_c"],
            "resolved_yes": r["resolved_yes"],
        })

    # For each event, find the Polymarket-resolved winner (resolved_yes=1 row)
    # and determine which bucket METAR resolves to.
    event_results = {}
    for key, bks in buckets_by_event.items():
        city, date, slug = key
        pm_winner = next((b["label"] for b in bks if b["resolved_yes"]), None)
        if city in {"Hong Kong"}:
            event_results[key] = {"metar_winner": None, "pm_winner": pm_winner,
                                  "metar_max_c": None, "status": "skip_hkobserv"}
            continue
        unit = units.get(city)
        if not unit:
            event_results[key] = {"metar_winner": None, "pm_winner": pm_winner,
                                  "metar_max_c": None, "status": "no_unit"}
            continue
        mx = daily_max_c(con, city, date)
        if mx is None:
            event_results[key] = {"metar_winner": None, "pm_winner": pm_winner,
                                  "metar_max_c": None, "status": "no_metar"}
            continue
        metar_winner = resolve_bucket(mx, unit, bks)
        status = "agree" if metar_winner == pm_winner else "disagree"
        event_results[key] = {
            "metar_winner": metar_winner, "pm_winner": pm_winner,
            "metar_max_c": round(mx, 2), "unit": unit, "status": status,
        }

    # Now compute: for each bet, would METAR-based winner have changed the
    # paper_pnl? (Hypothetical: if we'd used METAR instead of Polymarket's truth.)
    bet_deltas = []
    for r in rows:
        if r["would_bet_side"] is None:
            continue
        key = (r["city_name"], r["target_date"], r["event_slug"])
        er = event_results[key]
        pm_winner = er["pm_winner"]
        metar_winner = er["metar_winner"]
        pm_is_winner = 1 if pm_winner == r["bucket_label"] else 0
        metar_is_winner = 1 if metar_winner == r["bucket_label"] else 0
        # Current paper_pnl (already in DB) is based on pm_is_winner.
        mkt_yes = float(r["market_yes"])
        side = r["would_bet_side"]

        def _pnl(is_w: int) -> float:
            won = (side == "YES" and is_w) or (side == "NO" and not is_w)
            if won:
                p = mkt_yes if side == "YES" else (1 - mkt_yes)
                return (1.0 / p) - 1.0 if p > 0 else 0.0
            return -1.0

        pm_pnl = r["paper_pnl"]  # from DB
        metar_pnl = _pnl(metar_is_winner) if metar_winner is not None else None
        bet_deltas.append({
            "city": r["city_name"],
            "era": era_of.get(r["city_name"], "era_unknown"),
            "date": r["target_date"],
            "side": side,
            "pm_pnl": pm_pnl,
            "metar_pnl": metar_pnl,
            "agree": er["status"],
        })

    # === Summary ===
    n_total = len(event_results)
    n_agree = sum(1 for v in event_results.values() if v["status"] == "agree")
    n_disagree = sum(1 for v in event_results.values() if v["status"] == "disagree")
    n_skip = sum(1 for v in event_results.values() if v["status"].startswith("skip")
                 or v["status"].startswith("no_"))

    print(f"\n=== Event-level alignment ({n_total} events) ===")
    print(f"  METAR bucket matches Polymarket winner : {n_agree}/{n_total - n_skip} "
          f"({100 * n_agree / max(1, n_total - n_skip):.1f}%)")
    print(f"  Disagree                                : {n_disagree}")
    print(f"  Skipped (HK-observ / no data)            : {n_skip}")

    # Per-city
    per_city = defaultdict(lambda: {"agree": 0, "disagree": 0, "skip": 0})
    for (c, *_), v in event_results.items():
        per_city[c][v["status"] if v["status"] in ("agree", "disagree") else "skip"] += 1
    print(f"\n=== Per-city alignment ===")
    for c, d in sorted(per_city.items()):
        tot = d["agree"] + d["disagree"] + d["skip"]
        print(f"  {c:12s} agree={d['agree']:3d}  disagree={d['disagree']:3d}  "
              f"skip={d['skip']:3d}  (of {tot})")

    # Disagreement detail
    print(f"\n=== Disagreement events ===")
    for key, v in event_results.items():
        if v["status"] == "disagree":
            c, d, _ = key
            print(f"  {c} {d}: metar_max={v['metar_max_c']}°C  "
                  f"metar_picks={v['metar_winner']!r}  pm_winner={v['pm_winner']!r}")

    # Hypothetical PnL: apples-to-apples on METAR-covered subset only.
    covered = [b for b in bet_deltas if b["metar_pnl"] is not None]
    pm_covered = sum(b["pm_pnl"] for b in covered)
    metar_covered = sum(b["metar_pnl"] for b in covered)
    pm_all = sum(b["pm_pnl"] for b in bet_deltas if b["pm_pnl"] is not None)
    print(f"\n=== PnL cross-check (apples-to-apples) ===")
    print(f"  METAR covers {len(covered)}/{len(bet_deltas)} flagged resolved bets")
    print(f"  On covered subset  — Polymarket: ${pm_covered:+.2f}   METAR: ${metar_covered:+.2f}   Δ: ${metar_covered - pm_covered:+.2f}")
    print(f"  All flagged (ref)  — Polymarket: ${pm_all:+.2f}")
    print(f"  (Δ=0 on covered = METAR rounded max matches Polymarket winner 100%)")

    # Per-era
    per_era = defaultdict(lambda: {"n": 0, "pm": 0.0, "metar": 0.0})
    for b in bet_deltas:
        if b["metar_pnl"] is None:
            continue
        per_era[b["era"]]["n"] += 1
        per_era[b["era"]]["pm"] += b["pm_pnl"]
        per_era[b["era"]]["metar"] += b["metar_pnl"]
    print(f"\n=== Per-era ===")
    for era, d in sorted(per_era.items()):
        print(f"  {era:28s} n={d['n']:3d}  pm=${d['pm']:+7.2f}  "
              f"metar=${d['metar']:+7.2f}  delta=${d['metar'] - d['pm']:+6.2f}")

    # Persist
    summary = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "events": {
            f"{k[0]}|{k[1]}|{k[2]}": v for k, v in event_results.items()
        },
        "totals": {
            "events_total": n_total,
            "events_agree": n_agree,
            "events_disagree": n_disagree,
            "events_skip": n_skip,
            "bets_covered": len(covered),
            "pm_pnl_covered": round(pm_covered, 4),
            "metar_pnl_covered": round(metar_covered, 4),
            "pm_pnl_all_flagged": round(pm_all, 4),
            "delta_covered": round(metar_covered - pm_covered, 4),
        },
        "per_era": {k: {kk: round(vv, 4) if isinstance(vv, float) else vv
                       for kk, vv in v.items()} for k, v in per_era.items()},
        "per_city_alignment": {k: v for k, v in per_city.items()},
        "disagreements": [
            {"city": k[0], "date": k[1], "metar_max_c": v["metar_max_c"],
             "metar_picks": v["metar_winner"], "pm_winner": v["pm_winner"]}
            for k, v in event_results.items() if v["status"] == "disagree"
        ],
    }
    OUT_PATH.write_text(json.dumps(summary, indent=2, default=str))
    print(f"\n[OK] wrote {OUT_PATH}")

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
