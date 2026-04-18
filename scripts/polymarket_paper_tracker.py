#!/usr/bin/env python3
"""Polymarket weather paper P&L tracker.

Two modes:
  scan       — for each tracked city, fetch tomorrow's highest-temperature
               event + pull MOS p10/p50/p90 + compute per-bucket edge +
               insert into polymarket_scan. Safe to run daily from cron.

  resolve    — for yesterday's scan rows, pull the resolved winning bucket
               from Polymarket (the market with outcomePrices[0]~1), pull
               actual observed high from observations table, compute paper
               P&L as if we had bet $1 on every edge>EDGE_THRESHOLD bucket
               at the scanned market YES price, settled at Polymarket's
               resolution. No real money, no auth.

Usage:
  python3 scripts/polymarket_paper_tracker.py scan [--target 2026-04-20]
  python3 scripts/polymarket_paper_tracker.py resolve [--target 2026-04-18]

Purpose: build 30-60 days of MOS-vs-market paired data so we can verify
the edge claim BEFORE committing real capital (trading-lab Round 9 lesson:
measure skill before tuning execution).
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import db  # local project module
import mos_inference  # direct MOS predictions, skip API round-trip

DB_PATH = HERE.parent / "data" / "weather.db"
API_BASE = "http://127.0.0.1:5000"
GAMMA = "https://gamma-api.polymarket.com"
UA = {"User-Agent": "weather-predict-paper/0.1 (+github.com/saml-cell)"}
EDGE_THRESHOLD = 0.08  # only "bet" when abs(MOS_prob - market_yes) > 8%

# Polymarket slug city → weather-predict city name (for DB lookup)
CITY_MAP = {
    "london": "London",
    "dallas": "Dallas",
    "paris": "Paris",
    "chicago": "Chicago",
    "miami": "Miami",
    "hong-kong": "Hong Kong",
    "madrid": "Madrid",
    "austin": "Austin",
    "denver": "Denver",
    "houston": "Houston",
    "moscow": "Moscow",
    "istanbul": "Istanbul",
}


def fetch_json(url: str, timeout: int = 30):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def fetch_event(city_slug: str, target: date) -> dict | None:
    """Fetch the Polymarket event for a given city + date."""
    month_name = target.strftime("%B").lower()  # "april"
    # month-day-year slug convention
    slug = f"highest-temperature-in-{city_slug}-on-{month_name}-{target.day}-{target.year}"
    data = fetch_json(f"{GAMMA}/events?slug={slug}")
    return data[0] if isinstance(data, list) and data else None


_MOS_CACHE: dict[int, dict] = {}

def fetch_mos_for(city_id: int, target: date) -> tuple | None:
    """Return (p10, p50, p90) for temp_high on target_date, or None.

    Calls mos_inference.predict_for_city directly (no HTTP round-trip)
    to avoid contention with the running weather-api server.
    """
    if city_id in _MOS_CACHE:
        result = _MOS_CACHE[city_id]
    else:
        city = db.get_city_by_id(city_id)
        if not city:
            return None
        try:
            result = mos_inference.predict_for_city(
                {"id": city["id"], "lat": city["lat"], "lon": city["lon"], "name": city["name"]},
                forecast_days=7,
            )
            _MOS_CACHE[city_id] = result
        except Exception as e:
            print(f"  [WARN] MOS inference failed {city_id}: {e}", file=sys.stderr)
            return None
    if not result.get("models_loaded") or not result.get("daily_by_date"):
        return None
    day = result["daily_by_date"].get(target.isoformat())
    if not day:
        return None
    p10, p50, p90 = day.get("temp_high_p10"), day.get("temp_high_p50"), day.get("temp_high_p90")
    if p10 is None or p50 is None or p90 is None:
        return None
    return (float(p10), float(p50), float(p90))


def parse_bucket(label: str) -> tuple[float, float] | None:
    """Parse '15°C' / '74-75°F' / '50°F or below' / '86°F or higher' → (lo_c, hi_c)."""
    s = label.replace("–", "-").strip()
    is_f = "°F" in s
    nums: list[float] = []
    cur = ""
    for ch in s:
        if ch.isdigit() or ch == "-" or ch == ".":
            cur += ch
        else:
            if cur:
                try:
                    nums.append(float(cur))
                except ValueError:
                    pass
                cur = ""
    if cur:
        try:
            nums.append(float(cur))
        except ValueError:
            pass
    low = s.lower()
    if "or below" in low:
        if not nums:
            return None
        lo, hi = -999.0, nums[0] + 0.5
    elif "or higher" in low or "or above" in low:
        if not nums:
            return None
        lo, hi = nums[0] - 0.5, 999.0
    elif len(nums) == 1:
        lo, hi = nums[0] - 0.5, nums[0] + 0.5
    elif len(nums) >= 2:
        lo, hi = nums[0] - 0.5, nums[1] + 0.5
    else:
        return None
    if is_f:
        if lo > -900:
            lo = (lo - 32) * 5.0 / 9.0
        if hi < 900:
            hi = (hi - 32) * 5.0 / 9.0
    return lo, hi


def mos_cdf(x_c: float, p10: float, p50: float, p90: float) -> float:
    """Piecewise linear CDF through three quantiles."""
    if x_c <= p10:
        slope_p10 = (p50 - p10)  # span of 0.4 probability mass per (p50-p10)°C
        if slope_p10 <= 0:
            return 0.1 if x_c == p10 else 0.0
        return max(0.0, 0.1 - (p10 - x_c) / slope_p10 * 0.1)
    if x_c < p50:
        return 0.1 + (x_c - p10) / (p50 - p10) * 0.4
    if x_c < p90:
        return 0.5 + (x_c - p50) / (p90 - p50) * 0.4
    slope_p90 = (p90 - p50)
    if slope_p90 <= 0:
        return 0.9 if x_c == p90 else 1.0
    return min(1.0, 0.9 + (x_c - p90) / slope_p90 * 0.1)


def mos_bucket_prob(lo_c: float, hi_c: float, mos: tuple[float, float, float]) -> float:
    p10, p50, p90 = mos
    lo_p = 0.0 if lo_c <= -900 else mos_cdf(lo_c, p10, p50, p90)
    hi_p = 1.0 if hi_c >= 900 else mos_cdf(hi_c, p10, p50, p90)
    return max(0.0, hi_p - lo_p)


def scan_once(target: date) -> int:
    """Run one scan: write polymarket_scan rows for target date. Returns rows added."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).isoformat()
    added = 0
    for slug, city_name in CITY_MAP.items():
        city = db.get_city(city_name)
        if not city:
            print(f"  [WARN] city not tracked: {city_name}")
            continue
        try:
            ev = fetch_event(slug, target)
        except Exception as e:
            print(f"  [WARN] event fetch {slug}: {e}")
            continue
        if not ev:
            print(f"  [--] no event: {slug}")
            continue
        mos = fetch_mos_for(city["id"], target)
        if not mos:
            print(f"  [--] no MOS: {city_name}")
            continue
        ev_vol = float(ev.get("volume24hr") or 0)
        for m in (ev.get("markets") or []):
            label = m.get("groupItemTitle") or ""
            rng = parse_bucket(label)
            if not rng:
                continue
            lo, hi = rng
            try:
                p = json.loads(m.get("outcomePrices") or "[]")
                yes = float(p[0]) if p else None
            except Exception:
                yes = None
            if yes is None:
                continue
            m_vol = float(m.get("volumeNum") or m.get("volume") or 0)
            mp = mos_bucket_prob(lo, hi, mos)
            edge = mp - yes
            side = None
            if edge > EDGE_THRESHOLD:
                side = "YES"
            elif edge < -EDGE_THRESHOLD:
                side = "NO"
            try:
                con.execute(
                    """INSERT OR REPLACE INTO polymarket_scan
                    (scan_at, target_date, city_name, city_id, event_slug,
                     event_volume_24h, bucket_label, bucket_lo_c, bucket_hi_c,
                     market_yes, market_volume, mos_p10, mos_p50, mos_p90,
                     mos_prob, edge, would_bet_side)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (now, target.isoformat(), city_name, city["id"], ev.get("slug"),
                     ev_vol, label,
                     None if lo <= -900 else lo,
                     None if hi >= 900 else hi,
                     yes, m_vol, mos[0], mos[1], mos[2], mp, edge, side),
                )
                added += 1
            except Exception as e:
                print(f"  [WARN] insert {city_name} {label}: {e}")
    con.commit()
    con.close()
    return added


def resolve_day(target: date) -> dict:
    """For all scan rows with target_date=target, fetch resolution + actual +
    compute paper P&L. $1 stake per bet if would_bet_side was set."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    # Actual observed high per city
    actual: dict[str, float] = {}
    rows = con.execute(
        "SELECT c.name, o.temp_high_c FROM observations o "
        "JOIN cities c ON c.id=o.city_id "
        "WHERE o.obs_date=? AND o.temp_high_c IS NOT NULL",
        (target.isoformat(),),
    ).fetchall()
    for r in rows:
        actual[r["name"]] = float(r["temp_high_c"])

    # Resolution from Polymarket: refetch each event, find winning bucket
    winners: dict[str, str] = {}
    for slug, city_name in CITY_MAP.items():
        try:
            ev = fetch_event(slug, target)
        except Exception:
            continue
        if not ev:
            continue
        for m in (ev.get("markets") or []):
            try:
                p = json.loads(m.get("outcomePrices") or "[]")
                if p and float(p[0]) > 0.9:
                    winners[city_name] = m.get("groupItemTitle") or ""
                    break
            except Exception:
                continue

    # Join back to scan rows, compute P&L
    scans = con.execute(
        "SELECT * FROM polymarket_scan WHERE target_date=?",
        (target.isoformat(),),
    ).fetchall()
    stats = {"total_rows": len(scans), "bets": 0, "wins": 0, "losses": 0,
            "paper_pnl": 0.0, "by_city": {}}
    for r in scans:
        winner_label = winners.get(r["city_name"])
        is_winner = 1 if winner_label and winner_label == r["bucket_label"] else 0
        actual_c = actual.get(r["city_name"])
        pnl = None
        if r["would_bet_side"] and winner_label is not None:
            # $1 stake. If YES side: win 1/yes - 1, lose -1. If NO side: win 1/(1-yes) - 1, lose -1.
            mkt_yes = float(r["market_yes"])
            won = (r["would_bet_side"] == "YES" and is_winner) or \
                  (r["would_bet_side"] == "NO" and not is_winner)
            if won:
                if r["would_bet_side"] == "YES":
                    pnl = (1.0 / mkt_yes) - 1.0 if mkt_yes > 0 else 0
                else:
                    no_price = 1.0 - mkt_yes
                    pnl = (1.0 / no_price) - 1.0 if no_price > 0 else 0
            else:
                pnl = -1.0
            stats["bets"] += 1
            if won:
                stats["wins"] += 1
            else:
                stats["losses"] += 1
            stats["paper_pnl"] += pnl
            by_c = stats["by_city"].setdefault(r["city_name"], {"bets": 0, "pnl": 0.0})
            by_c["bets"] += 1
            by_c["pnl"] += pnl
        con.execute(
            "UPDATE polymarket_scan SET resolved_yes=?, actual_high_c=?, paper_pnl=? WHERE id=?",
            (is_winner, actual_c, pnl, r["id"]),
        )
    con.commit()
    con.close()
    return stats


def status_report() -> None:
    """Human-readable dashboard: latest scan summary + paper P&L so far."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    latest_target = con.execute(
        "SELECT MAX(target_date) FROM polymarket_scan"
    ).fetchone()[0]
    if not latest_target:
        print("No scans yet. Run: python3 scripts/polymarket_paper_tracker.py scan")
        return

    print(f"\n=== Polymarket Weather Tracker — Status at {datetime.now().strftime('%Y-%m-%d %H:%M')} ===\n")

    # Latest scan summary
    print(f"[Latest scan target: {latest_target}]")
    rows = con.execute("""SELECT city_name, mos_p50, COUNT(*) AS n,
                                  SUM(would_bet_side IS NOT NULL) AS bets
                          FROM polymarket_scan WHERE target_date=?
                          GROUP BY city_name ORDER BY city_name""",
                       (latest_target,)).fetchall()
    for r in rows:
        print(f"  {r['city_name']:12} MOS p50={r['mos_p50']:5.1f}°C  buckets={r['n']:2}  flagged-bets={r['bets']}")

    # Top edges tomorrow
    print(f"\n[Top 8 edges for {latest_target}]")
    for r in con.execute("""SELECT city_name, bucket_label, market_yes, mos_prob, edge, would_bet_side
                             FROM polymarket_scan
                             WHERE target_date=? AND would_bet_side IS NOT NULL
                             ORDER BY ABS(edge) DESC LIMIT 8""", (latest_target,)):
        print(f"  {r['city_name']:10} {r['would_bet_side']:3} {r['bucket_label']:20} mkt={r['market_yes']:.2f} mos={r['mos_prob']:.2f} edge={r['edge']:+.2f}")

    # Paper P&L aggregate
    print("\n[Paper P&L — resolved bets only]")
    p = con.execute("""SELECT COUNT(*) AS bets,
                              SUM(paper_pnl > 0) AS wins,
                              SUM(paper_pnl < 0) AS losses,
                              ROUND(SUM(paper_pnl), 2) AS total_pnl,
                              ROUND(AVG(paper_pnl), 3) AS avg_pnl
                       FROM polymarket_scan WHERE paper_pnl IS NOT NULL""").fetchone()
    bets = p["bets"] or 0
    if bets == 0:
        print("  No resolved bets yet. First resolution cron runs at 08:00 UTC daily.")
    else:
        wins = p["wins"] or 0
        winrate = 100.0 * wins / bets if bets else 0
        print(f"  Resolved bets: {bets}    Wins: {wins}    Losses: {p['losses']}    Win rate: {winrate:.1f}%")
        print(f"  Total paper P&L: {p['total_pnl']:+.2f}    Avg per bet: {p['avg_pnl']:+.3f}")

    # Per-city breakdown (resolved)
    bycity = con.execute("""SELECT city_name, COUNT(*) AS bets,
                                  SUM(paper_pnl > 0) AS wins,
                                  ROUND(SUM(paper_pnl),2) AS pnl
                            FROM polymarket_scan WHERE paper_pnl IS NOT NULL
                            GROUP BY city_name ORDER BY pnl DESC""").fetchall()
    if bycity:
        print("\n[Per-city P&L]")
        for r in bycity:
            print(f"  {r['city_name']:12} bets={r['bets']:2}  wins={r['wins']:2}  pnl={r['pnl']:+.2f}")

    # Coverage check
    print("\n[Data coverage]")
    covered = con.execute("SELECT COUNT(DISTINCT target_date) FROM polymarket_scan").fetchone()[0]
    resolved = con.execute("SELECT COUNT(DISTINCT target_date) FROM polymarket_scan WHERE paper_pnl IS NOT NULL").fetchone()[0]
    print(f"  Target dates scanned: {covered}    Resolved: {resolved}")
    print(f"  Log: ~/.openclaw/logs/polymarket-paper.log")

    con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["scan", "resolve", "status"])
    ap.add_argument("--target", help="YYYY-MM-DD; defaults to tomorrow for scan, yesterday for resolve")
    args = ap.parse_args()

    today = date.today()
    if args.mode == "scan":
        target = date.fromisoformat(args.target) if args.target else today + timedelta(days=1)
        print(f"Scanning Polymarket weather events for {target}...")
        n = scan_once(target)
        print(f"Inserted/updated {n} bucket rows.")
    elif args.mode == "resolve":
        target = date.fromisoformat(args.target) if args.target else today - timedelta(days=1)
        print(f"Resolving scans for {target}...")
        stats = resolve_day(target)
        print(json.dumps(stats, indent=2, default=str))
    else:
        status_report()


if __name__ == "__main__":
    main()
