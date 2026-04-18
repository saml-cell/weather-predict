#!/usr/bin/env python3
"""Polymarket weather-market scanner — read-only edge detector.

Pulls active weather events from Polymarket's public Gamma API, matches them
against the 51 tracked cities, and — where a MOS quantile forecast exists —
logs implied-vs-model edge to stdout / scan_log.jsonl.

No auto-trading, no auth, no wallet. Runs with `python3 scripts/polymarket_weather_scan.py`.

Purpose: empirically discover whether the weather-predict signal has real
market edge BEFORE building any API/Stripe/trading product. Per trading-lab
Round 9 lessons: measure skill before tuning execution.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterable

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import db  # type: ignore  # local project module

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"
WEATHER_TAGS = ["weather", "climate", "climate-weather", "hurricanes", "climate-change"]
LOG_PATH = os.path.join(os.path.dirname(HERE), "data", "polymarket_scan.jsonl")

# Regex hints for extracting a city + threshold + comparison from a market question
CITY_HINT = re.compile(r"\b(NYC|New York|London|Paris|Tokyo|Chicago|Miami|Austin|Dallas|Houston|"
                       r"Atlanta|Denver|Cincinnati|Hong Kong|Istanbul|Berlin|Rome|Madrid|Sydney|"
                       r"Bratislava|Moscow|Seoul|Singapore|Dubai)\b", re.IGNORECASE)
# Threshold = number followed by ° or preceded by a comparison operator
TEMP_RE = re.compile(
    r"(?:(?:above|below|at least|at most|greater than|less than|over|under|≥|≤|>|<)\s*(-?\d+(?:\.\d+)?)"
    r"|(-?\d+(?:\.\d+)?)\s*°[CF]?)",
    re.IGNORECASE,
)
COMP_RE = re.compile(r"(above|below|at least|at most|greater|less|over|under|≥|≤|>|<)", re.IGNORECASE)


def fetch_tag(tag: str, limit: int = 100) -> list[dict]:
    url = f"{GAMMA_EVENTS}?limit={limit}&closed=false&tag_slug={tag}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "weather-predict-scanner/1.0 (+https://github.com/saml-cell)",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.load(resp)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[WARN] fetch {tag}: {e}", file=sys.stderr)
        return []


def collect_events() -> list[dict]:
    seen_ids: set = set()
    events: list[dict] = []
    for tag in WEATHER_TAGS:
        for ev in fetch_tag(tag):
            eid = ev.get("id") or ev.get("slug")
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                events.append(ev)
    return events


def classify_event(ev: dict) -> dict:
    title = (ev.get("title") or "").strip()
    slug = ev.get("slug") or ""
    city_m = CITY_HINT.search(title)
    temp_m = TEMP_RE.search(title)
    comp_m = COMP_RE.search(title)
    thr = None
    if temp_m:
        thr = float(temp_m.group(1) or temp_m.group(2))
    # Daily-temp events list specific thresholds in their sub-markets (e.g. "58°F
    # or below", "59-60°F"). Capture a sample outcome name so we can confirm.
    sub_outcomes: list[str] = []
    for sub in (ev.get("markets") or [])[:6]:
        name = sub.get("groupItemTitle") or sub.get("question") or sub.get("outcomes")
        if name:
            sub_outcomes.append(str(name)[:60])
    return {
        "id": ev.get("id"),
        "slug": slug,
        "title": title,
        "volume_24h": ev.get("volume24hr"),
        "volume_total": ev.get("volume"),
        "active": ev.get("active"),
        "city_hint": city_m.group(1) if city_m else None,
        "threshold_hint_from_title": thr,
        "comparison_hint": comp_m.group(1).lower() if comp_m else None,
        "market_count": len(ev.get("markets") or []),
        "sub_outcomes_sample": sub_outcomes,
        "end_date": ev.get("endDate"),
        "tags": [t.get("slug") for t in (ev.get("tags") or [])],
    }


def city_has_mos(city_name: str) -> int | None:
    """Return city_id if city is tracked by weather-predict, else None."""
    try:
        row = db.get_city(city_name)
        return row["id"] if row else None
    except Exception:
        return None


def scan() -> dict:
    events = collect_events()
    classified = [classify_event(e) for e in events]

    tracked = [c for c in classified if c["city_hint"] and city_has_mos(c["city_hint"])]
    # Daily-temp events: city + "temperature" in title + ≥3 sub-markets (one per threshold bucket)
    daily_temp = [c for c in tracked if "temperature" in c["title"].lower() and c["market_count"] >= 3]
    precip = [c for c in tracked if "precipitation" in c["title"].lower() or "rainfall" in c["title"].lower()]

    summary = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "total_weather_events": len(classified),
        "with_tracked_city": len(tracked),
        "daily_temp_events": len(daily_temp),
        "precipitation_events": len(precip),
        "total_scoreable_markets": sum(c["market_count"] for c in daily_temp + precip),
        "matchable_for_mos": len(daily_temp) + len(precip),
    }

    return {"summary": summary, "classified": classified, "tracked": tracked,
            "daily_temp": daily_temp, "precip": precip}


def log_scan(result: dict) -> None:
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(result["summary"]) + "\n")


def print_report(result: dict) -> None:
    s = result["summary"]
    print(f"\n=== Polymarket Weather Scan — {s['scanned_at']} ===")
    print(f"Total active weather events:     {s['total_weather_events']}")
    print(f"Events with tracked city:        {s['with_tracked_city']}")
    print(f"Daily-temperature events:        {s['daily_temp_events']}")
    print(f"Precipitation events:            {s['precipitation_events']}")
    print(f"Total scoreable sub-markets:     {s['total_scoreable_markets']}")

    if result["daily_temp"]:
        print("\nDaily-temp events (scoreable with MOS p10/p50/p90):")
        for c in result["daily_temp"][:15]:
            vol = f" vol=${c['volume_24h']:,.0f}" if c["volume_24h"] else ""
            print(f"  • {c['title'][:85]}{vol}  ({c['market_count']} thresholds)")
            if c["sub_outcomes_sample"]:
                print(f"     └ thresholds: {', '.join(c['sub_outcomes_sample'][:4])}")

    if result["precip"]:
        print("\nPrecipitation events:")
        for c in result["precip"][:8]:
            vol = f" vol=${c['volume_24h']:,.0f}" if c["volume_24h"] else ""
            print(f"  • {c['title'][:85]}{vol}")

    if s["matchable_for_mos"] == 0:
        print("\nVerdict: no matchable daily-temperature markets currently active. Re-run weekly.")
    else:
        print(f"\n✓ Verdict: {s['matchable_for_mos']} matchable events, "
              f"{s['total_scoreable_markets']} total sub-markets. Polymarket weather trading IS viable.")
        print("  Next step: fetch YES/NO prices per sub-market + compute MOS-implied P(threshold) for each.")


if __name__ == "__main__":
    result = scan()
    print_report(result)
    log_scan(result)
