#!/usr/bin/env python3
"""Backfill METAR obs for Polymarket resolution sources.

Polymarket weather markets each cite a specific ICAO airport station (verified
via gamma-api descriptions on 2026-04-21). The paper tracker currently resolves
against the ensemble API `observations.temp_high_c` which has no alignment to
any of those stations (e.g. London → EGLC City Airport, Dallas → KDAL Love
Field — not Heathrow or DFW).

This script:
  1. Loads the city → (icao, provider, unit) map from
     data/polymarket_resolution_sources.json
  2. Pulls hourly temp_c from Iowa Environmental Mesonet's global ASOS archive
     for each ICAO over the date range of polymarket_scan
  3. Stores rows in observations_hourly_metar (city_id, icao, valid_time, tmpc)

Run: python3 scripts/backfill_polymarket_metar.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DB_PATH = ROOT / "data" / "weather.db"
MAP_PATH = ROOT / "data" / "polymarket_resolution_sources.json"

# Hong Kong uses HKO (not ICAO). Skip here — handle separately if needed.
SKIP_CITIES = {"Hong Kong"}

# Explicit map for cities the auto-probe missed (NYC had no active 04-22 market)
MANUAL_ICAO = {
    "New York": {"icao": "KLGA", "provider": "Wunderground", "unit": "F"},
}

UA = "weather-predict-metar-backfill/1.0 (+https://github.com/saml-cell)"
IEM_URL = (
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
    "station={icao}&data=tmpc&year1={y1}&month1={m1}&day1={d1}&"
    "year2={y2}&month2={m2}&day2={d2}&tz=Etc/UTC&format=onlycomma&"
    "latlon=no&missing=empty&trace=empty&direct=no&report_type=3&report_type=4"
)


def parse_rounding(rounding_str: str) -> str:
    """Return 'F', 'C', or 'C01' (0.1°C) based on the market's rounding clause."""
    s = (rounding_str or "").lower()
    if "fahrenheit" in s:
        return "F"
    if "one decimal" in s:
        return "C01"
    return "C"


def load_mapping() -> dict:
    mapping = json.loads(MAP_PATH.read_text())
    out = {}
    for city, entry in mapping.items():
        if city in SKIP_CITIES:
            print(f"  [SKIP] {city} — non-ICAO source (flag for separate handling)")
            continue
        if not entry:
            # auto-probe found nothing; try manual fallback
            m = MANUAL_ICAO.get(city)
            if not m:
                print(f"  [SKIP] {city} — no mapping and no manual entry")
                continue
            out[city] = m
            continue
        icaos = entry.get("icao_hits") or []
        if not icaos:
            m = MANUAL_ICAO.get(city)
            if m:
                out[city] = m
            else:
                print(f"  [SKIP] {city} — no ICAO extracted")
            continue
        out[city] = {
            "icao": icaos[0],
            "provider": entry.get("provider", "?"),
            "unit": parse_rounding(entry.get("rounding", "")),
        }
    # splice manual entries that auto-probe missed
    for city, m in MANUAL_ICAO.items():
        if city not in out:
            out[city] = m
    return out


def fetch_metar(icao: str, start: str, end: str) -> list[tuple[str, float]]:
    y1, m1, d1 = start.split("-")
    y2, m2, d2 = end.split("-")
    url = IEM_URL.format(icao=icao, y1=y1, m1=m1, d1=d1, y2=y2, m2=m2, d2=d2)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    attempts = 0
    while True:
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                text = r.read().decode()
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempts < 5:
                attempts += 1
                wait = 5 * attempts
                print(f"  [429] {icao} — waiting {wait}s (attempt {attempts})")
                time.sleep(wait)
                continue
            raise
    rows = []
    for line in text.strip().split("\n")[1:]:  # skip header
        parts = line.split(",")
        if len(parts) < 3:
            continue
        _st, valid, t = parts[0], parts[1], parts[2]
        if not t.strip():
            continue
        try:
            rows.append((valid, float(t)))
        except ValueError:
            continue
    return rows


def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS observations_hourly_metar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INTEGER NOT NULL,
            city_name TEXT NOT NULL,
            icao TEXT NOT NULL,
            provider TEXT NOT NULL,
            unit TEXT NOT NULL,
            valid_time TEXT NOT NULL,
            tmpc REAL NOT NULL,
            fetched_at TEXT NOT NULL,
            UNIQUE(icao, valid_time)
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS ix_metar_city_date "
        "ON observations_hourly_metar(city_name, valid_time)"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-04-18",
                    help="inclusive start date (YYYY-MM-DD)")
    ap.add_argument("--end", default="2026-04-22",
                    help="inclusive end date (YYYY-MM-DD)")
    args = ap.parse_args()

    mapping = load_mapping()
    print(f"Mapping → {len(mapping)} cities:")
    for c, m in sorted(mapping.items()):
        print(f"  {c:12s} → {m['icao']} ({m['provider']}, unit={m['unit']})")

    con = sqlite3.connect(DB_PATH)
    ensure_schema(con)

    # Resolve city_id
    cid_map = {r[1]: r[0] for r in con.execute("SELECT id, name FROM cities")}

    from datetime import datetime
    now = datetime.utcnow().isoformat()
    total_rows = 0
    for city, m in sorted(mapping.items()):
        cid = cid_map.get(city)
        if cid is None:
            print(f"  [WARN] {city} not in cities table — skipping")
            continue
        print(f"\n=== {city} ({m['icao']}) ===")
        try:
            rows = fetch_metar(m["icao"], args.start, args.end)
        except Exception as e:
            print(f"  [ERR] fetch failed: {e}")
            time.sleep(3)
            continue
        print(f"  {len(rows)} rows")
        if not rows:
            continue
        cur = con.cursor()
        before = cur.execute(
            "SELECT COUNT(*) FROM observations_hourly_metar WHERE icao=?",
            (m["icao"],),
        ).fetchone()[0]
        cur.executemany(
            "INSERT OR IGNORE INTO observations_hourly_metar "
            "(city_id, city_name, icao, provider, unit, valid_time, tmpc, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(cid, city, m["icao"], m["provider"], m["unit"], v, t, now)
             for v, t in rows],
        )
        con.commit()
        after = cur.execute(
            "SELECT COUNT(*) FROM observations_hourly_metar WHERE icao=?",
            (m["icao"],),
        ).fetchone()[0]
        new = after - before
        total_rows += new
        print(f"  +{new} new ({after} total for station)")
        time.sleep(2)  # polite pacing for IEM

    con.close()
    print(f"\n[OK] total new rows: {total_rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
