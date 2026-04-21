#!/usr/bin/env python3
"""Backfill METAR obs for Polymarket resolution sources.

Polymarket weather markets each cite a specific ICAO airport station (verified
via gamma-api descriptions on 2026-04-21). The paper tracker currently resolves
against Polymarket's own outcomePrices so no backfill is needed for paper PnL
accuracy — the backfill's value is UPSTREAM: enables retraining the MOS on the
source Polymarket actually uses, and fixes the historical backtest.

This script:
  1. Loads the city → (icao, provider, unit) map from
     data/polymarket_resolution_sources.json
  2. Pulls hourly weather variables from Iowa Environmental Mesonet's global
     ASOS archive for each ICAO over the requested range
  3. Stores rows in observations_hourly_metar with a schema matching
     observations_hourly_station (minus station metadata — ICAO is the key)

Run: python3 scripts/backfill_polymarket_metar.py
     [--start YYYY-MM-DD] [--end YYYY-MM-DD]
     [--chunk year|month]  (default: year — 13×6=78 requests for 2020-2025)
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DB_PATH = ROOT / "data" / "weather.db"
MAP_PATH = ROOT / "data" / "polymarket_resolution_sources.json"

SKIP_CITIES = {"Hong Kong"}  # HKO, not ICAO — handle separately if needed.

MANUAL_ICAO = {
    "New York": {"icao": "KLGA", "provider": "Wunderground", "unit": "F"},
}

UA = "weather-predict-metar-backfill/1.0 (+https://github.com/saml-cell)"

# IEM tmpc=C, dwpc=C, relh=%, sknt=knots, drct=deg, p01i=inches, mslp=mb
IEM_VARS = "tmpc,dwpc,relh,sknt,drct,p01i,mslp"
IEM_URL = (
    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"
    "station={icao}&data={vars}&year1={y1}&month1={m1}&day1={d1}&"
    "year2={y2}&month2={m2}&day2={d2}&tz=Etc/UTC&format=onlycomma&"
    "latlon=no&missing=empty&trace=empty&direct=no&report_type=3&report_type=4"
)

KNOTS_TO_KMH = 1.852
INCHES_TO_MM = 25.4


def _hour_align(ts: str) -> str:
    """Round IEM 'YYYY-MM-DD HH:MM' timestamp to the nearest hour and
    format as ISO8601 with UTC offset — matches forecasts_hourly / obs.
    Equivalent SQL: strftime('%Y-%m-%dT%H:00:00+00:00', datetime(ts, '+30 minutes')).
    """
    # ts like "2026-04-20 00:53" — space separator, no tz
    date_part, time_part = ts.split(" ")
    hh, mm = time_part.split(":")[:2]
    h = int(hh)
    if int(mm) >= 30:
        h += 1
    # roll over midnight if needed
    if h == 24:
        from datetime import datetime as _dt, timedelta as _td
        d = _dt.strptime(date_part, "%Y-%m-%d") + _td(days=1)
        date_part = d.strftime("%Y-%m-%d")
        h = 0
    return f"{date_part}T{h:02d}:00:00+00:00"


def parse_rounding(rounding_str: str) -> str:
    s = (rounding_str or "").lower()
    if "fahrenheit" in s:
        return "F"
    if "one decimal" in s:
        return "C01"
    return "C"


def load_mapping() -> dict:
    mapping = json.loads(MAP_PATH.read_text())
    out: dict[str, dict] = {}
    for city, entry in mapping.items():
        if city in SKIP_CITIES:
            continue
        if not entry:
            if city in MANUAL_ICAO:
                out[city] = MANUAL_ICAO[city]
            continue
        icaos = entry.get("icao_hits") or []
        if not icaos:
            if city in MANUAL_ICAO:
                out[city] = MANUAL_ICAO[city]
            continue
        out[city] = {
            "icao": icaos[0],
            "provider": entry.get("provider", "?"),
            "unit": parse_rounding(entry.get("rounding", "")),
        }
    for city, m in MANUAL_ICAO.items():
        out.setdefault(city, m)
    return out


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
            temp_c REAL,
            dew_point_c REAL,
            humidity_pct REAL,
            wind_speed_kmh REAL,
            wind_dir_deg REAL,
            precip_mm REAL,
            pressure_msl_hpa REAL,
            fetched_at TEXT NOT NULL,
            UNIQUE(icao, valid_time)
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS ix_metar_city_date "
        "ON observations_hourly_metar(city_name, valid_time)"
    )
    # Add columns if migrating from earlier single-temp_c schema.
    cols = {r[1] for r in con.execute("PRAGMA table_info(observations_hourly_metar)")}
    # In-place rename for prior (tmpc, dwpc) → (temp_c, dew_point_c) — SQLite 3.25+.
    if "tmpc" in cols and "temp_c" not in cols:
        con.execute("ALTER TABLE observations_hourly_metar RENAME COLUMN tmpc TO temp_c")
    if "dwpc" in cols and "dew_point_c" not in cols:
        con.execute("ALTER TABLE observations_hourly_metar RENAME COLUMN dwpc TO dew_point_c")
    cols = {r[1] for r in con.execute("PRAGMA table_info(observations_hourly_metar)")}
    for col, decl in [
        ("dew_point_c", "REAL"),
        ("humidity_pct", "REAL"),
        ("wind_speed_kmh", "REAL"),
        ("wind_dir_deg", "REAL"),
        ("precip_mm", "REAL"),
        ("pressure_msl_hpa", "REAL"),
    ]:
        if col not in cols:
            con.execute(f"ALTER TABLE observations_hourly_metar ADD COLUMN {col} {decl}")
    con.commit()


def _fetch_once(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.read().decode()
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 5 * (attempt + 1)
                print(f"  [429] waiting {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue
            raise
        except urllib.error.URLError:
            time.sleep(5)
            continue
    raise RuntimeError(f"fetch failed after retries: {url}")


def fetch_range(icao: str, y1: int, m1: int, d1: int,
                y2: int, m2: int, d2: int) -> list[dict]:
    url = IEM_URL.format(
        icao=icao, vars=IEM_VARS,
        y1=y1, m1=m1, d1=d1,
        y2=y2, m2=m2, d2=d2,
    )
    text = _fetch_once(url)
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return []
    header = lines[0].split(",")
    rows = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) != len(header):
            continue
        rec = dict(zip(header, parts))
        valid = rec.get("valid", "").strip()
        if not valid:
            continue

        def _f(key: str) -> float | None:
            v = rec.get(key, "").strip()
            try:
                return float(v) if v else None
            except ValueError:
                return None

        tmpc = _f("tmpc")  # IEM uses tmpc; we store as temp_c
        dwpc = _f("dwpc")
        relh = _f("relh")
        sknt = _f("sknt")
        drct = _f("drct")
        p01i = _f("p01i")
        mslp = _f("mslp")
        rows.append({
            "valid_time": valid,
            "temp_c": tmpc,
            "dew_point_c": dwpc,
            "humidity_pct": relh,
            "wind_speed_kmh": sknt * KNOTS_TO_KMH if sknt is not None else None,
            "wind_dir_deg": drct,
            "precip_mm": p01i * INCHES_TO_MM if p01i is not None else None,
            "pressure_msl_hpa": mslp,
        })
    return rows


def chunk_dates(start: str, end: str, chunk: str) -> list[tuple[str, str]]:
    s = datetime.fromisoformat(start).date()
    e = datetime.fromisoformat(end).date()
    out = []
    cur = s
    while cur <= e:
        if chunk == "year":
            nxt = cur.replace(year=cur.year + 1, month=1, day=1) - timedelta(days=1)
        else:  # month
            if cur.month == 12:
                nxt = cur.replace(year=cur.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                nxt = cur.replace(month=cur.month + 1, day=1) - timedelta(days=1)
        if nxt > e:
            nxt = e
        out.append((cur.isoformat(), nxt.isoformat()))
        cur = nxt + timedelta(days=1)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2020-01-01", help="inclusive (YYYY-MM-DD)")
    ap.add_argument("--end", default="2025-12-31", help="inclusive (YYYY-MM-DD)")
    ap.add_argument("--chunk", choices=["year", "month"], default="year")
    ap.add_argument("--pace-sec", type=float, default=2.5,
                    help="seconds between requests (default 2.5)")
    ap.add_argument("--only-city", help="restrict to one city for testing")
    args = ap.parse_args()

    mapping = load_mapping()
    if args.only_city:
        mapping = {args.only_city: mapping[args.only_city]}
    print(f"Backfill window: {args.start} → {args.end}  ({args.chunk} chunks)")
    print(f"Cities: {len(mapping)}")

    con = sqlite3.connect(DB_PATH)
    ensure_schema(con)

    cid_map = {r[1]: r[0] for r in con.execute("SELECT id, name FROM cities")}
    chunks = chunk_dates(args.start, args.end, args.chunk)
    print(f"Chunks per station: {len(chunks)}  "
          f"total requests: {len(chunks) * len(mapping)}")

    now = datetime.now().astimezone().isoformat()
    grand_new = 0
    for city, m in sorted(mapping.items()):
        cid = cid_map.get(city)
        if cid is None:
            print(f"  [WARN] {city}: not in cities table — skipping")
            continue
        icao = m["icao"]
        print(f"\n=== {city} ({icao}) ===")
        city_new = 0
        for (s, e) in chunks:
            y1, m1, d1 = map(int, s.split("-"))
            y2, m2, d2 = map(int, e.split("-"))
            try:
                rows = fetch_range(icao, y1, m1, d1, y2, m2, d2)
            except Exception as exc:
                print(f"  [ERR] {s}..{e}: {exc}")
                time.sleep(args.pace_sec * 2)
                continue
            if not rows:
                print(f"  {s}..{e}: 0 rows")
                time.sleep(args.pace_sec)
                continue
            cur = con.cursor()
            # Rows are stored hour-aligned so the table joins cleanly with
            # forecasts_hourly (which is on an exact hour grid). Multiple IEM
            # reports in the same hour (e.g. :20 + :50, or SPECI) collapse
            # via aggregation below: AVG for continuous vars, SUM for precip.
            # This loses daily-max fidelity vs raw METAR — see cross-check
            # comment — but matches what MOS training needs (point-in-hour
            # observation, not peak).
            cur.executemany(
                """INSERT INTO observations_hourly_metar
                   (city_id, city_name, icao, provider, unit, valid_time,
                    temp_c, dew_point_c, humidity_pct, wind_speed_kmh,
                    wind_dir_deg, precip_mm, pressure_msl_hpa, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(icao, valid_time) DO UPDATE SET
                     temp_c = COALESCE((temp_c + excluded.temp_c) / 2.0, excluded.temp_c, temp_c),
                     dew_point_c = COALESCE((dew_point_c + excluded.dew_point_c) / 2.0, excluded.dew_point_c, dew_point_c),
                     humidity_pct = COALESCE((humidity_pct + excluded.humidity_pct) / 2.0, excluded.humidity_pct, humidity_pct),
                     wind_speed_kmh = COALESCE((wind_speed_kmh + excluded.wind_speed_kmh) / 2.0, excluded.wind_speed_kmh, wind_speed_kmh),
                     wind_dir_deg = COALESCE((wind_dir_deg + excluded.wind_dir_deg) / 2.0, excluded.wind_dir_deg, wind_dir_deg),
                     precip_mm = COALESCE(precip_mm, 0) + COALESCE(excluded.precip_mm, 0),
                     pressure_msl_hpa = COALESCE((pressure_msl_hpa + excluded.pressure_msl_hpa) / 2.0, excluded.pressure_msl_hpa, pressure_msl_hpa),
                     fetched_at = excluded.fetched_at
                """,
                [
                    (cid, city, icao, m["provider"], m["unit"],
                     _hour_align(r["valid_time"]),
                     r["temp_c"], r["dew_point_c"],
                     r["humidity_pct"], r["wind_speed_kmh"],
                     r["wind_dir_deg"], r["precip_mm"],
                     r["pressure_msl_hpa"], now)
                    for r in rows
                ],
            )
            con.commit()
            added = cur.rowcount  # last-executemany rowcount is reliable on sqlite3
            city_new += added
            print(f"  {s}..{e}: +{added} new (fetched {len(rows)})")
            time.sleep(args.pace_sec)
        grand_new += city_new
        total = con.execute(
            "SELECT COUNT(*) FROM observations_hourly_metar WHERE icao=?", (icao,)
        ).fetchone()[0]
        print(f"  [{city}] total for {icao}: {total} rows (+{city_new} this run)")

    con.close()
    print(f"\n[OK] grand total new rows this run: {grand_new}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
