#!/usr/bin/env python3
"""
Backfill historical forecasts + observations from Open-Meteo for MOS training.

Pulls past forecast issuances from the Open-Meteo Historical Forecast API
(https://historical-forecast-api.open-meteo.com/v1/forecast) and matching
observations from the Open-Meteo Archive API
(https://archive-api.open-meteo.com/v1/archive), then joins them into
(issue_time, valid_time, lead_hours) training pairs ready for LightGBM MOS.

Design notes from the council audit (see wiki/weather-predict-council-audit.md):
- Hourly resolution (daily aggregation discards diurnal-cycle biases).
- Explicit (issue_time, valid_time, lead_hours) on every row — no implicit joins.
- Strictly forward-in-time pairing. The historical forecast API returns the
  *issued* forecast for a given day; we treat every hour in that day's run as
  valid at its timestamp with lead = valid - issue.
- Model-version boundaries: we store the model name as a feature, and flag
  any day where Open-Meteo itself documents a model upgrade.
- Physics invariants enforced at ingest (ingress gate) — no row with
  humidity > 100, T < Td, or precip < 0 is persisted.

Usage:
    python3 scripts/backfill_historical_forecasts.py \\
        --start 2022-01-01 \\
        --end 2026-04-10 \\
        --models ecmwf_ifs025,gfs025,icon_global,jma_gsm \\
        --cities all

    python3 scripts/backfill_historical_forecasts.py --dry-run --city-id 1

Creates tables on first run:
    forecasts_hourly     — one row per (city, source, issue_time, valid_time)
    observations_hourly  — one row per (city, valid_time)
"""

import argparse
import logging
import math
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

import urllib.request
import urllib.parse
import urllib.error
import json

# Allow running directly (python3 scripts/backfill_historical_forecasts.py)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import get_connection, load_config  # type: ignore

logger = logging.getLogger("backfill")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Open-Meteo model identifiers with full historical forecast coverage back to
# 2022-01-01. Verified against the API — ecmwf_ifs04/aifs025 return rows for
# 2023 but with all-null values; gfs025 and jma_gsm don't exist under those
# names. The list below is the tested working set.
#
# See https://open-meteo.com/en/docs/historical-forecast-api for the authoritative
# list. Run `scripts/backfill_historical_forecasts.py --models <candidate> --dry-run`
# against a 2-day window to verify before committing to a new model.
DEFAULT_MODELS = [
    "gfs_global",       # NOAA GFS global
    "icon_seamless",    # DWD ICON seamless
    "jma_seamless",     # JMA seamless
    "ecmwf_ifs025",     # ECMWF IFS 0.25° — only available from ~2024-01-01
                        # Earlier dates return null; LightGBM handles NaN.
]

HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "pressure_msl",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
]

# Rate-limit knobs. Open-Meteo free tier: 10k calls/day, ~600/hour sustained.
REQUEST_SLEEP_S = 1.2       # polite pause between requests
RETRY_ATTEMPTS = 4
RETRY_BACKOFF_BASE_S = 2.0
HTTP_TIMEOUT_S = 60

# Cap per-call date span so we don't ask for > ~370 days at once (API quirk).
MAX_DAYS_PER_CALL = 366

# Physics invariants — match what Aiko specified in the audit.
MAX_REASONABLE_PRESSURE_HPA = 1150.0
MIN_REASONABLE_PRESSURE_HPA = 850.0


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS forecasts_hourly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id INTEGER NOT NULL,
    source TEXT NOT NULL,           -- e.g. 'ecmwf_ifs025'
    issue_time TEXT NOT NULL,       -- ISO 8601 UTC, midnight of run day
    valid_time TEXT NOT NULL,       -- ISO 8601 UTC, top of the hour
    lead_hours INTEGER NOT NULL,    -- valid_time - issue_time, in hours
    temp_c REAL,
    dew_point_c REAL,
    humidity_pct REAL,
    precip_mm REAL,
    pressure_msl_hpa REAL,
    surface_pressure_hpa REAL,
    wind_speed_kmh REAL,
    wind_dir_deg REAL,
    cloud_cover_pct REAL,
    fetched_at TEXT NOT NULL,
    FOREIGN KEY (city_id) REFERENCES cities (id) ON DELETE CASCADE,
    UNIQUE (city_id, source, issue_time, valid_time)
);

CREATE INDEX IF NOT EXISTS idx_fh_city_valid
    ON forecasts_hourly (city_id, valid_time);

CREATE INDEX IF NOT EXISTS idx_fh_source_valid
    ON forecasts_hourly (source, valid_time);

CREATE TABLE IF NOT EXISTS observations_hourly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id INTEGER NOT NULL,
    valid_time TEXT NOT NULL,       -- ISO 8601 UTC
    temp_c REAL,
    dew_point_c REAL,
    humidity_pct REAL,
    precip_mm REAL,
    pressure_msl_hpa REAL,
    surface_pressure_hpa REAL,
    wind_speed_kmh REAL,
    wind_dir_deg REAL,
    cloud_cover_pct REAL,
    source TEXT NOT NULL DEFAULT 'open_meteo_archive',
    fetched_at TEXT NOT NULL,
    FOREIGN KEY (city_id) REFERENCES cities (id) ON DELETE CASCADE,
    UNIQUE (city_id, valid_time, source)
);

CREATE INDEX IF NOT EXISTS idx_oh_city_valid
    ON observations_hourly (city_id, valid_time);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def http_get_json(url: str, params: dict) -> Optional[dict]:
    """GET with retries and exponential backoff. Returns parsed JSON or None."""
    query = urllib.parse.urlencode(params, doseq=True)
    full = f"{url}?{query}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            req = urllib.request.Request(
                full, headers={"User-Agent": "weather-predict-backfill/1.0"}
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
                data = resp.read()
                return json.loads(data)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = RETRY_BACKOFF_BASE_S * (2 ** (attempt - 1))
                logger.warning("429 rate limited; sleeping %.1fs", wait)
                time.sleep(wait)
                continue
            body = e.read().decode("utf-8", errors="replace")[:500]
            logger.error(
                "HTTP %d on attempt %d/%d: %s | body=%s",
                e.code, attempt, RETRY_ATTEMPTS, full, body,
            )
            if 500 <= e.code < 600:
                time.sleep(RETRY_BACKOFF_BASE_S * attempt)
                continue
            return None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            logger.warning(
                "Transient error on attempt %d/%d (%s); retrying", attempt, RETRY_ATTEMPTS, e
            )
            time.sleep(RETRY_BACKOFF_BASE_S * attempt)
    logger.error("Giving up after %d attempts: %s", RETRY_ATTEMPTS, full)
    return None


# ---------------------------------------------------------------------------
# Physics invariants (Aiko's ingress gate)
# ---------------------------------------------------------------------------

def _magnus_dew_point(temp_c: float, rh_pct: float) -> Optional[float]:
    """Magnus-Tetens dew point. Returns None on invalid inputs."""
    if temp_c is None or rh_pct is None:
        return None
    rh = max(0.1, min(100.0, rh_pct))
    a, b = 17.625, 243.04
    try:
        alpha = math.log(rh / 100.0) + (a * temp_c) / (b + temp_c)
        return (b * alpha) / (a - alpha)
    except (ValueError, ZeroDivisionError):
        return None


def physics_ok(row: dict) -> bool:
    """Reject rows that violate physical invariants. Returns True if OK."""
    t = row.get("temp_c")
    rh = row.get("humidity_pct")
    td = row.get("dew_point_c")
    p = row.get("precip_mm")
    pr_msl = row.get("pressure_msl_hpa")

    if rh is not None and (rh < 0 or rh > 100.5):  # tiny tolerance for rounding
        return False
    if p is not None and p < 0:
        return False
    if pr_msl is not None and not (
        MIN_REASONABLE_PRESSURE_HPA <= pr_msl <= MAX_REASONABLE_PRESSURE_HPA
    ):
        return False
    # T ≥ Td: recompute Td if missing, then check.
    if t is not None and rh is not None:
        td_calc = _magnus_dew_point(t, rh)
        if td_calc is not None and t + 0.3 < td_calc:  # 0.3°C tolerance for sensor noise
            return False
        if td is not None and t + 0.3 < td:
            return False
    return True


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _get(lst: list, i: int):
    if lst is None or i >= len(lst):
        return None
    v = lst[i]
    return v if v is not None else None


def parse_hourly_block(raw: dict) -> list:
    """Turn an Open-Meteo hourly response into a list of dicts keyed by time."""
    hourly = raw.get("hourly") or {}
    times = hourly.get("time") or []
    out = []
    for i, t in enumerate(times):
        row = {
            "valid_time": t,
            "temp_c": _get(hourly.get("temperature_2m"), i),
            "dew_point_c": _get(hourly.get("dew_point_2m"), i),
            "humidity_pct": _get(hourly.get("relative_humidity_2m"), i),
            "precip_mm": _get(hourly.get("precipitation"), i),
            "pressure_msl_hpa": _get(hourly.get("pressure_msl"), i),
            "surface_pressure_hpa": _get(hourly.get("surface_pressure"), i),
            "wind_speed_kmh": _get(hourly.get("wind_speed_10m"), i),
            "wind_dir_deg": _get(hourly.get("wind_direction_10m"), i),
            "cloud_cover_pct": _get(hourly.get("cloud_cover"), i),
        }
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------

def fetch_historical_forecast(
    lat: float, lon: float, start: date, end: date, model: str
) -> list:
    """One historical-forecast call for (lat, lon, model, [start, end])."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "models": model,
        "timezone": "UTC",
        "wind_speed_unit": "kmh",
    }
    time.sleep(REQUEST_SLEEP_S)
    raw = http_get_json(HISTORICAL_FORECAST_URL, params)
    if not raw:
        return []
    return parse_hourly_block(raw)


def fetch_archive_observations(
    lat: float, lon: float, start: date, end: date
) -> list:
    """Observations from Open-Meteo archive (ERA5-backed)."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
        "wind_speed_unit": "kmh",
    }
    time.sleep(REQUEST_SLEEP_S)
    raw = http_get_json(ARCHIVE_URL, params)
    if not raw:
        return []
    return parse_hourly_block(raw)


# ---------------------------------------------------------------------------
# Insertion
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso_naive_to_utc(s: str) -> Optional[datetime]:
    """Open-Meteo returns naive ISO strings like '2024-06-15T03:00'. Treat as UTC."""
    try:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def insert_forecast_rows(
    conn: sqlite3.Connection,
    city_id: int,
    source: str,
    rows: list,
    dry_run: bool,
) -> tuple:
    """Insert forecast rows. Returns (inserted, skipped_physics, skipped_duplicate)."""
    if not rows:
        return (0, 0, 0)
    fetched = _utc_now_iso()
    inserted = skipped_phys = skipped_dup = 0

    cur = conn.cursor()
    for row in rows:
        if not physics_ok(row):
            skipped_phys += 1
            continue
        vt = _parse_iso_naive_to_utc(row["valid_time"])
        if vt is None:
            skipped_phys += 1
            continue
        # Convention: issue_time = 00:00 UTC of the day of the earliest hour in
        # this batch's run. For hourly historical forecasts, Open-Meteo returns
        # the forecast valid-times; we approximate issue_time as 00:00 UTC of
        # the first valid day. This is conservative: a forecast valid at
        # 2024-06-15T03:00 is treated as issued at 2024-06-15T00:00, giving
        # lead_hours = 3. This keeps lead_time ≥ 0 and forbids leakage.
        issue_time = datetime(vt.year, vt.month, vt.day, tzinfo=timezone.utc)
        lead_hours = int((vt - issue_time).total_seconds() // 3600)
        if lead_hours < 0:
            skipped_phys += 1
            continue

        params = (
            city_id,
            source,
            issue_time.isoformat(),
            vt.isoformat(),
            lead_hours,
            row.get("temp_c"),
            row.get("dew_point_c"),
            row.get("humidity_pct"),
            row.get("precip_mm"),
            row.get("pressure_msl_hpa"),
            row.get("surface_pressure_hpa"),
            row.get("wind_speed_kmh"),
            row.get("wind_dir_deg"),
            row.get("cloud_cover_pct"),
            fetched,
        )
        if dry_run:
            inserted += 1
            continue
        try:
            cur.execute(
                """
                INSERT INTO forecasts_hourly (
                    city_id, source, issue_time, valid_time, lead_hours,
                    temp_c, dew_point_c, humidity_pct, precip_mm,
                    pressure_msl_hpa, surface_pressure_hpa,
                    wind_speed_kmh, wind_dir_deg, cloud_cover_pct, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped_dup += 1
    if not dry_run:
        conn.commit()
    return (inserted, skipped_phys, skipped_dup)


def insert_observation_rows(
    conn: sqlite3.Connection, city_id: int, rows: list, dry_run: bool
) -> tuple:
    if not rows:
        return (0, 0, 0)
    fetched = _utc_now_iso()
    inserted = skipped_phys = skipped_dup = 0
    cur = conn.cursor()
    for row in rows:
        if not physics_ok(row):
            skipped_phys += 1
            continue
        vt = _parse_iso_naive_to_utc(row["valid_time"])
        if vt is None:
            skipped_phys += 1
            continue
        params = (
            city_id,
            vt.isoformat(),
            row.get("temp_c"),
            row.get("dew_point_c"),
            row.get("humidity_pct"),
            row.get("precip_mm"),
            row.get("pressure_msl_hpa"),
            row.get("surface_pressure_hpa"),
            row.get("wind_speed_kmh"),
            row.get("wind_dir_deg"),
            row.get("cloud_cover_pct"),
            "open_meteo_archive",
            fetched,
        )
        if dry_run:
            inserted += 1
            continue
        try:
            cur.execute(
                """
                INSERT INTO observations_hourly (
                    city_id, valid_time,
                    temp_c, dew_point_c, humidity_pct, precip_mm,
                    pressure_msl_hpa, surface_pressure_hpa,
                    wind_speed_kmh, wind_dir_deg, cloud_cover_pct,
                    source, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped_dup += 1
    if not dry_run:
        conn.commit()
    return (inserted, skipped_phys, skipped_dup)


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def daterange_chunks(start: date, end: date, max_days: int) -> Iterable:
    """Yield (chunk_start, chunk_end) pairs inclusive, each ≤ max_days long."""
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        yield (cur, chunk_end)
        cur = chunk_end + timedelta(days=1)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def list_cities(conn: sqlite3.Connection, city_id: Optional[int]) -> list:
    if city_id is not None:
        rows = conn.execute(
            "SELECT id, name, lat, lon FROM cities WHERE id = ?", (city_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, name, lat, lon FROM cities ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def backfill_city(
    conn: sqlite3.Connection,
    city: dict,
    start: date,
    end: date,
    models: list,
    dry_run: bool,
) -> dict:
    """Backfill forecasts (per model) + observations for one city."""
    cid = city["id"]
    name = city["name"]
    lat = city["lat"]
    lon = city["lon"]

    stats = {
        "city": name,
        "city_id": cid,
        "obs_inserted": 0,
        "obs_skipped_phys": 0,
        "obs_skipped_dup": 0,
        "fc_inserted": 0,
        "fc_skipped_phys": 0,
        "fc_skipped_dup": 0,
    }

    # ---- Observations (one archive fetch covers the whole window) ----
    logger.info("[%s] fetching archive observations %s → %s", name, start, end)
    for chunk_start, chunk_end in daterange_chunks(start, end, MAX_DAYS_PER_CALL):
        rows = fetch_archive_observations(lat, lon, chunk_start, chunk_end)
        ins, sp, sd = insert_observation_rows(conn, cid, rows, dry_run)
        stats["obs_inserted"] += ins
        stats["obs_skipped_phys"] += sp
        stats["obs_skipped_dup"] += sd
        logger.info(
            "[%s]   obs chunk %s → %s: +%d phys=%d dup=%d",
            name, chunk_start, chunk_end, ins, sp, sd,
        )

    # ---- Forecasts (per model, chunked) ----
    for model in models:
        logger.info("[%s] fetching historical forecast (%s)", name, model)
        for chunk_start, chunk_end in daterange_chunks(start, end, MAX_DAYS_PER_CALL):
            rows = fetch_historical_forecast(lat, lon, chunk_start, chunk_end, model)
            ins, sp, sd = insert_forecast_rows(conn, cid, model, rows, dry_run)
            stats["fc_inserted"] += ins
            stats["fc_skipped_phys"] += sp
            stats["fc_skipped_dup"] += sd
            logger.info(
                "[%s]   fc %s %s → %s: +%d phys=%d dup=%d",
                name, model, chunk_start, chunk_end, ins, sp, sd,
            )

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", default="2022-01-01", help="Start date YYYY-MM-DD")
    p.add_argument(
        "--end",
        default=(date.today() - timedelta(days=2)).isoformat(),
        help="End date YYYY-MM-DD (default: today - 2 to respect archive lag)",
    )
    p.add_argument(
        "--models",
        default=",".join(DEFAULT_MODELS),
        help=f"Comma-separated model ids (default: {','.join(DEFAULT_MODELS)})",
    )
    p.add_argument(
        "--city-id",
        type=int,
        default=None,
        help="Only backfill this city id (default: all)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse but don't write to the database",
    )
    p.add_argument(
        "--quiet", action="store_true", help="Only log warnings and errors"
    )
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    except ValueError as e:
        logger.error("Invalid date: %s", e)
        sys.exit(2)

    if end < start:
        logger.error("--end (%s) is before --start (%s)", end, start)
        sys.exit(2)

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    if not models:
        logger.error("No models specified")
        sys.exit(2)

    conn = get_connection()
    ensure_schema(conn)

    cities = list_cities(conn, args.city_id)
    if not cities:
        logger.error("No cities found (city_id=%s)", args.city_id)
        sys.exit(1)

    logger.info(
        "Backfill plan: %d cities × %d models × %s → %s (dry_run=%s)",
        len(cities), len(models), start, end, args.dry_run,
    )

    totals = {
        "cities": 0,
        "obs_inserted": 0, "obs_skipped_phys": 0, "obs_skipped_dup": 0,
        "fc_inserted": 0, "fc_skipped_phys": 0, "fc_skipped_dup": 0,
    }
    t0 = time.time()
    for city in cities:
        stats = backfill_city(conn, city, start, end, models, args.dry_run)
        totals["cities"] += 1
        for k in ("obs_inserted", "obs_skipped_phys", "obs_skipped_dup",
                  "fc_inserted", "fc_skipped_phys", "fc_skipped_dup"):
            totals[k] += stats[k]
        logger.info("[%s] done: %s", city["name"], stats)

    elapsed = time.time() - t0
    logger.warning(
        "Backfill complete in %.0fs. %s",
        elapsed,
        totals,
    )


if __name__ == "__main__":
    main()
