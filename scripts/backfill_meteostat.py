#!/usr/bin/env python3
"""
Backfill hourly station observations from Meteostat as an independent ground
truth, replacing Open-Meteo's ERA5 archive which has known circularity issues
with ECMWF-family forecast models.

See wiki page era5-ground-truth-gotcha for the full explanation of why this
matters. Short version: ERA5 reanalysis ingests ECMWF IFS forecasts during
data assimilation, so scoring an ECMWF-family MOS against ERA5 is degenerate.
Meteostat aggregates real station observations from NOAA GHCN, DWD, MeteoFrance,
ECCC, JMA, etc. — genuinely independent of forecast models.

Design notes:
- Pulls from the nearest Meteostat weather station(s) to each city's lat/lon.
- Stores raw station data plus station metadata (distance, elevation).
- Handles missing data gracefully: logs which cities are station-poor and
  lets the trainer decide what to do with them.
- Physics ingress gate (same as backfill_historical_forecasts.py) runs on every
  row — rejects humidity > 100, T < Td, precip < 0, pressure out of range.
- Idempotent: UNIQUE constraint on (city_id, valid_time, station_id).

Install: .venv/bin/pip install meteostat

Usage:
    .venv/bin/python scripts/backfill_meteostat.py \\
        --start 2022-01-01 \\
        --end 2026-04-10

    .venv/bin/python scripts/backfill_meteostat.py --city-id 1 --dry-run
"""

import argparse
import logging
import math
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection, DB_PATH  # type: ignore

logger = logging.getLogger("backfill_meteostat")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS observations_hourly_station (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT,
    station_distance_km REAL,
    station_elevation_m REAL,
    valid_time TEXT NOT NULL,
    temp_c REAL,
    dew_point_c REAL,
    humidity_pct REAL,
    precip_mm REAL,
    pressure_msl_hpa REAL,
    wind_speed_kmh REAL,
    wind_dir_deg REAL,
    wind_peak_gust_kmh REAL,
    fetched_at TEXT NOT NULL,
    FOREIGN KEY (city_id) REFERENCES cities (id) ON DELETE CASCADE,
    UNIQUE (city_id, valid_time, station_id)
);

CREATE INDEX IF NOT EXISTS idx_ohs_city_valid
    ON observations_hourly_station (city_id, valid_time);

CREATE INDEX IF NOT EXISTS idx_ohs_station
    ON observations_hourly_station (station_id);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


# ---------------------------------------------------------------------------
# Physics invariants (same as backfill_historical_forecasts.py)
# ---------------------------------------------------------------------------

MAX_REASONABLE_PRESSURE_HPA = 1150.0
MIN_REASONABLE_PRESSURE_HPA = 850.0


def _magnus_dew_point(temp_c: float, rh_pct: float) -> Optional[float]:
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
    t = row.get("temp_c")
    rh = row.get("humidity_pct")
    td = row.get("dew_point_c")
    p = row.get("precip_mm")
    pr_msl = row.get("pressure_msl_hpa")
    if rh is not None and (rh < 0 or rh > 100.5):
        return False
    if p is not None and p < 0:
        return False
    if pr_msl is not None and not (
        MIN_REASONABLE_PRESSURE_HPA <= pr_msl <= MAX_REASONABLE_PRESSURE_HPA
    ):
        return False
    if t is not None and rh is not None:
        td_calc = _magnus_dew_point(t, rh)
        if td_calc is not None and t + 0.3 < td_calc:
            return False
        if td is not None and t + 0.3 < td:
            return False
    return True


# ---------------------------------------------------------------------------
# Meteostat lookups
# ---------------------------------------------------------------------------

def _meteostat_imports():
    """Lazy import. Meteostat v2.x API."""
    try:
        from meteostat import hourly, stations, Point, Parameter, config  # type: ignore
    except ImportError as e:
        logger.error(
            "meteostat is not installed or version mismatch (need >=2.0). "
            "Install into project venv: .venv/bin/pip install 'meteostat>=2.0'"
        )
        logger.error("Import error: %s", e)
        sys.exit(2)
    # By default meteostat v2 blocks hourly requests > 3 years. We want the
    # full 4+ year window, so disable that guard.
    try:
        config.block_large_requests = False
    except Exception:
        pass
    return hourly, stations, Point, Parameter


# Parameters to request from Meteostat. We always want these; missing values
# come back as NaN and are filtered by physics_ok().
_METEOSTAT_PARAMS = None  # lazy-initialised on first use


def _get_params():
    global _METEOSTAT_PARAMS
    if _METEOSTAT_PARAMS is None:
        _, _, _, Parameter = _meteostat_imports()
        _METEOSTAT_PARAMS = [
            Parameter.TEMP, Parameter.DWPT, Parameter.RHUM,
            Parameter.PRCP, Parameter.PRES,
            Parameter.WSPD, Parameter.WDIR, Parameter.WPGT,
        ]
    return _METEOSTAT_PARAMS


def _normalise_col(c):
    """Meteostat v2 sometimes returns Parameter enums as column keys. Coerce to str."""
    if hasattr(c, "value"):
        return c.value
    return str(c)


def fetch_station_observations(city: dict, start: date, end: date) -> list:
    """Pull hourly station data for a city from Meteostat v2.x.

    Strategy: find the 5 nearest stations, pick the first one (nearest) that
    actually returns data in the requested window. Return a list of row dicts.
    """
    hourly, stations, Point, _ = _meteostat_imports()

    lat = city["lat"]
    lon = city["lon"]

    # 1. Find nearby stations. Default radius 50 km, widen if needed.
    point = Point(lat, lon)
    nearby = stations.nearby(point, 50000, 5)
    if nearby is None or len(nearby) == 0:
        # Try a wider radius for station-poor areas (mountain towns, remote coast)
        nearby = stations.nearby(point, 200000, 5)
    if nearby is None or len(nearby) == 0:
        logger.warning(
            "[%s] no Meteostat stations within 200 km of (%s, %s)",
            city["name"], lat, lon,
        )
        return []

    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end, datetime.max.time())

    # 2. Try stations in order of distance until one returns data.
    chosen_id = None
    chosen_row = None
    chosen_df = None
    for station_id, station_row in nearby.iterrows():
        try:
            ts = hourly(station_id, start_dt, end_dt, parameters=_get_params())
        except Exception as e:
            logger.warning(
                "[%s] station %s hourly() raised: %s", city["name"], station_id, e,
            )
            continue
        if ts is None or ts.empty:
            continue
        df = ts.fetch()
        if df is None or df.empty:
            continue
        chosen_id = station_id
        chosen_row = station_row
        chosen_df = df
        break

    if chosen_df is None:
        logger.warning(
            "[%s] no nearby station has hourly data in [%s, %s]",
            city["name"], start, end,
        )
        return []

    # Normalise column names (Parameter enums → strings)
    chosen_df = chosen_df.copy()
    chosen_df.columns = [_normalise_col(c) for c in chosen_df.columns]

    station_name = str(chosen_row.get("name", "") or "")
    station_elev = chosen_row.get("elevation")
    station_elev = float(station_elev) if station_elev is not None and not _is_nan(station_elev) else 0.0
    station_lat = float(chosen_row.get("latitude", lat))
    station_lon = float(chosen_row.get("longitude", lon))
    distance_km = _haversine_km(lat, lon, station_lat, station_lon)

    logger.info(
        "[%s] station=%s (%s) distance=%.1f km elev=%.0f m rows=%d",
        city["name"], chosen_id, station_name, distance_km, station_elev, len(chosen_df),
    )

    rows = []
    for ts_idx, r in chosen_df.iterrows():
        # Meteostat returns a naive UTC DatetimeIndex.
        if ts_idx.tzinfo is None:
            valid_time = ts_idx.tz_localize("UTC")
        else:
            valid_time = ts_idx.tz_convert("UTC")

        row = {
            "station_id": str(chosen_id),
            "station_name": station_name,
            "station_distance_km": distance_km,
            "station_elevation_m": station_elev,
            "valid_time": valid_time.isoformat(),
            "temp_c": _nan_to_none(r.get("temp")),
            "dew_point_c": _nan_to_none(r.get("dwpt")),
            "humidity_pct": _nan_to_none(r.get("rhum")),
            "precip_mm": _nan_to_none(r.get("prcp")),
            "pressure_msl_hpa": _nan_to_none(r.get("pres")),
            "wind_speed_kmh": _nan_to_none(r.get("wspd")),
            "wind_dir_deg": _nan_to_none(r.get("wdir")),
            "wind_peak_gust_kmh": _nan_to_none(r.get("wpgt")),
        }
        rows.append(row)
    return rows


def _is_nan(v) -> bool:
    try:
        return isinstance(v, float) and math.isnan(v)
    except (TypeError, ValueError):
        return False


def _nan_to_none(v):
    """Convert pandas NaN to Python None."""
    try:
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Insertion
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def insert_rows(
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
        params = (
            city_id,
            row["station_id"],
            row["station_name"],
            row["station_distance_km"],
            row["station_elevation_m"],
            row["valid_time"],
            row.get("temp_c"),
            row.get("dew_point_c"),
            row.get("humidity_pct"),
            row.get("precip_mm"),
            row.get("pressure_msl_hpa"),
            row.get("wind_speed_kmh"),
            row.get("wind_dir_deg"),
            row.get("wind_peak_gust_kmh"),
            fetched,
        )
        if dry_run:
            inserted += 1
            continue
        try:
            cur.execute(
                """
                INSERT INTO observations_hourly_station (
                    city_id, station_id, station_name, station_distance_km,
                    station_elevation_m, valid_time,
                    temp_c, dew_point_c, humidity_pct, precip_mm,
                    pressure_msl_hpa, wind_speed_kmh, wind_dir_deg,
                    wind_peak_gust_kmh, fetched_at
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


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", default="2022-01-01")
    p.add_argument(
        "--end",
        default=(date.today() - timedelta(days=2)).isoformat(),
    )
    p.add_argument("--city-id", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--quiet", action="store_true")
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

    conn = get_connection()
    ensure_schema(conn)
    cities = list_cities(conn, args.city_id)
    if not cities:
        logger.error("No cities found")
        sys.exit(1)

    logger.info(
        "Meteostat backfill plan: %d cities × %s → %s (dry_run=%s)",
        len(cities), start, end, args.dry_run,
    )

    totals = {"cities": 0, "stations_with_data": 0, "inserted": 0, "skipped_phys": 0, "skipped_dup": 0}
    t0 = time.time()
    for city in cities:
        logger.info("=" * 50)
        logger.info("[%s] fetching Meteostat station obs", city["name"])
        try:
            rows = fetch_station_observations(city, start, end)
        except Exception as e:
            logger.exception("[%s] Meteostat fetch failed: %s", city["name"], e)
            continue
        if rows:
            totals["stations_with_data"] += 1
        ins, sp, sd = insert_rows(conn, city["id"], rows, args.dry_run)
        totals["cities"] += 1
        totals["inserted"] += ins
        totals["skipped_phys"] += sp
        totals["skipped_dup"] += sd
        logger.info(
            "[%s] done: inserted=%d skipped_phys=%d skipped_dup=%d",
            city["name"], ins, sp, sd,
        )

    elapsed = time.time() - t0
    logger.warning(
        "Meteostat backfill complete in %.0fs. %s", elapsed, totals
    )


if __name__ == "__main__":
    main()
