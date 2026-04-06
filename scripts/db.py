#!/usr/bin/env python3
"""
Shared database helpers for the Weather Prediction System.

Centralizes all SQLite access, condition normalization, and config loading.
Every other script imports from here.
"""

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime
from datetime import timezone as _tz

logger = logging.getLogger(__name__)

def _now_utc():
    """Return current UTC time as ISO string."""
    return datetime.now(_tz.utc).isoformat()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
DB_PATH = os.path.join(_PROJECT_DIR, "data", "weather.db")
CONFIG_PATH = os.path.join(_PROJECT_DIR, "config.json")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_config_cache = None

def load_config():
    """Load config.json. Caches after first read."""
    global _config_cache
    if _config_cache is None:
        with open(CONFIG_PATH, "r") as f:
            _config_cache = json.load(f)
    return _config_cache

# ---------------------------------------------------------------------------
# Database connection pool (thread-local)
# ---------------------------------------------------------------------------
_local = threading.local()

def get_connection():
    """Return a SQLite connection with row_factory and WAL mode.

    Uses thread-local storage to reuse connections within the same thread,
    avoiding the overhead of opening/closing per call.
    """
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except sqlite3.ProgrammingError:
            _local.conn = None

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _local.conn = conn
    logger.debug("Opened new DB connection (thread %s)", threading.current_thread().name)
    return conn


def close_connection():
    """Explicitly close the thread-local connection."""
    conn = getattr(_local, "conn", None)
    if conn is not None:
        conn.close()
        _local.conn = None

# ---------------------------------------------------------------------------
# Condition normalization
# ---------------------------------------------------------------------------
# Maps raw weather descriptions from any source to canonical conditions.
# Checked longest-first to match "heavy rain" before "rain".
_CONDITION_MAP = [
    # Thunderstorms
    ("thunderstorm with heavy hail", "thunderstorm"),
    ("thunderstorm with slight hail", "thunderstorm"),
    ("thunderstorm", "thunderstorm"),
    ("thunder", "thunderstorm"),
    # Heavy precipitation
    ("violent rain showers", "heavy_rain"),
    ("heavy rain showers", "heavy_rain"),
    ("heavy rain", "heavy_rain"),
    ("heavy freezing rain", "heavy_rain"),
    ("heavy intensity rain", "heavy_rain"),
    ("torrential", "heavy_rain"),
    # Snow
    ("heavy snow showers", "snow"),
    ("heavy snow", "snow"),
    ("moderate snow showers", "snow"),
    ("moderate snow", "snow"),
    ("slight snow showers", "snow"),
    ("slight snow", "snow"),
    ("snow grains", "snow"),
    ("snow", "snow"),
    ("blizzard", "snow"),
    ("sleet", "snow"),
    # Freezing
    ("dense freezing drizzle", "drizzle"),
    ("light freezing drizzle", "drizzle"),
    ("light freezing rain", "rain"),
    # Rain
    ("moderate rain showers", "rain"),
    ("slight rain showers", "rain"),
    ("moderate rain", "rain"),
    ("slight rain", "rain"),
    ("light rain", "rain"),
    ("light intensity drizzle", "drizzle"),
    ("patchy rain", "rain"),
    ("rain shower", "rain"),
    ("rain", "rain"),
    # Drizzle
    ("dense drizzle", "drizzle"),
    ("moderate drizzle", "drizzle"),
    ("light drizzle", "drizzle"),
    ("drizzle", "drizzle"),
    ("mist", "drizzle"),
    # Fog
    ("rime fog", "fog"),
    ("fog", "fog"),
    ("haze", "fog"),
    # Cloudy
    ("overcast", "cloudy"),
    ("broken clouds", "cloudy"),
    ("cloudy", "cloudy"),
    # Partly cloudy
    ("partly cloudy", "partly_cloudy"),
    ("partly sunny", "partly_cloudy"),
    ("scattered clouds", "partly_cloudy"),
    ("few clouds", "partly_cloudy"),
    # Clear
    ("mainly clear", "clear"),
    ("clear sky", "clear"),
    ("clear", "clear"),
    ("sunny", "clear"),
]

def normalize_condition(raw):
    """Map raw weather description to canonical condition."""
    if not raw:
        return "unknown"
    lower = raw.lower().strip()
    for pattern, canonical in _CONDITION_MAP:
        if pattern in lower:
            return canonical
    return "unknown"

# ---------------------------------------------------------------------------
# City operations
# ---------------------------------------------------------------------------
def get_city(city_name):
    """Lookup city by name (case-insensitive). Returns dict or None."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM cities WHERE LOWER(name) = LOWER(?)", (city_name,)
    ).fetchone()
    return dict(row) if row else None

def get_city_by_id(city_id):
    """Lookup city by ID. Returns dict or None."""
    conn = get_connection()
    row = conn.execute("SELECT * FROM cities WHERE id = ?", (city_id,)).fetchone()
    return dict(row) if row else None

def get_all_cities():
    """Return all tracked cities as list of dicts."""
    conn = get_connection()
    rows = conn.execute("SELECT * FROM cities ORDER BY name").fetchall()
    return [dict(r) for r in rows]

def insert_city(name, country, lat, lon, timezone="auto"):
    """Insert a city. Returns city_id. Skips if lat/lon already exists."""
    conn = get_connection()
    existing = conn.execute(
        "SELECT id FROM cities WHERE ROUND(lat, 2) = ROUND(?, 2) AND ROUND(lon, 2) = ROUND(?, 2)",
        (lat, lon)
    ).fetchone()
    if existing:
        return existing["id"]
    conn.execute(
        "INSERT INTO cities (name, country, lat, lon, timezone, added_at) VALUES (?, ?, ?, ?, ?, ?)",
        (name, country, lat, lon, timezone, _now_utc())
    )
    conn.commit()
    city_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return city_id

# ---------------------------------------------------------------------------
# Forecast operations
# ---------------------------------------------------------------------------
def insert_forecast(city_id, source_name, fetched_at, forecast_date,
                    temp_high_c=None, temp_low_c=None, precip_prob=None,
                    precip_mm=None, wind_max_kmh=None, condition_text=None,
                    pressure_hpa=None, humidity_pct=None, raw_json=None):
    """Insert or replace a forecast row."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO forecasts
        (city_id, source_name, fetched_at, forecast_date,
         temp_high_c, temp_low_c, precip_prob, precip_mm,
         wind_max_kmh, condition_text, pressure_hpa, humidity_pct, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (city_id, source_name, fetched_at, forecast_date,
          temp_high_c, temp_low_c, precip_prob, precip_mm,
          wind_max_kmh, condition_text, pressure_hpa, humidity_pct, raw_json))
    conn.commit()


def insert_forecasts_batch(rows):
    """Insert multiple forecast rows in one transaction. Each row is a tuple."""
    conn = get_connection()
    conn.executemany("""
        INSERT OR REPLACE INTO forecasts
        (city_id, source_name, fetched_at, forecast_date,
         temp_high_c, temp_low_c, precip_prob, precip_mm,
         wind_max_kmh, condition_text, pressure_hpa, humidity_pct, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()


def get_forecasts_for_date(city_id, forecast_date):
    """All source predictions for a specific city+date. Returns list of dicts."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM forecasts
        WHERE city_id = ? AND forecast_date = ?
        ORDER BY source_name
    """, (city_id, forecast_date)).fetchall()

    return [dict(r) for r in rows]

def get_forecasts_in_window(city_id, source_name, start_date, end_date):
    """Get all forecasts from a source within a date range."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM forecasts
        WHERE city_id = ? AND source_name = ?
          AND forecast_date >= ? AND forecast_date <= ?
        ORDER BY forecast_date
    """, (city_id, source_name, start_date, end_date)).fetchall()

    return [dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Observation operations
# ---------------------------------------------------------------------------
def insert_observation(city_id, obs_date, temp_high_c=None, temp_low_c=None,
                       precip_mm=None, wind_max_kmh=None, condition_text=None,
                       pressure_hpa=None, humidity_pct=None, source="open_meteo_historical"):
    """Insert or replace an observation row."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO observations
        (city_id, obs_date, temp_high_c, temp_low_c, precip_mm,
         wind_max_kmh, condition_text, pressure_hpa, humidity_pct,
         source, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (city_id, obs_date, temp_high_c, temp_low_c, precip_mm,
          wind_max_kmh, condition_text, pressure_hpa, humidity_pct,
          source, _now_utc()))
    conn.commit()


def get_observation(city_id, obs_date):
    """Get observation for a specific city+date. Returns dict or None."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM observations WHERE city_id = ? AND obs_date = ?",
        (city_id, obs_date)
    ).fetchone()

    return dict(row) if row else None

def get_observations_in_window(city_id, start_date, end_date):
    """Get all observations in a date range."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM observations
        WHERE city_id = ? AND obs_date >= ? AND obs_date <= ?
        ORDER BY obs_date
    """, (city_id, start_date, end_date)).fetchall()

    return [dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Source accuracy / weights
# ---------------------------------------------------------------------------
def get_weights(city_id):
    """Load weights from source_accuracy table.

    Returns: {source_name: {metric: weight}}
    Empty dict if no data (cold start).
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM source_accuracy WHERE city_id = ?", (city_id,)
    ).fetchall()


    weights = {}
    for row in rows:
        r = dict(row)
        src = r["source_name"]
        if src not in weights:
            weights[src] = {}
        weights[src][r["metric"]] = {
            "weight": r["weight"],
            "mae": r["mae"],
            "accuracy_pct": r["accuracy_pct"],
            "sample_count": r["sample_count"],
        }
    return weights

def upsert_accuracy(city_id, source_name, metric, mae=None, accuracy_pct=None,
                    weight=0.0, sample_count=0, window_days=30,
                    bias=None, lead_time_group=None):
    """Insert or update accuracy score for a source/metric/city combo."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO source_accuracy
        (city_id, source_name, metric, mae, accuracy_pct, weight,
         sample_count, window_days, computed_at, bias, lead_time_group)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (city_id, source_name, metric, mae, accuracy_pct, weight,
          sample_count, window_days, _now_utc(),
          bias, lead_time_group))
    conn.commit()


# ---------------------------------------------------------------------------
# Climate index operations (seasonal forecasting)
# ---------------------------------------------------------------------------
def insert_climate_index(index_name, year, month, value):
    """Insert or replace a single climate index value."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO climate_indices
        (index_name, year, month, value, fetched_at)
        VALUES (?, ?, ?, ?, ?)
    """, (index_name, year, month, value, _now_utc()))
    conn.commit()


def insert_climate_indices_batch(rows):
    """Bulk insert climate index values.
    Each row: (index_name, year, month, value).
    """
    conn = get_connection()
    now = _now_utc()
    conn.executemany("""
        INSERT OR REPLACE INTO climate_indices
        (index_name, year, month, value, fetched_at)
        VALUES (?, ?, ?, ?, ?)
    """, [(r[0], r[1], r[2], r[3], now) for r in rows])
    conn.commit()


def get_climate_index(index_name, year, month):
    """Get a single index value. Returns float or None."""
    conn = get_connection()
    row = conn.execute(
        "SELECT value FROM climate_indices WHERE index_name = ? AND year = ? AND month = ?",
        (index_name, year, month)
    ).fetchone()

    return row["value"] if row else None

def get_climate_index_series(index_name, start_year=None, end_year=None):
    """Get full history for an index. Returns {(year, month): value}."""
    conn = get_connection()
    query = "SELECT year, month, value FROM climate_indices WHERE index_name = ?"
    params = [index_name]
    if start_year:
        query += " AND year >= ?"
        params.append(start_year)
    if end_year:
        query += " AND year <= ?"
        params.append(end_year)
    query += " ORDER BY year, month"
    rows = conn.execute(query, params).fetchall()

    return {(r["year"], r["month"]): r["value"] for r in rows}

def get_latest_climate_indices():
    """Return the most recent value for each index.
    Returns: {index_name: {'value': float, 'year': int, 'month': int}}
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT ci.index_name, ci.year, ci.month, ci.value, ci.fetched_at
        FROM climate_indices ci
        INNER JOIN (
            SELECT index_name, MAX(year * 100 + month) as ym
            FROM climate_indices
            GROUP BY index_name
        ) latest ON ci.index_name = latest.index_name
            AND (ci.year * 100 + ci.month) = latest.ym
    """).fetchall()

    return {r["index_name"]: {
        "value": r["value"], "year": r["year"],
        "month": r["month"], "fetched_at": r["fetched_at"]
    } for r in rows}

def get_indices_fetch_time():
    """Return the most recent fetched_at for any index, or None."""
    conn = get_connection()
    row = conn.execute(
        "SELECT MAX(fetched_at) as latest FROM climate_indices"
    ).fetchone()

    return row["latest"] if row and row["latest"] else None

# ---------------------------------------------------------------------------
# Climatology operations
# ---------------------------------------------------------------------------
def insert_climatology(city_id, month, stats):
    """Insert or replace climatology for a city/month.
    stats: dict with temp_high_mean, temp_high_std, temp_low_mean, temp_low_std,
           precip_mean, precip_std, wind_mean, wind_std, sample_years.
    """
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO climatology
        (city_id, month, temp_high_mean, temp_high_std, temp_low_mean, temp_low_std,
         precip_mean, precip_std, wind_mean, wind_std, sample_years, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (city_id, month,
          stats.get("temp_high_mean"), stats.get("temp_high_std"),
          stats.get("temp_low_mean"), stats.get("temp_low_std"),
          stats.get("precip_mean"), stats.get("precip_std"),
          stats.get("wind_mean"), stats.get("wind_std"),
          stats.get("sample_years"),
          _now_utc()))
    conn.commit()


def get_climatology(city_id):
    """Load climatology for all 12 months. Returns {month: dict} or {}."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM climatology WHERE city_id = ? ORDER BY month",
        (city_id,)
    ).fetchall()

    return {r["month"]: dict(r) for r in rows}

def has_climatology(city_id):
    """Check if climatology exists for this city (all 12 months)."""
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM climatology WHERE city_id = ?",
        (city_id,)
    ).fetchone()

    return row["cnt"] >= 12

# ---------------------------------------------------------------------------
# Seasonal forecast operations
# ---------------------------------------------------------------------------
def insert_seasonal_forecast(city_id, target_year, target_month, method, forecast):
    """Insert or replace a seasonal forecast.
    forecast: dict with temp_anomaly_c, precip_anomaly_pct, confidence,
              tercile_prob_bn, tercile_prob_nn, tercile_prob_an, details_json.
    """
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO seasonal_forecasts
        (city_id, target_year, target_month, method,
         temp_anomaly_c, precip_anomaly_pct, confidence,
         tercile_prob_bn, tercile_prob_nn, tercile_prob_an,
         details_json, generated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (city_id, target_year, target_month, method,
          forecast.get("temp_anomaly_c"), forecast.get("precip_anomaly_pct"),
          forecast.get("confidence"),
          forecast.get("tercile_prob_bn"), forecast.get("tercile_prob_nn"),
          forecast.get("tercile_prob_an"),
          forecast.get("details_json"),
          _now_utc()))
    conn.commit()


def get_seasonal_forecasts(city_id, target_year, target_month):
    """Get all method forecasts for a city/year/month. Returns list of dicts."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM seasonal_forecasts
        WHERE city_id = ? AND target_year = ? AND target_month = ?
        ORDER BY method
    """, (city_id, target_year, target_month)).fetchall()

    return [dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Seasonal skill operations
# ---------------------------------------------------------------------------
def upsert_seasonal_skill(city_id, method, metric, value, sample_count=0):
    """Insert or replace a seasonal skill score."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO seasonal_skill
        (city_id, method, metric, value, sample_count, computed_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (city_id, method, metric, value, sample_count,
          _now_utc()))
    conn.commit()

