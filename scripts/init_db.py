#!/usr/bin/env python3
"""
Initialize the SQLite database schema for the Weather Prediction System.

Creates data/weather.db with tables: cities, forecasts, observations, source_accuracy.
Safe to run multiple times (uses IF NOT EXISTS).

Usage:
  python scripts/init_db.py
"""

import os
import sys

# Add scripts dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db

def create_schema():
    conn = db.get_connection()

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cities (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            country     TEXT,
            lat         REAL NOT NULL,
            lon         REAL NOT NULL,
            timezone    TEXT DEFAULT 'auto',
            added_at    TEXT
        );

        CREATE TABLE IF NOT EXISTS forecasts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id         INTEGER NOT NULL REFERENCES cities(id),
            source_name     TEXT NOT NULL,
            fetched_at      TEXT NOT NULL,
            forecast_date   TEXT NOT NULL,
            temp_high_c     REAL CHECK (temp_high_c BETWEEN -70 AND 60 OR temp_high_c IS NULL),
            temp_low_c      REAL CHECK (temp_low_c BETWEEN -70 AND 60 OR temp_low_c IS NULL),
            precip_prob     REAL,
            precip_mm       REAL CHECK (precip_mm >= 0 OR precip_mm IS NULL),
            wind_max_kmh    REAL CHECK (wind_max_kmh >= 0 OR wind_max_kmh IS NULL),
            condition_text  TEXT,
            pressure_hpa    REAL,
            humidity_pct    REAL CHECK (humidity_pct BETWEEN 0 AND 100 OR humidity_pct IS NULL),
            raw_json        TEXT,
            UNIQUE(city_id, source_name, forecast_date)
        );

        CREATE TABLE IF NOT EXISTS observations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id         INTEGER NOT NULL REFERENCES cities(id),
            obs_date        TEXT NOT NULL,
            temp_high_c     REAL,
            temp_low_c      REAL,
            precip_mm       REAL,
            wind_max_kmh    REAL,
            condition_text  TEXT,
            pressure_hpa    REAL,
            humidity_pct    REAL,
            source          TEXT DEFAULT 'open_meteo_historical',
            fetched_at      TEXT,
            UNIQUE(city_id, obs_date)
        );

        CREATE TABLE IF NOT EXISTS source_accuracy (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id         INTEGER NOT NULL REFERENCES cities(id),
            source_name     TEXT NOT NULL,
            metric          TEXT NOT NULL,
            mae             REAL,
            accuracy_pct    REAL,
            weight          REAL DEFAULT 0.0,
            sample_count    INTEGER DEFAULT 0,
            window_days     INTEGER DEFAULT 30,
            computed_at     TEXT,
            bias            REAL DEFAULT NULL,
            lead_time_group TEXT DEFAULT NULL,
            UNIQUE(city_id, source_name, metric)
        );

        CREATE INDEX IF NOT EXISTS idx_forecasts_lookup
            ON forecasts(city_id, forecast_date, source_name);

        CREATE INDEX IF NOT EXISTS idx_forecasts_scoring
            ON forecasts(city_id, source_name, forecast_date);

        CREATE INDEX IF NOT EXISTS idx_observations_lookup
            ON observations(city_id, obs_date);

        CREATE INDEX IF NOT EXISTS idx_accuracy_lookup
            ON source_accuracy(city_id, source_name);

        -- ================================================================
        -- Seasonal forecasting tables
        -- ================================================================

        CREATE TABLE IF NOT EXISTS climate_indices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            index_name  TEXT NOT NULL,
            year        INTEGER NOT NULL,
            month       INTEGER NOT NULL,
            value       REAL NOT NULL,
            fetched_at  TEXT,
            UNIQUE(index_name, year, month)
        );

        CREATE INDEX IF NOT EXISTS idx_climate_indices_lookup
            ON climate_indices(index_name, year, month);

        CREATE TABLE IF NOT EXISTS climatology (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id         INTEGER NOT NULL REFERENCES cities(id),
            month           INTEGER NOT NULL,
            temp_high_mean  REAL,
            temp_high_std   REAL,
            temp_low_mean   REAL,
            temp_low_std    REAL,
            precip_mean     REAL,
            precip_std      REAL,
            wind_mean       REAL,
            wind_std        REAL,
            sample_years    INTEGER,
            ref_period      TEXT DEFAULT '1991-2020',
            computed_at     TEXT,
            UNIQUE(city_id, month)
        );

        CREATE TABLE IF NOT EXISTS seasonal_forecasts (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id             INTEGER NOT NULL REFERENCES cities(id),
            target_month        INTEGER NOT NULL,
            target_year         INTEGER NOT NULL,
            method              TEXT NOT NULL,
            temp_anomaly_c      REAL,
            precip_anomaly_pct  REAL,
            confidence          REAL,
            tercile_prob_bn     REAL,
            tercile_prob_nn     REAL,
            tercile_prob_an     REAL,
            details_json        TEXT,
            generated_at        TEXT,
            UNIQUE(city_id, target_year, target_month, method)
        );

        CREATE INDEX IF NOT EXISTS idx_seasonal_forecasts_lookup
            ON seasonal_forecasts(city_id, target_year, target_month);

        CREATE TABLE IF NOT EXISTS seasonal_skill (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id         INTEGER NOT NULL REFERENCES cities(id),
            method          TEXT NOT NULL,
            metric          TEXT NOT NULL,
            value           REAL,
            sample_count    INTEGER,
            computed_at     TEXT,
            UNIQUE(city_id, method, metric)
        );
    """)

    conn.close()
    print(f"Database initialized at: {db.DB_PATH}")

    # Verify tables exist
    conn = db.get_connection()
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    conn.close()
    print(f"Tables: {', '.join(t['name'] for t in tables)}")


if __name__ == "__main__":
    os.makedirs(os.path.dirname(db.DB_PATH), exist_ok=True)
    create_schema()
