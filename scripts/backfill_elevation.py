#!/usr/bin/env python3
"""
One-shot backfill of cities.elevation_m via Open-Meteo's free elevation API.

Adds the column if missing, then fetches elevation for each city with NULL
or missing elevation. Idempotent — re-run safely.

    https://api.open-meteo.com/v1/elevation?latitude=&longitude=
"""

import json
import logging
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection  # type: ignore

logger = logging.getLogger("backfill_elevation")
API = "https://api.open-meteo.com/v1/elevation"


def ensure_column(conn: sqlite3.Connection) -> None:
    cols = [r[1] for r in conn.execute("PRAGMA table_info(cities)").fetchall()]
    if "elevation_m" not in cols:
        logger.info("adding elevation_m column to cities")
        conn.execute("ALTER TABLE cities ADD COLUMN elevation_m REAL")
        conn.commit()


def fetch_elevation(lat: float, lon: float) -> float | None:
    params = {"latitude": lat, "longitude": lon}
    url = f"{API}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            d = json.loads(resp.read())
        elev = d.get("elevation", [None])
        if isinstance(elev, list) and elev:
            return float(elev[0])
        if isinstance(elev, (int, float)):
            return float(elev)
    except Exception as e:
        logger.warning("elevation fetch failed for (%s, %s): %s", lat, lon, e)
    return None


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = get_connection()
    ensure_column(conn)
    cities = conn.execute(
        "SELECT id, name, lat, lon, elevation_m FROM cities ORDER BY id"
    ).fetchall()
    updated = 0
    for c in cities:
        if c["elevation_m"] is not None:
            logger.info("[%s] already has elevation %.1f m", c["name"], c["elevation_m"])
            continue
        elev = fetch_elevation(c["lat"], c["lon"])
        if elev is None:
            logger.warning("[%s] no elevation returned", c["name"])
            continue
        conn.execute(
            "UPDATE cities SET elevation_m = ? WHERE id = ?", (elev, c["id"])
        )
        conn.commit()
        logger.info("[%s] elevation = %.1f m", c["name"], elev)
        updated += 1
        time.sleep(0.5)
    logger.warning("backfill_elevation done: %d updated", updated)


if __name__ == "__main__":
    main()
