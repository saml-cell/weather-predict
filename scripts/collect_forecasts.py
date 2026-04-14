#!/usr/bin/env python3
"""
Forecast Collection Pipeline — fetch and store predictions for all tracked cities.

Run on a schedule (e.g., every 6 hours via cron/Task Scheduler):
  python scripts/collect_forecasts.py

Or for a single city:
  python scripts/collect_forecasts.py --city Bratislava
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from db import normalize_condition
from weighted_forecast import fetch_all_sources as _fetch_all_sources


def fetch_all_sources(city):
    """Fetch from all available sources in parallel for a city.

    Thin wrapper around weighted_forecast.fetch_all_sources that discards
    the source_status dict (not needed for collection).
    """
    results, _status = _fetch_all_sources(city)
    return results


def store_forecasts(city_id, results, fetched_at):
    """Store all forecast data from all sources into the database."""
    rows = []
    today = fetched_at[:10] if fetched_at else None
    for result in results:
        source_name = result["source"]
        current = result.get("current", {})

        for day in result.get("daily", []):
            is_today = day.get("date") == today
            rows.append((
                city_id,
                source_name,
                fetched_at,
                day.get("date"),
                day.get("high_c"),
                day.get("low_c"),
                day.get("precip_prob"),
                day.get("precip_mm"),
                day.get("wind_max_kmh"),
                normalize_condition(day.get("condition", "")),
                current.get("pressure_hpa") if is_today else None,
                current.get("humidity") if is_today else None,
                json.dumps(day),
            ))

    if rows:
        db.insert_forecasts_batch(rows)
    return len(rows)


def collect_all(city_filter=None):
    """Main collection loop."""
    cities = db.get_all_cities()
    if city_filter:
        cities = [c for c in cities if c["name"].lower() == city_filter.lower()]
        if not cities:
            print(f"City not found in database: {city_filter}")
            sys.exit(1)

    fetched_at = datetime.now(timezone.utc).isoformat()
    total_rows = 0
    total_sources = 0

    for city in cities:
        print(f"\n{city['name']}, {city['country']}:")
        results = fetch_all_sources(city)

        sources = [r["source"] for r in results]
        print(f"  Sources: {', '.join(sources) if sources else 'NONE'}")

        count = store_forecasts(city["id"], results, fetched_at)
        print(f"  Stored: {count} forecast rows")

        total_rows += count
        total_sources += len(results)

    print(f"\n--- Summary ---")
    print(f"Cities: {len(cities)}")
    print(f"Total source responses: {total_sources}")
    print(f"Total forecast rows stored: {total_rows}")
    print(f"Timestamp: {fetched_at}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Collect weather forecasts")
    parser.add_argument("--city", help="Collect for a single city only")
    args = parser.parse_args()
    collect_all(city_filter=args.city)


if __name__ == "__main__":
    main()
