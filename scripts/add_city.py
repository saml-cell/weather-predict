#!/usr/bin/env python3
"""
Add cities to the weather tracking database.

Usage:
  python scripts/add_city.py Bratislava "New York" London Tokyo Sydney
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from fetch_weather import geocode


def add_city(city_name):
    """Resolve and add a city. Returns (city_id, was_new)."""
    location = geocode(city_name)
    if not location:
        print(f"  [!!] Could not find: {city_name}")
        return None, False

    existing = db.get_city(location["name"])
    if existing:
        print(f"  [--] Already tracked: {location['name']}, {location['country']} "
              f"({location['lat']}, {location['lon']})")
        return existing["id"], False

    city_id = db.insert_city(
        name=location["name"],
        country=location["country"],
        lat=location["lat"],
        lon=location["lon"],
        timezone=location.get("timezone", "auto"),
    )
    print(f"  [OK] Added: {location['name']}, {location['country']} "
          f"({location['lat']}, {location['lon']}) -> id={city_id}")
    return city_id, True


def main():
    if len(sys.argv) < 2:
        print("Usage: python add_city.py <city1> [city2] [city3] ...")
        sys.exit(1)

    cities = sys.argv[1:]
    added = 0
    for city_name in cities:
        city_id, was_new = add_city(city_name)
        if was_new:
            added += 1

    total = len(db.get_all_cities())
    print(f"\nAdded {added} new cities. Total tracked: {total}")


if __name__ == "__main__":
    main()
