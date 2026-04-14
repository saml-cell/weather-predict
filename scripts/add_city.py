#!/usr/bin/env python3
"""
Add cities to the weather tracking database.

Usage:
  python scripts/add_city.py Bratislava "New York" London Tokyo Sydney
  python scripts/add_city.py --skip-backfill ExperimentCity

After insert, this script SYNCHRONOUSLY runs Meteostat historical backfill for
the new city and refuses to declare success unless at least one observation row
landed. This closes the silent failure mode where new cities were forecast
without any verification loop (Round 4 Step 1 / Round 4.5 Step 2).
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from fetch_weather import geocode

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PY = PROJECT_ROOT / ".venv" / "bin" / "python"
BACKFILL_SCRIPT = PROJECT_ROOT / "scripts" / "backfill_meteostat.py"
NEW_CITY_AUDIT_LOG = PROJECT_ROOT / "data" / "new_city_audit.jsonl"


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


def count_station_obs(city_id):
    """Return number of observations_hourly_station rows for the city."""
    conn = db.get_connection()
    cur = conn.execute(
        "SELECT COUNT(*) FROM observations_hourly_station WHERE city_id = ?",
        (city_id,),
    )
    return cur.fetchone()[0]


def run_meteostat_backfill(city_id, quiet=False):
    """Synchronous Meteostat backfill for a single new city."""
    if not VENV_PY.exists():
        raise RuntimeError(f"venv python not found at {VENV_PY}")
    cmd = [
        str(VENV_PY),
        str(BACKFILL_SCRIPT),
        "--city-id", str(city_id),
    ]
    if quiet:
        cmd.append("--quiet")
    print(f"  [..] Meteostat backfill: {' '.join(cmd[-3:])}")
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return result.returncode == 0


def append_audit(record):
    NEW_CITY_AUDIT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with NEW_CITY_AUDIT_LOG.open("a") as f:
        f.write(json.dumps(record) + "\n")


def verify_and_backfill(city_id, city_name, skip_backfill=False):
    """Run Meteostat backfill and verify obs rows landed.

    Raises RuntimeError if backfill produced zero observation rows and
    skip_backfill is False. Always writes an audit log entry.
    """
    audit = {
        "city_id": city_id,
        "name": city_name,
        "added_at": datetime.now(timezone.utc).isoformat(),
        "skip_backfill": skip_backfill,
    }
    if skip_backfill:
        audit["status"] = "skipped_backfill"
        audit["obs_rows_after"] = count_station_obs(city_id)
        append_audit(audit)
        print(f"  [!!] --skip-backfill set; this city has NO observation history "
              f"and will appear cold-start on the /skill panel until obs flow.")
        return

    obs_before = count_station_obs(city_id)
    ok = run_meteostat_backfill(city_id, quiet=False)
    obs_after = count_station_obs(city_id)
    delta = obs_after - obs_before

    audit.update({
        "obs_rows_before": obs_before,
        "obs_rows_after": obs_after,
        "obs_rows_inserted": delta,
        "backfill_returncode_ok": ok,
    })

    if delta == 0:
        audit["status"] = "FAIL_no_station_data"
        append_audit(audit)
        raise RuntimeError(
            f"Meteostat backfill for city_id={city_id} ({city_name}) inserted "
            f"zero rows. This city has no nearby Meteostat station — it will "
            f"forecast forever without a verification loop. Use "
            f"--skip-backfill to override (NOT recommended). City row is in "
            f"the database; remove it manually if this was unintended."
        )

    audit["status"] = "OK"
    append_audit(audit)
    print(f"  [OK] {delta} station obs rows inserted for {city_name}.")
    print(f"       The next 06:00 UTC mos_shadow_verify cycle will compute "
          f"the first skill row. Watch the /api/mos/skill panel; the city "
          f"should transition cold-start → low-history within 24h.")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("cities", nargs="+", help="City names to add")
    p.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Skip Meteostat backfill and skill-loop verification. "
             "Only use for intentional cold-start experiments — the city will "
             "have no obs history and will appear in the cold-start warning "
             "card on the dashboard until obs flow some other way.",
    )
    return p.parse_args()


def main():
    args = parse_args()

    added = 0
    failures = []
    for city_name in args.cities:
        city_id, was_new = add_city(city_name)
        if city_id is None:
            continue
        if not was_new:
            continue
        try:
            verify_and_backfill(city_id, city_name, skip_backfill=args.skip_backfill)
            added += 1
        except RuntimeError as e:
            print(f"  [FAIL] {e}")
            failures.append((city_name, str(e)))

    total = len(db.get_all_cities())
    print(f"\nAdded {added} new cities. Total tracked: {total}")
    if failures:
        print(f"\n{len(failures)} city/cities FAILED post-add verification:")
        for name, err in failures:
            print(f"  - {name}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
