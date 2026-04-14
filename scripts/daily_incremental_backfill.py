#!/usr/bin/env python3
"""
Daily incremental backfill — pulls the last 3 days of historical forecasts
(Open-Meteo) and station observations (Meteostat) for all tracked cities.

Runs at 05:00 UTC, one hour before the daily MOS shadow verify cron at 06:00.
Without this, the verification loop verifies stale data and writes 0 rows —
the silent failure mode that round 3 of the weather forecasting council
(see wiki/Projects/weather-predict-council-round3.md) was convened to fix.

Window: end = today - 1, start = today - 3 (3 days inclusive). Both
underlying scripts are idempotent thanks to UNIQUE constraints, so a
3-day rolling window patches any partial fetches from prior runs.

Usage:
    .venv/bin/python scripts/daily_incremental_backfill.py
    .venv/bin/python scripts/daily_incremental_backfill.py --dry-run
    .venv/bin/python scripts/daily_incremental_backfill.py --quiet
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import date, timedelta

logger = logging.getLogger("daily_incremental_backfill")

_PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_VENV_PY = os.path.join(_PROJECT, ".venv", "bin", "python")


def run(cmd: list) -> int:
    logger.info("$ %s", " ".join(cmd))
    r = subprocess.run(cmd, cwd=_PROJECT)
    return r.returncode


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true",
                   help="Pass --dry-run to underlying backfills")
    p.add_argument("--quiet", action="store_true",
                   help="Only log warnings and errors")
    p.add_argument("--window-days", type=int, default=3,
                   help="Days to backfill, ending yesterday (default: 3)")
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    end_d = (date.today() - timedelta(days=1)).isoformat()
    start_d = (date.today() - timedelta(days=args.window_days)).isoformat()
    logger.info("Daily incremental backfill window: %s → %s", start_d, end_d)

    common = ["--start", start_d, "--end", end_d, "--quiet"]
    if args.dry_run:
        common.append("--dry-run")

    rc1 = run(["python3", "scripts/backfill_historical_forecasts.py", *common])
    if rc1 != 0:
        logger.error("historical forecast backfill failed (rc=%d)", rc1)

    rc2 = run([_VENV_PY, "scripts/backfill_meteostat.py", *common])
    if rc2 != 0:
        logger.error("meteostat backfill failed (rc=%d)", rc2)

    if rc1 != 0 or rc2 != 0:
        sys.exit(1)
    logger.warning("Daily incremental backfill complete: %s → %s", start_d, end_d)


if __name__ == "__main__":
    main()
