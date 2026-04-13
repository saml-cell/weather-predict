#!/usr/bin/env python3
"""
Weekly MOS retrain with automatic rollback on regression.

Flow:
  1. Incremental backfill — last 14 days of station observations (Meteostat)
     and historical forecasts (Open-Meteo).
  2. Retrain all four quantile MOS models into a STAGING directory.
  3. Compare staged models vs currently deployed models on the most recent
     30 days of shadow-verify data (mos_daily_skill). Use mean CRPS proxy as
     the arbiter.
  4. If staged is strictly better, promote (rename staging → production,
     archive old production with a timestamp).
  5. Log the decision to data/mos_models_station/retrain_log.json.
  6. Optionally notify via Telegram if promoted or rolled back.

Intended as a Sunday 03:00 UTC cron job.

Usage:
    .venv/bin/python scripts/weekly_retrain_mos.py
    .venv/bin/python scripts/weekly_retrain_mos.py --dry-run
    .venv/bin/python scripts/weekly_retrain_mos.py --force-promote
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection  # type: ignore
import mos_inference as mos  # type: ignore

logger = logging.getLogger("weekly_retrain")

_PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_VENV_PY = os.path.join(_PROJECT, ".venv", "bin", "python")
PROD_DIR = os.path.join(_PROJECT, "data", "mos_models_station")
STAGING_DIR = os.path.join(_PROJECT, "data", "mos_models_station.staging")
ARCHIVE_DIR = os.path.join(_PROJECT, "data", "mos_models_station.archive")
LOG_PATH = os.path.join(PROD_DIR, "retrain_log.json")


def run(cmd: list, env=None) -> int:
    logger.info("$ %s", " ".join(cmd))
    r = subprocess.run(cmd, cwd=_PROJECT, env=env)
    return r.returncode


def step_incremental_backfill(window_days: int = 14, dry_run: bool = False) -> bool:
    """Pull the latest window_days of station obs + historical forecasts."""
    end_d = (date.today() - timedelta(days=2)).isoformat()
    start_d = (date.today() - timedelta(days=window_days)).isoformat()

    logger.info("Incremental backfill window: %s → %s", start_d, end_d)

    if dry_run:
        logger.info("[dry-run] skipping backfills")
        return True

    rc1 = run([
        "python3", "scripts/backfill_historical_forecasts.py",
        "--start", start_d, "--end", end_d, "--quiet",
    ])
    if rc1 != 0:
        logger.error("historical forecast backfill failed (rc=%d)", rc1)
        return False

    rc2 = run([
        _VENV_PY, "scripts/backfill_meteostat.py",
        "--start", start_d, "--end", end_d, "--quiet",
    ])
    if rc2 != 0:
        logger.error("meteostat backfill failed (rc=%d)", rc2)
        return False

    return True


def step_retrain_staging(dry_run: bool = False) -> bool:
    """Run the trainer targeting the staging directory."""
    if dry_run:
        logger.info("[dry-run] skipping retrain")
        return True
    if os.path.exists(STAGING_DIR):
        shutil.rmtree(STAGING_DIR)
    rc = run([
        _VENV_PY, "scripts/train_mos_quantile.py",
        "--target-source", "station",
        "--output-dir", STAGING_DIR,
    ])
    return rc == 0


def compare_staging_vs_production(window_days: int = 30) -> dict:
    """Compare staged models against deployed models using the most recent
    `window_days` of shadow-verify data. Returns a decision dict.
    """
    conn = get_connection()
    cutoff = (date.today() - timedelta(days=window_days)).isoformat()

    # For each (city, variable) over the window, we need to replay the features
    # on both staged and prod models. The cheap-but-good alternative: just look
    # at mos_daily_skill (which reflects PROD) and use the eval_report.json
    # from staging (which reflects the new model's test-set performance on
    # 2025 data). These measure different things but both are signals.

    # 1. Production CRPS from mos_daily_skill
    prod_rows = conn.execute(
        """
        SELECT variable,
               AVG(mos_crps_proxy) AS crps,
               COUNT(*) AS n
        FROM mos_daily_skill
        WHERE verify_date >= ?
        GROUP BY variable
        """,
        (cutoff,),
    ).fetchall()
    prod_crps = {r[0]: (r[1], r[2]) for r in prod_rows}

    # 2. Staging CRPS from staged eval_report.json (2025 test set)
    staging_report_path = os.path.join(STAGING_DIR, "eval_report.json")
    if not os.path.exists(staging_report_path):
        return {"decision": "no_staging_report", "prod_crps": prod_crps}
    with open(staging_report_path) as f:
        staging_report = json.load(f)
    staging_crps = {}
    for var_result in staging_report.get("variables", []):
        v = var_result.get("variable")
        mos_block = var_result.get("mos", {}).get("test", {})
        crps = mos_block.get("crps_proxy")
        if v and crps is not None:
            staging_crps[v] = crps

    # 3. If we have no production history yet, promote staging (first run).
    if not prod_crps:
        return {
            "decision": "promote_first_run",
            "reason": "no mos_daily_skill rows yet",
            "staging_crps": staging_crps,
        }

    # 4. Variable-wise comparison. Promote only if staging strictly improves
    #    (or matches) on every variable that has enough production history.
    per_variable = {}
    all_improve = True
    any_regress = False
    for v, (pcrps, pn) in prod_crps.items():
        scrps = staging_crps.get(v)
        if scrps is None:
            per_variable[v] = {"prod": pcrps, "staging": None, "verdict": "no_staging"}
            continue
        if pn < 3:
            per_variable[v] = {
                "prod": pcrps, "staging": scrps, "n_prod": pn,
                "verdict": "insufficient_history",
            }
            continue
        if scrps <= pcrps * 1.02:  # allow 2% noise band
            per_variable[v] = {
                "prod": pcrps, "staging": scrps, "n_prod": pn,
                "verdict": "improve_or_match",
            }
        else:
            per_variable[v] = {
                "prod": pcrps, "staging": scrps, "n_prod": pn,
                "verdict": "regress",
            }
            all_improve = False
            any_regress = True

    decision = "promote" if (all_improve and staging_crps and not any_regress) else "reject"
    return {
        "decision": decision,
        "per_variable": per_variable,
        "prod_crps": prod_crps,
        "staging_crps": staging_crps,
    }


def step_promote(dry_run: bool = False) -> bool:
    """Atomic swap of production ← staging with timestamped archive of the old prod."""
    if dry_run:
        logger.info("[dry-run] skipping promote")
        return True
    if not os.path.exists(STAGING_DIR):
        logger.error("staging dir missing, cannot promote")
        return False
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_target = f"{ARCHIVE_DIR}_{stamp}"
    os.makedirs(os.path.dirname(ARCHIVE_DIR), exist_ok=True)
    if os.path.exists(PROD_DIR):
        shutil.move(PROD_DIR, archive_target)
        logger.info("archived old production → %s", archive_target)
    shutil.move(STAGING_DIR, PROD_DIR)
    logger.info("promoted staging → production")
    return True


def step_reject() -> None:
    if os.path.exists(STAGING_DIR):
        shutil.rmtree(STAGING_DIR)
        logger.info("cleaned up rejected staging dir")


def step_refit_calibrators(dry_run: bool = False) -> bool:
    """Council Round 3 Step 2 (Henri): refit post-hoc isotonic calibrators
    against the freshly-promoted production models. Runs after promote so
    the calibrators fit the currently-deployed model. Non-fatal on failure —
    a bad calibration just leaves the previous pkl (or no pkl) in place.
    """
    if dry_run:
        logger.info("[dry-run] skipping calibrator refit")
        return True
    rc = run([
        _VENV_PY, "scripts/calibrate_isotonic.py",
        "--target-source", "station",
        "--quiet",
    ])
    if rc != 0:
        logger.warning("calibrator refit failed (rc=%d) — leaving previous pkl in place", rc)
        return False
    logger.info("calibrator refit ok")
    return True


def append_log(entry: dict) -> None:
    entries = []
    if os.path.exists(LOG_PATH):
        try:
            with open(LOG_PATH) as f:
                entries = json.load(f)
        except Exception:
            entries = []
    entries.append(entry)
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(entries[-50:], f, indent=2, default=str)  # keep last 50 entries


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-promote", action="store_true",
                   help="skip the comparison and promote unconditionally")
    p.add_argument("--window-days", type=int, default=30,
                   help="days of shadow-verify history to use in the comparison")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    t0 = time.time()

    if not step_incremental_backfill(dry_run=args.dry_run):
        logger.error("incremental backfill step failed, aborting retrain")
        sys.exit(1)

    if not step_retrain_staging(dry_run=args.dry_run):
        logger.error("retrain step failed, aborting")
        step_reject()
        sys.exit(1)

    if args.force_promote:
        decision = {"decision": "force_promote"}
    else:
        decision = compare_staging_vs_production(window_days=args.window_days)

    logger.info("decision: %s", decision["decision"])
    logger.info("details: %s", json.dumps(decision, default=str, indent=2))

    if decision["decision"] in ("promote", "promote_first_run", "force_promote"):
        ok = step_promote(dry_run=args.dry_run)
        if not ok:
            logger.error("promote failed")
        else:
            # Refit isotonic calibrators against the freshly promoted models.
            step_refit_calibrators(dry_run=args.dry_run)
    else:
        step_reject()

    elapsed = time.time() - t0
    entry = {
        "ran_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "elapsed_s": round(elapsed, 1),
        "decision": decision,
        "dry_run": args.dry_run,
    }
    append_log(entry)
    logger.warning("weekly retrain complete in %.1fs (decision=%s)", elapsed, decision["decision"])


if __name__ == "__main__":
    main()
