#!/usr/bin/env python3
"""
Daily shadow verification for the MOS post-processing layer.

For each tracked city, this script:
  1. Loads yesterday's station observations from observations_hourly_station.
  2. Reconstructs the MOS prediction that the trainer would produce *for
     those exact valid_times* by building features from forecasts_hourly
     (stored historical forecasts) and running the trained quantile models.
  3. Computes per-variable pinball loss and CRPS proxy for (a) MOS,
     (b) ensemble_mean baseline, (c) best single source.
  4. Writes one row per (city, variable) to the mos_daily_skill table.
  5. Alerts via the weather-telegram channel on 3-consecutive-day regression.

Design notes:
- **Shadow** = doesn't affect the live forecast, just measures against what
  actually happened.
- **Reconstructs** rather than re-runs live inference — we want to score the
  *deployed* model against ground truth, so we use the same features the
  trainer saw on the historical forecasts table.
- Schema change on first run: adds `mos_daily_skill` table.

Usage (intended as a cron job, daily at 06:00 UTC):
    .venv/bin/python scripts/mos_shadow_verify.py
    .venv/bin/python scripts/mos_shadow_verify.py --date 2026-04-11
    .venv/bin/python scripts/mos_shadow_verify.py --city-id 1 --dry-run
"""

import argparse
import json
import logging
import math
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
import lightgbm as lgb  # noqa: F401  (loaded by mos_inference)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection  # type: ignore
import mos_inference as mos  # type: ignore
import ntfy_alert  # type: ignore

logger = logging.getLogger("mos_shadow_verify")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS mos_daily_skill (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    verify_date TEXT NOT NULL,        -- the observation date being scored
    city_id INTEGER NOT NULL,
    variable TEXT NOT NULL,
    n_obs INTEGER NOT NULL,
    mos_mae REAL,
    mos_rmse REAL,
    mos_pinball_p10 REAL,
    mos_pinball_p50 REAL,
    mos_pinball_p90 REAL,
    mos_mean_pinball REAL,
    mos_crps_proxy REAL,
    mos_coverage_80 REAL,
    ensemble_mae REAL,
    ensemble_rmse REAL,
    best_single_source TEXT,
    best_single_mae REAL,
    computed_at TEXT NOT NULL,
    FOREIGN KEY (city_id) REFERENCES cities (id) ON DELETE CASCADE,
    UNIQUE (verify_date, city_id, variable)
);

CREATE INDEX IF NOT EXISTS idx_mds_date ON mos_daily_skill (verify_date);
CREATE INDEX IF NOT EXISTS idx_mds_city_var ON mos_daily_skill (city_id, variable);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def load_eval_day(
    conn: sqlite3.Connection,
    city_id: int,
    target_date: date,
    variable: str,
) -> Optional[pd.DataFrame]:
    """Join forecasts_hourly (one row per source) with observations_hourly_station
    for a single (city, date, variable). Returns wide-format DataFrame with
    src_<model> columns + obs column + aux temp_c / humidity_pct per-source
    columns (required for v2 physics features).
    """
    day_start = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
    day_end = datetime.combine(target_date, datetime.max.time(), tzinfo=timezone.utc)

    # Pull every variable the trainer might need so v2 (physics) and v3
    # (cross-variable aux + pressure tendency) feature sets both work.
    fc_sql = """
        SELECT valid_time, source,
               temp_c, humidity_pct, precip_mm, wind_speed_kmh,
               dew_point_c, pressure_msl_hpa, cloud_cover_pct, wind_dir_deg
        FROM forecasts_hourly
        WHERE city_id = ?
          AND valid_time >= ?
          AND valid_time <= ?
    """
    obs_sql = f"""
        SELECT valid_time, {variable} AS obs
        FROM observations_hourly_station
        WHERE city_id = ?
          AND {variable} IS NOT NULL
          AND valid_time >= ?
          AND valid_time <= ?
    """
    fc_df = pd.read_sql_query(
        fc_sql, conn, params=(city_id, day_start.isoformat(), day_end.isoformat())
    )
    obs_df = pd.read_sql_query(
        obs_sql, conn, params=(city_id, day_start.isoformat(), day_end.isoformat())
    )
    if fc_df.empty or obs_df.empty:
        return None

    # Keep long frame on the side for physics-feature computation later.
    # Build the wide frame for the target variable.
    fc_target = fc_df[["valid_time", "source", variable]].dropna(subset=[variable])
    wide = fc_target.pivot_table(
        index="valid_time", columns="source", values=variable, aggfunc="first"
    )
    wide.columns = [f"src_{c}" for c in wide.columns]
    wide = wide.reset_index()
    wide = wide.merge(obs_df, on="valid_time", how="inner")
    if wide.empty:
        return None
    # Attach the full long frame (all vars × all sources) as a side attribute
    wide.attrs["fc_long"] = fc_df
    return wide


def predict_quantiles_for_eval(
    wide_df: pd.DataFrame, city_id: int, variable: str
) -> Optional[dict]:
    """Run the deployed MOS quantile models on the eval-day feature matrix.
    Returns {q: np.ndarray} or None if the variable's models aren't loaded.
    Automatically matches the deployed model's feature_set.
    """
    if wide_df.empty:
        return None

    # Use the full long frame (all variables × all sources) that load_eval_day
    # stashed in .attrs so v2 physics features can be computed.
    fc_long = wide_df.attrs.get("fc_long")
    if fc_long is None:
        return None
    long_df = fc_long.copy()
    long_df["valid_time"] = pd.to_datetime(long_df["valid_time"], utc=True)

    fset = mos.feature_set_for()
    X = mos.engineer_features(long_df, variable, city_id=city_id, feature_set=fset)
    if X.empty:
        return None

    models = mos.load_models()
    preds = {}
    for q in mos.QUANTILES:
        booster = models.get((variable, q))
        if booster is None:
            return None
        preds[q] = booster.predict(X)
    preds = mos._rearrange_quantiles(preds)
    for q in list(preds.keys()):
        preds[q] = mos._clip_physics(variable, preds[q])

    return {"preds": preds, "X_index": X.index}


def pinball(y, yhat, tau):
    diff = y - yhat
    return float(np.mean(np.where(diff >= 0, tau * diff, (tau - 1) * diff)))


def mae(y, yhat):
    return float(np.nanmean(np.abs(y - yhat)))


def rmse(y, yhat):
    return float(np.sqrt(np.nanmean((y - yhat) ** 2)))


def compute_skill_for_city_variable(
    conn: sqlite3.Connection, city_id: int, target_date: date, variable: str
) -> Optional[dict]:
    wide = load_eval_day(conn, city_id, target_date, variable)
    if wide is None or wide.empty:
        return None

    pred_out = predict_quantiles_for_eval(wide, city_id, variable)
    if pred_out is None:
        return None
    preds = pred_out["preds"]
    pred_index = pred_out["X_index"]

    # Reorder wide to match pred_index order (features pivoted index)
    wide_sorted = wide.set_index("valid_time")
    wide_sorted.index = pd.to_datetime(wide_sorted.index, utc=True)
    wide_sorted = wide_sorted.loc[pred_index]

    y_obs = wide_sorted["obs"].to_numpy().astype(float)
    n = len(y_obs)
    if n < 4:
        return None

    src_cols = [c for c in wide_sorted.columns if c.startswith("src_")]
    ensemble_mean = wide_sorted[src_cols].mean(axis=1, skipna=True).to_numpy()

    # Best single source by MAE on this day
    best_src, best_mae = None, float("inf")
    for c in src_cols:
        vals = wide_sorted[c].to_numpy()
        m = mae(y_obs, vals)
        if not math.isnan(m) and m < best_mae:
            best_mae = m
            best_src = c

    result = {
        "verify_date": target_date.isoformat(),
        "city_id": city_id,
        "variable": variable,
        "n_obs": int(n),
        "mos_mae": mae(y_obs, preds[0.5]),
        "mos_rmse": rmse(y_obs, preds[0.5]),
        "mos_pinball_p10": pinball(y_obs, preds[0.1], 0.1),
        "mos_pinball_p50": pinball(y_obs, preds[0.5], 0.5),
        "mos_pinball_p90": pinball(y_obs, preds[0.9], 0.9),
        "mos_coverage_80": float(np.mean((y_obs >= preds[0.1]) & (y_obs <= preds[0.9]))),
        "ensemble_mae": mae(y_obs, ensemble_mean),
        "ensemble_rmse": rmse(y_obs, ensemble_mean),
        "best_single_source": best_src,
        "best_single_mae": best_mae if best_mae != float("inf") else None,
    }
    result["mos_mean_pinball"] = float(np.mean([
        result["mos_pinball_p10"], result["mos_pinball_p50"], result["mos_pinball_p90"]
    ]))
    result["mos_crps_proxy"] = 2.0 * result["mos_mean_pinball"]
    return result


def write_skill_row(conn: sqlite3.Connection, row: dict, dry_run: bool) -> None:
    if dry_run:
        return
    conn.execute(
        """
        INSERT OR REPLACE INTO mos_daily_skill (
            verify_date, city_id, variable, n_obs,
            mos_mae, mos_rmse,
            mos_pinball_p10, mos_pinball_p50, mos_pinball_p90,
            mos_mean_pinball, mos_crps_proxy, mos_coverage_80,
            ensemble_mae, ensemble_rmse,
            best_single_source, best_single_mae,
            computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["verify_date"], row["city_id"], row["variable"], row["n_obs"],
            row.get("mos_mae"), row.get("mos_rmse"),
            row["mos_pinball_p10"], row["mos_pinball_p50"], row["mos_pinball_p90"],
            row["mos_mean_pinball"], row["mos_crps_proxy"], row.get("mos_coverage_80"),
            row.get("ensemble_mae"), row.get("ensemble_rmse"),
            row.get("best_single_source"), row.get("best_single_mae"),
            datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        ),
    )
    conn.commit()


def detect_low_precip_coverage(
    conn: sqlite3.Connection,
    threshold: float,
    window_days: int = 3,
) -> list:
    """Round 4.5.5 (Marcus): scan all cities for precip_mm cov80 below threshold
    for `window_days` consecutive verify dates. Returns list of dicts:
    {city_id, city_name, recent_cov80}.

    Triggered by the Bratislava all-zero hurdle prediction observed at smoke
    test — the same pattern would silently underpredict precip on any city
    where Stage 1 P(rain) < 0.1 every day. This catches the failure mode at
    the verify level once 3 consecutive days of low coverage have accumulated.
    """
    rows = conn.execute(
        """
        SELECT s.city_id, c.name AS city_name, s.verify_date, s.mos_coverage_80
        FROM mos_daily_skill s
        JOIN cities c ON c.id = s.city_id
        WHERE s.variable = 'precip_mm'
        ORDER BY s.city_id, s.verify_date DESC
        """
    ).fetchall()
    by_city = {}
    for r in rows:
        by_city.setdefault(r[0], {"name": r[1], "covs": []})["covs"].append(r[3])
    flagged = []
    for cid, d in by_city.items():
        recent = [c for c in d["covs"][:window_days] if c is not None]
        if len(recent) < window_days:
            continue
        if all(c < threshold for c in recent):
            flagged.append({
                "city_id": cid,
                "city_name": d["name"],
                "recent_cov80": recent,
            })
    return flagged


def detect_regression(
    conn: sqlite3.Connection, city_id: int, variable: str, window_days: int = 3
) -> bool:
    """Regression = MOS MAE > ensemble MAE for the last `window_days` days."""
    rows = conn.execute(
        """
        SELECT mos_mae, ensemble_mae FROM mos_daily_skill
        WHERE city_id = ? AND variable = ?
        ORDER BY verify_date DESC LIMIT ?
        """,
        (city_id, variable, window_days),
    ).fetchall()
    if len(rows) < window_days:
        return False
    return all(
        (r[0] is not None and r[1] is not None and r[0] > r[1]) for r in rows
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Date to verify (YYYY-MM-DD, default: yesterday)",
    )
    p.add_argument("--city-id", type=int, default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        target_date = date.fromisoformat(args.date)
    except ValueError as e:
        logger.error("Invalid date: %s", e)
        sys.exit(2)

    conn = get_connection()
    ensure_schema(conn)

    # Cities
    if args.city_id is not None:
        cities = [dict(r) for r in conn.execute(
            "SELECT id, name FROM cities WHERE id = ?", (args.city_id,)
        ).fetchall()]
    else:
        cities = [dict(r) for r in conn.execute(
            "SELECT id, name FROM cities ORDER BY id"
        ).fetchall()]

    total = 0
    regressions = []
    regression_details = []  # list of dicts with full info for the alert payload
    for city in cities:
        for variable in mos.VARIABLES:
            try:
                result = compute_skill_for_city_variable(
                    conn, city["id"], target_date, variable
                )
            except Exception as e:
                logger.exception(
                    "[%s/%s] compute failed: %s", city["name"], variable, e
                )
                continue
            if result is None:
                continue
            write_skill_row(conn, result, dry_run=args.dry_run)
            total += 1
            flag = ""
            if result.get("mos_mae") is not None and result.get("ensemble_mae") is not None:
                if result["mos_mae"] > result["ensemble_mae"]:
                    flag = " REGRESS"
            logger.info(
                "[%s/%s] n=%d mos_mae=%.3f ens_mae=%.3f cov=%.2f%s",
                city["name"], variable, result["n_obs"],
                result["mos_mae"] or float("nan"),
                result["ensemble_mae"] or float("nan"),
                result["mos_coverage_80"] or float("nan"),
                flag,
            )
            if not args.dry_run and detect_regression(conn, city["id"], variable):
                regressions.append(f"{city['name']}/{variable}")
                regression_details.append({
                    "city": city["name"],
                    "variable": variable,
                    "mos_mae": result.get("mos_mae"),
                    "ensemble_mae": result.get("ensemble_mae"),
                    "n_obs": result.get("n_obs"),
                })

    logger.warning(
        "Shadow verify complete: %d rows for %s, regressions=%s",
        total, target_date, regressions or "none",
    )

    # ---- ntfy push alert ----
    if regressions and not args.dry_run:
        title = f"MOS drift: {len(regressions)} variable(s) regressing"
        lines = [
            f"Verify date: {target_date.isoformat()}",
            f"Regressions (3-day consecutive): {len(regressions)}",
            "",
        ]
        for d in regression_details:
            delta = None
            if d["mos_mae"] is not None and d["ensemble_mae"] is not None:
                try:
                    delta = (d["mos_mae"] - d["ensemble_mae"]) / d["ensemble_mae"] * 100.0
                except ZeroDivisionError:
                    delta = None
            delta_str = f" ({delta:+.1f}% vs ensemble)" if delta is not None else ""
            lines.append(
                f"• {d['city']}/{d['variable']}: MOS MAE={d['mos_mae']:.3f}{delta_str}"
            )
        lines.append("")
        lines.append("Check: weather-ctl mos-status")
        body = "\n".join(lines)
        ok = ntfy_alert.send(
            title=title,
            message=body,
            priority="high",
            tags=["warning", "robot_face"],
        )
        if ok:
            logger.warning("ntfy alert sent")
        else:
            logger.warning("ntfy alert failed (NTFY_TOPIC set?)")
        print(json.dumps({"regressions": regressions, "date": target_date.isoformat()}))

    # ---- Round 4.5.5: low precip cov80 alert ----
    if not args.dry_run:
        precip_threshold = float(os.environ.get("MOS_PRECIP_ALERT_THRESHOLD", "0.6"))
        flagged_precip = detect_low_precip_coverage(
            conn, threshold=precip_threshold, window_days=3
        )
        if flagged_precip:
            title = (
                f"MOS precip cov80 < {precip_threshold:g} on {len(flagged_precip)} city/cities"
            )
            lines = [
                f"Verify date: {target_date.isoformat()}",
                f"Cities with precip_mm cov80 < {precip_threshold:g} for 3 consecutive days:",
                "",
            ]
            for f in flagged_precip:
                covs = ", ".join(f"{c:.3f}" for c in f["recent_cov80"])
                lines.append(f"• {f['city_name']} (id={f['city_id']}): [{covs}]")
            lines.append("")
            lines.append(
                "Likely cause: hurdle Stage 1 P(rain) < 0.1 — model predicting all-zero."
            )
            ntfy_alert.send(
                title=title,
                message="\n".join(lines),
                priority="default",
                tags=["umbrella", "warning"],
            )
            logger.warning(
                "precip cov80 alert: %d cities flagged",
                len(flagged_precip),
            )

    # ---- Optional heartbeat ("all clear") ----
    if not args.dry_run and not regressions and os.environ.get("NTFY_HEARTBEAT") == "1":
        ntfy_alert.send(
            title="MOS shadow verify: all clear",
            message=f"{target_date.isoformat()}: {total} rows, no regressions.",
            priority="min",
            tags=["white_check_mark"],
        )


if __name__ == "__main__":
    main()
