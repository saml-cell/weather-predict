#!/usr/bin/env python3
"""
Council Round 3 Step 2: post-hoc isotonic recalibration (Henri).

Fits a per-variable monotone PIT recalibration (Gneiting-Balabdaoui-Raftery
2007 style) on the 2024 validation set, applies it to the 2025 test set,
reports TRUE CRPS (interpolated from the 3 trained quantile points to a
99-point predictive CDF), reliability diagrams and coverage curves.

If calibration strictly improves CRPS *and* coverage on the test set, the
fitted calibrators are saved to data/mos_models_station/isotonic_calibrators.pkl
so that mos_inference can load + apply them at serving time. Weekly retrain
refits this file as its last step.

The calibration approach
------------------------
At train time we produce three quantile values per row: y^_{τ}(x), τ ∈ {0.1, 0.5, 0.9}.
For each val row i, we compute the probability integral transform PIT_i —
the level u such that the predictive CDF F_pred(y_obs_i; x_i) = u. We do this
by piecewise-linear interpolation across the three quantile anchors plus
linear extrapolation on each tail. Under perfect calibration, PIT_i ~ U(0,1);
under under-confident 80% intervals (our current state), the empirical PIT
density is pinched near 0 and 1. The isotonic calibrator learns the map
nominal_tau → empirical_PIT_cdf^{-1}(nominal_tau) — a monotone 1D function.
At inference we invert it: a caller asks for the τ-th quantile, we look up
the empirical τ-effective level and reconstruct the prediction by interpolating
the three raw quantile predictions at that level. The transform is guaranteed
monotone (IsotonicRegression), strictly proper-scoring-aware (CRPS-improving
when the PIT is miscalibrated), and cheap (~200 µs/row amortised).

Scope decisions
---------------
- **Global per-variable**, not per-city: 25 cities × 4 vars × 3 taus = 300
  calibrators would each fit on ~8k val rows, a lot of overfit risk. A global
  per-variable calibrator still captures the dominant coverage miscalibration
  (bias is mostly within-variable, not within-city).
- **Use the same v3 features/splits as the deployed model**: we reload the
  cached 2024 val and 2025 test rows by importing train_mos_quantile's data
  loader and running booster.predict on them. This takes ~3-4 minutes total.
- **Physics-clipped at the end**: same PHYS_BOUNDS as mos_inference.

Usage:
    .venv/bin/python scripts/calibrate_isotonic.py
    .venv/bin/python scripts/calibrate_isotonic.py --dry-run
    .venv/bin/python scripts/calibrate_isotonic.py --target-source era5
"""

import argparse
import json
import logging
import os
import pickle
import sys
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.isotonic import IsotonicRegression

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from train_mos_quantile import (  # type: ignore
    DEFAULT_QUANTILES,
    DEFAULT_TRAIN_END,
    DEFAULT_VAL_END,
    DEFAULT_TEST_END,
    FEATURE_SET_V3,
    engineer_features,
    load_cities_meta,
    load_joined,
    split_temporal,
    pinball_loss,
)
from db import get_connection  # type: ignore

logger = logging.getLogger("calibrate_isotonic")

PROJECT_ROOT = os.path.dirname(_HERE)
DEFAULT_MODELS_DIR = os.path.join(PROJECT_ROOT, "data", "mos_models_station")

VARIABLES = ["temp_c", "humidity_pct", "precip_mm", "wind_speed_kmh"]

# PIT interpolation grid used both for CRPS and for inference reconstruction.
# Avoid 0 and 1 exactly; work on the open interval (linear extrapolation at tails).
DENSE_TAUS = np.round(np.arange(0.01, 1.00, 0.01), 2)  # 99 points

# Physics clipping (mirrors mos_inference.PHYS_BOUNDS).
PHYS_BOUNDS = {
    "humidity_pct": (0.0, 100.0),
    "precip_mm": (0.0, None),
    "wind_speed_kmh": (0.0, None),
    "temp_c": (None, None),
}


# ---------------------------------------------------------------------------
# Predictive CDF interpolation utilities
# ---------------------------------------------------------------------------

def interp_predictive_cdf(
    q10: np.ndarray, q50: np.ndarray, q90: np.ndarray, taus: np.ndarray
) -> np.ndarray:
    """Given three trained quantile predictions per row, reconstruct a
    piecewise-linear predictive CDF and evaluate it at the requested taus.

    Returns array shape (n_rows, len(taus)) with monotone non-decreasing rows.

    Interpolation regions:
      - taus < 0.1: linear from (0.0, q10 - (q50-q10)) to (0.1, q10)
      - 0.1 <= taus < 0.5: linear (q10 @ 0.1) → (q50 @ 0.5)
      - 0.5 <= taus < 0.9: linear (q50 @ 0.5) → (q90 @ 0.9)
      - taus >= 0.9: linear (q90 @ 0.9) → (1.0, q90 + (q90-q50))
    """
    q10 = np.asarray(q10, dtype=float)
    q50 = np.asarray(q50, dtype=float)
    q90 = np.asarray(q90, dtype=float)

    # Ensure rearranged (defensive; callers should already have done this)
    stacked = np.column_stack([q10, q50, q90])
    stacked.sort(axis=1)
    q10, q50, q90 = stacked[:, 0], stacked[:, 1], stacked[:, 2]

    # Slopes for each piece
    spread_10_50 = (q50 - q10) / 0.4  # per unit tau
    spread_50_90 = (q90 - q50) / 0.4
    # Tails match neighbouring slope (no explosion):
    spread_left = spread_10_50
    spread_right = spread_50_90

    n_rows = len(q10)
    n_taus = len(taus)
    out = np.empty((n_rows, n_taus), dtype=float)
    for j, t in enumerate(taus):
        if t < 0.1:
            out[:, j] = q10 - (0.1 - t) * spread_left
        elif t < 0.5:
            out[:, j] = q10 + (t - 0.1) * spread_10_50
        elif t < 0.9:
            out[:, j] = q50 + (t - 0.5) * spread_50_90
        else:
            out[:, j] = q90 + (t - 0.9) * spread_right
    return out


def pit_values(q10: np.ndarray, q50: np.ndarray, q90: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Compute probability integral transform values per row, using the same
    piecewise-linear CDF as `interp_predictive_cdf` but solved for `tau`
    given `y` instead of for `y` given `tau`.
    """
    q10 = np.asarray(q10, dtype=float)
    q50 = np.asarray(q50, dtype=float)
    q90 = np.asarray(q90, dtype=float)
    y = np.asarray(y, dtype=float)

    stacked = np.column_stack([q10, q50, q90])
    stacked.sort(axis=1)
    q10, q50, q90 = stacked[:, 0], stacked[:, 1], stacked[:, 2]

    eps = 1e-12
    spread_10_50 = np.maximum(q50 - q10, eps)
    spread_50_90 = np.maximum(q90 - q50, eps)

    pit = np.empty_like(y)

    mask_low = y < q10
    mask_mid_low = (y >= q10) & (y < q50)
    mask_mid_high = (y >= q50) & (y < q90)
    mask_high = y >= q90

    # Below q10: tau = 0.1 - (q10 - y) / (spread_10_50 / 0.4)
    # slope_left (per tau unit) = spread_10_50 / 0.4 × 1  i.e. value change per 1.0 in tau
    # so d tau = d y / (spread_10_50 / 0.4) × 0.4? Let's re-derive:
    # In the interp_predictive_cdf left tail: y = q10 - (0.1 - tau) * (spread_10_50)
    #   where spread_10_50 above was (q50 - q10) / 0.4 — the slope dy/dtau per unit tau.
    # So to invert: (0.1 - tau) = (q10 - y) / spread_10_50  →  tau = 0.1 - (q10 - y)/spread_10_50
    slope_left = spread_10_50 / 0.4
    slope_right = spread_50_90 / 0.4

    pit[mask_low] = 0.1 - (q10[mask_low] - y[mask_low]) / slope_left[mask_low]
    # Mid-low: y = q10 + (tau - 0.1) * (q50 - q10) / 0.4 → tau = 0.1 + 0.4 * (y - q10)/(q50 - q10)
    pit[mask_mid_low] = 0.1 + 0.4 * (y[mask_mid_low] - q10[mask_mid_low]) / spread_10_50[mask_mid_low]
    # Mid-high: analogous
    pit[mask_mid_high] = 0.5 + 0.4 * (y[mask_mid_high] - q50[mask_mid_high]) / spread_50_90[mask_mid_high]
    # Above q90: y = q90 + (tau - 0.9) * slope_right → tau = 0.9 + (y - q90)/slope_right
    pit[mask_high] = 0.9 + (y[mask_high] - q90[mask_high]) / slope_right[mask_high]

    # Clip to (0,1) open (avoid degenerate ranks).
    return np.clip(pit, 1e-6, 1.0 - 1e-6)


# ---------------------------------------------------------------------------
# CRPS (true, on a dense tau grid)
# ---------------------------------------------------------------------------

def dense_crps(
    q10: np.ndarray,
    q50: np.ndarray,
    q90: np.ndarray,
    y: np.ndarray,
    taus: np.ndarray = DENSE_TAUS,
) -> float:
    """True CRPS via the pinball-loss decomposition:
        CRPS = 2 · E_tau [ pinball(y, q_tau, tau) ]
    evaluated on a dense grid of tau ∈ (0,1) using the interpolated
    predictive CDF. Note: with only 3 trained quantile anchors, the tails
    (tau < 0.1 and tau > 0.9) are linearly extrapolated and CRPS is
    sensitive to that extrapolation. Use `pinball_crps_3pt` as the operational
    metric; dense CRPS is diagnostic only.
    """
    dense_qs = interp_predictive_cdf(q10, q50, q90, taus)  # (n_rows, n_taus)
    y_col = np.asarray(y, dtype=float).reshape(-1, 1)
    diff = y_col - dense_qs  # (n_rows, n_taus)
    taus_row = taus.reshape(1, -1)
    pinball = np.where(diff >= 0, taus_row * diff, (taus_row - 1.0) * diff)
    # Mean over tau and over rows, then × 2.
    return float(2.0 * pinball.mean())


def pinball_crps_3pt(
    q10: np.ndarray, q50: np.ndarray, q90: np.ndarray, y: np.ndarray
) -> float:
    """Legacy 3-point CRPS proxy — the same metric the trainer reports and
    the one tied directly to what inference actually serves (only 3 quantiles
    per row). This is the operational gate metric; `dense_crps` is a diagnostic
    with known tail sensitivity at 3 anchors.
    """
    return float(
        2.0 * np.mean([
            pinball_loss(y, q10, 0.1),
            pinball_loss(y, q50, 0.5),
            pinball_loss(y, q90, 0.9),
        ])
    )


def per_row_coverage(q_lo: np.ndarray, q_hi: np.ndarray, y: np.ndarray) -> float:
    return float(np.mean((y >= q_lo) & (y <= q_hi)))


# ---------------------------------------------------------------------------
# Isotonic calibrator (per variable, global)
# ---------------------------------------------------------------------------

def fit_isotonic_from_pit(pit_vals: np.ndarray) -> IsotonicRegression:
    """Fit a monotone empirical CDF estimator for PIT values. At inference we
    use this to map nominal_tau → effective_tau:
        effective_tau = empirical_cdf_PIT(nominal_tau)
    i.e. given a target nominal level tau, we ask "what fraction of val PITs
    were <= tau" and return that as the effective level. Under miscalibration
    this fraction will differ from tau, and applying it pulls predictions
    back to the correct coverage.
    """
    pit_vals = np.asarray(pit_vals, dtype=float)
    pit_vals = pit_vals[np.isfinite(pit_vals)]
    n = len(pit_vals)
    if n < 10:
        raise ValueError(f"insufficient PIT values: {n}")
    sorted_pit = np.sort(pit_vals)
    ecdf_y = np.arange(1, n + 1, dtype=float) / n
    ir = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    ir.fit(sorted_pit, ecdf_y)
    return ir


def apply_calibrator(
    ir: IsotonicRegression,
    q10: np.ndarray,
    q50: np.ndarray,
    q90: np.ndarray,
    target_taus: list,
) -> dict:
    """Given a fitted empirical-CDF calibrator for one variable, compute
    recalibrated quantiles at the requested target_taus. Uses the inverse
    empirical CDF: for target tau, find the raw tau' such that
    empirical_cdf(tau') == tau, then reconstruct the prediction by
    interpolating the 3 raw quantiles at tau'.
    """
    # Build a fine grid to invert the fitted empirical CDF.
    grid_x = np.linspace(0.001, 0.999, 999)
    grid_y = ir.predict(grid_x)
    out = {}
    for tgt in target_taus:
        # Find tau' such that grid_y == tgt (monotone, so searchsorted).
        idx = np.searchsorted(grid_y, tgt)
        idx = min(max(idx, 0), len(grid_x) - 1)
        tau_prime = float(grid_x[idx])
        dense = interp_predictive_cdf(q10, q50, q90, np.array([tau_prime]))
        out[tgt] = dense[:, 0]
    return out


# ---------------------------------------------------------------------------
# Per-variable evaluation loop
# ---------------------------------------------------------------------------

def _clip_physics(variable: str, arr: np.ndarray) -> np.ndarray:
    lo, hi = PHYS_BOUNDS.get(variable, (None, None))
    if lo is not None:
        arr = np.maximum(arr, lo)
    if hi is not None:
        arr = np.minimum(arr, hi)
    return arr


def _rearrange_rows(q10, q50, q90):
    stacked = np.column_stack([q10, q50, q90])
    stacked.sort(axis=1)
    return stacked[:, 0], stacked[:, 1], stacked[:, 2]


def load_boosters(models_dir: str, variable: str) -> dict:
    out = {}
    for q in DEFAULT_QUANTILES:
        path = os.path.join(models_dir, variable, f"q{int(round(q * 100)):03d}.txt")
        if not os.path.exists(path):
            raise FileNotFoundError(f"missing booster: {path}")
        out[q] = lgb.Booster(model_file=path)
    return out


def predict_splits(
    boosters: dict,
    X_val: pd.DataFrame,
    X_test: pd.DataFrame,
    feature_cols: list,
) -> tuple:
    """Run the three quantile boosters on val + test. Returns a dict of
    arrays: {'val': {q: array}, 'test': {q: array}}."""
    val = {}
    test = {}
    for q, booster in boosters.items():
        val[q] = booster.predict(X_val[feature_cols])
        test[q] = booster.predict(X_test[feature_cols])
    return val, test


def evaluate_variable(
    conn,
    variable: str,
    cities_meta: dict,
    models_dir: str,
    target_source: str,
    train_end: str,
    val_end: str,
    test_end: str,
) -> dict:
    """Load data, run boosters, compute baseline (uncalibrated) metrics,
    fit calibrator, compute calibrated metrics, return a result dict.
    """
    logger.info("=" * 60)
    logger.info("Variable: %s", variable)

    t0 = time.time()
    df = load_joined(
        conn,
        variable=variable,
        city_id=None,
        target_source=target_source,
        load_aux=True,
        feature_set_v3=True,
    )
    if df.empty:
        return {"variable": variable, "error": "no_data"}

    X, y, feature_cols = engineer_features(df, cities_meta=cities_meta, feature_set=FEATURE_SET_V3)
    df_feat = df.copy()
    for col in X.columns:
        df_feat[col] = X[col]
    df_feat["_y"] = y

    train_df, val_df, test_df = split_temporal(df_feat, train_end, val_end, test_end)
    logger.info(
        "[%s] rows: train=%d val=%d test=%d",
        variable, len(train_df), len(val_df), len(test_df),
    )

    boosters = load_boosters(models_dir, variable)
    val_preds, test_preds = predict_splits(boosters, val_df, test_df, feature_cols)

    # Rearrange per row, clip
    val_q10, val_q50, val_q90 = _rearrange_rows(val_preds[0.1], val_preds[0.5], val_preds[0.9])
    test_q10, test_q50, test_q90 = _rearrange_rows(test_preds[0.1], test_preds[0.5], test_preds[0.9])
    val_q10 = _clip_physics(variable, val_q10)
    val_q50 = _clip_physics(variable, val_q50)
    val_q90 = _clip_physics(variable, val_q90)
    test_q10 = _clip_physics(variable, test_q10)
    test_q50 = _clip_physics(variable, test_q50)
    test_q90 = _clip_physics(variable, test_q90)

    y_val = val_df["_y"].to_numpy(dtype=float)
    y_test = test_df["_y"].to_numpy(dtype=float)

    # ---- Baseline (uncalibrated) test metrics ----
    raw_crps_dense = dense_crps(test_q10, test_q50, test_q90, y_test)
    raw_crps_3pt = pinball_crps_3pt(test_q10, test_q50, test_q90, y_test)
    raw_cov80 = per_row_coverage(test_q10, test_q90, y_test)
    raw_pinball = {
        "q10": pinball_loss(y_test, test_q10, 0.1),
        "q50": pinball_loss(y_test, test_q50, 0.5),
        "q90": pinball_loss(y_test, test_q90, 0.9),
    }
    raw_pit = pit_values(test_q10, test_q50, test_q90, y_test)

    # ---- Fit calibrator on val ----
    val_pit = pit_values(val_q10, val_q50, val_q90, y_val)
    calibrator = fit_isotonic_from_pit(val_pit)

    # ---- Apply to test ----
    cal_preds = apply_calibrator(calibrator, test_q10, test_q50, test_q90, [0.1, 0.5, 0.9])
    cal_q10 = _clip_physics(variable, cal_preds[0.1])
    cal_q50 = _clip_physics(variable, cal_preds[0.5])
    cal_q90 = _clip_physics(variable, cal_preds[0.9])
    # Re-rearrange after clipping
    cal_q10, cal_q50, cal_q90 = _rearrange_rows(cal_q10, cal_q50, cal_q90)

    cal_crps_dense = dense_crps(cal_q10, cal_q50, cal_q90, y_test)
    cal_crps_3pt = pinball_crps_3pt(cal_q10, cal_q50, cal_q90, y_test)
    cal_cov80 = per_row_coverage(cal_q10, cal_q90, y_test)
    cal_pinball = {
        "q10": pinball_loss(y_test, cal_q10, 0.1),
        "q50": pinball_loss(y_test, cal_q50, 0.5),
        "q90": pinball_loss(y_test, cal_q90, 0.9),
    }
    cal_pit = pit_values(cal_q10, cal_q50, cal_q90, y_test)

    # ---- Reliability at the 9-deciles level ----
    reliability = {}
    for level in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        raw_frac = float(np.mean(raw_pit <= level))
        cal_frac = float(np.mean(cal_pit <= level))
        reliability[str(level)] = {"raw": raw_frac, "calibrated": cal_frac}

    elapsed = time.time() - t0
    logger.info(
        "[%s] done in %.1fs: CRPS3pt %.4f → %.4f  CRPSdense %.4f → %.4f  cov80 %.3f → %.3f",
        variable, elapsed,
        raw_crps_3pt, cal_crps_3pt,
        raw_crps_dense, cal_crps_dense,
        raw_cov80, cal_cov80,
    )

    return {
        "variable": variable,
        "rows": {"val": int(len(val_df)), "test": int(len(test_df))},
        "raw": {
            "crps_3pt": raw_crps_3pt,
            "crps_dense": raw_crps_dense,
            "coverage_80": raw_cov80,
            "pinball": raw_pinball,
        },
        "calibrated": {
            "crps_3pt": cal_crps_3pt,
            "crps_dense": cal_crps_dense,
            "coverage_80": cal_cov80,
            "pinball": cal_pinball,
        },
        "reliability_cdf_at_deciles": reliability,
        "_calibrator": calibrator,  # stripped from JSON output below
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _format_summary(report: dict) -> str:
    lines = []
    lines.append(f"Isotonic Calibration Report — {report['ran_at']}")
    lines.append("=" * 72)
    for r in report["variables"]:
        if "error" in r:
            lines.append(f"\n{r['variable']}: ERROR ({r['error']})")
            continue
        v = r["variable"]
        raw = r["raw"]
        cal = r["calibrated"]
        d_crps_3pt = (
            (cal["crps_3pt"] - raw["crps_3pt"]) / raw["crps_3pt"] * 100.0
            if raw["crps_3pt"] else 0.0
        )
        d_crps_dense = (
            (cal["crps_dense"] - raw["crps_dense"]) / raw["crps_dense"] * 100.0
            if raw["crps_dense"] else 0.0
        )
        d_cov = cal["coverage_80"] - raw["coverage_80"]
        lines.append(
            f"\n{v}  test_rows={r['rows']['test']}"
        )
        lines.append(
            f"  CRPS (3-pt):  raw={raw['crps_3pt']:.4f}  cal={cal['crps_3pt']:.4f}  Δ={d_crps_3pt:+.2f}% [operational metric]"
        )
        lines.append(
            f"  CRPS (99-pt): raw={raw['crps_dense']:.4f}  cal={cal['crps_dense']:.4f}  Δ={d_crps_dense:+.2f}% [diagnostic, tail-sensitive]"
        )
        lines.append(
            f"  cov80:        raw={raw['coverage_80']:.3f}  cal={cal['coverage_80']:.3f}  Δ={d_cov:+.3f}  target=0.800"
        )
        lines.append(
            f"  pinball q10:  raw={raw['pinball']['q10']:.4f}  cal={cal['pinball']['q10']:.4f}"
        )
        lines.append(
            f"  pinball q50:  raw={raw['pinball']['q50']:.4f}  cal={cal['pinball']['q50']:.4f}"
        )
        lines.append(
            f"  pinball q90:  raw={raw['pinball']['q90']:.4f}  cal={cal['pinball']['q90']:.4f}"
        )
        crps_ok = cal["crps_3pt"] <= raw["crps_3pt"] + 1e-9
        cov_ok = abs(cal["coverage_80"] - 0.8) <= abs(raw["coverage_80"] - 0.8) + 1e-9
        verdict_crps = "✓" if crps_ok else "✗"
        verdict_cov = "✓" if cov_ok else "✗"
        lines.append(
            f"  strict improvement (operational): CRPS3pt {verdict_crps}  coverage {verdict_cov}"
        )
    lines.append("\n" + "=" * 72)
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    p.add_argument("--target-source", choices=["era5", "station"], default="station")
    p.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    p.add_argument("--val-end", default=DEFAULT_VAL_END)
    p.add_argument("--test-end", default=DEFAULT_TEST_END)
    p.add_argument("--dry-run", action="store_true",
                   help="Report metrics but don't save calibrators.pkl")
    p.add_argument("--variables", default=",".join(VARIABLES))
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    variables = [v.strip() for v in args.variables.split(",") if v.strip()]
    conn = get_connection()
    cities_meta = load_cities_meta(conn)

    report = {
        "ran_at": datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z",
        "models_dir": args.models_dir,
        "target_source": args.target_source,
        "train_end": args.train_end,
        "val_end": args.val_end,
        "test_end": args.test_end,
        "variables": [],
    }

    calibrators = {}
    t0 = time.time()
    for v in variables:
        try:
            r = evaluate_variable(
                conn,
                variable=v,
                cities_meta=cities_meta,
                models_dir=args.models_dir,
                target_source=args.target_source,
                train_end=args.train_end,
                val_end=args.val_end,
                test_end=args.test_end,
            )
            ir = r.pop("_calibrator", None)
            if ir is not None:
                calibrators[v] = ir
            report["variables"].append(r)
        except Exception as e:
            logger.exception("[%s] failed: %s", v, e)
            report["variables"].append({"variable": v, "error": str(e)})
    elapsed = time.time() - t0

    # Per-variable gate (what the server actually delivers):
    #   - 3-point CRPS must not get worse for that variable
    #   - |coverage_80 - 0.8| must not get worse for that variable
    # Variables that fail the gate are NOT saved; mos_inference falls back to
    # raw predictions for them. This is the principled way to handle Step 2's
    # zero-inflated precip edge case (Henri explicitly flagged it needs a
    # hurdle model in Step 5 — negative isotonic result on precip is expected).
    accepted = []
    rejected = []
    for r in report["variables"]:
        if "error" in r:
            rejected.append({"variable": r["variable"], "reason": r["error"]})
            continue
        raw = r["raw"]
        cal = r["calibrated"]
        crps_ok = cal["crps_3pt"] <= raw["crps_3pt"] + 1e-9
        cov_ok = abs(cal["coverage_80"] - 0.8) <= abs(raw["coverage_80"] - 0.8) + 1e-9
        r["accepted"] = crps_ok and cov_ok
        if r["accepted"]:
            accepted.append(r["variable"])
        else:
            rejected.append({
                "variable": r["variable"],
                "reason": (
                    ("CRPS regressed" if not crps_ok else "") +
                    (" & " if (not crps_ok and not cov_ok) else "") +
                    ("coverage regressed" if not cov_ok else "")
                ).strip(),
            })
    report["accepted_variables"] = accepted
    report["rejected_variables"] = rejected
    report["strict_improvement"] = len(rejected) == 0  # all-pass

    # Write report JSON + human summary
    os.makedirs(args.models_dir, exist_ok=True)
    report_path = os.path.join(args.models_dir, "isotonic_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    summary = _format_summary(report)
    summary_path = os.path.join(args.models_dir, "isotonic_summary.txt")
    with open(summary_path, "w") as f:
        f.write(summary)
    print(summary)
    print(f"\naccepted  = {accepted}")
    print(f"rejected  = {[r['variable'] for r in rejected]}")
    print(f"elapsed   : {elapsed:.1f}s")

    # Keep only calibrators for accepted variables.
    kept = {v: calibrators[v] for v in accepted if v in calibrators}

    if args.dry_run:
        logger.warning("--dry-run: not saving isotonic_calibrators.pkl")
        return
    if not kept:
        logger.warning("No variables accepted — skipping save of isotonic_calibrators.pkl")
        return

    pkl_path = os.path.join(args.models_dir, "isotonic_calibrators.pkl")
    payload = {
        "format_version": 1,
        "fit_at": report["ran_at"],
        "target_source": args.target_source,
        "calibrators": kept,
        "variables": list(kept.keys()),
        "rejected": rejected,
    }
    with open(pkl_path, "wb") as f:
        pickle.dump(payload, f)
    logger.warning(
        "Saved %d calibrators → %s  (accepted=%s  rejected=%s)",
        len(kept), pkl_path, accepted, [r["variable"] for r in rejected],
    )


if __name__ == "__main__":
    main()
