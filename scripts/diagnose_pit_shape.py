#!/usr/bin/env python3
"""Round 4.5.4 (Henri) — production PIT-shape diagnostic.

Compute empirical PIT histograms on the calibrator-aware production verify
rows (2026-04-13 onward) under two paths:

  (a) RAW MOS path: the same boosters + same engineer_features as
      mos_shadow_verify, with NO calibrator applied. This matches what the
      mos_daily_skill table actually contains.

  (b) CALIBRATED path: same boosters but with the live isotonic_calibrators
      applied (humidity_pct, wind_speed_kmh — temp_c was dropped 2026-04-14
      Round 4 Step 2). This matches what the production /api/forecast
      endpoint actually serves.

Compares both against a uniform U(0,1) reference (perfect calibration target)
and outputs an empirical CDF / 10-bucket histogram per variable.

Henri's purpose: produce *directional priors* for Round 4 Step 4 (production
calibrator refit, blocked until 2026-04-20). We want to know what the PIT
shape looks like before the refit lands instead of starting from zero.

Outputs:
  /tmp/pit_shape_diagnostic.json      — full numeric report
  /tmp/pit_shape_diagnostic_summary   — formatted markdown summary
"""

import argparse
import json
import os
import pickle
import sys
from collections import defaultdict
from datetime import date

import numpy as np
import pandas as pd

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, THIS_DIR)

import mos_inference as mos  # noqa: E402
from db import get_connection  # noqa: E402
from mos_shadow_verify import load_eval_day, predict_quantiles_for_eval  # noqa: E402

VARS = ("temp_c", "humidity_pct", "wind_speed_kmh", "precip_mm")
N_BUCKETS = 10


def pit_value(q10: float, q50: float, q90: float, y: float) -> float:
    """Empirical PIT from 3 quantiles by piecewise-linear CDF interpolation.

    Tail extrapolation: linear, capped to (0, 1).
    Same convention used by Henri's calibrate_isotonic.py (Round 3 Step 2).
    """
    if not all(np.isfinite([q10, q50, q90, y])):
        return float("nan")
    if y <= q10:
        # Linear extrap below q10. Use slope from q10..q50.
        if q50 == q10:
            return 0.0
        slope = 0.4 / (q50 - q10)
        return float(max(0.0, 0.1 + (y - q10) * slope))
    if y <= q50:
        if q50 == q10:
            return 0.1
        return float(0.1 + (y - q10) * 0.4 / (q50 - q10))
    if y <= q90:
        if q90 == q50:
            return 0.5
        return float(0.5 + (y - q50) * 0.4 / (q90 - q50))
    # y > q90: extrap above
    if q90 == q50:
        return 1.0
    slope = 0.4 / (q90 - q50)
    return float(min(1.0, 0.9 + (y - q90) * slope))


def collect_production_pit(target_date: date) -> dict:
    """For the given verify date, compute per-row (raw, calibrated) PIT pairs
    for every (city, variable). Returns {variable: {raw: [...], cal: [...]}}.
    """
    conn = get_connection()
    cities = [
        dict(r) for r in conn.execute(
            "SELECT id, name FROM cities ORDER BY id"
        ).fetchall()
    ]

    calibrators = mos.load_calibrators(mos.DEFAULT_MODELS_DIR)
    calibrator_vars = set(calibrators.keys())

    out = {v: {"raw": [], "cal": [], "n_rows": 0, "calibrated_applied": False} for v in VARS}

    for city in cities:
        for variable in VARS:
            wide = load_eval_day(conn, city["id"], target_date, variable)
            if wide is None or wide.empty:
                continue
            pred_out = predict_quantiles_for_eval(wide, city["id"], variable)
            if pred_out is None:
                continue
            preds = pred_out["preds"]  # already rearranged + clipped
            pred_index = pred_out["X_index"]

            wide_sorted = wide.set_index("valid_time")
            wide_sorted.index = pd.to_datetime(wide_sorted.index, utc=True)
            common = wide_sorted.index.intersection(pred_index)
            if len(common) == 0:
                continue
            # Keep only rows present in both wide (obs available) and pred_index
            wide_sorted = wide_sorted.loc[common]
            y_obs = wide_sorted["obs"].to_numpy().astype(float)
            # Slice predictions to the same common positions
            pred_pos = [list(pred_index).index(t) for t in common]

            q10_raw = np.asarray(preds[0.1], dtype=float)[pred_pos]
            q50_raw = np.asarray(preds[0.5], dtype=float)[pred_pos]
            q90_raw = np.asarray(preds[0.9], dtype=float)[pred_pos]

            # Calibrated path
            cal = calibrators.get(variable)
            if cal is not None:
                q10_cal, q50_cal, q90_cal = mos._apply_calibrator(
                    cal, q10_raw, q50_raw, q90_raw
                )
                out[variable]["calibrated_applied"] = True
            else:
                q10_cal, q50_cal, q90_cal = q10_raw, q50_raw, q90_raw

            for i in range(len(y_obs)):
                p_raw = pit_value(q10_raw[i], q50_raw[i], q90_raw[i], y_obs[i])
                p_cal = pit_value(q10_cal[i], q50_cal[i], q90_cal[i], y_obs[i])
                if np.isfinite(p_raw):
                    out[variable]["raw"].append(p_raw)
                if np.isfinite(p_cal):
                    out[variable]["cal"].append(p_cal)
                out[variable]["n_rows"] += 1

    return out


def histogram(pits: list, n_buckets: int = N_BUCKETS) -> list:
    if not pits:
        return [0] * n_buckets
    arr = np.clip(np.asarray(pits, dtype=float), 0.0, 1.0 - 1e-12)
    idx = (arr * n_buckets).astype(int)
    counts = np.bincount(idx, minlength=n_buckets)
    return counts.tolist()


def summarize_buckets(counts: list) -> dict:
    total = sum(counts)
    if total == 0:
        return {"n": 0}
    fracs = [c / total for c in counts]
    expected = 1.0 / len(counts)
    deviation = sum(abs(f - expected) for f in fracs) / 2.0  # total variation
    # Center mass (uniform = 0.5, U-shaped = >0.5 in tails)
    tail_mass = fracs[0] + fracs[-1]
    center_mass = sum(fracs[3:7])  # buckets 3..6 = central 0.3..0.7
    # Slope hint
    lower_half = sum(fracs[:5])
    upper_half = sum(fracs[5:])
    return {
        "n": total,
        "fracs": [round(f, 4) for f in fracs],
        "uniform_expected": round(expected, 4),
        "total_variation": round(deviation, 4),
        "tail_mass_q1_q10": round(tail_mass, 4),
        "center_mass_q4_q7": round(center_mass, 4),
        "lower_half_mass": round(lower_half, 4),
        "upper_half_mass": round(upper_half, 4),
    }


def shape_classify(summary: dict) -> str:
    if summary.get("n", 0) == 0:
        return "no-data"
    fracs = summary.get("fracs", [])
    if not fracs:
        return "no-data"
    expected = summary["uniform_expected"]
    tail = summary["tail_mass_q1_q10"]
    center = summary["center_mass_q4_q7"]
    lower = summary["lower_half_mass"]
    upper = summary["upper_half_mass"]
    if tail > 4 * expected:
        return "U-shaped (intervals too narrow / under-confident)"
    if center > 5 * expected:
        return "inverted-U (intervals too wide / over-confident)"
    if abs(lower - upper) > 0.2:
        if lower > upper:
            return "left-skewed (model over-predicting / obs systematically below)"
        return "right-skewed (model under-predicting / obs systematically above)"
    if summary["total_variation"] < 0.1:
        return "approximately uniform (well-calibrated)"
    return "mildly distorted"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--date", default="2026-04-13", help="Verify date (YYYY-MM-DD)")
    p.add_argument(
        "--out",
        default="/tmp/pit_shape_diagnostic.json",
        help="Output JSON path",
    )
    p.add_argument(
        "--summary",
        default="/tmp/pit_shape_diagnostic_summary.md",
        help="Output markdown summary path",
    )
    args = p.parse_args()

    target_date = date.fromisoformat(args.date)
    print(f"Computing production PIT for {target_date.isoformat()}...")
    raw_data = collect_production_pit(target_date)

    report = {
        "target_date": target_date.isoformat(),
        "source": "mos_inference.load_models + mos_inference.load_calibrators",
        "variables": {},
    }
    md_lines = [
        f"# Production PIT-Shape Diagnostic — {target_date.isoformat()}",
        "",
        "Henri (Round 4.5.4): empirical PIT histograms on the calibrator-aware",
        "production verify rows, both raw-MOS and calibrator-applied. Tells us",
        "what the production PIT distribution actually looks like *before* the",
        "Round 4 Step 4 production refit unblocks (~2026-04-20).",
        "",
        "**Key methodology check** — the `mos_shadow_verify.py` script writing",
        "`mos_daily_skill` rows uses `mos_inference.load_models` (boosters only)",
        "and does NOT call `_apply_calibrator`. So the rows on the dashboard",
        "/skill panel labeled '2026-04-13 calibrator-aware' are actually the",
        "RAW MOS metrics, not the live-served calibrated metrics. This",
        "diagnostic computes both sides for the first time.",
        "",
    ]

    for var in VARS:
        d = raw_data[var]
        raw_hist = histogram(d["raw"])
        cal_hist = histogram(d["cal"])
        raw_summary = summarize_buckets(raw_hist)
        cal_summary = summarize_buckets(cal_hist)
        raw_shape = shape_classify(raw_summary)
        cal_shape = shape_classify(cal_summary)
        report["variables"][var] = {
            "n_rows_total": d["n_rows"],
            "raw_pit_count": len(d["raw"]),
            "cal_pit_count": len(d["cal"]),
            "calibrator_applied": d["calibrated_applied"],
            "raw_histogram": raw_hist,
            "cal_histogram": cal_hist,
            "raw_summary": raw_summary,
            "cal_summary": cal_summary,
            "raw_shape": raw_shape,
            "cal_shape": cal_shape,
        }
        md_lines.append(f"## `{var}`")
        md_lines.append("")
        md_lines.append(
            f"**Calibrator deployed for this variable:** "
            f"{'yes' if d['calibrated_applied'] else 'no (rejected during fit)'}"
        )
        md_lines.append(f"**Rows analysed:** {d['n_rows']}")
        md_lines.append("")
        md_lines.append("| Bucket | RAW | CAL |")
        md_lines.append("|---|---|---|")
        for i in range(N_BUCKETS):
            md_lines.append(f"| [{i/10:.1f}, {(i+1)/10:.1f}) | {raw_hist[i]} | {cal_hist[i]} |")
        md_lines.append("")
        md_lines.append(f"**Total variation from uniform — RAW:** {raw_summary.get('total_variation', '-')}")
        md_lines.append(f"**Total variation from uniform — CAL:** {cal_summary.get('total_variation', '-')}")
        md_lines.append(f"**Tail mass (q1+q10) — RAW:** {raw_summary.get('tail_mass_q1_q10', '-')}")
        md_lines.append(f"**Tail mass (q1+q10) — CAL:** {cal_summary.get('tail_mass_q1_q10', '-')}")
        md_lines.append(f"**Lower-half mass — RAW:** {raw_summary.get('lower_half_mass', '-')}, **CAL:** {cal_summary.get('lower_half_mass', '-')}")
        md_lines.append(f"**Upper-half mass — RAW:** {raw_summary.get('upper_half_mass', '-')}, **CAL:** {cal_summary.get('upper_half_mass', '-')}")
        md_lines.append("")
        md_lines.append(f"**Shape — RAW:** {raw_shape}")
        md_lines.append(f"**Shape — CAL:** {cal_shape}")
        md_lines.append("")

    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    with open(args.summary, "w") as f:
        f.write("\n".join(md_lines))

    print(f"\nReport: {args.out}")
    print(f"Summary: {args.summary}")
    print()
    for var in VARS:
        d = report["variables"][var]
        print(
            f"{var}: n={d['n_rows_total']} raw_shape='{d['raw_shape']}' "
            f"cal_shape='{d['cal_shape']}'"
        )


if __name__ == "__main__":
    main()
