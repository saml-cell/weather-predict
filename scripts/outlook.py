#!/usr/bin/env python3
"""Long-range probabilities, reliability, and forecast-revision math.

Network helpers live here too, but every score the API quotes is computed
by the pure functions and stored by ``scripts/backtest_outlook.py``.
"""

from __future__ import annotations

import json
import math
import os
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

from scoring import brier_score, mae

PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DIR = os.path.join(PROJECT, "reports")
WET_MM = 1.0
POOL_DAYS = 7
USER_AGENT = "WeatherPredict/1.0 outlook"


def clamp01(value):
    return min(1.0, max(0.0, float(value)))


def logit(probability):
    p = min(0.999, max(0.001, float(probability)))
    return math.log(p / (1.0 - p))


def sigmoid(value):
    if value >= 40:
        return 1.0
    if value <= -40:
        return 0.0
    return 1.0 / (1.0 + math.exp(-value))


def normal_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def normal_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def crps_normal(mu, sigma, observation):
    """Closed-form CRPS of a normal distribution against one observation."""
    if mu is None or observation is None:
        return None
    sigma = float(sigma) if sigma else 0.0
    if sigma < 1e-6:
        return abs(float(mu) - float(observation))
    z = (float(observation) - float(mu)) / sigma
    return sigma * (z * (2.0 * normal_cdf(z) - 1.0) + 2.0 * normal_pdf(z) - 1.0 / math.sqrt(math.pi))


def mean_crps(mus, sigmas, observations):
    total = 0.0
    n = 0
    for mu, sigma, obs in zip(mus, sigmas, observations):
        score = crps_normal(mu, sigma, obs)
        if score is None:
            continue
        total += score
        n += 1
    if n == 0:
        return None
    return total / n


def brier_skill_score(probabilities, outcomes, climatology_probabilities):
    model = brier_score(probabilities, outcomes)
    reference = brier_score(climatology_probabilities, outcomes)
    if model is None or reference is None or reference <= 1e-12:
        return None
    return 1.0 - model / reference


def reliability_curve(probabilities, outcomes, bins=5):
    """Observed frequency against mean forecast probability in each bin."""
    edges = [i / bins for i in range(bins + 1)]
    rows = []
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        chosen = [
            (p, o) for p, o in zip(probabilities, outcomes)
            if p is not None and o is not None and (lo <= float(p) < hi or (i == bins - 1 and float(p) == 1.0))
        ]
        if not chosen:
            rows.append({"bin_low": lo, "bin_high": hi, "n": 0, "mean_probability": None, "observed_frequency": None})
            continue
        ps = [float(p) for p, _ in chosen]
        os_ = [1.0 if float(o) >= 0.5 else 0.0 for _, o in chosen]
        rows.append({
            "bin_low": lo,
            "bin_high": hi,
            "n": len(chosen),
            "mean_probability": round(sum(ps) / len(ps), 3),
            "observed_frequency": round(sum(os_) / len(os_), 3),
        })
    return rows


def fit_logistic_calibration(probabilities, outcomes):
    """Grid-search Platt scaling. Returns intercept, slope, and training Brier."""
    pairs = [(float(p), 1.0 if float(o) >= 0.5 else 0.0) for p, o in zip(probabilities, outcomes) if p is not None and o is not None]
    if len(pairs) < 20:
        return {"a": 0.0, "b": 1.0, "n": len(pairs), "fitted": False}
    best = None
    for ai in range(-8, 9):
        a = ai * 0.25
        for bi in range(0, 13):
            b = bi * 0.25
            preds = [sigmoid(a + b * logit(p)) for p, _ in pairs]
            score = brier_score(preds, [o for _, o in pairs])
            if best is None or score < best[0]:
                best = (score, a, b)
    return {"a": best[1], "b": best[2], "train_brier": round(best[0], 4), "n": len(pairs), "fitted": True}


def apply_calibration(probability, calibration):
    if probability is None:
        return None
    if not calibration or not calibration.get("fitted"):
        return clamp01(probability)
    return clamp01(sigmoid(calibration["a"] + calibration["b"] * logit(probability)))


def skill_weight(score):
    """Fraction of the model to keep. Negative or missing skill stays at climatology."""
    if score is None:
        return 0.0
    return max(0.0, min(1.0, float(score)))


def skill_label(brier_skill):
    if brier_skill is None:
        return "not_backtested"
    if brier_skill < 0.02:
        return "near_climatology"
    if brier_skill < 0.1:
        return "limited"
    return "useful"


def doy(day):
    return day.timetuple().tm_yday


def circular_distance(a, b):
    gap = abs(int(a) - int(b))
    return min(gap, 366 - gap)


def build_climatology(daily_rows, pool_days=POOL_DAYS):
    """Day-of-year normals from ``{date, tmax, tmin, precip}`` rows.

    Each day of year pools observations within ``pool_days`` so a single year
    of record is not treated as a climate.
    """
    parsed = []
    for row in daily_rows:
        day = row.get("date")
        if isinstance(day, str):
            day = date.fromisoformat(day[:10])
        if day is None or row.get("tmax") is None:
            continue
        parsed.append((day, doy(day), row.get("tmax"), row.get("tmin"), row.get("precip")))
    table = {}
    for target in range(1, 367):
        tmax, tmin, precip = [], [], []
        for _day, yd, high, low, rain in parsed:
            if circular_distance(yd, target) <= pool_days:
                if high is not None:
                    tmax.append(float(high))
                if low is not None:
                    tmin.append(float(low))
                if rain is not None:
                    precip.append(float(rain))
        if len(tmax) < 20:
            continue
        tmax.sort()
        wet = [1.0 if value >= WET_MM else 0.0 for value in precip]
        table[str(target)] = {
            "n_temp": len(tmax),
            "n_precip": len(precip),
            "tmax_mean": round(sum(tmax) / len(tmax), 2),
            "tmax_p10": round(percentile(tmax, 0.10), 2),
            "tmax_p33": round(percentile(tmax, 0.33), 2),
            "tmax_p50": round(percentile(tmax, 0.50), 2),
            "tmax_p67": round(percentile(tmax, 0.67), 2),
            "tmax_p90": round(percentile(tmax, 0.90), 2),
            "tmax_std": round(_std(tmax), 2),
            "tmin_mean": round(sum(tmin) / len(tmin), 2) if tmin else None,
            "p_rain": round(sum(wet) / len(wet), 3) if wet else None,
            "precip_mean": round(sum(precip) / len(precip), 2) if precip else None,
        }
    return table


def percentile(sorted_values, q):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _std(values):
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))


def member_distribution(values):
    nums = sorted(float(v) for v in values if isinstance(v, (int, float)))
    if not nums:
        return None
    return {
        "n": len(nums),
        "mean": sum(nums) / len(nums),
        "p10": percentile(nums, 0.10),
        "p50": percentile(nums, 0.50),
        "p90": percentile(nums, 0.90),
        "std": _std(nums),
    }


def probabilities_from_members(tmax_values, precip_values, climatology, calibration=None):
    """Threshold, tercile and rain probabilities from ensemble members."""
    temps = member_distribution(tmax_values)
    precips = [float(v) for v in precip_values if isinstance(v, (int, float))]
    if temps is None or not climatology:
        return None
    n = temps["n"]
    above = [v for v in tmax_values if isinstance(v, (int, float))]
    p_above_10 = sum(1 for v in above if v >= 10.0) / n
    p_below_0 = sum(1 for v in above if v < 0.0) / n
    p33 = climatology["tmax_p33"]
    p67 = climatology["tmax_p67"]
    p_below = sum(1 for v in above if v < p33) / n
    p_above = sum(1 for v in above if v > p67) / n
    p_near = max(0.0, 1.0 - p_below - p_above)
    p_rain = None
    if precips:
        p_rain = sum(1 for v in precips if v >= WET_MM) / len(precips)
    rain_cal = (calibration or {}).get("rain")
    return {
        "tmax_above_10c": round(p_above_10, 3),
        "tmax_below_0c": round(p_below_0, 3),
        "tercile_below": round(p_below, 3),
        "tercile_near": round(p_near, 3),
        "tercile_above": round(p_above, 3),
        "rain": None if p_rain is None else round(apply_calibration(p_rain, rain_cal), 3),
        "tmax_p10": round(temps["p10"], 1),
        "tmax_p50": round(temps["p50"], 1),
        "tmax_p90": round(temps["p90"], 1),
        "tmax_mean": round(temps["mean"], 1),
        "members": n,
    }


def blend_probabilities(model, climatology, weight, rain_weight=None):
    """Pull model probabilities toward the day-of-year climatology."""
    if model is None:
        model = {}
    weight = max(0.0, min(1.0, float(weight)))
    rain_weight = weight if rain_weight is None else max(0.0, min(1.0, float(rain_weight)))
    p_rain_c = climatology.get("p_rain")
    def mix(model_value, clim_value):
        if model_value is None:
            return clim_value
        if clim_value is None:
            return model_value
        return weight * float(model_value) + (1.0 - weight) * float(clim_value)
    tercile_clim = 1.0 / 3.0
    blended = {
        "tmax_above_10c": _round3(mix(model.get("tmax_above_10c"), _clim_rate_above(climatology, 10.0))),
        "tmax_below_0c": _round3(mix(model.get("tmax_below_0c"), _clim_rate_below(climatology, 0.0))),
        "tercile_below": _round3(mix(model.get("tercile_below"), tercile_clim)),
        "tercile_near": _round3(mix(model.get("tercile_near"), tercile_clim)),
        "tercile_above": _round3(mix(model.get("tercile_above"), tercile_clim)),
        "rain": _round3(rain_weight * float(model["rain"]) + (1.0 - rain_weight) * float(p_rain_c) if model.get("rain") is not None and p_rain_c is not None else mix(model.get("rain"), p_rain_c)),
        "tmax_p10": _round1(mix(model.get("tmax_p10"), climatology.get("tmax_p10"))),
        "tmax_p50": _round1(mix(model.get("tmax_p50"), climatology.get("tmax_p50"))),
        "tmax_p90": _round1(mix(model.get("tmax_p90"), climatology.get("tmax_p90"))),
        "tmax_mean": _round1(mix(model.get("tmax_mean"), climatology.get("tmax_mean"))),
        "members": model.get("members"),
        "climatology_weight": round(1.0 - weight, 3),
        "model_weight": round(weight, 3),
        "rain_model_weight": round(rain_weight, 3),
    }
    return blended


def _clim_rate_above(climatology, threshold):
    """Rough normal-tail rate from the pooled day-of-year distribution."""
    mean = climatology.get("tmax_mean")
    std = climatology.get("tmax_std") or 1.0
    if mean is None:
        return None
    z = (threshold - mean) / std
    return 1.0 - normal_cdf(z)


def _clim_rate_below(climatology, threshold):
    mean = climatology.get("tmax_mean")
    std = climatology.get("tmax_std") or 1.0
    if mean is None:
        return None
    return normal_cdf((threshold - mean) / std)


def _round3(value):
    return None if value is None else round(float(value), 3)


def _round1(value):
    return None if value is None else round(float(value), 1)


def lead_bucket(lead_days):
    lead = max(0, int(lead_days))
    if lead <= 7:
        return "1-7"
    if lead <= 14:
        return "8-14"
    if lead <= 46:
        return "15-46"
    return "47-180"


def source_for_lead(lead_days):
    lead = int(lead_days)
    if lead <= 14:
        return "ecmwf_ifs_ensemble"
    return "ecmwf_seas5"


def revision_summary(revisions):
    vals = [float(v) for v in revisions if v is not None]
    if not vals:
        return None
    return {
        "n": len(vals),
        "mean_c": round(sum(vals) / len(vals), 3),
        "mae_c": round(sum(abs(v) for v in vals) / len(vals), 3),
        "std_c": round(_std(vals), 3),
    }


def within_rate(revisions, half_width=1.5):
    vals = [float(v) for v in revisions if v is not None]
    if not vals:
        return None
    return sum(1 for v in vals if abs(v) <= half_width) / len(vals)


def flip_rate(early_wet, late_wet):
    pairs = [(a, b) for a, b in zip(early_wet, late_wet) if a is not None and b is not None]
    if not pairs:
        return None
    return sum(1 for a, b in pairs if bool(a) != bool(b)) / len(pairs)


def linear_fit(xs, ys):
    pairs = [(float(x), float(y)) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pairs) < 10:
        return None
    mx = sum(x for x, _ in pairs) / len(pairs)
    my = sum(y for _, y in pairs) / len(pairs)
    var = sum((x - mx) ** 2 for x, _ in pairs)
    if var <= 1e-12:
        return {"intercept": my, "slope": 0.0, "n": len(pairs)}
    slope = sum((x - mx) * (y - my) for x, y in pairs) / var
    return {"intercept": my - slope * mx, "slope": slope, "n": len(pairs)}


def predict_linear(fit, x):
    if fit is None or x is None:
        return None
    return fit["intercept"] + fit["slope"] * float(x)


def feature_skill(issue_values, target_anomalies, train_fraction=0.7):
    """Out-of-sample MAE of a one-predictor regression against predicting zero."""
    pairs = [(float(x), float(y)) for x, y in zip(issue_values, target_anomalies) if x is not None and y is not None]
    if len(pairs) < 40:
        return {"kept": False, "reason": "too_few_samples", "n": len(pairs)}
    cut = max(20, int(len(pairs) * train_fraction))
    train, test = pairs[:cut], pairs[cut:]
    if len(test) < 15:
        return {"kept": False, "reason": "too_few_test_samples", "n": len(pairs)}
    fit = linear_fit([x for x, _ in train], [y for _, y in train])
    preds = [predict_linear(fit, x) for x, _ in test]
    actual = [y for _, y in test]
    model_mae = mae(preds, actual)
    clim_mae = mae([0.0] * len(actual), actual)
    if model_mae is None or clim_mae is None:
        return {"kept": False, "reason": "undefined_mae", "n": len(pairs)}
    improvement = clim_mae - model_mae
    kept = improvement >= 0.05
    return {
        "kept": kept,
        "n_train": len(train),
        "n_test": len(test),
        "mae_model_c": round(model_mae, 3),
        "mae_climatology_c": round(clim_mae, 3),
        "improvement_c": round(improvement, 3),
        "slope": None if fit is None else round(fit["slope"], 4),
        "reason": "improves_test_mae" if kept else "no_meaningful_improvement",
    }


def http_json(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def load_json_report(*names):
    for name in names:
        path = os.path.join(REPORT_DIR, name)
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as handle:
                return json.load(handle)
    return None


def bucket_skill(report, lead_days):
    if not report:
        return None
    bucket = lead_bucket(lead_days)
    return (report.get("by_lead") or {}).get(bucket)


def _member_keys(daily, prefix):
    return [key for key in daily if key == prefix or key.startswith(prefix + "_member")]


def fetch_member_values(lat, lon, target, lead_days):
    """Return (tmax_members, precip_members, source_name) for one date."""
    day = target.isoformat()
    if lead_days <= 14:
        url = "https://ensemble-api.open-meteo.com/v1/ensemble?" + urllib.parse.urlencode({
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,precipitation_sum",
            "models": "ecmwf_ifs025",
            "forecast_days": min(16, max(1, lead_days + 1)),
            "timezone": "Europe/Bratislava",
        })
        source = "ecmwf_ifs_ensemble"
    else:
        url = "https://seasonal-api.open-meteo.com/v1/seasonal?" + urllib.parse.urlencode({
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,precipitation_sum",
            "models": "ecmwf_seas5",
            "timezone": "Europe/Bratislava",
        })
        source = "ecmwf_seas5"
    try:
        daily = http_json(url, timeout=60).get("daily") or {}
    except (OSError, json.JSONDecodeError, ValueError):
        return [], [], source
    times = daily.get("time") or []
    if day not in times:
        return [], [], source
    index = times.index(day)
    tmax, precip = [], []
    for key in _member_keys(daily, "temperature_2m_max"):
        values = daily.get(key) or []
        if index < len(values) and isinstance(values[index], (int, float)):
            tmax.append(float(values[index]))
    for key in _member_keys(daily, "precipitation_sum"):
        values = daily.get(key) or []
        if index < len(values) and isinstance(values[index], (int, float)):
            precip.append(float(values[index]))
    return tmax, precip, source


def compose_outlook(target, climatology_row, members_tmax, members_precip, source, skill_row, calibration=None):
    """Build one date's public outlook from already-fetched numbers."""
    lead = None
    model = probabilities_from_members(members_tmax, members_precip, climatology_row or {}, calibration)
    temp_skill = None if not skill_row else skill_row.get("crps_skill_score")
    rain_skill = None if not skill_row else skill_row.get("rain_brier_skill_score")
    if temp_skill is None and rain_skill is None and skill_row and skill_row.get("status") == "too_few_samples":
        temp_skill = None
        rain_skill = None
    weight = skill_weight(temp_skill)
    rain_w = skill_weight(rain_skill)
    blended = blend_probabilities(model, climatology_row or {}, weight, rain_weight=rain_w)
    label = skill_label(temp_skill if temp_skill is not None else rain_skill)
    if label == "near_climatology":
        message = "Backtested skill at this lead is near climatology, so the probabilities stay close to the day-of-year normal."
    elif label == "not_backtested":
        message = "This lead has no committed backtest, so the answer is the day-of-year climatology."
        blended = blend_probabilities(None, climatology_row or {}, 0.0)
    elif label == "limited":
        message = "Backtested skill at this lead is limited. The model is only a partial shift away from climatology."
    else:
        message = "Backtested skill at this lead is high enough to keep most of the model probability."
    return {
        "source": source,
        "probabilities": blended,
        "skill": {
            "label": label,
            "crps_skill_score": None if temp_skill is None else round(float(temp_skill), 3),
            "rain_brier_skill_score": None if rain_skill is None else round(float(rain_skill), 3),
            "model_weight": weight,
            "message": message,
            "backtest": skill_row,
        },
    }


def compose_drift(today_tmax, today_rain_probability, drift_report, horizon_days=7):
    """Apply the committed revision statistics to today's forecast."""
    revision = (drift_report or {}).get("revision_c") or {}
    held = (drift_report or {}).get("held_out") or {}
    mean = revision.get("mean_c")
    std = revision.get("std_c")
    if today_tmax is None or mean is None or std is None:
        return {
            "available": False,
            "message": "Drift statistics are not available yet.",
        }
    predicted = today_tmax + mean
    return {
        "available": True,
        "horizon_days": horizon_days,
        "today_forecast_tmax_c": round(float(today_tmax), 1),
        "predicted_forecast_tmax_c": round(predicted, 1),
        "expected_revision_c": mean,
        "uncertainty_low_c": round(predicted - 1.96 * std, 1),
        "uncertainty_high_c": round(predicted + 1.96 * std, 1),
        "probability_within_1_5c": held.get("probability_within_1_5c"),
        "probability_rain_call_flips": held.get("probability_rain_flips"),
        "today_rain_probability": today_rain_probability,
        "reliability": {
            "brier_within_1_5c": held.get("brier_within_1_5c"),
            "brier_rain_flips": held.get("brier_rain_flips"),
            "n_test": held.get("n"),
            "note": "These probabilities are the held-out training rates from the committed drift backtest, not a new fit for this single day.",
        },
        "bias_direction": drift_report.get("bias_direction"),
        "convergence": drift_report.get("convergence"),
        "spread_proxy": drift_report.get("spread_proxy"),
    }
