#!/usr/bin/env python3
"""
Seasonal Forecasting Models.

Implements four prediction methods and Bayesian Model Averaging:
1. Analog Forecasting — find similar historical teleconnection states
2. Ridge Regression — linear prediction from lagged indices (numpy/scipy)
3. Composite Analysis — phase-based historical grouping
4. ECMWF SEAS5 — dynamical model ensemble via Open-Meteo Seasonal API
5. BMA — Bayesian Model Averaging to combine all methods

All methods predict monthly temperature and precipitation ANOMALIES
relative to 30-year climatology.
"""

import calendar
import json
import math
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

import numpy as np
from scipy import stats as sp_stats

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from climate_indices import (
    build_predictor_vector,
    classify_enso,
    classify_binary,
    fetch_url,
    get_current_index_state,
)

# ---------------------------------------------------------------------------
# Monthly anomaly cache (persisted to disk)
# ---------------------------------------------------------------------------
_MONTHLY_ANOMALY_CACHE = {}
_CACHE_FILE = Path(__file__).parent.parent / "data" / "monthly_anomalies_cache.json"

def _load_anomaly_cache():
    """Load monthly anomaly cache from disk."""
    global _MONTHLY_ANOMALY_CACHE
    if _CACHE_FILE.exists():
        try:
            with open(_CACHE_FILE, "r") as f:
                raw = json.load(f)
            # Keys are stored as strings; convert back
            _MONTHLY_ANOMALY_CACHE = raw
        except Exception:
            _MONTHLY_ANOMALY_CACHE = {}

def _save_anomaly_cache():
    """Save monthly anomaly cache to disk."""
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_FILE, "w") as f:
            json.dump(_MONTHLY_ANOMALY_CACHE, f)
    except Exception:
        pass

# Load cache at import time
_load_anomaly_cache()


def get_observed_monthly_anomaly(lat, lon, year, month, clim_temp, clim_precip=None):
    """Fetch actual observed monthly weather from Open-Meteo Archive API.

    Args:
        lat, lon: Coordinates.
        year, month: Historical year/month to fetch.
        clim_temp: Climatological mean temperature for this month.
        clim_precip: Climatological mean precipitation for this month (optional).

    Returns:
        (temp_anomaly, precip_anomaly) or None on failure.
        temp_anomaly = observed_monthly_mean - clim_temp
        precip_anomaly = ((observed_total - clim_precip) / clim_precip) * 100  (%)
    """
    # Round coords for cache key stability
    cache_key = f"{round(lat,2)},{round(lon,2)},{year},{month}"
    if cache_key in _MONTHLY_ANOMALY_CACHE:
        cached = _MONTHLY_ANOMALY_CACHE[cache_key]
        temp_anom = cached["temp_mean"] - clim_temp
        precip_anom = 0.0
        if clim_precip and clim_precip > 0 and cached.get("precip_total") is not None:
            precip_anom = ((cached["precip_total"] - clim_precip) / clim_precip) * 100
        return (temp_anom, precip_anom)

    # Don't fetch future data
    now = datetime.utcnow()
    if year > now.year or (year == now.year and month >= now.month):
        return None

    last_day = calendar.monthrange(year, month)[1]
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={year}-{month:02d}-01"
        f"&end_date={year}-{month:02d}-{last_day}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&timezone=auto"
    )

    try:
        text = fetch_url(url, timeout=30)
        data = json.loads(text)
    except Exception:
        return None

    daily = data.get("daily", {})
    t_max = daily.get("temperature_2m_max", [])
    t_min = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])

    if not t_max or not t_min:
        return None

    # Compute monthly mean temp as average of (max+min)/2
    daily_means = []
    for hi, lo in zip(t_max, t_min):
        if hi is not None and lo is not None:
            daily_means.append((hi + lo) / 2.0)
    if not daily_means:
        return None

    temp_mean = sum(daily_means) / len(daily_means)
    precip_total = sum(p for p in precip if p is not None) if precip else None

    # Cache the raw observed values (not anomalies, since climatology may vary)
    _MONTHLY_ANOMALY_CACHE[cache_key] = {
        "temp_mean": round(temp_mean, 2),
        "precip_total": round(precip_total, 1) if precip_total is not None else None,
    }
    _save_anomaly_cache()

    temp_anom = temp_mean - clim_temp
    precip_anom = 0.0
    if clim_precip and clim_precip > 0 and precip_total is not None:
        precip_anom = ((precip_total - clim_precip) / clim_precip) * 100

    return (temp_anom, precip_anom)


# ---------------------------------------------------------------------------
# Tercile thresholds (standard normal deviates for 33rd/67th percentiles)
# ---------------------------------------------------------------------------
TERCILE_Z_LOW = -0.4307  # P(Z < -0.4307) = 1/3
TERCILE_Z_HIGH = 0.4307  # P(Z > 0.4307) = 1/3


# ===================================================================
# 1. ANALOG FORECASTING
# ===================================================================
def analog_forecast(city_id, lat, lon, target_year, target_month,
                    climatology, index_series_cache=None):
    """Find historical analogs and predict weather anomalies.

    Args:
        city_id: Database city ID.
        lat, lon: City coordinates.
        target_year, target_month: What we're predicting.
        climatology: {month: {temp_high_mean, temp_high_std, ...}} from DB.
        index_series_cache: Optional pre-loaded index data.

    Returns: dict with temp_anomaly_c, precip_anomaly_pct, confidence,
             analogs list, tercile_probs, spread.
    """
    cfg = db.load_config().get("seasonal", {}).get("analog", {})
    predictor_indices = cfg.get("predictor_indices", ["oni", "nao", "ao", "pdo", "amo", "pna"])
    lags = cfg.get("predictor_lags_months", [0, 1, 2, 3])
    num_analogs = cfg.get("num_analogs", 10)
    index_weights = cfg.get("index_weights", {})

    # Build current predictor vector
    current_vec, labels = build_predictor_vector(
        target_year, target_month, predictor_indices, lags, index_series_cache)

    if current_vec is None:
        return _empty_forecast("analog", "Insufficient index data for current period")

    # Build weight vector for distance calculation
    weight_vec = []
    for label in labels:
        idx_name = label.split("_lag")[0]
        weight_vec.append(index_weights.get(idx_name, 1.0))
    weight_vec = np.array(weight_vec)
    current_vec = np.array(current_vec)

    # Get climatology for target month
    clim = climatology.get(target_month)
    if not clim or clim.get("temp_high_mean") is None:
        return _empty_forecast("analog", "No climatology for target month")

    # Search historical years
    # Use data from 1950 to 2 years before target (avoid leakage)
    search_start = 1950
    search_end = target_year - 1

    analogs = []
    for hist_year in range(search_start, search_end + 1):
        hist_vec, _ = build_predictor_vector(
            hist_year, target_month, predictor_indices, lags, index_series_cache)
        if hist_vec is None:
            continue

        hist_vec = np.array(hist_vec)
        # Weighted Euclidean distance
        diff = current_vec - hist_vec
        distance = float(np.sqrt(np.sum(weight_vec * diff ** 2)))

        analogs.append({
            "year": hist_year,
            "distance": distance,
        })

    if len(analogs) < cfg.get("min_analogs", 5):
        return _empty_forecast("analog", f"Only {len(analogs)} analogs found (need {cfg.get('min_analogs', 5)})")

    # Sort by distance, take top N
    analogs.sort(key=lambda x: x["distance"])
    analogs = analogs[:num_analogs]

    # Gaussian kernel weighting
    distances = np.array([a["distance"] for a in analogs])
    sigma = float(np.median(distances)) if np.median(distances) > 0 else 1.0
    kernel_weights = np.exp(-distances ** 2 / (2 * sigma ** 2))
    kernel_weights /= kernel_weights.sum()

    # For each analog year, fetch the actual observed weather anomaly.
    # Falls back to index-based synthetic proxy only if API call fails.
    clim_temp_mean = (clim["temp_high_mean"] + clim.get("temp_low_mean", clim["temp_high_mean"])) / 2.0
    clim_precip_mean = clim.get("precip_mean")

    temp_anomalies = []
    precip_anomalies = []

    for a in analogs:
        # Try real observed anomaly first
        obs = get_observed_monthly_anomaly(
            lat, lon, a["year"], target_month, clim_temp_mean, clim_precip_mean)

        if obs is not None:
            temp_anomalies.append(obs[0])
            precip_anomalies.append(obs[1])
            continue

        # Fallback: synthetic proxy from index values
        hist_vec, _ = build_predictor_vector(
            a["year"], target_month, predictor_indices, lags, index_series_cache)
        if hist_vec is None:
            temp_anomalies.append(0.0)
            precip_anomalies.append(0.0)
            continue

        hv = np.array(hist_vec)
        oni_val = hv[0] if len(hv) > 0 else 0.0
        nao_val = hv[len(lags)] if len(hv) > len(lags) else 0.0

        abs_lat = abs(lat)
        if abs_lat < 23.5:
            enso_sensitivity = 0.5
        elif abs_lat < 45:
            enso_sensitivity = 0.3
        else:
            enso_sensitivity = 0.2

        nao_sensitivity = 0.0
        if -30 < lon < 60 and 30 < lat < 70:
            nao_sensitivity = 0.4

        temp_anom = oni_val * enso_sensitivity + nao_val * nao_sensitivity
        precip_anom = -oni_val * 5.0

        temp_anomalies.append(temp_anom)
        precip_anomalies.append(precip_anom)

    temp_anomalies = np.array(temp_anomalies)
    precip_anomalies = np.array(precip_anomalies)

    # Weighted forecast
    temp_forecast = float(np.sum(kernel_weights * temp_anomalies))
    precip_forecast = float(np.sum(kernel_weights * precip_anomalies))

    # Spread (uncertainty)
    temp_spread = float(np.sqrt(np.sum(kernel_weights * (temp_anomalies - temp_forecast) ** 2)))
    if temp_spread < 0.3:
        temp_spread = 0.3  # minimum uncertainty

    # Tercile probabilities
    tercile_probs = _compute_tercile_probs(
        temp_forecast, temp_spread,
        clim["temp_high_std"] if clim.get("temp_high_std") else 1.0)

    # Confidence from analog agreement
    confidence = max(0.1, min(0.9, 1.0 - temp_spread / 3.0))

    return {
        "method": "analog",
        "temp_anomaly_c": round(temp_forecast, 2),
        "precip_anomaly_pct": round(precip_forecast, 1),
        "confidence": round(confidence, 3),
        "spread": round(temp_spread, 2),
        "tercile_probs": tercile_probs,
        "analogs": [
            {"year": a["year"], "distance": round(a["distance"], 3)}
            for a in analogs[:5]  # top 5 for display
        ],
        "num_analogs_used": len(analogs),
    }


# ===================================================================
# 2. RIDGE REGRESSION
# ===================================================================
def ridge_regression_forecast(city_id, lat, lon, target_year, target_month,
                              climatology, index_series_cache=None):
    """Fit ridge regression and predict anomalies.

    Uses historical index values to predict weather anomalies via
    beta = (X'X + lambda*I)^(-1) X'y
    """
    cfg = db.load_config().get("seasonal", {}).get("regression", {})
    predictor_indices = cfg.get("predictor_indices", ["oni", "nao", "ao", "pdo", "amo", "pna"])
    lags = cfg.get("predictor_lags_months", [0, 1, 2])
    ridge_lambda = cfg.get("ridge_lambda", 1.0)
    min_years = cfg.get("min_training_years", 20)

    clim = climatology.get(target_month)
    if not clim or clim.get("temp_high_mean") is None:
        return _empty_forecast("regression", "No climatology")

    # Build training data
    # X: predictor matrix (each row is a year's index vector)
    # y: target variable (actual observed temperature anomaly for that year/month)
    clim_temp_mean = (clim["temp_high_mean"] + clim.get("temp_low_mean", clim["temp_high_mean"])) / 2.0
    clim_precip_mean = clim.get("precip_mean")

    X_rows = []
    y_temp = []
    y_precip = []
    training_years = []

    for hist_year in range(1950, target_year):
        vec, _ = build_predictor_vector(
            hist_year, target_month, predictor_indices, lags, index_series_cache)
        if vec is None:
            continue

        # Use actual observed anomaly as training target
        obs = get_observed_monthly_anomaly(
            lat, lon, hist_year, target_month, clim_temp_mean, clim_precip_mean)

        if obs is not None:
            X_rows.append(vec)
            y_temp.append(obs[0])
            y_precip.append(obs[1])
            training_years.append(hist_year)
        # Skip years without real observations — synthetic targets degrade skill

    if len(X_rows) < min_years:
        return _empty_forecast("regression", f"Only {len(X_rows)} training years (need {min_years})")

    X = np.array(X_rows)
    y = np.array(y_temp)
    n, p = X.shape

    # Ridge regression: beta = (X'X + lambda*I)^(-1) X'y
    XtX = X.T @ X
    Xty = X.T @ y
    I = np.eye(p)
    try:
        beta = np.linalg.solve(XtX + ridge_lambda * I, Xty)
    except np.linalg.LinAlgError:
        return _empty_forecast("regression", "Matrix solve failed")

    # Current prediction
    current_vec, labels = build_predictor_vector(
        target_year, target_month, predictor_indices, lags, index_series_cache)
    if current_vec is None:
        return _empty_forecast("regression", "Insufficient current index data")

    x_current = np.array(current_vec)
    temp_forecast = float(x_current @ beta)

    # Leave-one-out cross-validation for uncertainty
    residuals = []
    for i in range(n):
        X_loo = np.delete(X, i, axis=0)
        y_loo = np.delete(y, i)
        try:
            beta_loo = np.linalg.solve(X_loo.T @ X_loo + ridge_lambda * np.eye(p),
                                       X_loo.T @ y_loo)
            pred = float(X[i] @ beta_loo)
            residuals.append(y[i] - pred)
        except np.linalg.LinAlgError:
            continue

    if residuals:
        residual_std = float(np.std(residuals))
        r2_cv = 1.0 - np.var(residuals) / np.var(y) if np.var(y) > 0 else 0.0
    else:
        residual_std = 1.0
        r2_cv = 0.0

    residual_std = max(residual_std, 0.3)

    # Precipitation regression — train a separate model on real precip observations
    y_p = np.array(y_precip)
    if len(y_p) > min_years and np.var(y_p) > 0:
        try:
            beta_p = np.linalg.solve(XtX + ridge_lambda * I, X.T @ y_p)
            precip_forecast = float(x_current @ beta_p)
        except np.linalg.LinAlgError:
            precip_forecast = 0.0
    else:
        precip_forecast = 0.0

    # Tercile probabilities
    tercile_probs = _compute_tercile_probs(
        temp_forecast, residual_std,
        clim["temp_high_std"] if clim.get("temp_high_std") else 1.0)

    # Confidence from cross-validated R^2
    confidence = max(0.1, min(0.9, float(r2_cv)))

    # Feature importance (absolute beta values, normalized)
    abs_beta = np.abs(beta)
    if abs_beta.sum() > 0:
        importance = abs_beta / abs_beta.sum()
    else:
        importance = np.ones(p) / p

    return {
        "method": "regression",
        "temp_anomaly_c": round(temp_forecast, 2),
        "precip_anomaly_pct": round(precip_forecast, 1),
        "confidence": round(confidence, 3),
        "spread": round(residual_std, 2),
        "tercile_probs": tercile_probs,
        "r_squared_cv": round(float(r2_cv), 3),
        "training_years": len(training_years),
        "top_predictors": [
            {"name": labels[i] if labels else f"feat_{i}",
             "importance": round(float(importance[i]), 3)}
            for i in np.argsort(-importance)[:5]
        ] if labels else [],
    }


# ===================================================================
# 3. COMPOSITE ANALYSIS
# ===================================================================
def composite_forecast(city_id, lat, lon, target_year, target_month,
                       climatology, index_series_cache=None):
    """Composite analysis based on current teleconnection phases.

    Groups historical years by phase combination, computes mean anomaly.
    """
    cfg = db.load_config().get("seasonal", {}).get("composite", {})
    thresholds = cfg.get("thresholds", {})
    min_per_bin = cfg.get("min_years_per_bin", 5)

    clim = climatology.get(target_month)
    if not clim or clim.get("temp_high_mean") is None:
        return _empty_forecast("composite", "No climatology")

    # Get current phase
    state = get_current_index_state()
    oni_val = state.get("oni", {}).get("value")
    nao_val = state.get("nao", {}).get("value")
    ao_val = state.get("ao", {}).get("value")

    if oni_val is None:
        return _empty_forecast("composite", "ONI not available")

    # Classify current state
    oni_phase, oni_strength = classify_enso(oni_val)
    nao_phase = classify_binary(nao_val) if nao_val is not None else "neutral"
    ao_phase = classify_binary(ao_val) if ao_val is not None else "neutral"

    # Load ONI history
    oni_series = index_series_cache.get("oni") if index_series_cache else db.get_climate_index_series("oni")
    nao_series = index_series_cache.get("nao") if index_series_cache else db.get_climate_index_series("nao")

    # Find years matching current ENSO phase
    oni_thresh = thresholds.get("oni", {"pos": 0.5, "neg": -0.5})
    matching_years = []

    for hist_year in range(1950, target_year):
        hist_oni = oni_series.get((hist_year, target_month))
        if hist_oni is None:
            continue

        # Check if same ENSO phase
        if oni_val >= oni_thresh.get("pos", 0.5):
            if hist_oni < oni_thresh.get("pos", 0.5):
                continue
        elif oni_val <= oni_thresh.get("neg", -0.5):
            if hist_oni > oni_thresh.get("neg", -0.5):
                continue
        else:  # neutral
            if hist_oni >= oni_thresh.get("pos", 0.5) or hist_oni <= oni_thresh.get("neg", -0.5):
                continue

        # Optionally filter by NAO phase too
        hist_nao = nao_series.get((hist_year, target_month)) if nao_series else None
        nao_match = True
        if hist_nao is not None and nao_val is not None:
            hist_nao_phase = classify_binary(hist_nao)
            if hist_nao_phase != nao_phase:
                nao_match = False

        matching_years.append({
            "year": hist_year,
            "oni": hist_oni,
            "nao_match": nao_match,
        })

    if len(matching_years) < min_per_bin:
        # Fall back to ENSO-only composite (ignore NAO matching)
        matching_years_enso_only = [y for y in matching_years]
        if len(matching_years_enso_only) < min_per_bin:
            return _empty_forecast("composite",
                                   f"Only {len(matching_years_enso_only)} matching years")
        use_nao = False
    else:
        # Try with NAO filter
        nao_matched = [y for y in matching_years if y["nao_match"]]
        if len(nao_matched) >= min_per_bin:
            matching_years = nao_matched
            use_nao = True
        else:
            use_nao = False

    # Compute composite anomaly from matching years' actual observations
    clim_temp_mean = (clim["temp_high_mean"] + clim.get("temp_low_mean", clim["temp_high_mean"])) / 2.0
    clim_precip_mean = clim.get("precip_mean")

    temp_anomalies = []
    precip_anomalies_comp = []
    valid_years = []
    for my in matching_years:
        obs = get_observed_monthly_anomaly(
            lat, lon, my["year"], target_month, clim_temp_mean, clim_precip_mean)

        if obs is not None:
            temp_anomalies.append(obs[0])
            precip_anomalies_comp.append(obs[1])
            valid_years.append(my)
        # Skip years without real observations — synthetic proxies hurt skill

    if len(temp_anomalies) < 3:
        return _empty_forecast("composite",
                               f"Only {len(temp_anomalies)} years with real observations")

    matching_years = valid_years
    temp_anomalies = np.array(temp_anomalies)
    precip_anomalies_comp = np.array(precip_anomalies_comp)
    temp_forecast = float(np.mean(temp_anomalies))
    temp_spread = float(np.std(temp_anomalies)) if len(temp_anomalies) > 1 else 1.0
    temp_spread = max(temp_spread, 0.3)

    precip_forecast = float(np.mean(precip_anomalies_comp)) if len(precip_anomalies_comp) > 0 else -temp_forecast * 3.0

    tercile_probs = _compute_tercile_probs(
        temp_forecast, temp_spread,
        clim["temp_high_std"] if clim.get("temp_high_std") else 1.0)

    confidence = max(0.1, min(0.8, len(matching_years) / 30.0))

    return {
        "method": "composite",
        "temp_anomaly_c": round(temp_forecast, 2),
        "precip_anomaly_pct": round(precip_forecast, 1),
        "confidence": round(confidence, 3),
        "spread": round(temp_spread, 2),
        "tercile_probs": tercile_probs,
        "phase_description": f"ENSO: {oni_phase}" + (f", NAO: {nao_phase}" if use_nao else ""),
        "matching_years": len(matching_years),
        "sample_years": [y["year"] for y in matching_years[:10]],
    }


# ===================================================================
# 4. ECMWF SEAS5 (Open-Meteo Seasonal API)
# ===================================================================
def ecmwf_seasonal_forecast(city_id, lat, lon, target_year, target_month,
                            climatology):
    """Fetch ECMWF SEAS5 seasonal forecast and convert to anomalies.

    Uses Open-Meteo Seasonal API (free, no key).
    """
    cfg = db.load_config().get("seasonal", {})
    base_url = cfg.get("ecmwf_seasonal_url",
                       "https://seasonal-api.open-meteo.com/v1/seasonal")

    clim = climatology.get(target_month)
    if not clim or clim.get("temp_high_mean") is None:
        return _empty_forecast("ecmwf_seas5", "No climatology")

    url = (
        f"{base_url}?latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&models=ecmwf_seas5"
    )

    try:
        text = fetch_url(url, timeout=60)
        data = json.loads(text)
    except Exception as e:
        return _empty_forecast("ecmwf_seas5", f"API error: {e}")

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    t_max_all = daily.get("temperature_2m_max", [])
    t_min_all = daily.get("temperature_2m_min", [])
    precip_all = daily.get("precipitation_sum", [])

    if not dates:
        return _empty_forecast("ecmwf_seas5", "No forecast data returned")

    # Filter to target month
    target_prefix = f"{target_year}-{target_month:02d}"
    t_max_month = []
    t_min_month = []
    precip_month = []

    for i, d in enumerate(dates):
        if d.startswith(target_prefix):
            if i < len(t_max_all) and t_max_all[i] is not None:
                t_max_month.append(t_max_all[i])
            if i < len(t_min_all) and t_min_all[i] is not None:
                t_min_month.append(t_min_all[i])
            if i < len(precip_all) and precip_all[i] is not None:
                precip_month.append(precip_all[i])

    if not t_max_month:
        # Target month may be beyond forecast horizon
        return _empty_forecast("ecmwf_seas5",
                               f"No data for {target_prefix} (beyond forecast horizon)")

    # Compute monthly averages from ensemble
    avg_high = sum(t_max_month) / len(t_max_month)
    avg_low = sum(t_min_month) / len(t_min_month) if t_min_month else None
    total_precip = sum(precip_month) if precip_month else None

    # Convert to anomalies
    temp_anomaly = avg_high - clim["temp_high_mean"]

    precip_anomaly = 0.0
    if total_precip is not None and clim.get("precip_mean") and clim["precip_mean"] > 0:
        precip_anomaly = ((total_precip - clim["precip_mean"]) / clim["precip_mean"]) * 100

    # Estimate spread from daily variability
    if len(t_max_month) > 1:
        temp_spread = float(np.std(t_max_month))
    else:
        temp_spread = 1.0
    temp_spread = max(temp_spread, 0.3)

    tercile_probs = _compute_tercile_probs(
        temp_anomaly, temp_spread,
        clim["temp_high_std"] if clim.get("temp_high_std") else 1.0)

    # ECMWF SEAS5 confidence: base from ensemble agreement, decays with lead time
    # Narrower spread = more confident. Normalize against climatological std.
    clim_std = clim.get("temp_high_std", 3.0) or 3.0
    spread_ratio = temp_spread / clim_std if clim_std > 0 else 1.0
    # Low spread_ratio (<0.5) = ensemble agrees well → high confidence
    # High spread_ratio (>1.5) = no skill → low confidence
    base_confidence = max(0.1, min(0.8, 1.0 - spread_ratio * 0.5))

    # Lead time penalty: compute months between now and target
    now = datetime.utcnow()
    lead_approx = (target_year - now.year) * 12 + (target_month - now.month)
    lead_approx = max(1, lead_approx)
    lead_penalty = math.exp(-0.15 * lead_approx)
    confidence = base_confidence * lead_penalty

    return {
        "method": "ecmwf_seas5",
        "temp_anomaly_c": round(temp_anomaly, 2),
        "precip_anomaly_pct": round(precip_anomaly, 1),
        "confidence": round(confidence, 3),
        "spread": round(temp_spread, 2),
        "tercile_probs": tercile_probs,
        "ensemble_mean_high": round(avg_high, 1),
        "climatology_high": round(clim["temp_high_mean"], 1),
        "days_in_forecast": len(t_max_month),
    }


# ===================================================================
# 5. BAYESIAN MODEL AVERAGING (BMA)
# ===================================================================
def bayesian_model_average(method_forecasts):
    """Combine multiple method forecasts using BMA.

    Args:
        method_forecasts: list of forecast dicts from each method.
            Each must have: temp_anomaly_c, precip_anomaly_pct, confidence,
            spread, tercile_probs.

    Returns: combined forecast dict.
    """
    valid = [f for f in method_forecasts if f.get("temp_anomaly_c") is not None]
    if not valid:
        return _empty_forecast("bma", "No valid method forecasts")

    if len(valid) == 1:
        result = dict(valid[0])
        result["method"] = "bma"
        result["method_weights"] = {valid[0]["method"]: 1.0}
        return result

    # Compute weights from confidence and spread
    # Lower spread + higher confidence = higher weight
    raw_weights = []
    for f in valid:
        spread = max(f.get("spread", 1.0), 0.1)
        conf = max(f.get("confidence", 0.1), 0.01)
        # Log-likelihood proxy: -log(spread) + log(conf)
        w = conf / spread
        raw_weights.append(w)

    raw_weights = np.array(raw_weights)

    # Cold start fallback: when all methods have equal confidence/spread,
    # use the configured initial_weights from config.json
    if np.max(raw_weights) - np.min(raw_weights) < 1e-6:
        bma_cfg = db.load_config().get("seasonal", {}).get("bma", {})
        config_weights = bma_cfg.get("initial_weights", [0.30, 0.25, 0.20, 0.25])
        method_order = bma_cfg.get("methods", ["analog", "regression", "composite", "ecmwf_seas5"])
        # Map config weights to valid methods by name
        cfg_weight_map = dict(zip(method_order, config_weights))
        mapped = [cfg_weight_map.get(f["method"], 1.0 / len(valid)) for f in valid]
        mapped = np.array(mapped)
        weights = mapped / mapped.sum()
    else:
        weights = raw_weights / raw_weights.sum()

    # Weighted means
    temp_means = np.array([f["temp_anomaly_c"] for f in valid])
    precip_means = np.array([f["precip_anomaly_pct"] for f in valid])
    spreads = np.array([f.get("spread", 1.0) for f in valid])

    mu_temp = float(np.sum(weights * temp_means))
    mu_precip = float(np.sum(weights * precip_means))

    # BMA variance: sum(w * (sigma^2 + mu^2)) - mu_bma^2
    sigma_bma_sq = float(np.sum(weights * (spreads ** 2 + temp_means ** 2)) - mu_temp ** 2)
    sigma_bma = math.sqrt(max(sigma_bma_sq, 0.01))

    # Combined tercile probabilities
    tercile_bn = float(np.sum(weights * np.array([
        f.get("tercile_probs", {}).get("below_normal", 0.333) for f in valid])))
    tercile_nn = float(np.sum(weights * np.array([
        f.get("tercile_probs", {}).get("near_normal", 0.334) for f in valid])))
    tercile_an = float(np.sum(weights * np.array([
        f.get("tercile_probs", {}).get("above_normal", 0.333) for f in valid])))

    # Normalize
    total = tercile_bn + tercile_nn + tercile_an
    if total > 0:
        tercile_bn /= total
        tercile_nn /= total
        tercile_an /= total

    # Combined confidence
    conf = float(np.sum(weights * np.array([f.get("confidence", 0.3) for f in valid])))

    method_weights = {f["method"]: round(float(w), 3) for f, w in zip(valid, weights)}

    return {
        "method": "bma",
        "temp_anomaly_c": round(mu_temp, 2),
        "precip_anomaly_pct": round(mu_precip, 1),
        "confidence": round(conf, 3),
        "spread": round(sigma_bma, 2),
        "tercile_probs": {
            "below_normal": round(tercile_bn, 3),
            "near_normal": round(tercile_nn, 3),
            "above_normal": round(tercile_an, 3),
        },
        "method_weights": method_weights,
        "methods_used": len(valid),
    }


# ===================================================================
# LEAD-DEPENDENT FORECAST RUNNER
# ===================================================================
def run_seasonal_forecast(city_id, lat, lon, target_year, target_month,
                          lead_months, climatology, index_series_cache=None):
    """Run the appropriate methods based on lead time and combine via BMA.

    Args:
        lead_months: How many months ahead (1-12).
        Others: as in individual methods.

    Returns: BMA combined forecast dict with individual method results attached.
    """
    cfg = db.load_config().get("seasonal", {})
    lead_decay = cfg.get("lead_decay_rate", 0.1)

    methods = []

    # Lead-dependent method selection
    if lead_months <= 3:
        # All methods
        methods.append(analog_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology, index_series_cache))
        methods.append(ridge_regression_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology, index_series_cache))
        methods.append(composite_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology, index_series_cache))
        methods.append(ecmwf_seasonal_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology))

    elif lead_months <= 6:
        # Analog + Regression + ECMWF (composite loses skill)
        methods.append(analog_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology, index_series_cache))
        methods.append(ridge_regression_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology, index_series_cache))
        methods.append(ecmwf_seasonal_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology))

    else:
        # 7-12 months: Analog + ENSO-only regression
        methods.append(analog_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology, index_series_cache))
        # Use regression with ENSO-only predictors
        methods.append(ridge_regression_forecast(
            city_id, lat, lon, target_year, target_month,
            climatology, index_series_cache))

    # Apply lead decay to confidence
    decay_factor = math.exp(-lead_decay * lead_months)
    for m in methods:
        if m.get("confidence") is not None:
            m["confidence"] = round(m["confidence"] * decay_factor, 3)

    # BMA combination
    combined = bayesian_model_average(methods)
    combined["lead_months"] = lead_months
    combined["individual_methods"] = methods

    # Store in DB
    db.insert_seasonal_forecast(city_id, target_year, target_month, "bma", {
        "temp_anomaly_c": combined["temp_anomaly_c"],
        "precip_anomaly_pct": combined["precip_anomaly_pct"],
        "confidence": combined["confidence"],
        "tercile_prob_bn": combined["tercile_probs"]["below_normal"],
        "tercile_prob_nn": combined["tercile_probs"]["near_normal"],
        "tercile_prob_an": combined["tercile_probs"]["above_normal"],
        "details_json": json.dumps(combined, default=str),
    })

    # Also store individual methods
    for m in methods:
        if m.get("temp_anomaly_c") is not None:
            db.insert_seasonal_forecast(city_id, target_year, target_month, m["method"], {
                "temp_anomaly_c": m["temp_anomaly_c"],
                "precip_anomaly_pct": m["precip_anomaly_pct"],
                "confidence": m.get("confidence"),
                "tercile_prob_bn": m.get("tercile_probs", {}).get("below_normal"),
                "tercile_prob_nn": m.get("tercile_probs", {}).get("near_normal"),
                "tercile_prob_an": m.get("tercile_probs", {}).get("above_normal"),
                "details_json": json.dumps(m, default=str),
            })

    return combined


# ===================================================================
# SKILL SCORES
# ===================================================================
def compute_acc(forecasts, observations):
    """Anomaly Correlation Coefficient.
    ACC = sum(f*o) / sqrt(sum(f^2) * sum(o^2))
    """
    f = np.array(forecasts)
    o = np.array(observations)
    denom = np.sqrt(np.sum(f ** 2) * np.sum(o ** 2))
    if denom == 0:
        return 0.0
    return float(np.sum(f * o) / denom)


def compute_rpss(tercile_forecasts, observed_categories):
    """Ranked Probability Skill Score.
    tercile_forecasts: list of (bn, nn, an) probabilities.
    observed_categories: list of 0 (BN), 1 (NN), 2 (AN).
    """
    if not tercile_forecasts:
        return 0.0

    rps_sum = 0.0
    rps_clim_sum = 0.0
    clim_probs = [1 / 3, 1 / 3, 1 / 3]

    for probs, obs_cat in zip(tercile_forecasts, observed_categories):
        obs_vec = [0, 0, 0]
        obs_vec[obs_cat] = 1

        rps = 0.0
        rps_clim = 0.0
        cum_f = 0.0
        cum_o = 0.0
        cum_c = 0.0
        for k in range(3):
            cum_f += probs[k]
            cum_o += obs_vec[k]
            cum_c += clim_probs[k]
            rps += (cum_f - cum_o) ** 2
            rps_clim += (cum_c - cum_o) ** 2

        rps_sum += rps
        rps_clim_sum += rps_clim

    if rps_clim_sum == 0:
        return 0.0
    return 1.0 - rps_sum / rps_clim_sum


def compute_hss(predicted_categories, observed_categories):
    """Heidke Skill Score for categorical (tercile) predictions.
    HSS = (H - E) / (N - E) where E = N/K (K=3 for terciles).
    """
    n = len(predicted_categories)
    if n == 0:
        return 0.0

    h = sum(1 for p, o in zip(predicted_categories, observed_categories) if p == o)
    e = n / 3.0
    if n - e == 0:
        return 0.0
    return (h - e) / (n - e)


# ===================================================================
# HELPERS
# ===================================================================
def _compute_tercile_probs(forecast_anomaly, forecast_spread, clim_std):
    """Compute tercile probabilities from forecast distribution.

    Uses Gaussian CDF with the forecast mean and spread.
    Tercile boundaries are at +-0.4307 * clim_std from zero anomaly.
    """
    if clim_std <= 0:
        clim_std = 1.0

    bn_threshold = TERCILE_Z_LOW * clim_std  # below normal boundary
    an_threshold = TERCILE_Z_HIGH * clim_std  # above normal boundary

    if forecast_spread <= 0:
        forecast_spread = 0.3

    # P(below normal) = P(X < bn_threshold)
    p_bn = float(sp_stats.norm.cdf(bn_threshold, loc=forecast_anomaly, scale=forecast_spread))
    # P(above normal) = P(X > an_threshold)
    p_an = float(1.0 - sp_stats.norm.cdf(an_threshold, loc=forecast_anomaly, scale=forecast_spread))
    # P(near normal) = 1 - P(BN) - P(AN)
    p_nn = 1.0 - p_bn - p_an

    # Ensure non-negative
    p_bn = max(0.01, p_bn)
    p_nn = max(0.01, p_nn)
    p_an = max(0.01, p_an)
    total = p_bn + p_nn + p_an
    p_bn /= total
    p_nn /= total
    p_an /= total

    return {
        "below_normal": round(p_bn, 3),
        "near_normal": round(p_nn, 3),
        "above_normal": round(p_an, 3),
    }


def _empty_forecast(method, reason):
    """Return a placeholder forecast when a method can't run."""
    return {
        "method": method,
        "temp_anomaly_c": None,
        "precip_anomaly_pct": None,
        "confidence": None,
        "spread": None,
        "tercile_probs": {
            "below_normal": 0.333,
            "near_normal": 0.334,
            "above_normal": 0.333,
        },
        "error": reason,
    }


# ===================================================================
# HINDCAST VERIFICATION
# ===================================================================
def run_hindcast_verification(city_id, lat, lon, climatology,
                              verify_years=5, index_series_cache=None):
    """Run leave-one-out hindcast verification over recent years.

    For each of the last `verify_years` years, re-runs the seasonal forecast
    for each month and compares against observed anomalies.
    Stores skill scores in seasonal_skill table.

    Returns: dict of {method: {metric: value}}
    """
    import logging
    logger = logging.getLogger(__name__)
    now = datetime.utcnow()
    current_year = now.year
    current_month = now.month

    methods_forecasts = {}  # {method: [(forecast_anom, observed_anom, tercile_fc, obs_cat)]}

    for year_offset in range(1, verify_years + 1):
        verify_year = current_year - year_offset
        for month in range(1, 13):
            # Skip months we don't have observations for yet
            if verify_year == current_year and month >= current_month:
                continue

            clim = climatology.get(month)
            if not clim or clim.get("temp_high_mean") is None:
                continue

            clim_temp_mean = (clim["temp_high_mean"] +
                              clim.get("temp_low_mean", clim["temp_high_mean"])) / 2.0
            clim_precip_mean = clim.get("precip_mean")
            clim_std = clim.get("temp_high_std", 1.0) or 1.0

            # Get actual observed anomaly
            obs = get_observed_monthly_anomaly(
                lat, lon, verify_year, month, clim_temp_mean, clim_precip_mean)
            if obs is None:
                continue

            obs_temp_anom = obs[0]

            # Classify observed into tercile
            bn_thresh = TERCILE_Z_LOW * clim_std
            an_thresh = TERCILE_Z_HIGH * clim_std
            if obs_temp_anom < bn_thresh:
                obs_cat = 0  # below normal
            elif obs_temp_anom > an_thresh:
                obs_cat = 2  # above normal
            else:
                obs_cat = 1  # near normal

            # Run each method (lead=1 for simplicity)
            try:
                methods = [
                    analog_forecast(city_id, lat, lon, verify_year, month,
                                    climatology, index_series_cache),
                    ridge_regression_forecast(city_id, lat, lon, verify_year, month,
                                              climatology, index_series_cache),
                    composite_forecast(city_id, lat, lon, verify_year, month,
                                       climatology, index_series_cache),
                ]
            except Exception as e:
                logger.debug("Hindcast error for %d-%02d: %s", verify_year, month, e)
                continue

            for fc in methods:
                method = fc.get("method", "unknown")
                if fc.get("temp_anomaly_c") is None:
                    continue

                if method not in methods_forecasts:
                    methods_forecasts[method] = []

                tp = fc.get("tercile_probs", {})
                tercile_fc = (
                    tp.get("below_normal", 0.333),
                    tp.get("near_normal", 0.334),
                    tp.get("above_normal", 0.333),
                )
                # Predicted category = highest probability tercile
                pred_cat = int(np.argmax(tercile_fc))

                methods_forecasts[method].append({
                    "fc_anom": fc["temp_anomaly_c"],
                    "obs_anom": obs_temp_anom,
                    "tercile_fc": tercile_fc,
                    "obs_cat": obs_cat,
                    "pred_cat": pred_cat,
                })

    # Compute skill scores per method
    results = {}
    for method, pairs in methods_forecasts.items():
        if len(pairs) < 6:
            continue

        fc_anoms = [p["fc_anom"] for p in pairs]
        obs_anoms = [p["obs_anom"] for p in pairs]
        tercile_fcs = [p["tercile_fc"] for p in pairs]
        obs_cats = [p["obs_cat"] for p in pairs]
        pred_cats = [p["pred_cat"] for p in pairs]

        # MAE
        mae = float(np.mean(np.abs(np.array(fc_anoms) - np.array(obs_anoms))))
        # Correlation
        corr = float(np.corrcoef(fc_anoms, obs_anoms)[0, 1]) if len(pairs) > 2 else 0.0
        # ACC
        acc = compute_acc(fc_anoms, obs_anoms)
        # RPSS
        rpss = compute_rpss(tercile_fcs, obs_cats)
        # HSS
        hss = compute_hss(pred_cats, obs_cats)

        results[method] = {
            "mae": round(mae, 3),
            "correlation": round(corr, 3),
            "acc": round(acc, 3),
            "rpss": round(rpss, 3),
            "hss": round(hss, 3),
            "sample_count": len(pairs),
        }

        # Store in DB
        for metric, value in [("mae", mae), ("correlation", corr),
                               ("acc", acc), ("rpss", rpss), ("hss", hss)]:
            db.upsert_seasonal_skill(
                city_id=city_id,
                method=method,
                metric=metric,
                value=round(value, 4),
                sample_count=len(pairs),
            )

        logger.info("Hindcast %s: MAE=%.2f, corr=%.2f, ACC=%.2f, RPSS=%.2f, HSS=%.2f (n=%d)",
                     method, mae, corr, acc, rpss, hss, len(pairs))

    return results
