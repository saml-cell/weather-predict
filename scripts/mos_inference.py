#!/usr/bin/env python3
"""
MOS inference: load the trained quantile LightGBM models (station-target),
fetch live multi-model forecasts from Open-Meteo's forecast API, build the
same feature vector the trainer used, and produce p10/p50/p90 predictions
per variable per hour.

Design notes:
- **Live fetch, no DB read**. Inference calls Open-Meteo's public forecast API
  (free, no key) with `models=gfs_global,icon_seamless,jma_seamless,ecmwf_ifs025`
  and gets hourly forecasts from all four models in one request.
- **Feature parity with the trainer**. engineer_features() here mirrors the
  logic in train_mos_quantile.py exactly — same src_<model> columns, same
  ensemble stats, same cyclic time features, same categoricals. If you change
  one, change both.
- **lead_hours convention**: the trainer set `lead_hours = valid_time.hour`
  because the backfill's `issue_time = 00:00 UTC of valid day`. We mirror that
  here at inference so the model sees the same shape it was trained on.
- **Boosters are cached per process**. First call loads ~12 LightGBM text
  files from disk (~6 MB total), subsequent calls reuse.
- **Physics egress gate**: every quantile prediction is clipped to physical
  bounds. humidity ∈ [0,100], precip ≥ 0, wind ≥ 0. Temperature is not
  clipped (can go anywhere in its natural range).
- **Aggregation to daily**: for each calendar day, compute:
    - temp_high_p10/p50/p90 = max over hourly pXX temp values
    - temp_low_p10/p50/p90  = min over hourly pXX temp values
    - precip_mm_sum_pXX     = sum over hourly pXX precip
    - wind_max_kmh_pXX      = max over hourly pXX wind
    - humidity_mean_pXX     = mean over hourly pXX humidity

Usage from Python:
    from mos_inference import predict_for_city
    result = predict_for_city(city_dict)
    # result['daily_by_date']['2026-04-14'] = {
    #     'temp_high_p10': ..., 'temp_high_p50': ..., 'temp_high_p90': ...,
    #     ...
    # }

CLI:
    .venv/bin/python scripts/mos_inference.py --city-id 1
    .venv/bin/python scripts/mos_inference.py --city-id 1 --hours 168 --json
"""

import os
import sys

# ---------------------------------------------------------------------------
# Project venv bootstrap
# ---------------------------------------------------------------------------
# The production gunicorn (weather-api systemd service) runs with system
# Python to reuse flask/gunicorn that ship with the distro. lightgbm is only
# installed in the project venv though, so we prepend the venv's site-packages
# to sys.path so `import lightgbm` works without reinstalling anything at the
# system level or touching the service file. numpy/pandas happen to be the
# same version in both, so mixing is safe here; if that ever changes this
# shim will need replacing with a proper service-file switch.
_VENV_SP = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".venv", "lib", "python3.12", "site-packages",
)
if os.path.isdir(_VENV_SP) and _VENV_SP not in sys.path:
    sys.path.insert(0, _VENV_SP)

import argparse
import json
import logging
import math
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import lightgbm as lgb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection  # type: ignore

logger = logging.getLogger("mos_inference")

# ---------------------------------------------------------------------------
# Constants — must match train_mos_quantile.py
# ---------------------------------------------------------------------------

VARIABLES = ["temp_c", "humidity_pct", "precip_mm", "wind_speed_kmh"]
QUANTILES = [0.1, 0.5, 0.9]
MOS_SOURCES = ["gfs_global", "icon_seamless", "jma_seamless", "ecmwf_ifs025"]

FEATURE_SET_V1 = "v1"
FEATURE_SET_V2 = "v2"
FEATURE_SET_V3 = "v3"

# Map training variable name → Open-Meteo hourly parameter
OM_PARAMETER = {
    "temp_c": "temperature_2m",
    "humidity_pct": "relative_humidity_2m",
    "precip_mm": "precipitation",
    "wind_speed_kmh": "wind_speed_10m",
}

# Feature column order must match the trainer. See engineer_features() in
# train_mos_quantile.py. Both trainer and inference select the feature set
# from the model's metadata.json so that v1 models keep working with v1
# features and v2 models get v2 features automatically.
FEATURE_COLS_V1 = (
    [f"src_{s}" for s in MOS_SOURCES]
    + [
        "ensemble_mean", "ensemble_std", "ensemble_min",
        "ensemble_max", "ensemble_n", "lead_hours",
        "hour_sin", "hour_cos", "doy_sin", "doy_cos",
        "month", "year", "city_id",
    ]
)
FEATURE_COLS_V2 = FEATURE_COLS_V1 + [
    "lat", "lon", "elevation_m", "abs_lat", "hemisphere_ns", "climate_band",
    "saturation_distance_c", "specific_humidity_gkg", "lcl_height_m",
]
FEATURE_COLS_V3 = FEATURE_COLS_V2 + [
    "aux_temp_c_mean", "aux_temp_c_std",
    "aux_humidity_pct_mean", "aux_humidity_pct_std",
    "aux_dew_point_c_mean", "aux_dew_point_c_std",
    "aux_pressure_msl_hpa_mean", "aux_pressure_msl_hpa_std",
    "aux_cloud_cover_pct_mean", "aux_cloud_cover_pct_std",
    "aux_wind_dir_sin_mean", "aux_wind_dir_cos_mean",
    "pressure_tendency_3h_hpa",
]
CATEGORICAL_COLS_V1 = ["month", "city_id"]
CATEGORICAL_COLS_V2 = CATEGORICAL_COLS_V1 + ["hemisphere_ns", "climate_band"]
CATEGORICAL_COLS_V3 = CATEGORICAL_COLS_V2

# Defaults
DEFAULT_MODELS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "mos_models_station",
)
CALIBRATORS_FILENAME = "isotonic_calibrators.pkl"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HTTP_TIMEOUT_S = 30

# Physics clipping bounds
PHYS_BOUNDS = {
    "humidity_pct": (0.0, 100.0),
    "precip_mm": (0.0, None),
    "wind_speed_kmh": (0.0, None),
    "temp_c": (None, None),  # no clipping
}

# ---------------------------------------------------------------------------
# Model loading (cached)
# ---------------------------------------------------------------------------

_booster_cache: dict = {}
_booster_cache_lock = threading.Lock()
_cities_meta_cache: dict = {}
_cities_meta_lock = threading.Lock()
_calibrators_cache: dict = {}
_calibrators_cache_lock = threading.Lock()


def _read_feature_set(models_dir: str) -> str:
    """Read the feature_set recorded in any of the variable metadata files.
    Defaults to v1 if not present (backwards compatible with old models).
    """
    for v in VARIABLES:
        meta_path = os.path.join(models_dir, v, "metadata.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path) as f:
                    meta = json.load(f)
                return meta.get("feature_set", FEATURE_SET_V1)
            except Exception:
                pass
    return FEATURE_SET_V1


def load_models(models_dir: str = DEFAULT_MODELS_DIR) -> dict:
    """Load LightGBM boosters from disk. Returns {(variable, quantile): Booster}.
    Caches across calls within the same process.
    """
    global _booster_cache
    with _booster_cache_lock:
        if _booster_cache.get("_dir") == models_dir and "_models" in _booster_cache:
            return _booster_cache["_models"]
        models = {}
        missing = []
        for v in VARIABLES:
            for q in QUANTILES:
                path = os.path.join(
                    models_dir, v, f"q{int(round(q * 100)):03d}.txt"
                )
                if not os.path.exists(path):
                    missing.append(path)
                    continue
                models[(v, q)] = lgb.Booster(model_file=path)
        if missing:
            logger.warning(
                "MOS models missing (%d files): %s", len(missing), missing[:3]
            )
        _booster_cache = {
            "_dir": models_dir,
            "_models": models,
            "_feature_set": _read_feature_set(models_dir),
        }
        logger.debug(
            "Loaded %d MOS boosters from %s (feature_set=%s)",
            len(models), models_dir, _booster_cache["_feature_set"],
        )
        return models


def feature_set_for(models_dir: str = DEFAULT_MODELS_DIR) -> str:
    """Return the feature_set the deployed models were trained with."""
    load_models(models_dir)  # populates cache
    return _booster_cache.get("_feature_set", FEATURE_SET_V1)


def _load_cities_meta() -> dict:
    """Cache cities table metadata keyed by id. Used for v2 climate-zone features."""
    global _cities_meta_cache
    with _cities_meta_lock:
        if _cities_meta_cache:
            return _cities_meta_cache
        try:
            conn = get_connection()
            rows = conn.execute(
                "SELECT id, lat, lon, elevation_m FROM cities"
            ).fetchall()
            _cities_meta_cache = {int(r["id"]): dict(r) for r in rows}
        except Exception as e:
            logger.warning("failed to load cities meta: %s", e)
        return _cities_meta_cache


def models_available(models_dir: str = DEFAULT_MODELS_DIR) -> bool:
    """Quick check: are all required booster files on disk?"""
    for v in VARIABLES:
        for q in QUANTILES:
            path = os.path.join(models_dir, v, f"q{int(round(q * 100)):03d}.txt")
            if not os.path.exists(path):
                return False
    return True


def load_calibrators(models_dir: str = DEFAULT_MODELS_DIR) -> dict:
    """Load per-variable post-hoc isotonic calibrators from pickle, if present.

    Returns a mapping {variable: sklearn.isotonic.IsotonicRegression}. Missing
    file or unloadable pickle → empty dict (fall back to raw booster output).
    Only variables that strictly improved at calibrator-fit time are in the
    pickle; see calibrate_isotonic.py.
    """
    global _calibrators_cache
    path = os.path.join(models_dir, CALIBRATORS_FILENAME)
    with _calibrators_cache_lock:
        if _calibrators_cache.get("_dir") == models_dir and "_cals" in _calibrators_cache:
            return _calibrators_cache["_cals"]
        cals: dict = {}
        if os.path.exists(path):
            try:
                import pickle  # deferred import — tiny cost, avoids top-level dep
                with open(path, "rb") as f:
                    payload = pickle.load(f)
                cals = payload.get("calibrators", {}) or {}
                logger.info(
                    "loaded isotonic calibrators for %d variable(s): %s",
                    len(cals), sorted(cals.keys()),
                )
            except Exception as e:
                logger.warning("failed to load isotonic calibrators: %s", e)
                cals = {}
        _calibrators_cache = {"_dir": models_dir, "_cals": cals}
        return cals


def _apply_calibrator(calibrator, q10: np.ndarray, q50: np.ndarray, q90: np.ndarray) -> tuple:
    """Given a fitted empirical-CDF calibrator for one variable and three
    rearranged quantile predictions (numpy arrays), return the calibrated
    (q10, q50, q90) tuple.

    The calibrator maps raw_tau → effective empirical CDF level. At inference
    we invert it: for each target tau in {0.1, 0.5, 0.9}, look up the raw
    tau' whose empirical CDF equals tau, then reconstruct the prediction by
    piecewise-linear interpolation across the three raw anchors at tau'.
    """
    if calibrator is None:
        return q10, q50, q90
    grid_x = np.linspace(0.001, 0.999, 999)
    grid_y = calibrator.predict(grid_x)  # empirical CDF at each raw tau

    def _invert(target_tau: float) -> float:
        idx = int(np.searchsorted(grid_y, target_tau))
        idx = min(max(idx, 0), len(grid_x) - 1)
        return float(grid_x[idx])

    # Force monotone input (defensive — callers already rearrange).
    stacked = np.column_stack([q10, q50, q90])
    stacked.sort(axis=1)
    a, b, c = stacked[:, 0], stacked[:, 1], stacked[:, 2]

    # Slopes of the piecewise-linear CDF anchored at (0.1, a), (0.5, b), (0.9, c).
    # Match calibrate_isotonic.interp_predictive_cdf exactly.
    spread_ab = (b - a) / 0.4  # dy / dtau on [0.1, 0.5]
    spread_bc = (c - b) / 0.4  # dy / dtau on [0.5, 0.9]
    # Linear extension on each tail uses the neighbouring slope.

    def _interp_at(tau: float) -> np.ndarray:
        if tau < 0.1:
            return a - (0.1 - tau) * spread_ab
        if tau < 0.5:
            return a + (tau - 0.1) * spread_ab
        if tau < 0.9:
            return b + (tau - 0.5) * spread_bc
        return c + (tau - 0.9) * spread_bc

    cal_q10 = _interp_at(_invert(0.1))
    cal_q50 = _interp_at(_invert(0.5))
    cal_q90 = _interp_at(_invert(0.9))
    # Post-calibration rearrangement (very rare but cheap to enforce).
    stacked = np.column_stack([cal_q10, cal_q50, cal_q90])
    stacked.sort(axis=1)
    return stacked[:, 0], stacked[:, 1], stacked[:, 2]


# ---------------------------------------------------------------------------
# Live forecast fetch
# ---------------------------------------------------------------------------

def fetch_multi_model_forecast(
    lat: float, lon: float, forecast_days: int = 7
) -> Optional[pd.DataFrame]:
    """Call Open-Meteo's multi-model forecast endpoint. Returns a long-format
    DataFrame with columns: [valid_time, source, temp_c, humidity_pct,
    precip_mm, wind_speed_kmh, dew_point_c, pressure_msl_hpa, cloud_cover_pct,
    wind_dir_deg] or None on failure. v3 feature engineering needs all of
    these; v1/v2 ignore the ones they don't use.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "temperature_2m", "relative_humidity_2m", "dew_point_2m",
            "precipitation", "pressure_msl", "surface_pressure",
            "wind_speed_10m", "wind_direction_10m", "cloud_cover",
        ]),
        "models": ",".join(MOS_SOURCES),
        "timezone": "UTC",
        "wind_speed_unit": "kmh",
        "forecast_days": max(1, min(16, int(forecast_days))),
    }
    url = f"{OPEN_METEO_FORECAST_URL}?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "weather-predict-mos/1.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            raw = json.loads(resp.read())
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
        logger.warning("Open-Meteo forecast fetch failed: %s", e)
        return None
    except json.JSONDecodeError as e:
        logger.warning("Open-Meteo forecast returned invalid JSON: %s", e)
        return None

    hourly = raw.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        logger.warning("Open-Meteo forecast returned no hourly time array")
        return None

    # Build a long-format frame: one row per (time, source)
    rows = []
    for i, t in enumerate(times):
        # Treat timestamps as naive UTC (the request asked for timezone=UTC)
        try:
            vt = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        for src in MOS_SOURCES:
            row = {"valid_time": vt, "source": src}
            row["temp_c"] = _get(hourly, f"temperature_2m_{src}", i)
            row["humidity_pct"] = _get(hourly, f"relative_humidity_2m_{src}", i)
            row["precip_mm"] = _get(hourly, f"precipitation_{src}", i)
            row["wind_speed_kmh"] = _get(hourly, f"wind_speed_10m_{src}", i)
            # v3 aux columns
            row["dew_point_c"] = _get(hourly, f"dew_point_2m_{src}", i)
            row["pressure_msl_hpa"] = _get(hourly, f"pressure_msl_{src}", i)
            row["cloud_cover_pct"] = _get(hourly, f"cloud_cover_{src}", i)
            row["wind_dir_deg"] = _get(hourly, f"wind_direction_10m_{src}", i)
            rows.append(row)
    return pd.DataFrame(rows)


def _get(d: dict, key: str, i: int):
    lst = d.get(key)
    if lst is None or i >= len(lst):
        return None
    v = lst[i]
    if v is None:
        return None
    try:
        fv = float(v)
        if math.isnan(fv):
            return None
        return fv
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Feature engineering — must mirror train_mos_quantile.engineer_features
# ---------------------------------------------------------------------------

def _climate_band(abs_lat: float) -> int:
    al = abs(abs_lat)
    if al < 23.5:
        return 0
    if al < 35.0:
        return 1
    if al < 55.0:
        return 2
    if al < 66.5:
        return 3
    return 4


def _magnus_dew_point_np(temp_c, rh_pct):
    a, b = 17.625, 243.04
    rh = np.clip(rh_pct, 0.1, 100.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        alpha = np.log(rh / 100.0) + (a * temp_c) / (b + temp_c)
        td = (b * alpha) / (a - alpha)
    return td


def engineer_features(
    long_df: pd.DataFrame,
    variable: str,
    city_id: int,
    feature_set: str = FEATURE_SET_V1,
) -> pd.DataFrame:
    """Take long-format (valid_time, source, all_variable_values) → wide
    feature matrix ready for LightGBM inference. One row per valid_time.

    feature_set must match the value in the trained model's metadata.json.
    The caller (predict_hourly) passes it through from _read_feature_set().
    """
    if feature_set == FEATURE_SET_V3:
        feature_cols = FEATURE_COLS_V3
    elif feature_set == FEATURE_SET_V2:
        feature_cols = FEATURE_COLS_V2
    else:
        feature_cols = FEATURE_COLS_V1
    if long_df.empty:
        return pd.DataFrame(columns=feature_cols)

    # Pivot source → columns for the target variable
    wide = long_df.pivot_table(
        index="valid_time",
        columns="source",
        values=variable,
        aggfunc="first",
    )
    for src in MOS_SOURCES:
        col = f"src_{src}"
        wide[col] = wide[src] if src in wide.columns else np.nan
    src_cols = [f"src_{s}" for s in MOS_SOURCES]

    # Ensemble stats — NaN-safe
    wide["ensemble_mean"] = wide[src_cols].mean(axis=1, skipna=True)
    wide["ensemble_std"] = wide[src_cols].std(axis=1, skipna=True, ddof=0)
    wide["ensemble_min"] = wide[src_cols].min(axis=1, skipna=True)
    wide["ensemble_max"] = wide[src_cols].max(axis=1, skipna=True)
    wide["ensemble_n"] = wide[src_cols].notna().sum(axis=1)

    # Time features
    idx = pd.to_datetime(wide.index, utc=True)
    hour = idx.hour.to_numpy()
    doy = idx.dayofyear.to_numpy()
    wide["lead_hours"] = hour.astype(int)  # matches training convention
    wide["hour_sin"] = np.sin(2 * math.pi * hour / 24.0)
    wide["hour_cos"] = np.cos(2 * math.pi * hour / 24.0)
    wide["doy_sin"] = np.sin(2 * math.pi * doy / 366.0)
    wide["doy_cos"] = np.cos(2 * math.pi * doy / 366.0)
    wide["month"] = idx.month.astype("int32")
    wide["year"] = idx.year.astype("int32")
    wide["city_id"] = int(city_id)

    if feature_set in (FEATURE_SET_V2, FEATURE_SET_V3):
        # Climate-zone features from cities meta
        meta = _load_cities_meta().get(int(city_id), {})
        lat = meta.get("lat")
        lon = meta.get("lon")
        elev = meta.get("elevation_m")
        wide["lat"] = lat if lat is not None else np.nan
        wide["lon"] = lon if lon is not None else np.nan
        wide["elevation_m"] = elev if elev is not None else np.nan
        wide["abs_lat"] = abs(lat) if lat is not None else np.nan
        wide["hemisphere_ns"] = 1 if (lat is not None and lat >= 0) else 0
        wide["climate_band"] = _climate_band(lat) if lat is not None else 2

        # Compute aux means/stds from long_df across all sources.
        # For v2 we only need temp_c + humidity_pct (physics features).
        # For v3 we additionally need dew_point, pressure, cloud cover and
        # the std of all of them, plus circular wind direction.
        aux_needed = ["temp_c", "humidity_pct"]
        if feature_set == FEATURE_SET_V3:
            aux_needed += ["dew_point_c", "pressure_msl_hpa", "cloud_cover_pct"]

        aux_means = {}
        for aux_var in aux_needed:
            if aux_var not in long_df.columns:
                aux_means[aux_var] = pd.Series(np.nan, index=wide.index)
                if feature_set == FEATURE_SET_V3:
                    wide[f"aux_{aux_var}_mean"] = np.nan
                    wide[f"aux_{aux_var}_std"] = np.nan
                continue
            sub = long_df[["valid_time", "source", aux_var]].dropna(subset=[aux_var])
            if sub.empty:
                aux_means[aux_var] = pd.Series(np.nan, index=wide.index)
                if feature_set == FEATURE_SET_V3:
                    wide[f"aux_{aux_var}_mean"] = np.nan
                    wide[f"aux_{aux_var}_std"] = np.nan
                continue
            aux_wide = sub.pivot_table(
                index="valid_time", columns="source", values=aux_var, aggfunc="first"
            )
            aux_mean_series = aux_wide.mean(axis=1, skipna=True)
            aux_means[aux_var] = aux_mean_series
            if feature_set == FEATURE_SET_V3:
                wide[f"aux_{aux_var}_mean"] = aux_mean_series.reindex(wide.index)
                wide[f"aux_{aux_var}_std"] = (
                    aux_wide.std(axis=1, skipna=True, ddof=0).reindex(wide.index)
                )

        # Physics features — from ensemble-mean temp and humidity.
        T = aux_means["temp_c"].reindex(wide.index).to_numpy(dtype=float)
        RH = aux_means["humidity_pct"].reindex(wide.index).to_numpy(dtype=float)
        Td = _magnus_dew_point_np(T, RH)
        wide["saturation_distance_c"] = T - Td
        e_s = 6.112 * np.exp(17.67 * Td / (Td + 243.5))
        wide["specific_humidity_gkg"] = 0.622 * e_s / (1013.25 - e_s) * 1000.0
        wide["lcl_height_m"] = 125.0 * (T - Td)

        if feature_set == FEATURE_SET_V3:
            # Wind direction (circular) — convert to sin/cos per source, then
            # average across sources to get a stable circular mean.
            wd_sub = long_df[["valid_time", "source", "wind_dir_deg"]].dropna(
                subset=["wind_dir_deg"]
            )
            if not wd_sub.empty:
                wd_rad = np.radians(wd_sub["wind_dir_deg"].to_numpy(dtype=float))
                wd_sub = wd_sub.assign(_sin=np.sin(wd_rad), _cos=np.cos(wd_rad))
                sin_wide = wd_sub.pivot_table(
                    index="valid_time", columns="source", values="_sin", aggfunc="first",
                )
                cos_wide = wd_sub.pivot_table(
                    index="valid_time", columns="source", values="_cos", aggfunc="first",
                )
                wide["aux_wind_dir_sin_mean"] = (
                    sin_wide.mean(axis=1, skipna=True).reindex(wide.index)
                )
                wide["aux_wind_dir_cos_mean"] = (
                    cos_wide.mean(axis=1, skipna=True).reindex(wide.index)
                )
            else:
                wide["aux_wind_dir_sin_mean"] = np.nan
                wide["aux_wind_dir_cos_mean"] = np.nan

            # Pressure tendency: 3-hour diff on the ensemble-mean pressure.
            # wide is indexed by valid_time and is already sorted at this
            # point (pandas pivot_table returns a sorted index). diff(3) gives
            # NaN for the first 3 rows, which LightGBM handles.
            wide_sorted = wide.sort_index()
            wide["pressure_tendency_3h_hpa"] = (
                wide_sorted["aux_pressure_msl_hpa_mean"].diff(3).reindex(wide.index)
            )

    return wide[feature_cols]


# ---------------------------------------------------------------------------
# Prediction
# ---------------------------------------------------------------------------

def _clip_physics(variable: str, arr: np.ndarray) -> np.ndarray:
    lo, hi = PHYS_BOUNDS.get(variable, (None, None))
    if lo is not None:
        arr = np.maximum(arr, lo)
    if hi is not None:
        arr = np.minimum(arr, hi)
    return arr


def _rearrange_quantiles(pred_by_q: dict) -> dict:
    """Cross-quantile rearrangement: sort per-row so q10 ≤ q50 ≤ q90."""
    taus = sorted(pred_by_q.keys())
    stacked = np.column_stack([pred_by_q[t] for t in taus])
    stacked.sort(axis=1)
    return {t: stacked[:, i] for i, t in enumerate(taus)}


def predict_hourly(
    long_df: pd.DataFrame,
    city_id: int,
    models_dir: str = DEFAULT_MODELS_DIR,
) -> pd.DataFrame:
    """Run all trained quantile models for all variables on the given
    long-format forecast data. Returns a wide frame indexed by valid_time
    with columns like temp_c_p10/p50/p90, humidity_pct_p10/p50/p90, etc.
    """
    models = load_models(models_dir)
    if not models:
        logger.error("No MOS models loaded from %s", models_dir)
        return pd.DataFrame()

    fset = feature_set_for(models_dir)
    calibrators = load_calibrators(models_dir)

    out = None
    for variable in VARIABLES:
        X = engineer_features(long_df, variable, city_id, feature_set=fset)
        if X.empty:
            continue
        preds_by_q = {}
        for q in QUANTILES:
            booster = models.get((variable, q))
            if booster is None:
                continue
            pred = booster.predict(X)
            preds_by_q[q] = pred
        if not preds_by_q:
            continue
        rearranged = _rearrange_quantiles(preds_by_q)

        # Post-hoc isotonic recalibration (Council Round 3 Step 2, Henri).
        # Only applied to variables that strictly improved at fit time — others
        # fall through with raw predictions. See scripts/calibrate_isotonic.py.
        cal = calibrators.get(variable)
        if cal is not None and {0.1, 0.5, 0.9}.issubset(rearranged.keys()):
            cal_q10, cal_q50, cal_q90 = _apply_calibrator(
                cal, rearranged[0.1], rearranged[0.5], rearranged[0.9],
            )
            rearranged[0.1] = cal_q10
            rearranged[0.5] = cal_q50
            rearranged[0.9] = cal_q90

        for q, arr in rearranged.items():
            arr = _clip_physics(variable, arr)
            col = f"{variable}_p{int(round(q * 100)):02d}"
            if out is None:
                out = pd.DataFrame(index=X.index)
            out[col] = arr

    return out if out is not None else pd.DataFrame()


# ---------------------------------------------------------------------------
# Daily aggregation
# ---------------------------------------------------------------------------

def aggregate_to_daily(hourly_preds: pd.DataFrame) -> dict:
    """Fold hourly quantile predictions into per-date summaries.

    Returns: { 'YYYY-MM-DD': {
        'temp_high_p10/p50/p90': float,
        'temp_low_p10/p50/p90': float,
        'precip_mm_sum_p10/p50/p90': float,
        'wind_max_kmh_p10/p50/p90': float,
        'humidity_mean_p10/p50/p90': float,
        'n_hours': int,
    }}
    """
    if hourly_preds.empty:
        return {}

    df = hourly_preds.copy()
    df.index = pd.to_datetime(df.index, utc=True)
    df["_date"] = df.index.strftime("%Y-%m-%d")

    out = {}
    for date, group in df.groupby("_date"):
        row = {"n_hours": int(len(group))}
        for q in (10, 50, 90):
            qs = f"p{q:02d}"
            # Temperature: max + min per day
            if f"temp_c_{qs}" in group.columns:
                row[f"temp_high_{qs}"] = _to_num(group[f"temp_c_{qs}"].max())
                row[f"temp_low_{qs}"] = _to_num(group[f"temp_c_{qs}"].min())
            # Precip: sum per day
            if f"precip_mm_{qs}" in group.columns:
                row[f"precip_mm_sum_{qs}"] = _to_num(group[f"precip_mm_{qs}"].sum())
            # Wind: max per day
            if f"wind_speed_kmh_{qs}" in group.columns:
                row[f"wind_max_kmh_{qs}"] = _to_num(group[f"wind_speed_kmh_{qs}"].max())
            # Humidity: mean per day
            if f"humidity_pct_{qs}" in group.columns:
                row[f"humidity_mean_{qs}"] = _to_num(group[f"humidity_pct_{qs}"].mean())
        out[date] = row
    return out


def _to_num(v):
    try:
        f = float(v)
        if math.isnan(f):
            return None
        return round(f, 2)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def predict_for_city(
    city: dict, forecast_days: int = 7, models_dir: str = DEFAULT_MODELS_DIR
) -> dict:
    """End-to-end MOS prediction for one city.

    Input: city dict with id, lat, lon, name.
    Output: {
        'models_loaded': bool,
        'hours_predicted': int,
        'daily_by_date': { 'YYYY-MM-DD': {...} },
        'generated_at': ISO timestamp,
    }
    """
    t0 = datetime.now(timezone.utc)
    if not models_available(models_dir):
        logger.warning("MOS models not found at %s; skipping inference", models_dir)
        return {
            "models_loaded": False,
            "hours_predicted": 0,
            "daily_by_date": {},
            "generated_at": t0.isoformat(),
        }

    long_df = fetch_multi_model_forecast(city["lat"], city["lon"], forecast_days)
    if long_df is None or long_df.empty:
        return {
            "models_loaded": True,
            "hours_predicted": 0,
            "daily_by_date": {},
            "generated_at": t0.isoformat(),
            "error": "no forecast data from upstream",
        }

    city_id = city.get("id") or 0
    hourly_preds = predict_hourly(long_df, city_id=city_id, models_dir=models_dir)
    daily = aggregate_to_daily(hourly_preds)

    return {
        "models_loaded": True,
        "hours_predicted": int(len(hourly_preds)),
        "daily_by_date": daily,
        "generated_at": t0.isoformat(),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--city-id", type=int, required=True)
    p.add_argument("--hours", type=int, default=7, help="forecast horizon in days")
    p.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    p.add_argument("--json", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    conn = get_connection()
    row = conn.execute(
        "SELECT id, name, lat, lon FROM cities WHERE id = ?", (args.city_id,)
    ).fetchone()
    if not row:
        logger.error("City id %d not found", args.city_id)
        sys.exit(1)
    city = dict(row)

    result = predict_for_city(city, forecast_days=args.hours, models_dir=args.models_dir)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
        return

    print(f"MOS forecast for {city['name']} (city_id={city['id']})")
    print(f"  models_loaded: {result['models_loaded']}")
    print(f"  hours_predicted: {result['hours_predicted']}")
    print(f"  generated_at: {result['generated_at']}")
    print()
    print(f"  {'Date':<12} {'Hi p50':>8} {'Hi p10-p90':>12} "
          f"{'Lo p50':>8} {'Precip p50':>12} {'Wind p50':>10} {'Hum p50':>9}")
    print(f"  {'-' * 78}")
    for date in sorted(result.get("daily_by_date", {}).keys()):
        d = result["daily_by_date"][date]
        hi50 = d.get("temp_high_p50", "-")
        hi10 = d.get("temp_high_p10", "-")
        hi90 = d.get("temp_high_p90", "-")
        lo50 = d.get("temp_low_p50", "-")
        pr50 = d.get("precip_mm_sum_p50", "-")
        wi50 = d.get("wind_max_kmh_p50", "-")
        hu50 = d.get("humidity_mean_p50", "-")
        print(
            f"  {date:<12} {hi50!s:>8} {f'{hi10}-{hi90}':>12} "
            f"{lo50!s:>8} {pr50!s:>12} {wi50!s:>10} {hu50!s:>9}"
        )


if __name__ == "__main__":
    main()
