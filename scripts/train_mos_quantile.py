#!/usr/bin/env python3
"""
Train quantile LightGBM MOS (Model Output Statistics) on the backfilled
hourly forecasts ⋈ hourly observations produced by
`scripts/backfill_historical_forecasts.py`.

Design decisions from the council audit (see wiki: weather-predict-ml-plan):

- **Quantile regression at τ ∈ {0.1, 0.5, 0.9}** — gives a calibrated p10/p50/p90
  band instead of a single point estimate. Mean pinball loss is proportional to
  CRPS for discrete quantile sets, so this is the scoring rule Henri demanded.

- **Strictly temporal split** — 2022-2023 train / 2024 val / 2025 test. No
  shuffling. Random k-fold leaks future into past and produces illusory skill.
  Marcus's #1 hazard.

- **One model per variable per quantile**, pooled across cities with `city_id`
  as a categorical feature. This uses data 10× better than per-city training
  and lets the model learn station-specific bias via leaf splits. Phase 2 can
  stratify further by lead-time bucket; v1 treats `lead_hours` as a continuous
  feature.

- **Cross-quantile rearrangement** (Chernozhukov, Fernández-Val, Galichon 2010):
  after prediction we sort the three quantile estimates row-wise so that
  q10 ≤ q50 ≤ q90 always. Free accuracy gain, no model retraining.

- **Model-version drift**: we include `year` and `month` features so the model
  can pick up Open-Meteo's periodic ECMWF/GFS upgrades without an explicit
  version flag.

- **Baselines** we compare against on the 2025 test set:
    1. Ensemble mean (equal-weight average across model sources)
    2. Best single source (lowest validation MAE)
  The MOS must *strictly* beat both to be deployed.

- **Features**: raw source forecasts (one column per Open-Meteo model),
  ensemble mean & std, min lead hours, cyclic hour-of-day, cyclic day-of-year,
  month, year, city_id.

- **Hyperparameters are hand-set, not tuned**. The council was explicit: this
  is MOS, not Kaggle. Defaults are deliberate:
    num_leaves=31, lr=0.05, min_data_in_leaf=50, feature_fraction=0.9,
    bagging_fraction=0.9, num_boost_round=2000, early_stopping=50

Usage:
    # Train all default variables on all available data
    .venv/bin/python scripts/train_mos_quantile.py

    # Train just temperature, dry run, single city
    .venv/bin/python scripts/train_mos_quantile.py \\
        --variables temp_c --city-id 1 --dry-run

Outputs:
    data/mos_models/
        {variable}/
            q010.txt              LightGBM text model for τ=0.10
            q050.txt              LightGBM text model for τ=0.50
            q090.txt              LightGBM text model for τ=0.90
            metadata.json         Feature list, training stats, hyperparams
        eval_report.json          Full metrics on val + test, per baseline
        eval_summary.txt          Human-readable summary table
"""

import argparse
import json
import logging
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Optional

# Third-party (install via .venv/bin/pip install lightgbm pandas numpy scipy)
import numpy as np
import pandas as pd
import lightgbm as lgb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection, DB_PATH  # type: ignore

logger = logging.getLogger("train_mos")

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_VARIABLES = ["temp_c", "humidity_pct", "precip_mm", "wind_speed_kmh"]
DEFAULT_QUANTILES = [0.1, 0.5, 0.9]
DEFAULT_TRAIN_END = "2023-12-31"
DEFAULT_VAL_END = "2024-12-31"
DEFAULT_TEST_END = "2025-12-31"
MODELS_DIR_NAME = "mos_models"
MIN_TRAIN_ROWS = 200  # below this, skip the variable with a warning

LGB_BASE_PARAMS = {
    "objective": "quantile",
    "metric": "quantile",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 5,
    "max_depth": -1,
    "verbose": -1,
    "force_col_wise": True,
    "lambda_l2": 0.1,
}
NUM_BOOST_ROUND = 2000
EARLY_STOP = 50

# Per-variable overrides. Wind is noisier than temperature so it benefits
# from tighter regularization. Council Round 3 Step 3 (Lena, 2026-04-13)
# tried loosening these to num_leaves=20 / min_data_in_leaf=75 / lambda_l2=0.3
# on the theory that the original values were over-regularized. The empirical
# result on the 2025 test set was −0.033% CRPS (vs the council's expected
# −1 to −2%) and cov80 0.753→0.749 — a statistical wash. The loosening was
# NOT adopted; the deployed wind model remains on the original tight params.
# See isotonic_summary.txt / weather-predict-council-round3.md Step 3 for the
# full before/after numbers.
PER_VARIABLE_PARAMS = {
    "wind_speed_kmh": {
        "num_leaves": 15,
        "min_data_in_leaf": 150,
        "lambda_l2": 1.0,
        "feature_fraction": 0.8,
    },
}

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _project_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


OBS_TABLE_ERA5 = "observations_hourly"
OBS_TABLE_STATION = "observations_hourly_station"

# Feature set flags. Toggled per trainer run via --feature-set.
FEATURE_SET_V1 = "v1"  # original baseline
FEATURE_SET_V2 = "v2"  # + climate-zone + physics
FEATURE_SET_V3 = "v3"  # + cross-variable aux features + pressure tendency
                       # The v3 additions: ensemble mean+std of dew_point_c,
                       # pressure_msl_hpa, cloud_cover_pct (all ignored by
                       # v1/v2); wind_dir as sin/cos circular encoding;
                       # aux means/stds for temp_c and humidity_pct as direct
                       # features (v2 only used them for physics calcs);
                       # pressure_tendency_3h (per-city time lag on MSL pressure).


def load_cities_meta(conn: sqlite3.Connection) -> dict:
    """Cache cities table rows keyed by id (for climate-zone features)."""
    rows = conn.execute(
        "SELECT id, lat, lon, elevation_m FROM cities"
    ).fetchall()
    return {int(r["id"]): dict(r) for r in rows}


def load_joined(
    conn: sqlite3.Connection,
    variable: str,
    city_id: Optional[int] = None,
    target_source: str = "era5",
    load_aux: bool = False,
    feature_set_v3: bool = False,
) -> pd.DataFrame:
    """Join forecasts_hourly and observations_<target> on (city_id, valid_time),
    pivoting forecast source into columns. One row per (city, valid_time).

    target_source:
        'era5'     — use observations_hourly (Open-Meteo archive / ERA5)
        'station'  — use observations_hourly_station (Meteostat real station obs)

    load_aux:
        When True, also attaches `aux_temp_c_mean` and `aux_humidity_pct_mean`
        columns computed as the ensemble mean across sources at the same
        (city_id, valid_time). Required for v2 physics features.

    Returns a DataFrame with columns:
        city_id, valid_time, lead_hours, src_<model1>, ..., obs
        [+ aux_temp_c_mean, aux_humidity_pct_mean when load_aux=True]
    """
    obs_table = {
        "era5": OBS_TABLE_ERA5,
        "station": OBS_TABLE_STATION,
    }[target_source]

    fc_sql = f"""
        SELECT city_id, valid_time, source, lead_hours, {variable} AS fc
        FROM forecasts_hourly
        WHERE {variable} IS NOT NULL
    """
    obs_sql = f"""
        SELECT city_id, valid_time, {variable} AS obs
        FROM {obs_table}
        WHERE {variable} IS NOT NULL
    """
    if city_id is not None:
        fc_sql += f" AND city_id = {int(city_id)}"
        obs_sql += f" AND city_id = {int(city_id)}"

    fc = pd.read_sql_query(fc_sql, conn)
    obs = pd.read_sql_query(obs_sql, conn)
    if fc.empty or obs.empty:
        return pd.DataFrame()

    # Wide pivot: one column per source
    fc_wide = fc.pivot_table(
        index=["city_id", "valid_time"],
        columns="source",
        values="fc",
        aggfunc="first",
    )
    fc_wide.columns = [f"src_{c}" for c in fc_wide.columns]

    # Take the shortest lead across sources for this (city, valid_time)
    fc_lead = (
        fc.groupby(["city_id", "valid_time"])["lead_hours"].min().rename("lead_hours")
    )

    df = fc_wide.join(fc_lead).reset_index()
    df = df.merge(obs, on=["city_id", "valid_time"], how="inner")

    # Load auxiliary variables:
    #   v2 needs temp_c and humidity_pct means (for physics features only).
    #   v3 additionally pulls dew_point_c, pressure_msl_hpa, cloud_cover_pct
    #       with mean+std, wind_dir_deg as circular sin/cos, and uses all of
    #       them as direct features.
    # load_aux must be True for v2 and v3. For v1 it is False (no aux at all).
    if load_aux:
        # v2 set: temp and humidity (always needed for physics features)
        v2_aux = ["temp_c", "humidity_pct"]
        # v3 set: add the four cross-variable predictors
        v3_aux = ["dew_point_c", "pressure_msl_hpa", "cloud_cover_pct"]

        aux_vars = list(v2_aux)
        if feature_set_v3:
            aux_vars += v3_aux

        for aux in aux_vars:
            if aux == variable:
                # aux_<variable> mean == ensemble_mean (computed later),
                # but the trainer needs the column present now for cleanness.
                src_cols_local = [c for c in df.columns if c.startswith("src_")]
                df[f"aux_{aux}_mean"] = df[src_cols_local].mean(axis=1, skipna=True)
                df[f"aux_{aux}_std"] = df[src_cols_local].std(axis=1, skipna=True, ddof=0)
                continue
            aux_sql = f"""
                SELECT city_id, valid_time, source, {aux} AS val
                FROM forecasts_hourly
                WHERE {aux} IS NOT NULL
            """
            if city_id is not None:
                aux_sql += f" AND city_id = {int(city_id)}"
            aux_df = pd.read_sql_query(aux_sql, conn)
            if aux_df.empty:
                df[f"aux_{aux}_mean"] = np.nan
                df[f"aux_{aux}_std"] = np.nan
                continue
            aux_wide = aux_df.pivot_table(
                index=["city_id", "valid_time"],
                columns="source",
                values="val",
                aggfunc="first",
            )
            aux_mean = aux_wide.mean(axis=1, skipna=True).rename(f"aux_{aux}_mean")
            aux_std = aux_wide.std(axis=1, skipna=True, ddof=0).rename(f"aux_{aux}_std")
            aux_stats = pd.concat([aux_mean, aux_std], axis=1).reset_index()
            df = df.merge(
                aux_stats,
                on=["city_id", "valid_time"],
                how="left",
            )

        # v3: wind direction (circular) via sin/cos averaging across sources.
        if feature_set_v3:
            wd_sql = """
                SELECT city_id, valid_time, source, wind_dir_deg AS val
                FROM forecasts_hourly
                WHERE wind_dir_deg IS NOT NULL
            """
            if city_id is not None:
                wd_sql += f" AND city_id = {int(city_id)}"
            wd_df = pd.read_sql_query(wd_sql, conn)
            if wd_df.empty:
                df["aux_wind_dir_sin_mean"] = np.nan
                df["aux_wind_dir_cos_mean"] = np.nan
            else:
                rad = np.radians(wd_df["val"].to_numpy(dtype=float))
                wd_df["_sin"] = np.sin(rad)
                wd_df["_cos"] = np.cos(rad)
                sin_wide = wd_df.pivot_table(
                    index=["city_id", "valid_time"], columns="source",
                    values="_sin", aggfunc="first",
                )
                cos_wide = wd_df.pivot_table(
                    index=["city_id", "valid_time"], columns="source",
                    values="_cos", aggfunc="first",
                )
                sin_mean = sin_wide.mean(axis=1, skipna=True).rename("aux_wind_dir_sin_mean")
                cos_mean = cos_wide.mean(axis=1, skipna=True).rename("aux_wind_dir_cos_mean")
                wd_stats = pd.concat([sin_mean, cos_mean], axis=1).reset_index()
                df = df.merge(wd_stats, on=["city_id", "valid_time"], how="left")

    return df


def _climate_band(abs_lat: float) -> int:
    """Coarse climate-band classification from |lat|.

    0: tropical       (< 23.5°)
    1: subtropical    (23.5–35°)
    2: temperate      (35–55°)
    3: subpolar       (55–66.5°)
    4: polar          (> 66.5°)

    Captures ~80% of Köppen's signal with zero external data.
    """
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


def _magnus_dew_point_np(temp_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """Vectorised Magnus-Tetens dew point. Returns NaN where inputs are bad."""
    a, b = 17.625, 243.04
    rh = np.clip(rh_pct, 0.1, 100.0)
    with np.errstate(invalid="ignore", divide="ignore"):
        alpha = np.log(rh / 100.0) + (a * temp_c) / (b + temp_c)
        td = (b * alpha) / (a - alpha)
    return td


def _compute_physics_features(df: pd.DataFrame) -> None:
    """Add saturation_distance_c, specific_humidity, lcl_height_m to df (in-place).

    Uses the ensemble-mean temperature and humidity (columns aux_temp_c_mean
    and aux_humidity_pct_mean, which load_joined(load_aux=True) attaches).
    NaN-safe — missing inputs produce NaN features.
    """
    T = df.get("aux_temp_c_mean")
    RH = df.get("aux_humidity_pct_mean")
    if T is None or RH is None:
        df["saturation_distance_c"] = np.nan
        df["specific_humidity_gkg"] = np.nan
        df["lcl_height_m"] = np.nan
        return
    T_arr = T.to_numpy(dtype=float)
    RH_arr = RH.to_numpy(dtype=float)
    Td_arr = _magnus_dew_point_np(T_arr, RH_arr)
    # Saturation distance (T - Td): 0 = saturated (fog/cloud), large = dry.
    df["saturation_distance_c"] = T_arr - Td_arr
    # Specific humidity in g/kg (approximation at standard pressure)
    # q ≈ 0.622 * e_s(Td) / P, where e_s is saturation vapor pressure.
    e_s = 6.112 * np.exp(17.67 * Td_arr / (Td_arr + 243.5))
    df["specific_humidity_gkg"] = 0.622 * e_s / (1013.25 - e_s) * 1000.0
    # LCL height via Espy's formula (approximate, in metres): h ≈ 125 * (T - Td)
    df["lcl_height_m"] = 125.0 * (T_arr - Td_arr)


def engineer_features(
    df: pd.DataFrame,
    cities_meta: Optional[dict] = None,
    feature_set: str = FEATURE_SET_V1,
) -> tuple:
    """Add ensemble stats, cyclic time, and (v2) climate-zone + physics features.
    Returns (X, y, feature_names).

    feature_set='v1' reproduces the original feature set exactly.
    feature_set='v2' adds climate-zone (lat/lon/elev/abs_lat/hemisphere/band)
    and physics (saturation distance, specific humidity, LCL height) features.
    Inference time must call this with the same feature_set the model was
    trained on — the saved metadata.json records it.
    """
    if df.empty:
        return pd.DataFrame(), pd.Series(dtype=float), []

    src_cols = [c for c in df.columns if c.startswith("src_")]

    # Ensemble statistics — NaN-safe.
    df["ensemble_mean"] = df[src_cols].mean(axis=1, skipna=True)
    df["ensemble_std"] = df[src_cols].std(axis=1, skipna=True, ddof=0)
    df["ensemble_min"] = df[src_cols].min(axis=1, skipna=True)
    df["ensemble_max"] = df[src_cols].max(axis=1, skipna=True)
    df["ensemble_n"] = df[src_cols].notna().sum(axis=1)

    # Cyclic time features
    vt = pd.to_datetime(df["valid_time"], utc=True)
    hour = vt.dt.hour.to_numpy()
    doy = vt.dt.dayofyear.to_numpy()
    df["hour_sin"] = np.sin(2 * math.pi * hour / 24.0)
    df["hour_cos"] = np.cos(2 * math.pi * hour / 24.0)
    df["doy_sin"] = np.sin(2 * math.pi * doy / 366.0)
    df["doy_cos"] = np.cos(2 * math.pi * doy / 366.0)
    df["month"] = vt.dt.month.astype("int32")
    df["year"] = vt.dt.year.astype("int32")
    df["city_id"] = df["city_id"].astype("int32")

    feature_cols = (
        src_cols
        + [
            "ensemble_mean", "ensemble_std", "ensemble_min", "ensemble_max",
            "ensemble_n", "lead_hours",
            "hour_sin", "hour_cos", "doy_sin", "doy_cos",
            "month", "year", "city_id",
        ]
    )

    if feature_set in (FEATURE_SET_V2, FEATURE_SET_V3):
        # Climate-zone features from cities table
        if cities_meta is None:
            cities_meta = {}
        lat_map = {cid: m.get("lat") for cid, m in cities_meta.items()}
        lon_map = {cid: m.get("lon") for cid, m in cities_meta.items()}
        elev_map = {cid: m.get("elevation_m") for cid, m in cities_meta.items()}
        cid_arr = df["city_id"].to_numpy()
        df["lat"] = [lat_map.get(int(c), np.nan) for c in cid_arr]
        df["lon"] = [lon_map.get(int(c), np.nan) for c in cid_arr]
        df["elevation_m"] = [elev_map.get(int(c), np.nan) for c in cid_arr]
        df["abs_lat"] = np.abs(df["lat"].astype(float))
        df["hemisphere_ns"] = np.where(df["lat"].astype(float) >= 0, 1, 0).astype("int32")
        df["climate_band"] = df["abs_lat"].apply(_climate_band).astype("int32")

        # Physics features (require aux_temp_c_mean / aux_humidity_pct_mean)
        _compute_physics_features(df)

        feature_cols += [
            "lat", "lon", "elevation_m", "abs_lat", "hemisphere_ns", "climate_band",
            "saturation_distance_c", "specific_humidity_gkg", "lcl_height_m",
        ]

    if feature_set == FEATURE_SET_V3:
        # Cross-variable auxiliary features (already loaded by load_joined
        # when feature_set_v3=True). Add them as direct feature columns so
        # the trainer can actually use them.
        v3_extras = [
            "aux_temp_c_mean", "aux_temp_c_std",
            "aux_humidity_pct_mean", "aux_humidity_pct_std",
            "aux_dew_point_c_mean", "aux_dew_point_c_std",
            "aux_pressure_msl_hpa_mean", "aux_pressure_msl_hpa_std",
            "aux_cloud_cover_pct_mean", "aux_cloud_cover_pct_std",
            "aux_wind_dir_sin_mean", "aux_wind_dir_cos_mean",
        ]
        for c in v3_extras:
            if c not in df.columns:
                df[c] = np.nan

        # Pressure tendency: 3-hour diff on ensemble mean pressure, per-city,
        # sorted by valid_time. Rows at the start of each city's series get
        # NaN, which LightGBM handles natively.
        df_sorted = df.sort_values(["city_id", "valid_time"])
        df_sorted["pressure_tendency_3h_hpa"] = (
            df_sorted.groupby("city_id")["aux_pressure_msl_hpa_mean"].diff(3)
        )
        # Restore original order via index
        df["pressure_tendency_3h_hpa"] = df_sorted["pressure_tendency_3h_hpa"]

        feature_cols += v3_extras + ["pressure_tendency_3h_hpa"]

    X = df[feature_cols].copy()
    y = df["obs"].astype(float)
    return X, y, feature_cols


# ---------------------------------------------------------------------------
# Temporal split
# ---------------------------------------------------------------------------

def split_temporal(
    df: pd.DataFrame, train_end: str, val_end: str, test_end: str
) -> tuple:
    """Strict forward-in-time split by valid_time. No shuffling."""
    vt = pd.to_datetime(df["valid_time"], utc=True)
    train_end_ts = pd.Timestamp(train_end, tz="UTC") + pd.Timedelta(days=1)
    val_end_ts = pd.Timestamp(val_end, tz="UTC") + pd.Timedelta(days=1)
    test_end_ts = pd.Timestamp(test_end, tz="UTC") + pd.Timedelta(days=1)

    mask_train = vt < train_end_ts
    mask_val = (vt >= train_end_ts) & (vt < val_end_ts)
    mask_test = (vt >= val_end_ts) & (vt < test_end_ts)
    return (
        df.loc[mask_train].reset_index(drop=True),
        df.loc[mask_val].reset_index(drop=True),
        df.loc[mask_test].reset_index(drop=True),
    )


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, tau: float) -> float:
    """Average pinball loss at level τ. Lower is better. In the units of y."""
    diff = y_true - y_pred
    loss = np.where(diff >= 0, tau * diff, (tau - 1) * diff)
    return float(np.mean(loss))


def empirical_crps_from_quantiles(
    y_true: np.ndarray, quantile_preds: dict
) -> float:
    """Discrete CRPS approximation from a small set of quantile predictions.

    For a quantile set T = {τ_1 < τ_2 < ... < τ_K}, CRPS can be approximated as
    2 · mean_k pinball_{τ_k}(y, ŷ_{τ_k}) · w_k
    with uniform weights w_k = 1/K when the quantile set is symmetric.
    This is a standard discrete proxy and is proportional to true CRPS in the
    limit K → ∞. We use it as a rank-preserving summary metric.
    """
    taus = sorted(quantile_preds.keys())
    losses = [pinball_loss(y_true, quantile_preds[t], t) for t in taus]
    return float(2.0 * np.mean(losses))


def coverage(y_true: np.ndarray, y_lower: np.ndarray, y_upper: np.ndarray) -> float:
    """Empirical fraction of y_true falling in [y_lower, y_upper]."""
    return float(np.mean((y_true >= y_lower) & (y_true <= y_upper)))


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """NaN-safe MAE. Ignores rows where y_pred is NaN."""
    diff = np.abs(y_true - y_pred)
    return float(np.nanmean(diff))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """NaN-safe RMSE. Ignores rows where y_pred is NaN."""
    diff = (y_true - y_pred) ** 2
    return float(np.sqrt(np.nanmean(diff)))


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_one_quantile(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    alpha: float,
    categorical: list,
    variable: str = "",
) -> lgb.Booster:
    params = dict(LGB_BASE_PARAMS)
    params["alpha"] = alpha
    if variable in PER_VARIABLE_PARAMS:
        params.update(PER_VARIABLE_PARAMS[variable])

    train_set = lgb.Dataset(
        X_train, label=y_train, categorical_feature=categorical, free_raw_data=False
    )
    val_set = lgb.Dataset(
        X_val,
        label=y_val,
        categorical_feature=categorical,
        reference=train_set,
        free_raw_data=False,
    )
    booster = lgb.train(
        params,
        train_set,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[val_set],
        valid_names=["val"],
        callbacks=[
            lgb.early_stopping(stopping_rounds=EARLY_STOP, verbose=False),
            lgb.log_evaluation(period=0),  # silent
        ],
    )
    return booster


def rearrange_quantiles(pred_dict: dict) -> dict:
    """Post-hoc cross-quantile rearrangement (Chernozhukov et al., 2010).

    Given predictions keyed by τ, sort each row's values so that the final
    predictions are monotonic in τ. Rank-preserves the mean pinball loss and
    often improves it — a free win.
    """
    taus = sorted(pred_dict.keys())
    preds = np.column_stack([pred_dict[t] for t in taus])
    sorted_preds = np.sort(preds, axis=1)
    return {t: sorted_preds[:, i] for i, t in enumerate(taus)}


# ---------------------------------------------------------------------------
# Baselines
# ---------------------------------------------------------------------------

def baseline_ensemble_mean(X: pd.DataFrame) -> np.ndarray:
    """Equal-weight ensemble mean across all available source columns."""
    src_cols = [c for c in X.columns if c.startswith("src_")]
    return X[src_cols].mean(axis=1, skipna=True).to_numpy()


def baseline_best_single(
    X_val: pd.DataFrame, y_val: pd.Series, X_test: pd.DataFrame
) -> tuple:
    """Pick the source with lowest validation MAE; apply it on test."""
    src_cols = [c for c in X_val.columns if c.startswith("src_")]
    best_src = None
    best_mae = float("inf")
    for c in src_cols:
        mask = X_val[c].notna()
        if not mask.any():
            continue
        m = mae(y_val[mask].to_numpy(), X_val.loc[mask, c].to_numpy())
        if m < best_mae:
            best_mae = m
            best_src = c
    if best_src is None:
        return None, np.full(len(X_test), np.nan)
    return best_src, X_test[best_src].to_numpy()


# ---------------------------------------------------------------------------
# Per-variable orchestration
# ---------------------------------------------------------------------------

def train_variable(
    conn: sqlite3.Connection,
    variable: str,
    quantiles: list,
    train_end: str,
    val_end: str,
    test_end: str,
    output_dir: str,
    city_id: Optional[int] = None,
    dry_run: bool = False,
    target_source: str = "era5",
    feature_set: str = FEATURE_SET_V1,
    cities_meta: Optional[dict] = None,
) -> dict:
    """Train all quantile models for one target variable. Returns metrics dict."""
    logger.info("=" * 60)
    logger.info(
        "Variable: %s  (target_source=%s feature_set=%s)",
        variable, target_source, feature_set,
    )

    df = load_joined(
        conn,
        variable,
        city_id=city_id,
        target_source=target_source,
        load_aux=(feature_set in (FEATURE_SET_V2, FEATURE_SET_V3)),
        feature_set_v3=(feature_set == FEATURE_SET_V3),
    )
    if df.empty:
        logger.warning("[%s] no joined rows, skipping", variable)
        return {"variable": variable, "skipped": "no_data"}

    logger.info("[%s] loaded %d joined rows", variable, len(df))

    X, y, feature_cols = engineer_features(df, cities_meta=cities_meta, feature_set=feature_set)
    df_feat = df.copy()  # keep valid_time for split
    for col in X.columns:
        df_feat[col] = X[col]
    df_feat["_y"] = y

    train_df, val_df, test_df = split_temporal(df_feat, train_end, val_end, test_end)
    logger.info(
        "[%s] split: train=%d val=%d test=%d",
        variable, len(train_df), len(val_df), len(test_df),
    )

    if len(train_df) < MIN_TRAIN_ROWS:
        logger.warning(
            "[%s] only %d train rows (min %d), skipping",
            variable, len(train_df), MIN_TRAIN_ROWS,
        )
        return {
            "variable": variable,
            "skipped": f"insufficient_train_rows_{len(train_df)}",
        }
    if len(val_df) < 10 or len(test_df) < 10:
        logger.warning(
            "[%s] val or test too small (val=%d test=%d), skipping",
            variable, len(val_df), len(test_df),
        )
        return {
            "variable": variable,
            "skipped": f"insufficient_val_or_test",
        }

    X_train = train_df[feature_cols]
    y_train = train_df["_y"]
    X_val = val_df[feature_cols]
    y_val = val_df["_y"]
    X_test = test_df[feature_cols]
    y_test = test_df["_y"]

    categorical = ["month", "city_id"]
    if feature_set == FEATURE_SET_V2:
        categorical = categorical + ["hemisphere_ns", "climate_band"]

    # ---- Train one model per quantile ----
    boosters = {}
    val_preds = {}
    test_preds = {}
    for alpha in quantiles:
        logger.info("[%s] training τ=%.2f on %d rows", variable, alpha, len(X_train))
        t0 = time.time()
        booster = train_one_quantile(
            X_train, y_train, X_val, y_val,
            alpha=alpha, categorical=categorical, variable=variable,
        )
        elapsed = time.time() - t0
        boosters[alpha] = booster
        val_preds[alpha] = booster.predict(X_val, num_iteration=booster.best_iteration)
        test_preds[alpha] = booster.predict(
            X_test, num_iteration=booster.best_iteration
        )
        logger.info(
            "[%s] τ=%.2f trained in %.1fs (best iter %d)",
            variable, alpha, elapsed, booster.best_iteration,
        )

    # Cross-quantile rearrangement
    val_preds_rearranged = rearrange_quantiles(val_preds)
    test_preds_rearranged = rearrange_quantiles(test_preds)

    # ---- Metrics: MOS (rearranged) ----
    y_val_np = y_val.to_numpy()
    y_test_np = y_test.to_numpy()

    mos_metrics = {
        "val": {
            "pinball": {
                f"q{int(round(t*100)):02d}": pinball_loss(
                    y_val_np, val_preds_rearranged[t], t
                )
                for t in quantiles
            },
            "mean_pinball": float(
                np.mean(
                    [
                        pinball_loss(y_val_np, val_preds_rearranged[t], t)
                        for t in quantiles
                    ]
                )
            ),
            "crps_proxy": empirical_crps_from_quantiles(
                y_val_np, val_preds_rearranged
            ),
        },
        "test": {
            "pinball": {
                f"q{int(round(t*100)):02d}": pinball_loss(
                    y_test_np, test_preds_rearranged[t], t
                )
                for t in quantiles
            },
            "mean_pinball": float(
                np.mean(
                    [
                        pinball_loss(y_test_np, test_preds_rearranged[t], t)
                        for t in quantiles
                    ]
                )
            ),
            "crps_proxy": empirical_crps_from_quantiles(
                y_test_np, test_preds_rearranged
            ),
        },
    }

    # Coverage for the central 80% interval
    if 0.1 in quantiles and 0.9 in quantiles:
        mos_metrics["val"]["coverage_80"] = coverage(
            y_val_np, val_preds_rearranged[0.1], val_preds_rearranged[0.9]
        )
        mos_metrics["test"]["coverage_80"] = coverage(
            y_test_np, test_preds_rearranged[0.1], test_preds_rearranged[0.9]
        )

    # MAE of median
    if 0.5 in quantiles:
        mos_metrics["val"]["median_mae"] = mae(
            y_val_np, val_preds_rearranged[0.5]
        )
        mos_metrics["test"]["median_mae"] = mae(
            y_test_np, test_preds_rearranged[0.5]
        )
        mos_metrics["val"]["median_rmse"] = rmse(
            y_val_np, val_preds_rearranged[0.5]
        )
        mos_metrics["test"]["median_rmse"] = rmse(
            y_test_np, test_preds_rearranged[0.5]
        )

    # ---- Baselines ----
    ensemble_test = baseline_ensemble_mean(X_test)
    best_src_name, best_single_test = baseline_best_single(X_val, y_val, X_test)

    baseline_metrics = {
        "ensemble_mean": {
            "test_mae": mae(y_test_np, ensemble_test),
            "test_rmse": rmse(y_test_np, ensemble_test),
        }
    }
    if best_src_name is not None:
        baseline_metrics["best_single"] = {
            "source": best_src_name,
            "test_mae": mae(y_test_np, best_single_test),
            "test_rmse": rmse(y_test_np, best_single_test),
        }

    # ---- Persist ----
    var_dir = os.path.join(output_dir, variable)
    if not dry_run:
        os.makedirs(var_dir, exist_ok=True)
        for alpha, booster in boosters.items():
            path = os.path.join(var_dir, f"q{int(round(alpha*100)):03d}.txt")
            booster.save_model(path, num_iteration=booster.best_iteration)
        metadata = {
            "variable": variable,
            "quantiles": quantiles,
            "feature_set": feature_set,
            "feature_cols": feature_cols,
            "categorical": categorical,
            "train_rows": int(len(train_df)),
            "val_rows": int(len(val_df)),
            "test_rows": int(len(test_df)),
            "train_end": train_end,
            "val_end": val_end,
            "test_end": test_end,
            "hyperparams": LGB_BASE_PARAMS,
            "num_boost_round_cap": NUM_BOOST_ROUND,
            "early_stopping": EARLY_STOP,
            "best_iterations": {
                str(a): int(boosters[a].best_iteration) for a in quantiles
            },
            "trained_at": datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z",
        }
        with open(os.path.join(var_dir, "metadata.json"), "w") as f:
            json.dump(metadata, f, indent=2)
        logger.info("[%s] saved %d models to %s", variable, len(boosters), var_dir)

    return {
        "variable": variable,
        "rows": {
            "train": int(len(train_df)),
            "val": int(len(val_df)),
            "test": int(len(test_df)),
        },
        "mos": mos_metrics,
        "baselines": baseline_metrics,
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def format_summary(report: dict) -> str:
    """Human-readable summary table."""
    lines = []
    lines.append(
        f"MOS Training Report — {report.get('trained_at', '?')}"
    )
    lines.append("=" * 72)
    for var_result in report["variables"]:
        v = var_result["variable"]
        if "skipped" in var_result:
            lines.append(f"\n{v}: SKIPPED ({var_result['skipped']})")
            continue
        rows = var_result["rows"]
        mos = var_result["mos"]
        bl = var_result["baselines"]
        lines.append(
            f"\n{v}   train={rows['train']}  val={rows['val']}  test={rows['test']}"
        )
        lines.append(
            f"  MOS test:  median_MAE={mos['test'].get('median_mae', float('nan')):.3f}"
            f"  mean_pinball={mos['test']['mean_pinball']:.3f}"
            f"  CRPS≈{mos['test']['crps_proxy']:.3f}"
        )
        if "coverage_80" in mos["test"]:
            lines.append(
                f"  80% interval coverage (test): {mos['test']['coverage_80']:.3f}"
                f" (target 0.800)"
            )
        for name, m in bl.items():
            extra = f" [{m['source']}]" if "source" in m else ""
            lines.append(
                f"  baseline {name}{extra}: MAE={m['test_mae']:.3f}  RMSE={m['test_rmse']:.3f}"
            )
        # Deploy verdict
        mos_mae = mos["test"].get("median_mae")
        ens_mae = bl.get("ensemble_mean", {}).get("test_mae")
        best_mae = bl.get("best_single", {}).get("test_mae")
        if mos_mae is not None and ens_mae is not None:
            verdict = "✓ BEATS" if mos_mae < ens_mae else "✗ WORSE than"
            lines.append(f"  vs ensemble_mean: {verdict} ({mos_mae:.3f} vs {ens_mae:.3f})")
        if mos_mae is not None and best_mae is not None:
            verdict = "✓ BEATS" if mos_mae < best_mae else "✗ WORSE than"
            lines.append(f"  vs best_single:   {verdict} ({mos_mae:.3f} vs {best_mae:.3f})")
    lines.append("\n" + "=" * 72)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--variables",
        default=",".join(DEFAULT_VARIABLES),
        help=f"Comma-separated target variables (default: {','.join(DEFAULT_VARIABLES)})",
    )
    p.add_argument(
        "--quantiles",
        default=",".join(str(q) for q in DEFAULT_QUANTILES),
        help="Comma-separated quantile levels (default: 0.1,0.5,0.9)",
    )
    p.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    p.add_argument("--val-end", default=DEFAULT_VAL_END)
    p.add_argument("--test-end", default=DEFAULT_TEST_END)
    p.add_argument(
        "--output-dir",
        default=None,
        help="Where to save models (default: data/mos_models/ or data/mos_models_station/)",
    )
    p.add_argument(
        "--target-source",
        choices=["era5", "station"],
        default="era5",
        help="Which observation table to use as training target (default: era5). "
             "'station' uses the Meteostat-backfilled observations_hourly_station table.",
    )
    p.add_argument(
        "--feature-set",
        choices=[FEATURE_SET_V1, FEATURE_SET_V2, FEATURE_SET_V3],
        default=FEATURE_SET_V1,
        help="v1 = baseline (src+ensemble+cyclic+city_id). "
             "v2 = + climate-zone (lat/lon/elev/hemisphere/band) + physics "
             "(saturation_distance, specific_humidity, LCL height). "
             "v3 = + cross-variable aux features (dew_point/pressure/cloud_cover "
             "ensemble mean+std, wind_dir circular, pressure tendency).",
    )
    p.add_argument(
        "--city-id", type=int, default=None, help="Only train on this city"
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Train but don't save models"
    )
    p.add_argument("--quiet", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    variables = [v.strip() for v in args.variables.split(",") if v.strip()]
    quantiles = [float(q) for q in args.quantiles.split(",") if q.strip()]
    quantiles.sort()

    if args.target_source == "era5":
        default_dir = MODELS_DIR_NAME
    else:
        default_dir = f"{MODELS_DIR_NAME}_station"
    output_dir = args.output_dir or os.path.join(_project_dir(), "data", default_dir)

    conn = get_connection()
    cities_meta = (
        load_cities_meta(conn)
        if args.feature_set in (FEATURE_SET_V2, FEATURE_SET_V3)
        else None
    )
    logger.info(
        "Training plan: %d variables × %d quantiles, split=%s|%s|%s feature_set=%s",
        len(variables), len(quantiles), args.train_end, args.val_end, args.test_end,
        args.feature_set,
    )

    report = {
        "trained_at": datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat() + "Z",
        "db_path": DB_PATH,
        "train_end": args.train_end,
        "val_end": args.val_end,
        "test_end": args.test_end,
        "quantiles": quantiles,
        "city_id": args.city_id,
        "target_source": args.target_source,
        "feature_set": args.feature_set,
        "variables": [],
    }
    t0 = time.time()
    for v in variables:
        try:
            result = train_variable(
                conn,
                variable=v,
                quantiles=quantiles,
                train_end=args.train_end,
                val_end=args.val_end,
                test_end=args.test_end,
                output_dir=output_dir,
                city_id=args.city_id,
                dry_run=args.dry_run,
                target_source=args.target_source,
                feature_set=args.feature_set,
                cities_meta=cities_meta,
            )
            report["variables"].append(result)
        except Exception as e:
            logger.exception("[%s] training failed: %s", v, e)
            report["variables"].append({"variable": v, "error": str(e)})
    elapsed = time.time() - t0

    # Persist report
    if not args.dry_run:
        os.makedirs(output_dir, exist_ok=True)
        report_path = os.path.join(output_dir, "eval_report.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        summary_path = os.path.join(output_dir, "eval_summary.txt")
        summary = format_summary(report)
        with open(summary_path, "w") as f:
            f.write(summary)
        logger.info("Report saved: %s", report_path)
        print(summary)
    else:
        print(format_summary(report))

    logger.warning("Training complete in %.1fs", elapsed)


if __name__ == "__main__":
    main()
