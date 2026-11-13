#!/usr/bin/env python3
"""Backtests for the long-range outlook, climate drivers, and forecast drift.

Writes reports/outlook_backtest.json, reports/climate_drivers.json and
reports/forecast_drift.json. Every number in those files is computed here.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from climate_indices import fetch_url, parse_cpc_qbo_format, parse_mjo_format, parse_psl_format
from outlook import (
    WET_MM,
    apply_calibration,
    brier_skill_score,
    build_climatology,
    crps_normal,
    feature_skill,
    fit_logistic_calibration,
    flip_rate,
    mean_crps,
    reliability_curve,
    revision_summary,
    within_rate,
)
from scoring import brier_score, mae
from shmu import fetch_validated_daily

REPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
LAT = 48.15
LON = 17.11
STATION = 11816
TIMEZONE = "Europe/Bratislava"

INDICES = {
    "nao": ("https://psl.noaa.gov/data/correlation/nao.data", parse_psl_format),
    "ao": ("https://psl.noaa.gov/data/correlation/ao.data", parse_psl_format),
    "nino34": ("https://psl.noaa.gov/data/correlation/nina34.anom.data", parse_psl_format),
    "qbo": ("https://www.cpc.ncep.noaa.gov/data/indices/qbo.u30.index", parse_cpc_qbo_format),
    "amo": ("https://psl.noaa.gov/data/correlation/amon.us.data", parse_psl_format),
}
VORTEX_URLS = [
    "https://www.cpc.ncep.noaa.gov/products/stratosphere/strat-trop/data/u10_60N.txt",
    "https://downloads.psl.noaa.gov/Public/map/reanalysis/U10_60N.ascii",
]


def _get(url, timeout=90):
    last = None
    for attempt in range(4):
        req = urllib.request.Request(url, headers={"User-Agent": "WeatherPredict/1.0 outlook-backtest"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except Exception as exc:
            last = exc
            if attempt == 3:
                break
    raise last


def fetch_archive(lat, lon, start, end):
    url = "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode({
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": TIMEZONE,
    })
    daily = _get(url).get("daily") or {}
    rows = []
    for i, day in enumerate(daily.get("time") or []):
        rows.append({
            "date": day,
            "tmax": (daily.get("temperature_2m_max") or [None])[i] if i < len(daily.get("temperature_2m_max") or []) else None,
            "tmin": (daily.get("temperature_2m_min") or [None])[i] if i < len(daily.get("temperature_2m_min") or []) else None,
            "precip": (daily.get("precipitation_sum") or [None])[i] if i < len(daily.get("precipitation_sum") or []) else None,
        })
    return {row["date"]: row for row in rows}


def fetch_previous_daily(past_days=80):
    fields = [
        "temperature_2m", "temperature_2m_previous_day1", "temperature_2m_previous_day3",
        "temperature_2m_previous_day7",
        "precipitation", "precipitation_previous_day1", "precipitation_previous_day3",
        "precipitation_previous_day7",
    ]
    url = "https://previous-runs-api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode({
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(fields),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": TIMEZONE,
    })
    hourly = _get(url, timeout=120).get("hourly") or {}
    by_date = {}
    times = hourly.get("time") or []
    for i, stamp in enumerate(times):
        day = stamp[:10]
        bucket = by_date.setdefault(day, {})
        for key, values in hourly.items():
            if key == "time" or i >= len(values) or values[i] is None:
                continue
            if key.startswith("temperature_2m"):
                slot = bucket.setdefault(key, [])
                slot.append(float(values[i]))
            elif key.startswith("precipitation"):
                slot = bucket.setdefault(key, [])
                slot.append(float(values[i]))
    daily = {}
    for day, fields_map in by_date.items():
        daily[day] = {}
        for key, values in fields_map.items():
            if key.startswith("temperature"):
                daily[day][key] = max(values) if values else None
            else:
                daily[day][key] = sum(values) if values else None
    return daily


def index_lookup(series):
    """Map (year, month) -> value from parser tuples."""
    return {(year, month): value for year, month, value in series}


def try_vortex():
    for url in VORTEX_URLS:
        try:
            text = fetch_url(url, timeout=25)
        except Exception as exc:
            continue
        parsed = parse_psl_format(text)
        if len(parsed) > 24:
            return url, parsed
    return None, []


def month_value(lookup, day):
    return lookup.get((day.year, day.month))


def evaluate_lead(previous, observations, forecast_key, precip_key, climatology):
    dates = sorted(set(previous) & set(observations))
    dates = [day for day in dates if previous[day].get(forecast_key) is not None and observations[day].get("tmax") is not None]
    if len(dates) < 20:
        return {"n": len(dates), "status": "too_few_samples"}
    cut = max(10, int(len(dates) * 0.7))
    train, test = dates[:cut], dates[cut:]
    tmax_f = [previous[d][forecast_key] for d in test]
    tmax_o = [observations[d]["tmax"] for d in test]
    temp_mae = mae(tmax_f, tmax_o)
    clim_mu = []
    clim_sig = []
    model_mu = []
    model_sig = []
    # Spread is the historical train RMSE, a constant for this lead.
    train_errors = [previous[d][forecast_key] - observations[d]["tmax"] for d in train if previous[d].get(forecast_key) is not None and observations[d].get("tmax") is not None]
    sigma = (sum(e * e for e in train_errors) / len(train_errors)) ** 0.5 if train_errors else 3.0
    rain_probs = []
    rain_obs = []
    rain_clim = []
    crps_m = []
    crps_c = []
    obs_means = [observations[d]["tmax"] for d in train if observations[d].get("tmax") is not None]
    clim_mean = sum(obs_means) / len(obs_means) if obs_means else 8.0
    clim_std = (sum((v - clim_mean) ** 2 for v in obs_means) / len(obs_means)) ** 0.5 if len(obs_means) > 2 else 4.0
    # Logistic rain model from forecast millimetres, fit on the train window.
    train_mm = [previous[d].get(precip_key) for d in train]
    train_wet = [1.0 if (observations[d].get("precip") or 0) >= WET_MM else 0.0 for d in train]
    # Use the forecast mm directly as a pseudo-probability via a fitted map.
    raw_train = []
    for value, wet in zip(train_mm, train_wet):
        if value is None:
            continue
        raw_train.append((min(1.0, max(0.0, float(value) / 5.0)), wet))
    calibration = fit_logistic_calibration([p for p, _ in raw_train], [o for _, o in raw_train])
    for day in test:
        obs = observations[day]
        fc = previous[day][forecast_key]
        model_mu.append(fc)
        yd = str(date.fromisoformat(day).timetuple().tm_yday)
        day_clim = climatology.get(yd) or {}
        crps_m.append(crps_normal(fc, sigma, obs["tmax"]))
        crps_c.append(crps_normal(day_clim.get("tmax_mean", clim_mean), day_clim.get("tmax_std", clim_std) or clim_std, obs["tmax"]))
        mm = previous[day].get(precip_key)
        if mm is None or obs.get("precip") is None:
            continue
        raw = min(1.0, max(0.0, float(mm) / 5.0))
        rain_probs.append(apply_calibration(raw, calibration))
        rain_obs.append(1.0 if obs["precip"] >= WET_MM else 0.0)
        rain_clim.append(day_clim.get("p_rain", 0.3))
    crps_model = sum(v for v in crps_m if v is not None) / len(crps_m) if crps_m else None
    crps_clim = sum(v for v in crps_c if v is not None) / len(crps_c) if crps_c else None
    crps_skill = None if not crps_model or not crps_clim else 1.0 - crps_model / crps_clim
    return {
        "n_test": len(test),
        "n_train": len(train),
        "temperature_max_mae_c": None if temp_mae is None else round(temp_mae, 3),
        "crps_model": None if crps_model is None else round(crps_model, 3),
        "crps_climatology": None if crps_clim is None else round(crps_clim, 3),
        "crps_skill_score": None if crps_skill is None else round(crps_skill, 3),
        "rain_brier": None if not rain_probs else round(brier_score(rain_probs, rain_obs), 3),
        "rain_brier_climatology": None if not rain_probs else round(brier_score(rain_clim, rain_obs), 3),
        "rain_brier_skill_score": None if not rain_probs else (
            None if brier_skill_score(rain_probs, rain_obs, rain_clim) is None else round(brier_skill_score(rain_probs, rain_obs, rain_clim), 3)
        ),
        "rain_reliability": reliability_curve(rain_probs, rain_obs),
        "rain_calibration": calibration,
        "error_sigma_c": round(sigma, 3),
    }


def shmu_rows():
    months = []
    for year, month in [(2025, m) for m in range(1, 13)] + [(2026, m) for m in range(1, 9)]:
        months.append(f"{year}-{month:02d}")
    rows = fetch_validated_daily(STATION, months)
    return {row["date"]: {"date": row["date"], "tmax": row["temp_high_c"], "tmin": row["temp_low_c"], "precip": row["precip_mm"]} for row in rows}


def climate_driver_section(shmu, indices):
    # Target: SHMU daily max anomaly versus that month's own mean.
    by_month = {}
    for day, row in shmu.items():
        if row.get("tmax") is None:
            continue
        parsed = date.fromisoformat(day)
        by_month.setdefault(parsed.month, []).append(row["tmax"])
    month_mean = {month: sum(vals) / len(vals) for month, vals in by_month.items() if vals}
    days = sorted(shmu)
    results = {}
    unavailable = []
    for name, lookup in indices.items():
        if not lookup:
            unavailable.append(name)
            continue
        results[name] = {}
        for lead in (14, 28, 42, 90):
            xs, ys = [], []
            for day in days:
                target = date.fromisoformat(day)
                issue = target - timedelta(days=lead)
                x = month_value(lookup, issue)
                y_raw = shmu[day].get("tmax")
                if x is None or y_raw is None or target.month not in month_mean:
                    continue
                xs.append(x)
                ys.append(y_raw - month_mean[target.month])
            results[name][f"lead_{lead}d"] = feature_skill(xs, ys)
    kept = []
    for name, leads in results.items():
        for lead, stats in leads.items():
            if stats.get("kept"):
                kept.append({"feature": name, "lead": lead, **stats})
    return {"features": results, "kept": kept, "unavailable": unavailable, "station": STATION, "n_days": len(shmu)}


def drift_section(previous, observations):
    dates = sorted(set(previous) & set(observations))
    revisions = []
    early_wet = []
    late_wet = []
    spread_proxy = []
    for day in dates:
        early = previous[day].get("temperature_2m_previous_day7")
        late = previous[day].get("temperature_2m")
        obs = observations[day].get("tmax")
        if early is None or late is None:
            continue
        revisions.append(late - early)
        early_p = previous[day].get("precipitation_previous_day7")
        late_p = previous[day].get("precipitation")
        early_wet.append(None if early_p is None else early_p >= WET_MM)
        late_wet.append(None if late_p is None else late_p >= WET_MM)
        spread_proxy.append(None if obs is None else abs(early - obs))
    if len(revisions) < 20:
        return {"status": "too_few_samples", "n": len(revisions)}
    cut = max(10, int(len(revisions) * 0.7))
    train, test = revisions[:cut], revisions[cut:]
    train_rate = within_rate(train, 1.5)
    # Constant probability from the training window, scored on the test window.
    within_probs = [train_rate] * len(test)
    within_obs = [1.0 if abs(v) <= 1.5 else 0.0 for v in test]
    train_flip = flip_rate(early_wet[:cut], late_wet[:cut])
    test_early = early_wet[cut:]
    test_late = late_wet[cut:]
    flip_obs = [1.0 if (a is not None and b is not None and bool(a) != bool(b)) else 0.0 for a, b in zip(test_early, test_late) if a is not None and b is not None]
    flip_probs = [train_flip] * len(flip_obs) if train_flip is not None else []
    # Does |forecast error vs the eventual observation| predict |revision|? Compare std in two halves.
    paired = [(s, r) for s, r in zip(spread_proxy, revisions) if s is not None]
    paired.sort(key=lambda item: item[0])
    half = len(paired) // 2
    low = [abs(r) for _, r in paired[:half]]
    high = [abs(r) for _, r in paired[half:]]
    summary = revision_summary(revisions)
    return {
        "definition": "Revision is the latest archived run minus the run from 7 days earlier (Open-Meteo previous_day7 versus the latest previous-runs temperature_2m), for daily maximum temperature at 48.15, 17.11. Observations for the spread test are the Open-Meteo archive.",
        "n": len(revisions),
        "revision_c": summary,
        "bias_direction": "warmer" if summary and summary["mean_c"] > 0.1 else ("colder" if summary and summary["mean_c"] < -0.1 else "near_zero"),
        "rain_flip_rate": None if flip_rate(early_wet, late_wet) is None else round(flip_rate(early_wet, late_wet), 3),
        "within_1_5c_rate": None if within_rate(revisions, 1.5) is None else round(within_rate(revisions, 1.5), 3),
        "held_out": {
            "n": len(test),
            "probability_within_1_5c": None if train_rate is None else round(train_rate, 3),
            "brier_within_1_5c": None if not within_probs else round(brier_score(within_probs, within_obs), 3),
            "reliability_within_1_5c": reliability_curve(within_probs, within_obs, bins=1) if within_probs else [],
            "probability_rain_flips": None if train_flip is None else round(train_flip, 3),
            "brier_rain_flips": None if not flip_probs else round(brier_score(flip_probs, flip_obs), 3),
        },
        "spread_proxy": {
            "note": "Previous-runs responses used here do not include ensemble member spread. The proxy is |lead-7 forecast minus the later observation|.",
            "mean_abs_revision_when_proxy_low_c": None if not low else round(sum(low) / len(low), 3),
            "mean_abs_revision_when_proxy_high_c": None if not high else round(sum(high) / len(high), 3),
            "n_each": half,
        },
        "convergence": {
            "lead_7_mae_vs_latest_c": _mae_vs_latest(previous, "temperature_2m_previous_day7"),
            "lead_3_mae_vs_latest_c": _mae_vs_latest(previous, "temperature_2m_previous_day3"),
            "lead_1_mae_vs_latest_c": _mae_vs_latest(previous, "temperature_2m_previous_day1"),
        },
    }


def _mae_vs_latest(previous, key):
    errs = []
    for row in previous.values():
        early = row.get(key)
        late = row.get("temperature_2m")
        if early is None or late is None:
            continue
        errs.append(abs(late - early))
    if not errs:
        return None
    return round(sum(errs) / len(errs), 3)


def seas5_skill(observations):
    """Score the Open-Meteo SEAS5 archive for summer 2026 against observations."""
    url = "https://seasonal-api.open-meteo.com/v1/seasonal?" + urllib.parse.urlencode({
        "latitude": LAT,
        "longitude": LON,
        "start_date": "2026-06-01",
        "end_date": "2026-08-31",
        "daily": "temperature_2m_max,precipitation_sum",
        "models": "ecmwf_seas5",
        "timezone": TIMEZONE,
    })
    try:
        daily = _get_daily(url)
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}
    times = daily.get("time") or []
    member_keys = [k for k in daily if k.startswith("temperature_2m_max_member")]
    rain_p, rain_o, rain_c = [], [], []
    mus, sigs, obs = [], [], []
    wet_rate_obs = []
    for i, day in enumerate(times):
        if day not in observations or observations[day].get("tmax") is None:
            continue
        members = []
        for key in member_keys:
            value = (daily.get(key) or [None])[i] if i < len(daily.get(key) or []) else None
            if isinstance(value, (int, float)):
                members.append(float(value))
        if len(members) < 10:
            continue
        mean = sum(members) / len(members)
        var = sum((v - mean) ** 2 for v in members) / len(members)
        mus.append(mean)
        sigs.append(max(var ** 0.5, 0.3))
        obs.append(observations[day]["tmax"])
        precip_keys = [k for k in daily if k.startswith("precipitation_sum_member")]
        precips = []
        for key in precip_keys:
            value = (daily.get(key) or [None])[i] if i < len(daily.get(key) or []) else None
            if isinstance(value, (int, float)):
                precips.append(float(value))
        if precips and observations[day].get("precip") is not None:
            rain_p.append(sum(1 for v in precips if v >= WET_MM) / len(precips))
            rain_o.append(1.0 if observations[day]["precip"] >= WET_MM else 0.0)
            wet_rate_obs.append(observations[day]["precip"])
    if len(obs) < 20:
        return {
            "status": "too_few_samples",
            "n": len(obs),
            "note": "The Open-Meteo seasonal API listed the past dates but every SEAS5 member value was null, so no Brier or CRPS could be computed. Direct CDS hindcasts were not called.",
        }
    clim_mean = sum(obs) / len(obs)
    clim_std = (sum((v - clim_mean) ** 2 for v in obs) / len(obs)) ** 0.5 or 1.0
    crps_m = mean_crps(mus, sigs, obs)
    crps_c = mean_crps([clim_mean] * len(obs), [clim_std] * len(obs), obs)
    p_clim = 0.3
    if rain_o:
        p_clim = sum(rain_o) / len(rain_o)
    return {
        "status": "ok",
        "window": [times[0], times[-1]] if times else None,
        "n": len(obs),
        "members": len(member_keys),
        "temperature_max_mae_c": round(mae(mus, obs), 3),
        "crps_model": None if crps_m is None else round(crps_m, 3),
        "crps_climatology": None if crps_c is None else round(crps_c, 3),
        "crps_skill_score": None if not crps_m or not crps_c else round(1.0 - crps_m / crps_c, 3),
        "rain_brier": None if not rain_p else round(brier_score(rain_p, rain_o), 3),
        "rain_brier_skill_score": None if not rain_p else (
            None if brier_skill_score(rain_p, rain_o, [p_clim] * len(rain_o)) is None
            else round(brier_skill_score(rain_p, rain_o, [p_clim] * len(rain_o)), 3)
        ),
        "rain_reliability": reliability_curve(rain_p, rain_o) if rain_p else [],
        "note": "Open-Meteo seasonal API returned this past window without a CDS key. It is scored as archived SEAS5. ECMWF extended-range (46-day) member data was not available: the public ensemble API returns nulls after about 14 days.",
    }


def _get_daily(url):
    return _get(url, timeout=90).get("daily") or {}


def main():
    print("Archive climatology 2016-2025", flush=True)
    archive = fetch_archive(LAT, LON, date(2016, 1, 1), date(2025, 12, 31))
    climatology = build_climatology(archive.values())
    print("SHMU validated daily", flush=True)
    shmu = shmu_rows()
    print(f"  station days {len(shmu)}", flush=True)
    print("Previous runs", flush=True)
    previous = fetch_previous_daily(80)
    print("Recent archive for those dates", flush=True)
    recent = fetch_archive(LAT, LON, date(2026, 8, 15), date(2026, 11, 12))
    # Prefer SHMU where the validated file exists, otherwise the archive.
    observations = dict(recent)
    for day, row in shmu.items():
        if row.get("tmax") is not None:
            observations[day] = row

    by_lead = {
        "1-7": evaluate_lead(previous, observations, "temperature_2m_previous_day1", "precipitation_previous_day1", climatology),
        "8-14": evaluate_lead(previous, observations, "temperature_2m_previous_day7", "precipitation_previous_day7", climatology),
    }
    # Lead 3 sits inside 1-7 as extra detail.
    by_lead["lead_3_detail"] = evaluate_lead(previous, observations, "temperature_2m_previous_day3", "precipitation_previous_day3", climatology)
    print("SEAS5 past window", flush=True)
    seasonal = seas5_skill(observations)
    by_lead["15-46"] = seasonal
    by_lead["47-180"] = seasonal

    outlook = {
        "location": {"name": "Bratislava", "latitude": LAT, "longitude": LON, "station_ind_kli": STATION},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "climatology_years": "2016-2025 Open-Meteo archive, day-of-year pooled ±7 days",
        "shmu_validated_days": len(shmu),
        "sources": {
            "days_1_14": "ECMWF IFS ensemble via Open-Meteo (about 14 days of non-null members). Skill below uses previous-runs deterministic leads, not a second decoder.",
            "days_15_46": "No public ECMWF extended-range member feed past ~14 days on ensemble-api.open-meteo.com (forecast_days=46 returns HTTP 400; day 15+ members are null). SEAS5 from the Open-Meteo seasonal API is used instead.",
            "days_47_180": "ECMWF SEAS5 via https://seasonal-api.open-meteo.com (no CDS key). CDS_API_KEY is accepted but unused while this endpoint works.",
            "cds": "Not required for the path that ran. Direct Copernicus CDS was not called.",
            "observations": "Lead skill and drift use Open-Meteo archive temperatures where validated SHMU days are missing. SHMU validated daily climate (station 11816) covers 2025-01 through 2026-08 and is the climate-driver target. Previous runs used here start in mid-August 2026, so almost all lead samples are archive observations, not station minutes.",
        },
        "by_lead": by_lead,
        "climatology_sample": {key: climatology[key] for key in list(climatology)[:2]},
        "climatology": climatology,
    }
    print("Climate drivers", flush=True)
    indices = {}
    unavailable = ["mediterranean_sst", "soil_moisture"]
    for name, (url, parser) in INDICES.items():
        try:
            text = fetch_url(url, timeout=30)
            series = parser(text)
            indices[name] = index_lookup(series if not isinstance(series, tuple) else series[0])
            print(f"  {name} {len(indices[name])}", flush=True)
        except Exception as exc:
            indices[name] = {}
            unavailable.append(name)
            print(f"  {name} failed {exc}", flush=True)
    vortex_url, vortex = try_vortex()
    if vortex:
        indices["polar_vortex_u10_60n"] = index_lookup(vortex)
        print(f"  vortex {len(vortex)} from {vortex_url}", flush=True)
    else:
        unavailable.append("polar_vortex_u10_60n")
        print("  vortex unavailable", flush=True)
    # MJO amplitude from the CPC pentad file.
    try:
        mjo_text = fetch_url(
            "https://www.cpc.ncep.noaa.gov/products/precip/CWlink/daily_mjo_index/pentad.mjo.index.b50.current.ascii",
            timeout=30,
        )
        parsed = parse_mjo_format(mjo_text)
        amp = parsed[0] if isinstance(parsed, tuple) else parsed
        indices["mjo_amplitude"] = index_lookup(amp)
        print(f"  mjo {len(indices['mjo_amplitude'])}", flush=True)
    except Exception as exc:
        unavailable.append("mjo_amplitude")
        print(f"  mjo failed {exc}", flush=True)

    drivers = climate_driver_section(shmu, {k: v for k, v in indices.items() if v})
    drivers["unavailable"] = sorted(set(unavailable + drivers.get("unavailable", [])))
    drivers["generated_at"] = datetime.now(timezone.utc).isoformat()
    drivers["rule"] = "A feature is kept only when a one-predictor regression beats predicting zero anomaly by at least 0.05°C MAE on the last 30% of SHMU validated days."

    print("Drift", flush=True)
    drift = drift_section(previous, observations)
    drift["generated_at"] = datetime.now(timezone.utc).isoformat()
    drift["location"] = {"latitude": LAT, "longitude": LON}

    os.makedirs(REPORT_DIR, exist_ok=True)
    for name, payload in (
        ("outlook_backtest.json", outlook),
        ("climate_drivers.json", drivers),
        ("forecast_drift.json", drift),
    ):
        path = os.path.join(REPORT_DIR, name)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        print("wrote", path, flush=True)


if __name__ == "__main__":
    main()
