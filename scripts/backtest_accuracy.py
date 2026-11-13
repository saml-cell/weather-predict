#!/usr/bin/env python3
"""Score replayable forecasts against observations for Bratislava.

Sources with a public forecast archive are scored. Commercial APIs that need
a key and do not publish past runs (OpenWeatherMap, WeatherAPI, Visual
Crossing, wttr.in) stay in the live ensemble but are not invented into this
report.

Ground truth:
  * Open-Meteo archive (ERA5 / station blend) for the multi-week window
  * SHMÚ automatic-station minutes, aggregated to local days, for the recent
    days those files are still on opendata.shmu.sk

Run:
  python scripts/backtest_accuracy.py
  python scripts/backtest_accuracy.py --shmu-days 3 --skip-aladin
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scoring import (
    brier_score,
    hit_rate,
    inverse_mae_weights,
    mae,
    precipitation_event,
    rmse,
    round_or_none,
    weighted_values,
)

LAT = 48.15
LON = 17.11
PLACE = "Bratislava"
TIMEZONE = "Europe/Bratislava"
WET_MM = 1.0
REPORT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports",
    "bratislava_accuracy.json",
)


def _get_json(url):
    req = Request(url, headers={"User-Agent": "WeatherPredict/1.0 backtest"})
    with urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def _daily_url(base, params):
    return base + "?" + urlencode(params)


def fetch_archive(start, end):
    url = _daily_url(
        "https://archive-api.open-meteo.com/v1/archive",
        {
            "latitude": LAT,
            "longitude": LON,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": TIMEZONE,
        },
    )
    raw = _get_json(url)
    return _rows(raw.get("daily") or {}, precip_prob=False)


def fetch_historical_model(model, start, end):
    daily = "temperature_2m_max,temperature_2m_min,precipitation_sum"
    if model == "best_match":
        daily += ",precipitation_probability_max"
    url = _daily_url(
        "https://historical-forecast-api.open-meteo.com/v1/forecast",
        {
            "latitude": LAT,
            "longitude": LON,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "daily": daily,
            "timezone": TIMEZONE,
            "models": model,
        },
    )
    raw = _get_json(url)
    return _rows(raw.get("daily") or {}, precip_prob=True)


def _rows(daily, precip_prob):
    times = daily.get("time") or []
    highs = daily.get("temperature_2m_max") or []
    lows = daily.get("temperature_2m_min") or []
    precips = daily.get("precipitation_sum") or []
    probs = daily.get("precipitation_probability_max") or []
    rows = {}
    for i, day in enumerate(times):
        prob = None
        if precip_prob and i < len(probs) and probs[i] is not None:
            prob = float(probs[i]) / 100.0
        rows[day] = {
            "temp_high_c": highs[i] if i < len(highs) else None,
            "temp_low_c": lows[i] if i < len(lows) else None,
            "precip_mm": precips[i] if i < len(precips) else None,
            "precip_probability": prob,
        }
    return rows


def _aligned(forecast_rows, obs_rows, field):
    days = sorted(set(forecast_rows) & set(obs_rows))
    forecast = [forecast_rows[d].get(field) for d in days]
    observed = [obs_rows[d].get(field) for d in days]
    return days, forecast, observed


def _score_temp(forecast_rows, obs_rows, field):
    _days, forecast, observed = _aligned(forecast_rows, obs_rows, field)
    return {
        "n": sum(1 for f, o in zip(forecast, observed) if f is not None and o is not None),
        "mae": round_or_none(mae(forecast, observed)),
        "rmse": round_or_none(rmse(forecast, observed)),
    }


def _score_precip(forecast_rows, obs_rows):
    days = sorted(set(forecast_rows) & set(obs_rows))
    probs = []
    outcomes = []
    predicted = []
    observed_yes = []
    for day in days:
        obs_mm = obs_rows[day].get("precip_mm")
        event = precipitation_event(obs_mm, WET_MM)
        if event is None:
            continue
        prob = forecast_rows[day].get("precip_probability")
        amount = forecast_rows[day].get("precip_mm")
        if prob is None and amount is not None:
            prob = 1.0 if amount >= WET_MM else 0.0
        if prob is None:
            continue
        probs.append(prob)
        outcomes.append(1.0 if event else 0.0)
        predicted.append(prob >= 0.5)
        observed_yes.append(event)
    return {
        "n": len(outcomes),
        "wet_days": int(sum(outcomes)),
        "threshold_mm": WET_MM,
        "brier": round_or_none(brier_score(probs, outcomes)),
        "hit_rate": round_or_none(hit_rate(predicted, observed_yes)),
        "probability": "published" if any(
            forecast_rows[d].get("precip_probability") is not None for d in days
        ) else "deterministic_from_amount",
    }


def _subset(rows, days):
    return {day: rows[day] for day in days if day in rows}


def _ensemble_rows(source_rows, weights, days):
    built = {}
    for day in days:
        values_high = {}
        values_low = {}
        values_precip = {}
        values_prob = {}
        for source, rows in source_rows.items():
            row = rows.get(day)
            if not row:
                continue
            values_high[source] = row.get("temp_high_c")
            values_low[source] = row.get("temp_low_c")
            values_precip[source] = row.get("precip_mm")
            values_prob[source] = row.get("precip_probability")
        high = weighted_values(values_high, weights)
        low = weighted_values(values_low, weights)
        precip = weighted_values(values_precip, weights)
        prob = weighted_values(values_prob, weights)
        if prob is None and precip is not None:
            prob = 1.0 if precip >= WET_MM else 0.0
        built[day] = {
            "temp_high_c": None if high is None else round(high, 3),
            "temp_low_c": None if low is None else round(low, 3),
            "precip_mm": None if precip is None else round(precip, 3),
            "precip_probability": None if prob is None else round(prob, 3),
        }
    return built


def score_window(source_rows, obs_rows, days, weights):
    ensemble = _ensemble_rows(source_rows, weights, days)
    window_obs = _subset(obs_rows, days)
    by_source = {}
    for name, rows in source_rows.items():
        by_source[name] = {
            "temperature_max_c": _score_temp(rows, window_obs, "temp_high_c"),
            "temperature_min_c": _score_temp(rows, window_obs, "temp_low_c"),
            "precipitation": _score_precip(rows, window_obs),
        }
    by_source["ensemble"] = {
        "temperature_max_c": _score_temp(ensemble, window_obs, "temp_high_c"),
        "temperature_min_c": _score_temp(ensemble, window_obs, "temp_low_c"),
        "precipitation": _score_precip(ensemble, window_obs),
        "weights": {k: round(v, 3) for k, v in weights.items()},
    }
    return by_source


def shmu_section(shmu_days, skip_aladin):
    """Score ALADIN and the archive against SHMÚ station minutes."""
    import shmu

    today = datetime.now(timezone.utc).astimezone(shmu._zone(TIMEZONE)).date()
    # Yesterday is the last day with a complete minute archive.
    end = today - timedelta(days=1)
    start = end - timedelta(days=shmu_days - 1)
    dates = []
    cursor = start
    while cursor <= end:
        dates.append(cursor)
        cursor += timedelta(days=1)

    stations = shmu.load_stations()
    station = shmu.nearest_station(stations, LAT, LON)
    if station is None:
        return {"error": "no SHMU station near Bratislava"}
    print(f"SHMU station {station['name']} ({station['ind_kli']}) {station['distance_km']} km", flush=True)
    print(f"Downloading minute observations {start} .. {end}", flush=True)
    observed_days = shmu.collect_station_days(station["ind_kli"], dates)
    obs_rows = {row["date"]: {
        "temp_high_c": row["temp_high_c"],
        "temp_low_c": row["temp_low_c"],
        "precip_mm": row["precip_mm"],
    } for row in observed_days}

    aladin_rows = {}
    if not skip_aladin:
        for day in dates:
            run_day = day - timedelta(days=1)
            print(f"ALADIN run {run_day} verifying {day}", flush=True)
            try:
                daily = shmu.aladin_daily_for_run(run_day, LAT, LON, TIMEZONE)
            except Exception as exc:
                print(f"  ALADIN run failed: {exc}", flush=True)
                continue
            match = next((item for item in daily if item["date"] == day.isoformat()), None)
            if not match:
                continue
            aladin_rows[day.isoformat()] = {
                "temp_high_c": match.get("high_c"),
                "temp_low_c": match.get("low_c"),
                "precip_mm": match.get("precip_mm"),
                "precip_probability": None,
            }

    archive = fetch_archive(start, end)
    return {
        "station": station,
        "dates": [d.isoformat() for d in dates],
        "definition": (
            "SHMU truth is the sum/max/min of 1-minute AWS rows on the civil date. "
            "ALADIN is the previous day's 00 UTC run, aggregated on the same civil date "
            "(about 24-47 h lead). Precipitation probability for ALADIN is deterministic: "
            f"1 when the daily total is at least {WET_MM} mm."
        ),
        "against_shmu_station": score_window(
            {"SHMU": aladin_rows, "Open-Meteo-archive-grid": archive},
            obs_rows,
            [d.isoformat() for d in dates],
            {"SHMU": 1.0, "Open-Meteo-archive-grid": 1.0},
        ) if aladin_rows else {"note": "ALADIN runs were not scored", "observations": obs_rows},
        "shmu_observations": obs_rows,
        "aladin_forecasts": aladin_rows,
    }


def main():
    parser = argparse.ArgumentParser(description="Backtest Bratislava forecast accuracy")
    parser.add_argument("--days", type=int, default=24, help="Archive window ending yesterday")
    parser.add_argument("--train-days", type=int, default=16, help="Days used only to set ensemble weights")
    parser.add_argument("--shmu-days", type=int, default=3, help="Recent days scored against SHMU minutes")
    parser.add_argument("--skip-shmu", action="store_true")
    parser.add_argument("--skip-aladin", action="store_true")
    args = parser.parse_args()

    today = date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=args.days - 1)
    print(f"Archive window {start} .. {end}", flush=True)
    obs = fetch_archive(start, end)
    models = {
        "Open-Meteo": "best_match",
        "ECMWF": "ecmwf_ifs025",
    }
    source_rows = {}
    for label, model in models.items():
        print(f"Historical forecast {label}", flush=True)
        source_rows[label] = fetch_historical_model(model, start, end)

    days = sorted(set(obs) & set.intersection(*[set(rows) for rows in source_rows.values()]))
    if len(days) <= args.train_days:
        raise SystemExit(f"Not enough overlapping days ({len(days)})")
    train_days = days[: args.train_days]
    test_days = days[args.train_days :]

    train_mae = {}
    for name, rows in source_rows.items():
        train_mae[name] = mae(
            [rows[d]["temp_high_c"] for d in train_days],
            [obs[d]["temp_high_c"] for d in train_days],
        )
    weights = inverse_mae_weights(train_mae)
    equal = {name: 1.0 / len(source_rows) for name in source_rows}

    report = {
        "location": {"name": PLACE, "latitude": LAT, "longitude": LON, "timezone": TIMEZONE},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "observation_source": "Open-Meteo archive API (ERA5 / observation blend)",
        "forecast_source": "Open-Meteo historical forecast API, previous model runs",
        "window": {"start": days[0], "end": days[-1], "n_days": len(days)},
        "train_window": {"start": train_days[0], "end": train_days[-1], "n_days": len(train_days)},
        "test_window": {"start": test_days[0], "end": test_days[-1], "n_days": len(test_days)},
        "notes": [
            "Temperature scores are degrees Celsius for the daily maximum and minimum.",
            f"A wet day is precipitation of at least {WET_MM} mm.",
            "Open-Meteo publishes a daily precipitation probability; ECMWF here is deterministic, so its probability is 1 when the forecast total is at least the wet-day threshold.",
            "Ensemble weights for the test window are inverse-MAE on daily maximum temperature, fit only on the train window.",
            "The equal-weight ensemble is reported on the full window and does not use the train split.",
            "wttr.in, OpenWeatherMap, WeatherAPI and Visual Crossing are not in this file: they have no keyless forecast archive to replay.",
        ],
        "train_mae_temperature_max_c": {k: round_or_none(v) for k, v in train_mae.items()},
        "test": score_window(source_rows, obs, test_days, weights),
        "full_window_equal_weight": score_window(source_rows, obs, days, equal),
    }
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
    print(f"Wrote archive scores to {REPORT_PATH}", flush=True)
    if not args.skip_shmu:
        try:
            report["shmu"] = shmu_section(max(1, args.shmu_days), args.skip_aladin)
        except Exception as exc:
            report["shmu"] = {"error": str(exc)}
            print(f"SHMU section failed: {exc}", flush=True)
        with open(REPORT_PATH, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        print(f"Updated {REPORT_PATH} with SHMU", flush=True)
    test_ens = report["test"]["ensemble"]["temperature_max_c"]
    print(
        f"Test ensemble daily-max MAE {test_ens['mae']} RMSE {test_ens['rmse']} n={test_ens['n']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
