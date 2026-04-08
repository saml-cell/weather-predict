#!/usr/bin/env python3
"""
Verification and Scoring Engine — fetch observations, compare to predictions,
recompute source weights using Bayesian decay.

Run daily (after collect_forecasts.py has been running for a few days):
  python scripts/verify_and_score.py

Or for a single city:
  python scripts/verify_and_score.py --city Bratislava

Or with a specific date:
  python scripts/verify_and_score.py --date 2026-04-01
"""

import json
import logging
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from db import normalize_condition
from fetch_weather import fetch_json, WMO_CODES

# ---------------------------------------------------------------------------
# Fetch actual observations from Open-Meteo Historical/Archive API
# ---------------------------------------------------------------------------
def fetch_observation(lat, lon, date_str, tz="auto"):
    """Fetch actual weather for a given date from Open-Meteo Archive API.

    Returns dict with temp_high_c, temp_low_c, precip_mm, wind_max_kmh,
    condition_text, pressure_hpa, humidity_pct. Or None if unavailable.
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={date_str}&end_date={date_str}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
        f"wind_speed_10m_max,weather_code"
        f"&hourly=surface_pressure,relative_humidity_2m"
        f"&timezone={tz}"
    )
    raw = fetch_json(url, timeout=15)
    if not raw or "daily" not in raw:
        return None

    daily = raw["daily"]
    if not daily.get("time") or len(daily["time"]) == 0:
        return None

    # Extract daily values
    temp_high = daily.get("temperature_2m_max", [None])[0]
    temp_low = daily.get("temperature_2m_min", [None])[0]
    precip = daily.get("precipitation_sum", [None])[0]
    wind_max = daily.get("wind_speed_10m_max", [None])[0]
    wcode = daily.get("weather_code", [None])[0]
    condition = normalize_condition(WMO_CODES.get(wcode, "Unknown")) if wcode is not None else None

    # Average hourly pressure and humidity for the day
    hourly = raw.get("hourly", {})
    pressures = [p for p in hourly.get("surface_pressure", []) if p is not None]
    humidities = [h for h in hourly.get("relative_humidity_2m", []) if h is not None]

    avg_pressure = round(sum(pressures) / len(pressures), 1) if pressures else None
    avg_humidity = round(sum(humidities) / len(humidities)) if humidities else None

    return {
        "temp_high_c": temp_high,
        "temp_low_c": temp_low,
        "precip_mm": precip,
        "wind_max_kmh": wind_max,
        "condition_text": condition,
        "pressure_hpa": avg_pressure,
        "humidity_pct": avg_humidity,
    }


# ---------------------------------------------------------------------------
# Score forecasts against observations
# ---------------------------------------------------------------------------
def score_forecasts_for_date(city_id, target_date):
    """Compare all source forecasts for target_date against actual observation.

    Returns list of per-source error dicts, or empty list if no observation.
    """
    obs = db.get_observation(city_id, target_date)
    if not obs:
        return []

    forecasts = db.get_forecasts_for_date(city_id, target_date)
    if not forecasts:
        return []

    errors = []
    for fc in forecasts:
        error = {
            "source_name": fc["source_name"],
            "forecast_date": target_date,
        }

        # Temperature errors (absolute and signed)
        if fc["temp_high_c"] is not None and obs["temp_high_c"] is not None:
            error["temp_high_error"] = abs(fc["temp_high_c"] - obs["temp_high_c"])
            error["temp_high_bias"] = fc["temp_high_c"] - obs["temp_high_c"]
        else:
            error["temp_high_error"] = None
            error["temp_high_bias"] = None

        if fc["temp_low_c"] is not None and obs["temp_low_c"] is not None:
            error["temp_low_error"] = abs(fc["temp_low_c"] - obs["temp_low_c"])
            error["temp_low_bias"] = fc["temp_low_c"] - obs["temp_low_c"]
        else:
            error["temp_low_error"] = None
            error["temp_low_bias"] = None

        # Precipitation error (absolute and signed)
        if fc["precip_mm"] is not None and obs["precip_mm"] is not None:
            error["precip_error"] = abs(fc["precip_mm"] - obs["precip_mm"])
            error["precip_bias"] = fc["precip_mm"] - obs["precip_mm"]
        else:
            error["precip_error"] = None
            error["precip_bias"] = None

        # Wind error (absolute and signed)
        if fc["wind_max_kmh"] is not None and obs["wind_max_kmh"] is not None:
            error["wind_error"] = abs(fc["wind_max_kmh"] - obs["wind_max_kmh"])
            error["wind_bias"] = fc["wind_max_kmh"] - obs["wind_max_kmh"]
        else:
            error["wind_error"] = None
            error["wind_bias"] = None

        # Lead time (days between fetch and forecast target)
        if fc.get("fetched_at") and fc.get("forecast_date"):
            try:
                fetch_date = date.fromisoformat(fc["fetched_at"][:10])
                target = date.fromisoformat(fc["forecast_date"])
                error["lead_days"] = (target - fetch_date).days
            except (ValueError, TypeError):
                error["lead_days"] = None
        else:
            error["lead_days"] = None

        # Condition match
        if fc["condition_text"] and obs["condition_text"]:
            error["condition_match"] = 1.0 if fc["condition_text"] == obs["condition_text"] else 0.0
        else:
            error["condition_match"] = None

        errors.append(error)

    return errors


# ---------------------------------------------------------------------------
# Recompute weights with Bayesian exponential decay
# ---------------------------------------------------------------------------
def recompute_weights(city_id, window_days=None):
    """Recompute source weights for a city using Bayesian decay.

    Algorithm:
    1. For each source and metric, gather errors over the window
    2. Apply exponential decay (recent days weighted more)
    3. Compute weighted MAE
    4. Convert to skill scores: skill = 1 / (MAE + epsilon)
    5. Normalize across sources: weight = skill / sum(skills)
    6. Cold start: equal weights if < min_samples
    """
    config = db.load_config()
    scoring = config["scoring"]
    if window_days is None:
        window_days = scoring["window_days"]
    epsilon = scoring["epsilon"]
    min_samples = scoring["min_samples"]
    half_life = scoring.get("decay_half_life", 15)

    today = date.today()
    start_date = (today - timedelta(days=window_days)).isoformat()
    end_date = (today - timedelta(days=1)).isoformat()

    # Get all observations in window
    observations = {
        o["obs_date"]: o
        for o in db.get_observations_in_window(city_id, start_date, end_date)
    }

    if not observations:
        return {}

    # Get all source names that have forecasts for this city
    conn = db.get_connection()
    source_rows = conn.execute(
        "SELECT DISTINCT source_name FROM forecasts WHERE city_id = ?", (city_id,)
    ).fetchall()
    all_sources = [r["source_name"] for r in source_rows]

    if not all_sources:
        return {}

    metrics = ["temp_high", "temp_low", "precip_mm", "wind", "condition"]
    metric_to_error_key = {
        "temp_high": "temp_high_error",
        "temp_low": "temp_low_error",
        "precip_mm": "precip_error",
        "wind": "wind_error",
        "condition": "condition_match",
    }

    # Bias keys for signed error tracking
    metric_to_bias_key = {
        "temp_high": "temp_high_bias",
        "temp_low": "temp_low_bias",
        "precip_mm": "precip_bias",
        "wind": "wind_bias",
        "condition": None,  # no bias for categorical
    }

    # Lead-time group classifier
    def lead_time_group(lead_days):
        if lead_days is None or lead_days <= 1:
            return "day+1"
        elif lead_days <= 3:
            return "day+2-3"
        else:
            return "day+4-7"

    lead_time_groups = ["day+1", "day+2-3", "day+4-7"]

    # Gather errors per source per metric with decay weights
    # Also track bias sums and per-lead-time-group stats
    def _empty_bucket():
        return {"weighted_sum": 0.0, "weight_sum": 0.0, "count": 0,
                "bias_sum": 0.0, "bias_count": 0}

    source_metrics = {}  # {source: {metric: bucket}}
    # {source: {metric: {ltg: bucket}}}
    source_metrics_by_lt = {}

    for source in all_sources:
        source_metrics[source] = {m: _empty_bucket() for m in metrics}
        source_metrics_by_lt[source] = {
            m: {ltg: _empty_bucket() for ltg in lead_time_groups}
            for m in metrics
        }

    for obs_date_str, obs in observations.items():
        obs_date = date.fromisoformat(obs_date_str)
        days_ago = (today - obs_date).days
        decay_weight = math.exp(-days_ago * math.log(2) / half_life)

        errors = score_forecasts_for_date(city_id, obs_date_str)
        for err in errors:
            src = err["source_name"]
            if src not in source_metrics:
                continue

            ltg = lead_time_group(err.get("lead_days"))

            for metric in metrics:
                error_key = metric_to_error_key[metric]
                value = err.get(error_key)
                if value is None:
                    continue

                # Update overall bucket
                sm = source_metrics[src][metric]
                sm["weighted_sum"] += value * decay_weight
                sm["weight_sum"] += decay_weight
                sm["count"] += 1

                # Track signed bias
                bias_key = metric_to_bias_key.get(metric)
                if bias_key:
                    bias_val = err.get(bias_key)
                    if bias_val is not None:
                        sm["bias_sum"] += bias_val
                        sm["bias_count"] += 1

                # Update lead-time bucket
                lt_sm = source_metrics_by_lt[src][metric][ltg]
                lt_sm["weighted_sum"] += value * decay_weight
                lt_sm["weight_sum"] += decay_weight
                lt_sm["count"] += 1
                if bias_key:
                    bias_val = err.get(bias_key)
                    if bias_val is not None:
                        lt_sm["bias_sum"] += bias_val
                        lt_sm["bias_count"] += 1

    # Compute weighted MAE / match rate per source per metric
    skill_scores = {}  # {metric: {source: skill}}

    for metric in metrics:
        skill_scores[metric] = {}
        for source in all_sources:
            sm = source_metrics[source][metric]
            if sm["count"] < min_samples:
                # Cold start — will get equal weight later
                skill_scores[metric][source] = None
                continue

            weighted_value = sm["weighted_sum"] / sm["weight_sum"]

            if metric == "condition":
                # Match rate: higher is better, add epsilon
                skill = weighted_value + epsilon
            else:
                # MAE: lower is better, invert
                skill = 1.0 / (weighted_value + epsilon)

            skill_scores[metric][source] = skill

    # Normalize skills to weights
    weights = {}  # {source: {metric: weight}}
    for source in all_sources:
        weights[source] = {}

    for metric in metrics:
        skills = skill_scores[metric]
        non_null_skills = {s: v for s, v in skills.items() if v is not None}

        if not non_null_skills:
            # All sources in cold start — equal weights
            equal_w = 1.0 / len(all_sources) if all_sources else 0.0
            for source in all_sources:
                weights[source][metric] = equal_w
        else:
            total_skill = sum(non_null_skills.values())
            if total_skill == 0:
                equal_w = 1.0 / len(non_null_skills)
                for source in non_null_skills:
                    weights[source][metric] = equal_w
            else:
                for source in non_null_skills:
                    weights[source][metric] = non_null_skills[source] / total_skill

            # Sources still in cold start get average weight of scored sources
            cold_sources = [s for s in all_sources if s not in non_null_skills]
            if cold_sources:
                avg_weight = sum(weights[s].get(metric, 0) for s in non_null_skills) / len(non_null_skills)
                for source in cold_sources:
                    weights[source][metric] = avg_weight

                # Re-normalize all weights so they sum to 1.0
                total_w = sum(weights[s].get(metric, 0) for s in all_sources)
                if total_w > 0:
                    for source in all_sources:
                        weights[source][metric] = weights[source].get(metric, 0) / total_w

    # Ensure bias and lead_time_group columns exist in source_accuracy
    conn = db.get_connection()
    # Check existing columns
    cursor = conn.execute("PRAGMA table_info(source_accuracy)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    if "bias" not in existing_cols:
        conn.execute("ALTER TABLE source_accuracy ADD COLUMN bias REAL DEFAULT NULL")
    if "lead_time_group" not in existing_cols:
        conn.execute("ALTER TABLE source_accuracy ADD COLUMN lead_time_group TEXT DEFAULT NULL")
    conn.commit()

    # Store weights in database (overall)
    for source in all_sources:
        for metric in metrics:
            sm = source_metrics[source][metric]
            mae = None
            accuracy_pct = None
            bias = None

            if sm["count"] > 0 and sm["weight_sum"] > 0:
                weighted_value = sm["weighted_sum"] / sm["weight_sum"]
                if metric == "condition":
                    accuracy_pct = round(weighted_value * 100, 1)
                else:
                    mae = round(weighted_value, 3)

            if sm["bias_count"] > 0:
                bias = round(sm["bias_sum"] / sm["bias_count"], 3)

            db.upsert_accuracy(
                city_id=city_id,
                source_name=source,
                metric=metric,
                mae=mae,
                accuracy_pct=accuracy_pct,
                weight=round(weights[source].get(metric, 0.0), 6),
                sample_count=sm["count"],
                window_days=window_days,
                bias=bias,
            )

    # Store per-lead-time-group accuracy
    for source in all_sources:
        for metric in metrics:
            for ltg in lead_time_groups:
                lt_sm = source_metrics_by_lt[source][metric][ltg]
                if lt_sm["count"] == 0:
                    continue

                mae = None
                accuracy_pct = None
                bias = None

                if lt_sm["weight_sum"] > 0:
                    weighted_value = lt_sm["weighted_sum"] / lt_sm["weight_sum"]
                    if metric == "condition":
                        accuracy_pct = round(weighted_value * 100, 1)
                    else:
                        mae = round(weighted_value, 3)

                if lt_sm["bias_count"] > 0:
                    bias = round(lt_sm["bias_sum"] / lt_sm["bias_count"], 3)

                db.upsert_accuracy(
                    city_id=city_id,
                    source_name=source,
                    metric=f"{metric}:{ltg}",
                    mae=mae,
                    accuracy_pct=accuracy_pct,
                    weight=round(weights[source].get(metric, 0.0), 6),
                    sample_count=lt_sm["count"],
                    window_days=window_days,
                    bias=bias,
                    lead_time_group=ltg,
                )

    return weights


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Verify forecasts and compute source weights")
    parser.add_argument("--city", help="Process a single city only")
    parser.add_argument("--date", help="Verify a specific date (YYYY-MM-DD). Default: yesterday.")
    parser.add_argument("--backfill", type=int, help="Backfill observations for the last N days")
    args = parser.parse_args()

    cities = db.get_all_cities()
    if args.city:
        cities = [c for c in cities if c["name"].lower() == args.city.lower()]
        if not cities:
            print(f"City not found: {args.city}")
            sys.exit(1)

    # Determine dates to verify
    if args.backfill:
        dates_to_verify = [
            (date.today() - timedelta(days=i)).isoformat()
            for i in range(2, 2 + args.backfill)  # Start from 2 days ago (archive lag)
        ]
    elif args.date:
        dates_to_verify = [args.date]
    else:
        # Default: try yesterday, fallback to day before
        dates_to_verify = [
            (date.today() - timedelta(days=1)).isoformat(),
            (date.today() - timedelta(days=2)).isoformat(),
        ]

    print("=== Weather Verification & Scoring ===\n")

    for city in cities:
        print(f"{city['name']}, {city['country']}:")
        obs_stored = 0

        for target_date in dates_to_verify:
            existing = db.get_observation(city["id"], target_date)
            if existing:
                print(f"  [{target_date}] Observation already stored")
                obs_stored += 1
                continue

            obs = fetch_observation(city["lat"], city["lon"], target_date, city.get("timezone", "auto"))
            if obs and obs["temp_high_c"] is not None:
                db.insert_observation(
                    city_id=city["id"],
                    obs_date=target_date,
                    temp_high_c=obs["temp_high_c"],
                    temp_low_c=obs["temp_low_c"],
                    precip_mm=obs["precip_mm"],
                    wind_max_kmh=obs["wind_max_kmh"],
                    condition_text=obs["condition_text"],
                    pressure_hpa=obs["pressure_hpa"],
                    humidity_pct=obs["humidity_pct"],
                )
                print(f"  [{target_date}] Observation: {obs['temp_high_c']}C/{obs['temp_low_c']}C, "
                      f"precip={obs['precip_mm']}mm, pressure={obs['pressure_hpa']}hPa")
                obs_stored += 1
            else:
                print(f"  [{target_date}] No observation data available (archive lag?)")

        # Recompute weights
        weights = recompute_weights(city["id"])
        if weights:
            print(f"  Weights recomputed for {len(weights)} sources")
            for source, metrics in weights.items():
                parts = [f"{m}={w:.3f}" for m, w in metrics.items()]
                print(f"    {source}: {', '.join(parts)}")
        else:
            print(f"  No weight data yet (need more observations)")

        print()

    # Summary
    conn = db.get_connection()
    obs_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    acc_count = conn.execute("SELECT COUNT(*) FROM source_accuracy").fetchone()[0]
    conn.close()
    print(f"--- Summary ---")
    print(f"Total observations: {obs_count}")
    print(f"Accuracy entries: {acc_count}")


if __name__ == "__main__":
    main()
