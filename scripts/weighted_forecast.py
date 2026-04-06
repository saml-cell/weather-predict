#!/usr/bin/env python3
"""
Weighted Forecast Engine — Layer 1 + Layer 2 combined output.

Produces a weather forecast that:
1. Weights each API source by its historical accuracy (Layer 1)
2. Applies meteorological physics corrections (Layer 2)
3. Shows full transparency: weights, adjustments, confidence

Usage:
  python scripts/weighted_forecast.py Bratislava
  python scripts/weighted_forecast.py "New York" --json
  python scripts/weighted_forecast.py London --compact
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from db import normalize_condition
from fetch_weather import (
    geocode, fetch_open_meteo, fetch_wttr, fetch_openweather, fetch_weatherapi, WMO_CODES, c_to_f
)
from meteo import apply_physics_corrections, dew_point, dew_point_depression, feels_like


# ---------------------------------------------------------------------------
# Weighted math
# ---------------------------------------------------------------------------
def weighted_average(values_and_weights):
    """Compute weighted average from list of (value, weight) tuples.
    Skips None values. Returns None if no valid data."""
    valid = [(v, w) for v, w in values_and_weights if v is not None and w is not None]
    if not valid:
        return None
    total_weight = sum(w for _, w in valid)
    if total_weight == 0:
        return sum(v for v, _ in valid) / len(valid)
    return round(sum(v * w for v, w in valid) / total_weight, 1)


def weighted_vote(conditions_and_weights):
    """Weighted majority vote for categorical conditions.
    Returns the condition with the highest total weight."""
    if not conditions_and_weights:
        return "unknown"
    votes = defaultdict(float)
    for cond, weight in conditions_and_weights:
        if cond and weight is not None:
            votes[normalize_condition(cond)] += weight
    if not votes:
        return "unknown"
    return max(votes, key=votes.get)


def compute_confidence(spread, weight_data, num_sources):
    """Compute forecast confidence from agreement + historical accuracy.

    Returns: (label, score) where label is HIGH/MODERATE/LOW and score is 0-1.
    """
    # Spread score: small spread = high agreement
    spread_score = max(0, 1.0 - (spread / 10.0))

    # Accuracy score: average weight concentration (how much the best source dominates)
    if weight_data:
        # If weights are very uneven, we trust the top source more -> higher confidence
        max_weight = max(weight_data.values()) if weight_data else 0.5
        accuracy_score = max_weight
    else:
        accuracy_score = 0.5  # cold start

    # Source count bonus (coefficients sum to exactly 1.0: 0.50+0.35+0.10+0.05)
    source_bonus = min(num_sources / 4.0, 1.0)

    score = (0.50 * spread_score
             + 0.35 * accuracy_score
             + 0.10 * (1.0 if num_sources >= 2 else 0.5)
             + 0.05 * source_bonus)

    score = min(score, 1.0)

    if score >= 0.75:
        label = "HIGH"
    elif score >= 0.50:
        label = "MODERATE"
    else:
        label = "LOW"

    return label, round(score, 3)


# ---------------------------------------------------------------------------
# Fetch fresh data from all sources
# ---------------------------------------------------------------------------
def fetch_all_sources(city):
    """Fetch from all available sources in parallel."""
    lat, lon = city["lat"], city["lon"]
    tz = city.get("timezone", "auto")
    name = city["name"]

    results = []
    source_status = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(fetch_open_meteo, lat, lon, tz): "Open-Meteo",
            pool.submit(fetch_wttr, name): "wttr.in",
            pool.submit(fetch_openweather, lat, lon): "OpenWeatherMap",
            pool.submit(fetch_weatherapi, lat, lon): "WeatherAPI",
        }
        for future in as_completed(futures):
            source = futures[future]
            try:
                data = future.result()
                if data:
                    results.append(data)
                    source_status[source] = "OK"
                else:
                    source_status[source] = "no data/no key"
            except Exception as e:
                source_status[source] = f"error: {e}"

    return results, source_status


# ---------------------------------------------------------------------------
# Build weighted forecast
# ---------------------------------------------------------------------------
def produce_forecast(city_name):
    """Main function: produce a weighted ensemble forecast with physics corrections.

    Returns a comprehensive forecast dict.
    """
    # Resolve city
    city = db.get_city(city_name)
    if not city:
        location = geocode(city_name)
        if not location:
            return {"error": f"Could not find city: {city_name}"}
        city = {
            "id": None, "name": location["name"], "country": location["country"],
            "lat": location["lat"], "lon": location["lon"],
            "timezone": location.get("timezone", "auto")
        }

    # Load weights from database
    weights = db.get_weights(city["id"]) if city.get("id") else {}

    # Fetch fresh data
    results, source_status = fetch_all_sources(city)
    if not results:
        return {"error": "No weather data from any source", "source_status": source_status}

    sources_used = [r["source"] for r in results]
    num_sources = len(results)

    # Default equal weights for sources not in the database
    default_weight = 1.0 / num_sources

    def get_weight(source_name, metric):
        """Get weight for a source/metric, falling back to equal weight."""
        if source_name in weights and metric in weights[source_name]:
            return weights[source_name][metric]["weight"]
        return default_weight

    # --- Current conditions (weighted) ---
    temps = [(r["current"]["temp_c"], get_weight(r["source"], "temp_high")) for r in results]
    feels = [(r["current"]["feels_like_c"], get_weight(r["source"], "temp_high")) for r in results]
    humids = [(r["current"].get("humidity"), 1.0) for r in results]  # equal weight for auxiliary
    winds = [(r["current"]["wind_speed_kmh"], get_weight(r["source"], "wind")) for r in results]
    pressures = [(r["current"].get("pressure_hpa"), 1.0) for r in results]

    avg_temp = weighted_average(temps)
    avg_humidity = weighted_average(humids)
    avg_wind = weighted_average(winds)
    avg_pressure = weighted_average(pressures)

    temp_values = [r["current"]["temp_c"] for r in results if r["current"]["temp_c"] is not None]
    temp_spread = (max(temp_values) - min(temp_values)) if len(temp_values) > 1 else 0

    current = {
        "temp_c": avg_temp,
        "temp_f": c_to_f(avg_temp),
        "feels_like_c": weighted_average(feels),
        "humidity_pct": avg_humidity,
        "wind_speed_kmh": avg_wind,
        "pressure_hpa": avg_pressure,
        "conditions": [r["current"]["condition"] for r in results if r["current"].get("condition")],
        "temp_spread_c": round(temp_spread, 1),
    }

    # Dew point
    if avg_temp is not None and avg_humidity is not None:
        current["dew_point_c"] = dew_point(avg_temp, avg_humidity)
        current["dew_point_depression_c"] = dew_point_depression(avg_temp, avg_humidity)

    # Feels-like with physics
    if avg_temp is not None:
        current["feels_like_c"] = feels_like(avg_temp, avg_humidity, avg_wind)
        current["feels_like_f"] = c_to_f(current["feels_like_c"])

    # Confidence
    temp_high_weights = {r["source"]: get_weight(r["source"], "temp_high") for r in results}
    confidence_label, confidence_score = compute_confidence(temp_spread, temp_high_weights, num_sources)
    current["confidence"] = confidence_label
    current["confidence_score"] = confidence_score

    # --- Daily forecast (weighted) ---
    all_dates = set()
    for r in results:
        for d in r.get("daily", []):
            if d.get("date"):
                all_dates.add(d["date"])

    daily_forecast = []
    for forecast_date in sorted(all_dates)[:7]:
        day_data = {"date": forecast_date, "per_source": {}}

        highs, lows, probs, precips, winds_d, conditions = [], [], [], [], [], []

        for r in results:
            for d in r.get("daily", []):
                if d.get("date") == forecast_date:
                    source = r["source"]
                    day_data["per_source"][source] = d

                    w_temp = get_weight(source, "temp_high")
                    w_precip = get_weight(source, "precip_mm")
                    w_wind = get_weight(source, "wind")
                    w_cond = get_weight(source, "condition")

                    if d.get("high_c") is not None:
                        highs.append((d["high_c"], w_temp))
                    if d.get("low_c") is not None:
                        lows.append((d["low_c"], w_temp))
                    if d.get("precip_prob") is not None:
                        probs.append((d["precip_prob"], w_precip))
                    if d.get("precip_mm") is not None:
                        precips.append((d["precip_mm"], w_precip))
                    if d.get("wind_max_kmh") is not None:
                        winds_d.append((d["wind_max_kmh"], w_wind))
                    if d.get("condition"):
                        conditions.append((d["condition"], w_cond))

        day_data["weighted_high_c"] = weighted_average(highs)
        day_data["weighted_low_c"] = weighted_average(lows)
        day_data["weighted_precip_prob"] = weighted_average(probs)
        day_data["weighted_precip_mm"] = weighted_average(precips)
        day_data["weighted_wind_kmh"] = weighted_average(winds_d)
        day_data["weighted_condition"] = weighted_vote(conditions)

        # Spread for this day
        high_vals = [v for v, _ in highs]
        day_data["high_spread_c"] = round(max(high_vals) - min(high_vals), 1) if len(high_vals) > 1 else 0

        # Apply Layer 2 physics corrections to daily
        day_forecast = {
            "precip_prob": day_data["weighted_precip_prob"],
            "precip_mm": day_data["weighted_precip_mm"],
        }
        adjusted, corrections = apply_physics_corrections(
            day_forecast,
            pressure_hpa=avg_pressure,
            humidity_pct=avg_humidity,
            temp_c=day_data["weighted_high_c"],
            wind_kmh=day_data["weighted_wind_kmh"],
        )
        day_data["adjusted_precip_prob"] = adjusted.get("precip_prob", day_data["weighted_precip_prob"])
        day_data["adjusted_precip_mm"] = adjusted.get("precip_mm", day_data["weighted_precip_mm"])
        day_data["physics_corrections"] = corrections

        # Sunrise / sunset (take from first source that has them, prefer Open-Meteo)
        for r in results:
            for d in r.get("daily", []):
                if d.get("date") == forecast_date:
                    if d.get("sunrise") and "sunrise" not in day_data:
                        day_data["sunrise"] = d["sunrise"]
                    if d.get("sunset") and "sunset" not in day_data:
                        day_data["sunset"] = d["sunset"]
                    if d.get("wind_dir_deg") is not None and "wind_dir_deg" not in day_data:
                        day_data["wind_dir_deg"] = d["wind_dir_deg"]

        daily_forecast.append(day_data)

    # --- Source accuracy info for transparency ---
    source_info = {}
    for source in sources_used:
        if source in weights:
            source_info[source] = {
                metric: {
                    "weight": round(data["weight"], 3),
                    "mae": data.get("mae"),
                    "accuracy_pct": data.get("accuracy_pct"),
                    "samples": data.get("sample_count", 0),
                }
                for metric, data in weights[source].items()
            }
        else:
            source_info[source] = {"status": "cold start (equal weights)"}

    # --- Hourly forecast (from Open-Meteo if available) ---
    hourly_forecast = []
    for r in results:
        if r["source"] == "Open-Meteo" and r.get("hourly"):
            hourly_forecast = r["hourly"]
            break

    return {
        "location": {
            "name": city["name"],
            "country": city.get("country", ""),
            "lat": city["lat"],
            "lon": city["lon"],
        },
        "current": current,
        "daily": daily_forecast,
        "hourly": hourly_forecast,
        "sources_used": sources_used,
        "source_status": source_status,
        "source_accuracy": source_info,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------
def format_text(forecast):
    """Human-readable weighted forecast output."""
    loc = forecast["location"]
    cur = forecast["current"]
    lines = []

    lines.append(f"{'='*65}")
    lines.append(f"  WEIGHTED WEATHER FORECAST — {loc['name'].upper()}, {loc['country'].upper()}")
    lines.append(f"  Sources: {', '.join(forecast['sources_used'])} ({len(forecast['sources_used'])} sources)")
    lines.append(f"  Confidence: {cur['confidence']} (score: {cur['confidence_score']})")
    lines.append(f"  Generated: {forecast['generated_at'][:19]}")
    lines.append(f"{'='*65}")

    lines.append(f"\n--- CURRENT CONDITIONS ---")
    lines.append(f"  Temperature:    {cur['temp_c']}C / {cur['temp_f']}F (spread: {cur['temp_spread_c']}C)")
    lines.append(f"  Feels like:     {cur.get('feels_like_c')}C / {cur.get('feels_like_f')}F")
    lines.append(f"  Humidity:       {cur.get('humidity_pct')}%")
    lines.append(f"  Pressure:       {cur.get('pressure_hpa')} hPa")
    lines.append(f"  Wind:           {cur.get('wind_speed_kmh')} km/h")

    if cur.get("dew_point_c") is not None:
        lines.append(f"  Dew point:      {cur['dew_point_c']}C (depression: {cur.get('dew_point_depression_c')}C)")

    lines.append(f"  Conditions:     {', '.join(set(cur.get('conditions', [])))}")

    lines.append(f"\n--- 7-DAY WEIGHTED FORECAST ---")
    lines.append(f"  {'Date':<12} {'High':>6} {'Low':>6} {'Rain%':>6} {'Adj%':>6} {'Wind':>7} {'WDir':>5}  Condition")
    lines.append(f"  {'-'*70}")

    for day in forecast.get("daily", []):
        h = f"{day['weighted_high_c']}C" if day['weighted_high_c'] is not None else "  - "
        l = f"{day['weighted_low_c']}C" if day['weighted_low_c'] is not None else "  - "
        p = f"{day['weighted_precip_prob']:.0f}%" if day['weighted_precip_prob'] is not None else "  - "
        ap = f"{day['adjusted_precip_prob']:.0f}%" if day['adjusted_precip_prob'] is not None else "  - "
        w = f"{day['weighted_wind_kmh']}km" if day['weighted_wind_kmh'] is not None else "  - "
        wd = f"{day['wind_dir_deg']}°" if day.get('wind_dir_deg') is not None else "  - "
        c = day.get("weighted_condition", "-")
        spread = f" [{day['high_spread_c']}C]" if day['high_spread_c'] > 3 else ""

        lines.append(f"  {day['date']:<12} {h:>6} {l:>6} {p:>6} {ap:>6} {w:>7} {wd:>5}  {c}{spread}")

        # Show sunrise/sunset if available
        if day.get("sunrise") or day.get("sunset"):
            sr = day.get("sunrise", "?")
            ss = day.get("sunset", "?")
            # Extract just the time portion if it's a datetime string
            if isinstance(sr, str) and "T" in sr:
                sr = sr.split("T")[1][:5]
            if isinstance(ss, str) and "T" in ss:
                ss = ss.split("T")[1][:5]
            lines.append(f"    sunrise: {sr}  sunset: {ss}")

        # Show physics corrections if any
        for corr in day.get("physics_corrections", []):
            lines.append(f"    ^ {corr}")

    # Source accuracy transparency
    lines.append(f"\n--- SOURCE ACCURACY ---")
    for source, info in forecast.get("source_accuracy", {}).items():
        if isinstance(info, dict) and "status" in info:
            lines.append(f"  {source}: {info['status']}")
        else:
            parts = []
            for metric, data in info.items():
                if data.get("mae") is not None:
                    parts.append(f"{metric}: w={data['weight']:.3f} MAE={data['mae']:.2f} (n={data['samples']})")
                elif data.get("accuracy_pct") is not None:
                    parts.append(f"{metric}: w={data['weight']:.3f} acc={data['accuracy_pct']:.0f}% (n={data['samples']})")
            lines.append(f"  {source}:")
            for p in parts:
                lines.append(f"    {p}")

    lines.append(f"\n{'='*65}")
    return "\n".join(lines)


def format_compact(forecast):
    """Compact one-line briefing."""
    loc = forecast["location"]
    cur = forecast["current"]
    today = forecast["daily"][0] if forecast.get("daily") else {}

    conds = ", ".join(set(cur.get("conditions", [])))
    return (
        f"{loc['name']}: {cur['temp_c']}C ({conds}) | "
        f"High {today.get('weighted_high_c', '?')}C / Low {today.get('weighted_low_c', '?')}C | "
        f"Rain: {today.get('adjusted_precip_prob', '?')}% | "
        f"Confidence: {cur['confidence']} | "
        f"{len(forecast['sources_used'])} sources"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Weighted ensemble weather forecast")
    parser.add_argument("city", help="City name")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("--compact", action="store_true", help="One-line briefing")
    args = parser.parse_args()

    forecast = produce_forecast(args.city)

    if "error" in forecast:
        print(f"ERROR: {forecast['error']}")
        sys.exit(1)

    if args.json:
        print(json.dumps(forecast, indent=2, default=str))
    elif args.compact:
        print(format_compact(forecast))
    else:
        print(format_text(forecast))


if __name__ == "__main__":
    main()
