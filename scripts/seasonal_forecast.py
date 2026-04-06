#!/usr/bin/env python3
"""
Seasonal Weather Forecast — Main Entry Point.

Produces seasonal outlooks (1-12 months ahead) by combining teleconnection
index analysis with multiple statistical/dynamical prediction methods.

Usage:
  python scripts/seasonal_forecast.py Bratislava              # 3-month default
  python scripts/seasonal_forecast.py "New York" --months 6   # 6-month outlook
  python scripts/seasonal_forecast.py London --months 12      # Full year
  python scripts/seasonal_forecast.py Bratislava --json       # JSON output
  python scripts/seasonal_forecast.py --update-indices        # Force refresh
  python scripts/seasonal_forecast.py --build-climatology Bratislava
"""

import argparse
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from climate_indices import (
    fetch_all_indices,
    build_climatology,
    get_current_index_state,
)
from seasonal_model import run_seasonal_forecast

# Month names for display
MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
SEASON_MAP = {
    12: "DJF", 1: "DJF", 2: "DJF",
    3: "MAM", 4: "MAM", 5: "MAM",
    6: "JJA", 7: "JJA", 8: "JJA",
    9: "SON", 10: "SON", 11: "SON",
}


# ===================================================================
# TELECONNECTION NARRATIVE GENERATOR
# ===================================================================
# Rules: (index, phase, lat_range, season, narrative)
# lat_range: (min_lat, max_lat) or None for any
# season: "DJF", "JJA", "MAM", "SON", or None for any
TELECONNECTION_RULES = [
    # ENSO — Global
    ("oni", "strong_el_nino", None, "DJF",
     "Strong El Nino: significantly warmer northern winters expected, drier Mediterranean"),
    ("oni", "moderate_el_nino", None, "DJF",
     "Moderate El Nino: milder-than-normal winter conditions in northern latitudes"),
    ("oni", "weak_el_nino", None, "DJF",
     "Weak El Nino: slightly warmer winter signal, modest drying in tropical regions"),
    ("oni", "strong_el_nino", None, "JJA",
     "Strong El Nino: suppressed Atlantic hurricane activity, warmer tropical Pacific"),
    ("oni", "moderate_el_nino", None, "JJA",
     "Moderate El Nino: reduced monsoon rainfall likely in South/Southeast Asia"),
    ("oni", "strong_la_nina", None, "DJF",
     "Strong La Nina: colder-than-normal northern winters, wetter in Australia/SE Asia"),
    ("oni", "moderate_la_nina", None, "DJF",
     "Moderate La Nina: enhanced cold outbreaks in northern mid-latitudes"),
    ("oni", "weak_la_nina", None, "DJF",
     "Weak La Nina: slightly cooler winter conditions, enhanced tropical convection"),
    ("oni", "strong_la_nina", None, "JJA",
     "Strong La Nina: above-normal Atlantic hurricane season, enhanced monsoons"),

    # NAO — Europe & Eastern North America
    ("nao", "positive", (40, 70), "DJF",
     "Positive NAO: strong westerlies bringing mild wet winters to northern Europe, drier Mediterranean"),
    ("nao", "negative", (40, 70), "DJF",
     "Negative NAO: weak westerlies, cold continental blocking pattern over Europe"),
    ("nao", "negative", (30, 45), "DJF",
     "Negative NAO: wetter-than-normal conditions in the Mediterranean region"),
    ("nao", "positive", (40, 70), "JJA",
     "Positive NAO: warmer drier summers in northern Europe"),

    # AO — Northern Hemisphere
    ("ao", "negative", (30, 70), "DJF",
     "Negative Arctic Oscillation: weakened polar vortex increases risk of cold air outbreaks"),
    ("ao", "positive", (30, 70), "DJF",
     "Positive Arctic Oscillation: strong polar vortex confines cold air to Arctic, milder mid-latitudes"),

    # PDO — Modulates ENSO
    ("pdo", "positive", None, None,
     "Positive PDO phase amplifies current El Nino signal and Pacific warmth"),
    ("pdo", "negative", None, None,
     "Negative PDO phase amplifies La Nina effects on global circulation"),

    # AMO — Atlantic
    ("amo", "warm", None, "JJA",
     "Warm AMO phase: above-average Atlantic hurricane activity, warmer European summers"),
    ("amo", "cool", None, "JJA",
     "Cool AMO phase: reduced Atlantic hurricane activity, cooler European summers"),

    # EA — European supplement
    ("ea", "positive", (35, 65), "DJF",
     "Positive East Atlantic pattern: milder European winters independent of NAO"),
    ("ea", "negative", (35, 65), "DJF",
     "Negative East Atlantic pattern: colder European winters, enhanced blocking"),

    # Scandinavian Pattern
    ("scand", "positive", (50, 70), "DJF",
     "Positive Scandinavian pattern: cold dry winters over Scandinavia, blocking pattern"),
    ("scand", "negative", (50, 70), "DJF",
     "Negative Scandinavian pattern: milder wetter Scandinavian winters"),

    # IOD — Indian Ocean / Australia
    ("dmi", "positive", (-40, 10), None,
     "Positive IOD: drought risk in Australia/Indonesia, enhanced East African rainfall"),
    ("dmi", "negative", (-40, 10), None,
     "Negative IOD: wetter conditions in Australia, drier East Africa"),

    # QBO — Stratospheric
    ("qbo", "easterly", (30, 70), "DJF",
     "Easterly QBO phase: weakens polar vortex, increases cold outbreak probability"),
    ("qbo", "westerly", (30, 70), "DJF",
     "Westerly QBO phase: strengthens polar vortex, supports milder mid-latitude winters"),

    # PNA — North America
    ("pna", "positive", (30, 60), "DJF",
     "Positive PNA: amplified western ridge (warm/dry) and eastern trough (cold/stormy) over North America"),
    ("pna", "negative", (30, 60), "DJF",
     "Negative PNA: below-normal temperatures in western North America"),

    # AAO/SAM — Southern Hemisphere
    ("aao", "positive", (-70, -30), None,
     "Positive SAM: stronger Southern Hemisphere westerlies, drier southern Australia"),
    ("aao", "negative", (-70, -30), None,
     "Negative SAM: weaker westerlies, wetter southern Australia and New Zealand"),
]


def generate_narrative(current_state, city_lat, target_season):
    """Match applicable teleconnection rules and generate narrative text.

    Args:
        current_state: dict from get_current_index_state()
        city_lat: city latitude
        target_season: "DJF", "MAM", "JJA", "SON"

    Returns: list of narrative strings
    """
    narratives = []

    for index_name, phase, lat_range, season, text in TELECONNECTION_RULES:
        info = current_state.get(index_name)
        if not info:
            continue

        # Check phase match
        current_phase = info.get("phase", "")
        if current_phase != phase:
            continue

        # Check latitude range
        if lat_range:
            min_lat, max_lat = lat_range
            if not (min_lat <= city_lat <= max_lat):
                continue

        # Check season
        if season and season != target_season:
            continue

        narratives.append(text)

    return narratives


# ===================================================================
# OUTPUT FORMATTERS
# ===================================================================
def format_text_output(city, forecasts, index_state, months_ahead):
    """Format seasonal forecast as human-readable text."""
    now = datetime.utcnow()
    current_month = now.month
    current_year = now.year

    # Determine forecast range
    first_month = current_month + 1
    first_year = current_year
    if first_month > 12:
        first_month = 1
        first_year += 1

    last_month = current_month + months_ahead
    last_year = current_year
    while last_month > 12:
        last_month -= 12
        last_year += 1

    # Dominant signals
    dominant = []
    oni = index_state.get("oni")
    if oni and oni["phase"] != "neutral":
        label = oni["phase"].replace("_", " ").title()
        dominant.append(f"{label} (ONI: {oni['value']:+.1f})")
    nao = index_state.get("nao")
    if nao and nao["phase"] != "neutral":
        dominant.append(f"{'Positive' if nao['phase'] == 'positive' else 'Negative'} NAO ({nao['value']:+.1f})")
    ao = index_state.get("ao")
    if ao and ao["phase"] != "neutral":
        dominant.append(f"{'Positive' if ao['phase'] == 'positive' else 'Negative'} AO ({ao['value']:+.1f})")

    lines = []
    lines.append("=" * 65)
    lines.append(f"  SEASONAL OUTLOOK -- {city['name'].upper()}, {(city.get('country') or '').upper()}")
    lines.append(f"  Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"  Lead: {months_ahead} months ({MONTH_NAMES[first_month]} {first_year}"
                 f" - {MONTH_NAMES[last_month]} {last_year})")
    if dominant:
        lines.append(f"  Dominant signals: {', '.join(dominant)}")
    lines.append("=" * 65)

    # Teleconnection state
    lines.append("")
    lines.append("--- TELECONNECTION STATE ---")
    tier1 = ["oni", "nao", "ao", "pdo", "amo", "pna", "soi"]
    for idx_name in tier1:
        info = index_state.get(idx_name)
        if info:
            phase = info.get("phase", "").replace("_", " ").title()
            lines.append(f"  {idx_name.upper():8s}  {info['value']:+7.2f}  ({phase})")

    # Key drivers narrative
    target_season = SEASON_MAP.get(first_month, "DJF")
    narratives = generate_narrative(index_state, city["lat"], target_season)
    if narratives:
        lines.append("")
        lines.append("--- KEY DRIVERS ---")
        for n in narratives:
            lines.append(f"  * {n}")

    # Monthly outlook table
    lines.append("")
    lines.append("--- MONTHLY OUTLOOK ---")
    lines.append(f"  {'Month':12s}  {'Temp Anomaly':>13s}  {'Precip Anomaly':>15s}  {'Confidence':>11s}")
    lines.append(f"  {'-' * 12}  {'-' * 13}  {'-' * 15}  {'-' * 11}")

    for fc in forecasts:
        m = fc["target_month"]
        y = fc["target_year"]
        temp = fc.get("temp_anomaly_c")
        precip = fc.get("precip_anomaly_pct")
        conf = fc.get("confidence")

        temp_str = f"{temp:+.1f}C" if temp is not None else "N/A"
        precip_str = f"{precip:+.0f}%" if precip is not None else "N/A"

        if conf is not None:
            if conf >= 0.6:
                conf_str = "HIGH"
            elif conf >= 0.3:
                conf_str = "MODERATE"
            else:
                conf_str = "LOW"
        else:
            conf_str = "N/A"

        lines.append(f"  {MONTH_NAMES[m]} {y:4d}      {temp_str:>8s}         {precip_str:>8s}     {conf_str:>8s}")

    # Tercile probabilities
    lines.append("")
    lines.append("--- TERCILE PROBABILITIES ---")
    lines.append(f"  {'Month':12s}  {'Below':>7s}  {'Near':>7s}  {'Above':>7s}")
    lines.append(f"  {'-' * 12}  {'-' * 7}  {'-' * 7}  {'-' * 7}")

    for fc in forecasts:
        m = fc["target_month"]
        y = fc["target_year"]
        tp = fc.get("tercile_probs", {})
        bn = tp.get("below_normal", 0.333)
        nn = tp.get("near_normal", 0.334)
        an = tp.get("above_normal", 0.333)
        lines.append(f"  {MONTH_NAMES[m]} {y:4d}       {bn*100:4.0f}%    {nn*100:4.0f}%    {an*100:4.0f}%")

    # Method breakdown
    if forecasts and forecasts[0].get("method_weights"):
        lines.append("")
        lines.append("--- METHOD WEIGHTS ---")
        for method, weight in forecasts[0]["method_weights"].items():
            lines.append(f"  {method:15s}  {weight*100:5.1f}%")

    # Analog years
    if forecasts and forecasts[0].get("individual_methods"):
        for im in forecasts[0]["individual_methods"]:
            if im.get("method") == "analog" and im.get("analogs"):
                lines.append("")
                lines.append("--- ANALOG YEARS (closest matches) ---")
                for a in im["analogs"]:
                    lines.append(f"  {a['year']}  (distance: {a['distance']:.3f})")
                break

    lines.append("")
    lines.append("=" * 65)

    return "\n".join(lines)


def format_json_output(city, forecasts, index_state, months_ahead):
    """Format as JSON for API/programmatic use."""
    return {
        "city": {
            "id": city["id"],
            "name": city["name"],
            "country": city.get("country"),
            "lat": city["lat"],
            "lon": city["lon"],
        },
        "generated_at": datetime.utcnow().isoformat(),
        "months_ahead": months_ahead,
        "index_state": index_state,
        "monthly_forecasts": forecasts,
    }


# ===================================================================
# MAIN FORECAST PIPELINE
# ===================================================================
def produce_seasonal_forecast(city_name, months_ahead=3, force_update=False):
    """Full seasonal forecast pipeline.

    Returns: (city_dict, list_of_monthly_forecasts, index_state)
    """
    from fetch_weather import geocode

    # 1. Resolve city
    city = db.get_city(city_name)
    if not city:
        loc = geocode(city_name)
        if not loc:
            print(f"Could not find city: {city_name}")
            return None, None, None
        city_id = db.insert_city(
            loc["name"], loc["country"], loc["lat"], loc["lon"],
            loc.get("timezone", "auto"))
        city = db.get_city_by_id(city_id)

    # 2. Auto-update indices (respects cache)
    fetch_all_indices(force=force_update)

    # 3. Ensure climatology exists
    if not db.has_climatology(city["id"]):
        print(f"Building climatology for {city['name']} (first-time setup, may take a few minutes)...")
        build_climatology(city["id"], city["lat"], city["lon"])

    climatology = db.get_climatology(city["id"])

    # 4. Get current index state
    index_state = get_current_index_state()

    # 5. Pre-load index series for efficiency
    cfg = db.load_config().get("seasonal", {})
    all_indices = set()
    all_indices.update(cfg.get("analog", {}).get("predictor_indices", []))
    all_indices.update(cfg.get("regression", {}).get("predictor_indices", []))
    all_indices.add("oni")
    all_indices.add("nao")

    index_series_cache = {}
    for idx_name in all_indices:
        index_series_cache[idx_name] = db.get_climate_index_series(idx_name)

    # 6. Run forecasts for each target month
    now = datetime.utcnow()
    monthly_forecasts = []

    for lead in range(1, months_ahead + 1):
        target_month = now.month + lead
        target_year = now.year
        while target_month > 12:
            target_month -= 12
            target_year += 1

        print(f"  Forecasting {MONTH_NAMES[target_month]} {target_year} (lead: {lead} months)...")

        result = run_seasonal_forecast(
            city["id"], city["lat"], city["lon"],
            target_year, target_month, lead,
            climatology, index_series_cache)

        result["target_month"] = target_month
        result["target_year"] = target_year
        monthly_forecasts.append(result)

    return city, monthly_forecasts, index_state


# ===================================================================
# CLI
# ===================================================================
def main():
    parser = argparse.ArgumentParser(description="Seasonal Weather Forecast")
    parser.add_argument("city", nargs="?", help="City name")
    parser.add_argument("--months", type=int, default=3,
                        help="Months ahead (1-12, default: 3)")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON")
    parser.add_argument("--update-indices", action="store_true",
                        help="Force refresh climate indices")
    parser.add_argument("--build-climatology", metavar="CITY",
                        help="Build climatology for a city")
    args = parser.parse_args()

    if args.update_indices:
        fetch_all_indices(force=True)
        return

    if args.build_climatology:
        from fetch_weather import geocode
        city = db.get_city(args.build_climatology)
        if not city:
            loc = geocode(args.build_climatology)
            if not loc:
                print(f"City not found: {args.build_climatology}")
                return
            city_id = db.insert_city(
                loc["name"], loc["country"], loc["lat"], loc["lon"],
                loc.get("timezone", "auto"))
            city = db.get_city_by_id(city_id)
        build_climatology(city["id"], city["lat"], city["lon"])
        return

    if not args.city:
        parser.print_help()
        return

    months = max(1, min(12, args.months))
    city, forecasts, index_state = produce_seasonal_forecast(
        args.city, months, force_update=False)

    if not city:
        return

    if args.json:
        output = format_json_output(city, forecasts, index_state, months)
        print(json.dumps(output, indent=2, default=str))
    else:
        print(format_text_output(city, forecasts, index_state, months))


if __name__ == "__main__":
    main()
