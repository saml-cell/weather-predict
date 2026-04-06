#!/usr/bin/env python3
"""
Climate Teleconnection Index Fetcher and Climatology Builder.

Fetches ~20 teleconnection indices from NOAA PSL/CPC, parses them,
and stores in the climate_indices DB table. Also builds 30-year
climate normals (climatology) per city using Open-Meteo Archive API.

Usage:
  python scripts/climate_indices.py                     # Fetch all indices
  python scripts/climate_indices.py --climatology Bratislava  # Build normals
  python scripts/climate_indices.py --status             # Show current state
"""

import argparse
import json
import math
import os
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db

# ---------------------------------------------------------------------------
# NOAA PSL format parser
# ---------------------------------------------------------------------------
def parse_psl_format(text):
    """Parse NOAA PSL monthly index format.
    Format: first line has start_year end_year, then rows of year + 12 monthly values.
    Missing data: -99.90 or -99.99.
    Returns: list of (year, month, value) tuples.
    """
    results = []
    lines = text.strip().split("\n")
    for line in lines:
        parts = line.split()
        if len(parts) < 13:
            continue
        try:
            year = int(parts[0])
        except (ValueError, IndexError):
            continue
        if year < 1800 or year > 2100:
            continue
        for month_idx in range(12):
            try:
                val = float(parts[month_idx + 1])
            except (ValueError, IndexError):
                continue
            if val > -99.0:
                results.append((year, month_idx + 1, val))
    return results


def parse_cpc_qbo_format(text):
    """Parse CPC QBO index format.
    Similar to PSL but may have header lines and slightly different layout.
    """
    results = []
    lines = text.strip().split("\n")
    for line in lines:
        parts = line.split()
        if len(parts) < 13:
            continue
        try:
            year = int(parts[0])
        except (ValueError, IndexError):
            continue
        if year < 1800 or year > 2100:
            continue
        for month_idx in range(12):
            try:
                val = float(parts[month_idx + 1])
            except (ValueError, IndexError):
                continue
            if val > -999.0 and val < 900.0:
                results.append((year, month_idx + 1, val))
    return results


# ---------------------------------------------------------------------------
# Index fetching
# ---------------------------------------------------------------------------
def fetch_url(url, timeout=30):
    """Fetch text content from a URL."""
    req = urllib.request.Request(url, headers={"User-Agent": "WeatherPredictSystem/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_single_index(index_name, config):
    """Fetch and parse one index. Returns (index_name, count, error)."""
    url = config["url"]
    fmt = config.get("format", "psl")
    try:
        text = fetch_url(url)
        if fmt == "cpc_qbo":
            records = parse_cpc_qbo_format(text)
        else:
            records = parse_psl_format(text)

        if not records:
            return (index_name, 0, "No data parsed")

        # Batch insert
        rows = [(index_name, year, month, value) for year, month, value in records]
        db.insert_climate_indices_batch(rows)
        return (index_name, len(records), None)

    except Exception as e:
        return (index_name, 0, str(e))


def fetch_all_indices(force=False):
    """Fetch all configured indices. Respects cache_days unless force=True.
    Returns: list of (index_name, count, error).
    """
    cfg = db.load_config().get("seasonal", {})
    indices_cfg = cfg.get("indices", {})
    cache_days = cfg.get("cache_days", 7)

    if not force:
        last_fetch = db.get_indices_fetch_time()
        if last_fetch:
            try:
                last_dt = datetime.fromisoformat(last_fetch)
                age = datetime.utcnow() - last_dt
                if age < timedelta(days=cache_days):
                    print(f"Indices cached ({age.days}d old, cache={cache_days}d). Use --force to refresh.")
                    return []
            except (ValueError, TypeError):
                pass

    results = []
    print(f"Fetching {len(indices_cfg)} climate indices...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(fetch_single_index, name, icfg): name
            for name, icfg in indices_cfg.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            result = future.result()
            results.append(result)
            idx_name, count, err = result
            if err:
                print(f"  {idx_name}: ERROR - {err}")
            else:
                print(f"  {idx_name}: {count} values stored")

    return results


# ---------------------------------------------------------------------------
# Index state classification
# ---------------------------------------------------------------------------
ENSO_THRESHOLDS = {
    "strong_el_nino": 1.5,
    "moderate_el_nino": 1.0,
    "weak_el_nino": 0.5,
    "neutral": 0.0,
    "weak_la_nina": -0.5,
    "moderate_la_nina": -1.0,
    "strong_la_nina": -1.5,
}


def classify_enso(value):
    """Classify ENSO phase from ONI value."""
    if value is None:
        return "unknown", "unknown"
    if value >= 1.5:
        return "strong_el_nino", "strong"
    elif value >= 1.0:
        return "moderate_el_nino", "moderate"
    elif value >= 0.5:
        return "weak_el_nino", "weak"
    elif value > -0.5:
        return "neutral", "neutral"
    elif value > -1.0:
        return "weak_la_nina", "weak"
    elif value > -1.5:
        return "moderate_la_nina", "moderate"
    else:
        return "strong_la_nina", "strong"


def classify_binary(value, pos_threshold=0.5, neg_threshold=-0.5):
    """Classify a bipolar index as positive/neutral/negative."""
    if value is None:
        return "unknown"
    if value >= pos_threshold:
        return "positive"
    elif value <= neg_threshold:
        return "negative"
    return "neutral"


def get_current_index_state():
    """Return current state of all indices with phase classifications.
    Returns: dict of {index_name: {value, year, month, phase, strength}}.
    """
    latest = db.get_latest_climate_indices()
    state = {}

    for name, info in latest.items():
        val = info["value"]
        entry = {
            "value": round(val, 2),
            "year": info["year"],
            "month": info["month"],
        }

        if name == "oni":
            phase, strength = classify_enso(val)
            entry["phase"] = phase
            entry["strength"] = strength
        elif name in ("nao", "ao", "pna", "ea", "eawr", "scand", "wp",
                      "epnp", "tnh", "aao", "pdo"):
            entry["phase"] = classify_binary(val)
        elif name == "soi":
            # SOI is opposite to ONI (positive SOI = La Nina conditions)
            entry["phase"] = classify_binary(val)
        elif name == "dmi":
            entry["phase"] = classify_binary(val, 0.4, -0.4)
        elif name == "amo":
            entry["phase"] = "warm" if val > 0 else "cool"
        elif name == "qbo":
            entry["phase"] = "westerly" if val > 0 else "easterly"
        else:
            entry["phase"] = classify_binary(val)

        state[name] = entry

    return state


# ---------------------------------------------------------------------------
# Predictor vector builder
# ---------------------------------------------------------------------------
def build_predictor_vector(year, month, indices_list, lags, index_series_cache=None):
    """Build a feature vector of index values at specified lags.

    Args:
        year, month: Target period.
        indices_list: List of index names to include.
        lags: List of lag values in months (0 = current, 1 = one month prior).
        index_series_cache: Optional pre-loaded {index_name: {(y,m): val}} cache.

    Returns: (vector, labels) or (None, None) if too much data missing.
    """
    vector = []
    labels = []
    missing_count = 0
    total_count = 0

    for idx_name in indices_list:
        if index_series_cache and idx_name in index_series_cache:
            series = index_series_cache[idx_name]
        else:
            series = db.get_climate_index_series(idx_name)

        for lag in lags:
            total_count += 1
            # Compute lagged year/month
            lag_month = month - lag
            lag_year = year
            while lag_month <= 0:
                lag_month += 12
                lag_year -= 1

            val = series.get((lag_year, lag_month))
            if val is None:
                missing_count += 1
                vector.append(0.0)  # fill with 0 (neutral)
            else:
                vector.append(val)
            labels.append(f"{idx_name}_lag{lag}")

    # Reject if more than 30% missing
    if total_count > 0 and missing_count / total_count > 0.3:
        return None, None

    return vector, labels


# ---------------------------------------------------------------------------
# Climatology builder
# ---------------------------------------------------------------------------
def build_climatology(city_id, lat, lon, ref_start=None, ref_end=None):
    """Build 30-year climate normals using Open-Meteo Archive API.
    Fetches daily data year by year, aggregates to monthly stats.

    Args:
        city_id: Database city ID.
        lat, lon: Coordinates.
        ref_start, ref_end: Reference period years (default from config).
    """
    cfg = db.load_config().get("seasonal", {})
    if ref_start is None:
        ref_period = cfg.get("climatology_ref_period", [1991, 2020])
        ref_start, ref_end = ref_period[0], ref_period[1]

    print(f"Building climatology for city {city_id} ({ref_start}-{ref_end})...")

    # Collect monthly data: {month: {metric: [values]}}
    monthly_data = {m: {
        "temp_high": [], "temp_low": [], "precip": [], "wind": []
    } for m in range(1, 13)}

    for year in range(ref_start, ref_end + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={start_date}&end_date={end_date}"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
            f"&timezone=auto"
        )

        try:
            text = fetch_url(url, timeout=60)
            data = json.loads(text)
            daily = data.get("daily", {})

            dates = daily.get("time", [])
            t_max = daily.get("temperature_2m_max", [])
            t_min = daily.get("temperature_2m_min", [])
            precip = daily.get("precipitation_sum", [])
            wind = daily.get("wind_speed_10m_max", [])

            # Aggregate daily to monthly
            month_accum = {m: {"t_max": [], "t_min": [], "precip": 0.0, "wind": [],
                               "precip_days": 0} for m in range(1, 13)}

            for i, date_str in enumerate(dates):
                m = int(date_str[5:7])
                if i < len(t_max) and t_max[i] is not None:
                    month_accum[m]["t_max"].append(t_max[i])
                if i < len(t_min) and t_min[i] is not None:
                    month_accum[m]["t_min"].append(t_min[i])
                if i < len(precip) and precip[i] is not None:
                    month_accum[m]["precip"] += precip[i]
                if i < len(wind) and wind[i] is not None:
                    month_accum[m]["wind"].append(wind[i])

            for m in range(1, 13):
                acc = month_accum[m]
                if acc["t_max"]:
                    monthly_data[m]["temp_high"].append(
                        sum(acc["t_max"]) / len(acc["t_max"]))
                if acc["t_min"]:
                    monthly_data[m]["temp_low"].append(
                        sum(acc["t_min"]) / len(acc["t_min"]))
                monthly_data[m]["precip"].append(acc["precip"])
                if acc["wind"]:
                    monthly_data[m]["wind"].append(max(acc["wind"]))

            print(f"  {year}: OK")

        except Exception as e:
            print(f"  {year}: ERROR - {e}")

        # Rate limit: 1 req/sec
        time.sleep(1.0)

    # Compute stats and store
    for m in range(1, 13):
        md = monthly_data[m]
        stats = {"sample_years": len(md["temp_high"])}

        for key, metric in [("temp_high", "temp_high"), ("temp_low", "temp_low"),
                            ("precip", "precip"), ("wind", "wind")]:
            vals = md[key]
            if len(vals) >= 10:
                mean = sum(vals) / len(vals)
                variance = sum((v - mean) ** 2 for v in vals) / len(vals)
                std = math.sqrt(variance)
                stats[f"{metric}_mean"] = round(mean, 2)
                stats[f"{metric}_std"] = round(std, 2)
            else:
                stats[f"{metric}_mean"] = None
                stats[f"{metric}_std"] = None

        db.insert_climatology(city_id, m, stats)
        print(f"  Month {m:2d}: high={stats.get('temp_high_mean')}C, "
              f"low={stats.get('temp_low_mean')}C, "
              f"precip={stats.get('precip_mean')}mm")

    print("Climatology complete.")


# ---------------------------------------------------------------------------
# Historical weather for analog training
# ---------------------------------------------------------------------------
def get_historical_monthly_weather(city_id, lat, lon, year, month):
    """Fetch actual monthly weather for a specific year/month.
    Returns dict with temp_high_mean, temp_low_mean, precip_total, wind_max
    or None on failure.
    """
    days_in_month = [31, 28 + (1 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 0),
                     31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    last_day = days_in_month[month - 1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max"
        f"&timezone=auto"
    )

    try:
        text = fetch_url(url, timeout=30)
        data = json.loads(text)
        daily = data.get("daily", {})

        t_max = [v for v in daily.get("temperature_2m_max", []) if v is not None]
        t_min = [v for v in daily.get("temperature_2m_min", []) if v is not None]
        precip = [v for v in daily.get("precipitation_sum", []) if v is not None]
        wind = [v for v in daily.get("wind_speed_10m_max", []) if v is not None]

        if not t_max:
            return None

        return {
            "temp_high_mean": sum(t_max) / len(t_max),
            "temp_low_mean": sum(t_min) / len(t_min) if t_min else None,
            "precip_total": sum(precip) if precip else None,
            "wind_max": max(wind) if wind else None,
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def print_status():
    """Print current index state."""
    state = get_current_index_state()
    if not state:
        print("No index data. Run: python scripts/climate_indices.py")
        return

    print("=" * 60)
    print("  CLIMATE TELECONNECTION INDEX STATE")
    print("=" * 60)

    for name, info in sorted(state.items()):
        phase = info.get("phase", "")
        val = info["value"]
        ym = f"{info['year']}-{info['month']:02d}"
        print(f"  {name:8s}  {val:+7.2f}  ({phase:20s})  [{ym}]")

    print("=" * 60)
    fetch_time = db.get_indices_fetch_time()
    if fetch_time:
        print(f"  Last updated: {fetch_time}")


def main():
    parser = argparse.ArgumentParser(description="Climate Index Manager")
    parser.add_argument("--force", action="store_true",
                        help="Force refresh even if cache is fresh")
    parser.add_argument("--status", action="store_true",
                        help="Show current index state")
    parser.add_argument("--climatology", metavar="CITY",
                        help="Build climatology for a city")
    args = parser.parse_args()

    if args.status:
        print_status()
        return

    if args.climatology:
        from fetch_weather import geocode
        city = db.get_city(args.climatology)
        if not city:
            loc = geocode(args.climatology)
            if not loc:
                print(f"City not found: {args.climatology}")
                return
            city_id = db.insert_city(
                loc["name"], loc["country"], loc["lat"], loc["lon"],
                loc.get("timezone", "auto"))
            city = db.get_city_by_id(city_id)
        build_climatology(city["id"], city["lat"], city["lon"])
        return

    results = fetch_all_indices(force=args.force)
    if results:
        ok = sum(1 for _, _, err in results if not err)
        fail = sum(1 for _, _, err in results if err)
        print(f"\nDone: {ok} indices updated, {fail} errors")
    print_status()


if __name__ == "__main__":
    main()
