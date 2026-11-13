#!/usr/bin/env python3
"""Location forecast service for the web app and a future iOS client.

``GET /api/v1/forecast?lat&lon`` and ``?place`` both end here. The heavy
lifting stays in ``produce_forecast`` (source fetch, Bayesian weights,
physics). This module resolves the place, caches the result, and reshapes
it into a stable contract.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from urllib.parse import quote
from urllib.request import Request, urlopen

from fetch_weather import fetch_json
from weighted_forecast import compute_confidence, produce_forecast

logger = logging.getLogger(__name__)

_CACHE = {}
_CACHE_LOCK = threading.Lock()
_TTL_SECONDS = 15 * 60

ATTRIBUTION = [
    {"source": "Open-Meteo", "license": "CC BY 4.0", "url": "https://open-meteo.com"},
    {"source": "ECMWF", "license": "CC BY 4.0", "url": "https://www.ecmwf.int"},
    {
        "source": "SHMU",
        "license": "CC BY 4.0",
        "url": "https://opendata.shmu.sk",
        "note": "ALADIN 4.5 km and automatic stations, © SHMÚ, CC BY 4.0. No 2 m humidity in the open GRIB. A-LAEF is not published on opendata.shmu.sk.",
    },
    {"source": "NOAA NWS", "license": "public domain", "url": "https://www.weather.gov"},
    {"source": "wttr.in", "url": "https://wttr.in"},
]


def geocode_search(query, count=5):
    """Open-Meteo place search. Returns a list of location dicts."""
    query = (query or "").strip()
    if not query:
        return []
    count = max(1, min(int(count), 8))
    url = (
        "https://geocoding-api.open-meteo.com/v1/search"
        f"?name={quote(query)}&count={count}&language=en"
    )
    data = fetch_json(url, timeout=10, retries=2)
    results = []
    for row in (data or {}).get("results") or []:
        results.append(_location_from_geocode(row, fallback_name=query))
    return results


def reverse_geocode(lat, lon):
    """Nearest named place for a coordinate. Falls back to the coordinate label.

    Open-Meteo's geocoder searches by name and does not reverse a coordinate
    on this deployment, so the label comes from Nominatim and the IANA
    timezone from the closest Open-Meteo hit for that name.
    """
    fallback = {
        "name": f"{float(lat):.2f}, {float(lon):.2f}",
        "country": "",
        "admin1": "",
        "lat": float(lat),
        "lon": float(lon),
        "timezone": "auto",
    }
    url = (
        "https://nominatim.openstreetmap.org/reverse"
        f"?lat={lat}&lon={lon}&format=jsonv2&zoom=12&accept-language=en"
    )
    req = Request(url, headers={"User-Agent": "WeatherPredict/1.0 (forecast app; local)"})
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (OSError, json.JSONDecodeError):
        return fallback
    address = data.get("address") or {}
    name = address.get("city") or address.get("town") or address.get("village") or data.get("name")
    if not name:
        return fallback
    location = {
        "name": name,
        "country": address.get("country") or "",
        "admin1": address.get("state") or "",
        "lat": float(lat),
        "lon": float(lon),
        "timezone": "auto",
    }
    try:
        matches = geocode_search(name, count=5)
    except Exception:
        matches = []
    best = None
    best_dist = None
    for match in matches:
        if match.get("lat") is None:
            continue
        dist = (float(match["lat"]) - float(lat)) ** 2 + (float(match["lon"]) - float(lon)) ** 2
        if best_dist is None or dist < best_dist:
            best = match
            best_dist = dist
    if best and best.get("timezone"):
        location["timezone"] = best["timezone"]
        if best.get("country"):
            location["country"] = best["country"]
    return location


def _location_from_geocode(row, fallback_name):
    return {
        "name": row.get("name") or fallback_name,
        "country": row.get("country") or "",
        "admin1": row.get("admin1") or "",
        "lat": row.get("latitude"),
        "lon": row.get("longitude"),
        "timezone": row.get("timezone") or "auto",
    }


def _num(value, digits=1):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _source_current(reading):
    if not reading:
        return None
    return {
        "temperature_c": _num(reading.get("temp_c")),
        "feels_like_c": _num(reading.get("feels_like_c")),
        "humidity_pct": _num(reading.get("humidity"), 0),
        "wind_speed_kmh": _num(reading.get("wind_speed_kmh")),
        "wind_direction_deg": _num(reading.get("wind_dir_deg"), 0),
        "pressure_hpa": _num(reading.get("pressure_hpa")),
        "precipitation_mm": _num(reading.get("precipitation_mm"), 2),
        "condition": reading.get("condition"),
        "role": reading.get("role") or "forecast",
        "station": reading.get("station"),
        "model": reading.get("model"),
        "observed_at": reading.get("observed_at"),
    }


def _source_day(day):
    if not day:
        return None
    return {
        "temperature_max_c": _num(day.get("high_c")),
        "temperature_min_c": _num(day.get("low_c")),
        "precipitation_mm": _num(day.get("precip_mm"), 2),
        "precipitation_probability": _num(day.get("precip_prob"), 0),
        "wind_speed_kmh": _num(day.get("wind_max_kmh")),
        "condition": day.get("condition"),
    }


def _confidence(label, score, spread):
    return {"label": label, "score": score, "spread_c": spread}


def to_public_forecast(raw):
    """Reshape ``produce_forecast`` output into the versioned client contract."""
    if not raw or raw.get("error"):
        return raw
    current = raw.get("current") or {}
    by_source = {
        name: _source_current(reading)
        for name, reading in (raw.get("current_by_source") or {}).items()
    }
    label = current.get("confidence")
    score = current.get("confidence_score")
    spread = current.get("temp_spread_c")
    public_current = {
        "temperature_c": _num(current.get("temp_c")),
        "feels_like_c": _num(current.get("feels_like_c")),
        "humidity_pct": _num(current.get("humidity_pct"), 0),
        "wind_speed_kmh": _num(current.get("wind_speed_kmh")),
        "pressure_hpa": _num(current.get("pressure_hpa")),
        "precipitation_mm": None,
        "condition": _dominant_condition(current.get("conditions")),
        "confidence": _confidence(label, score, spread),
        "ensemble": {
            "temperature_c": _num(current.get("temp_c")),
            "feels_like_c": _num(current.get("feels_like_c")),
            "humidity_pct": _num(current.get("humidity_pct"), 0),
            "wind_speed_kmh": _num(current.get("wind_speed_kmh")),
            "pressure_hpa": _num(current.get("pressure_hpa")),
            "condition": _dominant_condition(current.get("conditions")),
        },
        "sources": by_source,
    }

    hourly = []
    for hour in raw.get("hourly") or []:
        sources = {}
        for name, reading in (hour.get("sources") or {}).items():
            sources[name] = {
                "temperature_c": _num(reading.get("temp_c")),
                "precipitation_mm": _num(reading.get("precip_mm"), 2),
                "precipitation_probability": _num(reading.get("precip_prob"), 0),
                "wind_speed_kmh": _num(reading.get("wind_kmh")),
                "condition": reading.get("condition"),
            }
        temp = _num(hour.get("temp_c"))
        precip = _num(hour.get("precip_mm"), 2)
        prob = _num(hour.get("precip_prob"), 0)
        wind = _num(hour.get("wind_kmh"))
        condition = hour.get("condition")
        spread_h = hour.get("temp_spread_c")
        h_label, h_score = compute_confidence(spread_h or 0, {}, len(sources) or 1)
        hourly.append({
            "time": hour.get("hour"),
            "temperature_c": temp,
            "precipitation_mm": precip,
            "precipitation_probability": prob,
            "wind_speed_kmh": wind,
            "condition": condition,
            "confidence": _confidence(h_label, h_score, spread_h),
            "ensemble": {
                "temperature_c": temp,
                "precipitation_mm": precip,
                "precipitation_probability": prob,
                "wind_speed_kmh": wind,
                "condition": condition,
            },
            "sources": sources,
        })

    daily = []
    for day in raw.get("daily") or []:
        sources = {}
        for name, reading in (day.get("per_source") or {}).items():
            shaped = _source_day(reading)
            if shaped:
                sources[name] = shaped
        ensemble_block = day.get("ensemble") or {}
        high = _num(day.get("weighted_high_c"))
        low = _num(day.get("weighted_low_c"))
        precip = _num(day.get("adjusted_precip_mm", day.get("weighted_precip_mm")), 2)
        prob = _num(day.get("adjusted_precip_prob", day.get("weighted_precip_prob")), 0)
        wind = _num(day.get("weighted_wind_kmh"))
        condition = day.get("weighted_condition")
        spread_d = day.get("high_spread_c")
        weights = {name: 1.0 for name in sources}
        d_label, d_score = compute_confidence(spread_d or 0, weights, len(sources) or 1)
        daily.append({
            "date": day.get("date"),
            "temperature_max_c": high,
            "temperature_min_c": low,
            "precipitation_mm": precip,
            "precipitation_probability": prob,
            "wind_speed_kmh": wind,
            "condition": condition,
            "sunrise": day.get("sunrise"),
            "sunset": day.get("sunset"),
            "confidence": _confidence(d_label, d_score, spread_d),
            "ensemble": {
                "temperature_max_c": high,
                "temperature_min_c": low,
                "precipitation_mm": precip,
                "precipitation_probability": prob,
                "wind_speed_kmh": wind,
                "condition": condition,
                "temperature_p10_c": _num(ensemble_block.get("temp_p10")),
                "temperature_p90_c": _num(ensemble_block.get("temp_p90")),
                "precipitation_p10_mm": _num(ensemble_block.get("precip_p10"), 2),
                "precipitation_p90_mm": _num(ensemble_block.get("precip_p90"), 2),
            },
            "sources": sources,
        })

    loc = raw.get("location") or {}
    statuses = []
    for name, status in (raw.get("source_status") or {}).items():
        statuses.append({
            "id": name,
            "status": "ok" if status == "OK" else "unavailable",
            "detail": status,
        })

    return {
        "location": {
            "name": loc.get("name"),
            "country": loc.get("country"),
            "latitude": loc.get("lat"),
            "longitude": loc.get("lon"),
            "timezone": loc.get("timezone"),
        },
        "units": {
            "temperature": "celsius",
            "wind_speed": "km/h",
            "pressure": "hPa",
            "precipitation": "mm",
            "probability": "percent",
        },
        "current": public_current,
        "hourly": hourly,
        "daily": daily,
        "sources": statuses,
        "attribution": ATTRIBUTION,
        "generated_at": raw.get("generated_at"),
    }


def _dominant_condition(conditions):
    if not conditions:
        return None
    counts = {}
    for cond in conditions:
        if not cond:
            continue
        counts[cond] = counts.get(cond, 0) + 1
    if not counts:
        return None
    return max(counts, key=counts.get)


def resolve_location(place=None, lat=None, lon=None):
    """Return ``(location, error_message, status_code)``."""
    if lat is not None or lon is not None:
        if lat is None or lon is None:
            return None, "Provide both lat and lon", 400
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            return None, "lat and lon must be numbers", 400
        if not -90 <= lat_f <= 90 or not -180 <= lon_f <= 180:
            return None, "lat or lon is out of range", 400
        return reverse_geocode(lat_f, lon_f), None, 200
    place = (place or "").strip()
    if not place:
        return None, "Provide place or lat and lon", 400
    if len(place) > 80:
        return None, "Place name is too long", 400
    matches = geocode_search(place, count=1)
    if not matches or matches[0].get("lat") is None:
        return None, f"Could not find place: {place}", 404
    return matches[0], None, 200


def build_forecast(place=None, lat=None, lon=None, days=10, use_cache=True):
    """Resolve a location, run the ensemble, and return ``(payload, http_status)``."""
    days = max(1, min(int(days or 10), 10))
    location, error, status = resolve_location(place=place, lat=lat, lon=lon)
    if error:
        return {"error": error}, status

    # The caller asked for a coordinate: keep those numbers even if reverse
    # geocoding snapped the label to a nearby town.
    if lat is not None and lon is not None:
        location = dict(location)
        location["lat"] = float(lat)
        location["lon"] = float(lon)

    cache_key = (
        round(float(location["lat"]), 2),
        round(float(location["lon"]), 2),
        days,
    )
    if use_cache:
        with _CACHE_LOCK:
            cached = _CACHE.get(cache_key)
        if cached and (time.time() - cached[0]) < _TTL_SECONDS:
            return cached[1], 200

    raw = produce_forecast(
        location.get("name"),
        location=location,
        forecast_days=days,
        respect_tiers=False,
    )
    if raw.get("error"):
        status = 404 if "Could not find" in raw["error"] else 503
        payload = {"error": raw["error"], "sources": _status_list(raw.get("source_status"))}
        return payload, status

    raw_location = raw.setdefault("location", {})
    raw_location["timezone"] = location.get("timezone")
    public = to_public_forecast(raw)
    if use_cache:
        with _CACHE_LOCK:
            _CACHE[cache_key] = (time.time(), public)
    return public, 200


def _status_list(source_status):
    if not source_status:
        return []
    return [
        {"id": name, "status": "ok" if status == "OK" else "unavailable", "detail": status}
        for name, status in source_status.items()
    ]


def clear_cache():
    with _CACHE_LOCK:
        _CACHE.clear()
