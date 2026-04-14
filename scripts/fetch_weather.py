#!/usr/bin/env python3
"""
Weather Analyst — Multi-Source Weather Intelligence
Fetches weather from multiple free APIs, compares forecasts, and delivers
consensus analysis with personal planning and market-relevant insights.

Sources:
  - Open-Meteo (free, no key)
  - wttr.in (free, no key)
  - OpenWeatherMap (free tier, optional key via OPENWEATHER_API_KEY)
  - WeatherAPI.com (free tier, optional key via WEATHERAPI_KEY)
  - NOAA National Weather Service (free, no key, US only)
  - ECMWF IFS via Open-Meteo (free, no key, 51-member ensemble)

Usage:
  python fetch_weather.py Bratislava
  python fetch_weather.py "New York"
  python fetch_weather.py --scheduled Bratislava    # compact briefing mode
  python fetch_weather.py --json Bratislava         # raw JSON output
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.parse import quote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Weather code descriptions (WMO standard, used by Open-Meteo)
# ---------------------------------------------------------------------------
WMO_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    56: "Light freezing drizzle", 57: "Dense freezing drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    66: "Light freezing rain", 67: "Heavy freezing rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
}

# ---------------------------------------------------------------------------
# Helper: safe JSON fetch
# ---------------------------------------------------------------------------
def fetch_json(url, timeout=10, retries=3, retry_delay=2):
    """Fetch a URL and return parsed JSON, or None on failure.
    Retries up to `retries` times on connection errors/timeouts (not 4xx)."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "WeatherAnalyst/1.0"})
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            if 400 <= e.code < 500:
                return None  # Don't retry client errors
            if attempt < retries - 1:
                time.sleep(retry_delay)
                continue
            return None
        except (URLError, TimeoutError, OSError) as e:
            if attempt < retries - 1:
                time.sleep(retry_delay)
                continue
            return None
        except (json.JSONDecodeError, Exception) as e:
            return None
    return None

# ---------------------------------------------------------------------------
# Geocoding via Open-Meteo
# ---------------------------------------------------------------------------
def geocode(city):
    """Resolve city name to (name, country, lat, lon). Returns None if not found."""
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={quote(city)}&count=3&language=en"
    data = fetch_json(url)
    if not data or "results" not in data or len(data["results"]) == 0:
        return None
    r = data["results"][0]
    return {
        "name": r.get("name", city),
        "country": r.get("country", ""),
        "lat": r["latitude"],
        "lon": r["longitude"],
        "timezone": r.get("timezone", "auto"),
    }

# ---------------------------------------------------------------------------
# Source 1: Open-Meteo
# ---------------------------------------------------------------------------
def fetch_open_meteo(lat, lon, tz="auto"):
    """Fetch from Open-Meteo (free, no key)."""
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
        f"precipitation,weather_code,wind_speed_10m,wind_direction_10m,"
        f"surface_pressure"
        f"&daily=weather_code,temperature_2m_max,temperature_2m_min,"
        f"precipitation_sum,precipitation_probability_max,wind_speed_10m_max,"
        f"wind_direction_10m_dominant,sunrise,sunset"
        f"&hourly=temperature_2m,relative_humidity_2m,precipitation,"
        f"precipitation_probability,wind_speed_10m,wind_direction_10m,weathercode,uv_index,"
        f"apparent_temperature,cloud_cover"
        f"&timezone={tz}&forecast_days=7"
    )
    raw = fetch_json(url)
    if not raw or "current" not in raw:
        return None

    c = raw["current"]
    daily = raw.get("daily", {})
    hourly = raw.get("hourly", {})

    result = {
        "source": "Open-Meteo",
        "current": {
            "temp_c": c.get("temperature_2m"),
            "feels_like_c": c.get("apparent_temperature"),
            "humidity": c.get("relative_humidity_2m"),
            "wind_speed_kmh": c.get("wind_speed_10m"),
            "wind_dir_deg": c.get("wind_direction_10m"),
            "precipitation_mm": c.get("precipitation"),
            "pressure_hpa": c.get("surface_pressure"),
            "condition": WMO_CODES.get(c.get("weather_code", -1), "Unknown"),
        },
        "daily": [],
        "hourly": [],
    }

    dates = daily.get("time", [])
    for i, date in enumerate(dates):
        day_entry = {
            "date": date,
            "high_c": daily["temperature_2m_max"][i] if i < len(daily.get("temperature_2m_max", [])) else None,
            "low_c": daily["temperature_2m_min"][i] if i < len(daily.get("temperature_2m_min", [])) else None,
            "precip_prob": daily["precipitation_probability_max"][i] if i < len(daily.get("precipitation_probability_max", [])) else None,
            "precip_mm": daily["precipitation_sum"][i] if i < len(daily.get("precipitation_sum", [])) else None,
            "wind_max_kmh": daily["wind_speed_10m_max"][i] if i < len(daily.get("wind_speed_10m_max", [])) else None,
            "wind_dir_deg": daily["wind_direction_10m_dominant"][i] if i < len(daily.get("wind_direction_10m_dominant", [])) else None,
            "sunrise": daily["sunrise"][i] if i < len(daily.get("sunrise", [])) else None,
            "sunset": daily["sunset"][i] if i < len(daily.get("sunset", [])) else None,
            "condition": WMO_CODES.get(daily["weather_code"][i], "Unknown") if i < len(daily.get("weather_code", [])) else None,
        }
        result["daily"].append(day_entry)

    # Compute daily average cloud cover from hourly data
    _daily_cloud = {}  # date -> list of values
    cloud_hourly_vals = hourly.get("cloud_cover", [])
    for i, htime in enumerate(hourly.get("time", [])):
        if i < len(cloud_hourly_vals) and cloud_hourly_vals[i] is not None:
            day_key = htime[:10]
            _daily_cloud.setdefault(day_key, []).append(cloud_hourly_vals[i])
    for day_entry in result["daily"]:
        cvals = _daily_cloud.get(day_entry.get("date"), [])
        day_entry["cloud_cover_avg"] = round(sum(cvals) / len(cvals), 1) if cvals else None

    # Parse hourly data
    hourly_times = hourly.get("time", [])
    for i, htime in enumerate(hourly_times):
        wcode_list = hourly.get("weathercode", [])
        wcode = wcode_list[i] if i < len(wcode_list) else None
        temp_list = hourly.get("temperature_2m", [])
        humid_list = hourly.get("relative_humidity_2m", [])
        precip_list = hourly.get("precipitation", [])
        precip_prob_list = hourly.get("precipitation_probability", [])
        wind_list = hourly.get("wind_speed_10m", [])
        wdir_list = hourly.get("wind_direction_10m", [])
        uv_list = hourly.get("uv_index", [])
        feels_list = hourly.get("apparent_temperature", [])
        cloud_list = hourly.get("cloud_cover", [])
        result["hourly"].append({
            "hour": htime,
            "temp_c": temp_list[i] if i < len(temp_list) else None,
            "humidity": humid_list[i] if i < len(humid_list) else None,
            "precip_mm": precip_list[i] if i < len(precip_list) else None,
            "precip_prob": precip_prob_list[i] if i < len(precip_prob_list) else None,
            "wind_kmh": wind_list[i] if i < len(wind_list) else None,
            "wind_dir": wdir_list[i] if i < len(wdir_list) else None,
            "condition": WMO_CODES.get(wcode, "Unknown") if wcode is not None else None,
            "uv_index": uv_list[i] if i < len(uv_list) else None,
            "feels_like": feels_list[i] if i < len(feels_list) else None,
            "cloud_cover": cloud_list[i] if i < len(cloud_list) else None,
        })

    return validate_weather_data(result)

# ---------------------------------------------------------------------------
# Source 2: wttr.in
# ---------------------------------------------------------------------------
def fetch_wttr(city):
    """Fetch from wttr.in (free, no key)."""
    url = f"https://wttr.in/{quote(city)}?format=j1"
    raw = fetch_json(url, timeout=15)
    if not raw or "current_condition" not in raw:
        return None

    cc = raw["current_condition"][0]
    result = {
        "source": "wttr.in",
        "current": {
            "temp_c": float(cc["temp_C"]) if cc.get("temp_C") else None,
            "feels_like_c": float(cc["FeelsLikeC"]) if cc.get("FeelsLikeC") else None,
            "humidity": int(cc["humidity"]) if cc.get("humidity") else None,
            "wind_speed_kmh": float(cc["windspeedKmph"]) if cc.get("windspeedKmph") else None,
            "wind_dir_deg": None,
            "precipitation_mm": float(cc["precipMM"]) if cc.get("precipMM") else None,
            "pressure_hpa": float(cc["pressure"]) if cc.get("pressure") else None,
            "condition": cc.get("weatherDesc", [{}])[0].get("value", "Unknown"),
        },
        "daily": [],
    }

    for day in raw.get("weather", []):
        result["daily"].append({
            "date": day.get("date"),
            "high_c": float(day["maxtempC"]) if day.get("maxtempC") else None,
            "low_c": float(day["mintempC"]) if day.get("mintempC") else None,
            "precip_prob": None,  # wttr.in doesn't give daily probability cleanly
            "precip_mm": None,
            "wind_max_kmh": None,
            "condition": day.get("hourly", [{}])[4].get("weatherDesc", [{}])[0].get("value", "Unknown") if len(day.get("hourly", [])) > 4 else None,
        })

    return validate_weather_data(result)

# ---------------------------------------------------------------------------
# Source 3: OpenWeatherMap (optional, needs OPENWEATHER_API_KEY)
# ---------------------------------------------------------------------------
def fetch_openweather(lat, lon):
    """Fetch from OpenWeatherMap free tier."""
    key = os.environ.get("OPENWEATHER_API_KEY")
    if not key:
        return None

    # Current weather
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric"
    raw = fetch_json(url)
    if not raw or "main" not in raw:
        return None

    # 5-day forecast
    fc_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={key}&units=metric"
    fc_raw = fetch_json(fc_url)

    result = {
        "source": "OpenWeatherMap",
        "current": {
            "temp_c": raw["main"].get("temp"),
            "feels_like_c": raw["main"].get("feels_like"),
            "humidity": raw["main"].get("humidity"),
            "wind_speed_kmh": round(raw.get("wind", {}).get("speed", 0) * 3.6, 1),
            "wind_dir_deg": raw.get("wind", {}).get("deg"),
            "precipitation_mm": raw.get("rain", {}).get("1h", 0),
            "pressure_hpa": raw["main"].get("pressure"),
            "condition": raw.get("weather", [{}])[0].get("description", "Unknown").title(),
        },
        "daily": [],
    }

    # Aggregate 3-hourly forecast into daily
    if fc_raw and "list" in fc_raw:
        days = {}
        for item in fc_raw["list"]:
            date = item["dt_txt"][:10]
            if date not in days:
                days[date] = {"highs": [], "lows": [], "conditions": []}
            days[date]["highs"].append(item["main"]["temp_max"])
            days[date]["lows"].append(item["main"]["temp_min"])
            days[date]["conditions"].append(item["weather"][0]["description"])

        for date in sorted(days.keys())[:7]:
            d = days[date]
            result["daily"].append({
                "date": date,
                "high_c": round(max(d["highs"]), 1),
                "low_c": round(min(d["lows"]), 1),
                "precip_prob": None,
                "precip_mm": None,
                "wind_max_kmh": None,
                "condition": max(set(d["conditions"]), key=d["conditions"].count).title(),
            })

    return validate_weather_data(result)

# ---------------------------------------------------------------------------
# Source 4: WeatherAPI.com (optional, needs WEATHERAPI_KEY)
# ---------------------------------------------------------------------------
def fetch_weatherapi(lat, lon):
    """Fetch from WeatherAPI.com free tier."""
    key = os.environ.get("WEATHERAPI_KEY")
    if not key:
        return None

    url = f"https://api.weatherapi.com/v1/forecast.json?key={key}&q={lat},{lon}&days=7&aqi=yes&alerts=yes"
    raw = fetch_json(url)
    if not raw or "current" not in raw:
        return None

    c = raw["current"]

    # Parse AQI data from current conditions
    aq = c.get("air_quality", {})
    aqi_data = {}
    if aq:
        aqi_data = {
            "aqi_index": aq.get("us-epa-index"),      # US EPA 1-6 scale
            "pm25": aq.get("pm2_5"),
            "pm10": aq.get("pm10"),
            "co": aq.get("co"),
            "no2": aq.get("no2"),
            "o3": aq.get("o3"),
            "so2": aq.get("so2"),
            "gb_defra_index": aq.get("gb-defra-index"),
        }

    result = {
        "source": "WeatherAPI",
        "current": {
            "temp_c": c.get("temp_c"),
            "feels_like_c": c.get("feelslike_c"),
            "humidity": c.get("humidity"),
            "wind_speed_kmh": c.get("wind_kph"),
            "wind_dir_deg": c.get("wind_degree"),
            "precipitation_mm": c.get("precip_mm"),
            "pressure_hpa": c.get("pressure_mb"),
            "condition": c.get("condition", {}).get("text", "Unknown"),
            **aqi_data,
        },
        "daily": [],
        "alerts": [],
    }

    for day in raw.get("forecast", {}).get("forecastday", []):
        d = day.get("day", {})
        day_entry = {
            "date": day.get("date"),
            "high_c": d.get("maxtemp_c"),
            "low_c": d.get("mintemp_c"),
            "precip_prob": d.get("daily_chance_of_rain"),
            "precip_mm": d.get("totalprecip_mm"),
            "wind_max_kmh": d.get("maxwind_kph"),
            "condition": d.get("condition", {}).get("text", "Unknown"),
        }
        # Daily AQI if available
        day_aq = d.get("air_quality", {})
        if day_aq:
            day_entry["aqi_index"] = day_aq.get("us-epa-index")
            day_entry["pm25"] = day_aq.get("pm2_5")
            day_entry["pm10"] = day_aq.get("pm10")
        result["daily"].append(day_entry)

    # Parse official weather alerts
    for alert in raw.get("alerts", {}).get("alert", []):
        result["alerts"].append({
            "headline": alert.get("headline"),
            "msgtype": alert.get("msgtype"),
            "severity": alert.get("severity"),
            "urgency": alert.get("urgency"),
            "areas": alert.get("areas"),
            "category": alert.get("category"),
            "certainty": alert.get("certainty"),
            "event": alert.get("event"),
            "note": alert.get("note"),
            "effective": alert.get("effective"),
            "expires": alert.get("expires"),
            "desc": alert.get("desc"),
            "instruction": alert.get("instruction"),
        })

    return validate_weather_data(result)

# ---------------------------------------------------------------------------
# Source 5: Visual Crossing (optional, needs VISUAL_CROSSING_KEY)
# ---------------------------------------------------------------------------
def fetch_visual_crossing(lat, lon):
    """Fetch from Visual Crossing Weather API free tier."""
    key = os.environ.get("VISUAL_CROSSING_KEY")
    if not key:
        return None

    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{lat},{lon}?unitGroup=metric&key={key}&contentType=json&include=days,current"
    )
    raw = fetch_json(url, timeout=15)
    if not raw or "currentConditions" not in raw:
        return None

    cc = raw["currentConditions"]
    result = {
        "source": "VisualCrossing",
        "current": {
            "temp_c": cc.get("temp"),
            "feels_like_c": cc.get("feelslike"),
            "humidity": cc.get("humidity"),
            "wind_speed_kmh": cc.get("windspeed"),
            "wind_dir_deg": cc.get("winddir"),
            "precipitation_mm": cc.get("precip") or 0,
            "pressure_hpa": cc.get("pressure"),
            "condition": cc.get("conditions", "Unknown"),
        },
        "daily": [],
    }

    for day in raw.get("days", [])[:7]:
        result["daily"].append({
            "date": day.get("datetime"),
            "high_c": day.get("tempmax"),
            "low_c": day.get("tempmin"),
            "precip_prob": day.get("precipprob"),
            "precip_mm": day.get("precip"),
            "wind_max_kmh": day.get("windspeed"),
            "condition": day.get("conditions", "Unknown"),
        })

    return validate_weather_data(result)


# ---------------------------------------------------------------------------
# Source 6: NOAA National Weather Service (free, no key, US only)
# ---------------------------------------------------------------------------
def _fetch_nws_json(url, timeout=10, retries=3, retry_delay=2):
    """Fetch JSON from NWS API with required User-Agent header."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={
                "User-Agent": "WeatherPredict/1.0 (weather-predict-app)",
                "Accept": "application/geo+json",
            })
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            if 400 <= e.code < 500:
                return None
            if attempt < retries - 1:
                time.sleep(retry_delay)
                continue
            return None
        except (URLError, TimeoutError, OSError):
            if attempt < retries - 1:
                time.sleep(retry_delay)
                continue
            return None
        except (json.JSONDecodeError, Exception):
            return None
    return None


def _nws_condition(short_forecast):
    """Extract a simple condition string from NWS short forecast text."""
    if not short_forecast:
        return "Unknown"
    lower = short_forecast.lower()
    if "thunder" in lower:
        return "Thunderstorm"
    if "snow" in lower and "heavy" in lower:
        return "Heavy snow"
    if "snow" in lower:
        return "Slight snow"
    if "rain" in lower and ("heavy" in lower or "downpour" in lower):
        return "Heavy rain"
    if "freezing rain" in lower or "ice" in lower:
        return "Slight rain"
    if "rain" in lower or "showers" in lower:
        return "Slight rain"
    if "sleet" in lower:
        return "Slight rain"
    if "drizzle" in lower:
        return "Light drizzle"
    if "fog" in lower or "haze" in lower or "mist" in lower:
        return "Fog"
    if "cloud" in lower or "overcast" in lower:
        return "Overcast"
    if "partly" in lower:
        return "Partly cloudy"
    if "clear" in lower or "sunny" in lower:
        return "Clear sky"
    return short_forecast


def fetch_noaa_nws(city_name, lat, lon):
    """Fetch from NOAA National Weather Service (free, no key, US only).

    Returns None silently for non-US locations.
    """
    # Only works for US locations — CONUS, Alaska, Hawaii, Puerto Rico
    is_conus = 24.0 <= lat <= 50.0 and -125.0 <= lon <= -66.0
    is_alaska = 51.0 <= lat <= 72.0 and -180.0 <= lon <= -129.0
    is_hawaii = 18.0 <= lat <= 23.0 and -161.0 <= lon <= -154.0
    is_pr = 17.0 <= lat <= 19.0 and -68.0 <= lon <= -65.0
    if not (is_conus or is_alaska or is_hawaii or is_pr):
        return None

    # Step 1: Get gridpoint metadata
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    points_data = _fetch_nws_json(points_url)
    if not points_data or "properties" not in points_data:
        return None

    props = points_data["properties"]
    forecast_url = props.get("forecast")
    hourly_url = props.get("forecastHourly")
    if not forecast_url:
        return None

    # Step 2: Fetch 7-day forecast
    forecast_data = _fetch_nws_json(forecast_url)
    if not forecast_data or "properties" not in forecast_data:
        return None

    periods = forecast_data["properties"].get("periods", [])
    if not periods:
        return None

    # Step 3: Fetch hourly forecast
    hourly_periods = []
    if hourly_url:
        hourly_data = _fetch_nws_json(hourly_url)
        if hourly_data and "properties" in hourly_data:
            hourly_periods = hourly_data["properties"].get("periods", [])

    # Build current conditions from the first period
    first = periods[0]
    # NWS temps are in Fahrenheit by default
    first_temp_f = first.get("temperature")
    first_temp_c = round((first_temp_f - 32) * 5 / 9, 1) if first_temp_f is not None else None
    wind_str = first.get("windSpeed", "0")
    # windSpeed can be like "10 mph" or "5 to 10 mph"
    wind_mph = 0
    try:
        parts = wind_str.replace("mph", "").strip().split("to")
        wind_mph = float(parts[-1].strip())
    except (ValueError, IndexError):
        wind_mph = 0
    wind_kmh = round(wind_mph * 1.60934, 1)

    result = {
        "source": "NOAA_NWS",
        "current": {
            "temp_c": first_temp_c,
            "feels_like_c": None,  # NWS doesn't provide feels-like in this endpoint
            "humidity": None,
            "wind_speed_kmh": wind_kmh,
            "wind_dir_deg": None,
            "precipitation_mm": None,
            "pressure_hpa": None,
            "condition": _nws_condition(first.get("shortForecast")),
        },
        "daily": [],
        "hourly": [],
    }

    # Parse daily periods: NWS gives day/night pairs
    # Group by date and extract high (daytime) / low (nighttime)
    day_map = {}  # date -> {high, low, condition, wind_max}
    for period in periods:
        start = period.get("startTime", "")
        date = start[:10] if len(start) >= 10 else None
        if not date:
            continue

        temp_f = period.get("temperature")
        temp_c = round((temp_f - 32) * 5 / 9, 1) if temp_f is not None else None
        is_day = period.get("isDaytime", True)

        ws = period.get("windSpeed", "0")
        try:
            ws_parts = ws.replace("mph", "").strip().split("to")
            ws_mph = float(ws_parts[-1].strip())
        except (ValueError, IndexError):
            ws_mph = 0
        ws_kmh = round(ws_mph * 1.60934, 1)

        precip_prob = period.get("probabilityOfPrecipitation", {})
        if isinstance(precip_prob, dict):
            precip_prob = precip_prob.get("value")

        if date not in day_map:
            day_map[date] = {
                "date": date,
                "high_c": None, "low_c": None,
                "precip_prob": None, "precip_mm": None,
                "wind_max_kmh": 0,
                "condition": None,
            }

        entry = day_map[date]
        if is_day:
            entry["high_c"] = temp_c
            entry["condition"] = _nws_condition(period.get("shortForecast"))
        else:
            entry["low_c"] = temp_c

        if ws_kmh > (entry["wind_max_kmh"] or 0):
            entry["wind_max_kmh"] = ws_kmh

        if precip_prob is not None:
            if entry["precip_prob"] is None or precip_prob > entry["precip_prob"]:
                entry["precip_prob"] = precip_prob

    for date in sorted(day_map.keys())[:7]:
        result["daily"].append(day_map[date])

    # Parse hourly data
    for hp in hourly_periods:
        start = hp.get("startTime", "")
        # Convert ISO time to match Open-Meteo format "YYYY-MM-DDTHH:MM"
        hour_str = start[:16] if len(start) >= 16 else start

        temp_f = hp.get("temperature")
        temp_c = round((temp_f - 32) * 5 / 9, 1) if temp_f is not None else None

        ws = hp.get("windSpeed", "0")
        try:
            ws_parts = ws.replace("mph", "").strip().split("to")
            ws_mph = float(ws_parts[-1].strip())
        except (ValueError, IndexError):
            ws_mph = 0

        hp_precip_prob = hp.get("probabilityOfPrecipitation", {})
        if isinstance(hp_precip_prob, dict):
            hp_precip_prob = hp_precip_prob.get("value")

        result["hourly"].append({
            "hour": hour_str,
            "temp_c": temp_c,
            "humidity": hp.get("relativeHumidity", {}).get("value") if isinstance(hp.get("relativeHumidity"), dict) else None,
            "precip_mm": None,  # NWS hourly doesn't give mm amounts
            "precip_prob": hp_precip_prob,
            "wind_kmh": round(ws_mph * 1.60934, 1),
            "wind_dir": hp.get("windDirection"),
            "condition": _nws_condition(hp.get("shortForecast")),
            "uv_index": None,
            "feels_like": None,
        })

    return validate_weather_data(result)


# ---------------------------------------------------------------------------
# Source 7: ECMWF IFS via Open-Meteo (free, no key)
# ---------------------------------------------------------------------------
def _compute_ensemble_stats(member_values):
    """Compute ensemble statistics from a list of member values.

    Returns dict with mean, std, min, max, and percentiles (p10, p25, p75, p90).
    Skips None values. Returns None if fewer than 2 valid members.
    """
    valid = [v for v in member_values if v is not None]
    if len(valid) < 2:
        return None
    valid.sort()
    n = len(valid)
    mean = sum(valid) / n
    variance = sum((x - mean) ** 2 for x in valid) / n
    std = variance ** 0.5

    def percentile(sorted_vals, pct):
        k = (len(sorted_vals) - 1) * pct / 100.0
        f = int(k)
        c = f + 1 if f + 1 < len(sorted_vals) else f
        d = k - f
        return round(sorted_vals[f] + d * (sorted_vals[c] - sorted_vals[f]), 2)

    return {
        "mean": round(mean, 2),
        "std": round(std, 2),
        "min": round(valid[0], 2),
        "max": round(valid[-1], 2),
        "p10": percentile(valid, 10),
        "p25": percentile(valid, 25),
        "p75": percentile(valid, 75),
        "p90": percentile(valid, 90),
        "members": n,
    }


def fetch_ecmwf(lat, lon):
    """Fetch ECMWF IFS forecast via Open-Meteo ECMWF endpoint (free, no key).

    Also attempts the ensemble endpoint to get individual member data for
    spread/confidence statistics.
    """
    # --- Deterministic ECMWF IFS forecast ---
    url = (
        f"https://api.open-meteo.com/v1/ecmwf?"
        f"latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
        f"wind_speed_10m,wind_direction_10m,surface_pressure"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
        f"wind_speed_10m_max,wind_direction_10m_dominant,weather_code"
        f"&hourly=temperature_2m,precipitation,wind_speed_10m,weather_code"
        f"&timezone=auto&forecast_days=7"
    )
    raw = fetch_json(url, timeout=15)
    if not raw:
        return None

    # The ECMWF endpoint may not have "current" — fall back to first hourly
    c = raw.get("current", {})
    hourly = raw.get("hourly", {})
    daily = raw.get("daily", {})

    # If no current block, synthesize from first hourly values
    if not c:
        h_temps = hourly.get("temperature_2m", [])
        h_winds = hourly.get("wind_speed_10m", [])
        h_wcodes = hourly.get("weather_code", [])
        c = {
            "temperature_2m": h_temps[0] if h_temps else None,
            "apparent_temperature": None,
            "relative_humidity_2m": None,
            "wind_speed_10m": h_winds[0] if h_winds else None,
            "wind_direction_10m": None,
            "surface_pressure": None,
        }
        first_wcode = h_wcodes[0] if h_wcodes else None
    else:
        first_wcode = c.get("weather_code")

    result = {
        "source": "ECMWF",
        "current": {
            "temp_c": c.get("temperature_2m"),
            "feels_like_c": c.get("apparent_temperature"),
            "humidity": c.get("relative_humidity_2m"),
            "wind_speed_kmh": c.get("wind_speed_10m"),
            "wind_dir_deg": c.get("wind_direction_10m"),
            "precipitation_mm": c.get("precipitation", 0),
            "pressure_hpa": c.get("surface_pressure"),
            "condition": WMO_CODES.get(first_wcode, "Unknown") if first_wcode is not None else "Unknown",
        },
        "daily": [],
    }

    dates = daily.get("time", [])
    for i, date in enumerate(dates):
        wcode_list = daily.get("weather_code", [])
        wcode = wcode_list[i] if i < len(wcode_list) else None
        day_entry = {
            "date": date,
            "high_c": daily["temperature_2m_max"][i] if i < len(daily.get("temperature_2m_max", [])) else None,
            "low_c": daily["temperature_2m_min"][i] if i < len(daily.get("temperature_2m_min", [])) else None,
            "precip_prob": None,  # ECMWF deterministic doesn't give probability directly
            "precip_mm": daily["precipitation_sum"][i] if i < len(daily.get("precipitation_sum", [])) else None,
            "wind_max_kmh": daily["wind_speed_10m_max"][i] if i < len(daily.get("wind_speed_10m_max", [])) else None,
            "wind_dir_deg": daily["wind_direction_10m_dominant"][i] if i < len(daily.get("wind_direction_10m_dominant", [])) else None,
            "condition": WMO_CODES.get(wcode, "Unknown") if wcode is not None else None,
        }
        result["daily"].append(day_entry)

    # --- Ensemble endpoint (51 members) ---
    ensemble_stats = _fetch_ecmwf_ensemble(lat, lon, dates)
    if ensemble_stats:
        result["ensemble"] = ensemble_stats
        # Enrich daily entries with ensemble spread info
        for day_entry in result["daily"]:
            date = day_entry["date"]
            if date in ensemble_stats:
                es = ensemble_stats[date]
                day_entry["ensemble_temp_spread"] = es.get("temp_std")
                day_entry["ensemble_precip_spread"] = es.get("precip_std")
                # Derive precip probability from ensemble: fraction of members with precip > 0.1mm
                if es.get("precip_prob_from_ensemble") is not None:
                    day_entry["precip_prob"] = es["precip_prob_from_ensemble"]

    return validate_weather_data(result)


def _fetch_ecmwf_ensemble(lat, lon, dates):
    """Fetch ECMWF ensemble data (51 members) and compute per-day statistics.

    Returns dict of {date: {temp_mean, temp_std, precip_mean, precip_std, ...}}.
    Returns None if ensemble endpoint is unavailable.
    """
    url = (
        f"https://ensemble-api.open-meteo.com/v1/ensemble?"
        f"latitude={lat}&longitude={lon}"
        f"&hourly=temperature_2m,precipitation"
        f"&models=ecmwf_ifs025"
        f"&timezone=auto&forecast_days=7"
    )
    raw = fetch_json(url, timeout=20)
    if not raw or "hourly" not in raw:
        return None

    hourly = raw["hourly"]
    times = hourly.get("time", [])

    # Collect ensemble member columns only (temperature_2m_member01 ... member50)
    # Exclude the deterministic/control run (exact key "temperature_2m" / "precipitation")
    temp_members = {}
    precip_members = {}
    for key, values in hourly.items():
        if key.startswith("temperature_2m_member"):
            temp_members[key] = values
        elif key.startswith("precipitation_member"):
            precip_members[key] = values

    if not temp_members:
        return None

    # Aggregate by date
    daily_stats = {}
    for date in (dates or []):
        # Find hourly indices for this date
        day_temps = []  # list of daily-max temps per member
        day_precips = []  # list of daily-sum precips per member

        for member_key, member_vals in temp_members.items():
            member_day_temps = []
            for i, t in enumerate(times):
                if i < len(member_vals) and t[:10] == date and member_vals[i] is not None:
                    member_day_temps.append(member_vals[i])
            if member_day_temps:
                day_temps.append(max(member_day_temps))  # daily max for this member

        for member_key, member_vals in precip_members.items():
            member_day_precips = []
            for i, t in enumerate(times):
                if i < len(member_vals) and t[:10] == date and member_vals[i] is not None:
                    member_day_precips.append(member_vals[i])
            if member_day_precips:
                day_precips.append(sum(member_day_precips))  # daily total for this member

        temp_stats = _compute_ensemble_stats(day_temps)
        precip_stats = _compute_ensemble_stats(day_precips)

        # Compute precip probability: fraction of members with > 0.1mm
        precip_prob = None
        if day_precips:
            wet_members = sum(1 for p in day_precips if p > 0.1)
            precip_prob = round(wet_members / len(day_precips) * 100, 0)

        if temp_stats or precip_stats:
            daily_stats[date] = {
                "temp_mean": temp_stats["mean"] if temp_stats else None,
                "temp_std": temp_stats["std"] if temp_stats else None,
                "temp_p10": temp_stats["p10"] if temp_stats else None,
                "temp_p90": temp_stats["p90"] if temp_stats else None,
                "precip_mean": precip_stats["mean"] if precip_stats else None,
                "precip_std": precip_stats["std"] if precip_stats else None,
                "precip_p10": precip_stats["p10"] if precip_stats else None,
                "precip_p90": precip_stats["p90"] if precip_stats else None,
                "precip_prob_from_ensemble": precip_prob,
                "temp_full": temp_stats,
                "precip_full": precip_stats,
            }

    return daily_stats if daily_stats else None


# ---------------------------------------------------------------------------
# Data validation
# ---------------------------------------------------------------------------
def _valid_or_none(value, low, high):
    """Return value if within [low, high], else None."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if low <= v <= high:
        return v
    return None


def validate_weather_data(result):
    """Validate and sanitize a source result in-place.

    Sets out-of-range values to None rather than storing garbage.
    Ranges:
      temp:         -70 to +60 C
      humidity:     0 to 100 %
      precipitation: >= 0
      wind speed:   0 to 500 km/h
      pressure:     850 to 1090 hPa
    """
    if result is None:
        return None

    cur = result.get("current", {})
    cur["temp_c"] = _valid_or_none(cur.get("temp_c"), -70, 60)
    cur["feels_like_c"] = _valid_or_none(cur.get("feels_like_c"), -70, 60)
    cur["humidity"] = _valid_or_none(cur.get("humidity"), 0, 100)
    cur["precipitation_mm"] = _valid_or_none(cur.get("precipitation_mm"), 0, float("inf"))
    cur["wind_speed_kmh"] = _valid_or_none(cur.get("wind_speed_kmh"), 0, 500)
    cur["pressure_hpa"] = _valid_or_none(cur.get("pressure_hpa"), 550, 1090)

    for day in result.get("daily", []):
        day["high_c"] = _valid_or_none(day.get("high_c"), -70, 60)
        day["low_c"] = _valid_or_none(day.get("low_c"), -70, 60)
        day["precip_mm"] = _valid_or_none(day.get("precip_mm"), 0, float("inf"))
        day["wind_max_kmh"] = _valid_or_none(day.get("wind_max_kmh"), 0, 500)

    return result


# ---------------------------------------------------------------------------
# Sea surface temperature (Open-Meteo Marine API)
# ---------------------------------------------------------------------------
def fetch_sea_temperature(lat, lon):
    """Fetch current sea surface temperature from Open-Meteo Marine API.

    Returns dict with sea_temp_c or None if location is not near coast.
    The Marine API returns null for inland locations, so this is self-filtering.
    """
    url = (
        f"https://marine-api.open-meteo.com/v1/marine?"
        f"latitude={lat}&longitude={lon}"
        f"&current=sea_surface_temperature"
        f"&daily=sea_surface_temperature_max"
        f"&timezone=auto&forecast_days=7"
    )
    raw = fetch_json(url, timeout=10, retries=1)
    if not raw:
        return None

    current = raw.get("current", {})
    sea_temp = current.get("sea_surface_temperature")
    if sea_temp is None:
        return None

    daily = raw.get("daily", {})
    daily_temps = []
    dates = daily.get("time", [])
    max_temps = daily.get("sea_surface_temperature_max", [])
    for i, date in enumerate(dates):
        if i < len(max_temps) and max_temps[i] is not None:
            daily_temps.append({"date": date, "sea_temp_c": max_temps[i]})

    return {
        "sea_temp_c": round(sea_temp, 1),
        "daily_sea_temps": daily_temps,
    }


# ---------------------------------------------------------------------------
# Consensus builder
# ---------------------------------------------------------------------------
def c_to_f(c):
    """Celsius to Fahrenheit."""
    if c is None:
        return None
    return round(c * 9 / 5 + 32, 1)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Multi-source weather analyst")
    parser.add_argument("city", help="City name to look up")
    parser.add_argument("--scheduled", action="store_true", help="Compact briefing mode")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    try:
        from weighted_forecast import produce_forecast, format_text as wf_text, format_compact
    except ImportError:
        print("ERROR: weighted_forecast.py not found. Run from the scripts/ directory.")
        sys.exit(1)

    forecast = produce_forecast(args.city)
    if "error" in forecast:
        print(f"ERROR: {forecast['error']}")
        sys.exit(1)
    if args.json:
        print(json.dumps(forecast, indent=2, default=str))
    elif args.scheduled:
        print(format_compact(forecast))
    else:
        print(wf_text(forecast))


if __name__ == "__main__":
    main()
