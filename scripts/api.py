#!/usr/bin/env python3
"""
Flask API for the Weather Prediction Dashboard.

Serves both the API endpoints and the static frontend.

Usage:
  python scripts/api.py
  # Dashboard at http://localhost:5000
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, timedelta, datetime, timezone

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from db import normalize_condition
from fetch_weather import geocode
from weighted_forecast import produce_forecast
from meteo import dew_point, dew_point_depression, pressure_stability_index
from seasonal_forecast import produce_seasonal_forecast, format_json_output
from climate_indices import get_current_index_state, fetch_all_indices, build_climatology

_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DASHBOARD_DIR = os.path.join(_PROJECT_DIR, "dashboard")

app = Flask(__name__, static_folder=_DASHBOARD_DIR, static_url_path="")
CORS(app, origins=["http://localhost:5000", "http://127.0.0.1:5000", "http://localhost:*"])

# ---------------------------------------------------------------------------
# Rate limiter (in-memory) — 5 cities per hour per IP
# ---------------------------------------------------------------------------
_rate_limit_store = defaultdict(list)  # ip -> [timestamps]
_RATE_LIMIT_MAX = 5
_RATE_LIMIT_WINDOW = 3600  # 1 hour in seconds

def _check_rate_limit(ip):
    """Return True if request is allowed, False if rate-limited."""
    now = time.time()
    timestamps = _rate_limit_store[ip]
    # Purge old entries
    _rate_limit_store[ip] = [t for t in timestamps if now - t < _RATE_LIMIT_WINDOW]
    if len(_rate_limit_store[ip]) >= _RATE_LIMIT_MAX:
        return False
    _rate_limit_store[ip].append(now)
    return True

# ---------------------------------------------------------------------------
# Forecast cache (in-memory TTL=30min)
# ---------------------------------------------------------------------------
_forecast_cache = {}  # city_name -> (timestamp, result)
_FORECAST_TTL = 1800  # 30 minutes

# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------
@app.route("/")
def serve_index():
    return send_from_directory(_DASHBOARD_DIR, "index.html")

@app.route("/<path:path>")
def serve_static(path):
    file_path = os.path.join(_DASHBOARD_DIR, path)
    if os.path.isfile(file_path):
        return send_from_directory(_DASHBOARD_DIR, path)
    return send_from_directory(_DASHBOARD_DIR, "index.html")

# ---------------------------------------------------------------------------
# API: Cities
# ---------------------------------------------------------------------------
@app.route("/api/cities", methods=["GET"])
def get_cities():
    cities = db.get_all_cities()
    return jsonify(cities)

@app.route("/api/cities", methods=["POST"])
def add_city():
    if not _check_rate_limit(request.remote_addr):
        return jsonify({"error": "Rate limit exceeded. Max 5 cities per hour."}), 429

    data = request.get_json()
    city_name = data.get("name", "").strip()
    if not city_name:
        return jsonify({"error": "City name required"}), 400

    location = geocode(city_name)
    if not location:
        return jsonify({"error": f"Could not find city: {city_name}"}), 404

    city_id = db.insert_city(
        name=location["name"],
        country=location["country"],
        lat=location["lat"],
        lon=location["lon"],
        timezone=location.get("timezone", "auto"),
    )
    city = db.get_city_by_id(city_id)
    return jsonify(city), 201

# ---------------------------------------------------------------------------
# API: Forecast
# ---------------------------------------------------------------------------
@app.route("/api/forecast/<int:city_id>", methods=["GET"])
def get_forecast(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    city_name = city["name"]
    cached = _forecast_cache.get(city_name)
    if cached and (time.time() - cached[0]) < _FORECAST_TTL:
        return jsonify(cached[1])

    forecast = produce_forecast(city_name)
    if "error" in forecast:
        return jsonify(forecast), 500

    _forecast_cache[city_name] = (time.time(), forecast)
    return jsonify(forecast)

# ---------------------------------------------------------------------------
# API: Observations (historical)
# ---------------------------------------------------------------------------
@app.route("/api/observations/<int:city_id>", methods=["GET"])
def get_observations(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    days = max(1, min(365, request.args.get("days", 30, type=int)))
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=days)).isoformat()

    observations = db.get_observations_in_window(city_id, start, end)
    return jsonify({
        "city": city,
        "observations": observations,
        "period": {"start": start, "end": end},
    })

# ---------------------------------------------------------------------------
# API: Source accuracy & weights
# ---------------------------------------------------------------------------
@app.route("/api/accuracy/<int:city_id>", methods=["GET"])
def get_accuracy(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    weights = db.get_weights(city_id)
    return jsonify({
        "city": city,
        "sources": weights,
    })

# ---------------------------------------------------------------------------
# API: Trends (temperature, precip, pressure over time)
# ---------------------------------------------------------------------------
@app.route("/api/trends/<int:city_id>", methods=["GET"])
def get_trends(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    days = max(1, min(365, request.args.get("days", 30, type=int)))
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=days)).isoformat()

    observations = db.get_observations_in_window(city_id, start, end)

    # Build trend arrays
    dates_list = []
    temp_highs = []
    temp_lows = []
    precip_vals = []
    pressure_vals = []
    humidity_vals = []

    for obs in observations:
        dates_list.append(obs["obs_date"])
        temp_highs.append(obs.get("temp_high_c"))
        temp_lows.append(obs.get("temp_low_c"))
        precip_vals.append(obs.get("precip_mm"))
        pressure_vals.append(obs.get("pressure_hpa"))
        humidity_vals.append(obs.get("humidity_pct"))

    # Source accuracy over time — single JOIN query instead of N+1
    source_errors = {}
    conn = db.get_connection()
    rows = conn.execute("""
        SELECT f.source_name, o.obs_date, ABS(f.temp_high_c - o.temp_high_c) AS error
        FROM observations o
        JOIN forecasts f ON f.city_id = o.city_id AND f.forecast_date = o.obs_date
        WHERE o.city_id = ? AND o.obs_date BETWEEN ? AND ?
        AND f.temp_high_c IS NOT NULL AND o.temp_high_c IS NOT NULL
        GROUP BY f.source_name, o.obs_date
    """, (city_id, start, end)).fetchall()

    for row in rows:
        source = row["source_name"]
        if source not in source_errors:
            source_errors[source] = []
        source_errors[source].append({
            "date": row["obs_date"],
            "error": round(row["error"], 2),
        })

    conn.close()

    return jsonify({
        "city": city,
        "dates": dates_list,
        "temp_highs": temp_highs,
        "temp_lows": temp_lows,
        "precip": precip_vals,
        "pressure": pressure_vals,
        "humidity": humidity_vals,
        "source_errors": source_errors,
        "period": {"start": start, "end": end},
    })

# ---------------------------------------------------------------------------
# API: Source comparison for today
# ---------------------------------------------------------------------------
@app.route("/api/compare/<int:city_id>", methods=["GET"])
def get_compare(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    today_str = date.today().isoformat()
    forecasts = db.get_forecasts_for_date(city_id, today_str)
    weights = db.get_weights(city_id)

    return jsonify({
        "city": city,
        "date": today_str,
        "forecasts": forecasts,
        "weights": weights,
    })

# ---------------------------------------------------------------------------
# API: Seasonal Forecast
# ---------------------------------------------------------------------------
@app.route("/api/seasonal/<int:city_id>", methods=["GET"])
def get_seasonal(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    months = request.args.get("months", 3, type=int)
    months = max(1, min(12, months))

    try:
        city_out, forecasts, index_state = produce_seasonal_forecast(
            city["name"], months_ahead=months, force_update=False)

        if not city_out or not forecasts:
            return jsonify({"error": "Could not produce seasonal forecast"}), 500

        result = format_json_output(city_out, forecasts, index_state, months)
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/indices", methods=["GET"])
def get_indices():
    """Return current teleconnection index state."""
    try:
        state = get_current_index_state()
        if not state:
            return jsonify({"error": "No index data. Fetch indices first."}), 404
        return jsonify({
            "indices": state,
            "last_updated": db.get_indices_fetch_time(),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/seasonal/climatology/<int:city_id>", methods=["POST"])
def build_city_climatology(city_id):
    """Build/refresh climatology for a city (can be slow)."""
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    try:
        build_climatology(city["id"], city["lat"], city["lon"])
        clim = db.get_climatology(city["id"])
        return jsonify({
            "city": city,
            "climatology": {str(k): v for k, v in clim.items()},
            "months": len(clim),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Cache-Control for GET responses
# ---------------------------------------------------------------------------
@app.after_request
def add_cache_headers(response):
    if request.method == "GET":
        response.headers["Cache-Control"] = "public, max-age=300"
    return response

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.route("/api/health", methods=["GET"])
def health_check():
    cities = db.get_all_cities()
    return jsonify({"status": "ok", "version": "1.0.0", "cities": len(cities)})

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Dashboard: http://localhost:5000")
    print(f"API: http://localhost:5000/api/cities")
    print(f"Serving frontend from: {_DASHBOARD_DIR}")
    app.run(host="0.0.0.0", port=5000, debug=False)
