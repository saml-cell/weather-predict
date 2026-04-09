#!/usr/bin/env python3
"""
Flask API for the Weather Prediction Dashboard.

Serves both the API endpoints and the static frontend.

Usage:
  python scripts/api.py
  # Dashboard at http://<your-ip>:5000

Environment variables:
  WEATHER_API_KEY   — Required for mutating endpoints (POST/DELETE). No default.
  CORS_ORIGINS      — Comma-separated allowed origins. Default: auto-detect.
  WEATHER_PORT      — Server port. Default: 5000.
  WEATHER_HOST      — Bind address. Default: 0.0.0.0 (all interfaces).
  WEATHER_MAX_BODY  — Max request body in bytes. Default: 1048576 (1MB).
"""

import json
import logging
import os
import re
import secrets
import sys
import time
import threading
import functools
import socket
from collections import defaultdict
from datetime import date, timedelta, datetime, timezone

# Load .env file if present (so WEATHER_API_KEY etc. work without manual export)
_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.isfile(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _, _val = _line.partition("=")
                _key = _key.strip()
                _val = _val.strip()
                if _key and _key not in os.environ:  # don't override existing
                    os.environ[_key] = _val

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from db import normalize_condition
from fetch_weather import geocode
from weighted_forecast import produce_forecast
from meteo import dew_point, dew_point_depression, pressure_stability_index
from seasonal_forecast import produce_seasonal_forecast, format_json_output
from climate_indices import get_current_index_state, fetch_all_indices, build_climatology
from alerts import check_city_alerts

_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_config_file_lock = threading.Lock()
_DASHBOARD_DIR = os.path.join(_PROJECT_DIR, "dashboard")
_PORT = int(os.environ.get("WEATHER_PORT", 5000))
_HOST = os.environ.get("WEATHER_HOST", "0.0.0.0")

app = Flask(__name__, static_folder=_DASHBOARD_DIR, static_url_path="")
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("WEATHER_MAX_BODY", 1048576))

# ---------------------------------------------------------------------------
# CORS: auto-detect all local IPs so the dashboard works from any address
# ---------------------------------------------------------------------------
def _build_cors_origins():
    """Build CORS origin list. If CORS_ORIGINS env var is set, use that.
    Otherwise auto-detect all local IPs so the dashboard works via
    localhost, LAN IP, or Tailscale IP."""
    env_origins = os.environ.get("CORS_ORIGINS", "").strip()
    if env_origins:
        return [o.strip() for o in env_origins.split(",") if o.strip()]

    origins = set()
    origins.add(f"http://localhost:{_PORT}")
    origins.add(f"http://127.0.0.1:{_PORT}")
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            origins.add(f"http://{ip}:{_PORT}")
        # Also try Tailscale / other interfaces via netifaces-like scan
        import subprocess
        result = subprocess.run(
            ["ip", "-4", "-o", "addr", "show"], capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.splitlines():
            parts = line.split()
            for part in parts:
                if "/" in part and re.match(r"\d+\.\d+\.\d+\.\d+/", part):
                    ip = part.split("/")[0]
                    if not ip.startswith("127."):
                        origins.add(f"http://{ip}:{_PORT}")
    except Exception:
        pass
    return list(origins)


_cors_origins = _build_cors_origins()
CORS(app, origins=_cors_origins)

# ---------------------------------------------------------------------------
# API key auth for mutating endpoints
# ---------------------------------------------------------------------------
_API_KEY = os.environ.get("WEATHER_API_KEY", "")

if not _API_KEY:
    logger.warning(
        "WEATHER_API_KEY not set — mutating endpoints (POST/DELETE) are UNPROTECTED. "
        "Set this env var before exposing to a network."
    )


def require_api_key(f):
    """Decorator: require valid API key via X-API-Key header.
    Disabled if WEATHER_API_KEY env var is not set (development mode).
    """
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not _API_KEY:
            return f(*args, **kwargs)
        key = request.headers.get("X-API-Key") or request.args.get("api_key")
        if not key or not secrets.compare_digest(key, _API_KEY):
            logger.warning("Unauthorized %s %s from %s",
                           request.method, request.path, request.remote_addr)
            return jsonify({"error": "Unauthorized. Provide X-API-Key header."}), 401
        return f(*args, **kwargs)
    return decorated

# ---------------------------------------------------------------------------
# Rate limiter (in-memory, per-IP)
# ---------------------------------------------------------------------------
_rate_limit_store = defaultdict(list)  # "bucket:ip" -> [timestamps]
_rate_limit_call_count = 0

_RATE_LIMITS = {
    "add_city": {"max": 5, "window": 3600},       # 5 cities/hour
    "api_read": {"max": 120, "window": 60},        # 120 reads/minute
    "api_write": {"max": 20, "window": 60},        # 20 writes/minute
}


def _check_rate_limit(ip, bucket="add_city"):
    """Return True if request is allowed, False if rate-limited."""
    global _rate_limit_call_count
    limits = _RATE_LIMITS.get(bucket, _RATE_LIMITS["api_read"])
    key = f"{bucket}:{ip}"
    now = time.time()
    timestamps = _rate_limit_store[key]
    _rate_limit_store[key] = [t for t in timestamps if now - t < limits["window"]]
    if len(_rate_limit_store[key]) >= limits["max"]:
        return False
    _rate_limit_store[key].append(now)

    _rate_limit_call_count += 1
    if _rate_limit_call_count % 100 == 0:
        max_window = max(r["window"] for r in _RATE_LIMITS.values())
        stale_keys = [k for k, v in _rate_limit_store.items()
                      if not v or now - v[-1] > max_window]
        for k in stale_keys:
            del _rate_limit_store[k]

    return True

# ---------------------------------------------------------------------------
# Forecast cache (in-memory TTL=30min)
# ---------------------------------------------------------------------------
_forecast_cache = {}  # city_id -> (timestamp, result)
_FORECAST_TTL = 1800  # 30 minutes


def invalidate_forecast_cache(city_id=None):
    """Clear forecast cache. If city_id given, only that city; else all."""
    if city_id is not None:
        _forecast_cache.pop(city_id, None)
        logger.info("Cache invalidated for city_id=%s", city_id)
    else:
        _forecast_cache.clear()
        logger.info("All forecast cache cleared")

# ---------------------------------------------------------------------------
# Static frontend (path-traversal safe via send_from_directory)
# ---------------------------------------------------------------------------
_STATUS_DIR = os.path.join(os.path.expanduser("~"), ".openclaw", "workspace", "dashboard")

# Allowed static file extensions — reject everything else
_STATIC_EXTS = {".html", ".css", ".js", ".json", ".png", ".jpg", ".jpeg",
                ".gif", ".svg", ".ico", ".woff", ".woff2", ".ttf", ".map"}


@app.route("/")
def serve_index():
    return send_from_directory(_DASHBOARD_DIR, "index.html")


@app.route("/status")
def serve_status():
    return send_from_directory(_STATUS_DIR, "status.html")


@app.route("/<path:path>")
def serve_static(path):
    # Block path traversal attempts
    if ".." in path or path.startswith("/"):
        abort(404)
    ext = os.path.splitext(path)[1].lower()
    if ext in _STATIC_EXTS:
        file_path = os.path.join(_DASHBOARD_DIR, path)
        if os.path.isfile(file_path):
            return send_from_directory(_DASHBOARD_DIR, path)
    # SPA fallback for client-side routes
    return send_from_directory(_DASHBOARD_DIR, "index.html")

# ---------------------------------------------------------------------------
# API: Cities
# ---------------------------------------------------------------------------
@app.route("/api/cities", methods=["GET"])
def get_cities():
    cities = db.get_all_cities()
    return jsonify(cities)

@app.route("/api/cities", methods=["POST"])
@require_api_key
def add_city():
    if not _check_rate_limit(request.remote_addr, "add_city"):
        return jsonify({"error": "Rate limit exceeded. Max 5 cities per hour."}), 429

    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400
    city_name = data.get("name", "").strip()
    if not city_name:
        return jsonify({"error": "City name required"}), 400
    # Sanitize: letters, spaces, hyphens, apostrophes, periods only. Max 100 chars.
    if len(city_name) > 100 or not re.match(r"^[\w\s\-'.À-ÿ]+$", city_name):
        return jsonify({"error": "Invalid city name"}), 400

    location = geocode(city_name)
    if not location:
        return jsonify({"error": f"Could not find city: {city_name}"}), 404

    # Check if already tracked
    existing = db.get_city(location["name"])
    if existing:
        return jsonify({**existing, "already_tracked": True}), 200

    city_id = db.insert_city(
        name=location["name"],
        country=location["country"],
        lat=location["lat"],
        lon=location["lon"],
        timezone=location.get("timezone", "auto"),
    )
    city = db.get_city_by_id(city_id)

    # Update config.json default_cities list
    _add_city_to_config(location["name"])

    # Trigger background initial fetch
    import threading
    def _initial_fetch(cid, city_dict):
        try:
            from collect_forecasts import fetch_all_sources, store_forecasts
            from datetime import datetime, timezone as tz
            logger.info("Initial fetch for new city: %s", city_dict.get("name"))
            results = fetch_all_sources(city_dict)
            store_forecasts(cid, results, datetime.now(tz.utc).isoformat())
            logger.info("Initial fetch done for %s: %d sources", city_dict.get("name"), len(results))
        except Exception as e:
            logger.error("Initial fetch failed for %s: %s", city_dict.get("name"), e)
    threading.Thread(target=_initial_fetch, args=(city_id, dict(city)), daemon=True).start()

    logger.info("New city added: %s (%s) id=%d", location["name"], location["country"], city_id)
    return jsonify({**city, "just_added": True}), 201


def _add_city_to_config(city_name):
    """Add city to config.json default_cities if not already there."""
    try:
        config_path = os.path.join(_PROJECT_DIR, "config.json")
        with _config_file_lock:
            with open(config_path) as f:
                config = json.load(f)
            cities = config.get("default_cities", [])
            if city_name not in cities:
                cities.append(city_name)
                config["default_cities"] = cities
                with open(config_path, "w") as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                logger.info("Added %s to config.json default_cities", city_name)
    except Exception as e:
        logger.error("Failed to update config.json: %s", e)


@app.route("/api/cities/<int:city_id>", methods=["DELETE"])
@require_api_key
def remove_city(city_id):
    """Remove a city from tracking."""
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    conn = db.get_connection()
    conn.execute("DELETE FROM forecasts WHERE city_id = ?", (city_id,))
    conn.execute("DELETE FROM observations WHERE city_id = ?", (city_id,))
    conn.execute("DELETE FROM source_accuracy WHERE city_id = ?", (city_id,))
    conn.execute("DELETE FROM climatology WHERE city_id = ?", (city_id,))
    conn.execute("DELETE FROM seasonal_forecasts WHERE city_id = ?", (city_id,))
    conn.execute("DELETE FROM seasonal_skill WHERE city_id = ?", (city_id,))
    conn.execute("DELETE FROM cities WHERE id = ?", (city_id,))
    conn.commit()

    # Remove from config.json
    try:
        config_path = os.path.join(_PROJECT_DIR, "config.json")
        with _config_file_lock:
            with open(config_path) as f:
                config = json.load(f)
            cities = config.get("default_cities", [])
            if city["name"] in cities:
                cities.remove(city["name"])
                config["default_cities"] = cities
                with open(config_path, "w") as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

    invalidate_forecast_cache(city_id)
    logger.info("Removed city: %s (id=%d)", city["name"], city_id)
    return jsonify({"status": "removed", "city": city})

# ---------------------------------------------------------------------------
# API: Forecast
# ---------------------------------------------------------------------------
@app.route("/api/forecast/<int:city_id>", methods=["GET"])
def get_forecast(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    cached = _forecast_cache.get(city_id)
    if cached and (time.time() - cached[0]) < _FORECAST_TTL:
        return jsonify(cached[1])

    forecast = produce_forecast(city["name"])
    if "error" in forecast:
        return jsonify(forecast), 500

    _forecast_cache[city_id] = (time.time(), forecast)
    return jsonify(forecast)

# ---------------------------------------------------------------------------
# API: Hourly forecast (lightweight, for animated map)
# ---------------------------------------------------------------------------
@app.route("/api/hourly/<int:city_id>", methods=["GET"])
def get_hourly(city_id):
    city = db.get_city_by_id(city_id)
    if not city:
        return jsonify({"error": "City not found"}), 404

    # Use the same cache as full forecast to avoid duplicate API calls
    cached = _forecast_cache.get(city_id)
    if cached and (time.time() - cached[0]) < _FORECAST_TTL:
        forecast = cached[1]
    else:
        forecast = produce_forecast(city["name"])
        if "error" in forecast:
            return jsonify(forecast), 500
        _forecast_cache[city_id] = (time.time(), forecast)

    return jsonify({
        "city_id": city["id"],
        "name": city["name"],
        "lat": city["lat"],
        "lon": city["lon"],
        "hourly": forecast.get("hourly", []),
    })

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
        logger.exception("Seasonal forecast error for city_id=%d", city_id)
        return jsonify({"error": "Seasonal forecast failed"}), 500


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
        logger.exception("Indices fetch error")
        return jsonify({"error": "Could not load indices"}), 500


@app.route("/api/seasonal/climatology/<int:city_id>", methods=["POST"])
@require_api_key
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
        logger.exception("Climatology build error for city_id=%d", city_id)
        return jsonify({"error": "Climatology build failed"}), 500


# ---------------------------------------------------------------------------
# API: Alerts
# ---------------------------------------------------------------------------
@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    """Return weather alerts for all cities or a specific city."""
    city_id = request.args.get("city_id", type=int)
    cities = db.get_all_cities()
    if city_id:
        cities = [c for c in cities if c["id"] == city_id]

    all_alerts = []
    for city in cities:
        alerts = check_city_alerts(city["id"], city["name"])
        all_alerts.extend(alerts)

    return jsonify({"alerts": all_alerts, "count": len(all_alerts)})

# ---------------------------------------------------------------------------
# API: Cache invalidation (called after fetch pipeline runs)
# ---------------------------------------------------------------------------
@app.route("/api/cache/invalidate", methods=["POST"])
@require_api_key
def api_invalidate_cache():
    data = request.get_json(silent=True) or {}
    city_name = data.get("city")
    city_id = data.get("city_id")
    if city_name and city_id is None:
        city_obj = db.get_city(city_name)
        city_id = city_obj["id"] if city_obj else None
    invalidate_forecast_cache(city_id)
    return jsonify({"status": "ok", "cleared": city_name or city_id or "all"})

# ---------------------------------------------------------------------------
# Request logging, rate limiting, security headers
# ---------------------------------------------------------------------------
@app.before_request
def _before_request():
    request._start_time = time.time()
    # Global rate limit on API endpoints
    if request.path.startswith("/api/"):
        bucket = "api_write" if request.method in ("POST", "PUT", "DELETE") else "api_read"
        if not _check_rate_limit(request.remote_addr, bucket):
            return jsonify({"error": "Too many requests. Slow down."}), 429


@app.after_request
def _after_request(response):
    # Security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(self)"
    # Remove server header
    response.headers.pop("Server", None)

    # Cache-Control for API GET responses
    if request.method == "GET" and request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "public, max-age=300"

    # Request logging
    elapsed = (time.time() - getattr(request, "_start_time", time.time())) * 1000
    if request.path.startswith("/api/"):
        logger.info("%s %s %d %.0fms %s", request.method, request.path,
                    response.status_code, elapsed, request.remote_addr)
    return response


# ---------------------------------------------------------------------------
# Global error handlers — return JSON, never leak stack traces
# ---------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": "Not found"}), 404
    return send_from_directory(_DASHBOARD_DIR, "index.html")


@app.errorhandler(413)
def payload_too_large(e):
    return jsonify({"error": "Request body too large (max 1MB)"}), 413


@app.errorhandler(429)
def rate_limited(e):
    return jsonify({"error": "Too many requests"}), 429


@app.errorhandler(500)
def internal_error(e):
    logger.exception("Internal server error on %s %s", request.method, request.path)
    return jsonify({"error": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# Health check (detailed)
# ---------------------------------------------------------------------------
@app.route("/api/health", methods=["GET"])
def health_check():
    cities = db.get_all_cities()
    conn = db.get_connection()

    obs_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    forecast_count = conn.execute("SELECT COUNT(*) FROM forecasts").fetchone()[0]
    last_fetch = conn.execute(
        "SELECT MAX(fetched_at) FROM forecasts"
    ).fetchone()[0]

    # DB file size
    db_size_mb = None
    if os.path.exists(db.DB_PATH):
        db_size_mb = round(os.path.getsize(db.DB_PATH) / (1024 * 1024), 2)

    return jsonify({
        "status": "ok",
        "version": "1.1.0",
        "cities": len(cities),
        "observations": obs_count,
        "forecasts": forecast_count,
        "last_fetch": last_fetch,
        "db_size_mb": db_size_mb,
        "cache_entries": len(_forecast_cache),
        "auth_enabled": bool(_API_KEY),
    })

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Serving frontend from: %s", _DASHBOARD_DIR)
    logger.info("Auth: %s", "ENABLED" if _API_KEY else "DISABLED (set WEATHER_API_KEY)")
    logger.info("CORS origins: %s", _cors_origins)
    logger.info("")
    logger.info("Access the dashboard at any of:")
    for origin in sorted(_cors_origins):
        logger.info("  %s", origin)
    logger.info("")
    logger.info("API: <address>/api/cities")
    logger.info("Health: <address>/api/health")
    app.run(host=_HOST, port=_PORT, debug=False)
