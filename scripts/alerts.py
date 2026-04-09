#!/usr/bin/env python3
"""
Weather Alert Notification System.

Checks forecasts for extreme conditions and sends alerts via configurable
backends (webhook, file log, or stdout).

Usage:
  python scripts/alerts.py                    # Check all cities
  python scripts/alerts.py --city Bratislava  # Check one city
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
from meteo import heat_index, wind_chill

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Alert thresholds
# ---------------------------------------------------------------------------
THRESHOLDS = {
    "extreme_heat_c": 38,
    "extreme_cold_c": -15,
    "heavy_precip_mm": 30,
    "strong_wind_kmh": 70,
    "high_precip_prob_pct": 90,
    "heat_index_c": 40,
    "wind_chill_c": -25,
    "flood_precip_mm_per_day": 15,
    "flood_consecutive_days": 3,
}

# ---------------------------------------------------------------------------
# Alert detection
# ---------------------------------------------------------------------------
def get_official_alerts(city_name):
    """Fetch official weather service alerts from WeatherAPI for a city.
    Returns a list of alert dicts with type='official'.
    Uses WeatherAPI directly instead of the full forecast pipeline."""
    from fetch_weather import fetch_weatherapi, geocode
    try:
        loc = geocode(city_name)
        if not loc:
            return []
        result = fetch_weatherapi(loc["lat"], loc["lon"])
        if not result:
            return []
        official = []
        for alert in result.get("alerts", []):
            official.append({
                "city": city_name,
                "date": alert.get("effective", ""),
                "type": "official",
                "severity": alert.get("severity"),
                "event": alert.get("event"),
                "headline": alert.get("headline"),
                "message": alert.get("headline") or alert.get("event", "Weather alert"),
                "expires": alert.get("expires"),
                "urgency": alert.get("urgency"),
                "description": alert.get("desc"),
                "instruction": alert.get("instruction"),
                "source": "WeatherAPI",
            })
        return official
    except Exception as e:
        logger.warning("Failed to fetch official alerts for %s: %s", city_name, e)
        return []


def check_city_alerts(city_id, city_name):
    """Check recent forecasts for a city and return list of alert dicts.
    Includes both threshold-based alerts and official weather service alerts."""
    from datetime import date, timedelta
    today = date.today().isoformat()
    end = (date.today() + timedelta(days=3)).isoformat()

    conn = db.get_connection()
    rows = conn.execute("""
        SELECT forecast_date, source_name, temp_high_c, temp_low_c,
               precip_mm, wind_max_kmh, precip_prob, condition_text,
               humidity_pct
        FROM forecasts
        WHERE city_id = ? AND forecast_date >= ? AND forecast_date <= ?
        ORDER BY forecast_date
    """, (city_id, today, end)).fetchall()

    alerts = []
    seen = set()  # deduplicate by (date, type)

    # Track daily precip for multi-day flood alert
    daily_precip = {}  # {date_str: max_precip_mm}

    for row in rows:
        r = dict(row)
        d = r["forecast_date"]

        if r.get("temp_high_c") is not None and r["temp_high_c"] >= THRESHOLDS["extreme_heat_c"]:
            key = (d, "extreme_heat")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "extreme_heat",
                    "message": f"Extreme heat: {r['temp_high_c']}C forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

        if r.get("temp_low_c") is not None and r["temp_low_c"] <= THRESHOLDS["extreme_cold_c"]:
            key = (d, "extreme_cold")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "extreme_cold",
                    "message": f"Extreme cold: {r['temp_low_c']}C forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

        if r.get("precip_mm") is not None and r["precip_mm"] >= THRESHOLDS["heavy_precip_mm"]:
            key = (d, "heavy_precip")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "heavy_precip",
                    "message": f"Heavy precipitation: {r['precip_mm']}mm forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

        if r.get("wind_max_kmh") is not None and r["wind_max_kmh"] >= THRESHOLDS["strong_wind_kmh"]:
            key = (d, "strong_wind")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "strong_wind",
                    "message": f"Strong winds: {r['wind_max_kmh']}km/h forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

        # Heat index alert
        if r.get("temp_high_c") is not None and r.get("humidity_pct") is not None:
            hi = heat_index(r["temp_high_c"], r["humidity_pct"])
            if hi is not None and hi >= THRESHOLDS["heat_index_c"]:
                key = (d, "heat_index")
                if key not in seen:
                    alerts.append({
                        "city": city_name, "date": d, "type": "heat_index",
                        "message": f"Dangerous heat index: {hi}C (temp {r['temp_high_c']}C, humidity {r['humidity_pct']}%)",
                        "source": r["source_name"],
                    })
                    seen.add(key)

        # Wind chill alert
        if r.get("temp_low_c") is not None and r.get("wind_max_kmh") is not None:
            wc = wind_chill(r["temp_low_c"], r["wind_max_kmh"])
            if wc is not None and wc <= THRESHOLDS["wind_chill_c"]:
                key = (d, "wind_chill")
                if key not in seen:
                    alerts.append({
                        "city": city_name, "date": d, "type": "wind_chill",
                        "message": f"Dangerous wind chill: {wc}C (temp {r['temp_low_c']}C, wind {r['wind_max_kmh']}km/h)",
                        "source": r["source_name"],
                    })
                    seen.add(key)

        # Track precip per day for flood risk
        if r.get("precip_mm") is not None:
            daily_precip[d] = max(daily_precip.get(d, 0), r["precip_mm"])

    # Multi-day precipitation accumulation flood risk
    sorted_dates = sorted(daily_precip.keys())
    threshold_per_day = THRESHOLDS["flood_precip_mm_per_day"]
    consec_needed = THRESHOLDS["flood_consecutive_days"]
    consecutive = 0
    consec_total = 0.0
    consec_start = None
    for i, d in enumerate(sorted_dates):
        if daily_precip[d] >= threshold_per_day:
            if consecutive == 0:
                consec_start = d
            consecutive += 1
            consec_total += daily_precip[d]
            if consecutive >= consec_needed:
                key = (consec_start, "flood_risk")
                if key not in seen:
                    alerts.append({
                        "city": city_name, "date": consec_start, "type": "flood_risk",
                        "message": f"Flood risk: {consec_total:.0f}mm over {consecutive} consecutive days ({consec_start} to {d})",
                        "source": "multi-source",
                    })
                    seen.add(key)
        else:
            consecutive = 0
            consec_total = 0.0
            consec_start = None

    # Mark all threshold-based alerts with category "threshold"
    for alert in alerts:
        alert["category"] = "threshold"

    # Append official weather service alerts from WeatherAPI
    official_alerts = get_official_alerts(city_name)
    alerts.extend(official_alerts)

    return alerts


# ---------------------------------------------------------------------------
# Notification backends
# ---------------------------------------------------------------------------
def send_webhook(alerts, webhook_url):
    """Send alerts to a webhook URL (Telegram, Slack, Discord, etc.)."""
    from urllib.request import urlopen, Request
    payload = json.dumps({
        "text": format_alert_text(alerts),
        "alerts": alerts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }).encode()
    try:
        req = Request(webhook_url, data=payload,
                      headers={"Content-Type": "application/json"})
        urlopen(req, timeout=10)
        logger.info("Webhook sent: %d alerts to %s", len(alerts), webhook_url)
    except Exception as e:
        logger.error("Webhook failed: %s", e)


def format_alert_text(alerts):
    """Format alerts as readable text."""
    if not alerts:
        return "No weather alerts."
    lines = [f"Weather Alerts ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})"]
    for a in alerts:
        lines.append(f"  [{a['date']}] {a['city']}: {a['message']}")
    return "\n".join(lines)


def log_alerts_to_file(alerts, path):
    """Append alerts to a log file."""
    with open(path, "a") as f:
        for a in alerts:
            f.write(json.dumps({**a, "checked_at": datetime.now(timezone.utc).isoformat()}) + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Weather alert checker")
    parser.add_argument("--city", help="Check a single city")
    parser.add_argument("--webhook", help="Send alerts to this webhook URL")
    parser.add_argument("--log-file", help="Append alerts to this file")
    args = parser.parse_args()

    cities = db.get_all_cities()
    if args.city:
        cities = [c for c in cities if c["name"].lower() == args.city.lower()]

    all_alerts = []
    for city in cities:
        alerts = check_city_alerts(city["id"], city["name"])
        all_alerts.extend(alerts)

    if all_alerts:
        print(format_alert_text(all_alerts))
        if args.webhook:
            send_webhook(all_alerts, args.webhook)
        if args.log_file:
            log_alerts_to_file(all_alerts, args.log_file)
    else:
        print("No weather alerts for tracked cities.")

    print(f"\nChecked {len(cities)} cities, found {len(all_alerts)} alerts.")


if __name__ == "__main__":
    main()
