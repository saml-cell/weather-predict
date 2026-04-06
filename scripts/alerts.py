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
}

# ---------------------------------------------------------------------------
# Alert detection
# ---------------------------------------------------------------------------
def check_city_alerts(city_id, city_name):
    """Check recent forecasts for a city and return list of alert dicts."""
    from datetime import date, timedelta
    today = date.today().isoformat()
    end = (date.today() + timedelta(days=3)).isoformat()

    conn = db.get_connection()
    rows = conn.execute("""
        SELECT forecast_date, source_name, temp_high_c, temp_low_c,
               precip_mm, wind_max_kmh, precip_prob, condition_text
        FROM forecasts
        WHERE city_id = ? AND forecast_date >= ? AND forecast_date <= ?
        ORDER BY forecast_date
    """, (city_id, today, end)).fetchall()

    alerts = []
    seen = set()  # deduplicate by (date, type)

    for row in rows:
        r = dict(row)
        d = r["forecast_date"]

        if r.get("temp_high_c") and r["temp_high_c"] >= THRESHOLDS["extreme_heat_c"]:
            key = (d, "extreme_heat")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "extreme_heat",
                    "message": f"Extreme heat: {r['temp_high_c']}C forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

        if r.get("temp_low_c") and r["temp_low_c"] <= THRESHOLDS["extreme_cold_c"]:
            key = (d, "extreme_cold")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "extreme_cold",
                    "message": f"Extreme cold: {r['temp_low_c']}C forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

        if r.get("precip_mm") and r["precip_mm"] >= THRESHOLDS["heavy_precip_mm"]:
            key = (d, "heavy_precip")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "heavy_precip",
                    "message": f"Heavy precipitation: {r['precip_mm']}mm forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

        if r.get("wind_max_kmh") and r["wind_max_kmh"] >= THRESHOLDS["strong_wind_kmh"]:
            key = (d, "strong_wind")
            if key not in seen:
                alerts.append({
                    "city": city_name, "date": d, "type": "strong_wind",
                    "message": f"Strong winds: {r['wind_max_kmh']}km/h forecast",
                    "source": r["source_name"],
                })
                seen.add(key)

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
