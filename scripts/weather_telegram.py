#!/usr/bin/env python3
"""
Weather Telegram Bridge — formats forecasts for Telegram delivery.

Designed to integrate with OpenClaw's Telegram bot (@otrok_cigan_bot).
Can be called standalone or from OpenClaw's weather skill.

Usage:
  python3 scripts/weather_telegram.py --city Bratislava
  python3 scripts/weather_telegram.py --all
  python3 scripts/weather_telegram.py --alerts
  python3 scripts/weather_telegram.py --morning        # Morning briefing
  python3 scripts/weather_telegram.py --send --city Bratislava  # Send via Telegram
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError

API_BASE = "http://localhost:5000"

# Telegram config (reads from OpenClaw config)
OPENCLAW_CONFIG = os.path.expanduser("~/.openclaw/openclaw.json")

CONDITION_EMOJI = {
    "clear": "☀️", "partly_cloudy": "⛅", "cloudy": "☁️",
    "rain": "🌧️", "heavy_rain": "🌧️", "drizzle": "🌦️",
    "snow": "🌨️", "thunderstorm": "⛈️", "fog": "🌫️",
    "unknown": "🌡️",
}

DAY_NAMES_SK = {
    0: "Pondelok", 1: "Utorok", 2: "Streda", 3: "Štvrtok",
    4: "Piatok", 5: "Sobota", 6: "Nedeľa",
}

CONFIDENCE_SK = {"HIGH": "VYSOKÁ", "MODERATE": "STREDNÁ", "LOW": "NÍZKA"}


def api_get(path):
    """Fetch JSON from the weather API."""
    try:
        with urlopen(f"{API_BASE}{path}", timeout=10) as resp:
            return json.loads(resp.read().decode())
    except (URLError, TimeoutError, json.JSONDecodeError) as e:
        return None


def get_cities():
    """Get all tracked cities."""
    return api_get("/api/cities") or []


def get_forecast(city_id):
    """Get forecast for a city."""
    return api_get(f"/api/forecast/{city_id}")


def get_alerts():
    """Get all weather alerts."""
    return api_get("/api/alerts")


def format_city_forecast(forecast):
    """Format a single city forecast for Telegram (Slovak)."""
    if not forecast or "error" in forecast:
        return None

    loc = forecast["location"]
    cur = forecast["current"]
    daily = forecast.get("daily", [])

    condition = cur.get("conditions", ["unknown"])[0] if cur.get("conditions") else "unknown"
    # Normalize condition for emoji lookup
    cond_lower = condition.lower()
    emoji = "🌡️"
    for key, em in CONDITION_EMOJI.items():
        if key in cond_lower:
            emoji = em
            break

    conf = CONFIDENCE_SK.get(cur.get("confidence", ""), cur.get("confidence", ""))
    sources = len(forecast.get("sources_used", []))

    lines = [
        f"{emoji} *Počasie — {loc['name']}*",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        f"🌡️ Teraz: *{cur.get('temp_c', '?')}°C* (pocit: {cur.get('feels_like_c', '?')}°C)",
        f"💧 Vlhkosť: {cur.get('humidity_pct', '?')}% | 💨 Vietor: {cur.get('wind_speed_kmh', '?')} km/h",
        f"📊 Tlak: {cur.get('pressure_hpa', '?')} hPa",
    ]

    if cur.get("dew_point_c") is not None:
        lines.append(f"🌫️ Rosný bod: {cur['dew_point_c']}°C")

    lines.append("")

    # Daily forecast (next 5 days)
    for i, day in enumerate(daily[:5]):
        d = day.get("date", "")
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            if i == 0:
                day_label = "Dnes"
            elif i == 1:
                day_label = "Zajtra"
            else:
                day_label = DAY_NAMES_SK.get(dt.weekday(), d)
        except ValueError:
            day_label = d

        high = day.get("weighted_high_c", "?")
        low = day.get("weighted_low_c", "?")
        precip = day.get("adjusted_precip_prob") or day.get("weighted_precip_prob")
        precip_str = f"{precip:.0f}%" if precip is not None else "?"

        cond = day.get("weighted_condition", "unknown")
        day_emoji = CONDITION_EMOJI.get(cond, "📅")

        lines.append(f"{day_emoji} {day_label}: ↑{high}°C ↓{low}°C | Dážď: {precip_str}")

        # Show physics corrections if any
        corrections = day.get("physics_corrections", [])
        if corrections:
            lines.append(f"   ⚡ {corrections[0]}")

    lines.append("")
    lines.append(f"🎯 Dôvera: {conf} ({sources} zdrojov)")

    return "\n".join(lines)


def format_compact(forecast):
    """One-line format for summary lists."""
    if not forecast or "error" in forecast:
        return None

    loc = forecast["location"]
    cur = forecast["current"]
    today = forecast.get("daily", [{}])[0]

    high = today.get("weighted_high_c", "?")
    low = today.get("weighted_low_c", "?")
    precip = today.get("adjusted_precip_prob") or today.get("weighted_precip_prob")
    precip_str = f"{precip:.0f}%" if precip is not None else "?"

    return f"📍 {loc['name']}: {cur.get('temp_c', '?')}°C | ↑{high} ↓{low} | 🌧️{precip_str}"


def format_alerts(alerts_data):
    """Format alerts for Telegram."""
    if not alerts_data or not alerts_data.get("alerts"):
        return "✅ Žiadne výstrahy pre sledované mestá."

    alerts = alerts_data["alerts"]
    lines = [f"⚠️ *Výstrahy počasia* ({len(alerts)})", "━━━━━━━━━━━━━━━━━━━━", ""]

    for a in alerts:
        type_emoji = {
            "extreme_heat": "🔥", "extreme_cold": "🥶",
            "heavy_precip": "🌊", "strong_wind": "💨",
        }.get(a.get("type", ""), "⚠️")
        lines.append(f"{type_emoji} [{a['date']}] {a['city']}: {a['message']}")

    return "\n".join(lines)


def format_morning_briefing():
    """Morning briefing — Bratislava + alerts + all cities summary."""
    now = datetime.now(timezone.utc)

    lines = [
        f"☀️ *Ranný prehľad počasia*",
        f"📅 {now.strftime('%d.%m.%Y')}",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # Bratislava detailed forecast
    cities = get_cities()
    ba_city = next((c for c in cities if "bratislava" in c["name"].lower()), None)

    if ba_city:
        fc = get_forecast(ba_city["id"])
        detail = format_city_forecast(fc)
        if detail:
            lines.append(detail)
            lines.append("")

    # Other cities compact
    other_cities = [c for c in cities if ba_city and c["id"] != ba_city["id"]]
    if other_cities:
        lines.append("📋 *Ostatné mestá:*")
        for city in other_cities:
            fc = get_forecast(city["id"])
            compact = format_compact(fc)
            if compact:
                lines.append(compact)
        lines.append("")

    # Alerts
    alerts_data = get_alerts()
    if alerts_data and alerts_data.get("alerts"):
        lines.append(format_alerts(alerts_data))
    else:
        lines.append("✅ Žiadne výstrahy.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Telegram sending
# ---------------------------------------------------------------------------
def load_telegram_config():
    """Load Telegram bot token and chat ID from OpenClaw config."""
    try:
        with open(OPENCLAW_CONFIG) as f:
            cfg = json.load(f)
        channels = cfg.get("channels", {}).get("telegram", {})
        token = channels.get("botToken")
        allow_from = channels.get("allowFrom", [])
        chat_id = allow_from[0] if allow_from else None
        return token, chat_id
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None, None


def send_telegram(text, parse_mode="Markdown"):
    """Send a message via OpenClaw's Telegram bot."""
    token, chat_id = load_telegram_config()
    if not token or not chat_id:
        print("ERROR: Could not load Telegram config from OpenClaw")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }).encode()

    try:
        req = Request(url, data=payload, headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                print(f"Sent to Telegram (chat {chat_id})")
                return True
            else:
                print(f"Telegram error: {result}")
                return False
    except Exception as e:
        print(f"Telegram send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Weather Telegram Bridge")
    parser.add_argument("--city", help="Forecast for a specific city")
    parser.add_argument("--all", action="store_true", help="Summary of all cities")
    parser.add_argument("--alerts", action="store_true", help="Show alerts only")
    parser.add_argument("--morning", action="store_true", help="Full morning briefing")
    parser.add_argument("--send", action="store_true",
                        help="Send to Telegram (via OpenClaw bot)")
    args = parser.parse_args()

    # Check API is running
    health = api_get("/api/health")
    if not health:
        msg = "❌ Weather API nie je dostupné (http://localhost:5000)"
        print(msg)
        if args.send:
            send_telegram(msg)
        sys.exit(1)

    # Generate output
    if args.morning:
        output = format_morning_briefing()
    elif args.alerts:
        alerts_data = get_alerts()
        output = format_alerts(alerts_data)
    elif args.all:
        cities = get_cities()
        parts = []
        for city in cities:
            fc = get_forecast(city["id"])
            compact = format_compact(fc)
            if compact:
                parts.append(compact)
        output = "🌍 *Počasie — všetky mestá*\n━━━━━━━━━━━━━━━━━━━━\n\n" + "\n".join(parts)
    elif args.city:
        cities = get_cities()
        match = next((c for c in cities if args.city.lower() in c["name"].lower()), None)
        if not match:
            output = f"❌ Mesto '{args.city}' nie je sledované."
        else:
            fc = get_forecast(match["id"])
            output = format_city_forecast(fc) or f"❌ Nemôžem získať predpoveď pre {args.city}"
    else:
        # Default: Bratislava
        cities = get_cities()
        ba = next((c for c in cities if "bratislava" in c["name"].lower()), None)
        if ba:
            fc = get_forecast(ba["id"])
            output = format_city_forecast(fc) or "❌ Predpoveď nie je dostupná"
        else:
            output = "❌ Bratislava nie je v databáze"

    # Output
    print(output)

    if args.send:
        send_telegram(output)


if __name__ == "__main__":
    main()
