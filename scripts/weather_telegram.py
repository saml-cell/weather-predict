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
  python3 scripts/weather_telegram.py --seasonal Bratislava     # 3-month outlook
  python3 scripts/weather_telegram.py --seasonal Bratislava --months 6
  python3 scripts/weather_telegram.py --indices        # Climate index state
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

MONTH_NAMES_SK = [
    "", "Január", "Február", "Marec", "Apríl", "Máj", "Jún",
    "Júl", "August", "September", "Október", "November", "December",
]

INDEX_NAMES = {
    "oni": "ENSO (ONI)", "nao": "NAO", "ao": "AO", "pdo": "PDO",
    "amo": "AMO", "pna": "PNA", "soi": "SOI", "qbo": "QBO",
    "ea": "East Atlantic", "scand": "Scandinavia", "dmi": "IOD (DMI)",
    "nino34": "NINO 3.4", "wp": "West Pacific", "tnh": "TNH",
    "aao": "AAO/SAM", "tna": "TNA", "tsa": "TSA", "np": "North Pacific",
}

PHASE_SK = {
    "strong_el_nino": "Silný El Niño",
    "moderate_el_nino": "Stredný El Niño",
    "weak_el_nino": "Slabý El Niño",
    "neutral": "Neutrálny",
    "weak_la_nina": "Slabá La Niña",
    "moderate_la_nina": "Stredná La Niña",
    "strong_la_nina": "Silná La Niña",
    "positive": "Pozitívny",
    "negative": "Negatívny",
    "warm": "Teplý",
    "cool": "Studený",
    "westerly": "Západný",
    "easterly": "Východný",
    "unknown": "Neznámy",
}

TERCILE_EMOJI = {"below": "🔵", "near": "⚪", "above": "🔴"}


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


def get_seasonal(city_id, months=3):
    """Get seasonal forecast for a city."""
    return api_get(f"/api/seasonal/{city_id}?months={months}")


def get_indices():
    """Get current climate index state."""
    return api_get("/api/indices")


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


def format_seasonal(seasonal_data, city_name):
    """Format seasonal forecast for Telegram (Slovak)."""
    if not seasonal_data or "error" in seasonal_data:
        return f"❌ Sezónna predpoveď pre {city_name} nie je dostupná."

    forecasts = seasonal_data.get("monthly_forecasts", [])
    index_state = seasonal_data.get("index_state", {})
    months_ahead = seasonal_data.get("months_ahead", 3)

    lines = [
        f"📅 *Sezónna predpoveď — {city_name}*",
        f"🔮 Výhľad na {months_ahead} mesiacov",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # Key climate drivers
    drivers = []
    oni = index_state.get("oni", {})
    if oni and oni.get("phase", "neutral") != "neutral":
        phase_sk = PHASE_SK.get(oni["phase"], oni["phase"])
        drivers.append(f"ENSO: {phase_sk} ({oni['value']:+.1f})")

    nao = index_state.get("nao", {})
    if nao and nao.get("phase", "neutral") != "neutral":
        phase_sk = PHASE_SK.get(nao["phase"], nao["phase"])
        drivers.append(f"NAO: {phase_sk} ({nao['value']:+.1f})")

    ao = index_state.get("ao", {})
    if ao and ao.get("phase", "neutral") != "neutral":
        phase_sk = PHASE_SK.get(ao["phase"], ao["phase"])
        drivers.append(f"AO: {phase_sk} ({ao['value']:+.1f})")

    pdo = index_state.get("pdo", {})
    if pdo and pdo.get("phase", "neutral") != "neutral":
        phase_sk = PHASE_SK.get(pdo["phase"], pdo["phase"])
        drivers.append(f"PDO: {phase_sk}")

    if drivers:
        lines.append("🌊 *Hlavné klimatické signály:*")
        for d in drivers:
            lines.append(f"  • {d}")
        lines.append("")

    # Monthly table
    lines.append("📊 *Mesačný výhľad:*")
    lines.append("")

    for fc in forecasts:
        m = fc.get("target_month", 0)
        y = fc.get("target_year", 0)
        month_name = MONTH_NAMES_SK[m] if 1 <= m <= 12 else str(m)

        temp_anom = fc.get("temp_anomaly_c")
        precip_anom = fc.get("precip_anomaly_pct")
        conf = fc.get("confidence", 0)

        # Temperature anomaly with emoji
        if temp_anom is not None:
            if temp_anom > 0.5:
                temp_emoji = "🔴"
                temp_word = "teplejší"
            elif temp_anom < -0.5:
                temp_emoji = "🔵"
                temp_word = "chladnejší"
            else:
                temp_emoji = "⚪"
                temp_word = "normálny"
            temp_str = f"{temp_emoji} {temp_anom:+.1f}°C ({temp_word})"
        else:
            temp_str = "⚪ N/A"

        # Precipitation anomaly
        if precip_anom is not None:
            if precip_anom > 10:
                precip_emoji = "💧"
                precip_word = "vlhkejší"
            elif precip_anom < -10:
                precip_emoji = "☀️"
                precip_word = "suchší"
            else:
                precip_emoji = "⚪"
                precip_word = "normálny"
            precip_str = f"{precip_emoji} {precip_anom:+.0f}% ({precip_word})"
        else:
            precip_str = "⚪ N/A"

        # Confidence
        if conf >= 0.6:
            conf_str = "🟢 vysoká"
        elif conf >= 0.3:
            conf_str = "🟡 stredná"
        else:
            conf_str = "🔴 nízka"

        lines.append(f"*{month_name} {y}*")
        lines.append(f"  🌡️ Teplota: {temp_str}")
        lines.append(f"  🌧️ Zrážky: {precip_str}")
        lines.append(f"  🎯 Dôvera: {conf_str}")

        # Tercile probabilities
        tp = fc.get("tercile_probs", {})
        bn = tp.get("below_normal", 0.333)
        nn = tp.get("near_normal", 0.334)
        an = tp.get("above_normal", 0.333)
        lines.append(f"  📊 Pod: {bn*100:.0f}% | Norma: {nn*100:.0f}% | Nad: {an*100:.0f}%")

        # Method info
        methods_used = fc.get("methods_used", 0)
        method_weights = fc.get("method_weights", {})
        if method_weights:
            methods_str = ", ".join(f"{k}: {v*100:.0f}%" for k, v in method_weights.items())
            lines.append(f"  🔬 Metódy ({methods_used}): {methods_str}")

        # Individual method details (show notable ones)
        for im in fc.get("individual_methods", []):
            if im.get("error"):
                continue
            method = im.get("method", "")
            if method == "composite" and im.get("phase_description"):
                lines.append(f"  📋 Kompozit: {im['phase_description']}")
                if im.get("sample_years"):
                    years_str = ", ".join(str(y) for y in im["sample_years"][:5])
                    lines.append(f"     Podobné roky: {years_str}...")
            elif method == "ecmwf_seas5" and im.get("temp_anomaly_c") is not None:
                lines.append(f"  🛰️ ECMWF: {im['temp_anomaly_c']:+.1f}°C, zrážky {im.get('precip_anomaly_pct', 0):+.0f}%")

        lines.append("")

    # Analysis summary
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("📝 *Analýza:*")

    analysis_parts = []

    # Temperature trend
    temp_anoms = [fc.get("temp_anomaly_c") for fc in forecasts if fc.get("temp_anomaly_c") is not None]
    if temp_anoms:
        avg_temp = sum(temp_anoms) / len(temp_anoms)
        if avg_temp > 0.5:
            analysis_parts.append(f"Celkový teplotný trend: teplejší ako normál ({avg_temp:+.1f}°C)")
        elif avg_temp < -0.5:
            analysis_parts.append(f"Celkový teplotný trend: chladnejší ako normál ({avg_temp:+.1f}°C)")
        else:
            analysis_parts.append("Teploty blízko normálu")

    # Precipitation trend
    precip_anoms = [fc.get("precip_anomaly_pct") for fc in forecasts if fc.get("precip_anomaly_pct") is not None]
    if precip_anoms:
        avg_precip = sum(precip_anoms) / len(precip_anoms)
        if avg_precip > 10:
            analysis_parts.append(f"Zrážky nadnormálne ({avg_precip:+.0f}%)")
        elif avg_precip < -10:
            analysis_parts.append(f"Zrážky podnormálne ({avg_precip:+.0f}%)")
        else:
            analysis_parts.append("Zrážky blízko normálu")

    # ENSO context
    if oni and oni.get("phase", "neutral") != "neutral":
        phase_sk = PHASE_SK.get(oni["phase"], oni["phase"])
        analysis_parts.append(f"ENSO v fáze {phase_sk} — ovplyvňuje globálnu cirkuláciu")

    # NAO for Europe
    if nao and nao.get("value") is not None:
        val = nao["value"]
        if val > 1.0:
            analysis_parts.append("Silné pozitívne NAO — mierne veterné zimy v sev. Európe")
        elif val < -1.0:
            analysis_parts.append("Silné negatívne NAO — blokácie, chladný vzduch nad Európou")

    # AO context
    if ao and ao.get("value") is not None:
        val = ao["value"]
        if val > 1.0:
            analysis_parts.append("Pozitívny AO — silný polárny vortex, mierne stredné šírky")
        elif val < -1.0:
            analysis_parts.append("Negatívny AO — slabý polárny vortex, riziko vpádov studeného vzduchu")

    for part in analysis_parts:
        lines.append(f"  • {part}")

    return "\n".join(lines)


def format_indices(indices_data):
    """Format climate indices state for Telegram (Slovak)."""
    if not indices_data or "error" in indices_data:
        return "❌ Klimatické indexy nie sú dostupné."

    indices = indices_data.get("indices", {})
    last_updated = indices_data.get("last_updated", "?")

    lines = [
        "🌊 *Klimatické telekonekčné indexy*",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # Tier 1 - most important
    lines.append("*Hlavné indexy:*")
    tier1 = ["oni", "nao", "ao", "pdo", "amo", "pna", "soi"]
    for idx_name in tier1:
        info = indices.get(idx_name)
        if not info:
            continue
        name = INDEX_NAMES.get(idx_name, idx_name.upper())
        val = info.get("value", 0)
        phase = PHASE_SK.get(info.get("phase", ""), info.get("phase", ""))
        ym = f"{info.get('year', '?')}-{info.get('month', '?'):02d}" if isinstance(info.get('month'), int) else "?"

        # Signal strength indicator
        abs_val = abs(val)
        if abs_val > 1.5:
            strength = "🔴"
        elif abs_val > 0.5:
            strength = "🟡"
        else:
            strength = "🟢"

        lines.append(f"  {strength} {name}: {val:+.2f} ({phase}) [{ym}]")

    # Tier 2 - secondary
    lines.append("")
    lines.append("*Doplnkové indexy:*")
    tier2 = ["qbo", "ea", "scand", "dmi", "wp"]
    for idx_name in tier2:
        info = indices.get(idx_name)
        if not info:
            continue
        name = INDEX_NAMES.get(idx_name, idx_name.upper())
        val = info.get("value", 0)
        phase = PHASE_SK.get(info.get("phase", ""), info.get("phase", ""))
        lines.append(f"  • {name}: {val:+.2f} ({phase})")

    lines.append("")
    lines.append(f"🕐 Posledná aktualizácia: {last_updated[:16] if last_updated else '?'}")

    return "\n".join(lines)


def _get_university_deadlines():
    """Read UNIVERSITY.md and find upcoming deadlines."""
    uni_path = os.path.expanduser("~/.openclaw/workspace/UNIVERSITY.md")
    if not os.path.exists(uni_path):
        return []

    from datetime import date
    today = date.today()
    deadlines = []

    try:
        with open(uni_path) as f:
            in_table = False
            for line in f:
                line = line.strip()
                if line.startswith("| Due Date"):
                    in_table = True
                    continue
                if in_table and line.startswith("|---"):
                    continue
                if in_table and line.startswith("|"):
                    parts = [p.strip() for p in line.split("|")[1:-1]]
                    if len(parts) >= 4 and parts[0] and parts[3].lower() not in ("done", "submitted", "hotovo"):
                        try:
                            due = date.fromisoformat(parts[0])
                            days_left = (due - today).days
                            if -1 <= days_left <= 7:
                                deadlines.append({
                                    "due": parts[0],
                                    "course": parts[1],
                                    "assignment": parts[2],
                                    "days_left": days_left,
                                })
                        except ValueError:
                            pass
                elif in_table and not line.startswith("|"):
                    in_table = False
    except OSError:
        pass

    return sorted(deadlines, key=lambda d: d["days_left"])


def _get_budget_status():
    """Read model-switcher state for budget info."""
    state_path = os.path.expanduser("~/.openclaw/workspace/state/model-switcher.json")
    try:
        with open(state_path) as f:
            state = json.load(f)
        return {
            "model": state.get("model_name", "?"),
            "budget_pct": state.get("budget_pct", 0),
            "days_left": state.get("days_left", 0),
            "daily_spend": state.get("daily", {}).get("spend", 0),
            "daily_budget": state.get("daily_budget", 0.667),
        }
    except (OSError, json.JSONDecodeError):
        return None


def format_morning_briefing():
    """Combined morning briefing — weather + deadlines + budget in one message."""
    from datetime import date
    now = datetime.now(timezone.utc)
    today = date.today()

    DAY_NAMES = {0: "Pondelok", 1: "Utorok", 2: "Streda", 3: "Štvrtok",
                 4: "Piatok", 5: "Sobota", 6: "Nedeľa"}
    day_name = DAY_NAMES.get(today.weekday(), "")

    lines = [
        f"🦞 *Dobré ráno, Sam!*",
        f"📅 {day_name} {today.strftime('%d.%m.%Y')}",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    # --- UNIVERSITY DEADLINES ---
    deadlines = _get_university_deadlines()
    if deadlines:
        lines.append("")
        lines.append("📚 *Deadliny:*")
        for dl in deadlines:
            if dl["days_left"] <= 0:
                emoji = "🔴"
                when = "DNES!"
            elif dl["days_left"] == 1:
                emoji = "🟠"
                when = "ZAJTRA!"
            elif dl["days_left"] <= 3:
                emoji = "🟡"
                when = f"o {dl['days_left']} dni"
            else:
                emoji = "🔵"
                when = f"o {dl['days_left']} dní"
            lines.append(f"  {emoji} {dl['assignment']} ({dl['course']}) — {when}")

    # --- WEATHER ---
    lines.append("")
    cities = get_cities()
    ba_city = next((c for c in cities if "bratislava" in c["name"].lower()), None)

    if ba_city:
        fc = get_forecast(ba_city["id"])
        if fc and "error" not in fc:
            cur = fc["current"]
            today_fc = fc.get("daily", [{}])[0]
            tmrw_fc = fc["daily"][1] if len(fc.get("daily", [])) > 1 else {}

            high = today_fc.get("weighted_high_c", "?")
            low = today_fc.get("weighted_low_c", "?")
            precip = today_fc.get("adjusted_precip_prob") or today_fc.get("weighted_precip_prob")
            precip_str = f"{precip:.0f}%" if precip is not None else "?"
            cond = today_fc.get("weighted_condition", "?")
            cond_emoji = CONDITION_EMOJI.get(cond, "🌡️")

            lines.append(f"{cond_emoji} *Bratislava:* {cur.get('temp_c', '?')}°C teraz")
            lines.append(f"  Dnes: ↑{high}°C ↓{low}°C | Dážď: {precip_str}")

            if tmrw_fc:
                t_high = tmrw_fc.get("weighted_high_c", "?")
                t_precip = tmrw_fc.get("adjusted_precip_prob") or tmrw_fc.get("weighted_precip_prob")
                t_precip_str = f"{t_precip:.0f}%" if t_precip is not None else "?"
                lines.append(f"  Zajtra: ↑{t_high}°C | Dážď: {t_precip_str}")

    # Other cities (compact, one line)
    other_cities = [c for c in cities if ba_city and c["id"] != ba_city["id"]]
    if other_cities:
        lines.append("")
        other_parts = []
        for city in other_cities[:6]:  # Max 6 to keep it short
            fc = get_forecast(city["id"])
            if fc and "error" not in fc:
                cur = fc["current"]
                name = city["name"]
                if len(name) > 8:
                    name = name[:7] + "."
                other_parts.append(f"{name} {cur.get('temp_c', '?')}°C")
        if other_parts:
            lines.append("🌍 " + " | ".join(other_parts))

    # Alerts
    alerts_data = get_alerts()
    if alerts_data and alerts_data.get("alerts"):
        lines.append("")
        for a in alerts_data["alerts"][:3]:  # Max 3
            type_emoji = {"extreme_heat": "🔥", "extreme_cold": "🥶",
                          "heavy_precip": "🌊", "strong_wind": "💨"}.get(a.get("type", ""), "⚠️")
            lines.append(f"{type_emoji} {a['city']}: {a['message']}")

    # --- BUDGET ---
    budget = _get_budget_status()
    if budget:
        lines.append("")
        b_pct = budget["budget_pct"] * 100
        if b_pct < 10:
            b_emoji = "🔴"
        elif b_pct < 30:
            b_emoji = "🟡"
        else:
            b_emoji = "🟢"
        lines.append(f"{b_emoji} Model: {budget['model']} | Budget: {b_pct:.0f}% | {budget['days_left']:.0f}d left")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

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
    """Send a message via OpenClaw's Telegram bot.
    Falls back to plain text if Markdown fails.
    Splits long messages into chunks (Telegram limit: 4096 chars).
    """
    token, chat_id = load_telegram_config()
    if not token or not chat_id:
        print("ERROR: Could not load Telegram config from OpenClaw")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Split long messages
    chunks = []
    if len(text) > 4000:
        lines = text.split("\n")
        chunk = []
        chunk_len = 0
        for line in lines:
            if chunk_len + len(line) + 1 > 3900 and chunk:
                chunks.append("\n".join(chunk))
                chunk = []
                chunk_len = 0
            chunk.append(line)
            chunk_len += len(line) + 1
        if chunk:
            chunks.append("\n".join(chunk))
    else:
        chunks = [text]

    success = True
    for chunk in chunks:
        payload = json.dumps({
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": parse_mode,
        }).encode()

        try:
            req = Request(url, data=payload, headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode())
                if not result.get("ok"):
                    raise Exception(f"API error: {result}")
        except Exception:
            # Fallback: retry without Markdown parsing
            payload_plain = json.dumps({
                "chat_id": chat_id,
                "text": chunk,
            }).encode()
            try:
                req = Request(url, data=payload_plain, headers={"Content-Type": "application/json"})
                with urlopen(req, timeout=10) as resp:
                    json.loads(resp.read().decode())
            except Exception as e:
                print(f"Telegram send failed: {e}")
                success = False

    if success:
        print(f"Sent to Telegram (chat {chat_id})")
    return success


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Weather Telegram Bridge")
    parser.add_argument("--city", help="Forecast for a specific city")
    parser.add_argument("--all", action="store_true", help="Summary of all cities")
    parser.add_argument("--alerts", action="store_true", help="Show alerts only")
    parser.add_argument("--morning", action="store_true", help="Full morning briefing")
    parser.add_argument("--seasonal", metavar="CITY",
                        help="Seasonal outlook for a city (1-12 months)")
    parser.add_argument("--months", type=int, default=3,
                        help="Months ahead for seasonal forecast (default: 3)")
    parser.add_argument("--indices", action="store_true",
                        help="Show current climate index state")
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
    elif args.seasonal:
        cities = get_cities()
        match = next((c for c in cities if args.seasonal.lower() in c["name"].lower()), None)
        if not match:
            output = f"❌ Mesto '{args.seasonal}' nie je sledované."
        else:
            months = max(1, min(12, args.months))
            data = get_seasonal(match["id"], months)
            output = format_seasonal(data, match["name"])
    elif args.indices:
        data = get_indices()
        output = format_indices(data)
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
