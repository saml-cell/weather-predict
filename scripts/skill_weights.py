#!/usr/bin/env python3
"""Inverse-MAE weights from verification against SHMÚ station observations.

A source is weighted by ``1 / (MAE + epsilon)`` for one variable and lead
bucket. Two or more sources need at least ``min_samples`` overlapping
cases. Otherwise every active source gets an equal weight. A source that
has no history of its own keeps the average skilled weight, so it is not
punished or preferred, and the set is renormalised to 1.

The committed table is ``reports/skill_weights.json``. Regenerating it
downloads SHMÚ validated daily climate and recent hourly station samples.
"""

from __future__ import annotations

import json
import os

from scoring import inverse_mae_weights

MIN_SAMPLES = 7
EPSILON = 0.1
VARIABLES = ("temperature_max", "temperature_min", "precipitation")
METRIC_TO_VARIABLE = {
    "temp_high": "temperature_max",
    "temp_low": "temperature_min",
    "precip_mm": "precipitation",
}

_PROJECT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TABLE_PATH = os.path.join(_PROJECT, "reports", "skill_weights.json")
_cached = None
_cached_mtime = None


def lead_bucket(lead_days):
    """Map a forecast lead in days to the buckets we verify."""
    try:
        lead = int(lead_days)
    except (TypeError, ValueError):
        lead = 1
    if lead <= 1:
        return "1"
    if lead <= 3:
        return "2-3"
    return "4-7"


def resolve_weights(table, sources, variable, lead_days, min_samples=None, epsilon=None):
    """Return ``({source: details}, scheme)``.

    ``scheme`` is ``inverse_mae`` or ``equal``. Details always include
    ``weight``, ``mae``, ``n``, ``fallback`` and ``basis``.
    """
    sources = list(dict.fromkeys(sources))
    if not sources:
        return {}, "equal"
    table = table or {}
    minimum = MIN_SAMPLES if min_samples is None else min_samples
    eps = EPSILON if epsilon is None else epsilon
    if table.get("min_samples") is not None and min_samples is None:
        minimum = int(table["min_samples"])
    if table.get("epsilon") is not None and epsilon is None:
        eps = float(table["epsilon"])

    stats = ((table.get("variables") or {}).get(variable) or {})
    bucket = lead_bucket(lead_days)
    if bucket in stats:
        chosen = stats[bucket]
        basis = bucket
    elif "climatology" in stats:
        chosen = stats["climatology"]
        basis = "climatology"
    else:
        chosen = {}
        basis = "equal"

    skilled = {}
    for source in sources:
        row = chosen.get(source) or {}
        mae = row.get("mae")
        count = row.get("n") or 0
        if mae is None or count < minimum:
            continue
        skilled[source] = {"mae": float(mae), "n": int(count)}

    if len(skilled) < 2:
        share = 1.0 / len(sources)
        return {
            source: {
                "weight": share,
                "mae": None,
                "n": 0,
                "fallback": True,
                "basis": "equal",
            }
            for source in sources
        }, "equal"

    inverse = inverse_mae_weights({source: row["mae"] for source, row in skilled.items()}, epsilon=eps)
    mean_weight = sum(inverse.values()) / len(inverse)
    raw = {source: inverse.get(source, mean_weight) for source in sources}
    total = sum(raw.values()) or 1.0
    resolved = {}
    for source in sources:
        row = skilled.get(source)
        resolved[source] = {
            "weight": raw[source] / total,
            "mae": None if row is None else round(row["mae"], 4),
            "n": 0 if row is None else row["n"],
            "fallback": row is None,
            "basis": basis if row is not None else "equal_share",
        }
    return resolved, "inverse_mae"


def load_table(path=None):
    """Load the committed skill table. Missing file means equal weights."""
    global _cached, _cached_mtime
    path = path or TABLE_PATH
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return {}
    if _cached is not None and mtime == _cached_mtime:
        return _cached
    with open(path, encoding="utf-8") as handle:
        _cached = json.load(handle)
    _cached_mtime = mtime
    return _cached


def save_table(table, path=None):
    path = path or TABLE_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(table, handle, indent=2)
        handle.write("\n")
    global _cached, _cached_mtime
    _cached = table
    _cached_mtime = os.path.getmtime(path)


def accumulate_errors(pairs_by_source):
    """MAE and sample count from ``{source: [(forecast, observation), ...]}``."""
    from scoring import mae as mean_abs

    out = {}
    for source, pairs in pairs_by_source.items():
        forecasts = [pair[0] for pair in pairs]
        observations = [pair[1] for pair in pairs]
        error = mean_abs(forecasts, observations)
        count = sum(
            1 for forecast, observation in pairs
            if forecast is not None and observation is not None
        )
        if error is None or count == 0:
            continue
        out[source] = {"mae": round(float(error), 4), "n": count}
    return out


def build_table():
    """Download SHMÚ observations and replayable forecasts, then write the table.

    Climatology uses validated daily climate at Bratislava-Koliba for
    January–August 2026, aligned to the 21:00 and 07:00 windows. Lead buckets
    use the last complete days of Open-Meteo previous runs (lead 1, 2 and 3)
    against hourly samples from the unvalidated minute feed. ALADIN's open
    archive does not cover the validated months, and three recent days are
    below the sample minimum, so SHMÚ stays on the equal-share fallback.
    """
    import urllib.parse
    import urllib.request
    from datetime import date, datetime, timedelta, timezone

    import shmu
    from scoring import align_hourly_to_shmu_days

    station_id = 11813
    stations = shmu.load_stations()
    station = next(row for row in stations if row["ind_kli"] == station_id)
    lat, lon = station["lat"], station["lon"]

    def fetch_json(url):
        req = urllib.request.Request(url, headers={"User-Agent": "WeatherPredict/1.0 skill"})
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read().decode())

    months = [f"2026-{month:02d}" for month in range(1, 9)]
    observed = {
        row["date"]: row
        for row in shmu.fetch_validated_daily(station_id, months)
    }
    start = date.fromisoformat(min(observed)) - timedelta(days=1)
    end = date.fromisoformat(max(observed)) + timedelta(days=1)

    def hourly_model(base, model):
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "hourly": "temperature_2m,precipitation",
            "timezone": "Europe/Bratislava",
        }
        if model:
            params["models"] = model
        url = base + "?" + urllib.parse.urlencode(params)
        block = fetch_json(url).get("hourly") or {}
        return align_hourly_to_shmu_days(
            block.get("time") or [],
            block.get("temperature_2m") or [],
            block.get("precipitation") or [],
        )

    open_meteo = hourly_model(
        "https://historical-forecast-api.open-meteo.com/v1/forecast", "best_match",
    )
    ecmwf = hourly_model(
        "https://historical-forecast-api.open-meteo.com/v1/forecast", "ecmwf_ifs025",
    )
    days = sorted(set(observed) & set(open_meteo) & set(ecmwf))

    def climatology(field, model_rows):
        pairs = {
            "Open-Meteo": [
                (model_rows["Open-Meteo"][day][field], observed[day][{"temp_high_c": "temp_high_c", "temp_low_c": "temp_low_c", "precip_mm": "precip_mm"}[field]])
                for day in days
            ],
            "ECMWF": [
                (model_rows["ECMWF"][day][field], observed[day][{"temp_high_c": "temp_high_c", "temp_low_c": "temp_low_c", "precip_mm": "precip_mm"}[field]])
                for day in days
            ],
        }
        return accumulate_errors(pairs)

    model_rows = {"Open-Meteo": open_meteo, "ECMWF": ecmwf}
    variables = {
        "temperature_max": {"climatology": climatology("temp_high_c", model_rows)},
        "temperature_min": {"climatology": climatology("temp_low_c", model_rows)},
        "precipitation": {"climatology": climatology("precip_mm", model_rows)},
    }

    # Recent lead-specific temperature skill. previous_dayK is the run from K days earlier.
    today = datetime.now(timezone.utc).astimezone(shmu._zone("Europe/Bratislava")).date()
    recent_end = today - timedelta(days=1)
    recent_start = recent_end - timedelta(days=7)
    lead_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(
            ["temperature_2m"]
            + [f"temperature_2m_previous_day{k}" for k in (1, 2, 3)]
        ),
        "timezone": "Europe/Bratislava",
        "start_date": recent_start.isoformat(),
        "end_date": recent_end.isoformat(),
    }
    runs = {}
    for label, model in (("Open-Meteo", None), ("ECMWF", "ecmwf_ifs025")):
        params = dict(lead_params)
        if model:
            params["models"] = model
        url = "https://previous-runs-api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
        runs[label] = fetch_json(url).get("hourly") or {}

    station_hours = _station_hourly_temps(shmu, station_id, recent_start, recent_end)
    variables = _finalize_variables(variables, runs, station_hours, today)
    table = {
        "min_samples": MIN_SAMPLES,
        "epsilon": EPSILON,
        "reference": {
            "station": {"ind_kli": station_id, "name": station["name"], "lat": lat, "lon": lon},
            "climatology": "Validated daily climate 2026-01 through 2026-08, 21:00-21:00 temperature and 07:00-07:00 precipitation.",
            "leads": "Open-Meteo previous_day1 and previous_day2/3 hourly temperature versus unvalidated SHMU minute samples at the matching local hour, last 8 complete days.",
            "shmu_model": "ALADIN has no overlapping validated archive and fewer than 7 recent scored days, so it uses the equal-share fallback.",
        },
        "variables": variables,
    }
    save_table(table)
    return table


def _station_hourly_temps(shmu, ind_kli, start, end):
    """Map ``YYYY-MM-DDTHH`` local time to temperature from the hourly AWS file."""
    from datetime import timedelta
    from urllib.parse import quote, urljoin

    found = {}
    day = start
    while day <= end:
        folder = day.strftime("%Y%m%d")
        try:
            names = shmu.list_directory(urljoin(shmu.OBS_ROOT, folder + "/"))
        except Exception:
            day += timedelta(days=1)
            continue
        wanted = [name for name in names if name.endswith("00-00.json")]
        for name in wanted:
            try:
                payload = shmu.fetch_observation_file(day, name)
            except Exception:
                continue
            rows = [row for row in payload.get("data", []) if row.get("ind_kli") == ind_kli and row.get("t") is not None]
            if not rows:
                continue
            latest = max(rows, key=lambda row: row.get("minuta") or "")
            local = _utc_stamp_to_local_hour(latest.get("minuta"))
            if local:
                found[local] = float(latest["t"])
        day += timedelta(days=1)
    return found


def _utc_stamp_to_local_hour(stamp):
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    if not stamp:
        return None
    try:
        moment = datetime.strptime(str(stamp)[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    local = moment.astimezone(ZoneInfo("Europe/Bratislava"))
    return local.strftime("%Y-%m-%dT%H")


def _finalize_variables(variables, runs, station_hours, today):
    """Score lead 1 and leads 2-3 from the previous-run pairs."""
    groups = {"1": (1,), "2-3": (2, 3)}
    for bucket, leads in groups.items():
        pairs = {label: [] for label in runs}
        days = {label: set() for label in runs}
        for lead in leads:
            key = f"temperature_2m_previous_day{lead}"
            for label, block in runs.items():
                for stamp, value in zip(block.get("time") or [], block.get(key) or []):
                    if not stamp or stamp[:10] >= today.isoformat() or value is None:
                        continue
                    obs = station_hours.get(stamp[:13])
                    if obs is None:
                        continue
                    pairs[label].append((value, obs))
                    days[label].add(stamp[:10])
        stats = accumulate_errors(pairs)
        for source, row in stats.items():
            row["n_hours"] = row["n"]
            row["n"] = len(days[source])
        variables["temperature_max"][bucket] = stats
        variables["temperature_min"][bucket] = stats
    # Drop the temporary lead dicts if the first pass stored them.
    for bucket in ("1", "2-3"):
        block = variables["temperature_max"].get(bucket)
        if isinstance(block, dict) and any(isinstance(v, list) for v in block.values()):
            variables["temperature_max"].pop(bucket, None)
    return variables


if __name__ == "__main__":
    built = build_table()
    for name, block in built["variables"].items():
        print(name, {bucket: {src: row.get("n") for src, row in stats.items()} for bucket, stats in block.items()})
