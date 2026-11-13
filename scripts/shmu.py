#!/usr/bin/env python3
"""SHMÚ (Slovak Hydrometeorological Institute) open data.

Two public datasets, both CC BY 4.0, no API key:

* ALADIN/SHMU 4.5 km GRIB2 forecasts
  https://opendata.shmu.sk/meteorology/weather/nwp/aladin/
* 1-minute automatic weather station observations
  https://opendata.shmu.sk/meteorology/climate/now/
* Station coordinates from the official climatological-station WFS
  https://arcgis.shmu.sk/

Forecast hours are read at the nearest grid point. Station observations are
matched to the nearest station. Outside Slovakia both calls return quickly
without downloading a model run.

Attribution: Slovak Hydrometeorological Institute (SHMÚ), CC BY 4.0.
https://opendata.shmu.sk/README.txt
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import shutil
import threading
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urljoin
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

try:
    import eccodes
except ImportError:  # pragma: no cover - exercised only when the extra is missing
    eccodes = None

OPENDATA = "https://opendata.shmu.sk"
ALADIN_ROOT = OPENDATA + "/meteorology/weather/nwp/aladin/sk/4.5km/"
OBS_ROOT = OPENDATA + "/meteorology/climate/now/data/"
STATION_WFS = (
    "https://arcgis.shmu.sk/arcgis/services/public/"
    "SietKlimatologickychStanic/MapServer/WFSServer"
)
USER_AGENT = "WeatherPredict/1.0 (forecast ensemble; CC-BY attribution to SHMU)"

# Rough Slovakia box. Points outside it skip SHMU before any download.
_SLOVAKIA_BBOX = {"lat_min": 47.6, "lat_max": 49.7, "lon_min": 16.7, "lon_max": 22.7}
_MAX_GRID_DISTANCE_KM = 30.0
_MAX_STATION_DISTANCE_KM = 35.0

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache", "shmu"
)
_HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)
_GRIB_RE = re.compile(r"al-grib_sk_(\d+)-(\d{8})-(\d{4})-")
_lock = threading.Lock()
_point_mem = {}


def in_slovakia(lat, lon):
    """True when a point is inside the rough SHMU domain box."""
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return False
    box = _SLOVAKIA_BBOX
    return box["lat_min"] <= lat <= box["lat_max"] and box["lon_min"] <= lon <= box["lon_max"]


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in kilometres."""
    radius = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def nearest_station(stations, lat, lon, max_km=_MAX_STATION_DISTANCE_KM):
    """Return the closest station dict, or None when all are farther than max_km."""
    best = None
    best_km = None
    for station in stations:
        try:
            dist = haversine_km(float(lat), float(lon), float(station["lat"]), float(station["lon"]))
        except (KeyError, TypeError, ValueError):
            continue
        if best_km is None or dist < best_km:
            best = station
            best_km = dist
    if best is None or best_km is None or best_km > max_km:
        return None
    chosen = dict(best)
    chosen["distance_km"] = round(best_km, 2)
    return chosen


def condition_from_fields(temp_c, precip_mm, cloud_fraction):
    """Map ALADIN temperature, hourly precip and cloud fraction to a short label."""
    precip = precip_mm if isinstance(precip_mm, (int, float)) else None
    cloud = cloud_fraction if isinstance(cloud_fraction, (int, float)) else None
    if cloud is not None and cloud > 1.5:
        cloud = cloud / 100.0
    if precip is not None and precip >= 0.2:
        if temp_c is not None and temp_c <= 0:
            return "Snow"
        if precip >= 4:
            return "Heavy rain"
        if precip >= 1:
            return "Moderate rain"
        return "Slight rain"
    if cloud is None:
        return "Unknown"
    if cloud >= 0.85:
        return "Overcast"
    if cloud >= 0.5:
        return "Partly cloudy"
    if cloud >= 0.2:
        return "Mainly clear"
    return "Clear sky"


def precip_increments(accumulated_mm):
    """Turn a running precipitation total into non-negative hourly increments.

    ``accumulated_mm`` is a list aligned with forecast steps. None entries are
    treated as a gap: the next present value is the increment over the gap,
    spread is not invented — the whole delta lands on the later step.
    """
    increments = []
    previous = 0.0
    seen = False
    for value in accumulated_mm:
        if value is None:
            increments.append(None)
            continue
        current = float(value)
        if not seen:
            delta = current
            seen = True
        else:
            delta = current - previous
        previous = current
        increments.append(round(max(0.0, delta), 3))
    return increments


def wind_from_components(u_ms, v_ms):
    """Return (speed_kmh, direction_deg) from meteorological U/V components."""
    if u_ms is None or v_ms is None:
        return None, None
    speed = math.hypot(float(u_ms), float(v_ms)) * 3.6
    direction = (270.0 - math.degrees(math.atan2(float(v_ms), float(u_ms)))) % 360.0
    return round(speed, 1), round(direction, 0)


def aggregate_observation_rows(rows):
    """Collapse minute rows for one station into daily max/min/precip.

    Each row is a dict with ``minuta`` (ISO timestamp), ``t`` (°C) and
    ``zra_uhrn`` (millimetres during that minute). The date is the timestamp's
    calendar date, which SHMÚ publishes in local civil time.
    """
    buckets = {}
    for row in rows:
        stamp = row.get("minuta") or row.get("time")
        if not stamp or len(str(stamp)) < 10:
            continue
        day = str(stamp)[:10]
        bucket = buckets.setdefault(day, {"temps": [], "precip": 0.0, "precip_n": 0, "n": 0})
        temp = row.get("t")
        if temp is not None:
            try:
                bucket["temps"].append(float(temp))
            except (TypeError, ValueError):
                pass
        precip = row.get("zra_uhrn")
        if precip is not None:
            try:
                bucket["precip"] += float(precip)
                bucket["precip_n"] += 1
            except (TypeError, ValueError):
                pass
        bucket["n"] += 1
    daily = []
    for day in sorted(buckets):
        bucket = buckets[day]
        temps = bucket["temps"]
        daily.append({
            "date": day,
            "temp_high_c": round(max(temps), 2) if temps else None,
            "temp_low_c": round(min(temps), 2) if temps else None,
            "precip_mm": round(bucket["precip"], 2) if bucket["precip_n"] else None,
            "samples": bucket["n"],
        })
    return daily


def parse_stations_xml(xml_text):
    """Parse an SHMÚ WFS FeatureCollection into station dicts.

    Coordinates are read from ``gml:pos``. With ``srsName=EPSG:4326`` the
    server returns latitude then longitude.
    """
    root = ET.fromstring(xml_text)
    stations = []
    for el in root.iter():
        if _local(el.tag) != "member":
            continue
        code = None
        name = None
        lat = None
        lon = None
        for child in el.iter():
            local = _local(child.tag)
            text = (child.text or "").strip()
            if local == "Meteorologický_kód_objektu":
                code = text
            elif local == "Názov_monitorovacieho_objektu":
                name = text
            elif local == "pos" and text:
                parts = text.split()
                if len(parts) >= 2:
                    lat = float(parts[0])
                    lon = float(parts[1])
        if not code or lat is None or lon is None:
            continue
        try:
            ind = int(float(code))
        except ValueError:
            continue
        if ind <= 0:
            continue
        stations.append({"ind_kli": ind, "name": name or str(ind), "lat": lat, "lon": lon})
    return stations


def list_directory(url):
    """Return decoded href names from an Apache directory index."""
    body = _http_text(url)
    names = []
    for href in _HREF_RE.findall(body):
        if href.startswith("?") or href.startswith("/") or href.startswith(".."):
            continue
        names.append(unquote(href))
    return names


def _local(tag):
    if not isinstance(tag, str):
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _http_text(url, timeout=25):
    req = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _download(url, dest, timeout=40):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    if os.path.isfile(dest) and os.path.getsize(dest) > 0:
        return dest
    tmp = dest + ".part"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=timeout) as resp, open(tmp, "wb") as handle:
            shutil.copyfileobj(resp, handle)
        os.replace(tmp, dest)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return dest


def _cache_path(*parts):
    return os.path.join(_CACHE_DIR, *parts)


def load_stations(force=False):
    """Download and cache the official station list. Returns a list of dicts."""
    dest = _cache_path("stations.json")
    if not force and os.path.isfile(dest):
        age = datetime.now(timezone.utc).timestamp() - os.path.getmtime(dest)
        if age < 7 * 86400:
            with open(dest, encoding="utf-8") as handle:
                return json.load(handle)
    query = (
        "?service=WFS&version=2.0.0&request=GetFeature"
        "&typeNames=" + quote("SietKlimatologickychStanic:Klimatologická_stanica")
        + "&count=500&srsName=EPSG:4326"
    )
    xml_text = _http_text(STATION_WFS + query, timeout=60)
    stations = parse_stations_xml(xml_text)
    if not stations:
        raise RuntimeError("SHMU station catalogue was empty")
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(dest, "w", encoding="utf-8") as handle:
        json.dump(stations, handle)
    return stations


def latest_observation_name(day=None):
    """Filename of the newest minute file for a UTC/local date folder."""
    day = day or datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Bratislava")).date()
    folder = day.strftime("%Y%m%d")
    names = [n for n in list_directory(urljoin(OBS_ROOT, folder + "/")) if n.endswith(".json")]
    if not names:
        return None
    return sorted(names)[-1]


def fetch_observation_file(day, filename):
    folder = day.strftime("%Y%m%d")
    url = urljoin(OBS_ROOT, folder + "/" + quote(filename))
    return json.loads(_http_text(url, timeout=40))


def station_row_from_payload(payload, ind_kli):
    """Return the newest minute row for one station inside an AWS file."""
    rows = [r for r in payload.get("data", []) if r.get("ind_kli") == ind_kli and r.get("t") is not None]
    if not rows:
        return None
    return max(rows, key=lambda r: r.get("minuta") or "")


def observation_to_current(row, station):
    """Map one AWS minute row onto the shared current-conditions shape."""
    wind_ms = row.get("vie_pr_rych")
    wind_kmh = round(float(wind_ms) * 3.6, 1) if wind_ms is not None else None
    precip = row.get("zra_uhrn")
    temp = row.get("t")
    condition = "Unknown"
    if precip is not None and float(precip) > 0:
        condition = "Snow" if temp is not None and float(temp) <= 0 else "Rain"
    return {
        "temp_c": float(temp) if temp is not None else None,
        "feels_like_c": None,
        "humidity": float(row["vlh_rel"]) if row.get("vlh_rel") is not None else None,
        "wind_speed_kmh": wind_kmh,
        "wind_dir_deg": row.get("vie_vp_smer"),
        "precipitation_mm": float(precip) if precip is not None else None,
        "pressure_hpa": float(row["tlak"]) if row.get("tlak") is not None else None,
        "condition": condition,
        "observed_at": row.get("minuta"),
        "station": {
            "id": station.get("ind_kli"),
            "name": station.get("name"),
            "distance_km": station.get("distance_km"),
            "latitude": station.get("lat"),
            "longitude": station.get("lon"),
        },
    }


def fetch_shmu_observation(lat, lon):
    """Latest SHMÚ station observation near a point, or an unavailable marker."""
    if not in_slovakia(lat, lon):
        return {"unavailable": "out_of_domain"}
    try:
        stations = load_stations()
        station = nearest_station(stations, lat, lon)
        if station is None:
            return {"unavailable": "no_station_nearby"}
        today = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Bratislava")).date()
        filename = latest_observation_name(today)
        if filename is None:
            yesterday = today - timedelta(days=1)
            filename = latest_observation_name(yesterday)
            today = yesterday
        if filename is None:
            return None
        payload = fetch_observation_file(today, filename)
        row = station_row_from_payload(payload, station["ind_kli"])
        if row is None:
            return {"unavailable": "station_silent"}
        current = observation_to_current(row, station)
        return {
            "source": "SHMU-OBS",
            "role": "observation",
            "station": current["station"],
            "current": current,
            "daily": [],
            "hourly": [],
        }
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError, RuntimeError, ET.ParseError) as exc:
        logger.warning("SHMU observation failed: %s", exc)
        return None


def _latest_run():
    """Return (date_folder, run_folder, file_names) for the newest ALADIN run."""
    days = sorted(n.strip("/") for n in list_directory(ALADIN_ROOT) if n.endswith("/"))
    for day in reversed(days):
        runs = sorted(n.strip("/") for n in list_directory(urljoin(ALADIN_ROOT, day + "/")) if n.endswith("/"))
        for run in reversed(runs):
            base = urljoin(ALADIN_ROOT, f"{day}/{run}/")
            files = [n for n in list_directory(base) if n.endswith(".grb")]
            if files:
                return day, run, files
    return None


def _read_grib_point(path, lat, lon):
    """Read the surface fields we use at the nearest grid point."""
    if eccodes is None:
        raise RuntimeError("eccodes is not installed")
    wanted = {"2t", "10u", "10v", "tp", "tcc", "prmsl"}
    found = {}
    distance = None
    with open(path, "rb") as handle:
        while True:
            gid = eccodes.codes_grib_new_from_file(handle)
            if gid is None:
                break
            try:
                short = eccodes.codes_get(gid, "shortName")
                if short not in wanted or short in found:
                    continue
                nearest = eccodes.codes_grib_find_nearest(gid, float(lat), float(lon))[0]
                found[short] = nearest["value"]
                distance = nearest["distance"]
            finally:
                eccodes.codes_release(gid)
    found["distance_km"] = distance
    return found


def _series_from_files(files, lat, lon, tz_name):
    """Build hourly dicts from already-downloaded GRIB paths named by step."""
    zone = _zone(tz_name)
    rows = []
    distance = None
    for name, path in files:
        match = _GRIB_RE.search(name)
        if not match:
            continue
        step = int(match.group(1))
        run = datetime.strptime(match.group(2) + match.group(3), "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        valid = run + timedelta(hours=step)
        fields = _read_grib_point(path, lat, lon)
        if fields.get("distance_km") is not None:
            distance = fields["distance_km"]
        temp_c = None
        if fields.get("2t") is not None:
            temp_c = fields["2t"] - 273.15
        wind_kmh, wind_dir = wind_from_components(fields.get("10u"), fields.get("10v"))
        pressure = None
        if fields.get("prmsl") is not None:
            pressure = fields["prmsl"] / 100.0
        cloud = fields.get("tcc")
        rows.append({
            "valid": valid,
            "local": valid.astimezone(zone),
            "temp_c": None if temp_c is None else round(temp_c, 2),
            "wind_kmh": wind_kmh,
            "wind_dir": wind_dir,
            "pressure_hpa": None if pressure is None else round(pressure, 1),
            "cloud": cloud,
            "tp": fields.get("tp"),
        })
    rows.sort(key=lambda row: row["valid"])
    increments = precip_increments([row["tp"] for row in rows])
    for row, precip in zip(rows, increments):
        row["precip_mm"] = precip
        row["condition"] = condition_from_fields(row["temp_c"], precip, row["cloud"])
    return rows, distance


def _zone(tz_name):
    if not tz_name or tz_name == "auto":
        return ZoneInfo("Europe/Bratislava")
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("Europe/Bratislava")


def load_aladin_point(lat, lon, tz_name="Europe/Bratislava"):
    """Download the latest ALADIN run and return hourly rows at this point.

    The parsed series is cached on disk and in memory for the run id.
    """
    if eccodes is None:
        raise RuntimeError("eccodes is not installed")
    key = f"{round(float(lat), 2):.2f}_{round(float(lon), 2):.2f}"
    with _lock:
        cached = _point_mem.get(key)
    if cached and (datetime.now(timezone.utc) - cached["fetched_at"]).total_seconds() < 3 * 3600:
        return cached

    located = _latest_run()
    if not located:
        return None
    day, run, names = located
    run_id = f"{day}_{run}"
    disk = _cache_path("points", f"{run_id}_{key}.json")
    if os.path.isfile(disk):
        with open(disk, encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["fetched_at"] = datetime.now(timezone.utc)
        with _lock:
            _point_mem[key] = payload
        return payload

    base_url = urljoin(ALADIN_ROOT, f"{day}/{run}/")
    dest_dir = _cache_path("grib", run_id)

    def _one(name):
        path = os.path.join(dest_dir, name)
        _download(urljoin(base_url, quote(name)), path)
        return name, path

    files = []
    workers = min(12, len(names))
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_one, name) for name in names]
        for future in as_completed(futures):
            try:
                files.append(future.result())
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                logger.warning("SHMU GRIB skipped: %s", exc)

    rows, distance = _series_from_files(files, lat, lon, tz_name)
    if distance is not None and distance > _MAX_GRID_DISTANCE_KM:
        return {"out_of_domain": True, "distance_km": distance, "rows": [], "run_id": run_id}
    serializable = []
    for row in rows:
        serializable.append({
            "time": row["local"].strftime("%Y-%m-%dT%H:%M"),
            "valid_utc": row["valid"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temp_c": row["temp_c"],
            "precip_mm": row["precip_mm"],
            "wind_kmh": row["wind_kmh"],
            "wind_dir": row["wind_dir"],
            "pressure_hpa": row["pressure_hpa"],
            "cloud": row["cloud"],
            "condition": row["condition"],
        })
    payload = {
        "run_id": run_id,
        "distance_km": None if distance is None else round(float(distance), 2),
        "rows": serializable,
        "out_of_domain": False,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    os.makedirs(os.path.dirname(disk), exist_ok=True)
    with open(disk, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    payload["fetched_at"] = datetime.now(timezone.utc)
    with _lock:
        _point_mem[key] = payload
    return payload


def _daily_from_rows(rows):
    by_day = {}
    for row in rows:
        day = row["time"][:10]
        by_day.setdefault(day, []).append(row)
    daily = []
    for day in sorted(by_day):
        hours = by_day[day]
        temps = [h["temp_c"] for h in hours if h.get("temp_c") is not None]
        precips = [h["precip_mm"] for h in hours if h.get("precip_mm") is not None]
        winds = [h["wind_kmh"] for h in hours if h.get("wind_kmh") is not None]
        # Representative condition: the wettest hour, else the middle hour.
        wet = max(hours, key=lambda h: h.get("precip_mm") or 0)
        cond_hour = wet if (wet.get("precip_mm") or 0) >= 0.2 else hours[len(hours) // 2]
        daily.append({
            "date": day,
            "high_c": round(max(temps), 1) if temps else None,
            "low_c": round(min(temps), 1) if temps else None,
            "precip_mm": round(sum(precips), 2) if precips else None,
            "precip_prob": None,
            "wind_max_kmh": round(max(winds), 1) if winds else None,
            "wind_dir_deg": cond_hour.get("wind_dir"),
            "condition": cond_hour.get("condition"),
        })
    return daily


def _current_from_rows(rows):
    if not rows:
        return None
    now = datetime.now(timezone.utc)
    best = None
    best_delta = None
    for row in rows:
        try:
            valid = datetime.strptime(row["valid_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            continue
        delta = abs((valid - now).total_seconds())
        if best_delta is None or delta < best_delta:
            best = row
            best_delta = delta
    if best is None:
        best = rows[0]
    return {
        "temp_c": best.get("temp_c"),
        "feels_like_c": None,
        "humidity": None,
        "wind_speed_kmh": best.get("wind_kmh"),
        "wind_dir_deg": best.get("wind_dir"),
        "precipitation_mm": best.get("precip_mm"),
        "pressure_hpa": best.get("pressure_hpa"),
        "condition": best.get("condition") or "Unknown",
        "valid_time": best.get("time"),
    }


def fetch_shmu(lat, lon, tz="Europe/Bratislava"):
    """ALADIN point forecast in the same shape as the other weather sources."""
    if not in_slovakia(lat, lon):
        return {"unavailable": "out_of_domain"}
    if eccodes is None:
        return None
    try:
        payload = load_aladin_point(lat, lon, tz)
    except (HTTPError, URLError, TimeoutError, OSError, RuntimeError, json.JSONDecodeError) as exc:
        logger.warning("SHMU ALADIN failed: %s", exc)
        return None
    if not payload or payload.get("out_of_domain"):
        return {"unavailable": "out_of_domain"}
    rows = payload.get("rows") or []
    if not rows:
        return None
    hourly = []
    for row in rows:
        hourly.append({
            "hour": row["time"],
            "temp_c": row.get("temp_c"),
            "precip_mm": row.get("precip_mm"),
            "precip_prob": None,
            "wind_kmh": row.get("wind_kmh"),
            "wind_dir": row.get("wind_dir"),
            "pressure_hpa": row.get("pressure_hpa"),
            "condition": row.get("condition"),
        })
    return {
        "source": "SHMU",
        "role": "forecast",
        "model": "ALADIN/SHMU 4.5km",
        "run_id": payload.get("run_id"),
        "grid_distance_km": payload.get("distance_km"),
        "license": "CC BY 4.0",
        "attribution": "Slovak Hydrometeorological Institute (SHMÚ)",
        "current": _current_from_rows(rows),
        "daily": _daily_from_rows(rows),
        "hourly": hourly,
    }


def collect_station_days(ind_kli, dates, workers=12):
    """Download minute files for each date and aggregate one station.

    ``dates`` is a list of ``datetime.date``. This is for verification, not
    the request path: a full day is 288 files. Failures of individual files
    are skipped and the resulting sample count is reported by the caller.
    """
    rows = []

    def _pull(day, filename):
        payload = fetch_observation_file(day, filename)
        return [r for r in payload.get("data", []) if r.get("ind_kli") == ind_kli]

    tasks = []
    for day in dates:
        folder = day.strftime("%Y%m%d")
        try:
            names = [n for n in list_directory(urljoin(OBS_ROOT, folder + "/")) if n.endswith(".json")]
        except (HTTPError, URLError, TimeoutError, OSError):
            continue
        for name in names:
            tasks.append((day, name))

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(_pull, day, name) for day, name in tasks]
        for future in as_completed(futures):
            try:
                rows.extend(future.result())
            except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError):
                continue
    return aggregate_observation_rows(rows)


def aladin_daily_for_run(run_date, lat, lon, tz_name="Europe/Bratislava"):
    """Daily aggregates from one 00 UTC ALADIN run already on the open-data server.

    ``run_date`` is a ``datetime.date``. Used by the backtest. Returns a list
    of daily dicts in the station's local timezone.
    """
    if eccodes is None:
        raise RuntimeError("eccodes is not installed")
    day = run_date.strftime("%Y%m%d")
    base = urljoin(ALADIN_ROOT, f"{day}/0000/")
    names = [n for n in list_directory(base) if n.endswith(".grb")]
    if not names:
        return []
    dest_dir = _cache_path("grib", f"{day}_0000")

    def _one(name):
        path = os.path.join(dest_dir, name)
        _download(urljoin(base, quote(name)), path)
        return name, path

    files = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(_one, name) for name in names]
        for future in as_completed(futures):
            try:
                files.append(future.result())
            except (HTTPError, URLError, TimeoutError, OSError):
                continue
    rows, distance = _series_from_files(files, lat, lon, tz_name)
    if distance is not None and distance > _MAX_GRID_DISTANCE_KM:
        return []
    serial = []
    for row in rows:
        serial.append({
            "time": row["local"].strftime("%Y-%m-%dT%H:%M"),
            "valid_utc": row["valid"].strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temp_c": row["temp_c"],
            "precip_mm": row["precip_mm"],
            "wind_kmh": row["wind_kmh"],
            "wind_dir": row["wind_dir"],
            "pressure_hpa": row["pressure_hpa"],
            "cloud": row["cloud"],
            "condition": row["condition"],
        })
    return _daily_from_rows(serial)
