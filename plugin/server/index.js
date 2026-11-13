"use strict";

const readline = require("node:readline");
const shmu = require("./shmu");
const ensemble = require("./ensemble");
const alerts = require("./alerts");

const SERVER_NAME = "open-meteo-weather";
const SERVER_VERSION = "0.1.0";
const FORECAST_URL = "https://api.open-meteo.com/v1/forecast";
const GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search";
const AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality";
const METAR_URL = "https://aviationweather.gov/api/data/metar";
const FETCH_HEADERS = {
  Accept: "application/json",
  "User-Agent": "open-meteo-weather/0.1.0 (Agent Plugin)",
};

const SUPPORTED_PROTOCOL_VERSIONS = [
  "2025-06-18",
  "2025-03-26",
  "2024-11-05",
];

const METAR_MAX_DISTANCE_KM = 25;
const METAR_MAX_ELEVATION_DIFF_M = 100;

const WEATHER_CODES = {
  0: "Clear sky",
  1: "Mainly clear",
  2: "Partly cloudy",
  3: "Overcast",
  45: "Fog",
  48: "Depositing rime fog",
  51: "Light drizzle",
  53: "Moderate drizzle",
  55: "Dense drizzle",
  56: "Light freezing drizzle",
  57: "Dense freezing drizzle",
  61: "Slight rain",
  63: "Moderate rain",
  65: "Heavy rain",
  66: "Light freezing rain",
  67: "Heavy freezing rain",
  71: "Slight snow",
  73: "Moderate snow",
  75: "Heavy snow",
  77: "Snow grains",
  80: "Slight rain showers",
  81: "Moderate rain showers",
  82: "Violent rain showers",
  85: "Slight snow showers",
  86: "Heavy snow showers",
  95: "Thunderstorm",
  96: "Thunderstorm with slight hail",
  99: "Thunderstorm with heavy hail",
};

const CLOUD_COVER_WEATHER = {
  CLR: "Clear sky",
  SKC: "Clear sky",
  CAVOK: "Clear sky",
  NSC: "Clear sky",
  NCD: "Clear sky",
  FEW: "Mainly clear",
  SCT: "Partly cloudy",
  BKN: "Overcast",
  OVC: "Overcast",
  VV: "Fog",
};

const CLOUD_COVER_PERCENT = {
  CLR: 0,
  SKC: 0,
  CAVOK: 0,
  NSC: 0,
  NCD: 0,
  FEW: 19,
  SCT: 44,
  BKN: 75,
  OVC: 100,
  VV: 100,
};

const METAR_WX = [
  ["TS", "Thunderstorm"],
  ["SHRA", "Rain showers"],
  ["SHSN", "Snow showers"],
  ["SH", "Showers"],
  ["FZRA", "Freezing rain"],
  ["FZDZ", "Freezing drizzle"],
  ["+RA", "Heavy rain"],
  ["-RA", "Light rain"],
  ["RA", "Rain"],
  ["+SN", "Heavy snow"],
  ["-SN", "Light snow"],
  ["SN", "Snow"],
  ["+DZ", "Heavy drizzle"],
  ["-DZ", "Light drizzle"],
  ["DZ", "Drizzle"],
  ["FG", "Fog"],
  ["BR", "Mist"],
  ["HZ", "Haze"],
  ["SQ", "Squalls"],
  ["GR", "Hail"],
  ["GS", "Small hail"],
];

const CURRENT_VARS = [
  "temperature_2m",
  "relative_humidity_2m",
  "apparent_temperature",
  "is_day",
  "precipitation",
  "weather_code",
  "cloud_cover",
  "pressure_msl",
  "wind_speed_10m",
  "wind_direction_10m",
  "wind_gusts_10m",
].join(",");

const HOURLY_VARS = [
  "temperature_2m",
  "relative_humidity_2m",
  "apparent_temperature",
  "precipitation_probability",
  "precipitation",
  "weather_code",
  "cloud_cover",
  "wind_speed_10m",
  "wind_direction_10m",
  "wind_gusts_10m",
  "is_day",
].join(",");

const DAILY_VARS = [
  "weather_code",
  "temperature_2m_max",
  "temperature_2m_min",
  "apparent_temperature_max",
  "apparent_temperature_min",
  "sunrise",
  "sunset",
  "precipitation_sum",
  "precipitation_probability_max",
  "wind_speed_10m_max",
  "wind_gusts_10m_max",
  "wind_direction_10m_dominant",
].join(",");

const MODEL_CATALOG = {
  best_match: {
    id: "best_match",
    label: "Open-Meteo best match",
    note: "Open-Meteo automatically selects the best model for the coordinates (may include ECMWF, ICON, GFS, MET Norway, JMA, GEM, Météo-France, KNMI, and others).",
  },
  ecmwf: {
    id: "ecmwf_ifs",
    fallback: "ecmwf_ifs025",
    label: "ECMWF IFS",
    note: "ECMWF IFS via Open-Meteo (HRES 9 km, falling back to 0.25° if needed).",
  },
  icon: {
    id: "icon_seamless",
    label: "DWD ICON seamless",
    note: "DWD ICON via Open-Meteo (seamless global / EU / D2 where available).",
  },
  aladin: {
    id: "chmi_aladin_seamless",
    label: "CHMI ALADIN seamless",
    note: "CHMI (Czech) ALADIN via Open-Meteo. This is not SHMÚ's official ALADIN or A-LAEF.",
    optional: true,
  },
  shmu_aladin: {
    id: "shmu_aladin",
    label: "SHMÚ ALADIN 4.5 km",
    note: "Official ALADIN/SHMÚ 4.5 km GRIB2 via opendata.shmu.sk (CC BY 4.0). Only available inside the Slovak ALADIN domain. Decoded in-process with a pure-JS simple-packing reader; no eccodes required.",
    optional: true,
    direct: true,
  },
};

const TOOLS = [
  {
    name: "get_forecast",
    description:
      "Get current conditions plus hourly and daily forecast, aligned with Meteric's publicly documented sources: Open-Meteo (best_match, plus ECMWF IFS and ICON for comparison, and CHMI ALADIN when in domain), official SHMÚ AWS station observations and SHMÚ ALADIN 4.5 km GRIB2 when the point is in Slovakia / the ALADIN domain, METAR from NOAA Aviation Weather Center when a nearby airport qualifies, and Open-Meteo CAMS air quality. Named places are geocoded first. Returns a normalized Forecast (location, timezone, current, hourly, daily) plus air_quality, models, ensemble uncertainty, alerts (MeteoAlarm), and sources. Never invent values.",
    inputSchema: {
      type: "object",
      properties: {
        place: {
          type: "string",
          description:
            'Place name to geocode, for example "Bratislava". Required unless both latitude and longitude are provided.',
        },
        latitude: {
          type: "number",
          description: "WGS84 latitude. Use together with longitude.",
        },
        longitude: {
          type: "number",
          description: "WGS84 longitude. Use together with latitude.",
        },
        days: {
          type: "integer",
          minimum: 1,
          maximum: 16,
          description: "Forecast length in days (1-16). Defaults to 7.",
        },
        model: {
          type: "string",
          enum: ["best_match", "ecmwf", "icon", "aladin", "shmu_aladin", "all"],
          description:
            "Primary model for current/hourly/daily. Default best_match (Open-Meteo's automatic mix). ecmwf, icon, or aladin (CHMI via Open-Meteo) select that Open-Meteo model as the primary series. shmu_aladin selects official SHMÚ ALADIN 4.5 km (only inside its domain). all still uses best_match as the primary series and always includes the comparison block.",
        },
      },
      additionalProperties: false,
    },
  },
  {
    name: "search_location",
    description:
      "Search for geographic locations by name using Open-Meteo geocoding. Returns a normalized list of matching places (name, country, coordinates, timezone).",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Place name or partial name to search for.",
        },
      },
      required: ["query"],
      additionalProperties: false,
    },
  },
];

function weatherDescription(code) {
  if (code == null) return null;
  return WEATHER_CODES[code] ?? `Unknown weather code ${code}`;
}

function asNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function asBool01(value) {
  if (value === 1 || value === true) return true;
  if (value === 0 || value === false) return false;
  return null;
}

function atIndex(block, key, i) {
  const arr = block?.[key];
  if (!Array.isArray(arr)) return null;
  return arr[i] ?? null;
}

function normalizeLocation(place, latitude, longitude, elevation) {
  return {
    name: place?.name ?? null,
    country: place?.country ?? null,
    country_code: place?.country_code ?? null,
    admin1: place?.admin1 ?? null,
    latitude,
    longitude,
    elevation_m: asNumber(place?.elevation ?? elevation),
  };
}

function normalizeSearchResult(item) {
  return {
    name: item.name ?? null,
    country: item.country ?? null,
    country_code: item.country_code ?? null,
    admin1: item.admin1 ?? null,
    latitude: item.latitude,
    longitude: item.longitude,
    elevation_m: asNumber(item.elevation),
    timezone: item.timezone ?? null,
    population: asNumber(item.population),
    feature_code: item.feature_code ?? null,
  };
}

function normalizeCurrent(current, source) {
  if (!current || typeof current !== "object") return null;
  const code = current.weather_code ?? null;
  return {
    time: current.time ?? null,
    temperature_c: asNumber(current.temperature_2m),
    apparent_temperature_c: asNumber(current.apparent_temperature),
    humidity_percent: asNumber(current.relative_humidity_2m),
    precipitation_mm: asNumber(current.precipitation),
    weather: weatherDescription(code),
    weather_code: code,
    cloud_cover_percent: asNumber(current.cloud_cover),
    pressure_hpa: asNumber(current.pressure_msl),
    wind_speed_kmh: asNumber(current.wind_speed_10m),
    wind_direction_deg: asNumber(current.wind_direction_10m),
    wind_gusts_kmh: asNumber(current.wind_gusts_10m),
    is_day: asBool01(current.is_day),
    source: source ?? null,
  };
}

function normalizeHourly(hourly) {
  const times = Array.isArray(hourly?.time) ? hourly.time : [];
  return times.map((time, i) => {
    const code = atIndex(hourly, "weather_code", i);
    return {
      time,
      temperature_c: asNumber(atIndex(hourly, "temperature_2m", i)),
      apparent_temperature_c: asNumber(atIndex(hourly, "apparent_temperature", i)),
      humidity_percent: asNumber(atIndex(hourly, "relative_humidity_2m", i)),
      precipitation_mm: asNumber(atIndex(hourly, "precipitation", i)),
      precipitation_probability_percent: asNumber(
        atIndex(hourly, "precipitation_probability", i)
      ),
      weather: weatherDescription(code),
      weather_code: code,
      cloud_cover_percent: asNumber(atIndex(hourly, "cloud_cover", i)),
      wind_speed_kmh: asNumber(atIndex(hourly, "wind_speed_10m", i)),
      wind_direction_deg: asNumber(atIndex(hourly, "wind_direction_10m", i)),
      wind_gusts_kmh: asNumber(atIndex(hourly, "wind_gusts_10m", i)),
      is_day: asBool01(atIndex(hourly, "is_day", i)),
    };
  });
}

function normalizeDaily(daily) {
  const dates = Array.isArray(daily?.time) ? daily.time : [];
  return dates.map((date, i) => {
    const code = atIndex(daily, "weather_code", i);
    return {
      date,
      temperature_max_c: asNumber(atIndex(daily, "temperature_2m_max", i)),
      temperature_min_c: asNumber(atIndex(daily, "temperature_2m_min", i)),
      apparent_temperature_max_c: asNumber(
        atIndex(daily, "apparent_temperature_max", i)
      ),
      apparent_temperature_min_c: asNumber(
        atIndex(daily, "apparent_temperature_min", i)
      ),
      weather: weatherDescription(code),
      weather_code: code,
      sunrise: atIndex(daily, "sunrise", i),
      sunset: atIndex(daily, "sunset", i),
      precipitation_sum_mm: asNumber(atIndex(daily, "precipitation_sum", i)),
      precipitation_probability_max_percent: asNumber(
        atIndex(daily, "precipitation_probability_max", i)
      ),
      wind_speed_max_kmh: asNumber(atIndex(daily, "wind_speed_10m_max", i)),
      wind_gusts_max_kmh: asNumber(atIndex(daily, "wind_gusts_10m_max", i)),
      wind_direction_dominant_deg: asNumber(
        atIndex(daily, "wind_direction_10m_dominant", i)
      ),
    };
  });
}

function normalizeAirQuality(data) {
  if (!data?.current) return null;
  const c = data.current;
  return {
    time: c.time ?? null,
    european_aqi: asNumber(c.european_aqi),
    us_aqi: asNumber(c.us_aqi),
    pm2_5_ugm3: asNumber(c.pm2_5),
    pm10_ugm3: asNumber(c.pm10),
    nitrogen_dioxide_ugm3: asNumber(c.nitrogen_dioxide),
    ozone_ugm3: asNumber(c.ozone),
    sulphur_dioxide_ugm3: asNumber(c.sulphur_dioxide),
    carbon_monoxide_ugm3: asNumber(c.carbon_monoxide),
    pollen: {
      alder: asNumber(c.alder_pollen),
      birch: asNumber(c.birch_pollen),
      grass: asNumber(c.grass_pollen),
      mugwort: asNumber(c.mugwort_pollen),
      olive: asNumber(c.olive_pollen),
      ragweed: asNumber(c.ragweed_pollen),
    },
    source: "Open-Meteo CAMS",
  };
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const r = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * r * Math.asin(Math.sqrt(a));
}

function relativeHumidity(tempC, dewC) {
  if (tempC == null || dewC == null) return null;
  const vp = (t) => Math.exp((17.625 * t) / (243.04 + t));
  const rh = 100 * (vp(dewC) / vp(tempC));
  if (!Number.isFinite(rh)) return null;
  return Math.max(0, Math.min(100, Math.round(rh)));
}

function toLocalIso(utcDate, utcOffsetSeconds) {
  const shifted = new Date(utcDate.getTime() + utcOffsetSeconds * 1000);
  return shifted.toISOString().slice(0, 16);
}

function metarCloudPercent(obs) {
  const layers = Array.isArray(obs.clouds) ? obs.clouds : [];
  let max = null;
  for (const layer of layers) {
    const p = CLOUD_COVER_PERCENT[layer?.cover];
    if (typeof p === "number") max = max == null ? p : Math.max(max, p);
  }
  if (max == null && obs.cover && CLOUD_COVER_PERCENT[obs.cover] != null) {
    max = CLOUD_COVER_PERCENT[obs.cover];
  }
  return max;
}

function metarWeatherText(obs) {
  const rawWx = String(obs.wxString || "").toUpperCase();
  const parts = [];
  let remain = rawWx;
  for (const [code, text] of METAR_WX) {
    if (remain.includes(code)) {
      parts.push(text);
      remain = remain.split(code).join(" ");
    }
  }
  const cover = obs.cover || (obs.clouds && obs.clouds[0] && obs.clouds[0].cover);
  const cloudText = CLOUD_COVER_WEATHER[cover] ?? null;
  if (parts.length && cloudText) return `${parts.join(", ")}, ${cloudText.toLowerCase()}`;
  if (parts.length) return parts.join(", ");
  return cloudText;
}

function enrichObservation(current, fillers) {
  if (!current) return current;
  const sources = { ...(current.field_sources || {}) };
  const markExisting = (obj, src) => {
    for (const [k, v] of Object.entries(obj || {})) {
      if (k === "station" || k === "field_sources" || k === "source") continue;
      if (v != null && sources[k] == null) sources[k] = src;
    }
  };
  markExisting(current, current.source);
  if (current.pressure_station_hpa != null && sources.pressure_station_hpa == null) {
    sources.pressure_station_hpa = "SHMÚ AWS";
  }
  if (
    current.source === "SHMÚ AWS" &&
    current.pressure_hpa != null &&
    current.pressure_station_hpa != null
  ) {
    sources.pressure_hpa = "SHMÚ AWS (reduced to MSL)";
  }
  const fill = (field, value, src) => {
    if ((current[field] == null || current[field] === "") && value != null && value !== "") {
      current[field] = value;
      sources[field] = src;
    }
  };
  for (const { values, source } of fillers) {
    if (!values || !source) continue;
    fill("weather", values.weather, source);
    fill("weather_code", values.weather_code, source);
    fill("cloud_cover_percent", values.cloud_cover_percent, source);
    fill("is_day", values.is_day, source);
    fill("apparent_temperature_c", values.apparent_temperature_c, source);
  }
  current.field_sources = sources;
  return current;
}

function normalizeMetar(obs, origin, elevationM, utcOffsetSeconds) {
  const distanceKm = haversineKm(origin.latitude, origin.longitude, obs.lat, obs.lon);
  const elevDiff =
    elevationM != null && obs.elev != null ? Math.abs(elevationM - obs.elev) : null;
  const qualifies =
    distanceKm <= METAR_MAX_DISTANCE_KM &&
    (elevDiff == null || elevDiff <= METAR_MAX_ELEVATION_DIFF_M);

  const reportTime = obs.reportTime ? new Date(obs.reportTime) : null;
  const time =
    reportTime && !Number.isNaN(reportTime.getTime())
      ? toLocalIso(reportTime, utcOffsetSeconds ?? 0)
      : obs.reportTime ?? null;

  const cover = obs.cover || (obs.clouds && obs.clouds[0] && obs.clouds[0].cover);
  return {
    current: {
      time,
      temperature_c: asNumber(obs.temp),
      apparent_temperature_c: null,
      humidity_percent: relativeHumidity(asNumber(obs.temp), asNumber(obs.dewp)),
      precipitation_mm: null,
      weather: metarWeatherText(obs),
      weather_code: null,
      cloud_cover_percent: metarCloudPercent(obs),
      pressure_hpa: asNumber(obs.altim),
      wind_speed_kmh:
        asNumber(obs.wspd) != null ? Math.round(obs.wspd * 1.852 * 10) / 10 : null,
      wind_direction_deg: asNumber(obs.wdir),
      wind_gusts_kmh:
        asNumber(obs.wgst) != null ? Math.round(obs.wgst * 1.852 * 10) / 10 : null,
      is_day: null,
      source: "METAR",
      station: {
        icao: obs.icaoId ?? null,
        name: obs.name ?? null,
        latitude: obs.lat,
        longitude: obs.lon,
        elevation_m: asNumber(obs.elev),
        distance_km: Math.round(distanceKm * 10) / 10,
        elevation_difference_m: elevDiff != null ? Math.round(elevDiff) : null,
        raw: obs.rawOb ?? null,
      },
    },
    qualifies,
    distanceKm,
  };
}

async function fetchJson(url, headers = FETCH_HEADERS) {
  const res = await fetch(url, {
    headers,
    signal: AbortSignal.timeout(20000),
  });
  let body;
  try {
    body = await res.json();
  } catch {
    throw new Error(`Non-JSON response (HTTP ${res.status}) from ${url}`);
  }
  if (!res.ok || body?.error) {
    const reason = body?.reason || body?.error || res.statusText;
    throw new Error(`Request failed (HTTP ${res.status}): ${reason}`);
  }
  return body;
}

async function geocode(query, count = 10) {
  const name = String(query ?? "").trim();
  if (!name) {
    throw new Error("A non-empty place query is required.");
  }
  const url = new URL(GEOCODE_URL);
  url.searchParams.set("name", name);
  url.searchParams.set("count", String(count));
  url.searchParams.set("language", "en");
  url.searchParams.set("format", "json");
  const data = await fetchJson(url);
  return Array.isArray(data.results) ? data.results : [];
}

async function searchLocation(args) {
  const query = args?.query;
  if (typeof query !== "string" || !query.trim()) {
    throw new Error('search_location requires a non-empty string "query".');
  }
  const results = await geocode(query, 10);
  return {
    query: query.trim(),
    results: results.map(normalizeSearchResult),
  };
}

function parseDays(value) {
  if (value == null || value === "") return 7;
  const days = Number(value);
  if (!Number.isInteger(days) || days < 1 || days > 16) {
    throw new Error("days must be an integer between 1 and 16.");
  }
  return days;
}

function parseCoord(value, label, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < min || n > max) {
    throw new Error(`${label} must be a number between ${min} and ${max}.`);
  }
  return n;
}

async function fetchOpenMeteoForecast(latitude, longitude, days, modelId) {
  const url = new URL(FORECAST_URL);
  url.searchParams.set("latitude", String(latitude));
  url.searchParams.set("longitude", String(longitude));
  url.searchParams.set("timezone", "auto");
  url.searchParams.set("forecast_days", String(days));
  url.searchParams.set("current", CURRENT_VARS);
  url.searchParams.set("hourly", HOURLY_VARS);
  url.searchParams.set("daily", DAILY_VARS);
  if (modelId && modelId !== "best_match") {
    url.searchParams.set("models", modelId);
  } else {
    url.searchParams.set("models", "best_match");
  }
  const data = await fetchJson(url);
  return {
    raw: data,
    current: normalizeCurrent(data.current, `Open-Meteo ${modelId}`),
    hourly: normalizeHourly(data.hourly),
    daily: normalizeDaily(data.daily),
  };
}

async function fetchForecastModel(latitude, longitude, days, alias) {
  const spec = MODEL_CATALOG[alias];
  const ids = [spec.id, spec.fallback].filter(Boolean);
  let lastError;
  for (const id of ids) {
    try {
      const result = await fetchOpenMeteoForecast(latitude, longitude, days, id);
      return { alias, modelId: id, label: spec.label, note: spec.note, ...result };
    } catch (err) {
      lastError = err;
    }
  }
  if (spec.optional) return null;
  throw lastError ?? new Error(`Failed to fetch model ${alias}`);
}

async function fetchAirQuality(latitude, longitude, days) {
  const url = new URL(AIR_QUALITY_URL);
  url.searchParams.set("latitude", String(latitude));
  url.searchParams.set("longitude", String(longitude));
  url.searchParams.set("timezone", "auto");
  url.searchParams.set("forecast_days", String(Math.min(days, 7)));
  url.searchParams.set(
    "current",
    [
      "european_aqi",
      "us_aqi",
      "pm2_5",
      "pm10",
      "nitrogen_dioxide",
      "ozone",
      "sulphur_dioxide",
      "carbon_monoxide",
      "alder_pollen",
      "birch_pollen",
      "grass_pollen",
      "mugwort_pollen",
      "olive_pollen",
      "ragweed_pollen",
    ].join(",")
  );
  const data = await fetchJson(url);
  return normalizeAirQuality(data);
}

async function fetchNearbyMetar(latitude, longitude, elevationM, utcOffsetSeconds) {
  const latDelta = METAR_MAX_DISTANCE_KM / 111.32;
  const lonDelta =
    METAR_MAX_DISTANCE_KM / (111.32 * Math.max(0.2, Math.cos((latitude * Math.PI) / 180)));
  const south = latitude - latDelta;
  const north = latitude + latDelta;
  const west = longitude - lonDelta;
  const east = longitude + lonDelta;
  const url = new URL(METAR_URL);
  url.searchParams.set("format", "json");
  url.searchParams.set("hours", "1");
  url.searchParams.set(
    "bbox",
    `${south.toFixed(4)},${west.toFixed(4)},${north.toFixed(4)},${east.toFixed(4)}`
  );
  const data = await fetchJson(url, FETCH_HEADERS);
  if (!Array.isArray(data) || data.length === 0) return null;
  const latestByStation = new Map();
  for (const obs of data) {
    if (obs?.lat == null || obs?.lon == null) continue;
    const key = obs.icaoId || `${obs.lat},${obs.lon}`;
    const prev = latestByStation.get(key);
    const t = Date.parse(obs.reportTime || "") || 0;
    if (!prev || t > prev._t) latestByStation.set(key, { ...obs, _t: t });
  }
  const origin = { latitude, longitude };
  const ranked = [...latestByStation.values()]
    .map((obs) => normalizeMetar(obs, origin, elevationM, utcOffsetSeconds))
    .filter((x) => x.qualifies)
    .sort((a, b) => a.distanceKm - b.distanceKm);
  return ranked[0] ?? null;
}

async function settledValue(promise) {
  try {
    return { ok: true, value: await promise };
  } catch (err) {
    return { ok: false, error: err instanceof Error ? err.message : String(err) };
  }
}

async function getForecast(args) {
  const raw = args && typeof args === "object" ? args : {};
  const hasPlace = typeof raw.place === "string" && raw.place.trim();
  const hasLat = raw.latitude != null && raw.latitude !== "";
  const hasLon = raw.longitude != null && raw.longitude !== "";

  if (hasLat !== hasLon) {
    throw new Error("Provide both latitude and longitude, or neither.");
  }
  if (!hasPlace && !hasLat) {
    throw new Error(
      "get_forecast requires a place name or both latitude and longitude."
    );
  }

  const days = parseDays(raw.days);
  const modelArg = raw.model == null || raw.model === "" ? "best_match" : String(raw.model);
  const MODEL_ARGS = ["best_match", "ecmwf", "icon", "aladin", "shmu_aladin", "all"];
  if (!MODEL_ARGS.includes(modelArg)) {
    throw new Error("model must be best_match, ecmwf, icon, aladin, shmu_aladin, or all.");
  }

  let place = null;
  let latitude;
  let longitude;

  if (hasLat) {
    latitude = parseCoord(raw.latitude, "latitude", -90, 90);
    longitude = parseCoord(raw.longitude, "longitude", -180, 180);
  }

  if (hasPlace) {
    const matches = await geocode(raw.place, 1);
    if (matches.length === 0) {
      throw new Error(`No locations found for "${raw.place.trim()}".`);
    }
    place = matches[0];
    if (!hasLat) {
      latitude = place.latitude;
      longitude = place.longitude;
    }
  }

  const aliases = ["best_match", "ecmwf", "icon", "aladin"];
  let resolveOffset = () => {};
  let resolveElev = () => {};
  const offsetPromise = new Promise((r) => {
    resolveOffset = r;
  });
  const elevPromise = new Promise((r) => {
    resolveElev = r;
  });

  const [bestRes, ecmwfRes, iconRes, aladinRes, aqRes, shmuAladinRes, shmuStationRes, ensembleRes, alertsRes] =
    await Promise.all([
      settledValue(fetchForecastModel(latitude, longitude, days, "best_match")).then((res) => {
        try {
          if (res.ok && res.value?.raw) {
            resolveOffset(res.value.raw.utc_offset_seconds ?? 0);
            resolveElev(res.value.raw.elevation ?? place?.elevation ?? null);
          } else {
            resolveOffset(0);
            resolveElev(place?.elevation ?? null);
          }
        } catch {
          resolveOffset(0);
          resolveElev(place?.elevation ?? null);
        }
        return res;
      }),
      settledValue(fetchForecastModel(latitude, longitude, days, "ecmwf")),
      settledValue(fetchForecastModel(latitude, longitude, days, "icon")),
      settledValue(fetchForecastModel(latitude, longitude, days, "aladin")),
      settledValue(fetchAirQuality(latitude, longitude, days)),
      settledValue(shmu.fetchAladinForecast(latitude, longitude, days, offsetPromise)),
      settledValue(shmu.fetchNearestStation(latitude, longitude, elevPromise)),
      settledValue(ensemble.fetchEnsemble(latitude, longitude, days, offsetPromise)),
      settledValue(alerts.fetchAlerts(latitude, longitude, place)),
    ]);

  const fetched = {
    best_match: bestRes,
    ecmwf: ecmwfRes,
    icon: iconRes,
    aladin: aladinRes,
  };

  const primaryAlias = modelArg === "all" ? "best_match" : modelArg;
  const openMeteoPrimary = fetched[primaryAlias];
  const tzFallback = place?.timezone ?? null;
  const elevFallback = place?.elevation ?? null;

  let primary;
  if (primaryAlias === "shmu_aladin") {
    if (!shmuAladinRes?.ok || !shmuAladinRes.value) {
      const detail = shmuAladinRes?.error || "no data for this location";
      throw new Error(`Could not fetch primary model shmu_aladin: ${detail}`);
    }
    const raw = bestRes.ok && bestRes.value?.raw ? bestRes.value.raw : {};
    primary = {
      ...shmuAladinRes.value,
      raw: {
        latitude: raw.latitude ?? latitude,
        longitude: raw.longitude ?? longitude,
        elevation: raw.elevation ?? elevFallback,
        timezone: raw.timezone ?? tzFallback,
        utc_offset_seconds: raw.utc_offset_seconds ?? 0,
      },
    };
  } else {
    if (!openMeteoPrimary?.ok || !openMeteoPrimary.value) {
      const detail = openMeteoPrimary?.error || "no data for this location";
      throw new Error(`Could not fetch primary model ${primaryAlias}: ${detail}`);
    }
    primary = openMeteoPrimary.value;
  }

  const location = normalizeLocation(
    place,
    primary.raw.latitude ?? latitude,
    primary.raw.longitude ?? longitude,
    primary.raw.elevation ?? elevFallback
  );

  const utcOffsetSeconds = primary.raw.utc_offset_seconds ?? 0;
  const metarRes = await settledValue(
    fetchNearbyMetar(latitude, longitude, location.elevation_m, utcOffsetSeconds)
  );

  const models = {};
  const sources = [];
  const warnings = [];

  for (const alias of aliases) {
    const res = fetched[alias];
    if (res?.ok && res.value) {
      const m = res.value;
      models[m.modelId] = {
        label: m.label,
        note: m.note,
        current: m.current,
        hourly: m.hourly,
        daily: m.daily,
      };
      sources.push({
        name: m.label,
        model: m.modelId,
        role: alias === primaryAlias ? "primary_forecast" : "comparison",
        url: "https://open-meteo.com/en/docs",
      });
    } else if (res && !res.ok && !MODEL_CATALOG[alias].optional) {
      warnings.push(`${alias}: ${res.error}`);
    }
  }

  if (shmuAladinRes?.ok && shmuAladinRes.value) {
    const m = shmuAladinRes.value;
    models.shmu_aladin = {
      label: m.label,
      note: m.note,
      run: m.run,
      grid: m.grid,
      current: m.current,
      hourly: m.hourly,
      daily: m.daily,
    };
    sources.push({
      name: m.label,
      model: "shmu_aladin",
      role: primaryAlias === "shmu_aladin" ? "primary_forecast" : "comparison",
      license: shmu.ATTRIBUTION.license,
      credit: shmu.ATTRIBUTION.credit,
      url: shmu.ATTRIBUTION.url,
      terms: shmu.ATTRIBUTION.terms,
      run: m.run,
    });
  } else if (shmuAladinRes && !shmuAladinRes.ok) {
    warnings.push(`shmu_aladin: ${shmuAladinRes.error}`);
  }

  if (aqRes.ok && aqRes.value) {
    sources.push({
      name: "Open-Meteo CAMS air quality",
      role: "air_quality",
      url: "https://open-meteo.com/en/docs/air-quality-api",
    });
  } else if (!aqRes.ok) {
    warnings.push(`air_quality: ${aqRes.error}`);
  }

  let current = primary.current;
  let usedShmuStation = false;
  if (shmuStationRes?.ok && shmuStationRes.value?.chosen) {
    const chosen = shmuStationRes.value.chosen;
    const observedMs = chosen.obs?.minuta ? Date.parse(`${chosen.obs.minuta}Z`) : NaN;
    const stale =
      Number.isFinite(observedMs) && Date.now() - observedMs > shmu.AWS_MAX_AGE_MS;
    if (!stale) {
      current = shmu.normalizeShmuCurrent(chosen, utcOffsetSeconds);
      usedShmuStation = true;
      sources.push({
        name: "SHMÚ automatic weather station",
        role: "current_observation",
        license: shmu.ATTRIBUTION.license,
        credit: shmu.ATTRIBUTION.credit,
        url: shmu.ATTRIBUTION.url,
        terms: shmu.ATTRIBUTION.terms,
        station: current.station,
      });
    } else {
      warnings.push(
        "shmu_station: latest qualifying observation is older than 3 hours; not used for current."
      );
    }
  } else if (shmuStationRes && !shmuStationRes.ok) {
    warnings.push(`shmu_station: ${shmuStationRes.error}`);
  } else if (shmuStationRes?.ok && shmuStationRes.value?.nearest) {
    const n = shmuStationRes.value.nearest;
    if (n.distanceKm <= shmu.SHMU_MAX_DISTANCE_KM && n.elevDiff != null && n.elevDiff > shmu.SHMU_MAX_ELEVATION_DIFF_M) {
      warnings.push(
        `shmu_station: nearest ${n.meta.name} is ${n.distanceKm.toFixed(1)} km away but elevation difference ${Math.round(n.elevDiff)} m exceeds ${shmu.SHMU_MAX_ELEVATION_DIFF_M} m; not used for current.`
      );
    }
  }

  if (metarRes.ok && metarRes.value) {
    if (!usedShmuStation) {
      current = metarRes.value.current;
      sources.push({
        name: "NOAA Aviation Weather Center METAR",
        role: "current_observation",
        url: "https://aviationweather.gov/data/api/",
        station: current.station,
      });
    } else {
      sources.push({
        name: "NOAA Aviation Weather Center METAR",
        role: "nearby_metar",
        url: "https://aviationweather.gov/data/api/",
        station: metarRes.value.current.station,
      });
    }
  } else if (!metarRes.ok) {
    warnings.push(`metar: ${metarRes.error}`);
  }

  if (current) {
    const metarCurrent = metarRes.ok && metarRes.value ? metarRes.value.current : null;
    const modelCurrent = primary.current;
    const fillers = [];
    if (metarCurrent && current.source !== "METAR") {
      const icao = metarCurrent.station?.icao || "METAR";
      fillers.push({ values: metarCurrent, source: `METAR ${icao}` });
    }
    if (modelCurrent && current.source !== modelCurrent.source) {
      fillers.push({
        values: modelCurrent,
        source: modelCurrent.source || `Open-Meteo ${primary.modelId}`,
      });
    }
    enrichObservation(current, fillers);
  }

  if (ensembleRes?.ok && ensembleRes.value) {
    sources.push({
      name: ensembleRes.value.label,
      model: ensembleRes.value.modelId,
      role: "ensemble_uncertainty",
      url: "https://open-meteo.com/en/docs/ensemble-api",
      members: ensembleRes.value.members,
    });
  } else if (ensembleRes && !ensembleRes.ok) {
    warnings.push(`ensemble: ${ensembleRes.error}`);
  }

  if (alertsRes?.ok && alertsRes.value) {
    sources.push({
      name: "MeteoAlarm (EUMETNET)",
      role: "alerts",
      license: alertsRes.value.attribution.license,
      credit: alertsRes.value.attribution.credit,
      url: alertsRes.value.attribution.url,
    });
    if (alertsRes.value.warning) warnings.push(`alerts: ${alertsRes.value.warning}`);
  } else if (alertsRes && !alertsRes.ok) {
    warnings.push(`alerts: ${alertsRes.error}`);
  }

  const forecast = {
    location,
    timezone: primary.raw.timezone ?? place?.timezone ?? null,
    current,
    hourly: primary.hourly,
    daily: primary.daily,
    air_quality: aqRes.ok ? aqRes.value : null,
    models,
    primary_model: primary.modelId,
    ensemble: ensembleRes?.ok ? ensembleRes.value : null,
    alerts: alertsRes?.ok ? alertsRes.value : { alerts: [], note: alertsRes?.error || "Alerts unavailable." },
    sources,
  };
  if (warnings.length) forecast.fetch_warnings = warnings;
  return forecast;
}

function send(message) {
  process.stdout.write(JSON.stringify(message) + "\n");
}

function sendResult(id, result) {
  send({ jsonrpc: "2.0", id, result });
}

function sendError(id, code, message, data) {
  const error = { code, message };
  if (data !== undefined) error.data = data;
  send({ jsonrpc: "2.0", id, error });
}

function textResult(payload, isError = false) {
  const result = {
    content: [
      {
        type: "text",
        text: typeof payload === "string" ? payload : JSON.stringify(payload, null, 2),
      },
    ],
  };
  if (isError) result.isError = true;
  return result;
}

function handleInitialize(params) {
  const requested = params?.protocolVersion;
  const protocolVersion = SUPPORTED_PROTOCOL_VERSIONS.includes(requested)
    ? requested
    : SUPPORTED_PROTOCOL_VERSIONS[0];
  return {
    protocolVersion,
    capabilities: { tools: {} },
    serverInfo: { name: SERVER_NAME, version: SERVER_VERSION },
  };
}

async function handleToolCall(params) {
  const name = params?.name;
  const args = params?.arguments ?? {};
  if (name === "get_forecast") return textResult(await getForecast(args));
  if (name === "search_location") return textResult(await searchLocation(args));
  const error = new Error(`Unknown tool: ${name ?? "(missing)"}`);
  error.code = -32601;
  throw error;
}

async function handleRequest(message) {
  const { id, method, params } = message;
  try {
    switch (method) {
      case "initialize":
        sendResult(id, handleInitialize(params));
        return;
      case "ping":
        sendResult(id, {});
        return;
      case "tools/list":
        sendResult(id, { tools: TOOLS });
        return;
      case "tools/call":
        sendResult(id, await handleToolCall(params));
        return;
      case "resources/list":
        sendResult(id, { resources: [] });
        return;
      case "prompts/list":
        sendResult(id, { prompts: [] });
        return;
      default:
        sendError(id, -32601, `Method not found: ${method}`);
    }
  } catch (err) {
    const messageText = err instanceof Error ? err.message : String(err);
    if (method === "tools/call") {
      sendResult(id, textResult({ error: messageText }, true));
      return;
    }
    const code = typeof err?.code === "number" ? err.code : -32603;
    sendError(id, code, messageText);
  }
}

async function handleLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return;
  let message;
  try {
    message = JSON.parse(trimmed);
  } catch {
    sendError(null, -32700, "Parse error");
    return;
  }
  if (message?.jsonrpc !== "2.0" || typeof message.method !== "string") {
    if (message && Object.prototype.hasOwnProperty.call(message, "id")) {
      sendError(message.id ?? null, -32600, "Invalid Request");
    }
    return;
  }
  if (!Object.prototype.hasOwnProperty.call(message, "id")) {
    return;
  }
  await handleRequest(message);
}

const rl = readline.createInterface({ input: process.stdin, terminal: false });
let chain = Promise.resolve();

rl.on("line", (line) => {
  chain = chain.then(() => handleLine(line)).catch((err) => {
    console.error(err);
  });
});

rl.on("close", () => {
  chain.finally(() => {
    // Flush the potentially large tools/call payload before exiting.
    const done = () => process.exit(0);
    if (process.stdout.writableEnded) {
      done();
      return;
    }
    process.stdout.write("", () => done());
  });
});
