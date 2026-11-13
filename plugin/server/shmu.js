"use strict";

const fs = require("node:fs");
const fsPromises = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const grib2 = require("./grib2");

const USER_AGENT = "open-meteo-weather/0.1.0 (Agent Plugin)";
const HEADERS = { Accept: "*/*", "User-Agent": USER_AGENT };

const STATIONS_WFS =
  "https://geo.shmu.sk/geoserver/ef-swn-cs/wfs?SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAMES=ef-swn-cs:EnvironmentalMonitoringFacility&outputFormat=application/json";
const AWS_DATA_BASE = "https://opendata.shmu.sk/meteorology/climate/now/data";
const ALADIN_BASE = "https://opendata.shmu.sk/meteorology/weather/nwp/aladin/sk/4.5km";
const ELEVATION_URL = "https://api.open-meteo.com/v1/elevation";

const SHMU_MAX_DISTANCE_KM = 25;
const SHMU_MAX_ELEVATION_DIFF_M = 100;
const AWS_MAX_AGE_MS = 3 * 60 * 60 * 1000;
const WFS_TTL_MS = 24 * 60 * 60 * 1000;
const ALADIN_HOURS = 72;
const DOWNLOAD_CONCURRENCY = 6;

const ATTRIBUTION = {
  license: "CC BY 4.0",
  credit: "© SHMÚ (Slovak Hydrometeorological Institute)",
  url: "https://opendata.shmu.sk",
  terms: "https://creativecommons.org/licenses/by/4.0/deed.en",
};

function cacheRoot() {
  return path.join(os.tmpdir(), "open-meteo-weather-shmu");
}

async function withRetry(fn, attempts = 3) {
  let last;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      last = err;
      if (i === attempts - 1) break;
      await new Promise((r) => setTimeout(r, 300 * (i + 1)));
    }
  }
  throw last;
}

async function fetchBuffer(url, timeoutMs = 25000) {
  return withRetry(async () => {
    const res = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(timeoutMs) });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return Buffer.from(await res.arrayBuffer());
  });
}

async function fetchText(url, timeoutMs = 25000) {
  return withRetry(async () => {
    const res = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(timeoutMs) });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.text();
  });
}

async function fetchJson(url, timeoutMs = 25000) {
  return withRetry(async () => {
    const res = await fetch(url, {
      headers: { ...HEADERS, Accept: "application/json" },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.json();
  });
}

function listHrefs(html) {
  const out = [];
  const re = /href="([^"]+)"/gi;
  let m;
  while ((m = re.exec(html))) {
    const href = decodeURIComponent(m[1].replace(/&amp;/g, "&"));
    if (href.startsWith("?") || href.startsWith("/") || href.startsWith("#")) continue;
    out.push(href.replace(/\/$/, ""));
  }
  return out;
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

function toLocalIso(utcDate, utcOffsetSeconds) {
  const shifted = new Date(utcDate.getTime() + utcOffsetSeconds * 1000);
  return shifted.toISOString().slice(0, 16);
}

async function readJsonCache(file, ttlMs) {
  try {
    const st = await fsPromises.stat(file);
    if (Date.now() - st.mtimeMs > ttlMs) return null;
    return JSON.parse(await fsPromises.readFile(file, "utf8"));
  } catch {
    return null;
  }
}

async function writeJsonCache(file, data) {
  await fsPromises.mkdir(path.dirname(file), { recursive: true });
  const tmp = file + ".tmp";
  await fsPromises.writeFile(tmp, JSON.stringify(data));
  await fsPromises.rename(tmp, file);
}

async function loadStationCatalog() {
  const file = path.join(cacheRoot(), "stations.json");
  const cached = await readJsonCache(file, WFS_TTL_MS);
  if (cached?.stations?.length) return cached.stations;

  const geo = await fetchJson(STATIONS_WFS, 40000);
  const stations = [];
  for (const f of geo.features || []) {
    const props = f.properties || {};
    const blob = JSON.stringify(props);
    const m = blob.match(/SK(\d{5})/);
    const coords = f.geometry?.coordinates;
    if (!m || !coords || coords.length < 2) continue;
    stations.push({
      ind_kli: Number(m[1]),
      name: props.name || `SHMÚ ${m[1]}`,
      longitude: coords[0],
      latitude: coords[1],
      elevation_m: null,
    });
  }
  if (!stations.length) throw new Error("SHMÚ WFS returned no stations.");

  // Open-Meteo elevation accepts at most 100 coordinate pairs per request.
  const ELEV_BATCH = 80;
  for (let i = 0; i < stations.length; i += ELEV_BATCH) {
    const batch = stations.slice(i, i + ELEV_BATCH);
    try {
      const elev = await fetchJson(
        `${ELEVATION_URL}?latitude=${encodeURIComponent(batch.map((s) => s.latitude).join(","))}&longitude=${encodeURIComponent(batch.map((s) => s.longitude).join(","))}`
      );
      const arr = Array.isArray(elev.elevation) ? elev.elevation : [elev.elevation];
      batch.forEach((s, j) => {
        if (typeof arr[j] === "number") s.elevation_m = arr[j];
      });
    } catch {
      // Elevation is required for the 100 m proximity rule; stations without it will not be used for current.
    }
  }

  await writeJsonCache(file, { fetched_at: new Date().toISOString(), stations });
  return stations;
}

function ymd(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function awsFileTime(name) {
  const m = name.match(
    /aws1min\s*-\s*(\d{4})-(\d{2})-(\d{2})\s+(\d{2})-(\d{2})-(\d{2})\.json$/i
  );
  if (!m) return 0;
  return Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
}

async function findLatestAwsSnapshot() {
  const now = new Date();
  const days = [ymd(now), ymd(new Date(now.getTime() - 86400000))];
  // Filenames use local civil date; also try Europe/Bratislava today.
  try {
    const local = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Europe/Bratislava",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(now);
    days.unshift(local.replace(/-/g, ""));
  } catch {
    /* ignore */
  }
  const tried = new Set();
  for (const day of days) {
    if (tried.has(day)) continue;
    tried.add(day);
    let html;
    try {
      html = await fetchText(`${AWS_DATA_BASE}/${day}/`);
    } catch {
      continue;
    }
    const files = listHrefs(html).filter((h) => /aws1min.*\.json$/i.test(h));
    if (!files.length) continue;
    files.sort((a, b) => awsFileTime(a) - awsFileTime(b));
    const latest = files[files.length - 1];
    const stamp = awsFileTime(latest);
    return {
      url: `${AWS_DATA_BASE}/${day}/${encodeURIComponent(latest)}`,
      name: latest,
      listedAtMs: stamp,
    };
  }
  throw new Error("No SHMÚ AWS snapshot found for today or yesterday.");
}

function latestRecordByStation(rows) {
  const map = new Map();
  for (const row of rows) {
    const id = row.ind_kli;
    if (id == null) continue;
    const prev = map.get(id);
    if (!prev || String(row.minuta) > String(prev.minuta)) map.set(id, row);
  }
  return map;
}

async function fetchNearestStation(latitude, longitude, elevationMOrPromise) {
  const [stations, snapshot] = await Promise.all([loadStationCatalog(), findLatestAwsSnapshot()]);
  const payload = await fetchJson(snapshot.url, 40000);
  const elevationM = await Promise.resolve(elevationMOrPromise);
  const rows = Array.isArray(payload.data) ? payload.data : [];
  const latest = latestRecordByStation(rows);
  const catalog = new Map(stations.map((s) => [s.ind_kli, s]));

  const candidates = [];
  for (const [id, obs] of latest) {
    const meta = catalog.get(id);
    if (!meta || obs.t == null) continue;
    const distanceKm = haversineKm(latitude, longitude, meta.latitude, meta.longitude);
    const elev = meta.elevation_m;
    const elevDiff =
      elevationM != null && elev != null ? Math.abs(elevationM - elev) : null;
    candidates.push({ meta, obs, distanceKm, elevDiff });
  }
  candidates.sort((a, b) => a.distanceKm - b.distanceKm);

  // Both distance and elevation must be known and within the rule. A missing
  // station height must not promote a hill site (e.g. Koliba) over the airport.
  const qualifying = candidates.filter(
    (c) =>
      c.distanceKm <= SHMU_MAX_DISTANCE_KM &&
      c.elevDiff != null &&
      c.elevDiff <= SHMU_MAX_ELEVATION_DIFF_M
  );
  const chosen = qualifying[0] || null;
  return { chosen, nearest: candidates[0] || null, snapshot };
}

function windKmh(ms) {
  return typeof ms === "number" && Number.isFinite(ms) ? Math.round(ms * 3.6 * 10) / 10 : null;
}

// Reduce station pressure to MSL with the standard-atmosphere hypsometric
// formula (lapse rate 6.5 K/km) using station elevation and 2 m temperature.
function reducePressureToMsl(stationHpa, elevationM, tempC) {
  if (typeof stationHpa !== "number" || !Number.isFinite(stationHpa)) return null;
  if (typeof elevationM !== "number" || !Number.isFinite(elevationM)) return null;
  if (typeof tempC !== "number" || !Number.isFinite(tempC)) return null;
  if (Math.abs(elevationM) < 0.5) return Math.round(stationHpa * 10) / 10;
  const G = 9.80665;
  const RD = 287.05;
  const GAMMA = 0.0065;
  const tK = tempC + 273.15;
  if (tK <= 0) return null;
  const exponent = G / (RD * GAMMA);
  const tMsl = tK + GAMMA * elevationM;
  const msl = stationHpa * Math.pow(tMsl / tK, exponent);
  return Number.isFinite(msl) ? Math.round(msl * 10) / 10 : null;
}

function cloudFractionToPercent(v) {
  if (v == null || !Number.isFinite(v)) return null;
  // Files store 0-1 fractions (sometimes slightly over 1 from packing).
  const frac = v > 1.5 ? v / 100 : v;
  return Math.min(100, Math.max(0, Math.round(frac * 1000) / 10));
}

function totalCloudFromLayers(low, mid, high) {
  const toFrac = (p) => (p == null ? 0 : p / 100);
  if (low == null && mid == null && high == null) return null;
  const total = 1 - (1 - toFrac(low)) * (1 - toFrac(mid)) * (1 - toFrac(high));
  return Math.min(100, Math.max(0, Math.round(total * 1000) / 10));
}

function normalizeShmuCurrent(chosen, utcOffsetSeconds) {
  const { meta, obs, distanceKm, elevDiff } = chosen;
  const observed = obs.minuta ? new Date(`${obs.minuta}Z`) : null;
  const time =
    observed && !Number.isNaN(observed.getTime())
      ? toLocalIso(observed, utcOffsetSeconds ?? 0)
      : obs.minuta ?? null;
  const windMs = obs.vie_pr_rych ?? obs.vie_vp_rych;
  const gustMs = obs.vie_max_rych;
  const dir = obs.vie_vp_smer ?? obs.vie_pr_smer;
  return {
    time,
    temperature_c: obs.t ?? null,
    apparent_temperature_c: null,
    humidity_percent: typeof obs.vlh_rel === "number" ? Math.round(obs.vlh_rel) : null,
    precipitation_mm: null,
    weather: null,
    weather_code: null,
    cloud_cover_percent: null,
    pressure_station_hpa: typeof obs.tlak === "number" ? obs.tlak : null,
    pressure_hpa: reducePressureToMsl(
      typeof obs.tlak === "number" ? obs.tlak : null,
      meta.elevation_m,
      obs.t
    ),
    wind_speed_kmh: windKmh(windMs),
    wind_direction_deg: typeof dir === "number" ? Math.round(dir) : null,
    wind_gusts_kmh: windKmh(gustMs),
    is_day: null,
    source: "SHMÚ AWS",
    station: {
      ind_kli: meta.ind_kli,
      name: meta.name,
      latitude: meta.latitude,
      longitude: meta.longitude,
      elevation_m: meta.elevation_m,
      distance_km: Math.round(distanceKm * 10) / 10,
      elevation_difference_m: elevDiff != null ? Math.round(elevDiff) : null,
      observed_time: obs.minuta ? `${obs.minuta}Z` : null,
      visibility_m: typeof obs.dohl === "number" ? obs.dohl : null,
    },
  };
}

async function listAladinRuns() {
  const html = await fetchText(`${ALADIN_BASE}/`);
  const days = listHrefs(html)
    .filter((h) => /^\d{8}$/.test(h))
    .sort();
  const runs = [];
  for (const day of days.slice(-4).reverse()) {
    let dayHtml;
    try {
      dayHtml = await fetchText(`${ALADIN_BASE}/${day}/`);
    } catch {
      continue;
    }
    const cycles = listHrefs(dayHtml)
      .filter((h) => /^(0000|0600|1200|1800)$/.test(h))
      .sort()
      .reverse();
    for (const cycle of cycles) runs.push({ day, cycle });
  }
  if (!runs.length) throw new Error("No SHMÚ ALADIN run directories found.");
  return runs;
}

async function listAladinHours(run) {
  const html = await fetchText(`${ALADIN_BASE}/${run.day}/${run.cycle}/`);
  const files = listHrefs(html).filter((h) => h.endsWith(".grb") || h.endsWith(".grib2"));
  const hours = [];
  for (const name of files) {
    const m = name.match(/al-grib_sk_(\d{3})-(\d{8})-(\d{4})-nwp-?\.grb$/i);
    if (!m) continue;
    hours.push({ hour: Number(m[1]), name, url: `${ALADIN_BASE}/${run.day}/${run.cycle}/${name}` });
  }
  hours.sort((a, b) => a.hour - b.hour);
  return hours;
}

async function downloadCached(url, dest) {
  try {
    const st = await fsPromises.stat(dest);
    if (st.size > 1000) return dest;
  } catch {
    /* missing */
  }
  await fsPromises.mkdir(path.dirname(dest), { recursive: true });
  const buf = await fetchBuffer(url, 30000);
  const tmp = dest + ".tmp";
  await fsPromises.writeFile(tmp, buf);
  await fsPromises.rename(tmp, dest);
  return dest;
}

async function mapPool(items, limit, fn) {
  const out = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => worker()));
  return out;
}

function windFromUv(u, v) {
  if (u == null || v == null) return { speed_ms: null, direction_deg: null };
  const speed = Math.hypot(u, v);
  const direction = (Math.atan2(-u, -v) * 180) / Math.PI;
  return {
    speed_ms: speed,
    direction_deg: Math.round((direction + 360) % 360),
  };
}

function findField(messages, want) {
  for (const msg of messages) {
    if (!msg[4] || !msg[5] || !msg[6] || !msg[7]) continue;
    const p = grib2.parseProduct(msg[4]);
    if (p.category !== want.category || p.number !== want.number) continue;
    if (want.surfaceType != null && p.surfaceType !== want.surfaceType) continue;
    if (want.height != null && p.height !== want.height) continue;
    if (want.discipline != null && msg.discipline !== want.discipline) continue;
    const unpacked = grib2.unpackSimple(msg[5], msg[6], msg[7]);
    return { product: p, ...unpacked };
  }
  return null;
}

function sample(field, grid, i, j) {
  if (!field) return null;
  return grib2.sampleBilinear(field.values, field.bitmap, grid, i, j);
}

const SURFACE = {
  t2: { category: 0, number: 0, surfaceType: 103, height: 2 },
  u10: { category: 2, number: 2, surfaceType: 103, height: 10 },
  v10: { category: 2, number: 3, surfaceType: 103, height: 10 },
  ugust: { category: 2, number: 23, surfaceType: 103, height: 10 },
  vgust: { category: 2, number: 24, surfaceType: 103, height: 10 },
  prmsl: { category: 3, number: 1, surfaceType: 101 },
  tp: { category: 1, number: 193, discipline: 0 },
  // Official metadata: lcc / mcc / "tcc" described as high cloud (vysoká oblačnosť).
  lcc: { category: 128, number: 186, discipline: 192 },
  mcc: { category: 128, number: 187, discipline: 192 },
  hcc: { category: 128, number: 164, discipline: 192 },
  cape: { category: 7, number: 6, surfaceType: 1 },
  rh925: { category: 1, number: 1, surfaceType: 100, height: 92500 },
};

async function extractHour(file, lat, lon, gridCache) {
  const buf = await fsPromises.readFile(file);
  const messages = grib2.parseMessages(buf);
  if (!messages.length) throw new Error(`No GRIB messages in ${path.basename(file)}`);
  const grid = grib2.parseGrid(messages[0][3]);
  const ij = grib2.latLonToIj(lat, lon, grid);
  const probe = findField(messages, SURFACE.t2);
  if (!probe || !grib2.inDomain(ij.i, ij.j, grid, probe.bitmap)) {
    const err = new Error("Point is outside the SHMÚ ALADIN 4.5 km domain.");
    err.code = "OUT_OF_DOMAIN";
    throw err;
  }
  if (gridCache) Object.assign(gridCache, grid, ij);

  const t2 = sample(findField(messages, SURFACE.t2), grid, ij.i, ij.j);
  const u10 = sample(findField(messages, SURFACE.u10), grid, ij.i, ij.j);
  const v10 = sample(findField(messages, SURFACE.v10), grid, ij.i, ij.j);
  const ug = sample(findField(messages, SURFACE.ugust), grid, ij.i, ij.j);
  const vg = sample(findField(messages, SURFACE.vgust), grid, ij.i, ij.j);
  const prmsl = sample(findField(messages, SURFACE.prmsl), grid, ij.i, ij.j);
  const tp = sample(findField(messages, SURFACE.tp), grid, ij.i, ij.j);
  const lcc = cloudFractionToPercent(sample(findField(messages, SURFACE.lcc), grid, ij.i, ij.j));
  const mcc = cloudFractionToPercent(sample(findField(messages, SURFACE.mcc), grid, ij.i, ij.j));
  const hcc = cloudFractionToPercent(sample(findField(messages, SURFACE.hcc), grid, ij.i, ij.j));
  const cape = sample(findField(messages, SURFACE.cape), grid, ij.i, ij.j);
  const rh925 = sample(findField(messages, SURFACE.rh925), grid, ij.i, ij.j);
  const wind = windFromUv(u10, v10);
  const gust = windFromUv(ug, vg);
  const product = grib2.parseProduct(messages[0][4]);
  // Official product has no 2 m RH or dewpoint — only RH on pressure levels.
  let humidity925 = null;
  if (rh925 != null && Number.isFinite(rh925)) {
    humidity925 = Math.round((rh925 > 1.5 ? rh925 : rh925 * 100) * 10) / 10;
    humidity925 = Math.min(100, Math.max(0, humidity925));
  }
  return {
    forecastHour: product.forecastTime,
    temperature_c: t2 != null ? Math.round((t2 - 273.15) * 10) / 10 : null,
    humidity_percent: null,
    humidity_925hpa_percent: humidity925,
    wind_speed_kmh: windKmh(wind.speed_ms),
    wind_direction_deg: wind.direction_deg,
    wind_gusts_kmh: windKmh(gust.speed_ms),
    pressure_hpa: prmsl != null ? Math.round(prmsl / 10) / 10 : null, // Pa → hPa
    precipitation_acc_mm: tp != null ? Math.round(tp * 10) / 10 : null,
    cloud_cover_low_percent: lcc,
    cloud_cover_mid_percent: mcc,
    cloud_cover_high_percent: hcc,
    cloud_cover_percent: totalCloudFromLayers(lcc, mcc, hcc),
    cape_jkg: cape != null ? Math.round(cape) : null,
  };
}

function runStartUtc(run) {
  const y = Number(run.day.slice(0, 4));
  const m = Number(run.day.slice(4, 6));
  const d = Number(run.day.slice(6, 8));
  const hh = Number(run.cycle.slice(0, 2));
  return Date.UTC(y, m - 1, d, hh, 0, 0);
}

function aggregateDaily(hourly, utcOffsetSeconds) {
  const byDate = new Map();
  for (const h of hourly) {
    const date = h.time.slice(0, 10);
    let d = byDate.get(date);
    if (!d) {
      d = {
        date,
        temps: [],
        precips: [],
        winds: [],
        gusts: [],
      };
      byDate.set(date, d);
    }
    if (h.temperature_c != null) d.temps.push(h.temperature_c);
    if (h.precipitation_mm != null) d.precips.push(h.precipitation_mm);
    if (h.wind_speed_kmh != null) d.winds.push(h.wind_speed_kmh);
    if (h.wind_gusts_kmh != null) d.gusts.push(h.wind_gusts_kmh);
  }
  return [...byDate.values()].map((d) => ({
    date: d.date,
    temperature_max_c: d.temps.length ? Math.max(...d.temps) : null,
    temperature_min_c: d.temps.length ? Math.min(...d.temps) : null,
    apparent_temperature_max_c: null,
    apparent_temperature_min_c: null,
    weather: null,
    weather_code: null,
    sunrise: null,
    sunset: null,
    precipitation_sum_mm:
      Math.round(d.precips.reduce((a, b) => a + b, 0) * 10) / 10,
    precipitation_probability_max_percent: null,
    wind_speed_max_kmh: d.winds.length ? Math.max(...d.winds) : null,
    wind_gusts_max_kmh: d.gusts.length ? Math.max(...d.gusts) : null,
    wind_direction_dominant_deg: null,
  }));
}

async function fetchAladinForecast(latitude, longitude, days, utcOffsetSeconds, onlyRun) {
  const runs = onlyRun ? [onlyRun] : await listAladinRuns();
  let lastError = null;
  for (const run of runs) {
    try {
      const hours = await listAladinHours(run);
      const needed = hours.filter((h) => h.hour >= 1 && h.hour <= ALADIN_HOURS);
      if (!needed.length) continue;
      const dir = path.join(cacheRoot(), "aladin", `${run.day}${run.cycle}`);
      await fsPromises.mkdir(dir, { recursive: true });

      // Domain check on the first cached/downloaded hour only.
      const first = needed[0];
      const firstPath = await downloadCached(first.url, path.join(dir, first.name));
      const gridInfo = {};
      await extractHour(firstPath, latitude, longitude, gridInfo);

      const extracted = (await mapPool(needed, DOWNLOAD_CONCURRENCY, async (h) => {
        try {
          const dest = path.join(dir, h.name);
          await downloadCached(h.url, dest);
          const point = await extractHour(dest, latitude, longitude);
          return { hour: h.hour, point };
        } catch (err) {
          if (err && err.code === "OUT_OF_DOMAIN") throw err;
          return { hour: h.hour, error: err instanceof Error ? err.message : String(err) };
        }
      })).filter((row) => row && row.point);
      if (extracted.length < 12) {
        throw new Error(
          `SHMÚ ALADIN run ${run.day} ${run.cycle} produced too few hours (${extracted.length}).`
        );
      }

      const start = runStartUtc(run);
      const offset = Number(await Promise.resolve(utcOffsetSeconds)) || 0;
      const hourly = extracted.map(({ hour, point }) => {
        const utc = new Date(start + hour * 3600 * 1000);
        // tp (parameter 193, PDT 8) is a 1-hour accumulation, not a run total.
        const increment =
          point.precipitation_acc_mm != null
            ? Math.max(0, Math.round(point.precipitation_acc_mm * 10) / 10)
            : null;
        return {
          time: toLocalIso(utc, offset),
          temperature_c: point.temperature_c,
          apparent_temperature_c: null,
          humidity_percent: null,
          humidity_925hpa_percent: point.humidity_925hpa_percent,
          precipitation_mm: increment,
          precipitation_probability_percent: null,
          weather: null,
          weather_code: null,
          cloud_cover_percent: point.cloud_cover_percent,
          cloud_cover_low_percent: point.cloud_cover_low_percent,
          cloud_cover_mid_percent: point.cloud_cover_mid_percent,
          cloud_cover_high_percent: point.cloud_cover_high_percent,
          cape_jkg: point.cape_jkg,
          wind_speed_kmh: point.wind_speed_kmh,
          wind_direction_deg: point.wind_direction_deg,
          wind_gusts_kmh: point.wind_gusts_kmh,
          is_day: null,
          pressure_hpa: point.pressure_hpa,
        };
      });

      const daily = aggregateDaily(hourly, offset);
      const maxDays = days ?? 7;
      const dailyCut = daily.slice(0, maxDays);
      const lastDate = dailyCut.length ? dailyCut[dailyCut.length - 1].date : null;
      const hourlyCut = lastDate ? hourly.filter((h) => h.time.slice(0, 10) <= lastDate) : hourly;

      const now = Date.now();
      let current = hourlyCut[0] || null;
      for (const h of hourlyCut) {
        const t = Date.parse(h.time + ":00Z") - offset * 1000;
        if (!Number.isNaN(t) && t <= now) current = h;
      }

      return {
        modelId: "shmu_aladin",
        label: "SHMÚ ALADIN 4.5 km",
        note: "Official ALADIN/SHMÚ 4.5 km GRIB2 via opendata.shmu.sk (CC BY 4.0). Not CHMI ALADIN. 2 m relative humidity and dewpoint are not in the published GRIB set (RH is only on pressure levels; humidity_925hpa_percent is extracted). Precipitation probability is left null because A-LAEF ensemble GRIB is not published on opendata.shmu.sk — see the top-level ensemble block (ECMWF/GEFS) for probabilities and uncertainty bands. Total cloud is the random-overlap combination of the published low/mid/high layers.",
        run: `${run.day} ${run.cycle} UTC`,
        grid: {
          nx: gridInfo.nx,
          ny: gridInfo.ny,
          dx_m: gridInfo.dx,
          i: Math.round((gridInfo.i ?? 0) * 100) / 100,
          j: Math.round((gridInfo.j ?? 0) * 100) / 100,
        },
        current: current
          ? {
              ...current,
              source: "SHMÚ ALADIN 4.5 km",
              apparent_temperature_c: null,
            }
          : null,
        hourly: hourlyCut,
        daily: dailyCut,
        attribution: ATTRIBUTION,
      };
    } catch (err) {
      lastError = err;
      if (err.code === "OUT_OF_DOMAIN") throw err;
    }
  }
  throw lastError ?? new Error("Could not fetch a SHMÚ ALADIN run.");
}

module.exports = {
  ATTRIBUTION,
  SHMU_MAX_DISTANCE_KM,
  SHMU_MAX_ELEVATION_DIFF_M,
  AWS_MAX_AGE_MS,
  fetchNearestStation,
  normalizeShmuCurrent,
  fetchAladinForecast,
  reducePressureToMsl,
};
