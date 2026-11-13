"use strict";

const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");

const USER_AGENT = "open-meteo-weather/0.1.0 (Agent Plugin)";
const FEED_BASE = "https://feeds.meteoalarm.org/api/v1/warnings/feeds";
const NOMINATIM = "https://nominatim.openstreetmap.org/reverse";
const FEED_TTL_MS = 10 * 60 * 1000;
const GEOCODE_TTL_MS = 24 * 60 * 60 * 1000;

const COUNTRY_FEEDS = {
  sk: "slovakia",
  at: "austria",
  cz: "czechia",
  hu: "hungary",
  pl: "poland",
  ua: "ukraine",
  de: "germany",
};

const SEVERITY_LEVEL = {
  Extreme: "red",
  Severe: "orange",
  Moderate: "yellow",
  Minor: "yellow",
  Unknown: null,
};

function cacheRoot() {
  return path.join(os.tmpdir(), "open-meteo-weather-shmu", "meteoalarm");
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\b(okres|district|bezirk|kraj|region|stadt)\b/g, " ")
    .replace(/[()]/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function readJsonCache(file, ttlMs) {
  try {
    const st = await fs.stat(file);
    if (Date.now() - st.mtimeMs > ttlMs) return null;
    return JSON.parse(await fs.readFile(file, "utf8"));
  } catch {
    return null;
  }
}

async function writeJsonCache(file, data) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const tmp = file + ".tmp";
  await fs.writeFile(tmp, JSON.stringify(data));
  await fs.rename(tmp, file);
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: { Accept: "application/json", "User-Agent": USER_AGENT },
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

async function reverseGeocode(latitude, longitude) {
  const key = `${latitude.toFixed(3)}_${longitude.toFixed(3)}`;
  const file = path.join(cacheRoot(), `rev-${key}.json`);
  const cached = await readJsonCache(file, GEOCODE_TTL_MS);
  if (cached) return cached;
  const url = new URL(NOMINATIM);
  url.searchParams.set("lat", String(latitude));
  url.searchParams.set("lon", String(longitude));
  url.searchParams.set("format", "jsonv2");
  url.searchParams.set("addressdetails", "1");
  url.searchParams.set("zoom", "10");
  const res = await fetch(url, {
    headers: { Accept: "application/json", "User-Agent": USER_AGENT },
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`Nominatim HTTP ${res.status}`);
  const body = await res.json();
  await writeJsonCache(file, body);
  return body;
}

function regionTokens(rev, place) {
  const addr = rev?.address || {};
  const raw = [
    rev?.name,
    addr.city_district,
    addr.county,
    addr.municipality,
    addr.city,
    addr.town,
    addr.village,
    addr.state_district,
    place?.name,
    place?.admin2,
    place?.admin1,
  ].filter(Boolean);
  const tokens = new Set();
  for (const r of raw) {
    const n = norm(r);
    if (n) tokens.add(n);
  }
  return tokens;
}

function areaMatches(areaDesc, tokens) {
  const area = norm(areaDesc);
  if (!area) return false;
  if (tokens.has(area)) return true;
  // "Bratislava I" should not match a token that is only "bratislava".
  return false;
}

function pickInfo(infos) {
  if (!Array.isArray(infos) || !infos.length) return null;
  return (
    infos.find((i) => String(i.language || "").toLowerCase().startsWith("en")) ||
    infos[0]
  );
}

function parseTime(s) {
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function awarenessLevel(severity, headline) {
  const fromTitle = String(headline || "").match(/\b(red|orange|yellow|green)\b/i);
  if (fromTitle) return fromTitle[1].toLowerCase();
  return SEVERITY_LEVEL[severity] ?? null;
}

async function loadFeed(slug) {
  const file = path.join(cacheRoot(), `feed-${slug}.json`);
  const cached = await readJsonCache(file, FEED_TTL_MS);
  if (cached) return cached;
  const data = await fetchJson(`${FEED_BASE}-${slug}`);
  await writeJsonCache(file, data);
  return data;
}

function normalizeAlert(warning) {
  const alert = warning.alert || warning;
  const info = pickInfo(alert.info);
  if (!info) return null;
  const area = (info.area && info.area[0]) || {};
  const emma = (area.geocode || []).find((g) => g.valueName === "EMMA_ID") || {};
  const expires = info.expires || null;
  const expDt = parseTime(expires);
  if (expDt && expDt.getTime() <= Date.now()) return null;
  if (alert.status && alert.status !== "Actual") return null;
  return {
    id: alert.identifier || warning.uuid || null,
    event: info.event || null,
    headline: info.headline || null,
    description: info.description || null,
    instruction: info.instruction || null,
    severity: info.severity || null,
    awareness_level: awarenessLevel(info.severity, info.headline),
    certainty: info.certainty || null,
    urgency: info.urgency || null,
    onset: info.onset || null,
    effective: info.effective || null,
    expires: expires,
    area: area.areaDesc || null,
    emma_id: emma.value || null,
    language: info.language || null,
    sender: info.senderName || alert.sender || "MeteoAlarm",
    url: info.web || (area.areaDesc
      ? `https://meteoalarm.org?geocode=EMMA_ID:${emma.value || ""}`
      : "https://meteoalarm.org"),
  };
}

async function fetchAlerts(latitude, longitude, place) {
  const countryCode = String(
    place?.country_code || ""
  ).toLowerCase();
  let rev = null;
  let revError = null;
  try {
    rev = await reverseGeocode(latitude, longitude);
  } catch (err) {
    revError = err instanceof Error ? err.message : String(err);
  }
  const cc = (rev?.address?.country_code || countryCode || "").toLowerCase();
  const slug = COUNTRY_FEEDS[cc];
  if (!slug) {
    return {
      region: null,
      country_code: cc || null,
      alerts: [],
      attribution: attribution(),
      note: cc
        ? `No MeteoAlarm country feed is configured for ${cc}.`
        : "Could not determine the country for MeteoAlarm.",
      warning: revError || undefined,
    };
  }

  const tokens = regionTokens(rev, place);
  const feed = await loadFeed(slug);
  const warnings = Array.isArray(feed.warnings) ? feed.warnings : [];
  const matched = [];
  for (const w of warnings) {
    const info = pickInfo(w.alert?.info);
    const areaDesc = info?.area?.[0]?.areaDesc;
    if (!areaMatches(areaDesc, tokens)) continue;
    const rec = normalizeAlert(w);
    if (rec) matched.push(rec);
  }
  matched.sort((a, b) => {
    const order = { red: 0, orange: 1, yellow: 2, green: 3 };
    return (order[a.awareness_level] ?? 9) - (order[b.awareness_level] ?? 9);
  });

  const regionName =
    rev?.name ||
    rev?.address?.city_district ||
    rev?.address?.county ||
    rev?.address?.city ||
    place?.name ||
    null;

  return {
    region: regionName,
    country_code: cc,
    emma_tokens: [...tokens],
    alerts: matched,
    attribution: attribution(),
    note:
      matched.length === 0
        ? `No active MeteoAlarm warnings for ${regionName || "this location's region"} (expired or other-region warnings are omitted).`
        : undefined,
  };
}

function attribution() {
  return {
    name: "MeteoAlarm (EUMETNET)",
    license: "CC BY 4.0",
    url: "https://meteoalarm.org",
    feeds: "https://feeds.meteoalarm.org",
    credit: "Warning data from MeteoAlarm / participating national meteorological services.",
  };
}

module.exports = { fetchAlerts, COUNTRY_FEEDS };
