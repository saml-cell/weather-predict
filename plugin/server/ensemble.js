"use strict";

const ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble";
const USER_AGENT = "open-meteo-weather/0.1.0 (Agent Plugin)";

const MODELS = [
  {
    id: "ecmwf_ifs025_ensemble",
    label: "ECMWF IFS 0.25° ensemble",
    note: "51-member ECMWF IFS ensemble via Open-Meteo. Used for uncertainty bands and precipitation probability because official SHMÚ A-LAEF GRIB is not published on opendata.shmu.sk (A-LAEF is visualisation-only on shmu.sk).",
  },
  {
    id: "ncep_gefs025",
    label: "NCEP GEFS 0.25° ensemble",
    note: "NOAA GEFS ensemble via Open-Meteo, used when the ECMWF ensemble is unavailable. Precipitation probability is the fraction of members with ≥ 0.1 mm in the hour.",
  },
];

const PRECIP_THRESHOLD_MM = 0.1;

function quantile(sorted, q) {
  if (!sorted.length) return null;
  if (sorted.length === 1) return sorted[0];
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return sorted[lo];
  return sorted[lo] * (hi - pos) + sorted[hi] * (pos - lo);
}

function round1(v) {
  return v == null || !Number.isFinite(v) ? null : Math.round(v * 10) / 10;
}

function memberKeys(hourly, prefix) {
  return Object.keys(hourly).filter((k) => k === prefix || k.startsWith(`${prefix}_member`));
}

function valuesAt(hourly, keys, i) {
  const out = [];
  for (const k of keys) {
    const v = hourly[k]?.[i];
    if (typeof v === "number" && Number.isFinite(v)) out.push(v);
  }
  return out;
}

function bands(values) {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  return {
    min: round1(s[0]),
    p10: round1(quantile(s, 0.1)),
    p25: round1(quantile(s, 0.25)),
    p50: round1(quantile(s, 0.5)),
    p75: round1(quantile(s, 0.75)),
    p90: round1(quantile(s, 0.9)),
    max: round1(s[s.length - 1]),
    members: values.length,
  };
}

function precipProbability(values) {
  if (!values.length) return null;
  const n = values.filter((v) => v >= PRECIP_THRESHOLD_MM).length;
  return Math.round((100 * n) / values.length);
}

function toLocalIso(iso, utcOffsetSeconds) {
  if (!iso) return iso;
  const utc = iso.endsWith("Z") || iso.includes("+") || iso.includes("-", 10)
    ? new Date(iso)
    : new Date(`${iso}Z`);
  if (Number.isNaN(utc.getTime())) return iso.slice(0, 16);
  const shifted = new Date(utc.getTime() + (utcOffsetSeconds ?? 0) * 1000);
  return shifted.toISOString().slice(0, 16);
}

async function fetchModel(modelId, latitude, longitude, days, utcOffsetSeconds) {
  const url = new URL(ENSEMBLE_URL);
  url.searchParams.set("latitude", String(latitude));
  url.searchParams.set("longitude", String(longitude));
  url.searchParams.set(
    "hourly",
    "temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m"
  );
  url.searchParams.set("models", modelId);
  url.searchParams.set("forecast_days", String(Math.min(Math.max(days ?? 7, 1), 16)));
  url.searchParams.set("timezone", "auto");
  url.searchParams.set("wind_speed_unit", "kmh");
  const res = await fetch(url, {
    headers: { Accept: "application/json", "User-Agent": USER_AGENT },
    signal: AbortSignal.timeout(30000),
  });
  const body = await res.json();
  if (!res.ok || body?.error) {
    throw new Error(body?.reason || `HTTP ${res.status}`);
  }
  const hourlyIn = body.hourly || {};
  const times = hourlyIn.time || [];
  const tempKeys = memberKeys(hourlyIn, "temperature_2m");
  const precKeys = memberKeys(hourlyIn, "precipitation");
  const windKeys = memberKeys(hourlyIn, "wind_speed_10m");
  const gustKeys = memberKeys(hourlyIn, "wind_gusts_10m");
  if (!times.length || tempKeys.length < 2) {
    throw new Error(`Ensemble ${modelId} returned too few members.`);
  }

  const offset = body.utc_offset_seconds ?? utcOffsetSeconds ?? 0;
  const hourly = times.map((t, i) => {
    const temps = valuesAt(hourlyIn, tempKeys, i);
    const precips = valuesAt(hourlyIn, precKeys, i);
    const winds = valuesAt(hourlyIn, windKeys, i);
    const gusts = valuesAt(hourlyIn, gustKeys, i);
    return {
      time: String(t).slice(0, 16),
      temperature_c: bands(temps),
      precipitation_mm: bands(precips),
      precipitation_probability_percent: precipProbability(precips),
      wind_speed_kmh: bands(winds),
      wind_gusts_kmh: bands(gusts),
    };
  });

  const byDate = new Map();
  // Per-member daily min/max/sum, then percentiles.
  const nMembers = Math.max(tempKeys.length, precKeys.length);
  for (let m = 0; m < nMembers; m++) {
    const tKey = tempKeys[m];
    const pKey = precKeys[m];
    const daily = new Map();
    for (let i = 0; i < times.length; i++) {
      const date = String(times[i]).slice(0, 10);
      let d = daily.get(date);
      if (!d) {
        d = { temps: [], precip: 0 };
        daily.set(date, d);
      }
      const tv = tKey ? hourlyIn[tKey]?.[i] : null;
      if (typeof tv === "number") d.temps.push(tv);
      const pv = pKey ? hourlyIn[pKey]?.[i] : null;
      if (typeof pv === "number") d.precip += pv;
    }
    for (const [date, d] of daily) {
      let acc = byDate.get(date);
      if (!acc) {
        acc = { date, mins: [], maxes: [], precips: [] };
        byDate.set(date, acc);
      }
      if (d.temps.length) {
        acc.mins.push(Math.min(...d.temps));
        acc.maxes.push(Math.max(...d.temps));
      }
      acc.precips.push(d.precip);
    }
  }

  const daily = [...byDate.values()].map((d) => {
    const dayHours = hourly.filter((h) => h.time.startsWith(d.date));
    const pops = dayHours
      .map((h) => h.precipitation_probability_percent)
      .filter((v) => v != null);
    return {
      date: d.date,
      temperature_min_c: bands(d.mins),
      temperature_max_c: bands(d.maxes),
      precipitation_sum_mm: bands(d.precips),
      precipitation_probability_max_percent: pops.length ? Math.max(...pops) : null,
    };
  });

  return {
    modelId,
    label: MODELS.find((m) => m.id === modelId)?.label || modelId,
    note: MODELS.find((m) => m.id === modelId)?.note,
    members: tempKeys.length,
    latitude: body.latitude,
    longitude: body.longitude,
    elevation_m: body.elevation,
    utc_offset_seconds: offset,
    hourly,
    daily,
  };
}

async function fetchEnsemble(latitude, longitude, days, utcOffsetSecondsOrPromise) {
  const utcOffsetSeconds = await Promise.resolve(utcOffsetSecondsOrPromise);
  let lastErr = null;
  for (const spec of MODELS) {
    try {
      return await fetchModel(spec.id, latitude, longitude, days, utcOffsetSeconds);
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr ?? new Error("No ensemble model available.");
}

module.exports = {
  fetchEnsemble,
  PRECIP_THRESHOLD_MM,
  MODELS,
};
