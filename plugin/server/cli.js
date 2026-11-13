"use strict";

// Thin command line over server/shmu.js so the Python app uses this decoder
// instead of a second GRIB implementation. Stdout is one JSON object.

const shmu = require("./shmu");

function arg(name, fallback) {
  const i = process.argv.indexOf(name);
  if (i === -1 || i + 1 >= process.argv.length) return fallback;
  return process.argv[i + 1];
}

async function pointElevation(lat, lon) {
  const url =
    "https://api.open-meteo.com/v1/elevation?latitude=" +
    encodeURIComponent(lat) +
    "&longitude=" +
    encodeURIComponent(lon);
  const res = await fetch(url, {
    headers: { "User-Agent": "open-meteo-weather/0.1.0 (Agent Plugin)" },
    signal: AbortSignal.timeout(20000),
  });
  if (!res.ok) return null;
  const body = await res.json();
  const value = Array.isArray(body.elevation) ? body.elevation[0] : body.elevation;
  return typeof value === "number" ? value : null;
}

async function main() {
  const lat = Number(arg("--lat"));
  const lon = Number(arg("--lon"));
  const days = Number(arg("--days", "7"));
  const offset = Number(arg("--offset", "0"));
  const command = arg("--cmd", "bundle");
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error("lat and lon are required");
  }

  if (command === "aladin-run") {
    const day = arg("--day");
    const cycle = arg("--cycle", "0000");
    const forecast = await shmu.fetchAladinForecast(lat, lon, days, offset, { day, cycle });
    process.stdout.write(JSON.stringify({ aladin: forecast, attribution: shmu.ATTRIBUTION }));
    return;
  }

  const elevation = command === "aladin" ? null : await pointElevation(lat, lon);
  let observation = null;
  let observationWarning = null;
  if (command === "aladin") {
    observationWarning = null;
  } else try {
    const nearest = await shmu.fetchNearestStation(lat, lon, elevation);
    if (nearest.chosen) {
      observation = shmu.normalizeShmuCurrent(nearest.chosen, offset);
    } else if (nearest.nearest) {
      observationWarning =
        "No SHMÚ station within 25 km and 100 m of elevation. Nearest is " +
        nearest.nearest.meta.name +
        " at " +
        nearest.nearest.distanceKm.toFixed(1) +
        " km.";
    } else {
      observationWarning = "No SHMÚ station observation near this point.";
    }
  } catch (err) {
    observationWarning = err instanceof Error ? err.message : String(err);
  }

  let aladin = null;
  let aladinWarning = null;
  if (command !== "obs") {
    try {
      aladin = await shmu.fetchAladinForecast(lat, lon, days, offset);
    } catch (err) {
      aladinWarning = err instanceof Error ? err.message : String(err);
      if (err && err.code === "OUT_OF_DOMAIN") aladinWarning = "out_of_domain";
    }
  }

  process.stdout.write(
    JSON.stringify({
      observation,
      observation_warning: observationWarning,
      aladin,
      aladin_warning: aladinWarning,
      point_elevation_m: elevation,
      attribution: shmu.ATTRIBUTION,
    })
  );
}

main().catch((err) => {
  process.stderr.write((err && err.stack) || String(err));
  process.stderr.write("\n");
  process.exit(1);
});
