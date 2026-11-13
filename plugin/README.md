# open-meteo-weather

Minimal [Agent Plugin](https://agent-plugins.org/specification) that gives an agent current conditions and forecasts from the same **publicly documented** sources the iOS app [Meteric](https://meteric.bajzath.cloud) attributes — fetched only through public, no-auth APIs. This plugin does not reverse engineer Meteric, wrap its servers, or use Apple WeatherKit.

No API key is required for the implemented path. Node 18+ (`global fetch`) is enough; there are no npm dependencies.

## Tools

| Tool | When to use |
| --- | --- |
| `get_forecast` | Current conditions and a forecast for a `place` name and/or `latitude`/`longitude`. Optional `days` (1–16, default 7). Optional `model`: `best_match` (default primary), `ecmwf`, `icon`, `aladin` (CHMI via Open-Meteo), `shmu_aladin` (official SHMÚ ALADIN 4.5 km), or `all`. Named places are geocoded first. |
| `search_location` | Resolve a place `query` to candidate locations. |

`get_forecast` keeps the Forecast shape `{ location, timezone, current, hourly, daily }` and adds:

- `current.source` — `SHMÚ AWS` when a nearby Slovak automatic station qualifies (see rule below), else `METAR` when a nearby airport report qualifies, else the primary model.
- `current.station` — when an observation is used: station name, coordinates, distance (km), elevation difference (m), and observed time.
- `current.pressure_hpa` — mean sea level pressure. For SHMÚ AWS this is the published station pressure (`pressure_station_hpa`) reduced to MSL with the standard-atmosphere hypsometric formula (lapse 6.5 K/km, station elevation + temperature).
- `current.field_sources` — per-field provenance. AWS gaps (weather text, cloud cover, and optionally `is_day` / apparent temperature) are filled from the nearest qualifying METAR, then the primary model, and labelled as such. Measured AWS fields are never overwritten.
- `air_quality` — Open-Meteo CAMS (European / US AQI, pollutants, pollen).
- `models` — side-by-side normalized series for `best_match`, `ecmwf_ifs`, `icon_seamless`, `chmi_aladin_seamless` (when the CHMI domain covers the point), and `shmu_aladin` (when the official SHMÚ ALADIN domain covers the point).
- `ensemble` — uncertainty bands (min / p10 / p25 / p50 / p75 / p90 / max) and precipitation probability from the ECMWF IFS 0.25° ensemble (fallback: NCEP GEFS 0.25°) via the Open-Meteo Ensemble API. Probability is the fraction of members with ≥ 0.1 mm in that hour — never invented.
- `alerts` — active [MeteoAlarm](https://meteoalarm.org) (EUMETNET) warnings for the location's district/region, from the public country Atom/JSON feeds (Slovakia and neighbours: AT, CZ, HU, PL, UA, DE). Expired and other-region warnings are omitted.
- `sources` — attribution list for whatever was actually fetched, including SHMÚ and MeteoAlarm CC BY 4.0 credit when those products are used.
- `fetch_warnings` — present when an optional source failed or was out of domain. The tool does not crash in those cases.

## Current observation rule (SHMÚ AWS vs METAR)

Both observation sources use the same proximity rule Meteric publishes for METAR: the station must be within **25 km** of the requested point **and** within **100 m** of the point's elevation (elevation comes from Open-Meteo / the geocoder; SHMÚ's WFS catalog does not include elevation, so station heights are filled from the Open-Meteo DEM and cached).

When both a qualifying SHMÚ automatic weather station and a qualifying METAR exist, **SHMÚ AWS is preferred** for `current`. Observations older than 3 hours are not used. The chosen station's name, distance, elevation difference, and observed time (UTC) are included on `current.station`.

This matters in Bratislava: Koliba (ind_kli 11813) is closer to the city centre (~2 km) but sits ~130 m above the requested elevation, so it fails the 100 m check. Bratislava-letisko (11816, the airport AWS) is farther (~8 km) but within 100 m of elevation and is the station that qualifies.

Outside Slovakia there is no SHMÚ AWS in range, so `current` falls through to METAR or the model. A SHMÚ catalog fetch error is reported in `fetch_warnings` and does not fail the forecast.

The AWS `tlak` field is station pressure and is returned as `pressure_station_hpa`. `pressure_hpa` is that value reduced to MSL (hypsometric formula, 6.5 K/km lapse, using the station's DEM elevation and observed temperature). Weather text and cloud cover are not in the 1-minute AWS JSON; they are copied from the nearby METAR or the primary model and listed in `field_sources`.

## SHMÚ ALADIN 4.5 km (`shmu_aladin`)

Official ALADIN/SHMÚ 4.5 km GRIB2 from [opendata.shmu.sk](https://opendata.shmu.sk) (`meteorology/weather/nwp/aladin/sk/4.5km`). The plugin discovers the latest available run (00/06/12/18 UTC), downloads one ~160 KB file per forecast hour, and extracts 2 m temperature, 1-hour precipitation, 10 m wind, gusts, MSL pressure, CAPE, low/mid/high cloud, and 925 hPa relative humidity at the requested lat/lon (Lambert conformal bilinear sample) for up to 72 hours. Total cloud cover is the random-overlap combination of the three published layers (the official metadata lists those layers; a standalone total-cloud field is not provided).

**2 m relative humidity / dewpoint are not in the published GRIB set** — only RH on 925/850/700/500/250 hPa. The series therefore leaves `humidity_percent` null and exposes `humidity_925hpa_percent`. This is not filled in from another model.

**Precipitation probability is also left null on `shmu_aladin`.** SHMÚ's A-LAEF ensemble is not on the open-data portal (opendata.shmu.sk publishes only deterministic ALADIN 4.5 km under `nwp/`; A-LAEF is visualisation/epsgrams on shmu.sk). Probability and uncertainty bands come from the top-level `ensemble` block (ECMWF IFS ensemble, GEFS fallback), not as a fake A-LAEF product.

- Select it as the primary series with `model=shmu_aladin`. It is always added to the `models` comparison block when the point is inside the domain.
- **Domain:** the official Slovak 4.5 km grid only (roughly Slovakia and a thin border strip). Vienna and Prague are outside. Out-of-domain points get a `fetch_warnings` entry (`Point is outside the SHMÚ ALADIN 4.5 km domain.`) and no `models.shmu_aladin`; other models still work.
- Open-Meteo `chmi_aladin_seamless` remains available as `model=aladin` and is **not** SHMÚ ALADIN.

### Decoding and cache (no extra runtime dependency)

These GRIB2 files use **edition 2, simple packing (template 5.0)** and Lambert conformal grid 3.33 — not JPEG2000 or complex packing. A small pure-JS decoder in `server/grib2.js` unpacks them (WMO sign-and-magnitude scale factors, bitmap, Lambert projection). **eccodes, cfgrib, and any npm GRIB package are not required.** If a future file used packing this decoder does not implement, that hour is skipped; if too few hours remain, that run is abandoned and the previous run is tried; if nothing can be decoded, `shmu_aladin` is omitted with a warning and the rest of the tool still returns.

Downloaded GRIB files are cached per run under `${os.tmpdir()}/open-meteo-weather-shmu/aladin/<YYYYMMDD><HHMM>/` so repeat calls do not re-download the ~12 MB 72-hour set. The SHMÚ station catalog (WFS + elevations) is cached in the same temp root for 24 hours. Each forecast hour is a separate ~160 KB file, so the plugin never downloads a single huge combined GRIB.

## What Meteric publicly states vs what this plugin fetches

Meteric's official site and App Store listing name Open-Meteo (automatic best-match models, including ECMWF and ICON), ECMWF for mid-range in Slovakia, METAR from NOAA Aviation Weather Center, Open-Meteo CAMS air quality/pollen, and — in Slovakia — SHMÚ stations plus SHMÚ ALADIN / A-LAEF.

This plugin implements the keyless path among those: Open-Meteo forecast (`models=best_match`, `ecmwf_ifs`, `icon_seamless`, and Open-Meteo's CHMI ALADIN seamless when available), Open-Meteo geocoding, air-quality, and Ensemble API (ECMWF IFS / GEFS), METAR from `aviationweather.gov`, SHMÚ near-real-time AWS JSON (`climate/now`) via [opendata.shmu.sk](https://opendata.shmu.sk) and the official station WFS at `geo.shmu.sk`, official SHMÚ ALADIN 4.5 km GRIB2 as `shmu_aladin`, and MeteoAlarm public country feeds for warnings.

Radar, Windy webcams, MetericCast, and SHMÚ A-LAEF GRIB (not published as open data) are not implemented here. Official SHMÚ CAP files also exist under `opendata.shmu.sk/meteorology/weather/alerts/cap/`; this plugin uses the EUMETNET MeteoAlarm feeds so the same path works in Slovakia and neighbouring countries.

## Local use

`mcp.json` starts the stdio server with `command: node`, `args: ["./server/index.js"]`, `cwd: ${PLUGIN_ROOT}`. Requires Node 18+ (global `fetch`).

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0.1.0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' \
  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_forecast","arguments":{"place":"Bratislava","days":3}}}' \
  | node ./server/index.js
```

The first Bratislava call may take a minute while the latest 72-hour ALADIN run is cached; later calls reuse the temp cache.

## Cursor note

This package follows the Agent Plugins standard, including `${PLUGIN_ROOT}` in `mcp.json`. Cursor currently documents that it expands `${CURSOR_PLUGIN_ROOT}` and does not expand `${PLUGIN_ROOT}`. The standard schema does not allow `${CURSOR_PLUGIN_ROOT}` as `cwd`, so this plugin keeps `${PLUGIN_ROOT}`.

## Attribution

- Weather data from [Open-Meteo](https://open-meteo.com) (CC BY 4.0).
- METAR from NOAA Aviation Weather Center.
- Slovak station observations and ALADIN/SHMÚ 4.5 km forecasts from [SHMÚ](https://opendata.shmu.sk) (Slovak Hydrometeorological Institute), licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en). Credit: © SHMÚ.
- Weather warnings from [MeteoAlarm](https://meteoalarm.org) / EUMETNET and the participating national meteorological services (CC BY 4.0). Follow each provider's terms when you redistribute forecasts.
