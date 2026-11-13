---
name: weather
description: Use when the user asks for weather, a forecast, current conditions, air quality, or to resolve a place for weather. Call the Open-Meteo plugin tools; never invent weather data.
---

# Weather (Meteric-aligned public sources)

Call the plugin MCP tools before answering any weather question. Do not invent, estimate, or recall temperatures, precipitation, wind, air quality, alerts, or conditions from memory or training data.

## When to call which tool

- **get_forecast** — current conditions or a forecast. Pass `place` (for example `"Bratislava"`) and optional `days` (1–16, default 7). If the user already gave coordinates, pass `latitude` and `longitude`. Optional `model`: `best_match` (default primary series; Open-Meteo's automatic model mix), `ecmwf`, `icon`, `aladin` (Open-Meteo CHMI ALADIN, when in domain), `shmu_aladin` (official SHMÚ ALADIN 4.5 km, when in the Slovak domain), or `all`.
- **search_location** — the place name is ambiguous, or the user only wants to resolve a location. If they still want a forecast, follow up with `get_forecast`.

## How to read the result

The core Forecast shape is `location`, `timezone`, `current`, `hourly`, `daily`.

- `current.source` is `SHMÚ AWS` when a nearby Slovak automatic station was used (name, distance, elevation difference, and observed time are on `current.station`), `METAR` when a nearby airport observation was used, otherwise an Open-Meteo or SHMÚ model id.
- `current.field_sources` says where each current field came from. A SHMÚ AWS current may have weather or cloud cover filled from METAR or a model — say so, and do not describe those as station measurements. `pressure_hpa` on AWS is reduced to MSL; `pressure_station_hpa` is the raw station reading.
- Prefer SHMÚ AWS wording only for fields actually sourced from SHMÚ AWS. Do not describe a METAR as an SHMÚ station reading, or the reverse.
- `models` holds side-by-side series. `shmu_aladin` is official SHMÚ ALADIN 4.5 km. Its `humidity_percent` and `precipitation_probability_percent` are null on purpose (no 2 m RH / dewpoint and no A-LAEF in the open GRIB). Use `humidity_925hpa_percent` if you need ALADIN humidity, and the top-level `ensemble` block for precipitation probability and uncertainty bands (ECMWF IFS ensemble or GEFS). `chmi_aladin_seamless` is CHMI ALADIN via Open-Meteo, not SHMÚ's ALADIN.
- `alerts` are official MeteoAlarm warnings for the location's region only. An empty list means no active warning for that region, not "no warnings exist in the country." Credit MeteoAlarm / the national service.
- `air_quality` comes from Open-Meteo CAMS. This is not SHMÚ's official air-quality index.
- If a model, ensemble, or observation is missing, say so (see `fetch_warnings`). Do not fill it in.
- When SHMÚ data is shown, credit SHMÚ (CC BY 4.0).

## Rules

- Never invent SHMÚ station measurements, SHMÚ ALADIN/A-LAEF, radar, or warnings. If the tool did not return them, they are not available.
- Never invent a precipitation probability for `shmu_aladin`. Use `ensemble` when the user asks about chance of rain or spread.
- Keep the tool's Celsius, mm, and km/h values unless the user asked for other units, and label the units in the answer.
