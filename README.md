# Weather Predict

A multi-source weather app. It fetches the public forecasts you already had (Open-Meteo, wttr.in, OpenWeatherMap, WeatherAPI, Visual Crossing, NOAA NWS, ECMWF), adds SHMÚ open data over Slovakia, and blends them with the existing accuracy weights and physics corrections.

The same JSON the website uses is the contract for a future iOS client.

## One-command local run

```bash
./scripts/dev.sh
```

Then open http://localhost:5000 . Bratislava is the default place:

http://localhost:5000/?lat=48.15&lon=17.11

`dev.sh` installs `requirements.txt`, copies `.env.example` to `.env` when needed, creates the SQLite database, and starts the API. Optional keys live only in `.env`, which is gitignored.

```bash
python3 -m pytest tests/ -q
```

## Sources and keys

| Source | Key | Role |
| --- | --- | --- |
| Open-Meteo | none | Forecast, geocoding, archive used for verification |
| wttr.in | none | Forecast |
| ECMWF IFS via Open-Meteo | none | Forecast plus 51-member spread |
| NOAA NWS | none | United States forecasts |
| SHMÚ ALADIN 4.5 km | none | Point forecast over Slovakia, from GRIB2 on opendata.shmu.sk |
| SHMÚ automatic stations | none | Current observation and Slovak verification |
| OpenWeatherMap | `OPENWEATHER_API_KEY` | Optional forecast |
| WeatherAPI | `WEATHERAPI_KEY` | Optional forecast, air quality, alerts |
| Visual Crossing | `VISUAL_CROSSING_KEY` | Optional forecast |

SHMÚ data is published with no registration under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en). Attribute the Slovak Hydrometeorological Institute. The server stores your IP for at most 30 days; see https://opendata.shmu.sk/README.txt .

ALADIN/SHMÚ is GRIB2 on a 4.5 km Lambert grid: four runs a day (00, 06, 12, 18 UTC), 72 hours at 1-hour steps, usually available 3–4 hours after the run. The forecast uses the newest run that has finished uploading, and only steps 0–72. Paths:

- `https://opendata.shmu.sk/meteorology/weather/nwp/aladin/sk/4.5km/YYYYMMDD/HHMM/`
- Field list: `https://opendata.shmu.sk/meteorology/weather/nwp/metadata/OpenData_AladinSHMU_metadata.json`
- Unvalidated 1-minute station JSON: `https://opendata.shmu.sk/meteorology/climate/now/`
- Validated daily climate JSON (about two months behind): `https://opendata.shmu.sk/meteorology/climate/recent/data/daily/`

Those validated daily files are the Slovak ground truth used in the committed accuracy report. Temperature there is the 21:00–21:00 local window and precipitation is 07:00–07:00. The minute feed is only the live observation and a short unvalidated check.

Live SHMÚ decoding is the pure-JS implementation in `plugin/server/grib2.js` and `plugin/server/shmu.js`. The Python app calls that CLI, so there is not a second GRIB decoder. ALADIN precipitation in those files is a 1-hour total. The open files have no 2 m relative humidity, and A-LAEF is not on opendata.shmu.sk, so this app does not invent either. A station is used only when it is within 25 km and 100 m of the point's elevation. Credit © SHMÚ, CC BY 4.0.

The same plugin also fetches Open-Meteo models, METAR, the ECMWF IFS ensemble, CAMS air quality, and MeteoAlarm warnings. MeteoAlarm data is CC BY 4.0; credit MeteoAlarm / EUMETNET and the national services when those warnings are shown.

Outside Slovakia, SHMÚ returns `out_of_domain` and the other sources still produce the forecast.

## HTTP API

`GET /api/v1/forecast?lat=48.15&lon=17.11`

`GET /api/v1/forecast?place=Bratislava`

`days` is optional (1–10, default 10).

The body has:

- `location`
- `current` with `ensemble`, `confidence` (`label`, `score`, `spread_c`) and `sources`
- `hourly` for the next 48 hours, each hour with `ensemble` and `sources`
- `daily` for up to 10 days, same split
- `sources` status list so a failed feed does not look like silence
- `attribution`

`SHMU` is the ALADIN forecast. `SHMU-OBS` is the nearest automatic station and is an observation, not a model. It votes on current conditions and is shown on its own. It is not used as a future forecast.

`GET /api/v1/geocode?q=Bratislava` returns place matches.

`GET /api/v1/accuracy` returns the last backtest file, `reports/bratislava_accuracy.json`.

Older city-id routes (`/api/forecast/<city_id>`, seasonal outlooks, alerts) are still mounted. The previous dashboard is at `/legacy`.

Responses are cached in memory for 15 minutes per rounded coordinate. ALADIN GRIB files and the parsed point series are cached on disk under `data/cache/` (gitignored). A source that errors is recorded in `sources` and dropped from the blend.

`POST` and `DELETE` still require `X-API-Key` when `WEATHER_API_KEY` is set.

## Accuracy

The numbers that count are the ones in `reports/bratislava_accuracy.json`. An older README table of MOS skill (about 1.1°C temperature MAE and similar humidity, wind and precipitation figures) came from `data/weather.db`. That database is gitignored and the table had no checked-in results file, so it is not repeated here.

Headline from the committed file, validated SHMÚ daily climate at Bratislava-Koliba, January–August 2026. Hours were aggregated to the station’s own windows. The July–August holdout was not used to set weights.

| Source | Test max MAE | Test n | Full-period max MAE | Full-period precip hit rate |
| --- | --- | --- | --- | --- |
| Open-Meteo | 1.598°C | 62 | 1.185°C (n=243) | 0.835 (103 days with a reported total) |
| ECMWF | 1.884°C | 62 | 1.525°C (n=243) | 0.825 |
| Equal-weight ensemble | 1.717°C | 62 | 1.337°C (n=243) | 0.825 |

The Open-Meteo archive analysis, scored the same way, has daily-max MAE 1.148°C against Koliba (n=243). Most of the forecast error above is the station-versus-grid gap, including hourly sampling of the peak, not lead time. Against the Open-Meteo archive at 48.15, 17.11 for 24 calendar days the equal-weight ensemble daily-max MAE is 0.275°C. That comparison is in the same JSON under `full_window_equal_weight` and is a different question (model versus its own analysis family).

Regenerate the report with:

```bash
python3 scripts/backtest_accuracy.py
```

It has three comparisons, all computed by that script:

- Open-Meteo and ECMWF historical runs versus the Open-Meteo archive at 48.15, 17.11, on calendar days.
- The same models versus SHMÚ validated daily climate at Bratislava-Koliba for January–August 2026. Hours are aggregated to SHMÚ’s 21:00–21:00 temperature window and 07:00–07:00 precipitation window before scoring. July–August is held out from the weight fit.
- A short unvalidated minute-station check of the previous day’s 00 UTC ALADIN run. ALADIN files on the server currently start on 2026-10-13, and the validated daily files currently end with 2026-08, so those two do not overlap.

A wet day is at least 1 mm. Hit rate is the fraction of correct wet/dry calls. wttr.in and the keyed commercial APIs stay in the live blend and out of this file: they have no keyless forecast archive.

## Deploy

Docker:

```bash
docker build -t weather-predict .
docker run --env-file .env -p 5000:5000 weather-predict
```

The image runs gunicorn with two workers and a 120 second timeout so the first Slovak request can download an ALADIN run.

Without Docker, from the repo root:

```bash
pip install -r requirements.txt
gunicorn -w 2 -b 0.0.0.0:5000 --timeout 120 scripts.api:app
```

Set `WEATHER_API_KEY` before the port is reachable beyond your own machine. `CORS_ORIGINS` can pin allowed origins. The database file is `data/weather.db`.

## What an iOS client can reuse

Keep using `GET /api/v1/forecast` and `GET /api/v1/geocode`. Temperatures are Celsius, wind is km/h, pressure is hPa, precipitation is millimetres, probabilities are percent. `units` repeats that. No API key is required for reads.

Still to do on the device, not in this repo:

- Sign in to Apple location services and call the same `lat`/`lon` endpoint.
- Cache the last successful payload for offline launch. The server cache is 15 minutes and is not a sync store.
- Map `condition` strings to SF Symbols.
- If you want the seasonal outlook or the tracked-city MOS adjustment, those remain on the older `/api/seasonal/<city_id>` and `/api/forecast/<city_id>` routes and need a city id in SQLite.

## Layout

```
scripts/api.py                HTTP server and the versioned routes
scripts/forecast_service.py   place resolution, cache, iOS-shaped JSON
scripts/weighted_forecast.py  Bayesian weights, physics, hourly blend
scripts/fetch_weather.py      existing forecast sources
scripts/shmu.py               SHMÚ stations, observations, ALADIN GRIB2
scripts/scoring.py            MAE, RMSE, Brier, hit rate
scripts/backtest_accuracy.py  writes the Bratislava report
dashboard/index.html          phone and desktop UI
dashboard/legacy.html         previous dashboard
```

## Tests

```bash
python3 -m pytest tests/ -q
```

The ensemble tests stub the network. They check that one live source still returns current, hourly and daily values when the others fail, and that the public JSON keeps per-source numbers beside the ensemble.
