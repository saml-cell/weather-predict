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

SHMÚ data is published under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.en). Attribute the Slovak Hydrometeorological Institute. The server stores your IP for at most 30 days; see https://opendata.shmu.sk/README.txt . This app only requests the open files (station list, minute observations, ALADIN GRIB2). It does not send them to anyone else.

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

Do not trust a number that this process did not compute. Run:

```bash
python3 scripts/backtest_accuracy.py
```

That writes `reports/bratislava_accuracy.json` for 48.15, 17.11.

- Daily maximum and minimum temperature: MAE and RMSE against the Open-Meteo archive.
- Precipitation: a wet day is at least 1 mm. Brier score uses Open-Meteo’s probability. ECMWF in this replay is deterministic, so its probability is 1 or 0 at the same 1 mm threshold. Hit rate is the fraction of correct wet/dry calls at probability 0.5.
- The test-window ensemble weights are inverse-MAE of daily maximum temperature, fit only on the earlier train days.
- SHMÚ station minutes (summed and min/maxed on the civil date) are the Slovak observation check. ALADIN is the previous day’s 00 UTC run.

wttr.in, OpenWeatherMap, WeatherAPI and Visual Crossing are in the live blend. They are not in the backtest: there is no keyless archive of their past forecasts. The report says this in `notes`.

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
