# Weather Predict Program

> Multi-source weather prediction system with ML models and physics corrections

## Project Overview

A **production weather intelligence system** combining:
- **5 weather APIs** (Open-Meteo, wttr.in, OWM, WeatherAPI, Visual Crossing)
- **4 ML models** (Analog, Ridge Regression, Composite, ECMWF SEAS5) + BMA
- **18+ climate indices** from NOAA
- **Bayesian accuracy weighting** with daily verification
- **Physics-informed corrections** (dew point, pressure trends)
- **React dashboard** with Flask API backend
- **Alert notification system** (webhook, file log, stdout)

## Architecture

```
scripts/
  fetch_weather.py      — Multi-source API data collection (5 sources)
  climate_indices.py    — NOAA teleconnection index downloader
  seasonal_model.py     — 4 ML prediction models + BMA
  seasonal_forecast.py  — Forecast generation pipeline
  weighted_forecast.py  — Bayesian consensus forecasting
  meteo.py              — Meteorological physics engine
  verify_and_score.py   — Accuracy verification & scoring
  alerts.py             — Weather alert detection & notification
  db.py                 — SQLite database layer (thread-local pooling)
  api.py                — Flask REST API server
  orchestrate.py        — Pipeline orchestrator (parallel city processing)
  init_db.py            — Database initialization
  add_city.py           — City management
  collect_forecasts.py  — Data collection orchestrator
  __init__.py           — Package marker
dashboard/
  index.html            — React 18 SPA (Tailwind, Leaflet, Chart.js)
tests/
  test_meteo.py         — Physics module unit tests
  test_db.py            — Database layer tests
data/
  weather.db            — SQLite database
config.json             — All configuration (APIs, scoring, physics, models)
```

## Pipeline

```
1. Fetch weather from 5 APIs     → Store in weather.db
2. Download NOAA climate indices  → 18+ teleconnection indices
3. Run 4 ML models + BMA         → Generate seasonal predictions
4. Compare vs actual observations → Update Bayesian accuracy weights
5. Check alerts                   → Notify on extreme weather
6. Serve Flask API + dashboard    → http://localhost:5000
```

## Behavioral Rules

- ALWAYS read a file before editing it
- NEVER save working files to root folder
- Use `scripts/` for Python code, `dashboard/` for frontend, `tests/` for tests
- NEVER commit API keys or secrets
- Run `python scripts/init_db.py` before first use
- The database is `data/weather.db` — do not delete it

## Running the System

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Initialize database
python scripts/init_db.py

# 3. Run full pipeline (fetch + model + verify)
python scripts/orchestrate.py --step all

# 4. Start API server + dashboard
python scripts/api.py
# → http://localhost:5000

# Production: use gunicorn
gunicorn -w 2 -b 0.0.0.0:5000 scripts.api:app
```

## Running Full-Time (cron + systemd)

```bash
# Data collection every 6 hours
0 */6 * * * cd "/home/samko/Weather predict program" && python3 scripts/orchestrate.py --step fetch
30 */6 * * * cd "/home/samko/Weather predict program" && python3 scripts/collect_forecasts.py

# Verify & score daily at midnight
0 0 * * * cd "/home/samko/Weather predict program" && python3 scripts/verify_and_score.py

# NOAA indices weekly (Sunday 3am)
0 3 * * 0 cd "/home/samko/Weather predict program" && python3 scripts/climate_indices.py --force

# Check alerts after each fetch
15 */6 * * * cd "/home/samko/Weather predict program" && python3 scripts/alerts.py

# Dashboard API runs as systemd user service: weather-api.service
```

## Running Tests

```bash
cd "/home/samko/Weather predict program"
python -m pytest tests/ -v
```

## API Endpoints

- `GET /api/cities` — List tracked cities
- `POST /api/cities` — Add a city
- `GET /api/forecast/<city_id>` — Weighted ensemble forecast
- `GET /api/observations/<city_id>` — Historical observations
- `GET /api/accuracy/<city_id>` — Source accuracy weights
- `GET /api/trends/<city_id>` — Time-series trends
- `GET /api/compare/<city_id>` — Source comparison for today
- `GET /api/seasonal/<city_id>` — Seasonal outlook
- `GET /api/indices` — Current teleconnection state
- `GET /api/alerts` — Weather alerts (optional `?city_id=N`)
- `POST /api/cache/invalidate` — Clear forecast cache
- `GET /api/health` — System health check

## Key Configuration (config.json)

- `default_cities`: Cities to track (default: Bratislava, New York, London, Tokyo, Sydney)
- `scoring.window_days`: Days of history for accuracy scoring (default: 30)
- `scoring.composite_weights`: Metric importance weights
- `physics.max_precip_adjustment_pct`: Max physics correction for precipitation
- `seasonal.indices`: NOAA climate index URLs and parameters
- `seasonal.bma`: Bayesian Model Averaging weights

## Environment Variables (optional)

- `OPENWEATHER_API_KEY` — OpenWeatherMap free tier
- `WEATHERAPI_KEY` — WeatherAPI.com free tier
- `VISUAL_CROSSING_KEY` — Visual Crossing free tier
