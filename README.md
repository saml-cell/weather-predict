# Weather Predict

A multi-source weather prediction system that combines 5 weather APIs, machine learning models, and meteorological physics to produce accurate, transparent forecasts with a live dashboard.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Flask](https://img.shields.io/badge/Flask-3.x-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## What It Does

Instead of trusting a single weather source, this system **fetches forecasts from 5 different APIs**, scores them against actual observations, and builds a **Bayesian-weighted ensemble** that automatically learns which source is most accurate for each city and metric. On top of that, a **physics correction layer** applies meteorological formulas (dew point, pressure trends, Clausius-Clapeyron) to catch things the raw data misses.

### Key Features

- **5 Weather Sources** — Open-Meteo, wttr.in, OpenWeatherMap, WeatherAPI, Visual Crossing
- **Bayesian Accuracy Weighting** — Each source earns trust based on its track record, with exponential decay favoring recent performance
- **Physics-Informed Corrections** — Dew point depression, pressure tendency (Buys-Ballot), heat index (NWS Rothfusz), wind chill, Clausius-Clapeyron precipitation scaling
- **Seasonal Outlooks** — 1-12 month forecasts using 4 ML models (analog matching, ridge regression, composite, ECMWF SEAS5) combined with Bayesian Model Averaging
- **18+ Climate Indices** — ENSO, NAO, AO, PDO, AMO, QBO, IOD, and more from NOAA
- **Live Dashboard** — React 18 SPA with interactive maps (Leaflet), charts (Chart.js), and source comparison
- **Weather Alerts** — Automatic detection of extreme heat, cold, wind, and precipitation with webhook notifications
- **Full Transparency** — Every forecast shows which sources contributed, their weights, and what physics corrections were applied

## How It Works

```
┌──────────────────────────────────────────────────────────┐
│                    DATA COLLECTION                        │
│  Open-Meteo · wttr.in · OWM · WeatherAPI · VisualCrossing│
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│              LAYER 1: BAYESIAN ENSEMBLE                   │
│  Per-source, per-metric accuracy weights                  │
│  Exponential decay (15-day half-life)                     │
│  Cold-start handling with equal weights                   │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│           LAYER 2: PHYSICS CORRECTIONS                    │
│  Dew point → precipitation adjustment                     │
│  Pressure trend → weather change prediction               │
│  Clausius-Clapeyron → precipitation intensity             │
│  Heat index / Wind chill → feels-like temperature         │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│              VERIFICATION & SCORING                       │
│  Fetch actual observations from archive API               │
│  Score each source against reality                        │
│  Update Bayesian weights for next cycle                   │
└──────────────────────────────────────────────────────────┘
```

### Seasonal Forecasting Pipeline

For longer-range outlooks (1-12 months), the system uses a separate pipeline:

1. **Climate Index Analysis** — Downloads 18+ teleconnection indices from NOAA (ONI, NAO, AO, PDO, AMO, etc.)
2. **Analog Method** — Finds the 10 most similar historical climate patterns and averages their outcomes
3. **Ridge Regression** — Statistical model trained on 30+ years of index-weather relationships
4. **Composite Method** — Threshold-based classification (e.g., "strong El Nino winters tend to be warmer")
5. **ECMWF SEAS5** — European Centre dynamical model output via Open-Meteo
6. **Bayesian Model Averaging** — Combines all 4 methods with learned weights

## Quick Start

```bash
# Clone
git clone https://github.com/saml-cell/weather-predict.git
cd weather-predict

# Install dependencies
pip install -r requirements.txt

# Initialize the database
python scripts/init_db.py

# Add cities to track
python scripts/add_city.py Bratislava "New York" London Tokyo Sydney

# Run the full pipeline (fetch → index → model → verify)
python scripts/orchestrate.py --step all

# Start the dashboard
python scripts/api.py
# → Open http://localhost:5000
```

### Optional: Add API Keys for More Sources

The system works out of the box with 2 free sources (Open-Meteo, wttr.in). To add more, set environment variables:

```bash
export OPENWEATHER_API_KEY=your_key    # https://openweathermap.org/api
export WEATHERAPI_KEY=your_key          # https://weatherapi.com
export VISUAL_CROSSING_KEY=your_key     # https://visualcrossing.com
```

## Running in Production

### Cron Schedule

```bash
# Fetch new forecasts every 6 hours
0 */6 * * * cd /path/to/weather-predict && python3 scripts/orchestrate.py --step fetch

# Collect and archive forecasts
30 */6 * * * cd /path/to/weather-predict && python3 scripts/collect_forecasts.py

# Verify accuracy and update weights daily at midnight
0 0 * * * cd /path/to/weather-predict && python3 scripts/verify_and_score.py

# Refresh NOAA climate indices weekly
0 3 * * 0 cd /path/to/weather-predict && python3 scripts/climate_indices.py --force

# Check for weather alerts
15 */6 * * * cd /path/to/weather-predict && python3 scripts/alerts.py
```

### Systemd Service (Dashboard)

```ini
# ~/.config/systemd/user/weather-api.service
[Unit]
Description=Weather Predict Dashboard API

[Service]
WorkingDirectory=/path/to/weather-predict
ExecStart=/usr/bin/python3 scripts/api.py
Restart=always

[Install]
WantedBy=default.target
```

```bash
systemctl --user enable --now weather-api.service
```

### Production Server

For production use, run with gunicorn instead of Flask's dev server:

```bash
gunicorn -w 2 -b 0.0.0.0:5000 scripts.api:app
```

## CLI Usage

```bash
# Fetch weather with consensus analysis
python scripts/fetch_weather.py Bratislava

# Weighted ensemble forecast (uses accuracy-based weights)
python scripts/weighted_forecast.py "New York" --json

# Seasonal outlook (3 months ahead)
python scripts/seasonal_forecast.py London --months 6

# Check current climate index state
python scripts/climate_indices.py --status

# Verify forecasts and update source weights
python scripts/verify_and_score.py --backfill 30

# Check for extreme weather alerts
python scripts/alerts.py --webhook https://your-webhook-url
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/cities` | GET | List all tracked cities |
| `/api/cities` | POST | Add a new city |
| `/api/forecast/<city_id>` | GET | Weighted ensemble forecast |
| `/api/observations/<city_id>` | GET | Historical observations |
| `/api/accuracy/<city_id>` | GET | Source accuracy weights |
| `/api/trends/<city_id>` | GET | Temperature, precip, pressure trends |
| `/api/compare/<city_id>` | GET | Side-by-side source comparison |
| `/api/seasonal/<city_id>` | GET | Seasonal outlook (1-12 months) |
| `/api/indices` | GET | Current teleconnection index state |
| `/api/alerts` | GET | Active weather alerts |
| `/api/cache/invalidate` | POST | Clear forecast cache |
| `/api/health` | GET | System health check |

## Project Structure

```
weather-predict/
├── scripts/
│   ├── fetch_weather.py        # Multi-source API data collection
│   ├── weighted_forecast.py    # Bayesian consensus forecasting
│   ├── meteo.py                # Meteorological physics engine
│   ├── verify_and_score.py     # Accuracy verification & weight updates
│   ├── climate_indices.py      # NOAA teleconnection index downloader
│   ├── seasonal_model.py       # 4 ML prediction models + BMA
│   ├── seasonal_forecast.py    # Seasonal outlook generator
│   ├── alerts.py               # Weather alert detection & notifications
│   ├── db.py                   # SQLite database layer (thread-local pooling)
│   ├── api.py                  # Flask REST API + dashboard server
│   ├── orchestrate.py          # Pipeline orchestrator (parallel processing)
│   ├── collect_forecasts.py    # Scheduled data collection
│   ├── init_db.py              # Database schema initialization
│   └── add_city.py             # City management utility
├── dashboard/
│   └── index.html              # React 18 SPA (Tailwind, Leaflet, Chart.js)
├── tests/
│   ├── test_meteo.py           # Physics module tests (41 tests)
│   └── test_db.py              # Database layer tests (12 tests)
├── data/
│   └── weather.db              # SQLite database (auto-created)
├── config.json                 # All configuration parameters
├── requirements.txt            # Python dependencies (pinned)
└── .env.example                # API key template
```

## Configuration

All tunable parameters live in `config.json`:

- **Scoring** — Window size, decay half-life, metric weights, minimum samples for Bayesian updates
- **Physics** — Maximum correction bounds for temperature and precipitation adjustments
- **Seasonal** — Climate index URLs, analog/regression/composite model parameters, BMA weights
- **Cities** — Default cities to track

## Currently Tracking

The system is currently running with 9 cities: Bratislava, Vienna, London, New York, Tokyo, Sydney, Banská Štiavnica, Bibinje, and Štrbské Pleso.

## Tech Stack

- **Backend**: Python 3.10+, Flask, NumPy, SciPy
- **Database**: SQLite with WAL mode and thread-local connection pooling
- **Frontend**: React 18, Tailwind CSS, Leaflet maps, Chart.js
- **ML**: Ridge regression, analog pattern matching, Bayesian Model Averaging
- **Data Sources**: Open-Meteo, wttr.in, OpenWeatherMap, WeatherAPI, Visual Crossing, NOAA PSL/CPC
