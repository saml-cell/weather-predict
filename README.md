# Weather Predict

A production weather intelligence system that combines **7 weather APIs**, **51-member ensemble forecasting**, **ML models**, and **meteorological physics** to produce accurate, transparent forecasts with a live dashboard.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Flask](https://img.shields.io/badge/Flask-3.x-green)
![License](https://img.shields.io/badge/License-MIT-yellow)
![Sources](https://img.shields.io/badge/Weather_Sources-7-orange)
![Tests](https://img.shields.io/badge/Tests-53_passing-brightgreen)

## What It Does

Instead of trusting a single weather source, this system **fetches forecasts from 7 different APIs**, scores them against actual observations, and builds a **Bayesian-weighted ensemble** that automatically learns which source is most accurate for each city and metric. On top of that:

- A **physics correction layer** applies meteorological formulas (dew point, pressure trends, Clausius-Clapeyron) with bias correction
- A **51-member ECMWF ensemble** provides confidence intervals and probabilistic precipitation forecasts
- **4 ML models + BMA** generate seasonal outlooks using 19 climate teleconnection indices
- An **animated hourly map** shows rain radar, wind arrows, and temperature gradients with time-lapse playback

### Key Features

- **7 Weather Sources** — Open-Meteo, wttr.in, OpenWeatherMap, WeatherAPI, Visual Crossing, NOAA NWS, ECMWF IFS
- **51-Member Ensemble** — ECMWF IFS ensemble with percentile confidence intervals (p10/p25/p75/p90)
- **Bayesian Accuracy Weighting** — Each source earns trust based on its track record, with exponential decay and bias correction
- **Physics-Informed Corrections** — Dew point depression, pressure tendency, heat index (NWS Rothfusz), wind chill, Clausius-Clapeyron (with NWP double-counting guard)
- **Seasonal Outlooks** — 1-12 month forecasts using 4 ML models combined with skill-score-weighted Bayesian Model Averaging
- **19 Climate Indices** — ENSO, NAO, AO, PDO, AMO, QBO, MJO, IOD, and more from NOAA, with season-stratified weighting
- **Air Quality Index** — EPA AQI scale, PM2.5, PM10, and pollutant data from WeatherAPI
- **Official Weather Alerts** — Severe weather warnings from weather services + physics-based threshold alerts (heat index, wind chill, multi-day flood risk)
- **Animated Hourly Map** — Time-lapse playback with rain radar (RainViewer), wind arrows, temperature gradient, and layer toggles
- **Live Dashboard** — React 18 SPA with interactive maps (Leaflet), charts (Chart.js), source comparison, and Learn Mode
- **Full Transparency** — Every forecast shows which sources contributed, their weights, RMSE, bias, and what corrections were applied

## How It Works

```
┌──────────────────────────────────────────────────────────┐
│                    DATA COLLECTION                        │
│  Open-Meteo · wttr.in · OWM · WeatherAPI · VisualCrossing│
│  NOAA NWS (US) · ECMWF IFS (51-member ensemble)         │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│          LAYER 1: BIAS-CORRECTED BAYESIAN ENSEMBLE        │
│  Per-source bias subtraction before averaging             │
│  Per-source, per-metric accuracy weights (MAE + RMSE)     │
│  Exponential decay (15-day half-life)                     │
│  Graduated condition scoring (adjacency-based similarity) │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│           LAYER 2: PHYSICS CORRECTIONS                    │
│  Dew point → precipitation adjustment                     │
│  Pressure trend → weather change prediction               │
│  C-C scaling (analog/composite only, NWP guard)           │
│  Heat index / Wind chill → feels-like temperature         │
└──────────────┬───────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────────┐
│        ENSEMBLE ENRICHMENT & VERIFICATION                 │
│  51-member ECMWF confidence intervals                     │
│  Fetch actual observations from archive API               │
│  Score each source against reality (MAE + RMSE)           │
│  Update Bayesian weights for next cycle                   │
└──────────────────────────────────────────────────────────┘
```

### Seasonal Forecasting Pipeline

For longer-range outlooks (1-12 months):

1. **Climate Index Analysis** — Downloads 19 teleconnection indices from NOAA (ONI, NAO, AO, PDO, AMO, MJO, QBO, IOD, and more)
2. **Analog Method** — Finds the 10 most similar historical climate patterns with season-stratified index weighting (NAO 1.8x in winter, MJO 1.5x in DJF)
3. **Ridge Regression** — Z-score standardized predictors, leave-one-out cross-validation
4. **Composite Method** — ENSO + NAO stratified classification with sample-size confidence
5. **ECMWF SEAS5** — European Centre dynamical model output via Open-Meteo
6. **Skill-Weighted BMA** — Combines all 4 methods using historical RPSS/ACC verification scores (with 3-tier fallback)

## Quick Start

```bash
# Clone
git clone https://github.com/saml-cell/weather-predict.git
cd weather-predict

# Install dependencies
pip install -r requirements.txt

# Copy and edit environment variables
cp .env.example .env
# Edit .env to add your API keys (optional — works with free sources)

# Initialize the database
python scripts/init_db.py

# Add cities to track
python scripts/add_city.py "New York" London Tokyo Sydney Paris

# Run the full pipeline (fetch → index → model → verify)
python scripts/orchestrate.py --step all

# Start the dashboard
python scripts/api.py
# → Open http://localhost:5000
```

### API Keys (All Optional — Free Tiers)

The system works out of the box with 3 free sources (Open-Meteo, wttr.in, ECMWF via Open-Meteo). Add more for better ensemble accuracy:

| Source | Free Tier | Get Key |
|--------|-----------|---------|
| OpenWeatherMap | 1M calls/month | [openweathermap.org](https://openweathermap.org/api) |
| WeatherAPI | 1M calls/month, includes AQI + alerts | [weatherapi.com](https://www.weatherapi.com) |
| Visual Crossing | 1,000 records/day | [visualcrossing.com](https://www.visualcrossing.com/sign-up) |
| ECMWF CDS | Unlimited (for direct access) | [copernicus.eu](https://cds.climate.copernicus.eu) |
| NOAA NWS | Unlimited, US only | No key needed |

```bash
# Add to .env
OPENWEATHER_API_KEY=your_key
WEATHERAPI_KEY=your_key
VISUAL_CROSSING_KEY=your_key
ECMWF_API_KEY=your_key
```

## Dashboard

### Features
- **Interactive Map** with city markers, radar overlay (RainViewer), and city weather previews
- **Animated Hourly Mode** — play/pause time-lapse with rain, wind arrows, and temperature gradient layers
- **City Detail** — current conditions, 7-day forecast (card layout on mobile), source accuracy charts
- **Seasonal Outlook** — tercile probability charts, climate index state, teleconnection narratives
- **Learn Mode** — click any metric to understand the math behind it
- **Mobile-First** — responsive design with 44px touch targets, city switcher, pull-to-refresh, safe-area support

## API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/cities` | GET | No | List all tracked cities |
| `/api/cities` | POST | Key | Add a new city |
| `/api/cities/<id>` | DELETE | Key | Remove a city |
| `/api/forecast/<id>` | GET | No | Weighted ensemble forecast with confidence intervals |
| `/api/hourly/<id>` | GET | No | Hourly forecast data for map overlays |
| `/api/observations/<id>` | GET | No | Historical observations |
| `/api/accuracy/<id>` | GET | No | Source accuracy weights, MAE, RMSE, bias |
| `/api/trends/<id>` | GET | No | Temperature, precip, pressure trends |
| `/api/compare/<id>` | GET | No | Side-by-side source comparison |
| `/api/seasonal/<id>` | GET | No | Seasonal outlook (1-12 months) |
| `/api/indices` | GET | No | Current teleconnection index state (19 indices) |
| `/api/alerts` | GET | No | Official + threshold weather alerts |
| `/api/health` | GET | No | System health check |
| `/api/cache/invalidate` | POST | Key | Clear forecast cache |

## Running in Production

### Management CLI

```bash
weather-ctl doctor        # Full diagnostic
weather-ctl status        # Quick health check
weather-ctl restart       # Restart API (gunicorn, 2 workers)
weather-ctl logs 100      # Last 100 log lines
weather-ctl fetch         # Trigger data fetch
weather-ctl pipeline      # Full pipeline (fetch + model + verify)
weather-ctl verify        # Run verification & scoring
weather-ctl cleanup       # Prune old data
weather-ctl key           # Show API key
weather-ctl key regen     # Rotate API key
```

### Cron Schedule

```bash
0 */6 * * *  weather-ctl fetch              # Fetch forecasts every 6h
30 */6 * * * python3 scripts/collect_forecasts.py  # Archive forecasts
0 0 * * *    weather-ctl verify             # Verify accuracy at midnight
0 3 * * 0    python3 scripts/climate_indices.py --force  # Refresh indices weekly
15 */6 * * * python3 scripts/alerts.py      # Check alerts
```

### Systemd Service

```ini
# ~/.config/systemd/user/weather-api.service
[Unit]
Description=Weather Predict Dashboard API

[Service]
WorkingDirectory=/path/to/weather-predict
ExecStart=/usr/bin/gunicorn -w 2 -b 0.0.0.0:5000 scripts.api:app
Restart=always

[Install]
WantedBy=default.target
```

## Project Structure

```
weather-predict/
├── scripts/
│   ├── api.py                  # Flask REST API + dashboard server
│   ├── fetch_weather.py        # 7-source API data collection
│   ├── weighted_forecast.py    # Bias-corrected Bayesian ensemble
│   ├── meteo.py                # Meteorological physics engine
│   ├── verify_and_score.py     # Accuracy verification (MAE + RMSE)
│   ├── climate_indices.py      # 19 NOAA teleconnection indices + MJO
│   ├── seasonal_model.py       # 4 ML models + skill-weighted BMA
│   ├── seasonal_forecast.py    # Seasonal outlook generator
│   ├── alerts.py               # Official + threshold alert detection
│   ├── db.py                   # SQLite layer (thread-local, WAL mode)
│   ├── orchestrate.py          # Pipeline orchestrator
│   ├── collect_forecasts.py    # Scheduled data collection
│   ├── weather_telegram.py     # Telegram bot integration
│   ├── init_db.py              # Database schema + migrations
│   └── add_city.py             # City management utility
├── dashboard/
│   └── index.html              # React 18 SPA (Tailwind, Leaflet, Chart.js)
├── tests/
│   ├── test_meteo.py           # Physics module tests (41 tests)
│   └── test_db.py              # Database layer tests (12 tests)
├── data/                       # SQLite database (auto-created, gitignored)
├── config.json                 # All configuration parameters
├── requirements.txt            # Python dependencies
├── .env.example                # API key template
└── weather-ctl                 # Management CLI
```

## Tech Stack

- **Backend**: Python 3.10+, Flask, NumPy, SciPy
- **Database**: SQLite with WAL mode and thread-local connection pooling
- **Frontend**: React 18, Tailwind CSS, Leaflet maps, Chart.js
- **ML**: Ridge regression (z-score standardized), analog pattern matching, composite analysis, ECMWF SEAS5, Bayesian Model Averaging
- **Data Sources**: Open-Meteo, wttr.in, OpenWeatherMap, WeatherAPI, Visual Crossing, NOAA NWS, ECMWF IFS (ensemble)
- **Climate Data**: 19 NOAA teleconnection indices including ENSO, NAO, AO, PDO, AMO, MJO, QBO, IOD

## Data Attribution

Weather data provided by:
- [OpenWeather](https://openweathermap.org) (ODbL license)
- [WeatherAPI.com](https://www.weatherapi.com)
- [Visual Crossing](https://www.visualcrossing.com)
- [Open-Meteo](https://open-meteo.com) (CC BY 4.0)
- [NOAA National Weather Service](https://www.weather.gov)
- [ECMWF](https://www.ecmwf.int) via Open-Meteo
- [RainViewer](https://www.rainviewer.com) (radar tiles)

## License

MIT
