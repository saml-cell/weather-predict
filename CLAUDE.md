# Weather Predict Program

> Multi-source weather prediction system with ML models and physics corrections

## Claude: Start Here

**Every session, run this first:**
```bash
weather-ctl doctor
```
This gives you: service status, API health, data freshness, errors, disk, cron, access URLs.

**Then check what to work on:**
- Read `BACKLOG.md` for prioritized tasks
- Check MemPalace diary: `mempalace_diary_read agent_name="claude-code"`

**After making changes:**
- Run tests: `python3 -m pytest tests/ -v`
- Restart if you changed api.py: `weather-ctl restart`
- Write diary entry to MemPalace with what you did

## Management CLI

All operations go through `weather-ctl` (available globally via symlink):

```bash
weather-ctl doctor        # Full diagnostic (run first every session)
weather-ctl status        # Quick service + health check
weather-ctl url           # Get access URLs (Tailscale + tunnel)
weather-ctl restart       # Restart API (gunicorn, 2 workers)
weather-ctl logs 100      # Last 100 log lines
weather-ctl logs-follow   # Tail logs live
weather-ctl fetch         # Trigger data fetch
weather-ctl pipeline      # Full pipeline (fetch + model + verify)
weather-ctl verify        # Run verification & scoring
weather-ctl cleanup       # Prune old data
weather-ctl key           # Show API key
weather-ctl key regen     # Rotate API key
weather-ctl tunnel-start  # Start Cloudflare tunnel
weather-ctl setup         # First-time setup
```

## Architecture

```
weather-ctl               — Management CLI (bash, runs from anywhere)
scripts/
  api.py                  — Flask REST API (gunicorn in prod)
  db.py                   — SQLite database layer (thread-local pooling)
  fetch_weather.py        — Multi-source API data collection (5 sources)
  collect_forecasts.py    — Data collection orchestrator
  weighted_forecast.py    — Bayesian consensus forecasting
  seasonal_model.py       — 4 ML prediction models + BMA
  seasonal_forecast.py    — Forecast generation pipeline
  climate_indices.py      — NOAA teleconnection index downloader
  meteo.py                — Meteorological physics engine
  verify_and_score.py     — Accuracy verification & scoring
  alerts.py               — Weather alert detection & notification
  orchestrate.py          — Pipeline orchestrator (parallel city processing)
  weather_telegram.py     — Telegram notifications (morning briefing + alerts)
  init_db.py              — Database schema initialization
  add_city.py             — City management CLI
  doctor_freshness.py     — Data freshness check (used by weather-ctl)
  __init__.py             — Package marker
dashboard/
  index.html              — React 18 SPA (Tailwind, Leaflet, Chart.js)
tests/
  test_meteo.py           — Physics module unit tests
  test_db.py              — Database layer tests
data/
  weather.db              — SQLite database (WAL mode)
config.json               — All configuration (APIs, scoring, physics, models)
.env                      — Secrets (WEATHER_API_KEY, API keys) — NOT in git
BACKLOG.md                — Prioritized improvement tasks
```

## Infrastructure

- **Server**: Gunicorn (2 workers) via systemd user service `weather-api`
- **Telegram Bot**: Interactive bot via `weather-bot` service (polling, responds to /forecast, /alerts, etc.)
- **Tunnel**: Cloudflare quick tunnel via `weather-tunnel` service (URL changes on restart)
- **Tailscale IP**: `<your-tailscale-ip>:5000` (permanent, works from PC + iPhone)
- **Cron**: 8 weather jobs (fetch every 6h, verify midnight, indices weekly, Telegram alerts)
- **Auth**: API key via `X-API-Key` header on all POST/DELETE endpoints
- **Security**: Rate limiting, input sanitization, security headers, path traversal protection

## Security Model

- `.env` file holds `WEATHER_API_KEY` — loaded automatically by api.py
- All POST/DELETE endpoints require `X-API-Key` header (disabled if key not set)
- Rate limits: 120 reads/min, 20 writes/min, 5 cities/hour per IP
- CORS auto-detects all local IPs (Tailscale, LAN, localhost)
- Security headers: X-Content-Type-Options, X-Frame-Options, X-XSS-Protection, Referrer-Policy
- Errors never leak stack traces to clients

## Pipeline

```
1. Fetch weather from 7 APIs     → Store in weather.db
2. Download NOAA climate indices  → 18+ teleconnection indices
3. Run 4 ML models + BMA         → Generate seasonal predictions
4. Compare vs actual observations → Update Bayesian accuracy weights
5. Check alerts                   → Notify on extreme weather
6. Cleanup old data               → Prune forecasts >90d, obs >365d
7. Serve Flask API + dashboard    → http://<your-ip>:5000
```

## Behavioral Rules

- ALWAYS run `weather-ctl doctor` at the start of a session
- ALWAYS read a file before editing it
- NEVER save working files to root folder
- Use `scripts/` for Python code, `dashboard/` for frontend, `tests/` for tests
- NEVER commit `.env`, API keys, or secrets
- After editing api.py, run `weather-ctl restart`
- After editing any script, run `python3 -m pytest tests/ -v`
- The database is `data/weather.db` — do not delete it
- Check `BACKLOG.md` for what to work on next

## Running Tests

```bash
cd "/path/to/weather-predict"
python3 -m pytest tests/ -v
```

## API Endpoints

**Public (no auth):**
- `GET /api/cities` — List tracked cities
- `GET /api/forecast/<city_id>` — Weighted ensemble forecast
- `GET /api/observations/<city_id>` — Historical observations
- `GET /api/accuracy/<city_id>` — Source accuracy weights
- `GET /api/trends/<city_id>` — Time-series trends
- `GET /api/compare/<city_id>` — Source comparison for today
- `GET /api/seasonal/<city_id>` — Seasonal outlook
- `GET /api/indices` — Current teleconnection state
- `GET /api/alerts` — Weather alerts (optional `?city_id=N`)
- `GET /api/health` — System health check

**Requires X-API-Key header:**
- `POST /api/cities` — Add a city
- `DELETE /api/cities/<city_id>` — Remove a city
- `POST /api/seasonal/climatology/<city_id>` — Build climatology
- `POST /api/cache/invalidate` — Clear forecast cache

## Key Configuration (config.json)

- `default_cities`: Cities to track (default: Bratislava, New York, London, Tokyo, Sydney)
- `scoring.window_days`: Days of history for accuracy scoring (default: 30)
- `scoring.composite_weights`: Metric importance weights
- `physics.max_precip_adjustment_pct`: Max physics correction for precipitation
- `seasonal.indices`: NOAA climate index URLs and parameters
- `seasonal.bma`: Bayesian Model Averaging weights

## Environment Variables (.env)

- `WEATHER_API_KEY` — **Required for POST/DELETE** endpoints
- `WEATHER_PORT` — Server port (default: 5000)
- `WEATHER_HOST` — Bind address (default: 0.0.0.0)
- `CORS_ORIGINS` — Comma-separated origins (default: auto-detect)
- `OPENWEATHER_API_KEY` — OpenWeatherMap free tier (optional)
- `WEATHERAPI_KEY` — WeatherAPI.com free tier (optional)
- `VISUAL_CROSSING_KEY` — Visual Crossing free tier (optional)
