#!/usr/bin/env bash
# One-command local setup: install deps, create .env, init the database, start the API.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m pip install -r requirements.txt
if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Optional API keys can be filled in there."
fi
mkdir -p data
python3 scripts/init_db.py
echo ""
echo "Starting on http://localhost:${WEATHER_PORT:-5000}"
echo "Try Bratislava: http://localhost:${WEATHER_PORT:-5000}/?lat=48.15&lon=17.11"
exec python3 scripts/api.py
