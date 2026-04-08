# Weather System Backlog

> Add ideas here. Claude picks them up at the start of each session.
> Mark done with [x]. Add new items under the right priority.

## P0 — Fix / Urgent

- [x] Security: disable debug mode, restrict CORS
- [x] Security: API key auth on POST/DELETE
- [x] Bug: conn.close() destroying thread pool in verify_and_score.py
- [x] Switch to gunicorn for production
- [x] Seasonal forecasting: removed synthetic targets, real obs-only training
- [x] Seasonal forecasting: separate precipitation regression (was hardcoded)
- [x] Seasonal forecasting: ECMWF confidence now lead-dependent
- [x] Seasonal forecasting: added hindcast verification (ACC, RPSS, HSS)

## P1 — Next Up

- [ ] Add more weather sources (Visual Crossing key is empty, could activate)
- [x] Dashboard: add "last updated" timestamp + auto-refresh
- [x] Dashboard: mobile-friendly layout improvements for iPhone
- [x] Telegram bot: add interactive commands (e.g. /forecast Bratislava)
- [ ] Add backup/restore command to weather-ctl
- [x] Add log rotation for cron job logs (~/.openclaw/logs/weather-*.log)

## P2 — Someday

- [ ] Add multi-day forecast view (7-day, not just today)
- [ ] Add historical accuracy charts to dashboard
- [ ] Named Cloudflare tunnel (needs domain) for stable public URL
- [ ] Add webhook alerts to Discord/Slack
- [ ] Add wind chill / heat index to dashboard display
- [ ] PWA support (service worker for offline, push notifications)
- [ ] Add city search autocomplete in dashboard
- [ ] Database migration system (schema versioning)
