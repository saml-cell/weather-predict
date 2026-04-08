#!/usr/bin/env python3
"""Data freshness check for weather-ctl doctor."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db

conn = db.get_connection()

row = conn.execute('SELECT MAX(fetched_at) as t FROM forecasts').fetchone()
last_fetch = row['t'] if row else 'never'

row = conn.execute('SELECT MAX(obs_date) as t FROM observations').fetchone()
last_obs = row['t'] if row else 'never'

row = conn.execute('SELECT MAX(computed_at) as t FROM source_accuracy').fetchone()
last_verify = row['t'] if row else 'never'

stale = conn.execute("""
    SELECT c.name FROM cities c
    LEFT JOIN (SELECT city_id, MAX(fetched_at) as last FROM forecasts GROUP BY city_id) f
    ON c.id = f.city_id
    WHERE f.last IS NULL OR f.last < datetime('now', '-24 hours')
""").fetchall()

print(f'  Last fetch:    {last_fetch}')
print(f'  Last obs:      {last_obs}')
print(f'  Last verify:   {last_verify}')
if stale:
    names = ', '.join(r['name'] for r in stale)
    print(f'  STALE cities:  {names}')
else:
    print(f'  Stale cities:  none (all fresh)')
