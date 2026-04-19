"""Polymarket historical backtest — shortcut the 14-day paper gate.

Uses the ~/polymarket-data library to pull the full history of closed
weather temperature markets (Jan 2025 → Apr 2026, ~17,785 clean-resolved
markets), joins them to 24-hour pre-close market prices from quant.parquet,
and runs multiple strategies:

  1. CEILING: bet $1 on the known winner. Upper bound on any forecaster.
  2. MISPRICING-ONLY CEILING: ceiling restricted to winner ≤ 20% priced.
  3. NAIVE CONTRARIAN: always bet the side priced below 0.50.
  4. MARKET MAJORITY: always bet the side priced above 0.50.
  5. MOS EDGE (Phase 2): bet the side where the production MOS model
     disagrees with the market by >= edge threshold. For each market, the
     MOS p10/p50/p90 of the day's high temperature is computed from the
     00-UTC ensemble forecast stored in data/weather.db (issue_time = 00
     UTC of the resolution date, matching the MOS training convention).
     A Gaussian is fit through the three quantiles to compute
     P(YES) = P(temp <= threshold) or P(temp >= threshold); edge = P_MOS -
     yes_price_pre. Genuinely out-of-sample: production v3.1 booster has
     train_end=2023-12-31 (val=2024, test=2025), so the Jan 2025 – Mar
     2026 markets were never in train/val. Remaining caveats: (1) MOS
     sees the 00 UTC forecast of resolution day while market price is
     captured 24h pre-close — ~12h MOS freshness advantage; (2) at high
     edge thresholds, top 5 bets carry >70% of total PnL — signal may be
     jackpot-driven. Covers all three market formats (threshold "X° or
     below/higher", exact "be X°", range "be between X-Y°") in the 57
     tracked cities. For exact/range, assumes Polymarket resolves on the
     nearest-integer rounded official daily high — exact "be 9°C"
     corresponds to high ∈ [8.5°C, 9.5°C); range "be between 46-47°F"
     corresponds to high ∈ [45.5°F, 47.5°F).

Output: ~/Weather predict program/data/polymarket_historical_backtest.json

This replaces N weeks of live paper-trading with ~minutes of compute.
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, "/home/samko/polymarket-data/src")
sys.path.insert(0, "/home/samko/polymarket-data/analysis")

PROJECT = Path(__file__).resolve().parent.parent
# Reach the project's scripts/ dir so we can import mos_inference (which
# handles its own venv bootstrap for lightgbm).
sys.path.insert(0, str(PROJECT / "scripts"))

import numpy as np
import pandas as pd

from polymarket_data import download  # noqa: E402
from _common import (  # noqa: E402
    clean_resolved, load_markets, load_quant_for_markets,
    price_before_close, tag_segments,
)
import mos_inference  # noqa: E402

OUT = PROJECT / "data" / "polymarket_historical_backtest.json"
DB = PROJECT / "data" / "weather.db"
MIN_PROB_FOR_BET = 0.05   # skip tiny prices (high transaction cost, low info)
MAX_PROB_FOR_BET = 0.95

MOS_FORECAST_COLS = [
    "valid_time", "source", "temp_c", "humidity_pct", "precip_mm",
    "wind_speed_kmh", "dew_point_c", "pressure_msl_hpa", "cloud_cover_pct",
    "wind_dir_deg",
]
MOS_SOURCES = ("gfs_global", "icon_seamless", "jma_seamless", "ecmwf_ifs025")

# Regexes for the three weather-temperature market formats.
# Examples:
#   "Will the highest temperature in New York City be 76°F or below on August 2?"  (threshold)
#   "Will the highest temperature in Chicago be 34°F or higher on February 2?"     (threshold)
#   "Will the highest temperature in Miami be between 86-87°F on March 24?"        (range)
#   "Will the highest temperature in Beijing be 21°C on March 24?"                 (exact)
# Order matters: "between" must be tried before the bare "be N°" pattern.
RANGE_RE = re.compile(
    r"highest temperature in (.+?) be between (\d+)[-–](\d+)°([CF]) on",
    re.IGNORECASE,
)
THRESHOLD_RE = re.compile(
    r"highest temperature in (.+?) be (\d+)°([CF]) or (below|higher) on",
    re.IGNORECASE,
)
EXACT_RE = re.compile(
    r"highest temperature in (.+?) be (\d+)°([CF]) on",
    re.IGNORECASE,
)

# Polymarket resolves on the official integer-rounded daily high, so a
# question of "be 9°C" covers high ∈ [8.5°C, 9.5°C) and "be between 46-47°F"
# covers high ∈ [45.5°F, 47.5°F). A threshold question "X° or below" means
# high ≤ X after rounding, i.e. T < X + 0.5 unit. The same 0.5-unit half-
# bucket is baked into the threshold boundaries below so all three formats
# share one convention.
INF = float("inf")


def _half_bucket_c(unit: str) -> float:
    return 0.5 if unit.upper() == "C" else (0.5 * 5.0 / 9.0)  # ≈ 0.2778°C


def _to_c(value: int, unit: str) -> float:
    return float(value) if unit.upper() == "C" else (value - 32.0) * 5.0 / 9.0


# City name variants that differ between Polymarket questions and the
# weather.db `cities.name` column.
CITY_ALIASES = {
    "New York City": "New York",
    "NYC": "New York",
}


def parse_market(question: str) -> dict | None:
    """Parse a weather temperature question into a normalized (low_c, high_c)
    YES-range in °C. Try range first (keyword "between"), then threshold
    ("or below/higher"), then bare exact ("be N°"). Returns None if none match.
    """
    m = RANGE_RE.search(question)
    if m:
        city_raw, a, b, unit = m.group(1).strip(), int(m.group(2)), int(m.group(3)), m.group(4)
        half = _half_bucket_c(unit)
        return {
            "kind": "range",
            "city_raw": city_raw,
            "low_c": _to_c(a, unit) - half,
            "high_c": _to_c(b, unit) + half,
        }
    m = THRESHOLD_RE.search(question)
    if m:
        city_raw, v, unit, direction = m.group(1).strip(), int(m.group(2)), m.group(3), m.group(4).lower()
        half = _half_bucket_c(unit)
        v_c = _to_c(v, unit)
        if direction == "below":    # high ≤ X  →  T < X + 0.5 unit
            return {"kind": "threshold", "city_raw": city_raw,
                    "low_c": -INF, "high_c": v_c + half}
        else:                       # high ≥ X  →  T ≥ X - 0.5 unit
            return {"kind": "threshold", "city_raw": city_raw,
                    "low_c": v_c - half, "high_c": INF}
    m = EXACT_RE.search(question)
    if m:
        city_raw, v, unit = m.group(1).strip(), int(m.group(2)), m.group(3)
        half = _half_bucket_c(unit)
        v_c = _to_c(v, unit)
        return {"kind": "exact", "city_raw": city_raw,
                "low_c": v_c - half, "high_c": v_c + half}
    return None


def load_city_map(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT name, id FROM cities").fetchall()
    m = {name: cid for name, cid in rows}
    for alias, canonical in CITY_ALIASES.items():
        if canonical in m:
            m[alias] = m[canonical]
    return m


def fetch_ensemble_long(conn: sqlite3.Connection, city_id: int,
                         resolution_date: str) -> pd.DataFrame:
    """Return long-format ensemble forecast for issue_time = 00 UTC of
    `resolution_date` (ISO YYYY-MM-DD). Empty if data is missing.
    """
    issue = f"{resolution_date}T00:00:00+00:00"
    cols = ",".join(MOS_FORECAST_COLS)
    q = f"""
        SELECT {cols}
        FROM forecasts_hourly
        WHERE city_id = ?
          AND issue_time = ?
          AND source IN ({",".join(["?"] * len(MOS_SOURCES))})
          AND date(valid_time) = ?
    """
    df = pd.read_sql_query(
        q, conn,
        params=[city_id, issue, *MOS_SOURCES, resolution_date],
    )
    if df.empty:
        return df
    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    return df


def _gaussian_cdf(x: float, mu: float, sigma: float) -> float:
    if x == -INF:
        return 0.0
    if x == INF:
        return 1.0
    return 0.5 * (1.0 + math.erf((x - mu) / (sigma * math.sqrt(2.0))))


def gaussian_p_in_range(p10: float, p50: float, p90: float,
                          low_c: float, high_c: float) -> float:
    """P(low_c <= T <= high_c) assuming T ~ N(p50, sigma) with sigma fit
    through the 10/90 quantiles. Handles ±INF bounds for threshold markets.
    """
    sigma = max((p90 - p10) / (2.0 * 1.2815515655446004), 0.1)
    return _gaussian_cdf(high_c, p50, sigma) - _gaussian_cdf(low_c, p50, sigma)


def simulate(df: pd.DataFrame, predicate) -> dict:
    """Simulate $1-per-trade P&L on rows where `predicate(row) is True`.

    If outcome=1 and yes_price=p, buying YES pays off 1/p per $ → P&L = 1/p - 1.
    If outcome=0 and we bet NO at price (1-p), pays off 1/(1-p) - 1.
    The strategy here always bets on one side — the `predicate` picks which.
    """
    mask = df.apply(predicate, axis=1)
    bet = df[mask]
    if bet.empty:
        return {"n_bets": 0, "total_pnl": 0.0, "hit_rate": None,
                "mean_pnl_per_bet": None}

    def pnl_row(row):
        p = row["yes_price_pre"]
        if row["bet_side"] == "YES":
            return (1.0 / p - 1.0) if row["outcome"] == 1 else -1.0
        else:   # NO
            q = 1.0 - p
            return (1.0 / q - 1.0) if row["outcome"] == 0 else -1.0

    bet = bet.copy()
    bet["pnl"] = bet.apply(pnl_row, axis=1)
    wins = (bet["pnl"] > 0).sum()
    return {
        "n_bets": int(len(bet)),
        "total_pnl": float(bet["pnl"].sum()),
        "hit_rate": float(wins / len(bet)),
        "mean_pnl_per_bet": float(bet["pnl"].mean()),
        "wins": int(wins),
        "losses": int(len(bet) - wins),
    }


def simulate_mos_edge(df: pd.DataFrame,
                       edge_thresholds: tuple[float, ...] = (0.10,)) -> dict:
    """Score every tracked-city market in `df` (threshold + exact + range
    formats) against the production MOS ensemble. Returns coverage stats,
    per-edge-threshold P&L, per-format breakdown, per-period stability.
    Cached by (city_id, resolution_date) so one MOS prediction serves all
    markets sharing the same day in the same city.
    """
    conn = sqlite3.connect(str(DB))
    city_map = load_city_map(conn)

    parsed_rows = []
    total = len(df)
    unparsed = 0
    untracked = 0
    for row in df.itertuples(index=False):
        p = parse_market(row.question)
        if p is None:
            unparsed += 1
            continue
        cid = city_map.get(p["city_raw"])
        if cid is None:
            untracked += 1
            continue
        resolution_date = pd.to_datetime(row.end_date).strftime("%Y-%m-%d")
        parsed_rows.append({
            "market_id": row.market_id,
            "outcome": int(row.outcome),
            "yes_price_pre": float(row.yes_price_pre),
            "question": row.question,
            "kind": p["kind"],
            "city_raw": p["city_raw"],
            "city_id": int(cid),
            "low_c": p["low_c"],
            "high_c": p["high_c"],
            "resolution_date": resolution_date,
        })
    kept = len(parsed_rows)
    by_kind = pd.Series([r["kind"] for r in parsed_rows]).value_counts().to_dict() if parsed_rows else {}
    print(f"MOS-candidate markets: tracked-city parsed = {kept:,} of {total:,} paired "
          f"(by format: {by_kind}; {unparsed:,} unparsed, {untracked:,} untracked)")

    scored = []
    skipped_no_fcst = 0
    cache: dict[tuple[int, str], tuple[float, float, float] | None] = {}
    for rec in parsed_rows:
        key = (rec["city_id"], rec["resolution_date"])
        if key not in cache:
            long_df = fetch_ensemble_long(conn, rec["city_id"], rec["resolution_date"])
            if long_df.empty or len(long_df) < 24 * 2:
                cache[key] = None
            else:
                hourly = mos_inference.predict_hourly(long_df, rec["city_id"])
                if hourly.empty or "temp_c_p50" not in hourly.columns:
                    cache[key] = None
                else:
                    daily = mos_inference.aggregate_to_daily(hourly)
                    d = daily.get(rec["resolution_date"])
                    if not d or any(d.get(k) is None for k in ("temp_high_p10", "temp_high_p50", "temp_high_p90")):
                        cache[key] = None
                    else:
                        cache[key] = (d["temp_high_p10"], d["temp_high_p50"], d["temp_high_p90"])
        triple = cache[key]
        if triple is None:
            skipped_no_fcst += 1
            continue
        p10, p50, p90 = triple
        p_yes = gaussian_p_in_range(p10, p50, p90, rec["low_c"], rec["high_c"])
        edge = p_yes - rec["yes_price_pre"]
        scored.append({
            **rec,
            "p10": p10, "p50": p50, "p90": p90,
            "mos_p_yes": p_yes, "edge": edge,
        })

    print(f"scored markets: {len(scored):,}  "
          f"(skipped {skipped_no_fcst:,} w/ no historical forecast)")
    conn.close()
    if not scored:
        return {"per_edge": {}, "coverage": {
            "parsed_threshold_in_tracked": kept,
            "scored": 0,
            "skipped_no_forecast": skipped_no_fcst,
        }}

    scored_df = pd.DataFrame(scored)

    per_edge = {}
    for thr in edge_thresholds:
        betters = scored_df[scored_df["edge"].abs() >= thr].copy()
        if betters.empty:
            per_edge[f"{thr:.2f}"] = {"n_bets": 0}
            continue
        # bet YES if MOS thinks YES more likely than the market; else NO
        betters["bet_side"] = np.where(betters["edge"] > 0, "YES", "NO")

        def pnl(row):
            p = row["yes_price_pre"]
            if row["bet_side"] == "YES":
                return (1.0 / p - 1.0) if row["outcome"] == 1 else -1.0
            q = 1.0 - p
            return (1.0 / q - 1.0) if row["outcome"] == 0 else -1.0

        betters["pnl"] = betters.apply(pnl, axis=1)
        wins = int((betters["pnl"] > 0).sum())
        n = int(len(betters))
        per_edge[f"{thr:.2f}"] = {
            "edge_threshold": thr,
            "n_bets": n,
            "total_pnl": float(betters["pnl"].sum()),
            "mean_pnl_per_bet": float(betters["pnl"].mean()),
            "roi": float(betters["pnl"].sum() / n),
            "hit_rate": float(wins / n),
            "wins": wins,
            "losses": n - wins,
            "bet_yes_share": float((betters["bet_side"] == "YES").mean()),
        }

    # Temporal-stability test at edge >= 0.20. If the signal is real and
    # distributed, ROI should be positive in every time window. If it's
    # concentrated in one period, the 74% headline survives "out-of-sample"
    # but fails "tradable".
    PERIODS = [
        ("2025-H1", "2025-01-01", "2025-07-01"),
        ("2025-H2", "2025-07-01", "2026-01-01"),
        ("2026-Q1", "2026-01-01", "2026-04-01"),
    ]
    per_period = {}
    for name, start, end in PERIODS:
        window = scored_df[(scored_df["resolution_date"] >= start) &
                           (scored_df["resolution_date"] < end)].copy()
        window_stats = {"start": start, "end": end, "n_candidates": int(len(window))}
        window_bets = window[window["edge"].abs() >= 0.20].copy()
        if window_bets.empty:
            window_stats.update({"n_bets": 0})
        else:
            window_bets["bet_side"] = np.where(window_bets["edge"] > 0, "YES", "NO")

            def pnl(row):
                p = row["yes_price_pre"]
                if row["bet_side"] == "YES":
                    return (1.0 / p - 1.0) if row["outcome"] == 1 else -1.0
                q = 1.0 - p
                return (1.0 / q - 1.0) if row["outcome"] == 0 else -1.0

            window_bets["pnl"] = window_bets.apply(pnl, axis=1)
            wins = int((window_bets["pnl"] > 0).sum())
            n = int(len(window_bets))
            window_stats.update({
                "n_bets": n,
                "total_pnl": float(window_bets["pnl"].sum()),
                "mean_pnl_per_bet": float(window_bets["pnl"].mean()),
                "roi": float(window_bets["pnl"].sum() / n),
                "hit_rate": float(wins / n),
                "wins": wins,
                "losses": n - wins,
                "top5_pnl": float(window_bets.nlargest(min(5, n), "pnl")["pnl"].sum()),
            })
        per_period[name] = window_stats

    # Per-format stability at edge >= 0.20. If the widened formats (range,
    # exact) post wildly different ROIs from threshold, the Polymarket
    # rounding assumption is probably wrong and needs revisiting.
    per_format = {}
    for kind in ("threshold", "range", "exact"):
        sub = scored_df[scored_df["kind"] == kind].copy()
        bets = sub[sub["edge"].abs() >= 0.20].copy()
        entry = {"n_candidates": int(len(sub)), "n_bets": int(len(bets))}
        if not bets.empty:
            bets["bet_side"] = np.where(bets["edge"] > 0, "YES", "NO")

            def pnl(row):
                p = row["yes_price_pre"]
                if row["bet_side"] == "YES":
                    return (1.0 / p - 1.0) if row["outcome"] == 1 else -1.0
                q = 1.0 - p
                return (1.0 / q - 1.0) if row["outcome"] == 0 else -1.0

            bets["pnl"] = bets.apply(pnl, axis=1)
            wins = int((bets["pnl"] > 0).sum())
            entry.update({
                "total_pnl": float(bets["pnl"].sum()),
                "mean_pnl_per_bet": float(bets["pnl"].mean()),
                "roi": float(bets["pnl"].sum() / len(bets)),
                "hit_rate": float(wins / len(bets)),
                "wins": wins,
                "losses": int(len(bets) - wins),
                "top5_pnl": float(bets.nlargest(min(5, len(bets)), "pnl")["pnl"].sum()),
            })
        per_format[kind] = entry

    # Format × period interaction. Pooled "range is distributed" could
    # hide a late-period collapse. One groupby answers it.
    per_fmt_period = {}
    for kind in ("threshold", "range", "exact"):
        for pname, pstart, pend in PERIODS:
            sub = scored_df[(scored_df["kind"] == kind) &
                            (scored_df["resolution_date"] >= pstart) &
                            (scored_df["resolution_date"] < pend)]
            bets = sub[sub["edge"].abs() >= 0.20].copy()
            if bets.empty:
                per_fmt_period[f"{kind}_{pname}"] = {"n_candidates": int(len(sub)), "n_bets": 0}
                continue
            bets["bet_side"] = np.where(bets["edge"] > 0, "YES", "NO")

            def pnl(row):
                p = row["yes_price_pre"]
                if row["bet_side"] == "YES":
                    return (1.0 / p - 1.0) if row["outcome"] == 1 else -1.0
                q = 1.0 - p
                return (1.0 / q - 1.0) if row["outcome"] == 0 else -1.0

            bets["pnl"] = bets.apply(pnl, axis=1)
            wins = int((bets["pnl"] > 0).sum())
            n = int(len(bets))
            per_fmt_period[f"{kind}_{pname}"] = {
                "n_candidates": int(len(sub)),
                "n_bets": n,
                "hit_rate": float(wins / n),
                "roi": float(bets["pnl"].sum() / n),
                "total_pnl": float(bets["pnl"].sum()),
                "top5_pnl": float(bets.nlargest(min(5, n), "pnl")["pnl"].sum()),
            }

    # Persist full scored frame for ad-hoc slicing without re-loading quant.
    try:
        scored_df.to_parquet(PROJECT / "data" / "polymarket_backtest_scored.parquet")
    except Exception as e:  # pragma: no cover - non-fatal
        print(f"warning: could not save scored parquet: {e}")

    return {
        "per_edge": per_edge,
        "per_period": per_period,
        "per_format": per_format,
        "per_format_x_period": per_fmt_period,
        "coverage": {
            "paired_total": int(len(df)),
            "parsed_in_tracked": kept,
            "by_format_candidates": by_kind,
            "unparsed": unparsed,
            "untracked": untracked,
            "scored": len(scored),
            "skipped_no_forecast": skipped_no_fcst,
        },
    }


def main():
    print("loading markets …")
    if not (Path.home() / "polymarket-data/cache/markets.parquet").exists():
        print("markets.parquet not cached. pulling …")
        download.fetch("markets")

    m = clean_resolved(load_markets())
    w = m[m["question"].str.contains(
        r"(?:highest|lowest) temperature in .+ on",
        case=False, regex=True, na=False)].copy()
    print(f"historical weather markets (clean-resolved): {len(w):,}")
    if len(w) == 0:
        raise SystemExit("no weather markets found")

    ids = w["id"].tolist()
    print(f"pulling quant trades for {len(ids):,} markets (may take a minute)…")
    trades = load_quant_for_markets(ids)
    print(f"loaded {len(trades):,} trades")

    pre = price_before_close(trades, w, hours_before=24)
    df = w[["id", "outcome", "question", "end_date"]].rename(
        columns={"id": "market_id"}
    ).merge(pre, on="market_id", how="inner")
    df = df[(df["yes_price_pre"] >= MIN_PROB_FOR_BET) &
            (df["yes_price_pre"] <= MAX_PROB_FOR_BET)]
    print(f"paired w/ 24h-pre-close price (after filter): {len(df):,}")

    # ---- Strategy 1: perfect-foresight ceiling (bet on actual winner) ----
    df["bet_side"] = np.where(df["outcome"] == 1, "YES", "NO")
    ceil = simulate(df, lambda r: True)

    # ---- Strategy 2: threshold-filtered ceiling (only "value" bets) ----
    # mispricing-heavy: winner was priced at <20% or loser was priced at >80%.
    def threshold_mask(row, thresh=0.20):
        if row["outcome"] == 1 and row["yes_price_pre"] <= thresh:
            return True
        if row["outcome"] == 0 and row["yes_price_pre"] >= (1 - thresh):
            return True
        return False
    filt = simulate(df, lambda r: threshold_mask(r, 0.20))

    # ---- Strategy 3: naive contrarian (always take the side priced <50%) ----
    df["bet_side_contrarian"] = np.where(df["yes_price_pre"] < 0.5, "YES", "NO")
    df_c = df.copy()
    df_c["bet_side"] = df_c["bet_side_contrarian"]
    contra = simulate(df_c, lambda r: True)

    # ---- Strategy 4: market-calibrated (bet on the market's majority side) ----
    df_m = df.copy()
    df_m["bet_side"] = np.where(df_m["yes_price_pre"] > 0.5, "YES", "NO")
    market = simulate(df_m, lambda r: True)

    total_invested_ceiling = float(ceil["n_bets"])
    roi_ceiling = ceil["total_pnl"] / total_invested_ceiling if total_invested_ceiling > 0 else None

    # ---- Strategy 5: MOS edge (Phase 2) -------------------------------
    # Cover all three tracked-city market formats (threshold / exact /
    # range). For each market, derive a Gaussian P(YES) from the production
    # MOS temp-high quantiles and bet when |P_MOS - yes_price_pre| >= edge.
    mos_results = simulate_mos_edge(df, edge_thresholds=(0.05, 0.10, 0.15, 0.20))

    report = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "n_markets_analysed": int(len(df)),
        "min_prob_for_bet": MIN_PROB_FOR_BET,
        "max_prob_for_bet": MAX_PROB_FOR_BET,
        "strategies": {
            "perfect_foresight_ceiling": {
                "description": "bet $1 on actual winner, every market",
                **ceil,
                "roi": roi_ceiling,
            },
            "mispricing_only_ceiling": {
                "description": "$1 on winner only when market priced winner ≤ 0.20",
                **filt,
            },
            "naive_contrarian": {
                "description": "always bet the side priced below 0.50",
                **contra,
            },
            "market_majority": {
                "description": "always bet the side priced above 0.50 (market agrees with us)",
                **market,
            },
            "mos_edge": {
                "description": (
                    "OUT-OF-SAMPLE. bet when |P_MOS - yes_price_pre| >= edge. "
                    "P_MOS = Gaussian(µ=temp_high_p50, σ=(p90-p10)/2.5631) "
                    "evaluated on the YES-range [low_c, high_c] decoded from "
                    "the question. threshold/exact/range tracked-city markets."
                ),
                "out_of_sample_evidence": (
                    "production v3.1 booster has train_end=2023-12-31, "
                    "val_end=2024-12-31, test_end=2025-12-31. the 318 "
                    "scored markets all resolved in Jan 2025 – Mar 2026, "
                    "so none were in train or val. isotonic calibrators "
                    "were fit on 2024 val; temp_c calibrator is pinned off "
                    "per Council R4 Step 2, so temp predictions are raw "
                    "booster output."
                ),
                "caveats": [
                    "MOS issue_time = 00 UTC of resolution day; market "
                    "price is 24h pre-close — MOS sees ~12h fresher data "
                    "than the price snapshot (small but nonzero leak)",
                    "at |edge| >= 0.20, top 5 bets carry ~73% of total "
                    "PnL — signal may be jackpot-driven rather than "
                    "broadly distributed",
                    "270 of 588 candidates skipped — all in cities added "
                    "2026-04-18 or later (Atlanta/Dallas/Seattle/Miami/"
                    "Buenos Aires/Hong Kong/Denver) without historical "
                    "forecasts; scored set biased toward longer-tracked "
                    "cities",
                ],
                "per_edge_threshold": mos_results["per_edge"],
                "per_period_stability": mos_results["per_period"],
                "per_format_stability": mos_results["per_format"],
                "per_format_x_period": mos_results["per_format_x_period"],
                "parse_coverage": mos_results["coverage"],
            },
        },
    }
    print(json.dumps(report, indent=2, default=str))
    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nwrote: {OUT}")


if __name__ == "__main__":
    main()
