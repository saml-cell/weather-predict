"""Polymarket historical backtest — shortcut the 14-day paper gate.

Uses the ~/polymarket-data library to pull the full history of closed
weather temperature markets (Jan 2025 → Apr 2026, ~17,785 clean-resolved
markets), joins them to 24-hour pre-close market prices from quant.parquet,
and answers two bounded questions:

  1. CEILING: if a perfect-foresight oracle had bet $1 on the actual
     outcome of every market, what P&L would it have earned?
     This is the absolute upper bound on any forecaster's P&L.

  2. BASELINE: if a naive "bet on whichever side the market priced below
     50%" rule traded instead, what P&L? (This is the "contrarian"
     strategy — assumes market is a coinflip, always takes the underpriced
     side.)

  3. THRESHOLD FILTER: ceiling P&L conditional on market YES < 0.20 for
     the winner — i.e., only count the mispricing headroom, not the trivial
     gains from known outcomes. This is the real "how much does a model
     need to beat" number.

Output: ~/Weather predict program/data/polymarket_historical_backtest.json

This replaces N weeks of live paper-trading with ~minutes of compute.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, "/home/samko/polymarket-data/src")
sys.path.insert(0, "/home/samko/polymarket-data/analysis")

import numpy as np
import pandas as pd

from polymarket_data import download  # noqa: E402
from _common import (  # noqa: E402
    clean_resolved, load_markets, load_quant_for_markets,
    price_before_close, tag_segments,
)

PROJECT = Path(__file__).resolve().parent.parent
OUT = PROJECT / "data" / "polymarket_historical_backtest.json"
MIN_PROB_FOR_BET = 0.05   # skip tiny prices (high transaction cost, low info)
MAX_PROB_FOR_BET = 0.95


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
    df = w[["id", "outcome", "question"]].rename(
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
        },
    }
    print(json.dumps(report, indent=2, default=str))
    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nwrote: {OUT}")


if __name__ == "__main__":
    main()
