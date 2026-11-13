#!/usr/bin/env python3
"""Pure forecast verification scores.

No network and no database. Backtests and unit tests both call these helpers
so the numbers in a report are produced by the same code the tests lock down.
"""

from __future__ import annotations


def _pairs(forecasts, observations):
    """Yield (forecast, observation) for aligned numeric pairs. Skip nulls."""
    out = []
    for fc, obs in zip(forecasts, observations):
        if fc is None or obs is None:
            continue
        try:
            out.append((float(fc), float(obs)))
        except (TypeError, ValueError):
            continue
    return out


def mae(forecasts, observations):
    """Mean absolute error. None when nothing overlaps."""
    pairs = _pairs(forecasts, observations)
    if not pairs:
        return None
    return sum(abs(f - o) for f, o in pairs) / len(pairs)


def rmse(forecasts, observations):
    """Root mean squared error. None when nothing overlaps."""
    pairs = _pairs(forecasts, observations)
    if not pairs:
        return None
    return (sum((f - o) ** 2 for f, o in pairs) / len(pairs)) ** 0.5


def bias(forecasts, observations):
    """Mean signed error (forecast minus observation)."""
    pairs = _pairs(forecasts, observations)
    if not pairs:
        return None
    return sum(f - o for f, o in pairs) / len(pairs)


def brier_score(probabilities, outcomes):
    """Brier score for probabilities in [0, 1] and binary outcomes.

    Lower is better. A perfect forecast scores 0. A constant 0.5 scores 0.25.
    Values outside [0, 1] are clamped. None outcomes or probabilities are skipped.
    """
    total = 0.0
    n = 0
    for prob, outcome in zip(probabilities, outcomes):
        if prob is None or outcome is None:
            continue
        try:
            p = float(prob)
            o = float(outcome)
        except (TypeError, ValueError):
            continue
        p = min(1.0, max(0.0, p))
        o = 1.0 if o >= 0.5 else 0.0
        total += (p - o) ** 2
        n += 1
    if n == 0:
        return None
    return total / n


def hit_rate(predicted, observed):
    """Fraction of cases where the yes/no call matches.

    Truthy values count as a positive event. None pairs are skipped.
    """
    n = 0
    hits = 0
    for pred, obs in zip(predicted, observed):
        if pred is None or obs is None:
            continue
        n += 1
        if bool(pred) == bool(obs):
            hits += 1
    if n == 0:
        return None
    return hits / n


def precipitation_event(amount_mm, threshold_mm=1.0):
    """True when a daily total counts as a wet day."""
    if amount_mm is None:
        return None
    try:
        return float(amount_mm) >= float(threshold_mm)
    except (TypeError, ValueError):
        return None


def inverse_mae_weights(mae_by_source, epsilon=0.1):
    """Turn per-source MAE into weights that sum to 1.

    Smaller error gets more weight. ``epsilon`` keeps a perfect source finite.
    Sources with a missing MAE are dropped.
    """
    raw = {}
    for source, err in mae_by_source.items():
        if err is None:
            continue
        raw[source] = 1.0 / (float(err) + float(epsilon))
    total = sum(raw.values())
    if total <= 0:
        return {}
    return {source: weight / total for source, weight in raw.items()}


def weighted_values(values_by_source, weights):
    """Weighted mean of a {source: value} map. Renormalises over present values."""
    num = 0.0
    den = 0.0
    for source, value in values_by_source.items():
        if value is None or source not in weights:
            continue
        w = weights[source]
        num += float(value) * w
        den += w
    if den <= 0:
        return None
    return num / den


def round_or_none(value, digits=3):
    if value is None:
        return None
    return round(float(value), digits)
