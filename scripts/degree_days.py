"""Heating and cooling degree days (HDD/CDD).

HDD = max(0, base_c - daily_avg_c)
CDD = max(0, daily_avg_c - base_c)

Daily average is approximated as (high + low) / 2 per standard meteorological
convention. Base 18°C is the EU/WMO standard; 18.33°C (65°F) is the US standard.

Used by energy traders for natural-gas heating demand (HDD) and power/cooling
demand (CDD) forecasting. Kalshi and Polymarket list weather-derivative-style
contracts that settle on degree-day accumulations.
"""
from __future__ import annotations

from typing import Optional

BASE_C_EU = 18.0
BASE_C_US = 18.333  # 65°F in Celsius


def daily_avg(high_c: Optional[float], low_c: Optional[float]) -> Optional[float]:
    """(high + low) / 2, or None if either is missing."""
    if high_c is None or low_c is None:
        return None
    return (high_c + low_c) / 2.0


def hdd(high_c: Optional[float], low_c: Optional[float], base_c: float = BASE_C_EU) -> Optional[float]:
    """Heating degree days for one day."""
    avg = daily_avg(high_c, low_c)
    if avg is None:
        return None
    return max(0.0, base_c - avg)


def cdd(high_c: Optional[float], low_c: Optional[float], base_c: float = BASE_C_EU) -> Optional[float]:
    """Cooling degree days for one day."""
    avg = daily_avg(high_c, low_c)
    if avg is None:
        return None
    return max(0.0, avg - base_c)


def enrich_day(day: dict, base_c: float = BASE_C_EU) -> dict:
    """Add hdd_c_<base> and cdd_c_<base> to a daily forecast entry in-place.

    Uses the ensemble high/low on top level. If a `mos` sub-dict exists with
    p50 quantiles, also enriches with HDD/CDD computed from the MOS p50 and
    quantile bounds (p10 low pair → p10 HDD floor, etc.).
    """
    base_label = f"{int(round(base_c))}"
    hi = day.get("high_c")
    lo = day.get("low_c")
    day[f"hdd_c_{base_label}"] = hdd(hi, lo, base_c)
    day[f"cdd_c_{base_label}"] = cdd(hi, lo, base_c)

    mos = day.get("mos")
    if mos:
        # Use p50 high + p50 low for the central estimate
        mos_p50_hi = mos.get("temp_high_p50")
        mos_p50_lo = mos.get("temp_low_p50")
        mos[f"hdd_c_{base_label}_p50"] = hdd(mos_p50_hi, mos_p50_lo, base_c)
        mos[f"cdd_c_{base_label}_p50"] = cdd(mos_p50_hi, mos_p50_lo, base_c)

        # p10 band = coldest plausible day (highest HDD, lowest CDD)
        # Uses p10 high + p10 low as the coldest-day pair
        mos_p10_hi = mos.get("temp_high_p10")
        mos_p10_lo = mos.get("temp_low_p10")
        mos[f"hdd_c_{base_label}_p90"] = hdd(mos_p10_hi, mos_p10_lo, base_c)  # cold → high HDD
        mos[f"cdd_c_{base_label}_p10"] = cdd(mos_p10_hi, mos_p10_lo, base_c)  # cold → low CDD

        # p90 band = warmest plausible day (lowest HDD, highest CDD)
        mos_p90_hi = mos.get("temp_high_p90")
        mos_p90_lo = mos.get("temp_low_p90")
        mos[f"hdd_c_{base_label}_p10"] = hdd(mos_p90_hi, mos_p90_lo, base_c)  # warm → low HDD
        mos[f"cdd_c_{base_label}_p90"] = cdd(mos_p90_hi, mos_p90_lo, base_c)  # warm → high CDD

    return day


def sum_period(days: list[dict], field: str) -> float:
    """Sum a degree-day field across a list of daily forecast entries, skipping None."""
    return sum(d.get(field) or 0.0 for d in days)
