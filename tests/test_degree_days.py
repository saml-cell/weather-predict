"""Tests for heating/cooling degree days module."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from degree_days import hdd, cdd, daily_avg, enrich_day, sum_period, BASE_C_EU


def test_daily_avg_basic():
    assert daily_avg(20.0, 10.0) == 15.0


def test_daily_avg_none_returns_none():
    assert daily_avg(None, 10.0) is None
    assert daily_avg(20.0, None) is None
    assert daily_avg(None, None) is None


def test_hdd_cold_day():
    # avg = 5°C, base 18°C → HDD = 13
    assert hdd(10.0, 0.0, base_c=18.0) == 13.0


def test_hdd_warm_day_is_zero():
    # avg = 25°C, base 18°C → HDD = 0 (no heating needed)
    assert hdd(30.0, 20.0, base_c=18.0) == 0.0


def test_cdd_hot_day():
    # avg = 25°C, base 18°C → CDD = 7
    assert cdd(30.0, 20.0, base_c=18.0) == 7.0


def test_cdd_cold_day_is_zero():
    # avg = 5°C, base 18°C → CDD = 0 (no cooling needed)
    assert cdd(10.0, 0.0, base_c=18.0) == 0.0


def test_hdd_cdd_mutual_exclusion():
    """On any given day, only one of HDD/CDD can be nonzero."""
    h = hdd(25.0, 15.0)
    c = cdd(25.0, 15.0)
    assert not (h > 0 and c > 0)


def test_enrich_day_without_mos():
    day = {"date": "2026-04-19", "high_c": 30.0, "low_c": 20.0}
    enrich_day(day, base_c=18.0)
    assert day["cdd_c_18"] == 7.0
    assert day["hdd_c_18"] == 0.0


def test_enrich_day_with_mos():
    day = {
        "date": "2026-04-19",
        "high_c": 30.0,
        "low_c": 20.0,
        "mos": {
            "temp_high_p50": 28.0,
            "temp_low_p50": 18.0,
            "temp_high_p10": 25.0,
            "temp_low_p10": 15.0,
            "temp_high_p90": 32.0,
            "temp_low_p90": 22.0,
        },
    }
    enrich_day(day, base_c=18.0)
    # main ensemble
    assert day["cdd_c_18"] == 7.0
    assert day["hdd_c_18"] == 0.0
    # mos p50: avg = 23, CDD = 5
    assert day["mos"]["cdd_c_18_p50"] == 5.0
    # mos p90 (warmest pair): avg = 27, CDD = 9 → goes into cdd_p90
    assert day["mos"]["cdd_c_18_p90"] == 9.0
    # mos p10 (coldest pair): avg = 20, CDD = 2 → goes into cdd_p10
    assert day["mos"]["cdd_c_18_p10"] == 2.0


def test_sum_period():
    days = [
        {"hdd_c_18": 5.0},
        {"hdd_c_18": 3.0},
        {"hdd_c_18": None},  # missing
        {"hdd_c_18": 2.0},
    ]
    assert sum_period(days, "hdd_c_18") == 10.0


def test_hdd_us_base():
    # 65°F = 18.333°C
    from degree_days import BASE_C_US
    # avg = 18.333, HDD = 0 exactly
    assert hdd(BASE_C_US + 1, BASE_C_US - 1, base_c=BASE_C_US) == 0.0


def test_enrich_day_missing_temps_safe():
    day = {"date": "2026-04-19", "high_c": None, "low_c": 20.0}
    enrich_day(day, base_c=18.0)
    assert day["hdd_c_18"] is None
    assert day["cdd_c_18"] is None
