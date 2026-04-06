#!/usr/bin/env python3
"""Unit tests for the meteorological physics module (scripts/meteo.py)."""

import os
import sys
import unittest

# Allow imports from the scripts/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from meteo import (
    dew_point,
    dew_point_depression,
    precip_adjustment_from_dewpoint,
    precip_adjustment_from_pressure,
    heat_index,
    wind_chill,
    feels_like,
    precip_intensity_factor,
    apply_physics_corrections,
    sea_level_pressure,
)


class TestDewPoint(unittest.TestCase):
    """Tests for the Magnus-Tetens dew point calculation."""

    def test_typical_conditions(self):
        """25C / 60% RH should give approximately 16.7C."""
        td = dew_point(25, 60)
        self.assertIsNotNone(td)
        self.assertAlmostEqual(td, 16.7, delta=0.5)

    def test_none_temperature(self):
        self.assertIsNone(dew_point(None, 60))

    def test_none_humidity(self):
        self.assertIsNone(dew_point(25, None))

    def test_zero_humidity(self):
        """0% humidity is physically meaningless; should return None."""
        self.assertIsNone(dew_point(25, 0))

    def test_high_humidity(self):
        """Near-saturation: dew point should be very close to air temp."""
        td = dew_point(20, 99)
        self.assertIsNotNone(td)
        self.assertAlmostEqual(td, 20, delta=1.0)


class TestDewPointDepression(unittest.TestCase):
    """Tests for dew point depression (T - Td)."""

    def test_high_humidity_small_depression(self):
        """10C / 95% RH should give a very small depression (< 1C)."""
        dpd = dew_point_depression(10, 95)
        self.assertIsNotNone(dpd)
        self.assertLess(dpd, 1.0)
        self.assertGreater(dpd, 0.0)

    def test_none_inputs(self):
        self.assertIsNone(dew_point_depression(None, 50))
        self.assertIsNone(dew_point_depression(20, None))


class TestPrecipAdjustmentFromDewpoint(unittest.TestCase):
    """Tests for precipitation adjustment based on dew point depression."""

    def test_near_saturation(self):
        """Very small depression -> +15."""
        adj = precip_adjustment_from_dewpoint(10, 99)
        self.assertEqual(adj, 15)

    def test_high_humidity(self):
        """10C / 95% RH has depression < 1.5C -> +15 (near saturation)."""
        adj = precip_adjustment_from_dewpoint(10, 95)
        self.assertEqual(adj, 15)

    def test_moderate_humidity(self):
        """Moderate depression -> +5 or 0."""
        adj = precip_adjustment_from_dewpoint(25, 70)
        self.assertIn(adj, [0, 5])

    def test_very_dry(self):
        """Very dry air (large depression) -> -5."""
        adj = precip_adjustment_from_dewpoint(35, 10)
        self.assertEqual(adj, -5)

    def test_none_inputs_return_zero(self):
        self.assertEqual(precip_adjustment_from_dewpoint(None, 50), 0)
        self.assertEqual(precip_adjustment_from_dewpoint(20, None), 0)


class TestPrecipAdjustmentFromPressure(unittest.TestCase):
    """Tests for precipitation adjustment based on pressure trend."""

    def test_rapid_drop(self):
        """Rapid pressure drop (< -2 hPa/3h) -> +15."""
        adj = precip_adjustment_from_pressure(1010, -3.0)
        self.assertEqual(adj, 15)

    def test_rapid_rise(self):
        """Rapid pressure rise (> +2 hPa/3h) -> -10."""
        adj = precip_adjustment_from_pressure(1010, 3.0)
        self.assertEqual(adj, -10)

    def test_none_trend_low_pressure(self):
        """No trend data with low pressure -> +5."""
        adj = precip_adjustment_from_pressure(995, None)
        self.assertEqual(adj, 5)

    def test_none_trend_high_pressure(self):
        """No trend data with high pressure -> -5."""
        adj = precip_adjustment_from_pressure(1030, None)
        self.assertEqual(adj, -5)

    def test_none_trend_none_pressure(self):
        """No data at all -> 0."""
        adj = precip_adjustment_from_pressure(None, None)
        self.assertEqual(adj, 0)

    def test_stable_pressure(self):
        """Small trend -> 0."""
        adj = precip_adjustment_from_pressure(1013, 0.2)
        self.assertEqual(adj, 0)


class TestHeatIndex(unittest.TestCase):
    """Tests for the NWS Rothfusz heat index regression."""

    def test_hot_humid(self):
        """35C / 70% RH should give a heat index well above air temp (~50C)."""
        hi = heat_index(35, 70)
        self.assertIsNotNone(hi)
        self.assertAlmostEqual(hi, 50, delta=2.0)

    def test_below_threshold(self):
        """Below 27C, heat index equals air temperature."""
        hi = heat_index(20, 50)
        self.assertEqual(hi, 20)

    def test_low_humidity_below_threshold(self):
        """Below 40% RH, heat index equals air temperature."""
        hi = heat_index(30, 30)
        self.assertEqual(hi, 30)

    def test_none_temp(self):
        self.assertIsNone(heat_index(None, 70))

    def test_none_humidity(self):
        """None humidity returns temp unchanged."""
        self.assertEqual(heat_index(35, None), 35)


class TestWindChill(unittest.TestCase):
    """Tests for the NWS wind chill formula."""

    def test_cold_windy(self):
        """-5C / 30 km/h should give approximately -12C."""
        wc = wind_chill(-5, 30)
        self.assertIsNotNone(wc)
        self.assertAlmostEqual(wc, -12, delta=1.5)

    def test_above_threshold(self):
        """Above 10C, wind chill equals air temperature."""
        wc = wind_chill(15, 20)
        self.assertEqual(wc, 15)

    def test_low_wind(self):
        """Wind <= 4.8 km/h, wind chill equals air temperature."""
        wc = wind_chill(-5, 3)
        self.assertEqual(wc, -5)

    def test_none_temp(self):
        self.assertIsNone(wind_chill(None, 30))

    def test_none_wind(self):
        self.assertEqual(wind_chill(-5, None), -5)


class TestFeelsLike(unittest.TestCase):
    """Tests for the composite feels-like temperature."""

    def test_hot_conditions_use_heat_index(self):
        """Hot+humid should return heat index (higher than air temp)."""
        fl = feels_like(35, 70, 10)
        self.assertGreater(fl, 35)

    def test_cold_conditions_use_wind_chill(self):
        """Cold+windy should return wind chill (lower than air temp)."""
        fl = feels_like(-5, 50, 30)
        self.assertLess(fl, -5)

    def test_moderate_conditions_return_temp(self):
        """Moderate conditions should return the raw temperature."""
        fl = feels_like(20, 50, 10)
        self.assertEqual(fl, 20)

    def test_none_temp(self):
        self.assertIsNone(feels_like(None, 50, 10))


class TestPrecipIntensityFactor(unittest.TestCase):
    """Tests for Clausius-Clapeyron precipitation intensity scaling."""

    def test_warm_conditions(self):
        """30C should give approximately 2.76 (1.07^15)."""
        f = precip_intensity_factor(30)
        self.assertAlmostEqual(f, 2.759, delta=0.05)

    def test_baseline(self):
        """15C is the baseline -> factor should be 1.0."""
        f = precip_intensity_factor(15)
        self.assertAlmostEqual(f, 1.0, places=3)

    def test_cold_conditions(self):
        """Below baseline -> factor < 1.0."""
        f = precip_intensity_factor(5)
        self.assertLess(f, 1.0)

    def test_none_temp(self):
        self.assertEqual(precip_intensity_factor(None), 1.0)


class TestApplyPhysicsCorrections(unittest.TestCase):
    """End-to-end test for the combined physics correction pipeline."""

    def test_basic_corrections(self):
        """Known inputs should produce reasonable adjusted output."""
        forecast = {"precip_prob": 40, "precip_mm": 5.0}
        adjusted, corrections = apply_physics_corrections(
            forecast,
            pressure_hpa=998,
            humidity_pct=90,
            temp_c=22,
            wind_kmh=15,
        )
        # Precipitation probability should be adjusted upward (low pressure + high humidity)
        self.assertGreater(adjusted["precip_prob"], 40)
        # Should have feels-like and dew point keys
        self.assertIn("feels_like_c", adjusted)
        self.assertIn("dew_point_c", adjusted)
        # Corrections log should not be empty
        self.assertTrue(len(corrections) > 0)

    def test_no_adjustments_on_empty_forecast(self):
        """Empty forecast dict with no weather data should not crash."""
        adjusted, corrections = apply_physics_corrections({})
        self.assertIsInstance(adjusted, dict)
        self.assertIsInstance(corrections, list)

    def test_pressure_stability_added(self):
        """When pressure is given, stability index should be added."""
        forecast = {"precip_prob": 50}
        adjusted, _ = apply_physics_corrections(
            forecast, pressure_hpa=1025, temp_c=20
        )
        self.assertIn("pressure_stability", adjusted)


class TestSeaLevelPressure(unittest.TestCase):
    """Tests for the hypsometric sea-level pressure correction."""

    def test_correction_increases_pressure(self):
        """At elevation > 0, sea-level pressure should exceed station pressure."""
        station = 950.0
        slp = sea_level_pressure(station, elevation_m=500, temp_c=15)
        self.assertGreater(slp, station)

    def test_zero_elevation(self):
        """At sea level (0 m), corrected pressure should equal station pressure."""
        station = 1013.25
        slp = sea_level_pressure(station, elevation_m=0, temp_c=15)
        self.assertAlmostEqual(slp, station, places=2)


if __name__ == "__main__":
    unittest.main()
