"""Verification math used by the Bratislava backtest."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from scoring import (
    align_hourly_to_shmu_days,
    brier_score,
    hit_rate,
    inverse_mae_weights,
    mae,
    precipitation_event,
    rmse,
    shmu_climate_day_keys,
    weighted_values,
)


class TestScores(unittest.TestCase):
    def test_mae_and_rmse(self):
        self.assertAlmostEqual(mae([10, 12], [11, 10]), 1.5)
        self.assertAlmostEqual(rmse([10, 12], [11, 10]), (1 + 4) ** 0.5 / (2 ** 0.5))
        #  errors  -1 and +2, squared 1 and 4, mean 2.5, sqrt
        self.assertAlmostEqual(rmse([10, 12], [11, 10]), (2.5) ** 0.5)

    def test_skips_missing(self):
        self.assertAlmostEqual(mae([None, 5], [1, 7]), 2.0)
        self.assertIsNone(mae([None], [1]))

    def test_brier_perfect_and_coin(self):
        self.assertEqual(brier_score([1, 0, 1], [1, 0, 1]), 0.0)
        self.assertAlmostEqual(brier_score([0.5, 0.5], [1, 0]), 0.25)

    def test_hit_rate(self):
        self.assertEqual(hit_rate([True, False, True], [True, False, False]), 2 / 3)
        self.assertIsNone(hit_rate([None], [True]))

    def test_wet_day_threshold(self):
        self.assertFalse(precipitation_event(0.9, 1.0))
        self.assertTrue(precipitation_event(1.0, 1.0))
        self.assertIsNone(precipitation_event(None))

    def test_inverse_mae_weights_prefer_the_better_source(self):
        weights = inverse_mae_weights({"a": 1.0, "b": 3.0}, epsilon=0.0)
        self.assertAlmostEqual(sum(weights.values()), 1.0)
        self.assertGreater(weights["a"], weights["b"])

    def test_weighted_values_renormalise(self):
        value = weighted_values({"a": 10, "b": None}, {"a": 0.25, "b": 0.75})
        self.assertEqual(value, 10)

    def test_shmu_climate_windows(self):
        # 21:00 stays on that date; 22:00 belongs to the next temperature day.
        self.assertEqual(shmu_climate_day_keys("2026-08-01T21:00")[0], "2026-08-01")
        self.assertEqual(shmu_climate_day_keys("2026-08-01T22:00")[0], "2026-08-02")
        # 07:00 ends the previous precip day; 08:00 starts the current one.
        self.assertEqual(shmu_climate_day_keys("2026-08-02T07:00")[1], "2026-08-01")
        self.assertEqual(shmu_climate_day_keys("2026-08-02T08:00")[1], "2026-08-02")

    def test_align_hourly_requires_enough_hours(self):
        times = [f"2026-08-02T{hour:02d}:00" for hour in range(24)]
        temps = [10.0] * 24
        temps[15] = 18.0
        precips = [0.0] * 24
        precips[8] = 2.0
        rows = align_hourly_to_shmu_days(times, temps, precips, min_temp_hours=18, min_precip_hours=20)
        # 00:00-21:00 on the 2nd is 22 hours, enough for a temperature day.
        self.assertEqual(rows["2026-08-02"]["temp_high_c"], 18.0)
        self.assertIsNotNone(rows["2026-08-02"]["temp_low_c"])


if __name__ == "__main__":
    unittest.main()
