"""Verification math used by the Bratislava backtest."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from scoring import (
    brier_score,
    hit_rate,
    inverse_mae_weights,
    mae,
    precipitation_event,
    rmse,
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


if __name__ == "__main__":
    unittest.main()
