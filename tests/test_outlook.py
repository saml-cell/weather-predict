"""Probabilities, reliability and drift math. No network."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from outlook import (
    blend_probabilities,
    brier_skill_score,
    build_climatology,
    compose_drift,
    compose_outlook,
    crps_normal,
    feature_skill,
    reliability_curve,
    skill_label,
    skill_weight,
    within_rate,
)
class TestOutlookMath(unittest.TestCase):
    def test_climatology_pools_nearby_days(self):
        rows = []
        for year in range(2016, 2026):
            for offset in range(-3, 4):
                rows.append({
                    "date": f"{year}-01-{15 + offset:02d}",
                    "tmax": 2.0 + offset,
                    "tmin": -2.0,
                    "precip": 2.0 if offset == 0 else 0.0,
                })
        table = build_climatology(rows, pool_days=7)
        jan15 = table["15"]
        self.assertGreater(jan15["n_temp"], 20)
        self.assertGreater(jan15["p_rain"], 0)
        self.assertLess(jan15["tmax_p33"], jan15["tmax_p67"])

    def test_crps_is_zero_only_for_a_perfect_narrow_forecast(self):
        perfect = crps_normal(5.0, 0.0, 5.0)
        worse = crps_normal(5.0, 2.0, 8.0)
        self.assertEqual(perfect, 0.0)
        self.assertGreater(worse, perfect)

    def test_negative_skill_does_not_get_model_weight(self):
        self.assertEqual(skill_weight(-0.1), 0.0)
        self.assertEqual(skill_label(-0.1), "near_climatology")
        self.assertEqual(skill_weight(None), 0.0)

    def test_blend_with_zero_weight_is_climatology(self):
        clim = {
            "tmax_mean": 4.0, "tmax_p10": 0.0, "tmax_p33": 2.0, "tmax_p50": 4.0,
            "tmax_p67": 6.0, "tmax_p90": 8.0, "tmax_std": 3.0, "p_rain": 0.25,
        }
        model = {
            "tmax_above_10c": 0.9, "tmax_below_0c": 0.0, "tercile_below": 0.1,
            "tercile_near": 0.1, "tercile_above": 0.8, "rain": 0.9,
            "tmax_p10": 12, "tmax_p50": 14, "tmax_p90": 16, "tmax_mean": 14,
        }
        blended = blend_probabilities(model, clim, 0.0)
        self.assertAlmostEqual(blended["rain"], 0.25)
        self.assertAlmostEqual(blended["tercile_above"], 1.0 / 3.0, places=3)
        self.assertEqual(blended["model_weight"], 0.0)

    def test_outlook_says_when_skill_is_climatology(self):
        clim = {
            "tmax_mean": 4.0, "tmax_p10": 0, "tmax_p33": 2, "tmax_p50": 4,
            "tmax_p67": 6, "tmax_p90": 8, "tmax_std": 3, "p_rain": 0.3,
        }
        body = compose_outlook(
            None, clim, [3, 4, 5, 6], [0, 0, 2, 0], "ecmwf_seas5",
            {"rain_brier_skill_score": 0.0, "crps_skill_score": 0.0},
        )
        self.assertEqual(body["skill"]["label"], "near_climatology")
        self.assertIn("climatology", body["skill"]["message"])

    def test_reliability_curve_counts(self):
        curve = reliability_curve([0.1, 0.2, 0.9, 0.95], [0, 0, 1, 1], bins=5)
        self.assertEqual(len(curve), 5)
        populated = [row for row in curve if row["n"]]
        self.assertGreaterEqual(len(populated), 2)

    def test_brier_skill_of_a_perfect_forecast_is_one(self):
        score = brier_skill_score([1, 0, 1, 0], [1, 0, 1, 0], [0.5, 0.5, 0.5, 0.5])
        self.assertAlmostEqual(score, 1.0)

    def test_feature_must_beat_climatology_on_the_held_out_tail(self):
        # A constant predictor cannot beat predicting the anomaly mean of zero.
        useless = feature_skill([1.0] * 60, [1, -1, 0.5, -0.4] * 15)
        self.assertFalse(useless["kept"])
        # A strong positive link should be kept.
        xs = list(range(60))
        ys = [0.5 * x for x in xs]
        useful = feature_skill(xs, ys)
        self.assertTrue(useful["kept"])
        self.assertGreater(useful["improvement_c"], 0.05)

    def test_drift_band_uses_committed_revision_stats(self):
        report = {
            "revision_c": {"mean_c": -0.4, "std_c": 1.0},
            "held_out": {
                "probability_within_1_5c": 0.7,
                "probability_rain_flips": 0.2,
                "brier_within_1_5c": 0.21,
                "brier_rain_flips": 0.16,
                "n": 24,
            },
            "bias_direction": "colder",
        }
        body = compose_drift(8.0, 0.4, report)
        self.assertTrue(body["available"])
        self.assertAlmostEqual(body["predicted_forecast_tmax_c"], 7.6)
        self.assertLess(body["uncertainty_low_c"], body["predicted_forecast_tmax_c"])
        self.assertEqual(body["probability_within_1_5c"], 0.7)
        self.assertEqual(body["reliability"]["brier_within_1_5c"], 0.21)


if __name__ == "__main__":
    unittest.main()
