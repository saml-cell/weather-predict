"""Skill weights fall back to equal shares without enough SHMÚ history."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from skill_weights import lead_bucket, resolve_weights


class TestSkillWeights(unittest.TestCase):
    def test_lead_buckets(self):
        self.assertEqual(lead_bucket(0), "1")
        self.assertEqual(lead_bucket(1), "1")
        self.assertEqual(lead_bucket(3), "2-3")
        self.assertEqual(lead_bucket(6), "4-7")

    def test_inverse_mae_prefers_the_better_source(self):
        table = {
            "min_samples": 7,
            "variables": {
                "temperature_max": {
                    "1": {
                        "Open-Meteo": {"mae": 1.0, "n": 30},
                        "ECMWF": {"mae": 2.0, "n": 30},
                    }
                }
            },
        }
        weights, scheme = resolve_weights(table, ["Open-Meteo", "ECMWF", "SHMU"], "temperature_max", 1)
        self.assertEqual(scheme, "inverse_mae")
        self.assertGreater(weights["Open-Meteo"]["weight"], weights["ECMWF"]["weight"])
        self.assertTrue(weights["SHMU"]["fallback"])
        self.assertAlmostEqual(sum(row["weight"] for row in weights.values()), 1.0)

    def test_equal_when_only_one_source_has_history(self):
        table = {
            "min_samples": 7,
            "variables": {
                "temperature_max": {
                    "1": {"Open-Meteo": {"mae": 1.0, "n": 30}}
                }
            },
        }
        weights, scheme = resolve_weights(table, ["Open-Meteo", "ECMWF"], "temperature_max", 1)
        self.assertEqual(scheme, "equal")
        self.assertAlmostEqual(weights["Open-Meteo"]["weight"], 0.5)
        self.assertTrue(weights["ECMWF"]["fallback"])

    def test_short_sample_does_not_count(self):
        table = {
            "min_samples": 7,
            "variables": {
                "precipitation": {
                    "climatology": {
                        "Open-Meteo": {"mae": 0.5, "n": 3},
                        "ECMWF": {"mae": 1.5, "n": 40},
                    }
                }
            },
        }
        weights, scheme = resolve_weights(table, ["Open-Meteo", "ECMWF"], "precipitation", 5)
        self.assertEqual(scheme, "equal")


if __name__ == "__main__":
    unittest.main()
