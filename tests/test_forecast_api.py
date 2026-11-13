"""Public forecast contract and the core ensemble path with sources stubbed out."""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import forecast_service
from weighted_forecast import blend_hourly, compute_confidence, weighted_average


def _source(name, temp, high, low, hour_temp, condition="Clear sky"):
    return {
        "source": name,
        "role": "forecast",
        "current": {
            "temp_c": temp,
            "feels_like_c": temp - 1,
            "humidity": 60,
            "wind_speed_kmh": 10,
            "wind_dir_deg": 180,
            "precipitation_mm": 0,
            "pressure_hpa": 1010,
            "condition": condition,
        },
        "daily": [{
            "date": "2026-11-13",
            "high_c": high,
            "low_c": low,
            "precip_mm": 0.0,
            "precip_prob": 10,
            "wind_max_kmh": 12,
            "condition": condition,
        }],
        "hourly": [{
            "hour": "2099-01-01T12:00",
            "temp_c": hour_temp,
            "precip_mm": 0.2,
            "precip_prob": 20,
            "wind_kmh": 11,
            "condition": condition,
        }],
    }


class TestEnsembleMath(unittest.TestCase):
    def test_weighted_average_ignores_nulls(self):
        self.assertEqual(weighted_average([(10, 1), (None, 5), (20, 1)]), 15.0)

    def test_confidence_rises_when_sources_agree(self):
        tight, tight_score = compute_confidence(0.4, {"a": 0.8}, 4)
        wide, wide_score = compute_confidence(9.0, {"a": 0.2}, 1)
        self.assertEqual(tight, "HIGH")
        self.assertLess(wide_score, tight_score)
        self.assertIn(wide, ("LOW", "MODERATE"))

    def test_hourly_blend_averages_sources_and_keeps_them(self):
        results = [
            _source("Open-Meteo", 10, 12, 4, 10),
            _source("SHMU", 12, 14, 5, 14, condition="Partly cloudy"),
        ]

        def weight(_source_name, _metric):
            return 1.0

        hours = blend_hourly(results, weight, timezone_name="UTC", hours=10)
        self.assertEqual(len(hours), 1)
        self.assertEqual(hours[0]["temp_c"], 12.0)
        self.assertEqual(set(hours[0]["sources"]), {"Open-Meteo", "SHMU"})

    def test_failed_source_does_not_wipe_the_forecast(self):
        """One live source is enough when the others return nothing."""
        city = {
            "name": "Bratislava", "country": "Slovakia",
            "lat": 48.15, "lon": 17.11, "timezone": "Europe/Bratislava", "id": None,
        }
        status = {
            "Open-Meteo": "OK",
            "ECMWF": "error: timeout",
            "SHMU": "out_of_domain",
            "wttr.in": "no data/no key",
        }
        with patch("weighted_forecast.fetch_all_sources", return_value=([
            _source("Open-Meteo", 8.0, 11.0, 3.0, 8.5),
        ], status)), patch("weighted_forecast.db.get_weights", return_value={}), \
                patch("weighted_forecast.db.get_city", return_value=None), \
                patch("weighted_forecast.fetch_sea_temperature", return_value=None):
            from weighted_forecast import produce_forecast
            forecast = produce_forecast("Bratislava", location=city, forecast_days=10, respect_tiers=False)
        self.assertNotIn("error", forecast)
        self.assertEqual(forecast["sources_used"], ["Open-Meteo"])
        self.assertEqual(forecast["source_status"]["ECMWF"], "error: timeout")
        self.assertEqual(forecast["daily"][0]["weighted_high_c"], 11.0)
        public = forecast_service.to_public_forecast(forecast)
        self.assertEqual(public["current"]["temperature_c"], 8.0)
        self.assertEqual(public["daily"][0]["temperature_max_c"], 11.0)
        self.assertIn("Open-Meteo", public["current"]["sources"])
        self.assertEqual(public["hourly"][0]["ensemble"]["temperature_c"], 8.5)
        failed = [s for s in public["sources"] if s["id"] == "ECMWF"][0]
        self.assertEqual(failed["status"], "unavailable")


class TestForecastHttp(unittest.TestCase):
    def test_missing_query_is_400(self):
        payload, status = forecast_service.build_forecast(use_cache=False)
        self.assertEqual(status, 400)
        self.assertIn("error", payload)

    def test_bad_coordinate_is_400(self):
        payload, status = forecast_service.build_forecast(lat=120, lon=10, use_cache=False)
        self.assertEqual(status, 400)
        self.assertIn("error", payload)

    def test_route_uses_the_service(self):
        os.environ.setdefault("WEATHER_API_KEY", "")
        import api
        client = api.app.test_client()
        sample = {
            "location": {"name": "Bratislava", "country": "Slovakia", "latitude": 48.15, "longitude": 17.11},
            "current": {"temperature_c": 7.5, "sources": {}, "ensemble": {"temperature_c": 7.5}},
            "hourly": [],
            "daily": [],
            "sources": [],
        }
        with patch("api.forecast_service.build_forecast", return_value=(sample, 200)):
            response = client.get("/api/v1/forecast?lat=48.15&lon=17.11")
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["location"]["name"], "Bratislava")
        self.assertEqual(body["current"]["temperature_c"], 7.5)


if __name__ == "__main__":
    unittest.main()
