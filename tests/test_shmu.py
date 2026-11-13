"""SHMÚ parsing helpers. No network and no GRIB files."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from shmu import (
    _within_horizon,
    aggregate_observation_rows,
    condition_from_fields,
    haversine_km,
    in_slovakia,
    nearest_station,
    parse_stations_xml,
    parse_validated_daily,
    precip_increments,
    wind_from_components,
)

_WFS = """<?xml version="1.0" encoding="utf-8"?>
<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0"
    xmlns:gml="http://www.opengis.net/gml/3.2"
    xmlns:Siet="https://example.test/shmu">
  <wfs:member>
    <Siet:Klimatologicka_stanica>
      <Siet:Meteorologický_kód_objektu>11816</Siet:Meteorologický_kód_objektu>
      <Siet:Názov_monitorovacieho_objektu>Bratislava-letisko</Siet:Názov_monitorovacieho_objektu>
      <Siet:Shape><gml:Point><gml:pos>48.171 17.210</gml:pos></gml:Point></Siet:Shape>
    </Siet:Klimatologicka_stanica>
  </wfs:member>
  <wfs:member>
    <Siet:Klimatologicka_stanica>
      <Siet:Meteorologický_kód_objektu>11936</Siet:Meteorologický_kód_objektu>
      <Siet:Názov_monitorovacieho_objektu>Tatranská Javorina</Siet:Názov_monitorovacieho_objektu>
      <Siet:Shape><gml:Point><gml:pos>49.264 20.141</gml:pos></gml:Point></Siet:Shape>
    </Siet:Klimatologicka_stanica>
  </wfs:member>
</wfs:FeatureCollection>
"""


class TestShmuHelpers(unittest.TestCase):
    def test_domain_box(self):
        self.assertTrue(in_slovakia(48.15, 17.11))
        self.assertFalse(in_slovakia(51.5, -0.12))

    def test_nearest_station_is_the_airport(self):
        stations = parse_stations_xml(_WFS)
        self.assertEqual(len(stations), 2)
        chosen = nearest_station(stations, 48.15, 17.11)
        self.assertEqual(chosen["ind_kli"], 11816)
        self.assertLess(chosen["distance_km"], 15)
        self.assertIsNone(nearest_station(stations, 40.0, 17.11, max_km=20))

    def test_haversine_is_short_inside_the_city(self):
        self.assertLess(haversine_km(48.15, 17.11, 48.17, 17.21), 12)

    def test_precip_increments_are_non_negative(self):
        self.assertEqual(precip_increments([None, 0.0, 1.2, 1.0, 3.5]), [None, 0.0, 1.2, 0.0, 2.5])

    def test_wind_components(self):
        speed, direction = wind_from_components(0, -5)  # from the north
        self.assertAlmostEqual(speed, 18.0, delta=0.1)
        self.assertAlmostEqual(direction, 0, delta=1)

    def test_condition_labels(self):
        self.assertEqual(condition_from_fields(12, 0.0, 0.1), "Clear sky")
        self.assertEqual(condition_from_fields(12, 2.0, 0.9), "Moderate rain")
        self.assertEqual(condition_from_fields(-2, 0.5, 0.9), "Snow")

    def test_daily_aggregation(self):
        rows = [
            {"minuta": "2026-11-12T01:00:00", "t": 4.0, "zra_uhrn": 0.2},
            {"minuta": "2026-11-12T15:00:00", "t": 9.5, "zra_uhrn": 1.0},
            {"minuta": "2026-11-13T08:00:00", "t": 6.0, "zra_uhrn": 0.0},
        ]
        daily = aggregate_observation_rows(rows)
        self.assertEqual(daily[0]["date"], "2026-11-12")
        self.assertEqual(daily[0]["temp_high_c"], 9.5)
        self.assertEqual(daily[0]["temp_low_c"], 4.0)
        self.assertEqual(daily[0]["precip_mm"], 1.2)
        self.assertEqual(daily[1]["precip_mm"], 0.0)

    def test_validated_daily_drops_missing_and_sentinels(self):
        payload = {"data": [
            {"ind_kli": 11813, "datum": "2026-08-01T00:00:00", "t_max": 38.2, "t_min": 24.6, "zra_uhrn": None},
            {"ind_kli": 11813, "datum": "2026-08-02T00:00:00", "t_max": 30.0, "t_min": 18.0, "zra_uhrn": -1},
            {"ind_kli": 11816, "datum": "2026-08-01T00:00:00", "t_max": 36.0, "t_min": 22.0, "zra_uhrn": 0.4},
            {"ind_kli": 11813, "datum": "2026-08-03T00:00:00", "t_max": 22.0, "t_min": 14.0, "zra_uhrn": 3.2},
        ]}
        rows = parse_validated_daily(payload, 11813)
        self.assertEqual([row["date"] for row in rows], ["2026-08-01", "2026-08-02", "2026-08-03"])
        self.assertIsNone(rows[0]["precip_mm"])
        self.assertIsNone(rows[1]["precip_mm"])
        self.assertEqual(rows[2]["precip_mm"], 3.2)
        self.assertEqual(rows[0]["temp_high_c"], 38.2)

    def test_horizon_keeps_steps_through_72_only(self):
        names = [f"al-grib_sk_{step:03d}-20261112-1800-nwp-.grb" for step in (0, 1, 72, 73, 102)]
        kept = _within_horizon(names)
        self.assertEqual(kept, names[:3])


if __name__ == "__main__":
    unittest.main()
