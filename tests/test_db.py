#!/usr/bin/env python3
"""Unit tests for the database layer (scripts/db.py)."""

import os
import sys
import unittest

# Allow imports from the scripts/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from db import normalize_condition, load_config, get_connection


class TestNormalizeCondition(unittest.TestCase):
    """Tests for weather condition normalization."""

    def test_heavy_rain(self):
        self.assertEqual(normalize_condition("Heavy rain"), "heavy_rain")

    def test_sunny(self):
        self.assertEqual(normalize_condition("Sunny"), "clear")

    def test_partly_cloudy(self):
        result = normalize_condition("Partly cloudy")
        self.assertEqual(result, "partly_cloudy")

    def test_unknown_string(self):
        """Unrecognized descriptions should map to 'unknown'."""
        self.assertEqual(normalize_condition("alien weather"), "unknown")

    def test_none_input(self):
        self.assertEqual(normalize_condition(None), "unknown")

    def test_empty_string(self):
        self.assertEqual(normalize_condition(""), "unknown")

    def test_case_insensitive(self):
        self.assertEqual(normalize_condition("THUNDERSTORM"), "thunderstorm")

    def test_whitespace_stripped(self):
        self.assertEqual(normalize_condition("  fog  "), "fog")


class TestLoadConfig(unittest.TestCase):
    """Tests for config loading."""

    def test_returns_dict(self):
        cfg = load_config()
        self.assertIsInstance(cfg, dict)

    def test_has_expected_keys(self):
        """Config should contain at least default_cities and scoring."""
        cfg = load_config()
        self.assertIn("default_cities", cfg)
        self.assertIn("scoring", cfg)


class TestGetConnection(unittest.TestCase):
    """Tests for database connection factory."""

    def test_returns_connection(self):
        import sqlite3
        conn = get_connection()
        self.assertIsInstance(conn, sqlite3.Connection)
        conn.close()

    def test_row_factory_set(self):
        import sqlite3
        conn = get_connection()
        self.assertEqual(conn.row_factory, sqlite3.Row)
        conn.close()


if __name__ == "__main__":
    unittest.main()
