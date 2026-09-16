import unittest

from financetracker.tracker.payloads import json_value, money_to_float, quotation_to_float, to_int
from financetracker.tracker.snapshot_sync import compute_expected_yield_pct


class TrackerPayloadTests(unittest.TestCase):
    def test_broker_numeric_values_and_field_aliases(self):
        self.assertEqual(to_int("bad", 7), 7)
        self.assertEqual(quotation_to_float({"units": "3", "nano": "250000000"}), 3.25)
        self.assertEqual(money_to_float(None), None)
        self.assertEqual(json_value({"currentNkd": 4, "current_nkd": 3}, "current_nkd"), 4)
        self.assertEqual(json_value({"current_nkd": 3}, "current_nkd"), 3)

    def test_snapshot_owner_returns_none_for_zero_cost_basis(self):
        self.assertIsNone(compute_expected_yield_pct(100.0, 100.0))
        self.assertEqual(compute_expected_yield_pct(100.0, 1_100.0), 10.0)
