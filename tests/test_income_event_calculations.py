import unittest
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src" / "tracker"))

from income_events import compute_income_net_amount, compute_income_net_yield_pct


class IncomeEventCalculationTests(unittest.TestCase):
    def test_compute_net_amount_coupon_with_tax(self):
        self.assertEqual(compute_income_net_amount(1200.0, -156.0), 1044.0)

    def test_compute_net_yield_pct_with_positive_cost_basis(self):
        result = compute_income_net_yield_pct(1044.0, 24000.0)
        self.assertAlmostEqual(result, 4.35, places=2)

    def test_compute_net_yield_pct_with_non_positive_cost_basis(self):
        self.assertEqual(compute_income_net_yield_pct(100.0, 0.0), 0.0)
        self.assertEqual(compute_income_net_yield_pct(100.0, -5.0), 0.0)
        self.assertEqual(compute_income_net_yield_pct(100.0, None), 0.0)


if __name__ == "__main__":
    unittest.main()
