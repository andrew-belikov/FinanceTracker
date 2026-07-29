from decimal import Decimal
import unittest

from src.common.finance import annualize_simple_yield_pct


class AnnualizedCouponYieldTests(unittest.TestCase):
    def test_annualizes_182_day_coupon_without_compounding(self):
        result = annualize_simple_yield_pct(Decimal("5.123455"), 182)

        self.assertEqual(
            result.quantize(Decimal("0.01")),
            Decimal("10.28"),
        )

    def test_uses_act_365f_for_leap_year_period(self):
        result = annualize_simple_yield_pct(Decimal("10"), 366)

        self.assertEqual(
            result.quantize(Decimal("0.0001")),
            Decimal("9.9727"),
        )

    def test_rejects_missing_invalid_and_non_positive_inputs(self):
        self.assertIsNone(annualize_simple_yield_pct(None, 182))
        self.assertIsNone(annualize_simple_yield_pct("NaN", 182))
        self.assertIsNone(annualize_simple_yield_pct(5, None))
        self.assertIsNone(annualize_simple_yield_pct(5, 0))
        self.assertIsNone(annualize_simple_yield_pct(5, -1))
        self.assertIsNone(annualize_simple_yield_pct(0, 182))
