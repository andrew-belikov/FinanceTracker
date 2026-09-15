from __future__ import annotations

from decimal import Decimal
import unittest

from financetracker.domain.payouts import estimate_net_payout_amount, resolve_payout_amount


class PayoutAmountTests(unittest.TestCase):
    def test_known_payout_is_reduced_by_tax(self):
        self.assertEqual(
            estimate_net_payout_amount(Decimal("100"), Decimal("13")),
            Decimal("87.00"),
        )

    def test_coupon_without_amount_uses_previous_coupon_per_unit(self):
        amount, estimated = resolve_payout_amount(
            {
                "event_type": "coupon",
                "expected_amount": None,
                "previous_coupon_amount_per_unit": Decimal("12.50"),
                "quantity": Decimal("4"),
            },
            Decimal("13"),
        )
        self.assertEqual(amount, Decimal("43.50"))
        self.assertTrue(estimated)

    def test_unknown_non_coupon_payout_remains_unknown(self):
        amount, estimated = resolve_payout_amount(
            {"event_type": "dividend", "expected_amount": None},
            Decimal("13"),
        )
        self.assertIsNone(amount)
        self.assertFalse(estimated)


if __name__ == "__main__":
    unittest.main()
