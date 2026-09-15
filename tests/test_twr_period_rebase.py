import unittest
from datetime import date
from financetracker.domain.cashflows import rebase_twr_to_period


class PeriodTWRRebaseTests(unittest.TestCase):
    def test_pre_period_gain_and_flat_selected_month_rebases_to_zero(self):
        rebase = rebase_twr_to_period

        result = rebase(
            [date(2026, 3, 31), date(2026, 4, 1), date(2026, 4, 30)],
            [0.20, 0.20, 0.20],
            date(2026, 4, 1),
            date(2026, 5, 1),
        )

        self.assertAlmostEqual(result[date(2026, 4, 30)], 0.0, places=8)

    def test_period_without_pre_period_snapshot_uses_first_point_as_baseline(self):
        rebase = rebase_twr_to_period

        result = rebase(
            [date(2026, 4, 5), date(2026, 4, 30)],
            [0.20, 0.32],
            date(2026, 4, 1),
            date(2026, 5, 1),
        )

        self.assertAlmostEqual(result[date(2026, 4, 5)], 0.0, places=8)
        self.assertAlmostEqual(result[date(2026, 4, 30)], 0.10, places=8)


if __name__ == "__main__":
    unittest.main()
