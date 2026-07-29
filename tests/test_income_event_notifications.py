from decimal import Decimal
from pathlib import Path
import sys
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import jobs as bot_jobs


class IncomeEventNotificationRenderingTests(unittest.TestCase):
    def test_coupon_notification_adds_simple_annual_equivalent(self):
        text = bot_jobs.build_income_event_notification_text(
            {
                "event_type": "coupon",
                "net_amount": Decimal("638.82"),
                "net_yield_pct": Decimal("5.123455"),
                "coupon_period_days": 182,
                "instrument_name": "ОФЗ 26233",
            }
        )

        self.assertIn("+638.82 ₽ (5.12 %)", text)
        self.assertIn("≈ 10.28 % годовых по этому купону", text)

    def test_missing_period_and_dividend_keep_original_format(self):
        coupon_text = bot_jobs.build_income_event_notification_text(
            {
                "event_type": "coupon",
                "net_amount": Decimal("100"),
                "net_yield_pct": Decimal("2"),
                "coupon_period_days": None,
                "instrument_name": "Облигация",
            }
        )
        dividend_text = bot_jobs.build_income_event_notification_text(
            {
                "event_type": "dividend",
                "net_amount": Decimal("100"),
                "net_yield_pct": Decimal("2"),
                "coupon_period_days": 90,
                "instrument_name": "Акция",
            }
        )

        self.assertNotIn("годовых", coupon_text)
        self.assertNotIn("годовых", dividend_text)
