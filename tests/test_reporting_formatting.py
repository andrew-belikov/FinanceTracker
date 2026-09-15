from datetime import datetime, timezone
from decimal import Decimal
import unittest

from financetracker.reporting.formatting import (
    display_date,
    display_pct_compact,
    display_rub,
    display_timestamp,
    to_decimal,
)


class ReportingFormattingTests(unittest.TestCase):
    def test_display_helpers_keep_report_contract(self):
        self.assertEqual(to_decimal(None), Decimal("0"))
        self.assertEqual(display_rub(Decimal("1200"), precision=0), "1 200 ₽")
        self.assertEqual(display_pct_compact(Decimal("12.34")), "12,3%")
        self.assertEqual(display_date("2026-09-15"), "15.09.2026")
        self.assertEqual(
            display_timestamp(datetime(2026, 9, 15, 8, 30, tzinfo=timezone.utc)),
            "15.09.2026 08:30 UTC",
        )
