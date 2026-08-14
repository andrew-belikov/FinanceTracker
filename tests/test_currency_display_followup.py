import sys
import unittest
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_payload  # noqa: E402
import queries  # noqa: E402
import services  # noqa: E402


BREAKDOWN = [
    {
        "currency": "RUB",
        "coupons": Decimal("87"),
        "dividends": Decimal("0"),
        "taxes": Decimal("13"),
        "tax_refunds": Decimal("3"),
    },
    {
        "currency": "UNKNOWN",
        "coupons": Decimal("2"),
        "dividends": Decimal("0"),
        "taxes": Decimal("0"),
        "tax_refunds": Decimal("0"),
    },
    {
        "currency": "USD",
        "coupons": Decimal("9"),
        "dividends": Decimal("4"),
        "taxes": Decimal("1"),
        "tax_refunds": Decimal("2"),
    },
]


@contextmanager
def fake_db_session():
    yield object()


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 4, 15, 12, tzinfo=timezone.utc)
        return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)


class CurrencySummaryDisplayTests(unittest.TestCase):
    def _common_patches(self):
        return (
            mock.patch.object(services, "datetime", FixedDateTime),
            mock.patch.object(services, "db_session", fake_db_session),
            mock.patch.object(services, "resolve_reporting_account_id", return_value="acc"),
            mock.patch.object(services, "get_income_for_period", return_value=(Decimal("87"), Decimal("0"))),
            mock.patch.object(
                services,
                "get_income_currency_breakdown_for_period",
                return_value=BREAKDOWN,
                create=True,
            ),
            mock.patch.object(services, "get_iis_tax_deductions_for_period", return_value=Decimal("0")),
            mock.patch.object(services, "get_commissions_for_period", return_value=Decimal("0")),
            mock.patch.object(services, "get_taxes_for_period", return_value=Decimal("13")),
            mock.patch.object(services, "get_tax_refunds_for_period", return_value=Decimal("3")),
            mock.patch.object(services, "get_deposits_for_period", return_value=0.0),
            mock.patch.object(services, "get_net_external_flow_for_period", return_value=0.0),
        )

    def _assert_breakdown_visible(self, text_value):
        self.assertIn("RUB", text_value)
        self.assertIn("87", text_value)
        self.assertIn("USD", text_value)
        self.assertIn("9", text_value)
        self.assertIn("налог", text_value.lower())
        self.assertIn("возврат", text_value.lower())
        self.assertIn("UNKNOWN", text_value)
        self.assertIn("не включена", text_value.lower())

    def test_today_week_month_show_each_income_currency_and_unknown_warning(self):
        builders = (
            (
                services.build_today_summary,
                (
                    mock.patch.object(
                        services,
                        "get_latest_snapshots",
                        return_value=[
                            {
                                "snapshot_date": date(2026, 4, 15),
                                "snapshot_at": None,
                                "total_value": 1000,
                                "currency": "RUB",
                            }
                        ],
                    ),
                    mock.patch.object(services, "get_net_external_contributions", return_value=1000),
                ),
            ),
            (
                services.build_week_summary,
                (
                    mock.patch.object(
                        services,
                        "get_latest_snapshot_with_id",
                        return_value={"id": 1, "snapshot_date": date(2026, 4, 15), "total_value": 1000},
                    ),
                    mock.patch.object(services, "get_last_snapshot_before_date", return_value=None),
                ),
            ),
            (
                services.build_month_summary,
                (
                    mock.patch.object(
                        services,
                        "get_month_snapshots",
                        return_value=(None, {"id": 1, "snapshot_date": date(2026, 4, 15), "total_value": 1000}),
                    ),
                    mock.patch.object(services, "get_positions_for_snapshot", return_value=[]),
                    mock.patch.object(services, "compute_positions_diff_lines", return_value=[]),
                ),
            ),
        )

        for builder, specific_patches in builders:
            with self.subTest(builder.__name__), ExitStack() as stack:
                for patcher in self._common_patches() + specific_patches:
                    stack.enter_context(patcher)
                self._assert_breakdown_visible(builder())


class CurrencyReportFactsTests(unittest.TestCase):
    def test_report_payload_and_ai_keep_currency_breakdown(self):
        payload = {
            "summary_metrics": {
                "deposits": "0",
                "withdrawals": "0",
                "income_net": "87",
                "iis_tax_deduction_income": "0",
                "total_income_net": "87",
                "commissions": "0",
                "taxes": "13",
                "tax_refunds": "3",
            },
            "income_by_currency": [
                {key: str(value) if isinstance(value, Decimal) else value for key, value in row.items()}
                for row in BREAKDOWN
            ],
            "operations_top": [],
        }

        facts = report_payload._build_cashflow_facts(payload)

        self.assertEqual([row["currency"] for row in facts["income_by_currency"]], ["RUB", "UNKNOWN", "USD"])
        self.assertEqual(facts["income_by_currency"][2]["coupons"], "9.00 USD")
        self.assertEqual(facts["income_by_currency"][2]["taxes"], "1.00 USD")
        self.assertEqual(facts["income_by_currency"][2]["tax_refunds"], "2.00 USD")
        self.assertTrue(facts["unknown_income_currency_warning"])


class _MappingRows:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _BreakdownDb:
    def __init__(self):
        self._results = [
            [
                {
                    "currency": "RUB",
                    "coupons": Decimal("87"),
                    "dividends": Decimal("0"),
                    "taxes": Decimal("13"),
                    "tax_refunds": Decimal("3"),
                },
                {
                    "currency": "USD",
                    "coupons": Decimal("9"),
                    "dividends": Decimal("4"),
                    "taxes": Decimal("1"),
                    "tax_refunds": Decimal("2"),
                },
            ],
            [
                {
                    "currency": "UNKNOWN",
                    "taxes": Decimal("5"),
                    "tax_refunds": Decimal("7"),
                }
            ],
        ]

    @contextmanager
    def begin_nested(self):
        yield

    def execute(self, *_args, **_kwargs):
        return _MappingRows(self._results.pop(0))


class _UndefinedTableError(Exception):
    pgcode = "42P01"


class _MissingIncomeDb:
    def __init__(self):
        self.calls = 0
        self.savepoints = 0

    @contextmanager
    def begin_nested(self):
        self.savepoints += 1
        yield

    def execute(self, *_args, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            raise queries.ProgrammingError("select income_events", {}, _UndefinedTableError())
        return _MappingRows([])


class CurrencyBreakdownQueryTests(unittest.TestCase):
    def test_income_taxes_and_refunds_stay_separate_by_nominal_currency(self):
        rows = queries.get_income_currency_breakdown_for_period(
            _BreakdownDb(),
            "acc",
            datetime(2026, 4, 1),
            datetime(2026, 5, 1),
        )

        self.assertEqual([row["currency"] for row in rows], ["RUB", "UNKNOWN", "USD"])
        self.assertEqual(rows[0]["coupons"], Decimal("87"))
        self.assertEqual(rows[0]["taxes"], Decimal("13"))
        self.assertEqual(rows[1]["taxes"], Decimal("5"))
        self.assertEqual(rows[1]["tax_refunds"], Decimal("7"))
        self.assertEqual(rows[2]["coupons"], Decimal("9"))
        self.assertEqual(rows[2]["tax_refunds"], Decimal("2"))

    def test_missing_optional_income_table_returns_empty_and_transaction_continues(self):
        db = _MissingIncomeDb()

        rows = queries.get_income_currency_breakdown_for_period(
            db,
            "acc",
            datetime(2026, 4, 1),
            datetime(2026, 5, 1),
        )

        self.assertEqual(rows, [])
        self.assertEqual(db.savepoints, 1)
        self.assertEqual(db.calls, 2)


if __name__ == "__main__":
    unittest.main()
