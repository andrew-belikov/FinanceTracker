import ast
import unittest
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICES_FILE = PROJECT_ROOT / "src" / "bot" / "services.py"
REPORT_PAYLOAD_FILE = PROJECT_ROOT / "src" / "bot" / "report_payload.py"
QUERIES_FILE = PROJECT_ROOT / "src" / "bot" / "queries.py"


def load_function(file_path: Path, name: str, namespace: dict):
    module_ast = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            copied = deepcopy(node)
            copied.returns = None
            for arg in (*copied.args.args, *copied.args.kwonlyargs):
                arg.annotation = None
            exec(compile(ast.Module(body=[copied], type_ignores=[]), str(file_path), "exec"), namespace)
            return namespace[name]
    return None


def normalize_decimal(value):
    return Decimal(str(value or 0))


class DailyPortfolioResultTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        namespace = {
            "Decimal": Decimal,
            "date": date,
            "normalize_decimal": normalize_decimal,
            "to_iso_datetime": lambda value: value.isoformat() if value is not None else None,
        }
        cls.build_rows = staticmethod(load_function(REPORT_PAYLOAD_FILE, "_build_timeseries_daily", namespace))
        cls.compute_period_pnl = staticmethod(load_function(REPORT_PAYLOAD_FILE, "_compute_period_pnl", namespace))

    def _day_result(
        self,
        *,
        end_value: str,
        deposit: str = "0",
        withdrawal: str = "0",
        income: str = "0",
        commission: str = "0",
        tax: str = "0",
    ) -> Decimal:
        first = date(2026, 4, 1)
        second = date(2026, 4, 2)
        rows = self.build_rows(
            [
                {"id": 1, "snapshot_date": first, "total_value": Decimal("100")},
                {"id": 2, "snapshot_date": second, "total_value": Decimal(end_value)},
            ],
            deposits_by_day={second: Decimal(deposit)},
            iis_tax_deductions_by_day={},
            withdrawals_by_day={second: Decimal(withdrawal)},
            income_net_by_day={second: Decimal(income)},
            commissions_by_day={second: Decimal(commission)},
            taxes_by_day={second: Decimal(tax)},
            income_tax_by_day={},
            twr_by_date={},
        )
        return rows[-1]["day_pnl"]

    def test_only_external_deposit_and_withdrawal_are_removed_from_value_change(self):
        cases = (
            ("deposit", {"end_value": "120", "deposit": "20"}, Decimal("0")),
            ("withdrawal", {"end_value": "80", "withdrawal": "20"}, Decimal("0")),
            ("dividend", {"end_value": "110", "income": "10"}, Decimal("10")),
            ("commission", {"end_value": "95", "commission": "5"}, Decimal("-5")),
            ("tax", {"end_value": "95", "tax": "5"}, Decimal("-5")),
        )
        for label, kwargs, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(self._day_result(**kwargs), expected)

    def test_no_start_snapshot_fallback_subtracts_only_external_flow(self):
        first = date(2026, 4, 1)
        second = date(2026, 4, 2)
        daily_rows = self.build_rows(
            [
                {"id": 1, "snapshot_date": first, "total_value": Decimal("100")},
                {"id": 2, "snapshot_date": second, "total_value": Decimal("110")},
            ],
            deposits_by_day={},
            iis_tax_deductions_by_day={},
            withdrawals_by_day={},
            income_net_by_day={second: Decimal("10")},
            commissions_by_day={},
            taxes_by_day={},
            income_tax_by_day={},
            twr_by_date={},
        )
        pnl_abs, pnl_pct = self.compute_period_pnl(
            start_snapshot=None,
            end_value=Decimal("110"),
            start_value=Decimal("100"),
            net_external_flow=Decimal("0"),
            daily_rows=daily_rows,
        )

        self.assertEqual(pnl_abs, Decimal("10"))
        self.assertEqual(pnl_pct, Decimal("10"))


class TodaySummaryContractTests(unittest.TestCase):
    def _build(self, snapshots, *, expected_interval_flow, deposits=100, net_contributions=50):
        calls = []

        @contextmanager
        def fake_db_session():
            yield object()

        def net_flow(_session, _account_id, start_dt, end_dt):
            calls.append((start_dt, end_dt))
            previous_date = snapshots[1]["snapshot_date"]
            latest_date = snapshots[0]["snapshot_date"]
            if start_dt.date() == previous_date + timedelta(days=1) and end_dt.date() == latest_date + timedelta(days=1):
                return expected_interval_flow
            return 0

        namespace = {
            "datetime": datetime,
            "time": time,
            "timedelta": timedelta,
            "timezone": timezone,
            "TZ": timezone.utc,
            "db_session": fake_db_session,
            "resolve_reporting_account_id": lambda _session: "acc",
            "get_latest_snapshots": lambda _session, _account_id, limit=2: snapshots,
            "get_net_external_flow_for_period": net_flow,
            "get_total_deposits": lambda _session, _account_id: deposits,
            "get_net_external_contributions": lambda _session, _account_id: net_contributions,
            "get_income_for_period": lambda *_args: (Decimal("0"), Decimal("0")),
            "get_iis_tax_deductions_for_period": lambda *_args: Decimal("0"),
            "get_commissions_for_period": lambda *_args: Decimal("0"),
            "get_taxes_for_period": lambda *_args: Decimal("0"),
            "get_tax_refunds_for_period": lambda *_args: Decimal("0"),
            "compute_period_delta_excluding_external_flow": lambda start, end, flow: (
                (None, None)
                if start in (None, 0) or end is None
                else (end - start - flow, (end - start - flow) / start * 100)
            ),
            "TodayContext": lambda **kwargs: kwargs,
            "render_today_text": lambda context: context,
            "fmt_rub": lambda value: value,
            "fmt_pct": lambda value: value,
            "fmt_decimal_rub": lambda value: value,
            "append_tax_refund_line": lambda text_value, _refunds: text_value,
            "REPORTING_ACCOUNT_UNAVAILABLE_TEXT": "unavailable",
        }
        build_today = load_function(SERVICES_FILE, "build_today_summary", namespace)
        return build_today(), calls

    def test_today_uses_full_interval_between_latest_snapshots(self):
        latest_date = datetime.now(timezone.utc).date()
        summary, calls = self._build(
            [
                {"snapshot_date": latest_date, "snapshot_at": None, "total_value": 150},
                {"snapshot_date": latest_date - timedelta(days=2), "snapshot_at": None, "total_value": 100},
            ],
            expected_interval_flow=50,
            net_contributions=150,
        )

        self.assertEqual(summary["delta_abs"], 0)
        self.assertEqual(len(calls), 1)

    def test_lifetime_round_trip_does_not_turn_withdrawal_into_loss(self):
        latest_date = datetime.now(timezone.utc).date()
        summary, _calls = self._build(
            [
                {"snapshot_date": latest_date, "snapshot_at": None, "total_value": 50},
                {"snapshot_date": latest_date - timedelta(days=1), "snapshot_at": None, "total_value": 50},
            ],
            expected_interval_flow=0,
            deposits=100,
            net_contributions=50,
        )

        self.assertEqual(summary["pnl_abs"], 0)
        self.assertEqual(summary["pnl_pct"], 0)


class StructurePnlDenominatorTests(unittest.TestCase):
    def _render(self, positions):
        @contextmanager
        def fake_db_session():
            yield object()

        namespace = {
            "db_session": fake_db_session,
            "resolve_reporting_account_id": lambda _session: "acc",
            "get_latest_snapshot_with_id": lambda _session, _account_id: {
                "id": 1,
                "snapshot_date": date(2026, 8, 13),
                "total_value": sum(float(row["position_value"]) for row in positions),
            },
            "get_positions_for_snapshot": lambda _session, _snapshot_id: positions,
            "_instrument_type_to_group": lambda _value: "Акции",
            "fmt_rub": lambda value, precision=2: f"{float(value):.{precision}f}",
            "ACCOUNT_FRIENDLY_NAME": "Test",
            "REPORTING_ACCOUNT_UNAVAILABLE_TEXT": "unavailable",
        }
        namespace["compute_cost_basis_pnl_pct"] = load_function(
            SERVICES_FILE,
            "compute_cost_basis_pnl_pct",
            namespace,
        )
        build_structure = load_function(SERVICES_FILE, "build_structure_text", namespace)
        return build_structure()

    def test_one_position_uses_same_cost_basis_pct_at_every_level(self):
        text = self._render(
            [
                {
                    "instrument_type": "share",
                    "quantity": 1,
                    "current_price": 110,
                    "position_value": 110,
                    "expected_yield": 10,
                    "expected_yield_pct": 10,
                    "weight_pct": 100,
                    "ticker": "ONE",
                    "figi": "ONE",
                    "name": "One",
                }
            ]
        )

        self.assertEqual(text.count("(+10.0 %)"), 3)

    def test_multiple_positions_use_aggregated_cost_basis(self):
        text = self._render(
            [
                {
                    "instrument_type": "share", "quantity": 1, "current_price": 110,
                    "position_value": 110, "expected_yield": 10, "expected_yield_pct": 999,
                    "weight_pct": 65, "ticker": "ONE", "figi": "ONE", "name": "One",
                },
                {
                    "instrument_type": "share", "quantity": 1, "current_price": 60,
                    "position_value": 60, "expected_yield": 10, "expected_yield_pct": 999,
                    "weight_pct": 35, "ticker": "TWO", "figi": "TWO", "name": "Two",
                },
            ]
        )

        self.assertEqual(text.count("(+13.3 %)"), 2)
        self.assertNotIn("+999.0 %", text)


class FakeSql(str):
    def bindparams(self, *_args, **_kwargs):
        return self


class FakeScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class SequentialDb:
    def __init__(self, values):
        self.values = iter(values)

    def execute(self, *_args, **_kwargs):
        return FakeScalarResult(next(self.values))


class TaxSignContractTests(unittest.TestCase):
    def _load_query(self, name):
        namespace = {
            "Decimal": Decimal,
            "text": FakeSql,
            "bindparam": lambda *_args, **_kwargs: object(),
            "OPERATIONS_DEDUP_CTE": "",
            "TAX_OPERATION_TYPES": ("OPERATION_TYPE_TAX",),
            "EXECUTED_OPERATION_STATE": "OPERATION_STATE_EXECUTED",
            "_is_undefined_table_error": lambda *_args: False,
        }
        return load_function(QUERIES_FILE, name, namespace)

    def test_withholding_and_operation_tax_are_positive_expenses(self):
        get_taxes = self._load_query("get_taxes_for_period")

        result = get_taxes(SequentialDb([-13, 5]), "acc", datetime(2026, 1, 1), datetime(2026, 2, 1))

        self.assertEqual(result, Decimal("18"))

    def test_positive_tax_cashflow_is_reported_as_separate_refund(self):
        get_taxes = self._load_query("get_taxes_for_period")
        get_refunds = self._load_query("get_tax_refunds_for_period")
        self.assertIsNotNone(get_refunds, "separate tax refund query is missing")

        self.assertEqual(
            get_taxes(SequentialDb([0, 0]), "acc", datetime(2026, 1, 1), datetime(2026, 2, 1)),
            Decimal("0"),
        )
        self.assertEqual(
            get_refunds(SequentialDb([7, 3]), "acc", datetime(2026, 1, 1), datetime(2026, 2, 1)),
            Decimal("10"),
        )


if __name__ == "__main__":
    unittest.main()
