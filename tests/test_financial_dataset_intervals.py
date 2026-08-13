import ast
import unittest
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATASET_FILE = PROJECT_ROOT / "src" / "bot" / "dataset.py"
SERVICES_FILE = PROJECT_ROOT / "src" / "bot" / "services.py"


def load_build_dataset_export(namespace):
    module_ast = ast.parse(DATASET_FILE.read_text(encoding="utf-8"), filename=str(DATASET_FILE))
    node = next(
        node
        for node in module_ast.body
        if isinstance(node, ast.FunctionDef) and node.name == "build_dataset_export"
    )
    copied = deepcopy(node)
    copied.returns = None
    for arg in (*copied.args.args, *copied.args.kwonlyargs):
        arg.annotation = None
    exec(compile(ast.Module(body=[copied], type_ignores=[]), str(DATASET_FILE), "exec"), namespace)
    return namespace["build_dataset_export"]


def load_interval_sum(namespace):
    module_ast = ast.parse(SERVICES_FILE.read_text(encoding="utf-8"), filename=str(SERVICES_FILE))
    node = next(
        node
        for node in module_ast.body
        if isinstance(node, ast.FunctionDef) and node.name == "sum_decimal_values_for_snapshot_interval"
    )
    copied = deepcopy(node)
    copied.returns = None
    for arg in (*copied.args.args, *copied.args.kwonlyargs):
        arg.annotation = None
    exec(compile(ast.Module(body=[copied], type_ignores=[]), str(SERVICES_FILE), "exec"), namespace)
    return namespace["sum_decimal_values_for_snapshot_interval"]


def load_service_function(name, namespace):
    module_ast = ast.parse(SERVICES_FILE.read_text(encoding="utf-8"), filename=str(SERVICES_FILE))
    node = next(node for node in module_ast.body if isinstance(node, ast.FunctionDef) and node.name == name)
    copied = deepcopy(node)
    copied.returns = None
    for arg in (*copied.args.args, *copied.args.kwonlyargs):
        arg.annotation = None
    exec(compile(ast.Module(body=[copied], type_ignores=[]), str(SERVICES_FILE), "exec"), namespace)
    return namespace[name]


class DatasetSnapshotIntervalTests(unittest.TestCase):
    def _build_export(self, operation_specs, *, end_value="150"):
        first = date(2026, 4, 1)
        flow_day = date(2026, 4, 2)
        second = date(2026, 4, 3)
        daily_rows = [
            {
                "id": 1,
                "snapshot_date": first,
                "snapshot_at": datetime(2026, 4, 1, 18, tzinfo=timezone.utc),
                "total_value": Decimal("100"),
                "expected_yield": Decimal("0"),
                "expected_yield_pct": Decimal("0"),
            },
            {
                "id": 2,
                "snapshot_date": second,
                "snapshot_at": datetime(2026, 4, 3, 18, tzinfo=timezone.utc),
                "total_value": Decimal(end_value),
                "expected_yield": Decimal("0"),
                "expected_yield_pct": Decimal("0"),
            },
        ]
        operations = []
        for index, (currency, amount) in enumerate(operation_specs):
            operations.append(
                {
                    "operation_id": f"synthetic-deposit-{index}",
                    "date": datetime(2026, 4, 2, 10, tzinfo=timezone.utc),
                    "operation_type": "OPERATION_TYPE_INPUT",
                    "cashflow_category": None,
                    "state": "OPERATION_STATE_EXECUTED",
                    "instrument_uid": None,
                    "asset_uid": None,
                    "figi": None,
                    "name": "Synthetic deposit",
                    "amount": Decimal(amount),
                    "currency": currency,
                    "price": Decimal("0"),
                    "quantity": Decimal("0"),
                    "commission": Decimal("0"),
                    "yield": Decimal("0"),
                    "description": "synthetic",
                    "source": "test",
                }
            )
        latest = {**daily_rows[-1], "currency": "RUB"}

        namespace = {
            "date": date,
            "datetime": datetime,
            "time": time,
            "timedelta": timedelta,
            "timezone": timezone,
            "Decimal": Decimal,
            "TZ_NAME": "Europe/Moscow",
            "ACCOUNT_FRIENDLY_NAME": "Test",
            "IIS_TAX_DEDUCTION_CATEGORY": "iis_tax_deduction",
            "REPORTING_ACCOUNT_UNAVAILABLE_TEXT": "unavailable",
            "resolve_reporting_account_id": lambda _session: "acc",
            "get_dataset_bounds": lambda *_args: {"min_date": first, "max_date": second},
            "get_latest_snapshot_with_id": lambda *_args: latest,
            "get_daily_snapshot_rows": lambda *_args: daily_rows,
            "get_positions_for_snapshot": lambda *_args: [],
            "get_asset_alias_rows": lambda *_args: [],
            "build_asset_alias_lookup": lambda _rows: ({}, {}),
            "get_dataset_operations": lambda *_args, **_kwargs: operations,
            "get_income_events_for_period": lambda *_args: [],
            "compute_twr_timeseries": lambda *_args: None,
            "rebase_twr_to_period": lambda *_args: {},
            "classify_operation_group": lambda value: "deposit" if value == "OPERATION_TYPE_INPUT" else "other",
            "is_income_event_backed_tax_operation": lambda _value: False,
            "build_logical_asset_id": lambda **_kwargs: None,
            "build_reconciliation_by_asset_type": lambda *_args: ([], Decimal("0"), Decimal("0")),
            "has_mojibake": lambda _value: False,
            "normalize_decimal": lambda value: Decimal(str(value or 0)),
            "to_local_market_date": lambda value: value.date(),
            "to_iso_datetime": lambda value: value.isoformat() if value is not None else None,
            "local_reporting_bounds_utc_naive": lambda start, end: (
                datetime.combine(start, time.min),
                datetime.combine(end, time.min),
            ),
        }
        namespace["sum_decimal_values_for_snapshot_interval"] = load_interval_sum(namespace)
        namespace["normalize_operation_currency"] = load_service_function(
            "normalize_operation_currency", namespace
        )
        namespace["add_operation_cashflow_by_currency_day"] = load_service_function(
            "add_operation_cashflow_by_currency_day", namespace
        )
        namespace["build_operation_cashflows_for_snapshot_interval"] = load_service_function(
            "build_operation_cashflows_for_snapshot_interval", namespace
        )
        build_export = load_build_dataset_export(namespace)

        return build_export(object())

    def test_gap_day_deposit_is_neutralized_in_daily_and_fallback_pnl(self):
        dataset, exported_daily, *_rest = self._build_export([("RUB", "50")])

        self.assertEqual(exported_daily[-1]["deposits"], Decimal("50"))
        self.assertEqual(exported_daily[-1]["day_pnl"], Decimal("0"))
        self.assertEqual(dataset["summary"]["period_external_cashflow"], Decimal("50"))
        self.assertEqual(dataset["summary"]["period_pnl_abs"], Decimal("0"))

    def test_mixed_currency_gap_deposits_stay_separate(self):
        dataset, exported_daily, *_rest = self._build_export(
            [("RUB", "50"), ("usd", "10"), (None, "3")]
        )

        result = exported_daily[-1]
        self.assertEqual(result["deposits"], Decimal("50"))
        self.assertEqual(result["day_pnl"], Decimal("0"))
        self.assertEqual(dataset["summary"]["period_external_cashflow"], Decimal("50"))
        self.assertEqual(dataset["summary"]["period_pnl_abs"], Decimal("0"))
        self.assertEqual(
            result["operation_cashflows_by_currency"],
            [
                {
                    "currency": "RUB",
                    "deposits": Decimal("50"),
                    "withdrawals": Decimal("0"),
                    "iis_tax_deduction_income": Decimal("0"),
                    "commissions": Decimal("0"),
                    "operation_taxes": Decimal("0"),
                    "operation_tax_refunds": Decimal("0"),
                    "net_external_flow": Decimal("50"),
                },
                {
                    "currency": "UNKNOWN",
                    "deposits": Decimal("3"),
                    "withdrawals": Decimal("0"),
                    "iis_tax_deduction_income": Decimal("0"),
                    "commissions": Decimal("0"),
                    "operation_taxes": Decimal("0"),
                    "operation_tax_refunds": Decimal("0"),
                    "net_external_flow": Decimal("3"),
                },
                {
                    "currency": "USD",
                    "deposits": Decimal("10"),
                    "withdrawals": Decimal("0"),
                    "iis_tax_deduction_income": Decimal("0"),
                    "commissions": Decimal("0"),
                    "operation_taxes": Decimal("0"),
                    "operation_tax_refunds": Decimal("0"),
                    "net_external_flow": Decimal("10"),
                },
            ],
        )
        self.assertEqual(result["unsupported_operation_currencies"], ["UNKNOWN", "USD"])

    def test_gap_day_usd_only_deposit_keeps_rub_performance(self):
        dataset, exported_daily, *_rest = self._build_export([("USD", "50")])

        result = exported_daily[-1]
        self.assertEqual(result["deposits"], Decimal("0"))
        self.assertEqual(result["net_external_flow"], Decimal("0"))
        self.assertEqual(result["day_pnl"], Decimal("50"))
        self.assertEqual(dataset["summary"]["period_external_cashflow"], Decimal("0"))
        self.assertEqual(dataset["summary"]["period_pnl_abs"], Decimal("50"))
        self.assertEqual(result["unsupported_operation_currencies"], ["USD"])


if __name__ == "__main__":
    unittest.main()
