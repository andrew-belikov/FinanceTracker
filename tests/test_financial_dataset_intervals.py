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


class DatasetSnapshotIntervalTests(unittest.TestCase):
    def test_gap_day_deposit_is_neutralized_in_daily_and_fallback_pnl(self):
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
                "total_value": Decimal("150"),
                "expected_yield": Decimal("0"),
                "expected_yield_pct": Decimal("0"),
            },
        ]
        operation = {
            "operation_id": "synthetic-deposit",
            "date": datetime(2026, 4, 2, 10, tzinfo=timezone.utc),
            "operation_type": "OPERATION_TYPE_INPUT",
            "cashflow_category": None,
            "state": "OPERATION_STATE_EXECUTED",
            "instrument_uid": None,
            "asset_uid": None,
            "figi": None,
            "name": "Synthetic deposit",
            "amount": Decimal("50"),
            "currency": "RUB",
            "price": Decimal("0"),
            "quantity": Decimal("0"),
            "commission": Decimal("0"),
            "yield": Decimal("0"),
            "description": "synthetic",
            "source": "test",
        }
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
            "get_dataset_operations": lambda *_args, **_kwargs: [operation],
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
        }
        namespace["sum_decimal_values_for_snapshot_interval"] = load_interval_sum(namespace)
        build_export = load_build_dataset_export(namespace)

        dataset, exported_daily, *_rest = build_export(object())

        self.assertEqual(exported_daily[-1]["deposits"], Decimal("50"))
        self.assertEqual(exported_daily[-1]["day_pnl"], Decimal("0"))
        self.assertEqual(dataset["summary"]["period_external_cashflow"], Decimal("50"))
        self.assertEqual(dataset["summary"]["period_pnl_abs"], Decimal("0"))


if __name__ == "__main__":
    unittest.main()
