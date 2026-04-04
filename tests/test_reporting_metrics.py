import ast
import os
import unittest
from copy import deepcopy
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICES_FILE = PROJECT_ROOT / "src" / "bot" / "services.py"
RUNTIME_FILE = PROJECT_ROOT / "src" / "bot" / "runtime.py"
QUERIES_FILE = PROJECT_ROOT / "src" / "bot" / "queries.py"

def load_selected_symbols(file_path: Path, wanted_assignments: set[str], wanted_functions: set[str], namespace=None):
    module_ast = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    selected_nodes = []
    for node in module_ast.body:
        if isinstance(node, ast.Assign):
            target_names = {
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            }
            if target_names & wanted_assignments:
                selected_nodes.append(node)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id in wanted_assignments:
                selected_nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            copied = deepcopy(node)
            copied.returns = None
            for arg in copied.args.args:
                arg.annotation = None
            for arg in copied.args.kwonlyargs:
                arg.annotation = None
            selected_nodes.append(copied)

    loaded_namespace = {} if namespace is None else namespace
    isolated_module = ast.Module(body=selected_nodes, type_ignores=[])
    code = compile(isolated_module, filename=str(file_path), mode="exec")
    exec(code, loaded_namespace)
    return loaded_namespace


def load_symbols():
    shared_namespace = {
        "os": os,
        "TZ": ZoneInfo("Europe/Moscow"),
    }
    exec("from datetime import date, datetime, timezone\n", shared_namespace)
    shared_namespace["text"] = lambda sql: sql
    load_selected_symbols(
        RUNTIME_FILE,
        {
            "DEPOSIT_OPERATION_TYPES",
            "WITHDRAWAL_OPERATION_TYPES",
            "TINKOFF_ACCOUNT_ID",
        },
        {
            "to_local_market_date",
        },
        namespace=shared_namespace,
    )
    load_selected_symbols(
        QUERIES_FILE,
        set(),
        {
            "normalize_reporting_account_id",
            "choose_reporting_account_id",
            "get_latest_snapshot_account_id",
            "resolve_reporting_account_id",
        },
        namespace=shared_namespace,
    )
    return load_selected_symbols(
        SERVICES_FILE,
        set(),
        {
            "build_net_external_flow_by_day",
            "compute_twr_series",
            "compute_xnpv",
            "compute_xirr",
            "project_run_rate_value",
        },
        namespace=shared_namespace,
    )


SYMBOLS = load_symbols()
normalize_reporting_account_id = SYMBOLS["normalize_reporting_account_id"]
choose_reporting_account_id = SYMBOLS["choose_reporting_account_id"]
resolve_reporting_account_id = SYMBOLS["resolve_reporting_account_id"]
build_net_external_flow_by_day = SYMBOLS["build_net_external_flow_by_day"]
compute_twr_series = SYMBOLS["compute_twr_series"]
compute_xnpv = SYMBOLS["compute_xnpv"]
compute_xirr = SYMBOLS["compute_xirr"]
project_run_rate_value = SYMBOLS["project_run_rate_value"]


class FakeResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class FakeSession:
    def __init__(self, latest_snapshot_account_id):
        self.latest_snapshot_account_id = latest_snapshot_account_id

    def execute(self, _query, _params=None):
        return FakeResult(self.latest_snapshot_account_id)


class ReportingAccountResolutionTests(unittest.TestCase):
    def test_normalize_reporting_account_id_treats_auto_as_missing(self):
        self.assertIsNone(normalize_reporting_account_id(None))
        self.assertIsNone(normalize_reporting_account_id(""))
        self.assertIsNone(normalize_reporting_account_id(" auto "))
        self.assertEqual(normalize_reporting_account_id(" 2271274706 "), "2271274706")

    def test_choose_reporting_account_id_prefers_explicit_env_value(self):
        self.assertEqual(
            choose_reporting_account_id("2271274706", "latest-from-db"),
            "2271274706",
        )

    def test_resolve_reporting_account_id_prefers_explicit_tinkoff_account_id(self):
        SYMBOLS["TINKOFF_ACCOUNT_ID"] = "2271274706"
        session = FakeSession("latest-from-db")
        self.assertEqual(resolve_reporting_account_id(session), "2271274706")

    def test_resolve_reporting_account_id_falls_back_to_latest_snapshot(self):
        SYMBOLS["TINKOFF_ACCOUNT_ID"] = "auto"
        session = FakeSession("latest-from-db")
        self.assertEqual(resolve_reporting_account_id(session), "latest-from-db")

    def test_resolve_reporting_account_id_returns_none_without_env_or_snapshot(self):
        SYMBOLS["TINKOFF_ACCOUNT_ID"] = "auto"
        session = FakeSession(None)
        self.assertIsNone(resolve_reporting_account_id(session))


class TWRComputationTests(unittest.TestCase):
    def test_compute_twr_series_neutralizes_single_deposit(self):
        rows = [
            {"snapshot_date": date(2026, 1, 1), "total_value": 100.0},
            {"snapshot_date": date(2026, 1, 2), "total_value": 120.0},
        ]

        data = compute_twr_series(rows, {date(2026, 1, 2): 20.0})
        self.assertIsNotNone(data)
        _dates, _values, twr = data
        self.assertAlmostEqual(twr[-1], 0.0, places=8)

    def test_compute_twr_series_handles_deposit_and_withdrawal_net_flow(self):
        rows = [
            {"snapshot_date": date(2026, 1, 1), "total_value": 100.0},
            {"snapshot_date": date(2026, 1, 2), "total_value": 115.0},
        ]

        data = compute_twr_series(rows, {date(2026, 1, 2): 5.0})
        self.assertIsNotNone(data)
        _dates, _values, twr = data
        self.assertAlmostEqual(twr[-1], 0.10, places=8)

    def test_build_net_external_flow_by_day_aggregates_multiple_flows_same_day(self):
        rows = [
            {
                "date": datetime(2026, 1, 2, 10, 0, 0),
                "amount": 20.0,
                "operation_type": "OPERATION_TYPE_INPUT",
            },
            {
                "date": datetime(2026, 1, 2, 12, 0, 0),
                "amount": -5.0,
                "operation_type": "OPERATION_TYPE_OUTPUT",
            },
        ]

        result = build_net_external_flow_by_day(rows)
        self.assertEqual(result[date(2026, 1, 2)], 15.0)

    def test_compute_twr_series_skips_step_when_previous_value_zero_or_current_missing(self):
        rows = [
            {"snapshot_date": date(2026, 1, 1), "total_value": 0.0},
            {"snapshot_date": date(2026, 1, 2), "total_value": 50.0},
            {"snapshot_date": date(2026, 1, 3), "total_value": None},
        ]

        data = compute_twr_series(rows, {})
        self.assertIsNotNone(data)
        _dates, _values, twr = data
        self.assertEqual(twr, [0.0, 0.0, 0.0])


class XIRRAndRunRateTests(unittest.TestCase):
    def test_compute_xirr_with_multiple_deposits_in_different_dates(self):
        cashflows = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), -100.0),
            (datetime(2025, 7, 2, tzinfo=timezone.utc), -50.0),
            (datetime(2026, 1, 1, tzinfo=timezone.utc), 162.4404424),
        ]

        result = compute_xirr(cashflows)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 0.10, delta=1e-4)

    def test_compute_xirr_handles_withdrawal_as_positive_investor_cashflow(self):
        cashflows = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), -100.0),
            (datetime(2025, 7, 2, tzinfo=timezone.utc), 20.0),
            (datetime(2026, 1, 1, tzinfo=timezone.utc), 89.0238227),
        ]

        result = compute_xirr(cashflows)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 0.10, delta=1e-4)

    def test_compute_xirr_returns_none_without_opposite_sign_cashflows(self):
        only_deposits = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), -100.0),
            (datetime(2025, 2, 1, tzinfo=timezone.utc), -50.0),
        ]
        self.assertIsNone(compute_xirr(only_deposits))

    def test_project_run_rate_value_returns_current_value_when_no_days_left(self):
        value = project_run_rate_value(433596.36, 0.178906, date(2026, 12, 31), date(2026, 12, 31))
        self.assertAlmostEqual(value, 433596.36, places=8)


if __name__ == "__main__":
    unittest.main()
