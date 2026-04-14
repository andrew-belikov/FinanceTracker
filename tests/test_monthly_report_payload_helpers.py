import ast
import unittest
from copy import deepcopy
from datetime import date
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))


SERVICES_FILE = PROJECT_ROOT / "src" / "bot" / "services.py"
RUNTIME_FILE = PROJECT_ROOT / "src" / "bot" / "runtime.py"


def load_selected_symbols(file_path: Path, wanted_functions: set[str], namespace=None):
    module_ast = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    selected_nodes = []
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted_functions:
            copied = deepcopy(node)
            copied.returns = None
            for arg in copied.args.args:
                arg.annotation = None
            for arg in copied.args.kwonlyargs:
                arg.annotation = None
            selected_nodes.append(copied)

    loaded_namespace = {} if namespace is None else dict(namespace)
    isolated_module = ast.Module(body=selected_nodes, type_ignores=[])
    code = compile(isolated_module, filename=str(file_path), mode="exec")
    exec(code, loaded_namespace)
    return loaded_namespace


RUNTIME_SYMBOLS = load_selected_symbols(
    RUNTIME_FILE,
    {
        "normalize_decimal",
        "decimal_to_str",
    },
    namespace={
        "Decimal": __import__("decimal").Decimal,
    },
)

SYMBOLS = load_selected_symbols(
    SERVICES_FILE,
    {
        "build_logical_asset_id",
        "_resolve_monthly_asset_identity",
        "_serialize_monthly_position_row",
        "_build_monthly_position_flow_groups",
        "_build_monthly_instrument_payload",
    },
    namespace={
        "Decimal": __import__("decimal").Decimal,
        "date": date,
        "normalize_decimal": RUNTIME_SYMBOLS["normalize_decimal"],
        "decimal_to_str": RUNTIME_SYMBOLS["decimal_to_str"],
    },
)

build_logical_asset_id = SYMBOLS["build_logical_asset_id"]
resolve_monthly_asset_identity = SYMBOLS["_resolve_monthly_asset_identity"]
serialize_monthly_position_row = SYMBOLS["_serialize_monthly_position_row"]
build_monthly_position_flow_groups = SYMBOLS["_build_monthly_position_flow_groups"]
build_monthly_instrument_payload = SYMBOLS["_build_monthly_instrument_payload"]


class MonthlyReportPayloadHelperTests(unittest.TestCase):
    def test_resolve_monthly_asset_identity_uses_alias_lookup(self):
        alias_by_instrument_uid = {
            "inst-1": {
                "asset_uid": "asset-1",
                "instrument_uid": "inst-1",
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha Asset",
            }
        }

        identity = resolve_monthly_asset_identity(
            {
                "instrument_uid": "inst-1",
                "figi": "FIGI-1",
                "ticker": "",
                "name": "",
                "asset_uid": None,
            },
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi={},
        )

        self.assertEqual(identity["logical_asset_id"], "asset-1")
        self.assertEqual(identity["ticker"], "AAA")
        self.assertEqual(identity["name"], "Alpha Asset")

    def test_build_monthly_position_flow_groups_splits_core_buckets(self):
        alias_lookup = {}
        start_positions = [
            {
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "instrument_type": "share",
                "quantity": 1,
                "position_value": 100,
            },
            {
                "figi": "FIGI-2",
                "ticker": "BBB",
                "name": "Beta",
                "instrument_uid": "inst-2",
                "asset_uid": "asset-2",
                "instrument_type": "bond",
                "quantity": 4,
                "position_value": 80,
            },
        ]
        end_positions = [
            {
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "instrument_type": "share",
                "quantity": 3,
                "position_value": 130,
            },
            {
                "figi": "FIGI-3",
                "ticker": "CCC",
                "name": "Gamma",
                "instrument_uid": "inst-3",
                "asset_uid": "asset-3",
                "instrument_type": "etf",
                "quantity": 2,
                "position_value": 200,
            },
        ]

        groups = build_monthly_position_flow_groups(
            start_positions,
            end_positions,
            alias_by_instrument_uid=alias_lookup,
            alias_by_figi=alias_lookup,
        )

        self.assertEqual(len(groups["new"]), 1)
        self.assertEqual(len(groups["closed"]), 1)
        self.assertEqual(len(groups["increased"]), 1)
        self.assertEqual(len(groups["decreased"]), 0)
        self.assertEqual(groups["new"][0]["logical_asset_id"], "asset-3")
        self.assertEqual(groups["closed"][0]["logical_asset_id"], "asset-2")
        self.assertEqual(groups["increased"][0]["delta_qty"], "2")

    def test_build_monthly_instrument_payload_groups_by_asset(self):
        eod_rows = [
            {
                "snapshot_id": 1,
                "snapshot_date": date(2026, 4, 1),
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_type": "share",
                "quantity": 1,
                "position_value": 100,
                "expected_yield": 10,
                "expected_yield_pct": 10,
                "weight_pct": 25,
            },
            {
                "snapshot_id": 2,
                "snapshot_date": date(2026, 4, 2),
                "instrument_uid": "inst-1",
                "asset_uid": "asset-1",
                "figi": "FIGI-1",
                "ticker": "AAA",
                "name": "Alpha",
                "instrument_type": "share",
                "quantity": 2,
                "position_value": 140,
                "expected_yield": 14,
                "expected_yield_pct": 10,
                "weight_pct": 30,
            },
            {
                "snapshot_id": 1,
                "snapshot_date": date(2026, 4, 1),
                "instrument_uid": "inst-2",
                "asset_uid": "asset-2",
                "figi": "FIGI-2",
                "ticker": "BBB",
                "name": "Beta",
                "instrument_type": "bond",
                "quantity": 4,
                "position_value": 80,
                "expected_yield": 5,
                "expected_yield_pct": 6,
                "weight_pct": 20,
            },
        ]

        payload_rows, movers = build_monthly_instrument_payload(
            eod_rows,
            alias_by_instrument_uid={},
            alias_by_figi={},
        )

        self.assertEqual(len(payload_rows), 2)
        self.assertEqual(payload_rows[0]["logical_asset_id"], "asset-1")
        self.assertEqual(payload_rows[0]["stats"]["eod_end_position_value"], "140")
        self.assertEqual(len(payload_rows[0]["series"]), 2)
        self.assertEqual(movers["top_growth"][0]["logical_asset_id"], "asset-1")
        self.assertEqual(movers["top_drawdown"][0]["logical_asset_id"], "asset-1")


if __name__ == "__main__":
    unittest.main()
