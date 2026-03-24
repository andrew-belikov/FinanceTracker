import ast
import unittest
from pathlib import Path
from copy import deepcopy


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BOT_FILE = PROJECT_ROOT / "src" / "bot" / "bot.py"


def load_symbols():
    module_ast = ast.parse(BOT_FILE.read_text(encoding="utf-8"), filename=str(BOT_FILE))
    wanted_assignments = {
        "DEPOSIT_OPERATION_TYPES",
        "WITHDRAWAL_OPERATION_TYPES",
        "BUY_OPERATION_TYPES",
        "SELL_OPERATION_TYPES",
        "COMMISSION_OPERATION_TYPES",
        "TAX_OPERATION_TYPES",
        "INCOME_EVENT_TAX_OPERATION_TYPES",
        "INCOME_TAX_OPERATION_TYPES",
    }
    wanted_functions = {
        "classify_operation_group",
        "decimal_to_str",
        "build_logical_asset_id",
        "is_income_event_backed_tax_operation",
    }

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

    isolated_module = ast.Module(body=selected_nodes, type_ignores=[])
    code = compile(isolated_module, filename=str(BOT_FILE), mode="exec")
    namespace = {}
    exec("from decimal import Decimal\n", namespace)
    exec(code, namespace)
    return namespace


SYMBOLS = load_symbols()
classify_operation_group = SYMBOLS["classify_operation_group"]
decimal_to_str = SYMBOLS["decimal_to_str"]
build_logical_asset_id = SYMBOLS["build_logical_asset_id"]
is_income_event_backed_tax_operation = SYMBOLS["is_income_event_backed_tax_operation"]


class DatasetHelpersTests(unittest.TestCase):
    def test_classify_operation_group_maps_core_types(self):
        self.assertEqual(classify_operation_group("OPERATION_TYPE_INPUT"), "deposit")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_OUTPUT"), "withdrawal")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_BUY"), "buy")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_SELL"), "sell")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_BROKER_FEE"), "commission")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_TAX"), "income_tax")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_DIVIDEND_TAX"), "income_tax")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_COUPON_TAX"), "income_tax")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_DIVIDEND"), "dividend")
        self.assertEqual(classify_operation_group("OPERATION_TYPE_COUPON"), "coupon")

    def test_classify_operation_group_falls_back_to_other(self):
        self.assertEqual(classify_operation_group("OPERATION_TYPE_UNKNOWN"), "other")
        self.assertEqual(classify_operation_group(None), "other")

    def test_decimal_to_str_keeps_plain_numeric_format(self):
        self.assertEqual(decimal_to_str(12), "12")
        self.assertEqual(decimal_to_str(12.5), "12.5")

    def test_build_logical_asset_id_prefers_asset_uid(self):
        self.assertEqual(
            build_logical_asset_id(asset_uid="asset-1", instrument_uid="inst-1", figi="figi-1"),
            "asset-1",
        )
        self.assertEqual(
            build_logical_asset_id(asset_uid=None, instrument_uid="inst-1", figi="figi-1"),
            "inst-1",
        )
        self.assertEqual(
            build_logical_asset_id(asset_uid=None, instrument_uid=None, figi="figi-1"),
            "figi-1",
        )

    def test_income_event_backed_tax_operation_detects_dividend_and_coupon_tax(self):
        self.assertTrue(is_income_event_backed_tax_operation("OPERATION_TYPE_DIVIDEND_TAX"))
        self.assertTrue(is_income_event_backed_tax_operation("OPERATION_TYPE_COUPON_TAX"))
        self.assertFalse(is_income_event_backed_tax_operation("OPERATION_TYPE_TAX"))


if __name__ == "__main__":
    unittest.main()
