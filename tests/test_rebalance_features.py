import ast
import unittest
from copy import deepcopy
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BOT_FILE = PROJECT_ROOT / "src" / "bot" / "bot.py"
RUNTIME_FILE = PROJECT_ROOT / "src" / "bot" / "runtime.py"

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

    loaded_namespace = {} if namespace is None else dict(namespace)
    isolated_module = ast.Module(body=selected_nodes, type_ignores=[])
    code = compile(isolated_module, filename=str(file_path), mode="exec")
    exec(code, loaded_namespace)
    return loaded_namespace


def load_symbols():
    runtime_symbols = load_selected_symbols(
        RUNTIME_FILE,
        {"MONTHS_RU_GENITIVE"},
        {"normalize_decimal"},
        namespace={
            "Decimal": Decimal,
        },
    )
    return load_selected_symbols(
        BOT_FILE,
        {
            "REBALANCE_ASSET_CLASSES",
            "REBALANCE_TARGET_ALIASES",
            "REBALANCE_CLASS_LABELS",
            "REBALANCE_GROUP_TO_CLASS",
            "REBALANCE_TOLERANCE_PCT",
        },
        {
            "_instrument_type_to_group",
            "quantize_ruble_amount",
            "parse_decimal_input",
            "parse_rebalance_targets_args",
            "aggregate_rebalance_values_by_class",
            "compute_rebalance_plan",
            "compute_invest_plan",
            "format_rebalance_weight",
            "format_human_date_ru",
            "_build_rebalance_diff_lines",
            "build_help_text",
        },
        namespace={
            **runtime_symbols,
            "Decimal": Decimal,
            "InvalidOperation": InvalidOperation,
            "ROUND_HALF_UP": ROUND_HALF_UP,
        },
    )


SYMBOLS = load_symbols()
parse_rebalance_targets_args = SYMBOLS["parse_rebalance_targets_args"]
aggregate_rebalance_values_by_class = SYMBOLS["aggregate_rebalance_values_by_class"]
compute_rebalance_plan = SYMBOLS["compute_rebalance_plan"]
compute_invest_plan = SYMBOLS["compute_invest_plan"]
format_human_date_ru = SYMBOLS["format_human_date_ru"]
_build_rebalance_diff_lines = SYMBOLS["_build_rebalance_diff_lines"]
build_help_text = SYMBOLS["build_help_text"]


class RebalanceTargetsParsingTests(unittest.TestCase):
    def test_parse_targets_supports_cash_alias_and_missing_class_defaults_to_zero(self):
        result = parse_rebalance_targets_args(["stocks=50", "bonds=30", "cash=20"])

        self.assertEqual(result["stocks"], Decimal("50"))
        self.assertEqual(result["bonds"], Decimal("30"))
        self.assertEqual(result["currency"], Decimal("20"))
        self.assertEqual(result["etf"], Decimal("0"))

    def test_parse_targets_requires_sum_exactly_100(self):
        with self.assertRaisesRegex(ValueError, "ровно 100"):
            parse_rebalance_targets_args(["stocks=50", "bonds=20", "cash=20"])

    def test_parse_targets_rejects_duplicate_alias_after_normalization(self):
        with self.assertRaisesRegex(ValueError, "указан несколько раз"):
            parse_rebalance_targets_args(["stocks=60", "cash=20", "currency=20"])


class RebalanceMathTests(unittest.TestCase):
    def test_aggregate_values_splits_supported_and_out_of_model_groups(self):
        class_values, other_groups = aggregate_rebalance_values_by_class(
            [
                {"instrument_type": "share", "position_value": Decimal("60000")},
                {"instrument_type": "bond", "position_value": Decimal("40000")},
                {"instrument_type": "futures", "position_value": Decimal("5000")},
            ]
        )

        self.assertEqual(class_values["stocks"], Decimal("60000"))
        self.assertEqual(class_values["bonds"], Decimal("40000"))
        self.assertEqual(other_groups["Фьючерсы"], Decimal("5000"))

    def test_rebalance_plan_marks_small_drift_as_normal_but_keeps_non_zero_delta_value(self):
        plan = compute_rebalance_plan(
            {
                "stocks": Decimal("52000"),
                "bonds": Decimal("48000"),
                "etf": Decimal("0"),
                "currency": Decimal("0"),
            },
            {
                "stocks": Decimal("50"),
                "bonds": Decimal("50"),
                "etf": Decimal("0"),
                "currency": Decimal("0"),
            },
        )

        rows_by_class = {row["asset_class"]: row for row in plan["rows"]}
        self.assertEqual(rows_by_class["stocks"]["status"], "в норме")
        self.assertEqual(rows_by_class["bonds"]["status"], "в норме")
        self.assertEqual(rows_by_class["stocks"]["delta_value"], Decimal("-2000"))
        self.assertEqual(rows_by_class["bonds"]["delta_value"], Decimal("2000"))

    def test_invest_plan_distributes_empty_portfolio_by_targets(self):
        plan = compute_invest_plan(
            {
                "stocks": Decimal("0"),
                "bonds": Decimal("0"),
                "etf": Decimal("0"),
                "currency": Decimal("0"),
            },
            {
                "stocks": Decimal("50"),
                "bonds": Decimal("30"),
                "etf": Decimal("0"),
                "currency": Decimal("20"),
            },
            Decimal("30000"),
        )

        self.assertEqual(plan["allocations"]["stocks"], Decimal("15000"))
        self.assertEqual(plan["allocations"]["bonds"], Decimal("9000"))
        self.assertEqual(plan["allocations"]["currency"], Decimal("6000"))
        self.assertEqual(sum(plan["allocations"].values()), Decimal("30000"))

    def test_invest_plan_sends_new_money_only_to_underweight_classes(self):
        plan = compute_invest_plan(
            {
                "stocks": Decimal("80000"),
                "bonds": Decimal("20000"),
                "etf": Decimal("0"),
                "currency": Decimal("0"),
            },
            {
                "stocks": Decimal("50"),
                "bonds": Decimal("50"),
                "etf": Decimal("0"),
                "currency": Decimal("0"),
            },
            Decimal("10000"),
        )

        self.assertEqual(plan["allocations"]["stocks"], Decimal("0"))
        self.assertEqual(plan["allocations"]["bonds"], Decimal("10000"))
        self.assertEqual(sum(plan["allocations"].values()), Decimal("10000"))

    def test_invest_plan_preserves_total_after_rounding(self):
        plan = compute_invest_plan(
            {
                "stocks": Decimal("10000"),
                "bonds": Decimal("10000"),
                "etf": Decimal("10000"),
                "currency": Decimal("0"),
            },
            {
                "stocks": Decimal("33"),
                "bonds": Decimal("33"),
                "etf": Decimal("34"),
                "currency": Decimal("0"),
            },
            Decimal("10001"),
        )

        self.assertEqual(sum(plan["allocations"].values()), Decimal("10001"))

    def test_diff_lines_render_fact_then_plan_with_status_emoji(self):
        lines = _build_rebalance_diff_lines(
            [
                {
                    "label": "Акции",
                    "current_pct": Decimal("27.01"),
                    "target_pct": Decimal("30"),
                    "status": "в норме",
                },
                {
                    "label": "ETF",
                    "current_pct": Decimal("66.2"),
                    "target_pct": Decimal("60"),
                    "status": "вне нормы",
                },
            ]
        )

        self.assertEqual(lines[0], "- ✅ Акции: 27,0% / 30,0%")
        self.assertEqual(lines[1], "- ⚠️ ETF: 66,2% / 60,0%")

    def test_human_date_uses_russian_month_genitive(self):
        self.assertEqual(format_human_date_ru(date(2026, 3, 24)), "24 марта 2026")


class HelpTextTests(unittest.TestCase):
    def test_help_text_mentions_new_commands_and_auto_deposit_hint(self):
        text = build_help_text()

        self.assertIn("/targets", text)
        self.assertIn("/rebalance", text)
        self.assertIn("/invest <sum>", text)
        self.assertIn("новое пополнение счёта", text)


if __name__ == "__main__":
    unittest.main()
