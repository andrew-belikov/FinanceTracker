import ast
import unittest
from copy import deepcopy
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORT_PAYLOAD_FILE = PROJECT_ROOT / "src" / "bot" / "report_payload.py"
REPORT_RENDER_FILE = PROJECT_ROOT / "src" / "bot" / "report_render.py"


def load_function(path, name, namespace):
    module_ast = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    node = next(node for node in module_ast.body if isinstance(node, ast.FunctionDef) and node.name == name)
    copied = deepcopy(node)
    copied.returns = None
    for arg in (*copied.args.args, *copied.args.kwonlyargs):
        arg.annotation = None
    exec(compile(ast.Module(body=[copied], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


def display_rub(value, *, precision=0):
    if value is None:
        return "—"
    return f"{Decimal(str(value)):.{precision}f} ₽"


def build_payload():
    return {
        "summary_metrics": {
            "current_value": Decimal("100"),
            "period_pnl_abs": Decimal("0"),
            "period_pnl_pct": Decimal("0"),
            "period_twr_pct": Decimal("0"),
            "net_external_flow": Decimal("0"),
            "deposits": Decimal("0"),
            "withdrawals": Decimal("0"),
            "income_net": Decimal("0"),
            "iis_tax_deduction_income": Decimal("0"),
            "total_income_net": Decimal("0"),
            "commissions": Decimal("0"),
            "taxes": Decimal("12.10"),
            "tax_refunds": Decimal("18"),
            "top_holding_name": None,
            "best_day_date": None,
            "best_day_pnl": None,
            "worst_day_date": None,
            "worst_day_pnl": None,
            "reconciliation_gap_abs": Decimal("0"),
            "top_holding_weight_pct": Decimal("0"),
        },
        "position_flow_groups": {"new": [], "closed": []},
        "instrument_movers": {"top_growth": [], "top_drawdown": []},
        "data_quality": {
            "positions_missing_label_count": 0,
            "mojibake_detected_count": 0,
            "has_full_history_from_zero": True,
            "has_rebalance_targets": True,
        },
        "operations_top": [],
    }


class TaxRefundReportingTests(unittest.TestCase):
    def test_ai_facts_keep_refund_separate_from_tax_expense(self):
        namespace = {
            "Any": object,
            "_display_rub": display_rub,
            "_to_decimal": lambda value: Decimal(str(value or 0)),
            "fmt_decimal_rub": lambda value, precision=0: display_rub(value, precision=precision),
            "fmt_pct": lambda value, precision=2: f"{value:.{precision}f}%",
            "_format_display_date": lambda value: value,
            "DEFAULT_AI_TOP_LIMIT": 5,
        }
        overview = load_function(REPORT_PAYLOAD_FILE, "_build_overview_facts", namespace)(build_payload())
        cashflow = load_function(REPORT_PAYLOAD_FILE, "_build_cashflow_facts", namespace)(build_payload())

        self.assertEqual(overview["taxes"], "12.10 ₽")
        self.assertEqual(overview["tax_refunds"], "18.00 ₽")
        self.assertEqual(cashflow["taxes"], "12.10 ₽")
        self.assertEqual(cashflow["tax_refunds"], "18.00 ₽")
        self.assertTrue(any("Возврат налога" in item for item in overview["highlights"]))

    def test_deterministic_pdf_notes_keep_refund_separate_from_taxes(self):
        namespace = {
            "Any": object,
            "Decimal": Decimal,
            "_display_rub": display_rub,
            "_display_pct": lambda value, precision=2: f"{Decimal(str(value or 0)):.{precision}f}%",
            "_display_date": lambda value: value or "—",
            "_to_decimal": lambda value: Decimal(str(value or 0)),
            "_report_title_default": lambda _payload: "Report",
        }
        narrative = load_function(
            REPORT_RENDER_FILE,
            "build_deterministic_monthly_narrative",
            namespace,
        )(build_payload())

        tax_note = next(item for item in narrative["cashflow_notes"] if "налоги:" in item)
        refund_note = next(item for item in narrative["cashflow_notes"] if "Возврат налога:" in item)
        self.assertIn("12.10 ₽", tax_note)
        self.assertNotIn("18.00 ₽", tax_note)
        self.assertIn("18.00 ₽", refund_note)


if __name__ == "__main__":
    unittest.main()
