import ast
import unittest
from copy import deepcopy
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVICES_FILE = PROJECT_ROOT / "src" / "bot" / "services.py"


def load_function(name: str):
    module_ast = ast.parse(SERVICES_FILE.read_text(encoding="utf-8"), filename=str(SERVICES_FILE))
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            copied = deepcopy(node)
            copied.returns = None
            for arg in (*copied.args.args, *copied.args.kwonlyargs):
                arg.annotation = None
            namespace = {"date": date}
            exec(compile(ast.Module(body=[copied], type_ignores=[]), str(SERVICES_FILE), "exec"), namespace)
            return namespace[name]
    return None


class PeriodTWRRebaseTests(unittest.TestCase):
    def test_pre_period_gain_and_flat_selected_month_rebases_to_zero(self):
        rebase = load_function("rebase_twr_to_period")
        self.assertIsNotNone(rebase, "period TWR rebase helper is missing")

        result = rebase(
            [date(2026, 3, 31), date(2026, 4, 1), date(2026, 4, 30)],
            [0.20, 0.20, 0.20],
            date(2026, 4, 1),
            date(2026, 5, 1),
        )

        self.assertAlmostEqual(result[date(2026, 4, 30)], 0.0, places=8)

    def test_period_without_pre_period_snapshot_uses_first_point_as_baseline(self):
        rebase = load_function("rebase_twr_to_period")
        self.assertIsNotNone(rebase, "period TWR rebase helper is missing")

        result = rebase(
            [date(2026, 4, 5), date(2026, 4, 30)],
            [0.20, 0.32],
            date(2026, 4, 1),
            date(2026, 5, 1),
        )

        self.assertAlmostEqual(result[date(2026, 4, 5)], 0.0, places=8)
        self.assertAlmostEqual(result[date(2026, 4, 30)], 0.10, places=8)


if __name__ == "__main__":
    unittest.main()
