import ast
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BOT_FILE = PROJECT_ROOT / "src" / "bot" / "bot.py"


def load_compute_positions_diff_lines():
    module_ast = ast.parse(BOT_FILE.read_text(encoding="utf-8"), filename=str(BOT_FILE))
    target_node = None
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name == "compute_positions_diff_lines":
            target_node = node
            break
    if target_node is None:
        raise RuntimeError("compute_positions_diff_lines not found")

    isolated_module = ast.Module(body=[target_node], type_ignores=[])
    code = compile(isolated_module, filename=str(BOT_FILE), mode="exec")
    namespace = {}
    exec(code, namespace)
    return namespace["compute_positions_diff_lines"]


compute_positions_diff_lines = load_compute_positions_diff_lines()


class MonthPositionsDiffTests(unittest.TestCase):
    def test_groups_and_sort_order(self):
        start_positions = [
            {"figi": "figi-sber", "ticker": "SBER", "quantity": 7},
            {"figi": "figi-gazp", "ticker": "GAZP", "quantity": 8},
            {"figi": "figi-tsla", "ticker": "TSLA", "quantity": 5},
        ]
        end_positions = [
            {"figi": "figi-aapl", "ticker": "AAPL", "quantity": 10},
            {"figi": "figi-gazp", "ticker": "GAZP", "quantity": 6},
            {"figi": "figi-sber", "ticker": "SBER", "quantity": 10},
        ]

        lines = compute_positions_diff_lines(start_positions, end_positions)

        self.assertEqual(
            lines,
            [
                "+ AAPL — 10 шт (новая)",
                "- TSLA — 5 шт (закрыта)",
                "↑ SBER — +3 шт (7 → 10)",
                "↓ GAZP — -2 шт (8 → 6)",
            ],
        )

    def test_no_changes_returns_empty(self):
        positions = [{"figi": "f1", "ticker": "AAA", "quantity": 3}]
        self.assertEqual(compute_positions_diff_lines(positions, positions), [])


if __name__ == "__main__":
    unittest.main()
