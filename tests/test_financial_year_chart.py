import ast
import unittest
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHARTS_FILE = PROJECT_ROOT / "src" / "bot" / "charts.py"


def load_function(name: str, namespace: dict):
    module_ast = ast.parse(CHARTS_FILE.read_text(encoding="utf-8"), filename=str(CHARTS_FILE))
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            copied = deepcopy(node)
            copied.returns = None
            for arg in (*copied.args.args, *copied.args.kwonlyargs):
                arg.annotation = None
            exec(compile(ast.Module(body=[copied], type_ignores=[]), str(CHARTS_FILE), "exec"), namespace)
            return namespace[name]
    raise AssertionError(f"function {name} not found")


class FakeFigure:
    def tight_layout(self, *_args, **_kwargs):
        return None

    def savefig(self, *_args, **_kwargs):
        return None

    def get_facecolor(self):
        return "white"


class FakeAxes:
    def __init__(self):
        self.bar_values = None

    def bar(self, _x, values, **_kwargs):
        self.bar_values = list(values)

    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


class FakePlot:
    def __init__(self):
        self.axes = FakeAxes()

    def subplots(self, *_args, **_kwargs):
        return FakeFigure(), self.axes

    def close(self, *_args, **_kwargs):
        return None


class YearMonthlyDeltaContractTests(unittest.TestCase):
    def _render_deltas(self, portfolio_values, monthly_external_flows, monthly_deposits=None):
        months = [date(2026, month, 1) for month in range(1, len(portfolio_values) + 1)]
        portfolio_rows = [
            {"month_start": month, "total_value": value}
            for month, value in zip(months, portfolio_values)
        ]
        flow_rows = [
            {"month_start": month, "amount": monthly_external_flows.get(month, 0)}
            for month in months
            if month in monthly_external_flows
        ]
        deposit_rows = [
            {"month_start": month, "amount": (monthly_deposits or {}).get(month, 0)}
            for month in months
            if month in (monthly_deposits or {})
        ]
        plot = FakePlot()

        @contextmanager
        def fake_db_session():
            yield object()

        namespace = {
            "date": date,
            "datetime": datetime,
            "time": time,
            "timedelta": timedelta,
            "TZ": timezone.utc,
            "db_session": fake_db_session,
            "resolve_reporting_account_id": lambda _session: "acc",
            "get_monthly_portfolio_values": lambda *_args: portfolio_rows,
            "get_monthly_deposits": lambda *_args: deposit_rows,
            "get_monthly_net_external_flows": lambda *_args: flow_rows,
            "get_first_snapshot_in_period": lambda *_args: {
                "snapshot_date": months[0],
                "total_value": portfolio_values[0],
            },
            "get_last_snapshot_before_date": lambda *_args: None,
            "get_deposits_sum_for_period": lambda *_args: 0,
            "get_net_external_flow_for_period": lambda *_args: monthly_external_flows.get(months[0], 0),
            "plt": plot,
            "set_chart_header": lambda *_args, **_kwargs: None,
            "apply_chart_style": lambda *_args, **_kwargs: None,
            "rub_axis_formatter": None,
            "CHART_COLORS": {"positive": "green", "negative": "red", "neutral": "gray", "spine": "line"},
            "format_month_short_label": lambda value: str(value.month),
            "fmt_compact_rub": lambda value, **_kwargs: str(value),
            "build_month_tick_labels": lambda values: [str(value.month) for value in values],
            "annotate_bar_values": lambda *_args, **_kwargs: None,
            "REPORTING_ACCOUNT_UNAVAILABLE_TEXT": "unavailable",
        }
        build_chart = load_function("build_year_monthly_delta_chart", namespace)
        build_chart("unused.png", 2026, date(2027, 1, 1))
        return plot.axes.bar_values

    def test_withdrawal_only_month_has_zero_market_delta(self):
        deltas = self._render_deltas(
            [100, 50],
            {date(2026, 2, 1): -50},
        )

        self.assertEqual(deltas, [0, 0])

    def test_mixed_deposit_and_withdrawal_month_uses_net_external_flow(self):
        deltas = self._render_deltas(
            [100, 120],
            {date(2026, 2, 1): 20},
            {date(2026, 2, 1): 30},
        )

        self.assertEqual(deltas, [0, 0])


if __name__ == "__main__":
    unittest.main()
