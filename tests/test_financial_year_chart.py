import ast
import unittest
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHARTS_FILE = PROJECT_ROOT / "src" / "bot" / "charts.py"


def local_bounds_utc_naive(start: date, end: date, zone: ZoneInfo):
    return (
        datetime.combine(start, time.min, tzinfo=zone)
        .astimezone(timezone.utc)
        .replace(tzinfo=None),
        datetime.combine(end, time.min, tzinfo=zone)
        .astimezone(timezone.utc)
        .replace(tzinfo=None),
    )


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
    def _render_deltas(
        self,
        portfolio_values,
        monthly_external_flows,
        monthly_deposits=None,
        *,
        first_snapshot_date=None,
        pre_period_value=None,
        external_flow_after_first_snapshot=0,
    ):
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
            "local_reporting_bounds_utc_naive": (
                lambda start, end: local_bounds_utc_naive(start, end, ZoneInfo("UTC"))
            ),
            "db_session": fake_db_session,
            "resolve_reporting_account_id": lambda _session: "acc",
            "get_monthly_portfolio_values": lambda *_args: portfolio_rows,
            "get_monthly_deposits": lambda *_args: deposit_rows,
            "get_monthly_net_external_flows": lambda *_args: flow_rows,
            "get_first_snapshot_in_period": lambda *_args: {
                "snapshot_date": first_snapshot_date or months[0],
                "total_value": portfolio_values[0],
            },
            "get_last_snapshot_before_date": lambda *_args: (
                {"snapshot_date": months[0] - timedelta(days=1), "total_value": pre_period_value}
                if pre_period_value is not None
                else None
            ),
            "get_deposits_sum_for_period": lambda *_args: 0,
            "get_net_external_flow_for_period": lambda *_args: external_flow_after_first_snapshot,
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

    def test_first_snapshot_on_period_start_is_baseline_and_does_not_reapply_its_deposit(self):
        deltas = self._render_deltas(
            [100],
            {date(2026, 1, 1): 100},
            first_snapshot_date=date(2026, 1, 1),
            pre_period_value=None,
            external_flow_after_first_snapshot=0,
        )

        self.assertEqual(deltas, [0])

    def test_pre_period_baseline_includes_period_start_flow(self):
        deltas = self._render_deltas(
            [100],
            {date(2026, 1, 1): 100},
            first_snapshot_date=date(2026, 1, 1),
            pre_period_value=0,
        )

        self.assertEqual(deltas, [0])


class YearChartTimezoneBoundaryTests(unittest.TestCase):
    def _namespace(self, **overrides):
        plot = FakePlot()

        @contextmanager
        def fake_db_session():
            yield object()

        zone = ZoneInfo("Europe/Moscow")
        namespace = {
            "date": date,
            "datetime": datetime,
            "time": time,
            "timedelta": timedelta,
            "TZ": zone,
            "local_reporting_bounds_utc_naive": (
                lambda start, end: local_bounds_utc_naive(start, end, zone)
            ),
            "db_session": fake_db_session,
            "resolve_reporting_account_id": lambda _session: "acc",
            "get_monthly_portfolio_values": lambda *_args: [],
            "get_monthly_deposits": lambda *_args: [],
            "get_monthly_iis_tax_deductions": lambda *_args: [],
            "get_monthly_net_external_flows": lambda *_args: [],
            "get_first_snapshot_in_period": lambda *_args: None,
            "get_last_snapshot_before_date": lambda *_args: None,
            "get_net_external_flow_for_period": lambda *_args: 0,
            "plt": plot,
            "set_chart_header": lambda *_args, **_kwargs: None,
            "apply_chart_style": lambda *_args, **_kwargs: None,
            "rub_axis_formatter": None,
            "CHART_COLORS": {
                "positive": "green",
                "negative": "red",
                "neutral": "gray",
                "spine": "line",
            },
            "format_month_short_label": lambda value: str(value.month),
            "fmt_compact_rub": lambda value, **_kwargs: str(value),
            "build_month_tick_labels": lambda values: [str(value.month) for value in values],
            "annotate_bar_values": lambda *_args, **_kwargs: None,
            "REPORTING_ACCOUNT_UNAVAILABLE_TEXT": "unavailable",
        }
        namespace.update(overrides)
        return namespace, plot

    def test_year_chart_queries_use_local_civil_utc_bounds(self):
        calls = []

        def capture_portfolio(_session, _account_id, start, end, _is_ytd):
            calls.append((start, end))
            return []

        namespace, _plot = self._namespace(
            get_monthly_portfolio_values=capture_portfolio,
            get_monthly_deposits=lambda _session, _account_id, start, end: calls.append((start, end)) or [],
            get_monthly_iis_tax_deductions=(
                lambda _session, _account_id, start, end: calls.append((start, end)) or []
            ),
        )
        build_chart = load_function("build_year_chart", namespace)

        build_chart("unused.png", 2026, date(2026, 1, 2))

        expected = (datetime(2025, 12, 31, 21), datetime(2026, 1, 1, 21))
        self.assertEqual(calls, [expected, expected, expected])

    def test_year_monthly_delta_includes_local_jan1_deposit_stored_previous_utc_day(self):
        operation_at = datetime(2025, 12, 31, 21, 30)

        def monthly_flows(_session, _account_id, start, end):
            if start <= operation_at < end:
                return [{"month_start": date(2026, 1, 1), "amount": 100}]
            return []

        namespace, plot = self._namespace(
            get_monthly_portfolio_values=lambda *_args: [
                {"month_start": date(2026, 1, 1), "total_value": 100}
            ],
            get_monthly_net_external_flows=monthly_flows,
            get_first_snapshot_in_period=lambda *_args: {
                "snapshot_date": date(2026, 1, 1),
                "total_value": 100,
            },
            get_last_snapshot_before_date=lambda *_args: {
                "snapshot_date": date(2025, 12, 31),
                "total_value": 0,
            },
        )
        build_chart = load_function("build_year_monthly_delta_chart", namespace)

        build_chart("unused.png", 2026, date(2026, 1, 2))

        self.assertEqual(plot.axes.bar_values, [0])

    def test_first_snapshot_fallback_uses_local_bounds_but_date_snapshot_helpers(self):
        snapshot_calls = []
        flow_calls = []

        def first_snapshot(_session, _account_id, start, end):
            snapshot_calls.append((start, end))
            return {"snapshot_date": date(2026, 1, 1), "total_value": 100}

        namespace, _plot = self._namespace(
            get_monthly_portfolio_values=lambda *_args: [
                {"month_start": date(2026, 1, 1), "total_value": 100}
            ],
            get_first_snapshot_in_period=first_snapshot,
            get_net_external_flow_for_period=(
                lambda _session, _account_id, start, end: flow_calls.append((start, end)) or 0
            ),
        )
        build_chart = load_function("build_year_monthly_delta_chart", namespace)

        build_chart("unused.png", 2026, date(2026, 1, 2))

        self.assertEqual(snapshot_calls, [(date(2026, 1, 1), date(2026, 2, 1))])
        self.assertEqual(
            flow_calls,
            [(datetime(2026, 1, 1, 21), datetime(2026, 1, 31, 21))],
        )

if __name__ == "__main__":
    unittest.main()
