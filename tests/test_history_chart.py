import sys
import unittest
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from unittest import mock

from matplotlib import colors as mcolors
from matplotlib import dates as mdates
from matplotlib.collections import PathCollection


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import charts  # noqa: E402


class HistoryChartTests(unittest.TestCase):
    def test_build_history_chart_marks_peak_with_date_and_value(self):
        timeseries = [
            {"snapshot_date": date(2026, 1, 1), "total_value": 100000},
            {"snapshot_date": date(2026, 1, 8), "total_value": 120000},
            {"snapshot_date": date(2026, 1, 15), "total_value": 150000},
            {"snapshot_date": date(2026, 1, 22), "total_value": 130000},
        ]
        deposits = [
            {"d": date(2026, 1, 1), "s": 90000},
            {"d": date(2026, 1, 12), "s": 5000},
        ]
        captured: dict[str, object] = {}

        def fake_savefig(fig, *args, **kwargs):
            captured["fig"] = fig

        with (
            mock.patch.object(charts, "db_session", return_value=nullcontext(object())),
            mock.patch.object(charts, "resolve_reporting_account_id", return_value="account-1"),
            mock.patch.object(charts, "get_portfolio_timeseries", return_value=timeseries),
            mock.patch.object(charts, "get_deposits_by_date", return_value=deposits),
            mock.patch.object(charts.plt, "close", return_value=None),
            mock.patch("matplotlib.figure.Figure.savefig", new=fake_savefig),
        ):
            result = charts.build_history_chart("/tmp/history-test.png")

        self.assertEqual(result, "/tmp/history-test.png")
        figure = captured["fig"]
        ax = figure.axes[0]

        peak_annotations = [text for text in ax.texts if text.get_text() == "15 янв 2026\n150 000 ₽"]
        self.assertEqual(len(peak_annotations), 1)

        peak_x = mdates.date2num(date(2026, 1, 15))
        peak_markers = []
        for collection in ax.collections:
            if not isinstance(collection, PathCollection):
                continue
            offsets = collection.get_offsets()
            if len(offsets) != 1:
                continue
            if abs(offsets[0][0] - peak_x) < 1e-6 and abs(offsets[0][1] - 150000.0) < 1e-6:
                peak_markers.append(collection)

        self.assertTrue(peak_markers)
        expected_color = mcolors.to_rgba(charts.CHART_COLORS["peak"])
        actual_color = peak_markers[0].get_facecolors()[0]
        for actual_channel, expected_channel in zip(actual_color, expected_color):
            self.assertAlmostEqual(actual_channel, expected_channel, places=2)


if __name__ == "__main__":
    unittest.main()
