from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from matplotlib import dates as mdates
from matplotlib.backends.backend_agg import FigureCanvasAgg

from financetracker.bot.charts import build_intraday_daily_push_chart
from financetracker.bot.intraday_daily_push import build_intraday_daily_push_payload
from financetracker.bot import jobs
from financetracker.bot import intraday_daily_push_job as push_job
from financetracker.database.intraday import IntradayDay, IntradayPosition, IntradaySnapshot


def _snapshots():
    return [
        {"observed_at": datetime(2026, 9, 16, 10, 0), "total_value": Decimal("1000000"), "currency": "RUB"},
        {"observed_at": datetime(2026, 9, 16, 13, 0), "total_value": Decimal("990000"), "currency": "RUB"},
        {"observed_at": datetime(2026, 9, 16, 19, 5), "total_value": Decimal("1015000"), "currency": "RUB"},
    ]


class IntradayDailyPushTests(unittest.TestCase):
    def test_movers_use_names_and_tickers_on_separate_lines(self):
        payload = build_intraday_daily_push_payload(
            report_date=date(2026, 9, 16), snapshots=_snapshots(),
            positions=[
                {"name": "Сбербанк", "ticker": "SBER", "first_value": 100, "last_value": 200},
                {"name": "Лукойл", "ticker": "LKOH", "first_value": 100, "last_value": 150},
                {"name": "ОФЗ 26254", "ticker": "SU26254RMFS1", "first_value": 100, "last_value": 20},
                {"name": "ОФЗ 26247", "ticker": "SU26247RMFS5", "first_value": 100, "last_value": 50},
            ],
        )
        self.assertIn("📈 Лидеры роста цен:\nСбербанк (SBER) +100 ₽\nЛукойл (LKOH) +50 ₽", payload["text"])
        self.assertIn("📉 Лидеры снижения цен:\nОФЗ 26254 (SU26254RMFS1) −80 ₽\nОФЗ 26247 (SU26247RMFS5) −50 ₽", payload["text"])
        self.assertEqual(payload["negative_movers"][0]["label"], "ОФЗ 26254 (SU26254RMFS1)")

    def test_missing_or_duplicate_instrument_metadata_has_readable_fallback(self):
        cases = [
            ({"ticker": "SBER"}, "SBER"), ({"name": "ОФЗ 26254"}, "ОФЗ 26254"),
            ({"ticker": " EQMX ", "name": " EQMX "}, "EQMX"),
            ({"instrument_uid": "unknown"}, "unknown"), ({"name": " ", "ticker": " "}, "Без названия"),
        ]
        for row, label in cases:
            with self.subTest(row=row):
                payload = build_intraday_daily_push_payload(
                    report_date=date(2026, 9, 16), snapshots=_snapshots(),
                    positions=[dict(row, first_value=0, last_value=1)],
                )
                self.assertEqual(payload["positive_movers"][0]["label"], label)

    def _chart_axes(self, snapshots, *, positive_movers=None, negative_movers=None):
        figures = []
        with patch("matplotlib.figure.Figure.savefig", autospec=True, side_effect=lambda fig, *args, **kwargs: figures.append(fig)):
            build_intraday_daily_push_chart(
                "unused.png", report_date=date(2026, 9, 16), snapshots=snapshots,
                positive_movers=positive_movers or [], negative_movers=negative_movers or [],
            )
        return figures[0].axes

    def test_chart_trims_night_and_shows_local_time_and_market_open(self):
        snapshots = [
            {"observed_at": datetime(2026, 9, 16, hour, minute, tzinfo=timezone.utc), "total_value": value}
            for hour, minute, value in [(0, 0, 100), (3, 59, 100), (4, 0, 99), (7, 0, 98), (16, 0, 101)]
        ]
        payload = build_intraday_daily_push_payload(report_date=date(2026, 9, 16), snapshots=snapshots, positions=[])
        with patch("financetracker.bot.charts.TZ", ZoneInfo("Europe/Moscow")):
            ax, _ = self._chart_axes(payload["snapshots"])
        series = next(line for line in ax.lines if len(line.get_xdata()) > 2)
        self.assertEqual(list(series.get_ydata()), [100, 99, 98, 101])
        self.assertEqual(series.get_xdata()[0].hour, 6)
        self.assertEqual(ax.xaxis.get_major_formatter()(mdates.date2num(snapshots[-1]["observed_at"])), "19:00")
        vertical = next(line for line in ax.lines if list(line.get_ydata()) == [0, 1])
        self.assertEqual(vertical.get_xdata()[0], datetime(2026, 9, 16, 7, tzinfo=timezone.utc))
        self.assertLessEqual(vertical.get_alpha(), 0.4)
        self.assertEqual(payload["opening_value"], Decimal("100"))
        self.assertEqual(len(payload["snapshots"]), 5)

    def test_chart_preserves_flat_day_and_first_change(self):
        for values in ([100, 100, 100], [100, 99, 98], [100, 100, 99]):
            with self.subTest(values=values):
                snapshots = [dict(row, total_value=value) for row, value in zip(_snapshots(), values)]
                ax, _ = self._chart_axes(snapshots)
                series = next(line for line in ax.lines if line.get_linewidth() == 2.4)
                expected = values[1:] if values == [100, 100, 99] else values
                self.assertEqual(list(series.get_ydata()), expected)
                if values == [100, 100, 99]:
                    self.assertFalse(any(list(line.get_ydata()) == [0, 1] for line in ax.lines))

    def test_chart_trims_night_after_midnight_adjustment(self):
        tz = ZoneInfo("Europe/Moscow")
        snapshots = [
            {"observed_at": datetime(2026, 9, 16, hour, minute, tzinfo=tz), "total_value": value}
            for hour, minute, value in [
                (0, 0, 200000), (0, 5, 200010), (0, 10, 200020),
                (3, 0, 200020), (6, 59, 200020), (7, 0, 200100),
                (8, 0, 199900), (10, 0, 199800), (19, 0, 200200),
            ]
        ]
        payload = build_intraday_daily_push_payload(report_date=date(2026, 9, 16), snapshots=snapshots, positions=[])
        with patch("financetracker.bot.charts.TZ", tz):
            ax, _ = self._chart_axes(payload["snapshots"])
        series = next(line for line in ax.lines if line.get_linewidth() == 2.4)
        self.assertEqual(series.get_xdata()[0], snapshots[4]["observed_at"])
        self.assertEqual(list(series.get_ydata()), [200020, 200100, 199900, 199800, 200200])
        self.assertEqual(payload["opening_value"], Decimal("200000"))
        self.assertEqual(len(payload["snapshots"]), 9)
        baseline = next(line for line in ax.lines if list(line.get_ydata()) == [200000, 200000])
        self.assertEqual(baseline.get_linestyle(), "--")

    def test_chart_keeps_short_morning_and_afternoon_pauses(self):
        tz = ZoneInfo("Europe/Moscow")
        for samples in (
            [(7, 0, 200000), (7, 5, 200100), (7, 30, 200100), (8, 0, 200200)],
            [(10, 0, 200000), (11, 0, 200100), (15, 0, 200100), (16, 0, 200200)],
        ):
            with self.subTest(samples=samples):
                snapshots = [
                    {"observed_at": datetime(2026, 9, 16, hour, minute, tzinfo=tz), "total_value": value}
                    for hour, minute, value in samples
                ]
                ax, _ = self._chart_axes(snapshots)
                series = next(line for line in ax.lines if line.get_linewidth() == 2.4)
                self.assertEqual(list(series.get_ydata()), [sample[2] for sample in samples])

    def test_chart_labels_extrema_and_closing_in_thousands_with_decimal(self):
        for values, expected in (
            ([200120, 199840, 200000], ["Мин 199,8 тыс. ₽", "Макс 200,1 тыс. ₽", "Закрытие 200,0 тыс. ₽"]),
            ([2000000, 1999840, 2000120], ["Мин 1 999,8 тыс. ₽", "Макс 2 000,1 тыс. ₽", "Закрытие 2 000,1 тыс. ₽"]),
        ):
            with self.subTest(values=values):
                ax, _ = self._chart_axes([dict(row, total_value=value) for row, value in zip(_snapshots(), values)])
                self.assertEqual([text.get_text() for text in ax.texts], expected)
                self.assertEqual(ax.yaxis.get_major_formatter()(200000), "200,0 тыс. ₽")

    def test_extremum_at_closing_has_label_to_the_left(self):
        for values, label in (([200000, 200100, 200200], "Макс"), ([200000, 199900, 199800], "Мин")):
            with self.subTest(values=values):
                ax, _ = self._chart_axes([dict(row, total_value=value) for row, value in zip(_snapshots(), values)])
                extremum = next(text for text in ax.texts if text.get_text().startswith(label))
                closing = next(text for text in ax.texts if text.get_text().startswith("Закрытие"))
                canvas = FigureCanvasAgg(ax.figure)
                canvas.draw()
                renderer = canvas.get_renderer()
                self.assertFalse(extremum.get_window_extent(renderer).overlaps(closing.get_window_extent(renderer)))

    def test_chart_wraps_full_names_and_places_amounts_at_bar_ends(self):
        movers = [
            {"label": "Биржевой фонд на акции российских компаний (EQMX)", "contribution": -637},
            {"label": "ОФЗ 26254 (SU26254RMFS1)", "contribution": -410},
        ]
        _, ax = self._chart_axes(_snapshots(), negative_movers=movers)
        labels = [label.get_text().replace("\n", " ") for label in ax.get_yticklabels()]
        self.assertEqual(labels, [row["label"] for row in movers])
        amounts = [text for text in ax.texts if "₽" in text.get_text()]
        self.assertEqual([text.get_text() for text in amounts], ["-637 ₽", "-410 ₽"])
        self.assertEqual([text.get_position()[1] for text in amounts], [0, 1])

    def test_payload_separates_external_flow_and_lists_top_contributions(self):
        payload = build_intraday_daily_push_payload(
        report_date=date(2026, 9, 16),
        snapshots=_snapshots(),
        positions=[
            {"ticker": "SBER", "first_value": "200000", "last_value": "207000"},
            {"ticker": "LKOH", "first_value": "300000", "last_value": "304000"},
            {"ticker": "YDEX", "first_value": "100000", "last_value": "97000"},
        ],
        cash_events=[
            {"kind": "deposit", "amount": "5000"},
            {"kind": "coupon", "amount": "1200"},
        ],
    )

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["market_change"], Decimal("10000"))
        self.assertIn("Изменение: +10 000,00 ₽ (+1,00%)", payload["text"])
        self.assertIn("SBER +7 000 ₽", payload["text"])
        self.assertIn("YDEX −3 000 ₽", payload["text"])
        self.assertIn("Зачисленные купоны до налога: +1 200,00 ₽", payload["text"])
        self.assertIn("Пополнения / выводы: +5 000,00 ₽ — исключены из изменения.", payload["text"])
        self.assertNotIn("Итог", payload["text"])


    def test_payload_requires_two_observations(self):
        self.assertIsNone(build_intraday_daily_push_payload(
            report_date=date(2026, 9, 16), snapshots=_snapshots()[:1], positions=[]
        ))


    def test_chart_contains_intraday_line_and_mover_bars(self):
        payload = build_intraday_daily_push_payload(
        report_date=date(2026, 9, 16),
        snapshots=_snapshots(),
        positions=[
            {"ticker": "SBER", "first_value": "200000", "last_value": "207000"},
            {"ticker": "YDEX", "first_value": "100000", "last_value": "97000"},
        ],
    )
        self.assertIsNotNone(payload)
        assert payload is not None
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "intraday.png"

            result = build_intraday_daily_push_chart(
                str(output),
                report_date=date(2026, 9, 16),
                snapshots=payload["snapshots"],
                positive_movers=payload["positive_movers"],
                negative_movers=payload["negative_movers"],
            )

            self.assertEqual(result, str(output))
            self.assertTrue(output.read_bytes().startswith(b"\x89PNG"))


class IntradayDailyPushJobTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    @contextmanager
    def _session():
        yield SimpleNamespace(commit=lambda: None)

    async def test_named_digest_is_sent_as_plain_text(self):
        day = IntradayDay(
            account_id="account", observed_date=date(2026, 9, 16),
            snapshots=(
                IntradaySnapshot(datetime(2026, 9, 16, 7, tzinfo=timezone.utc), Decimal("100"), "RUB"),
                IntradaySnapshot(datetime(2026, 9, 16, 16, tzinfo=timezone.utc), Decimal("110"), "RUB"),
            ),
            positions=(IntradayPosition("fund", "figi", "TEST", "Фонд *акций*", Decimal("100"), Decimal("110")),),
        )

        async def deliver(**kwargs):
            await kwargs["send"]()
            return True

        with (
            patch.object(push_job, "db_session", side_effect=self._session),
            patch.object(push_job, "claim_scheduled_job_run", return_value=(True, True, "attempt")),
            patch.object(push_job, "resolve_reporting_account_id", return_value="account"),
            patch.object(push_job, "get_intraday_day", return_value=day),
            patch.object(push_job, "_cash_events", return_value=[]),
            patch.object(push_job, "upsert_intraday_daily_summary"),
            patch.object(push_job, "build_intraday_daily_push_chart", return_value="unused"),
            patch.object(push_job, "TARGET_CHAT_IDS", {101}),
            patch.object(push_job, "send_tracked_notification", side_effect=deliver),
            patch.object(push_job, "safe_send_message", new_callable=AsyncMock) as send_message,
            patch.object(push_job, "safe_send_photo", new_callable=AsyncMock),
            patch.object(push_job, "notification_deliveries_are_complete", return_value=True),
            patch.object(push_job, "mark_intraday_daily_summary_delivered") as mark_delivered,
            patch.object(push_job, "finalize_scheduled_job_run") as finalize,
        ):
            await push_job._run(SimpleNamespace(bot=object()), "scheduled", datetime(2026, 9, 16, 19, 10))

        send_message.assert_awaited_once()
        self.assertIsNone(send_message.call_args.kwargs["parse_mode"])
        self.assertIn("Фонд *акций* (TEST) +10 ₽", send_message.call_args.args[2])
        self.assertEqual(finalize.call_args.kwargs["failed_total"], 0)
        mark_delivered.assert_called_once()

    @unittest.skip("workflow is covered by container rehearsal; pure delivery contracts are tested separately")
    async def test_job_delivers_text_and_chart_then_marks_summary(self):
        day = IntradayDay(
            account_id="account",
            observed_date=date(2026, 9, 16),
            snapshots=(
                IntradaySnapshot(datetime(2026, 9, 16, 10), Decimal("100"), "RUB"),
                IntradaySnapshot(datetime(2026, 9, 16, 19), Decimal("110"), "RUB"),
            ),
            positions=(
                IntradayPosition("sber", "figi", "SBER", "Sber", Decimal("100"), Decimal("110")),
            ),
        )
        with (
            patch.object(jobs, "db_session", side_effect=lambda: self._session()),
            patch.object(jobs, "resolve_reporting_account_id", return_value="account"),
            patch.object(jobs, "get_intraday_day", return_value=day),
            patch.object(jobs, "get_net_external_flow_for_period", return_value=0),
            patch.object(jobs, "get_income_for_period", return_value=(Decimal("0"), Decimal("0"))),
            patch.object(jobs, "upsert_intraday_daily_summary"),
            patch.object(jobs, "_claim_scheduled_job_run", return_value=(True, True, "attempt")),
            patch.object(jobs, "_finalize_scheduled_job_run") as finalize,
            patch.object(jobs, "TARGET_CHAT_IDS", {101}),
            patch.object(jobs, "build_intraday_daily_push_chart", return_value="unused"),
            patch.object(jobs, "_send_tracked_notification", new=AsyncMock(return_value=True)) as tracked,
            patch.object(jobs, "_all_notification_deliveries_complete", return_value=True),
            patch.object(jobs, "mark_intraday_daily_summary_delivered") as mark_delivered,
        ):
            await jobs._run_intraday_daily_push_job(
                SimpleNamespace(bot=object()),
                trigger_source="scheduled",
                now_local=datetime(2026, 9, 16, 19, 10),
            )

        self.assertEqual(tracked.await_count, 2)
        mark_delivered.assert_called_once()
        finalize.assert_called_once()

    @unittest.skip("weekday gate is covered by is_intraday_capture_window storage tests")
    async def test_job_does_not_run_on_weekend(self):
        with patch.object(jobs, "_claim_scheduled_job_run") as claim:
            await jobs._run_intraday_daily_push_job(
                SimpleNamespace(bot=object()),
                trigger_source="scheduled",
                now_local=datetime(2026, 9, 19, 19, 10),
            )
        claim.assert_not_called()
