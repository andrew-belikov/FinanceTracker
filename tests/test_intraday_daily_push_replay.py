from contextlib import nullcontext
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from financetracker.bot import intraday_daily_push_replay as replay
from financetracker.database.intraday import IntradayDay, IntradaySnapshot


class IntradayReplayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.report_date = date(2026, 9, 28)
        self.day = IntradayDay(
            "synthetic-account", self.report_date,
            tuple(IntradaySnapshot(datetime(2026, 9, 28, hour, tzinfo=timezone.utc), Decimal(value), "RUB") for hour, value in [(7, "100"), (16, "110")]),
            (),
        )
        self.session = Mock()

    def _patches(self, day=None):
        return (
            patch.object(replay, "db_session", return_value=nullcontext(self.session)),
            patch.object(replay, "resolve_reporting_account_id", return_value="synthetic-account"),
            patch.object(replay, "get_intraday_day", return_value=day or self.day),
            patch.object(replay, "_cash_events", return_value=[]),
            patch.object(replay, "TARGET_CHAT_IDS", {1, 2}),
        )

    async def test_dry_run_renders_without_sending_or_writing(self):
        patches = self._patches()
        with (
            patches[0], patches[1], patches[2], patches[3], patches[4],
            patch.object(replay, "build_intraday_daily_push_chart", return_value="rendered") as render,
            patch.object(replay, "safe_send_message", new_callable=AsyncMock) as message,
            patch.object(replay, "safe_send_photo", new_callable=AsyncMock) as photo,
        ):
            self.assertEqual(await replay.replay_intraday_daily_push(self.report_date), 0)
        render.assert_called_once()
        message.assert_not_awaited()
        photo.assert_not_awaited()
        self.session.commit.assert_not_called()

    async def test_send_replays_both_parts_to_configured_recipients_without_writes(self):
        bot = AsyncMock()
        bot.__aenter__.return_value = bot
        patches = self._patches()
        with (
            patches[0], patches[1], patches[2], patches[3], patches[4],
            patch.object(replay, "build_intraday_daily_push_chart", return_value="rendered"),
            patch("financetracker.bot.bot.build_application", return_value=SimpleNamespace(bot=bot)),
            patch.object(replay, "safe_send_message", new_callable=AsyncMock) as message,
            patch.object(replay, "safe_send_photo", new_callable=AsyncMock) as photo,
        ):
            self.assertEqual(await replay.replay_intraday_daily_push(self.report_date, send=True), 2)
        self.assertEqual([call.args[1] for call in message.await_args_list], [1, 2])
        self.assertEqual(photo.await_count, 2)
        self.assertTrue(all(call.kwargs["parse_mode"] is None for call in message.await_args_list))
        self.session.commit.assert_not_called()

    async def test_missing_observations_stop_before_render_or_send(self):
        day = IntradayDay("synthetic-account", self.report_date, (), ())
        patches = self._patches(day)
        with (
            patches[0], patches[1], patches[2], patches[3], patches[4],
            patch.object(replay, "build_intraday_daily_push_chart") as render,
            patch.object(replay, "safe_send_message", new_callable=AsyncMock) as message,
        ):
            with self.assertRaisesRegex(ValueError, "Нет минутных данных"):
                await replay.replay_intraday_daily_push(self.report_date, send=True)
        render.assert_not_called()
        message.assert_not_awaited()


class IntradayReplayCliTests(unittest.TestCase):
    def test_cli_requires_explicit_send_flag(self):
        with patch.object(replay, "replay_intraday_daily_push", new_callable=AsyncMock, return_value=0) as run, patch("builtins.print"):
            self.assertEqual(replay.main(["--date", "2026-09-28"]), 0)
        run.assert_awaited_once_with(date(2026, 9, 28), send=False)


if __name__ == "__main__":
    unittest.main()
