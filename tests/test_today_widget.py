import json
import unittest
import zlib
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from telegram.error import BadRequest, Forbidden

from financetracker.bot import today_widget, today_widget_view
from financetracker.bot.today_widget_view import TODAY_WIDGET_HTML, _chart, _encode
from financetracker.database.intraday import IntradayDay, IntradaySnapshot
from financetracker.database.models import IntradayPortfolioSnapshot


class WidgetDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.bot = SimpleNamespace(
            id=99,
            do_api_request=AsyncMock(),
            unpin_chat_message=AsyncMock(),
            delete_message=AsyncMock(),
        )
        self.context = SimpleNamespace(bot=self.bot, bot_data={})
        self.message = {
            "message_id": 5,
            "from": {"id": 99},
            "caption": today_widget.WIDGET_CAPTION,
            "widget": {"chat_data": "old"},
        }
        data = patch.object(today_widget, "build_today_widget_data", return_value="new")
        self.build_data = data.start()
        self.addCleanup(data.stop)
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state_path = Path(directory.name) / "widgets.json"
        state = patch.object(today_widget, "STATE_PATH", self.state_path)
        state.start()
        self.addCleanup(state.stop)

    async def test_each_command_sends_a_new_message_and_refresh_uses_latest_after_restart(self):
        today_widget._save_state(self.bot, {})
        self.bot.do_api_request.side_effect = [{"message_id": 5}, {"message_id": 6}, True]
        await today_widget.send_today_widget(self.context, 1)
        self.bot.delete_message.assert_not_awaited()
        await today_widget.send_today_widget(self.context, 1)
        self.bot.delete_message.assert_awaited_once_with(1, 5)
        sends = self.bot.do_api_request.await_args_list
        self.assertEqual([item.args[0] for item in sends], ["sendWidget", "sendWidget"])
        for item in sends:
            sent = item.args[1]
            self.assertEqual(sent["chat_data"], "new")
            self.assertTrue(sent["protect_content"])
            self.assertNotIn("public_data", sent)
        self.bot.unpin_chat_message.assert_not_awaited()

        self.context.bot_data = {}
        self.build_data.return_value = "newer"
        with patch.object(today_widget, "TARGET_CHAT_IDS", (1,)):
            await today_widget.refresh_today_widgets(self.context)
        self.bot.do_api_request.assert_awaited_with("editMessageWidgetData", {
            "chat_id": 1, "message_id": 6, "chat_data": "newer",
        })

    async def test_failed_send_keeps_the_previous_widget(self):
        original = {"1": [5, today_widget._digest("old")]}
        today_widget._save_state(self.bot, original)
        self.bot.do_api_request.side_effect = BadRequest("Send failed")
        with self.assertRaises(BadRequest):
            await today_widget.send_today_widget(self.context, 1)
        self.bot.delete_message.assert_not_awaited()
        self.assertEqual(await today_widget._load_state(self.bot), original)

    async def test_rejected_deletion_keeps_the_new_widget_active(self):
        today_widget._save_state(self.bot, {"1": [5, today_widget._digest("old")]})
        self.bot.do_api_request.return_value = {"message_id": 6}
        self.bot.delete_message.side_effect = BadRequest("Message can't be deleted")
        await today_widget.send_today_widget(self.context, 1)
        self.bot.delete_message.assert_awaited_once_with(1, 5)
        self.assertEqual(await today_widget._load_state(self.bot), {"1": [6, today_widget._digest("new")]})

    async def test_replacing_bot_never_deletes_previous_bots_message(self):
        today_widget._save_state(self.bot, {"1": [5, today_widget._digest("old")]})
        self.bot.id = 100
        self.bot.do_api_request.side_effect = [{"type": "private"}, {"message_id": 6}]
        with patch.object(today_widget, "TARGET_CHAT_IDS", (1,)):
            await today_widget.send_today_widget(self.context, 1)
        self.bot.delete_message.assert_not_awaited()
        self.assertEqual(json.loads(self.state_path.read_text())["bot_id"], 100)

    async def test_migrates_only_owned_legacy_pins_and_skips_unchanged_data(self):
        self.message["widget"]["chat_data"] = "new"
        self.bot.do_api_request.side_effect = [
            Forbidden("blocked"),
            {"type": "private", "pinned_message": {**self.message, "from": {"id": 2}}},
            {"type": "private", "pinned_message": self.message},
        ]
        with patch.object(today_widget, "TARGET_CHAT_IDS", (1, 2, 3)):
            await today_widget.refresh_today_widgets(self.context)
            await today_widget.refresh_today_widgets(self.context)
        self.bot.unpin_chat_message.assert_awaited_once_with(3, 5)
        self.assertEqual([item.args[0] for item in self.bot.do_api_request.await_args_list], ["getChat"] * 3)
        self.assertEqual(await today_widget._load_state(self.bot), {"3": [5, today_widget._digest("new")]})

    async def test_refresh_forgets_deleted_message(self):
        today_widget._save_state(self.bot, {"1": [5, today_widget._digest("old")]})
        self.bot.do_api_request.side_effect = BadRequest("Message to edit not found")
        with patch.object(today_widget, "TARGET_CHAT_IDS", (1,)):
            await today_widget.refresh_today_widgets(self.context)
            await today_widget.refresh_today_widgets(self.context)
        self.assertEqual(self.bot.do_api_request.await_args_list, [
            call("editMessageWidgetData", {"chat_id": 1, "message_id": 5, "chat_data": "new"}),
        ])
        self.assertEqual(await today_widget._load_state(self.bot), {})


class WidgetViewTests(unittest.TestCase):
    def test_state_reuses_daily_change_and_caps_chart_at_the_snapshot(self):
        end = datetime(2026, 11, 9, 14, tzinfo=timezone.utc)
        day = IntradayDay(
            "account", end.date(), (IntradaySnapshot(end, Decimal("1150"), "RUB"),), (),
            baseline=IntradaySnapshot(end - timedelta(days=1), Decimal("1000"), "RUB"),
        )
        events = [{"kind": "deposit", "amount": 100}, {"kind": "coupon", "amount": 40}]
        engine = create_engine("sqlite://")
        IntradayPortfolioSnapshot.__table__.create(engine)
        with Session(engine) as session:
            for minutes, value in [(-421, 9999), (-420, 1110), (0, 1150), (1, 9999)]:
                session.add(IntradayPortfolioSnapshot(
                    account_id=day.account_id, snapshot_date=day.observed_date,
                    snapshot_at=end + timedelta(minutes=minutes), currency="RUB", total_value=value,
                ))
            session.commit()
            with (
                patch.object(today_widget_view, "db_session", return_value=nullcontext(session)),
                patch.object(today_widget_view, "load_today_inputs", return_value=(day, events)),
            ):
                data = json.loads(today_widget_view.build_today_widget_data())
        engine.dispose()

        self.assertEqual(data["value"], "1 150,00 ₽")
        self.assertEqual(data["change"], "+50,00 ₽ (+5,00%)")
        self.assertEqual(data["parts"], [["coupon", "+40,00 ₽"], ["other", "+10,00 ₽"]])
        self.assertEqual(data["line"], "6,70 294,12")
        self.assertEqual([point[3] for point in data["stops"]], ["1 110,00 ₽"])

    def test_full_day_keeps_endpoints_and_fits_telegram_limits(self):
        stamp = datetime(2026, 11, 9, 7, tzinfo=timezone.utc)
        observations = [(stamp + timedelta(minutes=i), Decimal("1234567.89") + i) for i in range(700)]
        line, stops = _chart(observations)
        self.assertEqual(len(line.split()), 700)
        self.assertEqual((line.split()[0], line.split()[-1]), ("6,70", "294,12"))
        self.assertEqual(len(stops), 4)
        encoded = _encode({"line": line, "stops": stops, "value": "1 235 266,89 ₽"})
        state = json.loads(encoded)
        self.assertEqual(state["stops"], stops)
        rendered = state["line"].split()
        self.assertGreater(len(rendered), 100)
        self.assertEqual((rendered[0], rendered[-1]), ("6,70", "294,12"))
        self.assertLessEqual(len(encoded.encode()), 1024)
        self.assertLessEqual(len(TODAY_WIDGET_HTML.encode()), 32768)
        self.assertLessEqual(len(zlib.compress(TODAY_WIDGET_HTML.encode(), level=9)), 4096)

    def test_byte_budget_drops_preview_points_before_chart(self):
        state = json.loads(_encode({"line": "6,70 294,12", "stops": ["₽" * 400], "value": "1 ₽"}))
        self.assertEqual(state, {"line": "6,70 294,12", "stops": [], "value": "1 ₽"})
