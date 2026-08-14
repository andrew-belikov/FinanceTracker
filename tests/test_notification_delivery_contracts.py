import asyncio
import sys
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch

from telegram.error import BadRequest, TimedOut


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import jobs  # noqa: E402
import queries  # noqa: E402
import runtime  # noqa: E402


@contextmanager
def fake_db_session():
    yield object()


class IncomeAndInvestNotificationContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_income_query_row_sends_without_iis_markup_or_keyerror(self):
        row = {
            "id": 17,
            "figi": "FIGI1",
            "event_type": "coupon",
            "net_amount": Decimal("100"),
            "net_yield_pct": Decimal("1.25"),
            "coupon_period_days": 182,
            "instrument_name": "Облигация",
        }
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "resolve_reporting_account_id", return_value="account"),
            patch.object(jobs, "get_unnotified_income_events", return_value=[row]),
            patch.object(jobs, "TARGET_CHAT_IDS", {101}),
            patch.object(jobs, "safe_send_message", new=AsyncMock()) as send,
            patch.object(jobs, "claim_notification_delivery", return_value=True),
            patch.object(jobs, "complete_notification_delivery", return_value=True),
            patch.object(jobs, "notification_deliveries_complete", return_value=True),
            patch.object(jobs, "mark_income_event_notified") as mark,
        ):
            await jobs.check_income_events(SimpleNamespace(bot=object()))

        self.assertNotIn("reply_markup", send.await_args.kwargs)
        mark.assert_called_once_with(ANY, 17)

    async def test_invest_notification_has_reversible_iis_markup(self):
        row = {
            "operation_id": "deposit-1",
            "date": datetime(2026, 8, 14, 8, 0),
            "amount": Decimal("10000"),
            "cashflow_category": None,
        }
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "resolve_reporting_account_id", return_value="account"),
            patch.object(jobs, "get_pending_invest_notifications", return_value=[row]),
            patch.object(jobs, "build_invest_text_for_account", return_value="Пополнение"),
            patch.object(jobs, "TARGET_CHAT_IDS", {101}),
            patch.object(jobs, "safe_send_message", new=AsyncMock()) as send,
            patch.object(jobs, "claim_notification_delivery", return_value=True),
            patch.object(jobs, "complete_notification_delivery", return_value=True),
            patch.object(jobs, "notification_deliveries_complete", return_value=True),
            patch.object(jobs, "mark_invest_notification_sent", return_value=True),
        ):
            await jobs.check_invest_notifications(SimpleNamespace(bot=object()))

        button = send.await_args.kwargs["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(button.callback_data, "iis_deduction:set:deposit-1")


class TelegramFallbackContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_is_not_retried_as_plain_text(self):
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=TimedOut("timeout")))

        with self.assertRaises(TimedOut):
            await runtime.safe_send_message(bot, 101, "text", parse_mode="Markdown")

        self.assertEqual(bot.send_message.await_count, 1)

    async def test_parse_bad_request_has_exactly_one_plain_text_fallback(self):
        bot = SimpleNamespace(
            send_message=AsyncMock(
                side_effect=[BadRequest("Can't parse entities: bad markdown"), "sent"]
            )
        )

        result = await runtime.safe_send_message(bot, 101, "text", parse_mode="Markdown")

        self.assertEqual(result, "sent")
        self.assertEqual(bot.send_message.await_count, 2)
        self.assertNotIn("parse_mode", bot.send_message.await_args_list[1].kwargs)

    async def test_non_parse_bad_request_is_not_retried(self):
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=BadRequest("chat not found")))

        with self.assertRaises(BadRequest):
            await runtime.safe_send_message(bot, 101, "text", parse_mode="Markdown")

        self.assertEqual(bot.send_message.await_count, 1)


class DurableLedgerSqlContractTests(unittest.TestCase):
    def test_forward_migration_defines_fenced_leases_and_recipient_ledger(self):
        sql = (
            PROJECT_ROOT / "migrations" / "20260814_bot_notification_delivery_leases.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("ADD COLUMN IF NOT EXISTS attempt_id TEXT", sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS bot_notification_deliveries", sql)
        self.assertIn(
            "UNIQUE (notification_kind, notification_key, chat_id, message_type)",
            " ".join(sql.split()),
        )

    def test_job_claim_is_stale_reclaimable_and_completed_is_terminal(self):
        session = _RecordingSession(returning={"attempt_id": "new-owner"})
        claimed = queries.claim_daily_job_run(
            session,
            job_name="daily_summary",
            run_date=date(2026, 8, 14),
            attempt_id="new-owner",
            now_utc=datetime(2026, 8, 14, 12, 0),
            lease_timeout=timedelta(minutes=15),
        )

        sql = session.statements[0][0]
        self.assertTrue(claimed)
        self.assertIn("status <> 'completed'", sql)
        self.assertIn("heartbeat_at < :stale_before", sql)
        self.assertIn("RETURNING attempt_id", sql)

    def test_finalize_and_release_are_fenced_by_owner_token(self):
        for function in (
            queries.complete_daily_job_run,
            queries.heartbeat_daily_job_run,
            queries.release_daily_job_run,
        ):
            with self.subTest(function=function.__name__):
                session = _RecordingSession(rowcount=1)
                kwargs = {
                    "job_name": "daily_summary",
                    "run_date": date(2026, 8, 14),
                    "attempt_id": "owner-1",
                }
                if function is queries.complete_daily_job_run:
                    kwargs.update(sent_total=1, failed_total=0)
                self.assertTrue(function(session, **kwargs))
                sql, params = session.statements[0]
                self.assertIn("attempt_id = :attempt_id", sql)
                self.assertEqual(params["attempt_id"], "owner-1")


class RecipientLedgerBehaviorTests(unittest.IsolatedAsyncioTestCase):
    async def test_fresh_started_claim_blocks_concurrent_worker(self):
        send = AsyncMock()
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "claim_notification_delivery", return_value=False) as claim,
            patch.object(jobs, "get_notification_delivery_status", return_value="started"),
        ):
            with self.assertRaisesRegex(RuntimeError, "active worker"):
                await jobs._send_tracked_notification(
                    notification_kind="income_event",
                    notification_key="17",
                    chat_id=101,
                    message_type="income",
                    send=send,
                )

        send.assert_not_awaited()
        self.assertTrue(claim.call_args.kwargs["reclaim_stale"])

    async def test_uncertain_delivery_is_terminal_and_not_reclaimed(self):
        send = AsyncMock()
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "claim_notification_delivery", return_value=False),
            patch.object(jobs, "get_notification_delivery_status", return_value="uncertain"),
        ):
            with self.assertRaisesRegex(RuntimeError, "ambiguous outcome"):
                await jobs._send_tracked_notification(
                    notification_kind="income_event",
                    notification_key="17",
                    chat_id=101,
                    message_type="income",
                    send=send,
                )

        send.assert_not_awaited()

    async def test_income_stale_started_claim_recovers_after_pre_send_crash(self):
        row = {
            "id": 17,
            "figi": "FIGI1",
            "event_type": "coupon",
            "net_amount": Decimal("100"),
            "net_yield_pct": Decimal("1.25"),
            "coupon_period_days": 182,
            "instrument_name": "Облигация",
        }
        ledger = _CrashRecoveryLedger(
            notification_kind="income_event",
            notification_key="17",
            chat_id=101,
            message_type="income",
        )
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "resolve_reporting_account_id", return_value="account"),
            patch.object(jobs, "get_unnotified_income_events", return_value=[row]),
            patch.object(jobs, "TARGET_CHAT_IDS", {101}),
            patch.object(jobs, "safe_send_message", new=AsyncMock()) as send,
            patch.object(jobs, "claim_notification_delivery", side_effect=ledger.claim),
            patch.object(jobs, "complete_notification_delivery", side_effect=ledger.complete),
            patch.object(jobs, "get_notification_delivery_status", side_effect=ledger.status),
            patch.object(
                jobs,
                "notification_deliveries_complete",
                side_effect=ledger.all_complete,
            ),
            patch.object(jobs, "mark_income_event_notified") as mark,
        ):
            await jobs.check_income_events(SimpleNamespace(bot=object()))

        send.assert_awaited_once()
        self.assertEqual(ledger.delivery_status, "sent")
        mark.assert_called_once_with(ANY, 17)

    async def test_invest_stale_started_claim_recovers_after_pre_send_crash(self):
        row = {
            "operation_id": "deposit-1",
            "date": datetime(2026, 8, 14, 8, 0),
            "amount": Decimal("10000"),
            "cashflow_category": None,
        }
        ledger = _CrashRecoveryLedger(
            notification_kind="invest_notification",
            notification_key="deposit-1",
            chat_id=101,
            message_type="invest",
        )
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "resolve_reporting_account_id", return_value="account"),
            patch.object(jobs, "get_pending_invest_notifications", return_value=[row]),
            patch.object(jobs, "build_invest_text_for_account", return_value="Пополнение"),
            patch.object(jobs, "TARGET_CHAT_IDS", {101}),
            patch.object(jobs, "safe_send_message", new=AsyncMock()) as send,
            patch.object(jobs, "claim_notification_delivery", side_effect=ledger.claim),
            patch.object(jobs, "complete_notification_delivery", side_effect=ledger.complete),
            patch.object(jobs, "get_notification_delivery_status", side_effect=ledger.status),
            patch.object(
                jobs,
                "notification_deliveries_complete",
                side_effect=ledger.all_complete,
            ),
            patch.object(jobs, "mark_invest_notification_sent", return_value=True) as mark,
        ):
            await jobs.check_invest_notifications(SimpleNamespace(bot=object()))

        send.assert_awaited_once()
        self.assertEqual(ledger.delivery_status, "sent")
        mark.assert_called_once()

    async def test_restart_skips_recipient_already_delivered(self):
        state = set()
        send = AsyncMock()

        def claim(_session, **kwargs):
            return (kwargs["notification_key"], kwargs["chat_id"], kwargs["message_type"]) not in state

        def complete(_session, **kwargs):
            state.add((kwargs["notification_key"], kwargs["chat_id"], kwargs["message_type"]))
            return True

        def deliveries_complete(_session, **kwargs):
            return all(
                (kwargs["notification_key"], chat_id, message_type) in state
                for chat_id in kwargs["chat_ids"]
                for message_type in kwargs["message_types"]
            )

        def delivery_status(_session, **kwargs):
            key = (kwargs["notification_key"], kwargs["chat_id"], kwargs["message_type"])
            return "sent" if key in state else "started"

        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "claim_notification_delivery", side_effect=claim),
            patch.object(jobs, "complete_notification_delivery", side_effect=complete),
            patch.object(jobs, "notification_deliveries_complete", side_effect=deliveries_complete),
            patch.object(jobs, "get_notification_delivery_status", side_effect=delivery_status),
        ):
            first = await jobs._send_tracked_notification(
                notification_kind="income_event",
                notification_key="17",
                chat_id=101,
                message_type="income",
                send=send,
            )
            second = await jobs._send_tracked_notification(
                notification_kind="income_event",
                notification_key="17",
                chat_id=101,
                message_type="income",
                send=send,
            )

        self.assertTrue(first)
        self.assertFalse(second)
        send.assert_awaited_once()

    async def test_ambiguous_timeout_is_fenced_without_retry_claim(self):
        send = AsyncMock(side_effect=TimedOut("timeout"))
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "claim_notification_delivery", return_value=True),
            patch.object(jobs, "mark_notification_delivery_uncertain", return_value=True) as uncertain,
            patch.object(jobs, "release_notification_delivery") as release,
        ):
            with self.assertRaises(TimedOut):
                await jobs._send_tracked_notification(
                    notification_kind="income_event",
                    notification_key="17",
                    chat_id=101,
                    message_type="income",
                    send=send,
                    reclaim_stale=False,
                )

        send.assert_awaited_once()
        uncertain.assert_called_once()
        release.assert_not_called()

    async def test_non_parse_bad_request_releases_delivery_for_retry(self):
        state = {}
        send = AsyncMock(side_effect=[BadRequest("chat not found"), None])

        def claim(_session, **kwargs):
            if state:
                return False
            state["status"] = "started"
            return True

        def release(_session, **kwargs):
            state.clear()
            return True

        def uncertain_delivery(_session, **kwargs):
            state["status"] = "uncertain"
            return True

        def complete(_session, **kwargs):
            state["status"] = "sent"
            return True

        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "claim_notification_delivery", side_effect=claim),
            patch.object(jobs, "release_notification_delivery", side_effect=release) as released,
            patch.object(
                jobs,
                "mark_notification_delivery_uncertain",
                side_effect=uncertain_delivery,
            ) as uncertain,
            patch.object(jobs, "complete_notification_delivery", side_effect=complete),
            patch.object(
                jobs,
                "get_notification_delivery_status",
                side_effect=lambda *_args, **_kwargs: state.get("status"),
            ),
        ):
            with self.assertRaises(BadRequest):
                await jobs._send_tracked_notification(
                    notification_kind="scheduled_job",
                    notification_key="daily_summary:2026-08-14",
                    chat_id=101,
                    message_type="trigger:0",
                    send=send,
                )
            second = await jobs._send_tracked_notification(
                notification_kind="scheduled_job",
                notification_key="daily_summary:2026-08-14",
                chat_id=101,
                message_type="trigger:0",
                send=send,
            )

        self.assertTrue(second)
        self.assertEqual(state["status"], "sent")
        self.assertEqual(send.await_count, 2)
        released.assert_called_once()
        uncertain.assert_not_called()

    async def test_scheduled_timeout_is_uncertain_and_not_retried(self):
        state = {}
        send_attempts = []

        def claim_delivery(_session, **kwargs):
            key = (kwargs["notification_key"], kwargs["chat_id"], kwargs["message_type"])
            return key not in state

        def complete_delivery(_session, **kwargs):
            key = (kwargs["notification_key"], kwargs["chat_id"], kwargs["message_type"])
            state[key] = "sent"
            return True

        def mark_uncertain(_session, **kwargs):
            key = (kwargs["notification_key"], kwargs["chat_id"], kwargs["message_type"])
            state[key] = "uncertain"
            return True

        def deliveries_complete(_session, **kwargs):
            return all(
                state.get((kwargs["notification_key"], chat_id, message_type)) == "sent"
                for chat_id in kwargs["chat_ids"]
                for message_type in kwargs["message_types"]
            )

        def delivery_status(_session, **kwargs):
            key = (kwargs["notification_key"], kwargs["chat_id"], kwargs["message_type"])
            return state.get(key, "started")

        async def send_message(bot, chat_id, text, parse_mode=None):
            send_attempts.append(chat_id)
            raise TimedOut("timeout")

        now_local = jobs.datetime(2026, 8, 14, 8, 0, tzinfo=jobs.TZ)
        with (
            patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()),
            patch.object(jobs, "TARGET_CHAT_IDS", {202}),
            patch.object(jobs, "claim_daily_job_run", return_value=True),
            patch.object(jobs, "complete_daily_job_run", return_value=True) as complete_run,
            patch.object(jobs, "release_daily_job_run", return_value=True) as release_run,
            patch.object(jobs, "_heartbeat_scheduled_job_run", return_value=True),
            patch.object(jobs, "claim_notification_delivery", side_effect=claim_delivery),
            patch.object(jobs, "complete_notification_delivery", side_effect=complete_delivery),
            patch.object(jobs, "mark_notification_delivery_uncertain", side_effect=mark_uncertain) as uncertain,
            patch.object(jobs, "release_notification_delivery", return_value=True) as release_delivery,
            patch.object(jobs, "notification_deliveries_complete", side_effect=deliveries_complete),
            patch.object(jobs, "get_notification_delivery_status", side_effect=delivery_status),
            patch.object(jobs, "build_yesterday_peak_alert_message", return_value="peak"),
            patch.object(jobs, "safe_send_message", side_effect=send_message),
        ):
            await jobs._run_yesterday_peak_alert_job(
                SimpleNamespace(bot=object()),
                trigger_source="scheduled",
                now_local=now_local,
            )
            await jobs._run_yesterday_peak_alert_job(
                SimpleNamespace(bot=object()),
                trigger_source="startup_catchup",
                now_local=now_local,
            )

        self.assertEqual(send_attempts, [202])
        self.assertEqual(next(iter(state.values())), "uncertain")
        uncertain.assert_called_once()
        release_delivery.assert_not_called()
        self.assertEqual(release_run.call_count, 2)
        complete_run.assert_not_called()


class _CrashRecoveryLedger:
    def __init__(
        self,
        *,
        notification_kind: str,
        notification_key: str,
        chat_id: int,
        message_type: str,
    ):
        self.identity = (notification_kind, notification_key, chat_id, message_type)
        self.delivery_status = "started"
        self.claimed_at = datetime(2026, 8, 13, 8, 0)
        self.attempt_id = "crashed-owner"

    def claim(self, _session, **kwargs):
        identity = (
            kwargs["notification_kind"],
            kwargs["notification_key"],
            kwargs["chat_id"],
            kwargs["message_type"],
        )
        if identity != self.identity:
            return False
        stale_before = datetime(2026, 8, 14, 8, 0) - timedelta(minutes=15)
        if (
            kwargs["reclaim_stale"]
            and self.delivery_status == "started"
            and self.claimed_at < stale_before
        ):
            self.attempt_id = kwargs["attempt_id"]
            self.claimed_at = datetime(2026, 8, 14, 8, 0)
            return True
        return False

    def complete(self, _session, **kwargs):
        if kwargs["attempt_id"] != self.attempt_id or self.delivery_status != "started":
            return False
        self.delivery_status = "sent"
        return True

    def status(self, _session, **_kwargs):
        return self.delivery_status

    def all_complete(self, _session, **_kwargs):
        return self.delivery_status == "sent"


class _RecordingResult:
    def __init__(self, *, returning=None, rowcount=1):
        self._returning = returning
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._returning


class _RecordingSession:
    def __init__(self, *, returning=None, rowcount=1):
        self.returning = returning
        self.rowcount = rowcount
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params or {}))
        return _RecordingResult(returning=self.returning, rowcount=self.rowcount)

    def commit(self):
        pass

    def rollback(self):
        pass


if __name__ == "__main__":
    unittest.main()
