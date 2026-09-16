import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from financetracker.bot.polling_watchdog import PollingWatchdogState, run_polling_watchdog


class PollingWatchdogTests(unittest.IsolatedAsyncioTestCase):
    async def test_confirmed_backlog_requests_self_heal_once(self):
        application = SimpleNamespace(stop_running=Mock())
        context = SimpleNamespace(
            bot=SimpleNamespace(get_webhook_info=AsyncMock(return_value=SimpleNamespace(pending_update_count=7))),
            application=application,
        )
        state = await run_polling_watchdog(
            context,
            state=PollingWatchdogState(detection_streak=1),
            now_utc=datetime(2026, 9, 16, tzinfo=timezone.utc),
            get_last_update_received_at_utc=lambda: datetime(2026, 9, 16, tzinfo=timezone.utc),
            get_process_started_at_utc=lambda: datetime(2026, 9, 15, tzinfo=timezone.utc),
            is_polling_backlog_detected=lambda **_kwargs: True,
            next_polling_backlog_detection_streak=lambda **_kwargs: 2,
            should_trigger_polling_self_heal=lambda **_kwargs: True,
            to_iso_datetime=lambda value: value.isoformat() if value else None,
            pending_threshold=5,
            stall_threshold_seconds=60,
            recovery_confirmation_count=2,
            self_heal_exit_code=75,
            logger=Mock(),
        )

        self.assertTrue(state.backlog_active)
        self.assertTrue(state.self_heal_requested)
        self.assertEqual(state.exit_code, 75)
        application.stop_running.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
