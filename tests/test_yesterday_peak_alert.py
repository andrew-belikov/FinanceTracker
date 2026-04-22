import asyncio
import sys
import unittest
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import jobs  # noqa: E402
import services  # noqa: E402


@contextmanager
def fake_db_session():
    yield object()


class YesterdayPeakAlertMessageTests(unittest.TestCase):
    def test_message_includes_yesterday_value_and_previous_peak_date(self):
        now_local = datetime(2026, 4, 21, 8, 0, 0, tzinfo=services.TZ)

        with patch.object(services, "db_session", side_effect=lambda: fake_db_session()), \
             patch.object(services, "resolve_reporting_account_id", return_value="account-1"), \
             patch.object(
                 services,
                 "get_snapshot_for_date",
                 return_value={
                     "snapshot_date": date(2026, 4, 20),
                     "total_value": 483450.28,
                 },
             ), \
             patch.object(
                 services,
                 "get_max_snapshot_before_date",
                 return_value={
                     "snapshot_date": date(2026, 4, 6),
                     "total_value": 482463.40,
                 },
             ):
            message = services.build_yesterday_peak_alert_message(now_local=now_local)

        self.assertIsNotNone(message)
        self.assertIn("Итоговая оценка за 20.04.26: *483 450 ₽*", message)
        self.assertIn("Предыдущий максимум: 482 463 ₽.", message)
        self.assertIn("Дата предыдущего максимума: 06.04.26.", message)

    def test_message_is_skipped_when_yesterday_did_not_exceed_previous_peak(self):
        now_local = datetime(2026, 4, 21, 8, 0, 0, tzinfo=services.TZ)

        with patch.object(services, "db_session", side_effect=lambda: fake_db_session()), \
             patch.object(services, "resolve_reporting_account_id", return_value="account-1"), \
             patch.object(
                 services,
                 "get_snapshot_for_date",
                 return_value={
                     "snapshot_date": date(2026, 4, 20),
                     "total_value": 483113.13,
                 },
             ), \
             patch.object(
                 services,
                 "get_max_snapshot_before_date",
                 return_value={
                     "snapshot_date": date(2026, 4, 19),
                     "total_value": 483450.28,
                 },
             ):
            message = services.build_yesterday_peak_alert_message(now_local=now_local)

        self.assertIsNone(message)


class YesterdayPeakAlertJobTests(unittest.TestCase):
    def test_job_sends_message_once_per_yesterday_date(self):
        context = SimpleNamespace(bot=object())
        now_local = jobs.datetime(2026, 4, 21, 8, 0, 0, tzinfo=jobs.TZ)

        with patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()), \
             patch.object(jobs, "TARGET_CHAT_IDS", {123, 456}), \
             patch.object(jobs, "claim_daily_job_run", return_value=True) as claim_run, \
             patch.object(jobs, "complete_daily_job_run") as complete_run, \
             patch.object(jobs, "release_daily_job_run") as release_run, \
             patch.object(jobs, "build_yesterday_peak_alert_message", return_value="peak alert"), \
             patch.object(jobs, "safe_send_message", new=AsyncMock()) as send_message:
            asyncio.run(
                jobs._run_yesterday_peak_alert_job(
                    context,
                    trigger_source="scheduled",
                    now_local=now_local,
                )
            )

        claim_run.assert_called_once_with(
            ANY,
            job_name=jobs.YESTERDAY_PEAK_ALERT_JOB_NAME,
            run_date=date(2026, 4, 20),
        )
        self.assertEqual(send_message.await_count, 2)
        release_run.assert_not_called()
        complete_run.assert_called_once_with(
            ANY,
            job_name=jobs.YESTERDAY_PEAK_ALERT_JOB_NAME,
            run_date=date(2026, 4, 20),
            sent_total=2,
            failed_total=0,
        )


if __name__ == "__main__":
    unittest.main()
