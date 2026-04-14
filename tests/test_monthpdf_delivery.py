import asyncio
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import jobs  # noqa: E402
from report_client import ReporterClientError  # noqa: E402


@contextmanager
def fake_db_session():
    yield object()


class MonthPdfDeliveryTests(unittest.TestCase):
    def _build_context(self):
        return SimpleNamespace(bot=object())

    def test_month_end_daily_job_prefers_pdf_delivery(self):
        context = self._build_context()
        temp_file = tempfile.NamedTemporaryFile(prefix="daily_monthpdf_", suffix=".pdf", delete=False)
        temp_file.write(b"%PDF-test")
        temp_file.close()
        today = jobs.datetime(2026, 4, 30, 18, 0, 0, tzinfo=jobs.TZ)

        try:
            with patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()), \
                 patch.object(jobs, "TARGET_CHAT_IDS", {123}), \
                 patch.object(jobs, "claim_daily_job_run", side_effect=[True, True]), \
                 patch.object(jobs, "complete_daily_job_run") as complete_run, \
                 patch.object(jobs, "release_daily_job_run") as release_run, \
                 patch.object(jobs, "build_month_summary", return_value="fallback month"), \
                 patch.object(jobs, "build_triggers_messages", return_value=[]), \
                 patch.object(jobs, "build_week_summary") as build_week_summary, \
                 patch.object(
                     jobs,
                     "request_monthly_report_pdf",
                     return_value=(temp_file.name, "fintracker_monthly_2026-04.pdf"),
                 ) as request_pdf, \
                 patch.object(jobs, "safe_send_document", new=AsyncMock()) as send_document, \
                 patch.object(jobs, "safe_send_message", new=AsyncMock()) as send_message:
                asyncio.run(jobs._run_daily_job(context, trigger_source="scheduled", now_local=today))

            request_pdf.assert_called_once_with(year=2026, month=4)
            send_document.assert_awaited_once()
            send_message.assert_not_awaited()
            build_week_summary.assert_not_called()
            release_run.assert_not_called()
            self.assertFalse(Path(temp_file.name).exists())
            complete_calls = [call.kwargs for call in complete_run.call_args_list]
            self.assertEqual(
                complete_calls,
                [
                    {
                        "job_name": jobs.DAILY_JOB_NAME,
                        "run_date": today.date(),
                        "sent_total": 0,
                        "failed_total": 0,
                    },
                    {
                        "job_name": jobs.MONTHLY_PDF_JOB_NAME,
                        "run_date": today.date(),
                        "sent_total": 1,
                        "failed_total": 0,
                    },
                ],
            )
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

    def test_month_end_daily_job_falls_back_to_text_when_reporter_fails(self):
        context = self._build_context()
        today = jobs.datetime(2026, 4, 30, 18, 0, 0, tzinfo=jobs.TZ)

        with patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()), \
             patch.object(jobs, "TARGET_CHAT_IDS", {123}), \
             patch.object(jobs, "claim_daily_job_run", side_effect=[True, True]), \
             patch.object(jobs, "complete_daily_job_run") as complete_run, \
             patch.object(jobs, "release_daily_job_run") as release_run, \
             patch.object(jobs, "build_month_summary", return_value="fallback month"), \
             patch.object(jobs, "build_triggers_messages", return_value=[]), \
             patch.object(
                 jobs,
                 "request_monthly_report_pdf",
                 side_effect=ReporterClientError("reporter down"),
             ), \
             patch.object(jobs, "safe_send_document", new=AsyncMock()) as send_document, \
             patch.object(jobs, "safe_send_message", new=AsyncMock()) as send_message:
            asyncio.run(jobs._run_daily_job(context, trigger_source="scheduled", now_local=today))

        send_document.assert_not_awaited()
        send_message.assert_awaited_once_with(context.bot, 123, "fallback month", parse_mode="Markdown")
        release_run.assert_not_called()
        complete_calls = [call.kwargs for call in complete_run.call_args_list]
        self.assertEqual(complete_calls[-1]["job_name"], jobs.MONTHLY_PDF_JOB_NAME)
        self.assertEqual(complete_calls[-1]["sent_total"], 1)
        self.assertEqual(complete_calls[-1]["failed_total"], 0)

    def test_monthly_pdf_delivery_runs_even_if_daily_summary_already_processed(self):
        context = self._build_context()
        temp_file = tempfile.NamedTemporaryFile(prefix="daily_monthpdf_", suffix=".pdf", delete=False)
        temp_file.write(b"%PDF-test")
        temp_file.close()
        today = jobs.datetime(2026, 4, 30, 18, 0, 0, tzinfo=jobs.TZ)

        try:
            with patch.object(jobs, "db_session", side_effect=lambda: fake_db_session()), \
                 patch.object(jobs, "TARGET_CHAT_IDS", {123}), \
                 patch.object(jobs, "claim_daily_job_run", side_effect=[False, True]), \
                 patch.object(jobs, "complete_daily_job_run") as complete_run, \
                 patch.object(jobs, "release_daily_job_run") as release_run, \
                 patch.object(jobs, "build_month_summary", return_value="fallback month"), \
                 patch.object(jobs, "build_triggers_messages", return_value=[]), \
                 patch.object(jobs, "build_week_summary") as build_week_summary, \
                 patch.object(
                     jobs,
                     "request_monthly_report_pdf",
                     return_value=(temp_file.name, "fintracker_monthly_2026-04.pdf"),
                 ), \
                 patch.object(jobs, "safe_send_document", new=AsyncMock()) as send_document, \
                 patch.object(jobs, "safe_send_message", new=AsyncMock()) as send_message:
                asyncio.run(jobs._run_daily_job(context, trigger_source="startup_catchup", now_local=today))

            send_document.assert_awaited_once()
            send_message.assert_not_awaited()
            build_week_summary.assert_not_called()
            release_run.assert_not_called()
            complete_run.assert_called_once_with(
                ANY,
                job_name=jobs.MONTHLY_PDF_JOB_NAME,
                run_date=today.date(),
                sent_total=1,
                failed_total=0,
            )
        finally:
            Path(temp_file.name).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
