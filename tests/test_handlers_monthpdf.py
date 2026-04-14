import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import handlers  # noqa: E402
from report_client import ReporterClientError  # noqa: E402


class FakeStatusMessage:
    def __init__(self):
        self.delete = AsyncMock()


class FakeMessage:
    def __init__(self):
        self._status_message = FakeStatusMessage()
        self.reply_text = AsyncMock(return_value=self._status_message)


class MonthPdfHandlerTests(unittest.TestCase):
    def _build_context(self, args=None):
        return SimpleNamespace(args=args or [], bot=object())

    def _build_update(self):
        message = FakeMessage()
        return SimpleNamespace(
            message=message,
            effective_message=message,
            effective_chat=SimpleNamespace(id=123),
        )

    def test_cmd_monthpdf_requests_report_and_sends_document(self):
        update = self._build_update()
        context = self._build_context()
        temp_file = tempfile.NamedTemporaryFile(prefix="monthpdf_", suffix=".pdf", delete=False)
        temp_file.write(b"%PDF-test")
        temp_file.close()

        try:
            with patch.object(handlers, "is_authorized", return_value=True), \
                 patch.object(handlers, "log_update_received"), \
                 patch.object(handlers, "request_monthly_report_pdf", return_value=(temp_file.name, "report.pdf")), \
                 patch.object(handlers, "safe_send_document", new=AsyncMock()) as send_document:
                asyncio.run(handlers.cmd_monthpdf(update, context))

            update.message.reply_text.assert_awaited()
            send_document.assert_awaited_once()
            self.assertFalse(Path(temp_file.name).exists())
            update.message._status_message.delete.assert_awaited_once()
        finally:
            Path(temp_file.name).unlink(missing_ok=True)

    def test_cmd_monthpdf_rejects_bad_args(self):
        update = self._build_update()
        context = self._build_context(args=["bad"])

        with patch.object(handlers, "is_authorized", return_value=True), \
             patch.object(handlers, "log_update_received"), \
             patch.object(handlers, "request_monthly_report_pdf") as request_report:
            asyncio.run(handlers.cmd_monthpdf(update, context))

        request_report.assert_not_called()
        update.message.reply_text.assert_awaited_once_with("Формат: /monthpdf или /monthpdf YYYY MM")

    def test_cmd_monthpdf_surfaces_reporter_error(self):
        update = self._build_update()
        context = self._build_context()

        with patch.object(handlers, "is_authorized", return_value=True), \
             patch.object(handlers, "log_update_received"), \
             patch.object(
                 handlers,
                 "request_monthly_report_pdf",
                 side_effect=ReporterClientError("Reporter временно недоступен."),
             ), \
             patch.object(handlers, "safe_send_document", new=AsyncMock()):
            asyncio.run(handlers.cmd_monthpdf(update, context))

        self.assertEqual(update.message.reply_text.await_count, 2)
        update.message.reply_text.assert_any_await("Reporter временно недоступен.")


if __name__ == "__main__":
    unittest.main()
