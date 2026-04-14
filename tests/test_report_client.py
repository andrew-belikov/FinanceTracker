import io
import json
import sys
import unittest
from pathlib import Path
from urllib import error
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_client  # noqa: E402


class FakeResponse:
    def __init__(self, body: bytes, headers: dict[str, str]):
        self._body = body
        self.headers = headers

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class ReportClientTests(unittest.TestCase):
    def test_request_monthly_report_pdf_saves_pdf_to_temp_file(self):
        response = FakeResponse(
            b"%PDF-test",
            {
                "Content-Type": "application/pdf",
                "Content-Disposition": 'attachment; filename="fintracker_monthly_2026-04.pdf"',
            },
        )
        opener = mock.Mock()
        opener.open.return_value = response

        with mock.patch.object(report_client.request, "build_opener", return_value=opener) as build_opener:
            path, filename = report_client.request_monthly_report_pdf(year=2026, month=4)

        try:
            self.assertEqual(filename, "fintracker_monthly_2026-04.pdf")
            self.assertEqual(Path(path).read_bytes(), b"%PDF-test")
            proxy_handler = build_opener.call_args.args[0]
            self.assertEqual(proxy_handler.proxies, {})
            opener.open.assert_called_once()
        finally:
            Path(path).unlink(missing_ok=True)

    def test_request_monthly_report_pdf_surfaces_http_json_error(self):
        http_error = error.HTTPError(
            url="http://reporter/reports/monthly/pdf",
            code=400,
            msg="Bad Request",
            hdrs=None,
            fp=io.BytesIO(json.dumps({"message": "Нет данных для отчёта."}).encode("utf-8")),
        )
        opener = mock.Mock()
        opener.open.side_effect = http_error

        with mock.patch.object(report_client.request, "build_opener", return_value=opener):
            with self.assertRaises(report_client.ReporterClientError) as exc_info:
                report_client.request_monthly_report_pdf(year=2026, month=4)

        self.assertEqual(str(exc_info.exception), "Нет данных для отчёта.")

    def test_request_monthly_report_pdf_bypasses_proxy_env_for_internal_reporter(self):
        response = FakeResponse(
            b"%PDF-test",
            {
                "Content-Type": "application/pdf",
                "Content-Disposition": 'attachment; filename="fintracker_monthly_2026-04.pdf"',
            },
        )
        opener = mock.Mock()
        opener.open.return_value = response

        with mock.patch.dict(
            report_client.os.environ,
            {
                "HTTP_PROXY": "socks5h://xray-client:1080",
                "HTTPS_PROXY": "socks5h://xray-client:1080",
                "ALL_PROXY": "socks5h://xray-client:1080",
            },
            clear=False,
        ):
            with mock.patch.object(report_client.request, "build_opener", return_value=opener) as build_opener:
                path, _filename = report_client.request_monthly_report_pdf(year=2026, month=4)

        try:
            proxy_handler = build_opener.call_args.args[0]
            self.assertEqual(proxy_handler.proxies, {})
            opener.open.assert_called_once()
            self.assertEqual(Path(path).read_bytes(), b"%PDF-test")
        finally:
            Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
