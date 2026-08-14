import http.client
import json
import socket
import sys
import threading
import time
import unittest
from pathlib import Path
from typing import Dict, Optional
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

import report_server
from report_server import build_reporter_server


class ReporterServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = build_reporter_server(
            host="127.0.0.1",
            port=0,
            service_key="synthetic-service-key",
            max_concurrent_requests=1,
            socket_timeout_seconds=0.2,
            request_timeout_seconds=1.0,
        )
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self.thread.start()
        self._wait_until_ready()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2.0)

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.05)
        self.fail("reporter server did not start in time")

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2.0)
        conn.request(method, path, body=body, headers=headers or {})
        response = conn.getresponse()
        payload = response.read()
        content_type = response.getheader("Content-Type") or ""
        content_disposition = response.getheader("Content-Disposition")
        conn.close()
        if "application/json" in content_type:
            return response.status, json.loads(payload.decode("utf-8")), content_type, content_disposition
        return response.status, payload, content_type, content_disposition

    def test_healthz_returns_ok(self):
        status, payload, _content_type, _disposition = self._request("GET", "/healthz")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "reporter")
        self.assertEqual(payload["pdf_engine"], "weasyprint")

    def test_missing_service_key_fails_before_socket_bind(self):
        with mock.patch.object(report_server.ThreadingHTTPServer, "__init__") as server_init:
            with self.assertRaises(ValueError):
                build_reporter_server(
                    host="127.0.0.1",
                    port=0,
                    service_key="",
                    max_concurrent_requests=1,
                    socket_timeout_seconds=1,
                    request_timeout_seconds=1,
                )
        server_init.assert_not_called()

    def test_monthly_pdf_returns_pdf_response(self):
        with mock.patch.object(
            report_server,
            "MONTHLY_REPORT_BUILDER",
            return_value={
                "filename": "fintracker_monthly_2026-04.pdf",
                "period": "2026-04",
                "pdf_bytes": b"%PDF-test",
            },
        ):
            status, payload, content_type, disposition = self._request(
                "POST",
                "/reports/monthly/pdf",
                body=json.dumps({"year": 2026, "month": 4}).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "X-Reporter-Service-Key": "synthetic-service-key",
                },
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, b"%PDF-test")
        self.assertEqual(content_type, "application/pdf")
        self.assertEqual(disposition, 'attachment; filename="fintracker_monthly_2026-04.pdf"')

    def test_missing_and_wrong_key_rejected_before_body_or_builder(self):
        with mock.patch.object(report_server.ReporterRequestHandler, "_read_json_body") as read_body, \
             mock.patch.object(report_server, "MONTHLY_REPORT_BUILDER") as builder:
            missing = self._request("POST", "/reports/monthly/pdf", body=b"ignored")
            wrong = self._request(
                "POST",
                "/reports/monthly/pdf",
                body=b"ignored",
                headers={"X-Reporter-Service-Key": "wrong"},
            )
        self.assertEqual(missing[0], 401)
        self.assertEqual(wrong[0], 403)
        read_body.assert_not_called()
        builder.assert_not_called()

    def test_monthly_pdf_stub_rejects_non_object_json(self):
        status, payload, _content_type, _disposition = self._request(
            "POST",
            "/reports/monthly/pdf",
            body=json.dumps(["bad"]).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Reporter-Service-Key": "synthetic-service-key",
            },
        )

        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_request")

    def test_concurrency_budget_rejects_overload_and_bounds_builder(self):
        entered = threading.Event()
        release = threading.Event()
        active = 0
        maximum = 0
        lock = threading.Lock()

        def builder(_request):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
            entered.set()
            release.wait(timeout=2)
            with lock:
                active -= 1
            return {"filename": "report.pdf", "period": "2026-04", "pdf_bytes": b"%PDF"}

        first_result = []
        headers = {
            "Content-Type": "application/json",
            "X-Reporter-Service-Key": "synthetic-service-key",
        }
        with mock.patch.object(report_server, "MONTHLY_REPORT_BUILDER", side_effect=builder):
            thread = threading.Thread(
                target=lambda: first_result.append(
                    self._request("POST", "/reports/monthly/pdf", body=b"{}", headers=headers)
                )
            )
            thread.start()
            self.assertTrue(entered.wait(timeout=1))
            overloaded = self._request("POST", "/reports/monthly/pdf", body=b"{}", headers=headers)
            release.set()
            thread.join(timeout=2)
        self.assertEqual(overloaded[0], 503)
        self.assertEqual(overloaded[1]["error"], "reporter_overloaded")
        self.assertEqual(first_result[0][0], 200)
        self.assertEqual(maximum, 1)

    def test_request_deadline_returns_service_unavailable(self):
        self.server.request_timeout_seconds = 0.02
        release = threading.Event()

        def builder(_request):
            release.wait(timeout=1)
            return {"filename": "report.pdf", "period": "2026-04", "pdf_bytes": b"%PDF"}

        try:
            with mock.patch.object(report_server, "MONTHLY_REPORT_BUILDER", side_effect=builder):
                result = self._request(
                    "POST",
                    "/reports/monthly/pdf",
                    body=b"{}",
                    headers={
                        "Content-Type": "application/json",
                        "X-Reporter-Service-Key": "synthetic-service-key",
                    },
                )
            self.assertEqual(result[0], 503)
            self.assertEqual(result[1]["error"], "report_timeout")
        finally:
            release.set()

    def test_slow_body_timeout_releases_concurrency_budget(self):
        headers = {
            "Content-Type": "application/json",
            "X-Reporter-Service-Key": "synthetic-service-key",
        }
        artifact = {"filename": "report.pdf", "period": "2026-04", "pdf_bytes": b"%PDF"}
        slow = socket.create_connection(("127.0.0.1", self.port), timeout=1)
        slow.sendall(
            b"POST /reports/monthly/pdf HTTP/1.1\r\n"
            b"Host: reporter\r\n"
            b"X-Reporter-Service-Key: synthetic-service-key\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 10\r\n\r\n{"
        )
        time.sleep(0.03)
        overloaded = self._request(
            "POST", "/reports/monthly/pdf", body=b"{}", headers=headers
        )
        time.sleep(0.25)
        with mock.patch.object(report_server, "MONTHLY_REPORT_BUILDER", return_value=artifact):
            recovered = self._request(
                "POST", "/reports/monthly/pdf", body=b"{}", headers=headers
            )
        slow.close()
        self.assertEqual(overloaded[0], 503)
        self.assertEqual(recovered[0], 200)

    def test_unknown_path_returns_not_found(self):
        status, payload, _content_type, _disposition = self._request("GET", "/unknown")

        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "not_found")


if __name__ == "__main__":
    unittest.main()
