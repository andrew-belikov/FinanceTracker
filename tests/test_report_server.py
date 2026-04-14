import http.client
import json
import socket
import sys
import threading
import time
import unittest
from pathlib import Path
from typing import Dict, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

from report_server import build_reporter_server


class ReporterServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = build_reporter_server(host="127.0.0.1", port=0)
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
        payload = response.read().decode("utf-8")
        conn.close()
        return response.status, json.loads(payload)

    def test_healthz_returns_ok(self):
        status, payload = self._request("GET", "/healthz")

        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["service"], "reporter")
        self.assertEqual(payload["pdf_engine"], "placeholder")

    def test_monthly_pdf_stub_returns_not_implemented(self):
        status, payload = self._request(
            "POST",
            "/reports/monthly/pdf",
            body=json.dumps({"year": 2026, "month": 4}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(status, 501)
        self.assertEqual(payload["status"], "not_implemented")
        self.assertEqual(payload["report_kind"], "monthly_pdf")
        self.assertEqual(payload["period"], "2026-04")
        self.assertEqual(payload["request_keys"], ["month", "year"])

    def test_monthly_pdf_stub_rejects_non_object_json(self):
        status, payload = self._request(
            "POST",
            "/reports/monthly/pdf",
            body=json.dumps(["bad"]).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )

        self.assertEqual(status, 400)
        self.assertEqual(payload["error"], "invalid_request")

    def test_unknown_path_returns_not_found(self):
        status, payload = self._request("GET", "/unknown")

        self.assertEqual(status, 404)
        self.assertEqual(payload["error"], "not_found")


if __name__ == "__main__":
    unittest.main()
