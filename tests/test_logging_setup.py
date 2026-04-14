import io
import json
import logging
import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "src" / "bot"))

from common.logging_setup import StructuredLogger, _JsonLineFormatter, relay_text_stream
import proxy_smoke
import xray_client.healthcheck as xray_healthcheck


@contextmanager
def captured_logger(name: str):
    raw_logger = logging.getLogger(f"tests.{name}")
    old_handlers = list(raw_logger.handlers)
    old_level = raw_logger.level
    old_propagate = raw_logger.propagate

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(_JsonLineFormatter())
    raw_logger.handlers = [handler]
    raw_logger.setLevel(logging.INFO)
    raw_logger.propagate = False

    try:
        with mock.patch.dict(os.environ, {"APP_SERVICE": "test_service", "APP_ENV": "test"}, clear=False):
            yield StructuredLogger(raw_logger), raw_logger, stream
    finally:
        raw_logger.handlers = old_handlers
        raw_logger.setLevel(old_level)
        raw_logger.propagate = old_propagate


def read_payloads(stream: io.StringIO) -> list[dict]:
    lines = [line for line in stream.getvalue().splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


class LoggingSetupTests(unittest.TestCase):
    def test_structured_logger_renders_json_payload(self):
        with captured_logger("structured") as (logger, _raw_logger, stream):
            logger.info(
                "sample_event",
                "Sample message.",
                {"count": 2},
                request_id="req-1",
            )

        payload = read_payloads(stream)[0]
        self.assertEqual(payload["service"], "test_service")
        self.assertEqual(payload["env"], "test")
        self.assertEqual(payload["event"], "sample_event")
        self.assertEqual(payload["msg"], "Sample message.")
        self.assertEqual(payload["ctx"]["count"], 2)
        self.assertEqual(payload["request_id"], "req-1")

    def test_structured_logger_redacts_sensitive_values(self):
        with captured_logger("redaction") as (logger, _raw_logger, stream):
            logger.info(
                "secret_event",
                "Bearer abc.def.ghi",
                {
                    "token": "12345",
                    "url": "https://api.telegram.org/bot123456:abcdefghijklmnopqrstuvwxyz/getMe",
                },
            )

        payload = read_payloads(stream)[0]
        self.assertEqual(payload["event"], "secret_event")
        self.assertIn("***REDACTED***", payload["msg"])
        self.assertEqual(payload["ctx"]["token"], "***REDACTED***")
        self.assertIn("***REDACTED***", payload["ctx"]["url"])

    def test_formatter_survives_malformed_stdlib_record(self):
        with captured_logger("malformed") as (_logger, raw_logger, stream):
            raw_logger.info("event_name", "hello world")

        payload = read_payloads(stream)[0]
        self.assertNotEqual(payload["event"], "logging_formatter_failed")
        self.assertEqual(payload["event"], "auto_log")
        self.assertEqual(payload["msg"], "event_name")
        self.assertEqual(payload["ctx"]["event_source"], "library")
        self.assertEqual(payload["ctx"]["logging_args"], ["hello world"])

    def test_relay_text_stream_preserves_stream_context(self):
        with captured_logger("relay") as (logger, _raw_logger, stream):
            thread = relay_text_stream(
                logger,
                io.StringIO("first line\nsecond line\n"),
                "xray_process_output",
                stream_name="stderr",
                ctx={"child_process": "xray"},
            )
            thread.join(timeout=1.0)

        payloads = read_payloads(stream)
        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0]["event"], "xray_process_output")
        self.assertEqual(payloads[0]["msg"], "first line")
        self.assertEqual(payloads[0]["ctx"]["stream"], "stderr")
        self.assertEqual(payloads[0]["ctx"]["child_process"], "xray")

    def test_proxy_smoke_emits_schema_compliant_json(self):
        smoke_results = [{"check": "proxy_mode", "ok": "true", "details": "disabled"}]

        with captured_logger("proxy_smoke") as (logger, _raw_logger, stream):
            with mock.patch.object(proxy_smoke, "logger", logger):
                with mock.patch.object(proxy_smoke, "collect_results", return_value=(0, smoke_results)):
                    exit_code = proxy_smoke.run_startup_smoke()

        self.assertEqual(exit_code, 0)
        payload = read_payloads(stream)[0]
        self.assertEqual(payload["event"], "bot_startup_smoke_completed")
        self.assertEqual(payload["ctx"]["exit_code"], 0)
        self.assertEqual(payload["ctx"]["results"], smoke_results)

    def test_xray_healthcheck_failure_emits_single_json_error(self):
        with captured_logger("healthcheck_failure") as (logger, _raw_logger, stream):
            with mock.patch.object(xray_healthcheck, "logger", logger):
                with mock.patch.object(xray_healthcheck.os.path, "exists", return_value=False):
                    exit_code = xray_healthcheck.main()

        self.assertEqual(exit_code, 1)
        payloads = read_payloads(stream)
        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["event"], "xray_healthcheck_status_missing")

    def test_xray_healthcheck_success_stays_quiet(self):
        with captured_logger("healthcheck_success") as (logger, _raw_logger, stream):
            with mock.patch.object(xray_healthcheck, "logger", logger):
                with mock.patch.object(xray_healthcheck.os.path, "exists", return_value=True):
                    with mock.patch.object(xray_healthcheck, "load_status", return_value={"mode": "disabled"}):
                        exit_code = xray_healthcheck.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(read_payloads(stream), [])


if __name__ == "__main__":
    unittest.main()
