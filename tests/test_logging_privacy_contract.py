import io
import json
import logging
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from common.logging_setup import StructuredLogger, _JsonLineFormatter  # noqa: E402


class LoggingPrivacyContractTests(unittest.TestCase):
    def test_private_fields_and_stable_ids_are_not_emitted(self):
        raw_logger = logging.getLogger("tests.logging_privacy")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(_JsonLineFormatter())
        previous = (list(raw_logger.handlers), raw_logger.level, raw_logger.propagate)
        raw_logger.handlers = [handler]
        raw_logger.setLevel(logging.INFO)
        raw_logger.propagate = False
        try:
            StructuredLogger(raw_logger).warning(
                "privacy_probe",
                "Safe summary.",
                {
                    "message_text": "private financial sentence",
                    "text_preview": "private preview",
                    "username": "private_username",
                    "user_id": 987654321,
                    "chat_id": -100123456789,
                    "response_body": {"arbitrary_private_field": "private upstream body"},
                    "error_message": "private upstream failure detail",
                    "error": "private exception detail",
                    "payload": {"private": "private arbitrary payload"},
                    "status_code": 400,
                },
            )
        finally:
            raw_logger.handlers, raw_logger.level, raw_logger.propagate = previous

        rendered = stream.getvalue()
        for forbidden in (
            "private financial sentence",
            "private preview",
            "private_username",
            "987654321",
            "100123456789",
            "private upstream body",
            "private upstream failure detail",
            "private exception detail",
            "private arbitrary payload",
        ):
            self.assertNotIn(forbidden, rendered)
        payload = json.loads(rendered)
        self.assertEqual(payload["ctx"]["status_code"], 400)

    def test_nested_financial_identifiers_are_redacted_but_operational_counts_remain(self):
        raw_logger = logging.getLogger("tests.logging_privacy.nested")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(_JsonLineFormatter())
        previous = (list(raw_logger.handlers), raw_logger.level, raw_logger.propagate)
        raw_logger.handlers = [handler]
        raw_logger.setLevel(logging.INFO)
        raw_logger.propagate = False
        try:
            StructuredLogger(raw_logger).info(
                "runtime_privacy_probe",
                "Safe runtime summary.",
                {
                    "status": "completed",
                    "processed_count": 3,
                    "sent_total": 2,
                    "rows": [
                        {
                            "account_id": "account-sensitive-001",
                            "account_name": "private portfolio name",
                            "operation_id": "operation-sensitive-002",
                            "income_event_id": "income-sensitive-003",
                            "figi": "FIGI-SENSITIVE-004",
                            "net_amount": "123456.78",
                            "position_value": "987654.32",
                            "current_value": "876543.21",
                            "quantity": "42",
                            "proxy_endpoint": "socks5h://private-user:private-pass@proxy.invalid:1080",
                            "nested": {"instrument_id": "instrument-sensitive-005"},
                        }
                    ],
                },
            )
        finally:
            raw_logger.handlers, raw_logger.level, raw_logger.propagate = previous

        rendered = stream.getvalue()
        for forbidden in (
            "account-sensitive-001",
            "private portfolio name",
            "operation-sensitive-002",
            "income-sensitive-003",
            "FIGI-SENSITIVE-004",
            "123456.78",
            "987654.32",
            "876543.21",
            "private-pass",
            "instrument-sensitive-005",
        ):
            self.assertNotIn(forbidden, rendered)
        payload = json.loads(rendered)
        self.assertEqual(payload["ctx"]["status"], "completed")
        self.assertEqual(payload["ctx"]["processed_count"], 3)
        self.assertEqual(payload["ctx"]["sent_total"], 2)


if __name__ == "__main__":
    unittest.main()
