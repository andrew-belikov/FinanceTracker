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


if __name__ == "__main__":
    unittest.main()
