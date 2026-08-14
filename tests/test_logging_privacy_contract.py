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
                    "income_events_count": 4,
                    "deposit_count": 1,
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
        self.assertEqual(payload["ctx"]["income_events_count"], 4)
        self.assertEqual(payload["ctx"]["deposit_count"], 1)
        self.assertEqual(payload["ctx"]["sent_total"], 2)

    def test_counter_contract_rejects_deceptive_names_types_and_bounds(self):
        raw_logger = logging.getLogger("tests.logging_privacy.counter_contract")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(_JsonLineFormatter())
        previous = (list(raw_logger.handlers), raw_logger.level, raw_logger.propagate)
        raw_logger.handlers = [handler]
        raw_logger.setLevel(logging.INFO)
        raw_logger.propagate = False
        try:
            StructuredLogger(raw_logger).info(
                "counter_privacy_probe",
                "Safe counter summary.",
                {
                    "count": 2,
                    "commands_count": 3,
                    "income_events_count": 4,
                    "deposit_count": 5,
                    "sent_total": 6,
                    "failed_total": 0,
                    "applied_total": 7,
                    "processed_count": 8,
                    "deposit_total": 700001,
                    "income_total": 700002,
                    "tax_total": 700003,
                    "amount_total": 700004,
                    "portfolio_value_total": 700005,
                    "string_count": "700006",
                    "float_count": 700007.0,
                    "boolean_count": True,
                    "negative_count": -1,
                    "oversized_count": 1_000_000_001,
                    "account_id_count": 700008,
                    "token_count": 700009,
                    "amount_count": 700010,
                    "balance_count": 700011,
                    "portfolio_value_count": 700012,
                    "tax_count": 700013,
                    "rows_count": 700014,
                    "unknown_count": 700015,
                    "income_count": 700016,
                    "deposit_events_count": 700017,
                    "total": 700010,
                    "unknown_total": 700008,
                },
            )
        finally:
            raw_logger.handlers, raw_logger.level, raw_logger.propagate = previous

        ctx = json.loads(stream.getvalue())["ctx"]
        for key, expected in {
            "count": 2,
            "commands_count": 3,
            "income_events_count": 4,
            "deposit_count": 5,
            "sent_total": 6,
            "failed_total": 0,
            "applied_total": 7,
            "processed_count": 8,
        }.items():
            self.assertEqual(ctx[key], expected)
        for key in (
            "deposit_total",
            "income_total",
            "tax_total",
            "amount_total",
            "portfolio_value_total",
            "string_count",
            "float_count",
            "boolean_count",
            "negative_count",
            "oversized_count",
            "account_id_count",
            "token_count",
            "amount_count",
            "balance_count",
            "portfolio_value_count",
            "tax_count",
            "rows_count",
            "unknown_count",
            "income_count",
            "deposit_events_count",
            "total",
            "unknown_total",
        ):
            self.assertEqual(ctx[key], "***REDACTED***")


if __name__ == "__main__":
    unittest.main()
