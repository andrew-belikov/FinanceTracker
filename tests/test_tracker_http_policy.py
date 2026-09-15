import unittest
from datetime import datetime, timezone

from financetracker.tracker.http_policy import parse_retry_delay, retry_delay_seconds


class FakeResponse:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}


class RetryDelayPolicyTests(unittest.TestCase):
    def test_parses_numeric_retry_after_and_caps_it(self):
        self.assertEqual(
            parse_retry_delay("120", max_backoff_seconds=60.0),
            60.0,
        )

    def test_parses_http_date_against_supplied_clock(self):
        now = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)

        self.assertEqual(
            parse_retry_delay(
                "Tue, 15 Sep 2026 12:00:03 GMT",
                max_backoff_seconds=60.0,
                now=now,
            ),
            3.0,
        )

    def test_invalid_or_expired_values_fall_back_to_exponential_backoff(self):
        for value in ("invalid", "-1", "Tue, 15 Sep 2020 12:00:00 GMT"):
            with self.subTest(value=value):
                self.assertEqual(
                    retry_delay_seconds(
                        FakeResponse(503, {"Retry-After": value}),
                        3,
                        backoff_seconds=1.5,
                        max_backoff_seconds=60.0,
                    ),
                    6.0,
                )

    def test_429_uses_rate_limit_reset_when_retry_after_is_absent(self):
        self.assertEqual(
            retry_delay_seconds(
                FakeResponse(429, {"x-ratelimit-reset": "4"}),
                1,
                backoff_seconds=1.0,
                max_backoff_seconds=60.0,
            ),
            4.0,
        )
