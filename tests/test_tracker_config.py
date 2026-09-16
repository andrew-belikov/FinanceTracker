import os
import unittest
from unittest import mock

from financetracker.config.tracker import read_tracker_http_settings


class TrackerHttpSettingsTests(unittest.TestCase):
    def test_defaults_preserve_tracker_http_contract(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            settings = read_tracker_http_settings()

        self.assertEqual(settings.base_url, "https://invest-public-api.tbank.ru/rest")
        self.assertEqual(settings.timeout_seconds, 20.0)
        self.assertEqual(settings.retry_total, 3)
        self.assertEqual(settings.instrument_cache_ttl_seconds, 86400.0)

    def test_normalizes_negative_retry_and_cache_limits(self):
        with mock.patch.dict(
            os.environ,
            {
                "TINVEST_HTTP_RETRY_TOTAL": "-1",
                "TINVEST_HTTP_BACKOFF_SECONDS": "-2",
                "TINVEST_HTTP_POOL_CONNECTIONS": "0",
                "TINVEST_INSTRUMENT_CACHE_MAX_ENTRIES": "-1",
            },
            clear=True,
        ):
            settings = read_tracker_http_settings()

        self.assertEqual(settings.retry_total, 0)
        self.assertEqual(settings.backoff_seconds, 0.0)
        self.assertEqual(settings.pool_connections, 1)
        self.assertEqual(settings.instrument_cache_max_entries, 0)
