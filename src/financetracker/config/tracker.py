"""Tracker settings that can be parsed without creating runtime clients."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class TrackerHttpSettings:
    api_token: str
    base_url: str
    account_status: str
    portfolio_currency: str
    timeout_seconds: float
    retry_total: int
    backoff_seconds: float
    max_backoff_seconds: float
    pool_connections: int
    pool_maxsize: int
    instrument_cache_ttl_seconds: float
    instrument_cache_max_entries: int


def read_tracker_http_settings() -> TrackerHttpSettings:
    return TrackerHttpSettings(
        api_token=os.getenv("TINVEST_API_TOKEN", "").strip(),
        base_url=os.getenv("TINVEST_BASE_URL", "https://invest-public-api.tbank.ru/rest"),
        account_status=os.getenv("TINVEST_ACCOUNT_STATUS", "ACCOUNT_STATUS_ALL"),
        portfolio_currency=os.getenv("TINVEST_PORTFOLIO_CURRENCY", "RUB"),
        timeout_seconds=float(os.getenv("TINVEST_HTTP_TIMEOUT_SECONDS", "20")),
        retry_total=max(0, int(os.getenv("TINVEST_HTTP_RETRY_TOTAL", "3"))),
        backoff_seconds=max(0.0, float(os.getenv("TINVEST_HTTP_BACKOFF_SECONDS", "1"))),
        max_backoff_seconds=max(0.0, float(os.getenv("TINVEST_HTTP_MAX_BACKOFF_SECONDS", "60"))),
        pool_connections=max(1, int(os.getenv("TINVEST_HTTP_POOL_CONNECTIONS", "8"))),
        pool_maxsize=max(1, int(os.getenv("TINVEST_HTTP_POOL_MAXSIZE", "8"))),
        instrument_cache_ttl_seconds=max(
            0.0, float(os.getenv("TINVEST_INSTRUMENT_CACHE_TTL_SECONDS", "86400"))
        ),
        instrument_cache_max_entries=max(
            0, int(os.getenv("TINVEST_INSTRUMENT_CACHE_MAX_ENTRIES", "1024"))
        ),
    )
