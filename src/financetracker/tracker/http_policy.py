"""Pure retry-delay policy for T-Invest HTTP calls."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Protocol


class RetryResponse(Protocol):
    """Minimal HTTP response contract used by the retry-delay policy."""

    headers: Mapping[str, str]
    status_code: int


def parse_retry_delay(value: str | None, *, max_backoff_seconds: float, now: datetime | None = None) -> float | None:
    if value is None or not (raw_value := value.strip()):
        return None
    try:
        delay = float(raw_value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        delay = (retry_at - (now or datetime.now(timezone.utc))).total_seconds()
    return None if delay < 0 else min(delay, max_backoff_seconds)


def retry_delay_seconds(
    response: RetryResponse,
    attempt: int,
    *,
    backoff_seconds: float,
    max_backoff_seconds: float,
) -> float:
    delay = parse_retry_delay(response.headers.get("Retry-After"), max_backoff_seconds=max_backoff_seconds)
    if delay is None and response.status_code == 429:
        delay = parse_retry_delay(response.headers.get("x-ratelimit-reset"), max_backoff_seconds=max_backoff_seconds)
    if delay is None:
        delay = backoff_seconds * (2 ** (attempt - 1))
    return min(max(0.0, delay), max_backoff_seconds)
