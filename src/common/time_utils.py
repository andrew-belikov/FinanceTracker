from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo


def _zone(value: str | ZoneInfo) -> ZoneInfo:
    return value if isinstance(value, ZoneInfo) else ZoneInfo(value)


def _local_civil_datetime(value: date | datetime, zone: ZoneInfo) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=zone)
        return value.astimezone(zone)
    return datetime.combine(value, time.min, tzinfo=zone)


def local_civil_bounds_to_utc_naive(
    start: date | datetime,
    end_exclusive: date | datetime,
    timezone_name: str | ZoneInfo,
) -> tuple[datetime, datetime]:
    """Convert a local civil ``[start, end)`` interval to UTC-naive storage bounds."""
    zone = _zone(timezone_name)
    start_utc = _local_civil_datetime(start, zone).astimezone(timezone.utc)
    end_utc = _local_civil_datetime(end_exclusive, zone).astimezone(timezone.utc)
    if end_utc <= start_utc:
        raise ValueError("Local civil interval end must be after start")
    return start_utc.replace(tzinfo=None), end_utc.replace(tzinfo=None)


def utc_naive_to_local_date(
    value: datetime,
    timezone_name: str | ZoneInfo,
) -> date:
    """Map a UTC-naive storage timestamp to its local civil date."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.astimezone(_zone(timezone_name)).date()
