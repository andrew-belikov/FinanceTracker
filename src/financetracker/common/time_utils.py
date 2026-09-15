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


def utc_now() -> datetime:
    """Return a timezone-aware UTC instant."""
    return datetime.now(timezone.utc)


def local_civil_bounds_to_utc(
    start: date | datetime,
    end_exclusive: date | datetime,
    timezone_name: str | ZoneInfo,
) -> tuple[datetime, datetime]:
    """Convert a local civil ``[start, end)`` interval to timezone-aware UTC bounds."""
    zone = _zone(timezone_name)
    start_utc = _local_civil_datetime(start, zone).astimezone(timezone.utc)
    end_utc = _local_civil_datetime(end_exclusive, zone).astimezone(timezone.utc)
    if end_utc <= start_utc:
        raise ValueError("Local civil interval end must be after start")
    return start_utc, end_utc


def utc_to_local_date(
    value: datetime,
    timezone_name: str | ZoneInfo,
) -> date:
    """Map a timezone-aware UTC timestamp to its local civil date."""
    if value.tzinfo is None:
        # SQLite does not preserve tzinfo and this is also the explicit adapter
        # rule for rows that predate the TIMESTAMPTZ migration.
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).astimezone(_zone(timezone_name)).date()
