"""Reporter-owned settings and presentation primitives.

This module deliberately has no Telegram dependency.  It is the composition
root for the report service's read-only database access.
"""

import os
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import lru_cache
from zoneinfo import ZoneInfo

from financetracker.common.time_utils import local_civil_bounds_to_utc, utc_to_local_date
from financetracker.config.database import read_database_settings
from financetracker.database.session import create_session_factory


ACCOUNT_FRIENDLY_NAME = os.getenv("ACCOUNT_FRIENDLY_NAME", "Семейный капитал")
PLAN_ANNUAL_CONTRIB_RUB = float(os.getenv("PLAN_ANNUAL_CONTRIB_RUB", "400000"))
IIS_TAX_DEDUCTION_CATEGORY = "iis_tax_deduction"
REPORTING_ACCOUNT_UNAVAILABLE_TEXT = (
    "Не удалось определить активный счёт для отчёта. "
    "Укажите корректный TINKOFF_ACCOUNT_ID или дождитесь первого снапшота."
)
TZ_NAME = os.getenv("TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
TZ = ZoneInfo(TZ_NAME)
MONTHS_RU = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель", 5: "май", 6: "июнь",
    7: "июль", 8: "август", 9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}

DB_DSN = read_database_settings().dsn
@lru_cache(maxsize=1)
def _session_local():
    """Construct DB access only when the report use case actually needs it."""
    _engine, session_local = create_session_factory(DB_DSN)
    return session_local


@contextmanager
def db_session():
    session = _session_local()()
    try:
        yield session
    finally:
        session.close()


def to_local_market_date(dt: datetime | None) -> date | None:
    return utc_to_local_date(dt, TZ) if dt is not None else None


def local_reporting_bounds_utc(start: date | datetime, end_exclusive: date | datetime) -> tuple[datetime, datetime]:
    return local_civil_bounds_to_utc(start, end_exclusive, TZ)


def to_iso_datetime(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return (dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)).isoformat()


def decimal_to_str(value: Decimal | float | int | None) -> str | None:
    return None if value is None else format(value if isinstance(value, Decimal) else Decimal(str(value)), "f")


def normalize_decimal(value: object) -> Decimal:
    return Decimal("0") if value is None else Decimal(str(value))


def fmt_decimal_rub(value: Decimal | float | int | None, precision: int = 2) -> str:
    return f"{float(value or 0):,.{precision}f} ₽".replace(",", " ")


def fmt_pct(value: float | None, precision: int = 2) -> str:
    return "—" if value is None else f"{value:+.{precision}f} %"
