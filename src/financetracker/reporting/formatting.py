"""Display-only formatting shared by reporting narrative, charts and HTML."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from financetracker.reporting.runtime import fmt_decimal_rub, fmt_pct


def to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def display_rub(value: Any, *, precision: int = 0) -> str:
    if value is None:
        return "—"
    return fmt_decimal_rub(value, precision=precision)


def display_pct(value: Any, *, precision: int = 2) -> str:
    if value is None:
        return "—"
    return fmt_pct(float(to_decimal(value)), precision=precision)


def display_pct_compact(value: Any, *, precision: int = 1) -> str:
    if value is None:
        return "—"
    quantizer = Decimal("1") if precision == 0 else Decimal(f"1.{'0' * precision}")
    decimal_value = to_decimal(value).quantize(quantizer)
    return f"{format(decimal_value, f'.{precision}f').replace('.', ',')}%"


def display_date(value: str | None) -> str:
    return datetime.fromisoformat(value).strftime("%d.%m.%Y") if value else "—"


def display_day(value: str | None) -> str:
    return datetime.fromisoformat(value).strftime("%d.%m") if value else "—"


def display_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "—"
    dt_value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt_value.strftime("%d.%m.%Y %H:%M %Z").strip() if dt_value.tzinfo else dt_value.strftime("%d.%m.%Y %H:%M")
