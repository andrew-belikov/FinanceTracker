from __future__ import annotations

from decimal import Decimal, InvalidOperation


def annualize_simple_yield_pct(
    period_yield_pct: Decimal | float | int | str | None,
    period_days: int | None,
) -> Decimal | None:
    """Return an ACT/365F simple annual equivalent without compounding."""
    if period_yield_pct is None or period_days is None or period_days <= 0:
        return None

    try:
        value = Decimal(str(period_yield_pct))
    except (InvalidOperation, ValueError):
        return None
    if not value.is_finite() or value <= 0:
        return None

    return value * Decimal("365") / Decimal(period_days)
