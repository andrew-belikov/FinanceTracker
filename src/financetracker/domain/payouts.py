"""Pure payout amount calculations independent of persistence and presentation."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, ROUND_HALF_UP


def to_decimal(value: object) -> Decimal:
    return Decimal("0") if value is None else Decimal(str(value))


def estimate_net_payout_amount(amount: Decimal | None, tax_rate_pct: Decimal) -> Decimal | None:
    if amount is None:
        return None
    tax_factor = (Decimal("100") - tax_rate_pct) / Decimal("100")
    return (to_decimal(amount) * tax_factor).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def resolve_payout_amount(row: Mapping[str, object], tax_rate_pct: Decimal) -> tuple[Decimal | None, bool]:
    """Return the net payout and whether a prior coupon was used as an estimate."""
    gross_amount = row.get("expected_amount")
    estimated = False
    if gross_amount is None and row.get("event_type") == "coupon":
        previous_amount, quantity = row.get("previous_coupon_amount_per_unit"), row.get("quantity")
        if previous_amount is not None and quantity is not None:
            gross_amount = (to_decimal(previous_amount) * to_decimal(quantity)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            estimated = True
    normalized_amount = None if gross_amount is None else to_decimal(gross_amount)
    return estimate_net_payout_amount(normalized_amount, tax_rate_pct), estimated
