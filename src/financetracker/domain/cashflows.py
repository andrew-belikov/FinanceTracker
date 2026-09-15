"""Pure cashflow aggregation used by datasets and monthly reporting."""

from datetime import date
from decimal import Decimal


def _decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def sum_decimal_values_for_snapshot_interval(
    values_by_day: dict[date, Decimal],
    previous_snapshot_date: date | None,
    current_snapshot_date: date,
) -> Decimal:
    """Aggregate values over ``(previous_snapshot_date, current_snapshot_date]``."""
    if previous_snapshot_date is None:
        return _decimal(values_by_day.get(current_snapshot_date))
    return sum(
        (
            _decimal(value)
            for value_date, value in values_by_day.items()
            if previous_snapshot_date < value_date <= current_snapshot_date
        ),
        Decimal("0"),
    )


def normalize_operation_currency(value: object) -> str:
    currency = str(value or "").strip().upper()
    return currency or "UNKNOWN"


def add_operation_cashflow_by_currency_day(
    aggregates: dict[str, dict[str, dict[date, Decimal]]],
    *,
    currency: object,
    field: str,
    flow_date: date,
    amount: Decimal,
) -> None:
    currency_key = normalize_operation_currency(currency)
    values_by_day = aggregates.setdefault(currency_key, {}).setdefault(field, {})
    values_by_day[flow_date] = values_by_day.get(flow_date, Decimal("0")) + _decimal(amount)


def build_operation_cashflows_for_snapshot_interval(
    aggregates: dict[str, dict[str, dict[date, Decimal]]],
    previous_snapshot_date: date | None,
    current_snapshot_date: date,
) -> list[dict[str, object]]:
    fields = (
        "deposits", "withdrawals", "iis_tax_deduction_income", "commissions",
        "operation_taxes", "operation_tax_refunds",
    )
    result: list[dict[str, object]] = []
    for currency in sorted(aggregates):
        currency_values = aggregates[currency]
        fact: dict[str, object] = {"currency": currency}
        for field in fields:
            fact[field] = sum_decimal_values_for_snapshot_interval(
                currency_values.get(field, {}), previous_snapshot_date, current_snapshot_date
            )
        fact["net_external_flow"] = _decimal(fact["deposits"]) - _decimal(fact["withdrawals"])
        if any(_decimal(fact[field]) != 0 for field in fields):
            result.append(fact)
    return result


def rebase_twr_to_period(
    dates: list[date], twr: list[float], period_start: date, period_end_exclusive: date
) -> dict[date, float]:
    points = [(item_date, item_return) for item_date, item_return in zip(dates, twr) if item_date < period_end_exclusive]
    period_points = [(item_date, item_return) for item_date, item_return in points if item_date >= period_start]
    if not period_points:
        return {}
    pre_period_returns = [item_return for item_date, item_return in points if item_date < period_start]
    base_multiplier = 1.0 + (pre_period_returns[-1] if pre_period_returns else period_points[0][1])
    if base_multiplier == 0:
        return {}
    return {item_date: (1.0 + item_return) / base_multiplier - 1.0 for item_date, item_return in period_points}
