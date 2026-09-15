"""Pure return and valuation calculations for portfolio reporting."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal


def compute_period_delta_excluding_external_flow(
    start_value: float | None,
    end_value: float | None,
    net_external_flow: float,
) -> tuple[float | None, float | None]:
    if start_value is None or start_value == 0 or end_value is None:
        return None, None
    delta_abs = end_value - start_value - net_external_flow
    return delta_abs, delta_abs / start_value * 100.0


def compute_cost_basis_pnl_pct(
    current_value: Decimal | float | int,
    pnl: Decimal | float | int,
) -> float | None:
    value = float(current_value)
    pnl_value = float(pnl)
    cost_basis = value - pnl_value
    if cost_basis <= 0:
        return None
    return pnl_value / cost_basis * 100.0


def compute_twr_series(
    snapshot_rows: list[dict], net_external_flow_by_day: dict[date, float]
) -> tuple[list[date], list[float | None], list[float]] | None:
    if len(snapshot_rows) < 2:
        return None
    dates = [row["snapshot_date"] for row in snapshot_rows]
    values = [float(row["total_value"]) if row.get("total_value") is not None else None for row in snapshot_rows]
    cumulative_multiplier = 1.0
    twr: list[float] = [0.0]
    for index in range(1, len(dates)):
        previous_value, current_value = values[index - 1], values[index]
        previous_date, current_date = dates[index - 1], dates[index]
        net_external_flow = sum(
            flow for flow_date, flow in net_external_flow_by_day.items()
            if previous_date < flow_date <= current_date
        )
        if previous_value is None or previous_value == 0 or current_value is None:
            twr.append(cumulative_multiplier - 1.0)
            continue
        cumulative_multiplier *= 1.0 + ((current_value - net_external_flow) / previous_value - 1.0)
        twr.append(cumulative_multiplier - 1.0)
    return dates, values, twr


def compute_xnpv(rate: float, cashflows: list[tuple[datetime, float]]) -> float:
    base_dt = cashflows[0][0]
    return float(
        sum(
            amount / ((1.0 + rate) ** ((dt - base_dt).total_seconds() / (365.0 * 24 * 3600)))
            for dt, amount in cashflows
        )
    )


def compute_xirr(cashflows: list[tuple[datetime, float]]) -> float | None:
    if not cashflows or not any(amount < 0 for _, amount in cashflows) or not any(amount > 0 for _, amount in cashflows):
        return None
    low, high = -0.9999, 10.0
    low_value, high_value = compute_xnpv(low, cashflows), compute_xnpv(high, cashflows)
    for _ in range(20):
        if low_value * high_value <= 0:
            break
        high *= 2
        high_value = compute_xnpv(high, cashflows)
    if low_value * high_value > 0:
        return None
    for _ in range(200):
        mid = (low + high) / 2.0
        mid_value = compute_xnpv(mid, cashflows)
        if abs(mid_value) < 1e-8:
            return mid
        if low_value * mid_value <= 0:
            high, high_value = mid, mid_value
        else:
            low, low_value = mid, mid_value
    return (low + high) / 2.0


def project_run_rate_value(
    current_value: float | None, annual_rate: float | None, from_date: date, to_date: date
) -> float | None:
    if current_value is None or annual_rate is None:
        return None
    day_count = (to_date - from_date).days
    if day_count < 0:
        return None
    if day_count == 0:
        return current_value
    return float(current_value * ((1.0 + annual_rate) ** (day_count / 365.0)))
