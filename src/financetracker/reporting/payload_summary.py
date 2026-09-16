"""Reconciliation, rebalance and summary metrics for monthly reports."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from financetracker.domain.assets import build_reconciliation_by_asset_type
from financetracker.domain.rebalance import (
    aggregate_rebalance_values_by_class,
    compute_rebalance_plan,
)
from financetracker.reporting.payload_timeseries import (
    compute_period_pnl,
    find_best_and_worst_day,
)
from financetracker.reporting.runtime import TZ, normalize_decimal


def build_reconciliation_rows(
    latest_snapshot: dict[str, Any] | None,
    end_positions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], Decimal, Decimal]:
    if latest_snapshot is None:
        return [], Decimal("0"), Decimal("0")

    reconciliation_rows, positions_value_sum, reconciliation_gap_abs = build_reconciliation_by_asset_type(
        latest_snapshot,
        end_positions,
    )
    return [
        {
            "instrument_type": row.get("asset_type"),
            "positions_value_sum": normalize_decimal(row.get("positions_sum")),
            "snapshot_total": normalize_decimal(row.get("snapshot_total")),
            "delta_abs": normalize_decimal(row.get("gap_abs")),
        }
        for row in reconciliation_rows
    ], positions_value_sum, reconciliation_gap_abs


def build_rebalance_snapshot(
    snapshot_date: date | None,
    end_positions: list[dict[str, Any]],
    targets: dict[str, Decimal] | None,
) -> dict[str, Any]:
    if not targets:
        return {
            "snapshot_date": snapshot_date,
            "rebalanceable_base": Decimal("0"),
            "rows": [],
            "other_groups": [],
        }

    class_values, other_groups = aggregate_rebalance_values_by_class(end_positions)
    plan = compute_rebalance_plan(class_values, targets)
    return {
        "snapshot_date": snapshot_date,
        "rebalanceable_base": normalize_decimal(plan["rebalanceable_base"]),
        "rows": [
            {
                "asset_class": row["asset_class"],
                "label": row["label"],
                "current_value": normalize_decimal(row["current_value"]),
                "current_pct": normalize_decimal(row["current_pct"]),
                "target_pct": normalize_decimal(row["target_pct"]),
                "delta_pct": normalize_decimal(row["delta_pct"]),
                "target_value": normalize_decimal(row["target_value"]),
                "delta_value": normalize_decimal(row["delta_value"]),
                "status": row["status"],
            }
            for row in plan["rows"]
        ],
        "other_groups": [
            {
                "label": label,
                "value": normalize_decimal(value),
            }
            for label, value in sorted(other_groups.items(), key=lambda item: item[1], reverse=True)
        ],
    }


def build_summary_metrics(
    *,
    year: int,
    period_end: date,
    end_snapshot: dict[str, Any] | None,
    daily_rows: list[dict[str, Any]],
    positions_current: list[dict[str, Any]],
    deposits: Decimal,
    withdrawals: Decimal,
    income_net: Decimal,
    iis_tax_deduction_income: Decimal,
    coupon_net: Decimal,
    dividend_net: Decimal,
    commissions: Decimal,
    taxes: Decimal,
    tax_refunds: Decimal,
    deposits_ytd: Decimal,
    plan_annual_contrib: Decimal,
    reconciliation_gap_abs: Decimal,
    positions_value_sum: Decimal,
    income_events_count: int,
    start_snapshot: dict[str, Any] | None,
    start_value: Decimal | None,
    net_external_flow: Decimal,
    period_twr_pct: Decimal | None,
) -> dict[str, Any]:
    end_value = normalize_decimal(end_snapshot.get("total_value")) if end_snapshot is not None else None
    current_value = end_value
    period_pnl_abs, period_pnl_pct = compute_period_pnl(
        start_snapshot=start_snapshot,
        end_value=end_value,
        start_value=start_value,
        net_external_flow=net_external_flow,
        daily_rows=daily_rows,
    )

    top_holding = positions_current[0] if positions_current else None
    best_day, worst_day = find_best_and_worst_day(daily_rows)

    reference_today = min(datetime.now(TZ).date(), period_end)
    year_start = date(year, 1, 1)
    next_year_start = date(year + 1, 1, 1)
    days_in_year = (next_year_start - year_start).days or 1
    days_passed = (reference_today - year_start).days + 1
    target_to_date = plan_annual_contrib * Decimal(days_passed) / Decimal(days_in_year)
    plan_progress_pct = Decimal("0")
    if plan_annual_contrib > 0:
        plan_progress_pct = deposits_ytd * Decimal("100") / plan_annual_contrib

    return {
        "start_value": start_value,
        "end_value": end_value,
        "current_value": current_value,
        "period_pnl_abs": period_pnl_abs,
        "period_pnl_pct": period_pnl_pct,
        "period_twr_pct": period_twr_pct,
        "net_external_flow": net_external_flow,
        "deposits": deposits,
        "withdrawals": withdrawals,
        "income_net": income_net,
        "iis_tax_deduction_income": iis_tax_deduction_income,
        "total_income_net": income_net + iis_tax_deduction_income,
        "coupon_net": coupon_net,
        "dividend_net": dividend_net,
        "commissions": commissions,
        "taxes": taxes,
        "tax_refunds": tax_refunds,
        "deposits_ytd": deposits_ytd,
        "plan_annual_contrib": plan_annual_contrib,
        "plan_progress_pct": plan_progress_pct,
        "target_to_date": target_to_date,
        "reconciliation_gap_abs": reconciliation_gap_abs,
        "positions_value_sum": positions_value_sum,
        "top_holding_name": top_holding.get("name") if top_holding is not None else None,
        "top_holding_value": normalize_decimal(top_holding.get("position_value")) if top_holding is not None else None,
        "top_holding_weight_pct": normalize_decimal(top_holding.get("weight_pct")) if top_holding is not None else None,
        "best_day_date": best_day.get("date") if best_day is not None else None,
        "best_day_pnl": normalize_decimal(best_day.get("day_pnl")) if best_day is not None else None,
        "worst_day_date": worst_day.get("date") if worst_day is not None else None,
        "worst_day_pnl": normalize_decimal(worst_day.get("day_pnl")) if worst_day is not None else None,
        "income_events_count": income_events_count,
    }
