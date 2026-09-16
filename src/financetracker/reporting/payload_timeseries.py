"""Instrument end-of-day series and mover transformations for reporting."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from financetracker.domain.cashflows import (
    build_operation_cashflows_for_snapshot_interval,
    normalize_operation_currency,
    sum_decimal_values_for_snapshot_interval,
)
from financetracker.reporting.payload_identity import build_asset_identity, pick_alias_row
from financetracker.reporting.runtime import normalize_decimal, to_iso_datetime


DEFAULT_AI_TOP_LIMIT = 5


def build_timeseries_daily(
    snapshot_rows: list[dict[str, Any]],
    *,
    deposits_by_day: dict[date, Decimal],
    iis_tax_deductions_by_day: dict[date, Decimal],
    withdrawals_by_day: dict[date, Decimal],
    income_net_by_day: dict[date, Decimal],
    commissions_by_day: dict[date, Decimal],
    taxes_by_day: dict[date, Decimal],
    income_tax_by_day: dict[date, Decimal],
    twr_by_date: dict[date, Decimal],
    tax_refunds_by_day: dict[date, Decimal] | None = None,
    income_tax_refunds_by_day: dict[date, Decimal] | None = None,
    base_currency: str | None = None,
    operation_cashflows_by_currency_day: dict[str, dict[str, dict[date, Decimal]]] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    previous_value: Decimal | None = None
    previous_snapshot_date: date | None = None

    for row in snapshot_rows:
        snapshot_date = row["snapshot_date"]
        portfolio_value = normalize_decimal(row.get("total_value"))
        interval_args = (previous_snapshot_date, snapshot_date)
        has_currency_aware_cashflows = operation_cashflows_by_currency_day is not None
        interval_operation_cashflows = build_operation_cashflows_for_snapshot_interval(
            operation_cashflows_by_currency_day or {}, *interval_args
        )
        snapshot_currency = normalize_operation_currency(row.get("currency") or base_currency)
        base_cashflows = None
        if snapshot_currency != "UNKNOWN":
            base_cashflows = next(
                (item for item in interval_operation_cashflows if item["currency"] == snapshot_currency),
                None,
            )
        if has_currency_aware_cashflows:
            base_cashflows = base_cashflows or {}
            deposits = normalize_decimal(base_cashflows.get("deposits"))
            withdrawals = normalize_decimal(base_cashflows.get("withdrawals"))
            iis_tax_deduction_income = normalize_decimal(base_cashflows.get("iis_tax_deduction_income"))
            commissions = normalize_decimal(base_cashflows.get("commissions"))
            operation_taxes = normalize_decimal(base_cashflows.get("operation_taxes"))
            operation_tax_refunds = normalize_decimal(base_cashflows.get("operation_tax_refunds"))
        else:
            deposits = sum_decimal_values_for_snapshot_interval(deposits_by_day, *interval_args)
            withdrawals = sum_decimal_values_for_snapshot_interval(withdrawals_by_day, *interval_args)
            iis_tax_deduction_income = sum_decimal_values_for_snapshot_interval(
                iis_tax_deductions_by_day, *interval_args
            )
            commissions = sum_decimal_values_for_snapshot_interval(commissions_by_day, *interval_args)
            operation_taxes = sum_decimal_values_for_snapshot_interval(taxes_by_day, *interval_args)
            operation_tax_refunds = sum_decimal_values_for_snapshot_interval(tax_refunds_by_day or {}, *interval_args)
        income_net = sum_decimal_values_for_snapshot_interval(income_net_by_day, *interval_args)
        total_income_net = income_net + iis_tax_deduction_income
        income_taxes = sum_decimal_values_for_snapshot_interval(income_tax_by_day, *interval_args)
        income_tax_refunds = sum_decimal_values_for_snapshot_interval(
            income_tax_refunds_by_day or {}, *interval_args
        )
        net_external_flow = deposits - withdrawals
        net_cashflow = net_external_flow
        day_pnl = Decimal("0") if previous_value is None else portfolio_value - previous_value - net_cashflow
        previous_value = portfolio_value
        previous_snapshot_date = snapshot_date

        rows.append(
            {
                "date": snapshot_date,
                "snapshot_id": row.get("id"),
                "snapshot_at_utc": to_iso_datetime(row.get("snapshot_at")),
                "portfolio_value": portfolio_value,
                "expected_yield": normalize_decimal(row.get("expected_yield")),
                "expected_yield_pct": normalize_decimal(row.get("expected_yield_pct")),
                "deposits": deposits,
                "withdrawals": withdrawals,
                "income_net": income_net,
                "iis_tax_deduction_income": iis_tax_deduction_income,
                "total_income_net": total_income_net,
                "commissions": commissions,
                "operation_taxes": operation_taxes,
                "income_taxes": income_taxes,
                "operation_tax_refunds": operation_tax_refunds,
                "income_tax_refunds": income_tax_refunds,
                "tax_refunds": operation_tax_refunds + income_tax_refunds,
                "net_external_flow": net_external_flow,
                "net_cashflow": net_cashflow,
                "day_pnl": day_pnl,
                "twr_pct": twr_by_date.get(snapshot_date),
                "operation_cashflows_by_currency": interval_operation_cashflows,
                "unsupported_operation_currencies": [
                    item["currency"]
                    for item in interval_operation_cashflows
                    if snapshot_currency == "UNKNOWN" or item["currency"] != snapshot_currency
                ],
            }
        )

    return rows


def find_best_and_worst_day(rows: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    meaningful_rows = rows[1:] if len(rows) > 1 else rows
    if not meaningful_rows:
        return None, None
    best_day = max(meaningful_rows, key=lambda row: normalize_decimal(row.get("day_pnl")))
    worst_day = min(meaningful_rows, key=lambda row: normalize_decimal(row.get("day_pnl")))
    return best_day, worst_day


def resolve_start_metrics(
    start_snapshot: dict[str, Any] | None,
    daily_rows: list[dict[str, Any]],
) -> tuple[Decimal | None, int | None]:
    if start_snapshot is not None:
        return normalize_decimal(start_snapshot.get("total_value")), start_snapshot.get("id")
    if daily_rows:
        return normalize_decimal(daily_rows[0].get("portfolio_value")), daily_rows[0].get("snapshot_id")
    return None, None


def compute_period_pnl(
    *,
    start_snapshot: dict[str, Any] | None,
    end_value: Decimal | None,
    start_value: Decimal | None,
    net_external_flow: Decimal,
    daily_rows: list[dict[str, Any]],
) -> tuple[Decimal | None, Decimal | None]:
    if end_value is None or start_value is None:
        return None, None

    if start_snapshot is not None:
        period_pnl_abs = end_value - start_value - net_external_flow
    else:
        month_external_flow = sum(
            (normalize_decimal(row.get("net_external_flow", row.get("net_cashflow"))) for row in daily_rows[1:]),
            Decimal("0"),
        )
        period_pnl_abs = end_value - start_value - month_external_flow

    if start_value == 0:
        return period_pnl_abs, None
    return period_pnl_abs, period_pnl_abs * Decimal("100") / start_value


def build_instrument_stats(series: list[dict[str, Any]]) -> dict[str, Any]:
    if not series:
        return {}

    min_position = min(series, key=lambda row: normalize_decimal(row["position_value"]))
    max_position = max(series, key=lambda row: normalize_decimal(row["position_value"]))
    min_expected_yield = min(series, key=lambda row: normalize_decimal(row["expected_yield"]))
    max_expected_yield = max(series, key=lambda row: normalize_decimal(row["expected_yield"]))
    end_point = series[-1]

    running_min = normalize_decimal(series[0]["expected_yield"])
    running_min_date = series[0]["date"]
    max_rise_abs = Decimal("0")
    max_rise_start_date = series[0]["date"]
    max_rise_end_date = series[0]["date"]

    running_max = normalize_decimal(series[0]["expected_yield"])
    running_max_date = series[0]["date"]
    max_drawdown_abs = Decimal("0")
    max_drawdown_start_date = series[0]["date"]
    max_drawdown_end_date = series[0]["date"]

    for point in series[1:]:
        current_expected_yield = normalize_decimal(point["expected_yield"])
        current_date = point["date"]
        rise_abs = current_expected_yield - running_min
        if rise_abs > max_rise_abs:
            max_rise_abs = rise_abs
            max_rise_start_date = running_min_date
            max_rise_end_date = current_date
        if current_expected_yield < running_min:
            running_min = current_expected_yield
            running_min_date = current_date

        drawdown_abs = current_expected_yield - running_max
        if drawdown_abs < max_drawdown_abs:
            max_drawdown_abs = drawdown_abs
            max_drawdown_start_date = running_max_date
            max_drawdown_end_date = current_date
        if current_expected_yield > running_max:
            running_max = current_expected_yield
            running_max_date = current_date

    return {
        "eod_min_position_value": normalize_decimal(min_position["position_value"]),
        "eod_min_position_value_date": min_position["date"],
        "eod_max_position_value": normalize_decimal(max_position["position_value"]),
        "eod_max_position_value_date": max_position["date"],
        "eod_end_position_value": normalize_decimal(end_point["position_value"]),
        "eod_min_expected_yield": normalize_decimal(min_expected_yield["expected_yield"]),
        "eod_min_expected_yield_date": min_expected_yield["date"],
        "eod_max_expected_yield": normalize_decimal(max_expected_yield["expected_yield"]),
        "eod_max_expected_yield_date": max_expected_yield["date"],
        "eod_end_expected_yield": normalize_decimal(end_point["expected_yield"]),
        "eod_end_expected_yield_pct": normalize_decimal(end_point["expected_yield_pct"]),
        "max_rise_abs": max_rise_abs,
        "max_rise_start_date": max_rise_start_date,
        "max_rise_end_date": max_rise_end_date,
        "max_drawdown_abs": max_drawdown_abs,
        "max_drawdown_start_date": max_drawdown_start_date,
        "max_drawdown_end_date": max_drawdown_end_date,
    }


def build_instrument_eod_timeseries(
    rows: list[dict[str, Any]],
    alias_by_instrument_uid: dict[str, dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        alias_row = pick_alias_row(row, alias_by_instrument_uid, alias_by_figi)
        identity = build_asset_identity(row, alias_row=alias_row)
        logical_asset_id = identity["logical_asset_id"]
        item = grouped.setdefault(
            logical_asset_id,
            {
                "logical_asset_id": logical_asset_id,
                "asset_uid": identity["asset_uid"],
                "instrument_uid": identity["instrument_uid"],
                "figi": identity["figi"],
                "ticker": identity["ticker"],
                "name": identity["name"] or identity["figi"] or logical_asset_id,
                "instrument_type": row.get("instrument_type"),
                "series": [],
            },
        )
        item["series"].append(
            {
                "date": row.get("snapshot_date"),
                "snapshot_id": row.get("snapshot_id"),
                "snapshot_at_utc": to_iso_datetime(row.get("snapshot_at")),
                "quantity": normalize_decimal(row.get("quantity")),
                "position_value": normalize_decimal(row.get("position_value")),
                "expected_yield": normalize_decimal(row.get("expected_yield")),
                "expected_yield_pct": normalize_decimal(row.get("expected_yield_pct")),
                "weight_pct": normalize_decimal(row.get("weight_pct")),
            }
        )

    normalized: list[dict[str, Any]] = []
    for item in grouped.values():
        item["series"].sort(key=lambda point: (point["date"], point["snapshot_id"] or 0))
        item["stats"] = build_instrument_stats(item["series"])
        normalized.append(item)

    normalized.sort(
        key=lambda item: (
            normalize_decimal(item["stats"].get("eod_end_position_value")),
            item.get("ticker") or "",
            item.get("name") or "",
        ),
        reverse=True,
    )
    return normalized


def build_instrument_movers(
    instrument_eod_timeseries: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_AI_TOP_LIMIT,
) -> dict[str, list[dict[str, Any]]]:
    top_growth: list[dict[str, Any]] = []
    top_drawdown: list[dict[str, Any]] = []
    for row in instrument_eod_timeseries:
        stats = row.get("stats") or {}
        rise_abs = normalize_decimal(stats.get("max_rise_abs"))
        drawdown_abs = normalize_decimal(stats.get("max_drawdown_abs"))
        if rise_abs > 0:
            top_growth.append({"logical_asset_id": row["logical_asset_id"], "ticker": row.get("ticker") or "", "name": row.get("name") or row.get("figi") or row["logical_asset_id"], "metric_kind": "expected_yield_rise", "rise_abs": rise_abs, "start_date": stats.get("max_rise_start_date"), "end_date": stats.get("max_rise_end_date"), "end_expected_yield": normalize_decimal(stats.get("eod_end_expected_yield")), "end_expected_yield_pct": normalize_decimal(stats.get("eod_end_expected_yield_pct"))})
        if drawdown_abs < 0:
            top_drawdown.append({"logical_asset_id": row["logical_asset_id"], "ticker": row.get("ticker") or "", "name": row.get("name") or row.get("figi") or row["logical_asset_id"], "metric_kind": "expected_yield_drawdown", "drawdown_abs": drawdown_abs, "start_date": stats.get("max_drawdown_start_date"), "end_date": stats.get("max_drawdown_end_date"), "end_expected_yield": normalize_decimal(stats.get("eod_end_expected_yield")), "end_expected_yield_pct": normalize_decimal(stats.get("eod_end_expected_yield_pct"))})

    top_growth.sort(key=lambda item: (normalize_decimal(item["rise_abs"]), normalize_decimal(item["end_expected_yield"])), reverse=True)
    top_drawdown.sort(key=lambda item: (normalize_decimal(item["drawdown_abs"]), normalize_decimal(item["end_expected_yield"])))
    return {"top_growth": top_growth[:limit], "top_drawdown": top_drawdown[:limit]}
