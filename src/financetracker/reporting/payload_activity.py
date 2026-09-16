"""Activity transformations for the monthly reporting payload."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from financetracker.common.text_utils import has_mojibake
from financetracker.domain.cashflows import (
    add_operation_cashflow_by_currency_day,
    normalize_operation_currency,
)
from financetracker.domain.operations import (
    classify_operation_group,
    is_income_event_backed_tax_operation,
)
from financetracker.reporting.payload_identity import build_asset_identity, pick_alias_row
from financetracker.reporting.runtime import (
    IIS_TAX_DEDUCTION_CATEGORY,
    normalize_decimal,
    to_iso_datetime,
    to_local_market_date,
)


DEFAULT_OPERATIONS_TOP_LIMIT = 15


def build_operations_month_data(
    operations_rows: list[dict[str, Any]],
    alias_by_instrument_uid: dict[str, dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    cashflows_by_currency_day: dict[str, dict[str, dict[date, Decimal]]] = {}
    unknown_operation_group_count = 0
    mojibake_detected_count = 0

    for row in operations_rows:
        alias_row = pick_alias_row(row, alias_by_instrument_uid, alias_by_figi)
        identity = build_asset_identity(row, alias_row=alias_row)
        dt = row.get("date")
        local_date = to_local_market_date(dt)
        operation_group = classify_operation_group(row.get("operation_type"))
        description = row.get("description")
        description_has_mojibake = has_mojibake(description)

        if operation_group == "other":
            unknown_operation_group_count += 1
        if description_has_mojibake:
            mojibake_detected_count += 1

        amount = normalize_decimal(row.get("amount"))
        amount_abs = abs(amount)

        cashflow_field: str | None = None
        cashflow_amount = Decimal("0")
        if local_date is not None:
            if operation_group == "deposit" and row.get("cashflow_category") == IIS_TAX_DEDUCTION_CATEGORY:
                cashflow_field = "iis_tax_deduction_income"
                cashflow_amount = amount_abs
            elif operation_group == "deposit":
                cashflow_field = "deposits"
                cashflow_amount = amount_abs
            elif operation_group == "withdrawal":
                cashflow_field = "withdrawals"
                cashflow_amount = amount_abs
            elif operation_group == "commission":
                cashflow_field = "commissions"
                cashflow_amount = amount_abs
            elif operation_group == "income_tax" and not is_income_event_backed_tax_operation(row.get("operation_type")):
                if amount < 0:
                    cashflow_field = "operation_taxes"
                    cashflow_amount = amount_abs
                elif amount > 0:
                    cashflow_field = "operation_tax_refunds"
                    cashflow_amount = amount

        if local_date is not None and cashflow_field is not None:
            add_operation_cashflow_by_currency_day(
                cashflows_by_currency_day,
                currency=row.get("currency"),
                field=cashflow_field,
                flow_date=local_date,
                amount=cashflow_amount,
            )

        normalized_rows.append(
            {
                "operation_id": row.get("operation_id"),
                "date_utc": to_iso_datetime(dt),
                "local_date": local_date,
                "operation_type": row.get("operation_type"),
                "operation_group": operation_group,
                "cashflow_category": row.get("cashflow_category"),
                "logical_asset_id": identity["logical_asset_id"],
                "asset_uid": identity["asset_uid"],
                "instrument_uid": identity["instrument_uid"],
                "figi": identity["figi"],
                "ticker": identity["ticker"],
                "name": identity["name"],
                "amount": amount,
                "currency": row.get("currency"),
                "price": normalize_decimal(row.get("price")),
                "quantity": normalize_decimal(row.get("quantity")),
                "commission": normalize_decimal(row.get("commission")),
                "yield_amount": normalize_decimal(row.get("yield")),
                "description": description,
                "description_has_mojibake": description_has_mojibake,
                "source": row.get("source"),
            }
        )

    return normalized_rows, {
        "cashflows_by_currency_day": cashflows_by_currency_day,
        "unknown_operation_group_count": unknown_operation_group_count,
        "mojibake_detected_count": mojibake_detected_count,
    }


def build_income_month_data(
    income_rows: list[dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
    *,
    base_currency: str,
) -> tuple[
    list[dict[str, Any]],
    dict[date, Decimal],
    dict[date, Decimal],
    dict[date, Decimal],
]:
    normalized_rows: list[dict[str, Any]] = []
    income_net_by_day: dict[date, Decimal] = {}
    income_tax_by_day: dict[date, Decimal] = {}
    income_tax_refunds_by_day: dict[date, Decimal] = {}

    for row in income_rows:
        alias_row = alias_by_figi.get(row.get("figi")) if row.get("figi") else None
        identity = build_asset_identity(
            {
                "figi": row.get("figi"),
                "ticker": row.get("ticker"),
                "instrument_name": row.get("instrument_name"),
            },
            alias_row=alias_row,
        )
        event_date = row.get("event_date")
        net_amount = normalize_decimal(row.get("net_amount"))
        tax_amount = normalize_decimal(row.get("tax_amount"))
        currency = normalize_operation_currency(row.get("currency"))

        if currency == base_currency and base_currency != "UNKNOWN":
            income_net_by_day[event_date] = income_net_by_day.get(event_date, Decimal("0")) + net_amount
            if tax_amount < 0:
                income_tax_by_day[event_date] = income_tax_by_day.get(event_date, Decimal("0")) + abs(tax_amount)
            elif tax_amount > 0:
                income_tax_refunds_by_day[event_date] = (
                    income_tax_refunds_by_day.get(event_date, Decimal("0")) + tax_amount
                )

        normalized_rows.append(
            {
                "event_date": event_date,
                "event_type": row.get("event_type"),
                "currency": currency,
                "logical_asset_id": identity["logical_asset_id"],
                "asset_uid": identity["asset_uid"],
                "figi": identity["figi"],
                "ticker": identity["ticker"],
                "instrument_name": identity["name"],
                "gross_amount": normalize_decimal(row.get("gross_amount")),
                "tax_amount": tax_amount,
                "net_amount": net_amount,
                "net_yield_pct": normalize_decimal(row.get("net_yield_pct")),
                "notified": row.get("notified"),
            }
        )

    return normalized_rows, income_net_by_day, income_tax_by_day, income_tax_refunds_by_day


def normalize_realized_rows(
    rows: list[dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        alias_row = alias_by_figi.get(row.get("figi")) if row.get("figi") else None
        identity = build_asset_identity(row, alias_row=alias_row)
        normalized.append(
            {
                "logical_asset_id": identity["logical_asset_id"],
                "figi": identity["figi"],
                "ticker": identity["ticker"],
                "name": identity["name"] or identity["figi"] or identity["logical_asset_id"],
                "amount": normalize_decimal(row.get("amount")),
            }
        )
    normalized.sort(key=lambda item: abs(normalize_decimal(item["amount"])), reverse=True)
    return normalized


def build_income_by_asset_rows(
    income_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in income_rows:
        key = (row["logical_asset_id"], row.get("event_type") or "income")
        item = grouped.setdefault(
            key,
            {
                "logical_asset_id": row["logical_asset_id"],
                "figi": row.get("figi"),
                "ticker": row.get("ticker") or "",
                "name": row.get("instrument_name") or row.get("figi") or row["logical_asset_id"],
                "income_kind": row.get("event_type") or "income",
                "amount": Decimal("0"),
            },
        )
        item["amount"] += normalize_decimal(row.get("net_amount"))

    rows = list(grouped.values())
    rows.sort(key=lambda item: abs(normalize_decimal(item["amount"])), reverse=True)
    return rows


def build_open_pl_end_rows(positions_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        {
            "logical_asset_id": row["logical_asset_id"],
            "ticker": row.get("ticker") or "",
            "name": row.get("name") or row.get("figi") or row["logical_asset_id"],
            "amount": normalize_decimal(row.get("expected_yield")),
            "amount_pct": normalize_decimal(row.get("expected_yield_pct")),
        }
        for row in positions_rows
    ]
    rows.sort(key=lambda item: abs(normalize_decimal(item["amount"])), reverse=True)
    return rows


def build_operations_top(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_OPERATIONS_TOP_LIMIT,
) -> list[dict[str, Any]]:
    priority = {
        "deposit": 0,
        "withdrawal": 1,
        "sell": 2,
        "buy": 3,
        "commission": 4,
        "dividend": 5,
        "coupon": 6,
        "income_tax": 7,
        "other": 8,
    }
    ranked = sorted(
        rows,
        key=lambda row: (
            priority.get(row.get("operation_group"), 99),
            -abs(normalize_decimal(row.get("amount"))),
            row.get("local_date") or date.min,
        ),
    )
    return ranked[:limit]
