from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from common.logging_setup import get_logger
from common.text_utils import has_mojibake
from queries import (
    compute_realized_by_asset,
    get_asset_alias_rows,
    get_dataset_operations,
    get_income_events_for_period,
    get_income_for_period,
    get_instrument_eod_rows,
    get_month_snapshots,
    get_net_external_flow_for_period,
    get_period_daily_snapshot_rows,
    get_positions_for_snapshot,
    get_unrealized_at_period_end,
    resolve_reporting_account_id,
    get_commissions_for_period,
    get_deposits_for_period,
    get_taxes_for_period,
)
from runtime import (
    ACCOUNT_FRIENDLY_NAME,
    MONTHS_RU,
    PLAN_ANNUAL_CONTRIB_RUB,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TZ,
    TZ_NAME,
    db_session,
    decimal_to_str,
    fmt_decimal_rub,
    fmt_pct,
    normalize_decimal,
    to_iso_datetime,
    to_local_market_date,
)
from services import (
    aggregate_rebalance_values_by_class,
    build_asset_alias_lookup,
    build_logical_asset_id,
    build_reconciliation_by_asset_type,
    classify_operation_group,
    compute_income_by_asset_net,
    compute_rebalance_plan,
    compute_twr_timeseries,
    get_rebalance_targets,
    is_income_event_backed_tax_operation,
)


MONTHLY_REPORT_PAYLOAD_SCHEMA_VERSION = "monthly_report_payload.v1"
MONTHLY_AI_INPUT_SCHEMA_VERSION = "monthly_ai_input.v1"
AI_INPUT_STYLE = "calm precise non-promotional"
DEFAULT_AI_INPUT_MAX_CHARS = 12_000
DEFAULT_OPERATIONS_TOP_LIMIT = 15
DEFAULT_AI_TOP_LIMIT = 5
REPORT_DEBUG_SAVE_PAYLOAD = os.getenv("REPORT_DEBUG_SAVE_PAYLOAD", "false").strip().lower() in {"1", "true", "yes", "on"}

logger = get_logger(__name__)


def resolve_monthly_report_period(
    *,
    year: int | None = None,
    month: int | None = None,
    now: datetime | None = None,
) -> tuple[int, int]:
    current = now.astimezone(TZ) if now is not None else datetime.now(TZ)
    resolved_year = year if year is not None else current.year
    resolved_month = month if month is not None else current.month

    if resolved_year < 1900 or resolved_year > 2100:
        raise ValueError("Поле year должно быть в диапазоне 1900..2100.")
    if resolved_month < 1 or resolved_month > 12:
        raise ValueError("Поле month должно быть в диапазоне 1..12.")

    return resolved_year, resolved_month


def _get_month_bounds(year: int, month: int) -> dict[str, Any]:
    period_start = date(year, month, 1)
    if month == 12:
        period_end_exclusive = date(year + 1, 1, 1)
    else:
        period_end_exclusive = date(year, month + 1, 1)
    return {
        "period_start": period_start,
        "period_end": period_end_exclusive - timedelta(days=1),
        "period_end_exclusive": period_end_exclusive,
        "period_start_dt": datetime.combine(period_start, time.min),
        "period_end_dt": datetime.combine(period_end_exclusive, time.min) - timedelta(microseconds=1),
        "period_end_exclusive_dt": datetime.combine(period_end_exclusive, time.min),
    }


def _resolve_currency(snapshot_rows: list[dict[str, Any]], positions_rows: list[dict[str, Any]]) -> str:
    for row in reversed(snapshot_rows):
        currency = (row.get("currency") or "").strip()
        if currency:
            return currency
    for row in positions_rows:
        currency = (row.get("currency") or "").strip()
        if currency:
            return currency
    return "RUB"


def _pick_alias_row(
    row: dict[str, Any],
    alias_by_instrument_uid: dict[str, dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    instrument_uid = row.get("instrument_uid")
    figi = row.get("figi")
    if instrument_uid:
        alias_row = alias_by_instrument_uid.get(instrument_uid)
        if alias_row is not None:
            return alias_row
    if figi:
        return alias_by_figi.get(figi)
    return None


def _build_asset_identity(
    row: dict[str, Any],
    *,
    alias_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
    instrument_uid = row.get("instrument_uid") or (alias_row.get("instrument_uid") if alias_row is not None else None)
    figi = row.get("figi") or (alias_row.get("figi") if alias_row is not None else None)
    ticker = (row.get("ticker") or (alias_row.get("ticker") if alias_row is not None else "")).strip()
    name = (row.get("name") or row.get("instrument_name") or (alias_row.get("name") if alias_row is not None else "")).strip()

    logical_asset_id = build_logical_asset_id(
        asset_uid=asset_uid,
        instrument_uid=instrument_uid,
        figi=figi,
    )
    if logical_asset_id is None:
        logical_asset_id = ticker or name or figi or "unknown_asset"

    return {
        "logical_asset_id": logical_asset_id,
        "asset_uid": asset_uid,
        "instrument_uid": instrument_uid,
        "figi": figi,
        "ticker": ticker,
        "name": name,
    }


def _serialize_report_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_to_str(value)
    if isinstance(value, datetime):
        return to_iso_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_report_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_report_value(item) for key, item in value.items()}
    return value


def serialize_report_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _serialize_report_value(payload)


def _normalize_positions(
    rows: list[dict[str, Any]],
    alias_by_instrument_uid: dict[str, dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        alias_row = _pick_alias_row(row, alias_by_instrument_uid, alias_by_figi)
        identity = _build_asset_identity(row, alias_row=alias_row)
        normalized.append(
            {
                **identity,
                "ticker": identity["ticker"] or (row.get("ticker") or ""),
                "name": identity["name"] or (row.get("name") or identity["figi"] or ""),
                "instrument_type": row.get("instrument_type"),
                "quantity": normalize_decimal(row.get("quantity")),
                "currency": row.get("currency"),
                "position_value": normalize_decimal(row.get("position_value")),
                "expected_yield": normalize_decimal(row.get("expected_yield")),
                "expected_yield_pct": normalize_decimal(row.get("expected_yield_pct")),
                "weight_pct": normalize_decimal(row.get("weight_pct")),
            }
        )
    normalized.sort(
        key=lambda item: (
            normalize_decimal(item.get("position_value")),
            item.get("ticker") or "",
            item.get("name") or "",
        ),
        reverse=True,
    )
    return normalized


def build_position_flow_groups(
    start_positions: list[dict[str, Any]],
    end_positions: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    start_map = {row["logical_asset_id"]: row for row in start_positions}
    end_map = {row["logical_asset_id"]: row for row in end_positions}

    grouped = {
        "new": [],
        "closed": [],
        "increased": [],
        "decreased": [],
    }

    for logical_asset_id in sorted(set(start_map) | set(end_map)):
        start_row = start_map.get(logical_asset_id)
        end_row = end_map.get(logical_asset_id)
        if start_row is None and end_row is None:
            continue

        start_qty = normalize_decimal(start_row.get("quantity")) if start_row is not None else Decimal("0")
        end_qty = normalize_decimal(end_row.get("quantity")) if end_row is not None else Decimal("0")
        start_value = normalize_decimal(start_row.get("position_value")) if start_row is not None else Decimal("0")
        end_value = normalize_decimal(end_row.get("position_value")) if end_row is not None else Decimal("0")

        base_row = end_row or start_row or {}
        item = {
            "logical_asset_id": logical_asset_id,
            "ticker": base_row.get("ticker") or "",
            "name": base_row.get("name") or base_row.get("figi") or logical_asset_id,
            "instrument_type": base_row.get("instrument_type"),
            "start_qty": start_qty,
            "end_qty": end_qty,
            "delta_qty": end_qty - start_qty,
            "start_value": start_value,
            "end_value": end_value,
            "delta_value": end_value - start_value,
        }

        if start_row is None and end_row is not None:
            grouped["new"].append(item)
        elif start_row is not None and end_row is None:
            grouped["closed"].append(item)
        elif end_qty > start_qty:
            grouped["increased"].append(item)
        elif end_qty < start_qty:
            grouped["decreased"].append(item)

    for items in grouped.values():
        items.sort(
            key=lambda item: (
                abs(normalize_decimal(item["delta_value"])),
                abs(normalize_decimal(item["delta_qty"])),
                item["ticker"] or "",
                item["name"] or "",
            ),
            reverse=True,
        )

    return grouped


def _build_operations_month_data(
    operations_rows: list[dict[str, Any]],
    alias_by_instrument_uid: dict[str, dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    deposits_by_day: dict[date, Decimal] = {}
    withdrawals_by_day: dict[date, Decimal] = {}
    commissions_by_day: dict[date, Decimal] = {}
    taxes_by_day: dict[date, Decimal] = {}
    unknown_operation_group_count = 0
    mojibake_detected_count = 0

    for row in operations_rows:
        alias_row = _pick_alias_row(row, alias_by_instrument_uid, alias_by_figi)
        identity = _build_asset_identity(row, alias_row=alias_row)
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

        if local_date is not None:
            if operation_group == "deposit":
                deposits_by_day[local_date] = deposits_by_day.get(local_date, Decimal("0")) + amount
            elif operation_group == "withdrawal":
                withdrawals_by_day[local_date] = withdrawals_by_day.get(local_date, Decimal("0")) + amount_abs
            elif operation_group == "commission":
                commissions_by_day[local_date] = commissions_by_day.get(local_date, Decimal("0")) + amount_abs
            elif operation_group == "income_tax" and not is_income_event_backed_tax_operation(row.get("operation_type")):
                taxes_by_day[local_date] = taxes_by_day.get(local_date, Decimal("0")) + amount_abs

        normalized_rows.append(
            {
                "operation_id": row.get("operation_id"),
                "date_utc": to_iso_datetime(dt),
                "local_date": local_date,
                "operation_type": row.get("operation_type"),
                "operation_group": operation_group,
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
        "deposits_by_day": deposits_by_day,
        "withdrawals_by_day": withdrawals_by_day,
        "commissions_by_day": commissions_by_day,
        "taxes_by_day": taxes_by_day,
        "unknown_operation_group_count": unknown_operation_group_count,
        "mojibake_detected_count": mojibake_detected_count,
    }


def _build_income_month_data(
    income_rows: list[dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Decimal], dict[str, Decimal]]:
    normalized_rows: list[dict[str, Any]] = []
    income_net_by_day: dict[date, Decimal] = {}
    income_tax_by_day: dict[date, Decimal] = {}

    for row in income_rows:
        alias_row = alias_by_figi.get(row.get("figi")) if row.get("figi") else None
        identity = _build_asset_identity(
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

        income_net_by_day[event_date] = income_net_by_day.get(event_date, Decimal("0")) + net_amount
        income_tax_by_day[event_date] = income_tax_by_day.get(event_date, Decimal("0")) + abs(tax_amount)

        normalized_rows.append(
            {
                "event_date": event_date,
                "event_type": row.get("event_type"),
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

    return normalized_rows, income_net_by_day, income_tax_by_day


def _build_timeseries_daily(
    snapshot_rows: list[dict[str, Any]],
    *,
    deposits_by_day: dict[date, Decimal],
    withdrawals_by_day: dict[date, Decimal],
    income_net_by_day: dict[date, Decimal],
    commissions_by_day: dict[date, Decimal],
    taxes_by_day: dict[date, Decimal],
    income_tax_by_day: dict[date, Decimal],
    twr_by_date: dict[date, Decimal],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    previous_value: Decimal | None = None

    for row in snapshot_rows:
        snapshot_date = row["snapshot_date"]
        portfolio_value = normalize_decimal(row.get("total_value"))
        deposits = deposits_by_day.get(snapshot_date, Decimal("0"))
        withdrawals = withdrawals_by_day.get(snapshot_date, Decimal("0"))
        income_net = income_net_by_day.get(snapshot_date, Decimal("0"))
        commissions = commissions_by_day.get(snapshot_date, Decimal("0"))
        operation_taxes = taxes_by_day.get(snapshot_date, Decimal("0"))
        income_taxes = income_tax_by_day.get(snapshot_date, Decimal("0"))
        net_cashflow = deposits - withdrawals + income_net - commissions - operation_taxes
        day_pnl = Decimal("0")
        if previous_value is not None:
            day_pnl = portfolio_value - previous_value - net_cashflow
        previous_value = portfolio_value

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
                "commissions": commissions,
                "operation_taxes": operation_taxes,
                "income_taxes": income_taxes,
                "net_cashflow": net_cashflow,
                "day_pnl": day_pnl,
                "twr_pct": twr_by_date.get(snapshot_date),
            }
        )

    return rows


def _find_best_and_worst_day(rows: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    meaningful_rows = rows[1:] if len(rows) > 1 else rows
    if not meaningful_rows:
        return None, None
    best_day = max(meaningful_rows, key=lambda row: normalize_decimal(row.get("day_pnl")))
    worst_day = min(meaningful_rows, key=lambda row: normalize_decimal(row.get("day_pnl")))
    return best_day, worst_day


def _resolve_start_metrics(
    start_snapshot: dict[str, Any] | None,
    daily_rows: list[dict[str, Any]],
) -> tuple[Decimal | None, int | None]:
    if start_snapshot is not None:
        return normalize_decimal(start_snapshot.get("total_value")), start_snapshot.get("id")
    if daily_rows:
        return normalize_decimal(daily_rows[0].get("portfolio_value")), daily_rows[0].get("snapshot_id")
    return None, None


def _compute_period_pnl(
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
        month_net_cashflow = sum((normalize_decimal(row.get("net_cashflow")) for row in daily_rows[1:]), Decimal("0"))
        period_pnl_abs = end_value - start_value - month_net_cashflow

    if start_value == 0:
        return period_pnl_abs, None
    return period_pnl_abs, period_pnl_abs * Decimal("100") / start_value


def _build_instrument_stats(series: list[dict[str, Any]]) -> dict[str, Any]:
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
        alias_row = _pick_alias_row(row, alias_by_instrument_uid, alias_by_figi)
        identity = _build_asset_identity(row, alias_row=alias_row)
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
        item["stats"] = _build_instrument_stats(item["series"])
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
            top_growth.append(
                {
                    "logical_asset_id": row["logical_asset_id"],
                    "ticker": row.get("ticker") or "",
                    "name": row.get("name") or row.get("figi") or row["logical_asset_id"],
                    "metric_kind": "expected_yield_rise",
                    "rise_abs": rise_abs,
                    "start_date": stats.get("max_rise_start_date"),
                    "end_date": stats.get("max_rise_end_date"),
                    "end_expected_yield": normalize_decimal(stats.get("eod_end_expected_yield")),
                    "end_expected_yield_pct": normalize_decimal(stats.get("eod_end_expected_yield_pct")),
                }
            )
        if drawdown_abs < 0:
            top_drawdown.append(
                {
                    "logical_asset_id": row["logical_asset_id"],
                    "ticker": row.get("ticker") or "",
                    "name": row.get("name") or row.get("figi") or row["logical_asset_id"],
                    "metric_kind": "expected_yield_drawdown",
                    "drawdown_abs": drawdown_abs,
                    "start_date": stats.get("max_drawdown_start_date"),
                    "end_date": stats.get("max_drawdown_end_date"),
                    "end_expected_yield": normalize_decimal(stats.get("eod_end_expected_yield")),
                    "end_expected_yield_pct": normalize_decimal(stats.get("eod_end_expected_yield_pct")),
                }
            )

    top_growth.sort(
        key=lambda item: (
            normalize_decimal(item["rise_abs"]),
            normalize_decimal(item["end_expected_yield"]),
        ),
        reverse=True,
    )
    top_drawdown.sort(
        key=lambda item: (
            normalize_decimal(item["drawdown_abs"]),
            normalize_decimal(item["end_expected_yield"]),
        )
    )

    return {
        "top_growth": top_growth[:limit],
        "top_drawdown": top_drawdown[:limit],
    }


def _normalize_realized_rows(
    rows: list[dict[str, Any]],
    alias_by_figi: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        alias_row = alias_by_figi.get(row.get("figi")) if row.get("figi") else None
        identity = _build_asset_identity(row, alias_row=alias_row)
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


def _build_income_by_asset_rows(
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


def _build_open_pl_end_rows(positions_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def _build_reconciliation_rows(
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


def _build_rebalance_snapshot(
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


def _build_summary_metrics(
    *,
    year: int,
    period_end: date,
    end_snapshot: dict[str, Any] | None,
    daily_rows: list[dict[str, Any]],
    positions_current: list[dict[str, Any]],
    deposits: Decimal,
    withdrawals: Decimal,
    income_net: Decimal,
    coupon_net: Decimal,
    dividend_net: Decimal,
    commissions: Decimal,
    taxes: Decimal,
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
    period_pnl_abs, period_pnl_pct = _compute_period_pnl(
        start_snapshot=start_snapshot,
        end_value=end_value,
        start_value=start_value,
        net_external_flow=net_external_flow,
        daily_rows=daily_rows,
    )

    top_holding = positions_current[0] if positions_current else None
    best_day, worst_day = _find_best_and_worst_day(daily_rows)

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
        "coupon_net": coupon_net,
        "dividend_net": dividend_net,
        "commissions": commissions,
        "taxes": taxes,
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


def _format_display_date(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    return datetime.fromisoformat(raw_value).strftime("%d.%m.%Y")


def _format_display_day(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    return datetime.fromisoformat(raw_value).strftime("%d.%m")


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _display_rub(value: Any, *, precision: int = 0) -> str:
    if value is None:
        return "—"
    return fmt_decimal_rub(value, precision=precision)


def _build_overview_facts(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload["summary_metrics"]
    highlights = []
    highlights.append(
        f"Стоимость на конец месяца: {fmt_decimal_rub(summary.get('current_value'), precision=0)}."
    )
    if summary.get("period_pnl_abs") is not None:
        highlights.append(
            f"Результат периода: {fmt_decimal_rub(summary.get('period_pnl_abs'), precision=0)} "
            f"({fmt_pct(float(_to_decimal(summary.get('period_pnl_pct'))), precision=2) if summary.get('period_pnl_pct') is not None else '—'})."
        )
    highlights.append(
        f"Внешний поток: {fmt_decimal_rub(summary.get('net_external_flow'), precision=0)}; "
        f"пополнения {fmt_decimal_rub(summary.get('deposits'), precision=0)}, "
        f"выводы {fmt_decimal_rub(summary.get('withdrawals'), precision=0)}."
    )
    if summary.get("top_holding_name"):
        highlights.append(
            f"Крупнейшая позиция: {summary.get('top_holding_name')} "
            f"{fmt_decimal_rub(summary.get('top_holding_value'), precision=0)} "
            f"({fmt_pct(float(_to_decimal(summary.get('top_holding_weight_pct'))), precision=1)})."
        )

    return {
        "current_value": _display_rub(summary.get("current_value"), precision=0),
        "period_pnl_abs": _display_rub(summary.get("period_pnl_abs"), precision=0),
        "period_pnl_pct": fmt_pct(float(_to_decimal(summary.get("period_pnl_pct"))), precision=2)
        if summary.get("period_pnl_pct") is not None
        else "—",
        "period_twr_pct": fmt_pct(float(_to_decimal(summary.get("period_twr_pct"))), precision=2)
        if summary.get("period_twr_pct") is not None
        else "—",
        "net_external_flow": _display_rub(summary.get("net_external_flow"), precision=0),
        "income_net": _display_rub(summary.get("income_net"), precision=2),
        "commissions": _display_rub(summary.get("commissions"), precision=2),
        "taxes": _display_rub(summary.get("taxes"), precision=2),
        "top_holding_name": summary.get("top_holding_name"),
        "top_holding_value": _display_rub(summary.get("top_holding_value"), precision=0),
        "top_holding_weight_pct": fmt_pct(float(_to_decimal(summary.get("top_holding_weight_pct"))), precision=1)
        if summary.get("top_holding_weight_pct") is not None
        else "—",
        "best_day": {
            "date": _format_display_date(summary.get("best_day_date")),
            "pnl": _display_rub(summary.get("best_day_pnl"), precision=0),
        },
        "worst_day": {
            "date": _format_display_date(summary.get("worst_day_date")),
            "pnl": _display_rub(summary.get("worst_day_pnl"), precision=0),
        },
        "highlights": highlights[:5],
    }


def _build_performance_facts(payload: dict[str, Any]) -> dict[str, Any]:
    daily_rows = payload["timeseries_daily"]
    if daily_rows:
        peak = max(daily_rows, key=lambda row: _to_decimal(row.get("portfolio_value")))
        trough = min(daily_rows, key=lambda row: _to_decimal(row.get("portfolio_value")))
    else:
        peak = {}
        trough = {}

    summary = payload["summary_metrics"]
    return {
        "period_twr_pct": fmt_pct(float(_to_decimal(summary.get("period_twr_pct"))), precision=2)
        if summary.get("period_twr_pct") is not None
        else "—",
        "period_pnl_abs": _display_rub(summary.get("period_pnl_abs"), precision=0),
        "period_pnl_pct": fmt_pct(float(_to_decimal(summary.get("period_pnl_pct"))), precision=2)
        if summary.get("period_pnl_pct") is not None
        else "—",
        "best_day": {
            "date": _format_display_date(summary.get("best_day_date")),
            "pnl": _display_rub(summary.get("best_day_pnl"), precision=0),
        },
        "worst_day": {
            "date": _format_display_date(summary.get("worst_day_date")),
            "pnl": _display_rub(summary.get("worst_day_pnl"), precision=0),
        },
        "portfolio_peak": {
            "date": _format_display_date(peak.get("date")),
            "value": _display_rub(peak.get("portfolio_value"), precision=0),
        },
        "portfolio_trough": {
            "date": _format_display_date(trough.get("date")),
            "value": _display_rub(trough.get("portfolio_value"), precision=0),
        },
    }


def _build_structure_facts(payload: dict[str, Any]) -> dict[str, Any]:
    positions = payload["positions_current"]
    top_positions = positions[:DEFAULT_AI_TOP_LIMIT]
    concentration_top3 = sum((_to_decimal(row.get("weight_pct")) for row in positions[:3]), Decimal("0"))
    asset_mix = [
        {
            "instrument_type": row.get("instrument_type"),
            "snapshot_total": _display_rub(row.get("snapshot_total"), precision=0),
            "delta_abs": _display_rub(row.get("delta_abs"), precision=0),
        }
        for row in payload["reconciliation_by_asset_type"]
        if _to_decimal(row.get("snapshot_total")) != 0
    ]
    return {
        "top_positions": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "position_value": fmt_decimal_rub(row.get("position_value"), precision=0),
                "weight_pct": fmt_pct(float(_to_decimal(row.get("weight_pct"))), precision=1),
            }
            for row in top_positions
        ],
        "concentration_top3_weight_pct": fmt_pct(float(concentration_top3), precision=1),
        "asset_mix": asset_mix,
    }


def _build_position_flow_facts(payload: dict[str, Any]) -> dict[str, Any]:
    grouped = payload["position_flow_groups"]

    def _normalize(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "delta_qty": decimal_to_str(_to_decimal(row.get("delta_qty"))),
                "delta_value": fmt_decimal_rub(row.get("delta_value"), precision=0),
            }
            for row in items[:DEFAULT_AI_TOP_LIMIT]
        ]

    return {
        "new": _normalize(grouped.get("new", [])),
        "closed": _normalize(grouped.get("closed", [])),
        "increased": _normalize(grouped.get("increased", [])),
        "decreased": _normalize(grouped.get("decreased", [])),
    }


def _build_mover_facts(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "top_growth": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "rise_abs": fmt_decimal_rub(row.get("rise_abs"), precision=0),
                "window": f"{_format_display_day(row.get('start_date'))} → {_format_display_day(row.get('end_date'))}",
            }
            for row in payload["instrument_movers"].get("top_growth", [])[:DEFAULT_AI_TOP_LIMIT]
        ],
        "top_drawdown": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "drawdown_abs": fmt_decimal_rub(row.get("drawdown_abs"), precision=0),
                "window": f"{_format_display_day(row.get('start_date'))} → {_format_display_day(row.get('end_date'))}",
            }
            for row in payload["instrument_movers"].get("top_drawdown", [])[:DEFAULT_AI_TOP_LIMIT]
        ],
    }


def _build_contribution_facts(payload: dict[str, Any]) -> dict[str, Any]:
    realized_positive = [row for row in payload["realized_by_asset"] if _to_decimal(row.get("amount")) > 0]
    realized_negative = [row for row in payload["realized_by_asset"] if _to_decimal(row.get("amount")) < 0]
    open_pl_positive = [row for row in payload["open_pl_end"] if _to_decimal(row.get("amount")) > 0]
    open_pl_negative = [row for row in payload["open_pl_end"] if _to_decimal(row.get("amount")) < 0]

    return {
        "realized_winners": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in realized_positive[:DEFAULT_AI_TOP_LIMIT]
        ],
        "realized_losers": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in realized_negative[:DEFAULT_AI_TOP_LIMIT]
        ],
        "income_contributors": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "income_kind": row.get("income_kind"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=2),
            }
            for row in payload["income_by_asset"][:DEFAULT_AI_TOP_LIMIT]
        ],
        "open_pl_leaders": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in open_pl_positive[:DEFAULT_AI_TOP_LIMIT]
        ],
        "open_pl_laggards": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in open_pl_negative[:DEFAULT_AI_TOP_LIMIT]
        ],
    }


def _build_cashflow_facts(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload["summary_metrics"]
    return {
        "deposits": _display_rub(summary.get("deposits"), precision=0),
        "withdrawals": _display_rub(summary.get("withdrawals"), precision=0),
        "income_net": _display_rub(summary.get("income_net"), precision=2),
        "commissions": _display_rub(summary.get("commissions"), precision=2),
        "taxes": _display_rub(summary.get("taxes"), precision=2),
        "operations_top": [
            {
                "local_date": _format_display_date(row.get("local_date")),
                "operation_group": row.get("operation_group"),
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in payload["operations_top"][:DEFAULT_AI_TOP_LIMIT]
        ],
    }


def _build_quality_facts(payload: dict[str, Any]) -> dict[str, Any]:
    quality = payload["data_quality"]
    return {
        "reconciliation_gap_abs": _display_rub(payload["summary_metrics"].get("reconciliation_gap_abs"), precision=0),
        "unknown_operation_group_count": quality.get("unknown_operation_group_count"),
        "mojibake_detected_count": quality.get("mojibake_detected_count"),
        "positions_missing_label_count": quality.get("positions_missing_label_count"),
        "asset_alias_rows_count": quality.get("asset_alias_rows_count"),
        "has_full_history_from_zero": quality.get("has_full_history_from_zero"),
        "income_events_available": quality.get("income_events_available"),
        "has_rebalance_targets": quality.get("has_rebalance_targets"),
    }


def _trim_ai_input(ai_input: dict[str, Any], max_input_chars: int | None) -> dict[str, Any]:
    if max_input_chars is None:
        return ai_input

    def _size() -> int:
        return len(json.dumps(ai_input, ensure_ascii=False))

    if _size() <= max_input_chars:
        return ai_input

    trim_paths = [
        ("cashflow_facts", "operations_top", 3),
        ("structure_facts", "top_positions", 3),
        ("mover_facts", "top_growth", 2),
        ("mover_facts", "top_drawdown", 2),
        ("contribution_facts", "income_contributors", 2),
        ("contribution_facts", "realized_winners", 2),
        ("contribution_facts", "realized_losers", 2),
        ("contribution_facts", "open_pl_leaders", 2),
        ("contribution_facts", "open_pl_laggards", 2),
        ("position_flow_facts", "new", 2),
        ("position_flow_facts", "closed", 2),
        ("position_flow_facts", "increased", 2),
        ("position_flow_facts", "decreased", 2),
    ]

    for section_name, field_name, min_keep in trim_paths:
        while _size() > max_input_chars and len(ai_input[section_name][field_name]) > min_keep:
            ai_input[section_name][field_name].pop()

    return ai_input


def build_monthly_ai_input(
    payload: dict[str, Any],
    *,
    max_input_chars: int | None = DEFAULT_AI_INPUT_MAX_CHARS,
) -> dict[str, Any]:
    ai_input = {
        "schema_version": MONTHLY_AI_INPUT_SCHEMA_VERSION,
        "meta": {
            "period_label_ru": payload["meta"]["period_label_ru"],
            "account_friendly_name": payload["meta"]["account_friendly_name"],
            "currency": payload["meta"]["currency"],
            "timezone": payload["meta"]["timezone"],
            "style": AI_INPUT_STYLE,
        },
        "overview_facts": _build_overview_facts(payload),
        "performance_facts": _build_performance_facts(payload),
        "structure_facts": _build_structure_facts(payload),
        "position_flow_facts": _build_position_flow_facts(payload),
        "mover_facts": _build_mover_facts(payload),
        "contribution_facts": _build_contribution_facts(payload),
        "cashflow_facts": _build_cashflow_facts(payload),
        "quality_facts": _build_quality_facts(payload),
    }
    return _trim_ai_input(ai_input, max_input_chars=max_input_chars)


def save_debug_report_payload(payload: dict[str, Any]) -> str:
    handle = tempfile.NamedTemporaryFile(
        prefix="monthly_report_payload_",
        suffix=".json",
        delete=False,
    )
    with open(handle.name, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)
    return handle.name


def build_monthly_report_payload(
    session,
    *,
    year: int,
    month: int,
    account_id: str | None = None,
) -> dict[str, Any]:
    bounds = _get_month_bounds(year, month)
    report_account_id = account_id or resolve_reporting_account_id(session)
    if report_account_id is None:
        raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

    period_start = bounds["period_start"]
    period_end = bounds["period_end"]
    period_start_dt = bounds["period_start_dt"]
    period_end_dt = bounds["period_end_dt"]
    period_end_exclusive = bounds["period_end_exclusive"]
    period_end_exclusive_dt = bounds["period_end_exclusive_dt"]

    start_snapshot, end_snapshot = get_month_snapshots(session, report_account_id, year, month)
    daily_snapshot_rows = list(
        get_period_daily_snapshot_rows(
            session,
            report_account_id,
            period_start,
            period_end_exclusive,
        )
    )
    if not daily_snapshot_rows or end_snapshot is None:
        raise ValueError(f"Пока нет снапшотов для отчёта за {MONTHS_RU[month]} {year}.")

    positions_month_start_raw = list(get_positions_for_snapshot(session, start_snapshot["id"])) if start_snapshot else []
    positions_month_end_raw = list(get_positions_for_snapshot(session, end_snapshot["id"]))
    asset_alias_rows = list(get_asset_alias_rows(session))
    alias_by_instrument_uid, alias_by_figi = build_asset_alias_lookup(asset_alias_rows)
    operations_rows = list(
        get_dataset_operations(
            session,
            account_id=report_account_id,
            start_dt=period_start_dt,
            end_dt=period_end_exclusive_dt,
        )
    )
    income_event_rows = list(get_income_events_for_period(session, report_account_id, period_start, period_end))
    instrument_eod_rows = list(get_instrument_eod_rows(session, report_account_id, period_start, period_end_exclusive))

    positions_month_start = _normalize_positions(
        positions_month_start_raw,
        alias_by_instrument_uid,
        alias_by_figi,
    )
    positions_month_end = _normalize_positions(
        positions_month_end_raw,
        alias_by_instrument_uid,
        alias_by_figi,
    )
    positions_current = list(positions_month_end)
    position_flow_groups = build_position_flow_groups(positions_month_start, positions_month_end)

    normalized_operations, operation_aggregates = _build_operations_month_data(
        operations_rows,
        alias_by_instrument_uid,
        alias_by_figi,
    )
    normalized_income_events, income_net_by_day, income_tax_by_day = _build_income_month_data(
        income_event_rows,
        alias_by_figi,
    )

    twr_series = compute_twr_timeseries(session, report_account_id)
    twr_by_date: dict[date, Decimal] = {}
    if twr_series is not None:
        series_dates, _values, series_returns = twr_series
        twr_by_date = {
            item_date: normalize_decimal(round(item_return * 100.0, 6))
            for item_date, item_return in zip(series_dates, series_returns)
            if period_start <= item_date < period_end_exclusive
        }

    timeseries_daily = _build_timeseries_daily(
        daily_snapshot_rows,
        deposits_by_day=operation_aggregates["deposits_by_day"],
        withdrawals_by_day=operation_aggregates["withdrawals_by_day"],
        income_net_by_day=income_net_by_day,
        commissions_by_day=operation_aggregates["commissions_by_day"],
        taxes_by_day=operation_aggregates["taxes_by_day"],
        income_tax_by_day=income_tax_by_day,
        twr_by_date=twr_by_date,
    )

    deposits = normalize_decimal(get_deposits_for_period(session, report_account_id, period_start_dt, period_end_exclusive_dt))
    net_external_flow = normalize_decimal(
        get_net_external_flow_for_period(
            session,
            report_account_id,
            period_start_dt,
            period_end_exclusive_dt,
        )
    )
    withdrawals = deposits - net_external_flow
    coupon_net, dividend_net = get_income_for_period(session, report_account_id, period_start_dt, period_end_dt)
    coupon_net = normalize_decimal(coupon_net)
    dividend_net = normalize_decimal(dividend_net)
    income_net = coupon_net + dividend_net
    commissions = normalize_decimal(get_commissions_for_period(session, report_account_id, period_start_dt, period_end_dt))
    taxes = normalize_decimal(get_taxes_for_period(session, report_account_id, period_start_dt, period_end_dt))
    deposits_ytd = normalize_decimal(
        get_deposits_for_period(
            session,
            report_account_id,
            datetime(year, 1, 1),
            period_end_exclusive_dt,
        )
    )

    reconciliation_rows, positions_value_sum, reconciliation_gap_abs = _build_reconciliation_rows(
        end_snapshot,
        positions_month_end_raw,
    )
    targets = get_rebalance_targets(session, report_account_id)
    rebalance_snapshot = _build_rebalance_snapshot(
        end_snapshot.get("snapshot_date") if end_snapshot is not None else None,
        positions_month_end_raw,
        targets,
    )

    instrument_eod_timeseries = build_instrument_eod_timeseries(
        instrument_eod_rows,
        alias_by_instrument_uid,
        alias_by_figi,
    )
    instrument_movers = build_instrument_movers(instrument_eod_timeseries)
    realized_by_asset_raw, _realized_total = compute_realized_by_asset(
        session,
        report_account_id,
        period_start_dt,
        period_end_exclusive_dt,
    )
    income_by_asset_raw, _income_total = compute_income_by_asset_net(
        session,
        report_account_id,
        period_start_dt,
        period_end_exclusive_dt,
    )
    realized_by_asset = _normalize_realized_rows(realized_by_asset_raw, alias_by_figi)
    income_by_asset = _build_income_by_asset_rows(normalized_income_events)
    if not income_by_asset:
        income_by_asset = _normalize_realized_rows(income_by_asset_raw, alias_by_figi)
        for row in income_by_asset:
            row["income_kind"] = "income"
    open_pl_end = _build_open_pl_end_rows(positions_current)

    start_value, start_snapshot_id = _resolve_start_metrics(start_snapshot, timeseries_daily)
    period_twr_pct = timeseries_daily[-1]["twr_pct"] if timeseries_daily else None
    summary_metrics = _build_summary_metrics(
        year=year,
        period_end=period_end,
        end_snapshot=end_snapshot,
        daily_rows=timeseries_daily,
        positions_current=positions_current,
        deposits=deposits,
        withdrawals=withdrawals,
        income_net=income_net,
        coupon_net=coupon_net,
        dividend_net=dividend_net,
        commissions=commissions,
        taxes=taxes,
        deposits_ytd=deposits_ytd,
        plan_annual_contrib=normalize_decimal(PLAN_ANNUAL_CONTRIB_RUB),
        reconciliation_gap_abs=reconciliation_gap_abs,
        positions_value_sum=positions_value_sum,
        income_events_count=len(normalized_income_events),
        start_snapshot=start_snapshot,
        start_value=start_value,
        net_external_flow=net_external_flow,
        period_twr_pct=period_twr_pct,
    )
    summary_metrics["open_pl_end_total"] = normalize_decimal(get_unrealized_at_period_end(session, report_account_id, period_end_exclusive_dt))

    positions_missing_label_count = sum(1 for row in positions_current if not ((row.get("ticker") or "").strip() or (row.get("name") or "").strip()))

    payload = {
        "schema_version": MONTHLY_REPORT_PAYLOAD_SCHEMA_VERSION,
        "meta": {
            "report_kind": "monthly_review",
            "account_id": report_account_id,
            "account_friendly_name": ACCOUNT_FRIENDLY_NAME,
            "timezone": TZ_NAME,
            "currency": _resolve_currency(daily_snapshot_rows, positions_current),
            "period_year": year,
            "period_month": month,
            "period_label_ru": f"{MONTHS_RU[month]} {year}",
            "period_start": period_start,
            "period_end": period_end,
            "generated_at_utc": datetime.now(timezone.utc),
            "has_ai_narrative": False,
            "data_schema_version": 1,
            "source_snapshot_start_id": start_snapshot_id,
            "source_snapshot_end_id": end_snapshot.get("id") if end_snapshot is not None else None,
            "source_snapshot_count": len(daily_snapshot_rows),
        },
        "summary_metrics": summary_metrics,
        "timeseries_daily": timeseries_daily,
        "positions_current": positions_current,
        "positions_month_start": positions_month_start,
        "positions_month_end": positions_month_end,
        "position_flow_groups": position_flow_groups,
        "instrument_eod_timeseries": instrument_eod_timeseries,
        "instrument_movers": instrument_movers,
        "realized_by_asset": realized_by_asset,
        "income_by_asset": income_by_asset,
        "open_pl_end": open_pl_end,
        "operations_top": build_operations_top(normalized_operations),
        "income_events": normalized_income_events,
        "reconciliation_by_asset_type": reconciliation_rows,
        "data_quality": {
            "unknown_operation_group_count": operation_aggregates["unknown_operation_group_count"],
            "mojibake_detected_count": operation_aggregates["mojibake_detected_count"],
            "positions_missing_label_count": positions_missing_label_count,
            "has_full_history_from_zero": start_value == Decimal("0") if start_value is not None else False,
            "income_events_available": True,
            "asset_alias_rows_count": len(asset_alias_rows),
            "has_rebalance_targets": bool(targets),
        },
        "rebalance_snapshot": rebalance_snapshot,
    }

    serialized_payload = serialize_report_payload(payload)
    if REPORT_DEBUG_SAVE_PAYLOAD:
        debug_path = save_debug_report_payload(serialized_payload)
        logger.info(
            "monthly_report_payload_debug_saved",
            "Saved monthly report payload to a debug JSON file.",
            {
                "period": f"{year}-{month:02d}",
                "path": debug_path,
            },
        )

    logger.info(
        "monthly_report_payload_built",
        "Built deterministic monthly report payload.",
        {
            "period": f"{year}-{month:02d}",
            "account_id": report_account_id,
            "positions_count": len(positions_current),
            "daily_points": len(timeseries_daily),
            "operations_top_count": len(payload["operations_top"]),
            "income_events_count": len(normalized_income_events),
        },
    )
    return serialized_payload


def create_monthly_report_payload(
    *,
    year: int | None = None,
    month: int | None = None,
    account_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_year, resolved_month = resolve_monthly_report_period(
        year=year,
        month=month,
        now=now,
    )
    with db_session() as session:
        return build_monthly_report_payload(
            session,
            year=resolved_year,
            month=resolved_month,
            account_id=account_id,
        )
