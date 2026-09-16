from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from financetracker.common.logging_setup import get_logger
from financetracker.domain.cashflows import (
    build_operation_cashflows_for_snapshot_interval,
    normalize_operation_currency,
    rebase_twr_to_period,
)
from financetracker.domain.assets import (
    build_asset_alias_lookup,
)
from financetracker.reporting.debug_artifacts import save_debug_text
from financetracker.reporting.payload_identity import (
    build_asset_identity as _build_asset_identity,  # noqa: F401
    build_position_flow_groups,
    normalize_positions as _normalize_positions,
    pick_alias_row as _pick_alias_row,  # noqa: F401
)
from financetracker.reporting.payload_activity import (
    build_income_by_asset_rows as _build_income_by_asset_rows,
    build_income_month_data as _build_income_month_data,
    build_open_pl_end_rows as _build_open_pl_end_rows,
    build_operations_month_data as _build_operations_month_data,
    build_operations_top,
    normalize_realized_rows as _normalize_realized_rows,
)
from financetracker.reporting.payload_ai_input import (  # noqa: F401
    _build_cashflow_facts,
    _build_overview_facts,
    build_monthly_ai_input,
)
from financetracker.reporting.payload_timeseries import (
    build_timeseries_daily as _build_timeseries_daily,
    compute_period_pnl as _compute_period_pnl,  # noqa: F401
    find_best_and_worst_day as _find_best_and_worst_day,  # noqa: F401
    build_instrument_eod_timeseries,
    build_instrument_movers,
    resolve_start_metrics as _resolve_start_metrics,
)
from financetracker.reporting.payload_summary import (
    build_rebalance_snapshot as _build_rebalance_snapshot,
    build_reconciliation_rows as _build_reconciliation_rows,
    build_summary_metrics as _build_summary_metrics,
)
from financetracker.reporting.repository import (
    compute_realized_by_asset,
    compute_income_by_asset_net,
    compute_twr_timeseries,
    get_asset_alias_rows,
    get_dataset_operations,
    get_income_currency_breakdown_for_period,
    get_income_events_for_period,
    get_income_for_period,
    get_iis_tax_deductions_for_period,
    get_instrument_eod_rows,
    get_month_snapshots,
    get_period_daily_snapshot_rows,
    get_positions_for_snapshot,
    get_unrealized_at_period_end,
    resolve_reporting_account_id,
    get_commissions_for_period,
    get_deposits_for_period,
    get_taxes_for_period,
    get_tax_refunds_for_period,
    get_rebalance_targets,
)
from financetracker.reporting.runtime import (
    ACCOUNT_FRIENDLY_NAME,
    MONTHS_RU,
    PLAN_ANNUAL_CONTRIB_RUB,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TZ,
    TZ_NAME,
    db_session,
    decimal_to_str,
    normalize_decimal,
    to_iso_datetime,
    local_reporting_bounds_utc,
)
MONTHLY_REPORT_PAYLOAD_SCHEMA_VERSION = "monthly_report_payload.v1"
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
    period_start_dt, period_end_exclusive_dt = local_reporting_bounds_utc(
        period_start,
        period_end_exclusive,
    )
    return {
        "period_start": period_start,
        "period_end": period_end_exclusive - timedelta(days=1),
        "period_end_exclusive": period_end_exclusive,
        "period_start_dt": period_start_dt,
        "period_end_dt": period_end_exclusive_dt - timedelta(microseconds=1),
        "period_end_exclusive_dt": period_end_exclusive_dt,
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
    return "UNKNOWN"


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


def save_debug_report_payload(payload: dict[str, Any]) -> str:
    return save_debug_text(
        kind="payload",
        suffix=".json",
        text=json.dumps(payload, ensure_ascii=False, indent=2),
    )


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
    base_currency = normalize_operation_currency(_resolve_currency(daily_snapshot_rows, positions_current))
    (
        normalized_income_events,
        income_net_by_day,
        income_tax_by_day,
        income_tax_refunds_by_day,
    ) = _build_income_month_data(
        income_event_rows,
        alias_by_figi,
        base_currency=base_currency,
    )

    twr_series = compute_twr_timeseries(session, report_account_id)
    twr_by_date: dict[date, Decimal] = {}
    if twr_series is not None:
        series_dates, _values, series_returns = twr_series
        period_twr_by_date = rebase_twr_to_period(
            series_dates,
            series_returns,
            period_start,
            period_end_exclusive,
        )
        twr_by_date = {
            item_date: normalize_decimal(round(item_return * 100.0, 6))
            for item_date, item_return in period_twr_by_date.items()
        }

    timeseries_daily = _build_timeseries_daily(
        daily_snapshot_rows,
        deposits_by_day={},
        iis_tax_deductions_by_day={},
        withdrawals_by_day={},
        income_net_by_day=income_net_by_day,
        commissions_by_day={},
        taxes_by_day={},
        income_tax_by_day=income_tax_by_day,
        twr_by_date=twr_by_date,
        tax_refunds_by_day={},
        income_tax_refunds_by_day=income_tax_refunds_by_day,
        base_currency=base_currency,
        operation_cashflows_by_currency_day=operation_aggregates["cashflows_by_currency_day"],
    )

    operation_cashflows_by_currency = build_operation_cashflows_for_snapshot_interval(
        operation_aggregates["cashflows_by_currency_day"],
        period_start - timedelta(days=1),
        period_end,
    )
    base_period_cashflows = None
    if base_currency != "UNKNOWN":
        base_period_cashflows = next(
            (item for item in operation_cashflows_by_currency if item["currency"] == base_currency),
            None,
        )
    deposits = normalize_decimal(base_period_cashflows["deposits"]) if base_period_cashflows else Decimal("0")
    withdrawals = (
        normalize_decimal(base_period_cashflows["withdrawals"]) if base_period_cashflows else Decimal("0")
    )
    net_external_flow = deposits - withdrawals
    coupon_net, dividend_net = get_income_for_period(
        session,
        report_account_id,
        period_start_dt,
        period_end_dt,
        currency=base_currency,
    )
    coupon_net = normalize_decimal(coupon_net)
    dividend_net = normalize_decimal(dividend_net)
    income_net = coupon_net + dividend_net
    iis_tax_deduction_income = normalize_decimal(
        get_iis_tax_deductions_for_period(
            session,
            report_account_id,
            period_start_dt,
            period_end_exclusive_dt,
        )
    )
    commissions = normalize_decimal(get_commissions_for_period(session, report_account_id, period_start_dt, period_end_dt))
    taxes = normalize_decimal(get_taxes_for_period(session, report_account_id, period_start_dt, period_end_dt))
    tax_refunds = normalize_decimal(
        get_tax_refunds_for_period(session, report_account_id, period_start_dt, period_end_dt)
    )
    income_by_currency = [
        {
            "currency": str(row.get("currency") or "UNKNOWN").strip().upper() or "UNKNOWN",
            "coupons": normalize_decimal(row.get("coupons")),
            "dividends": normalize_decimal(row.get("dividends")),
            "taxes": normalize_decimal(row.get("taxes")),
            "tax_refunds": normalize_decimal(row.get("tax_refunds")),
        }
        for row in get_income_currency_breakdown_for_period(
            session, report_account_id, period_start_dt, period_end_dt
        )
    ]
    deposits_ytd = normalize_decimal(
        get_deposits_for_period(
            session,
            report_account_id,
            local_reporting_bounds_utc(
                date(year, 1, 1),
                date(year + 1, 1, 1),
            )[0],
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
        iis_tax_deduction_income=iis_tax_deduction_income,
        coupon_net=coupon_net,
        dividend_net=dividend_net,
        commissions=commissions,
        taxes=taxes,
        tax_refunds=tax_refunds,
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
            "currency": base_currency,
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
        "income_by_currency": income_by_currency,
        "operation_cashflows_by_currency": operation_cashflows_by_currency,
        "reconciliation_by_asset_type": reconciliation_rows,
        "data_quality": {
            "unknown_operation_group_count": operation_aggregates["unknown_operation_group_count"],
            "mojibake_detected_count": operation_aggregates["mojibake_detected_count"],
            "positions_missing_label_count": positions_missing_label_count,
            "has_full_history_from_zero": start_value == Decimal("0") if start_value is not None else False,
            "income_events_available": True,
            "asset_alias_rows_count": len(asset_alias_rows),
            "has_rebalance_targets": bool(targets),
            "unknown_income_currency_warning": any(
                row["currency"] == "UNKNOWN" for row in income_by_currency
            ),
            "unsupported_operation_currencies": [
                item["currency"]
                for item in operation_cashflows_by_currency
                if base_currency == "UNKNOWN" or item["currency"] != base_currency
            ],
        },
        "rebalance_snapshot": rebalance_snapshot,
    }

    serialized_payload = serialize_report_payload(payload)
    if REPORT_DEBUG_SAVE_PAYLOAD:
        save_debug_report_payload(serialized_payload)
        logger.info(
            "monthly_report_payload_debug_saved",
            "Saved monthly report payload to a debug JSON file.",
            {
                "period": f"{year}-{month:02d}",
                "artifact_kind": "payload",
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
