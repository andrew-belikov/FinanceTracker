from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping

from common.text_utils import has_mojibake
from queries import (
    compute_income_by_asset_net,
    compute_realized_by_asset,
    get_asset_alias_rows,
    get_commissions_for_period,
    get_daily_snapshot_rows,
    get_dataset_operations,
    get_deposits_for_period,
    get_income_events_for_period,
    get_income_for_period,
    get_latest_snapshot_with_totals_before_date,
    get_month_snapshots,
    get_net_external_flow_for_period,
    get_positions_for_snapshot,
    get_rebalance_targets,
    get_taxes_for_period,
    get_unrealized_at_period_end,
    resolve_reporting_account_id,
)
from runtime import (
    ACCOUNT_FRIENDLY_NAME,
    MONTHS_RU_GENITIVE,
    PLAN_ANNUAL_CONTRIB_RUB,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TZ,
    TZ_NAME,
    decimal_to_str,
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
    compute_rebalance_plan,
    compute_twr_series,
    is_income_event_backed_tax_operation,
)


MONTHLY_REPORT_SCHEMA_VERSION = "monthly_report_payload.v1"
MONTHLY_REPORT_KIND = "monthly_review"
DEFAULT_OPERATIONS_TOP_LIMIT = 15
DEFAULT_INSTRUMENT_MOVERS_LIMIT = 5


class MonthlyReportPayloadError(ValueError):
    pass


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
        raise MonthlyReportPayloadError("Поле year должно быть в диапазоне 1900..2100.")
    if resolved_month < 1 or resolved_month > 12:
        raise MonthlyReportPayloadError("Поле month должно быть в диапазоне 1..12.")

    return resolved_year, resolved_month


def _month_bounds(year: int, month: int) -> tuple[date, date, datetime, datetime, date]:
    month_start_date = date(year, month, 1)
    if month == 12:
        next_month_start_date = date(year + 1, 1, 1)
    else:
        next_month_start_date = date(year, month + 1, 1)

    month_start_dt = datetime.combine(month_start_date, time.min)
    next_month_start_dt = datetime.combine(next_month_start_date, time.min)
    month_end_date = next_month_start_date - timedelta(days=1)
    return month_start_date, next_month_start_date, month_start_dt, next_month_start_dt, month_end_date


def _month_label_ru(year: int, month: int) -> str:
    return f"{MONTHS_RU_GENITIVE[month]} {year}"


def _decimal_or_zero(value: Any) -> Decimal:
    return normalize_decimal(value)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    return normalize_decimal(value)


def _build_monthly_logical_asset_id(
    *,
    asset_uid: str | None,
    instrument_uid: str | None,
    figi: str | None,
    ticker: str | None,
) -> str | None:
    logical_asset_id = build_logical_asset_id(
        asset_uid=asset_uid,
        instrument_uid=instrument_uid,
        figi=figi,
    )
    if logical_asset_id is not None:
        return logical_asset_id
    ticker_value = (ticker or "").strip()
    return ticker_value or None


def serialize_monthly_report_payload(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_to_str(value)
    if isinstance(value, datetime):
        normalized = value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return normalized.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: serialize_monthly_report_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialize_monthly_report_payload(item) for item in value]
    if isinstance(value, tuple):
        return [serialize_monthly_report_payload(item) for item in value]
    return value


def _normalize_position_row(
    row: Mapping[str, Any],
    *,
    alias_by_instrument_uid: dict[str, dict] | None = None,
    alias_by_figi: dict[str, dict] | None = None,
) -> dict[str, Any]:
    instrument_uid = row.get("instrument_uid")
    figi = row.get("figi")
    alias_row = None
    if instrument_uid and alias_by_instrument_uid is not None:
        alias_row = alias_by_instrument_uid.get(instrument_uid)
    if alias_row is None and figi and alias_by_figi is not None:
        alias_row = alias_by_figi.get(figi)

    asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
    ticker = (row.get("ticker") or (alias_row.get("ticker") if alias_row is not None else "") or "").strip()
    name = (row.get("name") or (alias_row.get("name") if alias_row is not None else "") or "").strip()
    instrument_type = (row.get("instrument_type") or "other").strip().lower()
    logical_asset_id = _build_monthly_logical_asset_id(
        asset_uid=asset_uid,
        instrument_uid=instrument_uid,
        figi=figi,
        ticker=ticker,
    )

    return {
        "logical_asset_id": logical_asset_id,
        "asset_uid": asset_uid,
        "instrument_uid": instrument_uid,
        "figi": figi,
        "ticker": ticker,
        "name": name,
        "instrument_type": instrument_type,
        "quantity": _decimal_or_zero(row.get("quantity")),
        "currency": row.get("currency"),
        "position_value": _decimal_or_zero(row.get("position_value")),
        "expected_yield": _decimal_or_zero(row.get("expected_yield")),
        "expected_yield_pct": _decimal_or_none(row.get("expected_yield_pct")),
        "weight_pct": _decimal_or_none(row.get("weight_pct")),
    }


def _group_positions_by_identity(
    rows: list[Mapping[str, Any]],
    *,
    alias_by_instrument_uid: dict[str, dict] | None = None,
    alias_by_figi: dict[str, dict] | None = None,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        normalized = _normalize_position_row(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        key = normalized["logical_asset_id"] or normalized["figi"] or normalized["ticker"]
        if key is None:
            continue
        bucket = grouped.setdefault(
            key,
            {
                "logical_asset_id": key,
                "asset_uid": normalized["asset_uid"],
                "instrument_uid": normalized["instrument_uid"],
                "figi": normalized["figi"],
                "ticker": normalized["ticker"],
                "name": normalized["name"],
                "instrument_type": normalized["instrument_type"],
                "quantity": Decimal("0"),
                "position_value": Decimal("0"),
                "expected_yield": Decimal("0"),
                "expected_yield_pct": normalized["expected_yield_pct"],
                "weight_pct": normalized["weight_pct"],
            },
        )
        bucket["quantity"] += normalized["quantity"]
        bucket["position_value"] += normalized["position_value"]
        bucket["expected_yield"] += normalized["expected_yield"]
        if bucket["expected_yield_pct"] is None:
            bucket["expected_yield_pct"] = normalized["expected_yield_pct"]
        if bucket["weight_pct"] is None:
            bucket["weight_pct"] = normalized["weight_pct"]
        if not bucket["ticker"] and normalized["ticker"]:
            bucket["ticker"] = normalized["ticker"]
        if not bucket["name"] and normalized["name"]:
            bucket["name"] = normalized["name"]
        if bucket["asset_uid"] is None and normalized["asset_uid"] is not None:
            bucket["asset_uid"] = normalized["asset_uid"]
        if bucket["instrument_uid"] is None and normalized["instrument_uid"] is not None:
            bucket["instrument_uid"] = normalized["instrument_uid"]
        if bucket["figi"] is None and normalized["figi"] is not None:
            bucket["figi"] = normalized["figi"]
    return grouped


def build_position_flow_groups(
    start_positions: list[Mapping[str, Any]],
    end_positions: list[Mapping[str, Any]],
    *,
    alias_by_instrument_uid: dict[str, dict] | None = None,
    alias_by_figi: dict[str, dict] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    start_grouped = _group_positions_by_identity(
        start_positions,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )
    end_grouped = _group_positions_by_identity(
        end_positions,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )

    groups = {
        "new": [],
        "closed": [],
        "increased": [],
        "decreased": [],
    }

    for key in sorted(set(start_grouped) | set(end_grouped)):
        start_row = start_grouped.get(key)
        end_row = end_grouped.get(key)
        start_qty = start_row["quantity"] if start_row is not None else Decimal("0")
        end_qty = end_row["quantity"] if end_row is not None else Decimal("0")
        start_value = start_row["position_value"] if start_row is not None else Decimal("0")
        end_value = end_row["position_value"] if end_row is not None else Decimal("0")
        delta_qty = end_qty - start_qty
        delta_value = end_value - start_value

        template = end_row or start_row
        if template is None:
            continue
        payload_row = {
            "logical_asset_id": template["logical_asset_id"],
            "ticker": template["ticker"],
            "name": template["name"],
            "instrument_type": template["instrument_type"],
            "start_qty": start_qty,
            "end_qty": end_qty,
            "delta_qty": delta_qty,
            "start_value": start_value,
            "end_value": end_value,
            "delta_value": delta_value,
        }

        if start_row is None and end_row is not None:
            groups["new"].append(payload_row)
        elif start_row is not None and end_row is None:
            groups["closed"].append(payload_row)
        elif delta_qty > 0:
            groups["increased"].append(payload_row)
        elif delta_qty < 0:
            groups["decreased"].append(payload_row)

    for group_name in groups:
        groups[group_name] = sorted(
            groups[group_name],
            key=lambda row: (
                abs(row["delta_value"]),
                (row["ticker"] or ""),
                (row["name"] or ""),
                (row["logical_asset_id"] or ""),
            ),
            reverse=True,
        )

    return groups


def build_instrument_eod_timeseries(
    snapshot_rows: list[Mapping[str, Any]],
    positions_by_snapshot_id: Mapping[int, list[Mapping[str, Any]]],
    *,
    alias_by_instrument_uid: dict[str, dict] | None = None,
    alias_by_figi: dict[str, dict] | None = None,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}

    for snapshot_row in snapshot_rows:
        snapshot_id = snapshot_row["id"]
        snapshot_date = snapshot_row["snapshot_date"]
        positions = positions_by_snapshot_id.get(snapshot_id, [])
        for position in positions:
            normalized = _normalize_position_row(
                position,
                alias_by_instrument_uid=alias_by_instrument_uid,
                alias_by_figi=alias_by_figi,
            )
            key = normalized["logical_asset_id"] or normalized["figi"] or normalized["ticker"]
            if key is None:
                continue

            bucket = buckets.setdefault(
                key,
                {
                    "logical_asset_id": key,
                    "asset_uid": normalized["asset_uid"],
                    "instrument_uid": normalized["instrument_uid"],
                    "figi": normalized["figi"],
                    "ticker": normalized["ticker"],
                    "name": normalized["name"],
                    "instrument_type": normalized["instrument_type"],
                    "series": [],
                },
            )

            if not bucket["ticker"] and normalized["ticker"]:
                bucket["ticker"] = normalized["ticker"]
            if not bucket["name"] and normalized["name"]:
                bucket["name"] = normalized["name"]
            if bucket["asset_uid"] is None and normalized["asset_uid"] is not None:
                bucket["asset_uid"] = normalized["asset_uid"]
            if bucket["instrument_uid"] is None and normalized["instrument_uid"] is not None:
                bucket["instrument_uid"] = normalized["instrument_uid"]
            if bucket["figi"] is None and normalized["figi"] is not None:
                bucket["figi"] = normalized["figi"]

            bucket["series"].append(
                {
                    "date": snapshot_date,
                    "snapshot_id": snapshot_id,
                    "quantity": normalized["quantity"],
                    "position_value": normalized["position_value"],
                    "expected_yield": normalized["expected_yield"],
                    "expected_yield_pct": normalized["expected_yield_pct"],
                    "weight_pct": normalized["weight_pct"],
                }
            )

    result: list[dict[str, Any]] = []
    for bucket in buckets.values():
        series = sorted(
            bucket["series"],
            key=lambda point: (point["date"], point["snapshot_id"]),
        )
        if not series:
            continue

        position_values = [point["position_value"] for point in series]
        expected_yields = [point["expected_yield"] for point in series]
        min_position_value = min(position_values)
        max_position_value = max(position_values)
        min_expected_yield = min(expected_yields)
        max_expected_yield = max(expected_yields)
        end_point = series[-1]
        start_point = series[0]
        month_change_abs = end_point["position_value"] - start_point["position_value"]
        month_change_pct = None
        if start_point["position_value"] != 0:
            month_change_pct = month_change_abs / start_point["position_value"] * Decimal("100")

        stats = {
            "eod_min_position_value": min_position_value,
            "eod_min_position_value_date": next(point["date"] for point in series if point["position_value"] == min_position_value),
            "eod_max_position_value": max_position_value,
            "eod_max_position_value_date": next(point["date"] for point in series if point["position_value"] == max_position_value),
            "eod_end_position_value": end_point["position_value"],
            "eod_min_expected_yield": min_expected_yield,
            "eod_min_expected_yield_date": next(point["date"] for point in series if point["expected_yield"] == min_expected_yield),
            "eod_max_expected_yield": max_expected_yield,
            "eod_max_expected_yield_date": next(point["date"] for point in series if point["expected_yield"] == max_expected_yield),
            "eod_end_expected_yield": end_point["expected_yield"],
            "max_rise_abs": max_position_value - min_position_value,
            "max_drawdown_abs": min_position_value - max_position_value,
            "month_change_abs": month_change_abs,
            "month_change_pct": month_change_pct,
        }

        result.append(
            {
                "logical_asset_id": bucket["logical_asset_id"],
                "asset_uid": bucket["asset_uid"],
                "instrument_uid": bucket["instrument_uid"],
                "figi": bucket["figi"],
                "ticker": bucket["ticker"],
                "name": bucket["name"],
                "instrument_type": bucket["instrument_type"],
                "series": series,
                "stats": stats,
            }
        )

    result.sort(
        key=lambda row: (
            row["stats"]["eod_end_position_value"],
            row["ticker"] or "",
            row["name"] or "",
            row["logical_asset_id"] or "",
        ),
        reverse=True,
    )
    return result


def build_instrument_movers(
    instrument_eod_timeseries: list[Mapping[str, Any]],
    *,
    limit: int = DEFAULT_INSTRUMENT_MOVERS_LIMIT,
) -> dict[str, list[dict[str, Any]]]:
    growth_candidates: list[dict[str, Any]] = []
    drawdown_candidates: list[dict[str, Any]] = []

    for row in instrument_eod_timeseries:
        series = list(row.get("series") or [])
        if len(series) < 2:
            continue

        start_point = series[0]
        end_point = series[-1]
        month_change_abs = end_point["position_value"] - start_point["position_value"]
        payload_row = {
            "logical_asset_id": row.get("logical_asset_id"),
            "ticker": row.get("ticker"),
            "name": row.get("name"),
            "metric_kind": "position_value",
            "rise_abs": month_change_abs if month_change_abs > 0 else None,
            "drawdown_abs": month_change_abs if month_change_abs < 0 else None,
            "start_date": start_point["date"],
            "end_date": end_point["date"],
            "end_expected_yield": end_point["expected_yield"],
            "end_expected_yield_pct": end_point["expected_yield_pct"],
        }
        if month_change_abs > 0:
            growth_candidates.append(payload_row)
        elif month_change_abs < 0:
            drawdown_candidates.append(payload_row)

    growth_candidates.sort(
        key=lambda row: (
            row["rise_abs"] or Decimal("0"),
            (row["ticker"] or ""),
            (row["name"] or ""),
        ),
        reverse=True,
    )
    drawdown_candidates.sort(
        key=lambda row: (
            row["drawdown_abs"] or Decimal("0"),
            (row["ticker"] or ""),
            (row["name"] or ""),
        ),
    )

    return {
        "top_growth": growth_candidates[:limit],
        "top_drawdown": drawdown_candidates[:limit],
    }


def _build_operations_top(
    operations_rows: list[Mapping[str, Any]],
    *,
    alias_by_instrument_uid: dict[str, dict] | None = None,
    alias_by_figi: dict[str, dict] | None = None,
    limit: int = DEFAULT_OPERATIONS_TOP_LIMIT,
) -> list[dict[str, Any]]:
    priority = {
        "deposit": 0,
        "withdrawal": 1,
        "sell": 2,
        "buy": 3,
        "commission": 4,
        "income_tax": 5,
        "dividend": 6,
        "coupon": 7,
        "other": 8,
    }

    normalized_rows: list[dict[str, Any]] = []
    for row in operations_rows:
        dt = row["date"]
        group = classify_operation_group(row.get("operation_type"))
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        alias_row = None
        if instrument_uid and alias_by_instrument_uid is not None:
            alias_row = alias_by_instrument_uid.get(instrument_uid)
        if alias_row is None and figi and alias_by_figi is not None:
            alias_row = alias_by_figi.get(figi)

        asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
        ticker = (row.get("figi") or (alias_row.get("ticker") if alias_row is not None else "") or "").strip()
        name = (row.get("name") or (alias_row.get("name") if alias_row is not None else "") or "").strip()
        logical_asset_id = _build_monthly_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=figi,
            ticker=ticker,
        )

        normalized_rows.append(
            {
                "date_utc": to_iso_datetime(dt),
                "local_date": to_local_market_date(dt),
                "operation_type": row.get("operation_type"),
                "operation_group": group,
                "logical_asset_id": logical_asset_id,
                "ticker": ticker,
                "name": name,
                "amount": _decimal_or_zero(row.get("amount")),
                "quantity": _decimal_or_none(row.get("quantity")),
                "description": row.get("description"),
                "_priority": priority.get(group, 99),
                "_sort_amount": abs(_decimal_or_zero(row.get("amount"))),
                "_operation_id": row.get("operation_id"),
            }
        )

    normalized_rows.sort(
        key=lambda row: (
            row["_sort_amount"],
            -row["_priority"],
            row["date_utc"] or "",
            row["_operation_id"] or "",
        ),
        reverse=True,
    )
    top_rows = []
    for row in normalized_rows[:limit]:
        top_rows.append(
            {
                key: value
                for key, value in row.items()
                if not key.startswith("_")
            }
        )
    return top_rows


def _build_income_events(
    income_rows: list[Mapping[str, Any]],
    *,
    alias_by_figi: dict[str, dict] | None = None,
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in income_rows:
        alias_row = alias_by_figi.get(row["figi"]) if row.get("figi") and alias_by_figi is not None else None
        asset_uid = alias_row.get("asset_uid") if alias_row is not None else None
        instrument_uid = alias_row.get("instrument_uid") if alias_row is not None else None
        logical_asset_id = _build_monthly_logical_asset_id(
            asset_uid=asset_uid,
            instrument_uid=instrument_uid,
            figi=row.get("figi"),
            ticker=row.get("ticker"),
        )
        normalized_rows.append(
            {
                "event_date": row.get("event_date"),
                "event_type": row.get("event_type"),
                "logical_asset_id": logical_asset_id,
                "figi": row.get("figi"),
                "ticker": row.get("ticker", ""),
                "instrument_name": row.get("instrument_name"),
                "gross_amount": _decimal_or_zero(row.get("gross_amount")),
                "tax_amount": _decimal_or_zero(row.get("tax_amount")),
                "net_amount": _decimal_or_zero(row.get("net_amount")),
                "net_yield_pct": _decimal_or_none(row.get("net_yield_pct")),
                "notified": row.get("notified"),
            }
        )
    return normalized_rows


def _build_open_pl_end(positions_end: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    total_abs = sum((abs(_decimal_or_zero(row.get("expected_yield"))) for row in positions_end), Decimal("0"))
    rows: list[dict[str, Any]] = []
    for row in positions_end:
        amount = _decimal_or_zero(row.get("expected_yield"))
        amount_pct = None
        if total_abs > 0:
            amount_pct = amount / total_abs * Decimal("100")
        rows.append(
            {
                "logical_asset_id": row.get("logical_asset_id"),
                "ticker": row.get("ticker"),
                "name": row.get("name"),
                "amount": amount,
                "amount_pct": amount_pct,
            }
        )
    rows.sort(
        key=lambda item: (
            item["amount"],
            (item["ticker"] or ""),
            (item["name"] or ""),
        ),
        reverse=True,
    )
    return rows


def _build_summary_metrics(
    *,
    start_snapshot: Mapping[str, Any],
    end_snapshot: Mapping[str, Any],
    daily_rows: list[Mapping[str, Any]],
    positions_end: list[Mapping[str, Any]],
    operations_rows: list[Mapping[str, Any]],
    income_rows: list[Mapping[str, Any]],
    period_year: int,
    period_month: int,
    period_end_date: date,
    month_twr_pct: Decimal | None,
    reconciliation_gap_abs: Decimal,
    positions_value_sum: Decimal,
    coupon_net: Decimal,
    dividend_net: Decimal,
    deposits_ytd: Decimal,
) -> dict[str, Any]:
    start_value = _decimal_or_zero(start_snapshot.get("total_value"))
    end_value = _decimal_or_zero(end_snapshot.get("total_value"))

    deposits = sum(
        (
            _decimal_or_zero(row["deposits"])
            for row in daily_rows
        ),
        Decimal("0"),
    )
    withdrawals = sum(
        (
            _decimal_or_zero(row["withdrawals"])
            for row in daily_rows
        ),
        Decimal("0"),
    )
    income_net = sum(
        (
            _decimal_or_zero(row["income_net"])
            for row in daily_rows
        ),
        Decimal("0"),
    )
    commissions = sum(
        (
            _decimal_or_zero(row["commissions"])
            for row in daily_rows
        ),
        Decimal("0"),
    )
    operation_taxes = sum(
        (
            _decimal_or_zero(row["operation_taxes"])
            for row in daily_rows
        ),
        Decimal("0"),
    )
    income_taxes = sum(
        (
            _decimal_or_zero(row["income_taxes"])
            for row in daily_rows
        ),
        Decimal("0"),
    )

    period_net_cashflow = deposits - withdrawals + income_net - commissions - operation_taxes
    period_pnl_abs = end_value - start_value - period_net_cashflow
    period_pnl_pct = None
    if start_value != 0:
        period_pnl_pct = period_pnl_abs / start_value * Decimal("100")

    best_day = max(daily_rows, key=lambda row: (_decimal_or_zero(row["day_pnl"]), row["date"]))
    worst_day = min(daily_rows, key=lambda row: (_decimal_or_zero(row["day_pnl"]), row["date"]))

    top_holding = positions_end[0] if positions_end else None
    ytd_days = (period_end_date - date(period_year, 1, 1)).days + 1
    year_days = (date(period_year, 12, 31) - date(period_year, 1, 1)).days + 1
    target_to_date = Decimal(str(PLAN_ANNUAL_CONTRIB_RUB)) * Decimal(ytd_days) / Decimal(year_days)
    plan_progress_pct = None
    if PLAN_ANNUAL_CONTRIB_RUB > 0:
        plan_progress_pct = deposits_ytd / Decimal(str(PLAN_ANNUAL_CONTRIB_RUB)) * Decimal("100")

    summary = {
        "start_value": start_value,
        "end_value": end_value,
        "current_value": end_value,
        "period_pnl_abs": period_pnl_abs,
        "period_pnl_pct": period_pnl_pct,
        "period_twr_pct": month_twr_pct,
        "net_external_flow": deposits - withdrawals,
        "deposits": deposits,
        "withdrawals": withdrawals,
        "income_net": income_net,
        "coupon_net": coupon_net,
        "dividend_net": dividend_net,
        "commissions": commissions,
        "taxes": income_taxes + operation_taxes,
        "deposits_ytd": Decimal(str(deposits_ytd)),
        "plan_annual_contrib": Decimal(str(PLAN_ANNUAL_CONTRIB_RUB)),
        "plan_progress_pct": plan_progress_pct,
        "target_to_date": target_to_date,
        "reconciliation_gap_abs": reconciliation_gap_abs,
        "positions_value_sum": positions_value_sum,
        "top_holding_name": top_holding["name"] if top_holding is not None else None,
        "top_holding_value": _decimal_or_zero(top_holding["position_value"]) if top_holding is not None else None,
        "top_holding_weight_pct": _decimal_or_none(top_holding["weight_pct"]) if top_holding is not None else None,
        "best_day_date": best_day["date"],
        "best_day_pnl": _decimal_or_zero(best_day["day_pnl"]),
        "worst_day_date": worst_day["date"],
        "worst_day_pnl": _decimal_or_zero(worst_day["day_pnl"]),
        "income_events_count": len(income_rows),
        "snapshot_count": len(daily_rows),
        "positions_count": len(positions_end),
        "operations_count": len(operations_rows),
    }
    return summary


def build_monthly_report_payload_raw(
    session,
    *,
    year: int | None = None,
    month: int | None = None,
    account_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_year, resolved_month = resolve_monthly_report_period(year=year, month=month, now=now)
    month_start_date, next_month_start_date, month_start_dt, next_month_start_dt, month_end_date = _month_bounds(
        resolved_year,
        resolved_month,
    )

    resolved_account_id = account_id or resolve_reporting_account_id(session)
    if resolved_account_id is None:
        raise MonthlyReportPayloadError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

    month_daily_rows_all = list(get_daily_snapshot_rows(session, resolved_account_id))
    month_daily_rows = [
        row
        for row in month_daily_rows_all
        if month_start_date <= row["snapshot_date"] < next_month_start_date
    ]
    if not month_daily_rows:
        raise MonthlyReportPayloadError("Пока нет снапшотов за выбранный месяц.")

    start_snapshot, end_snapshot = get_month_snapshots(session, resolved_account_id, resolved_year, resolved_month)
    if start_snapshot is None or end_snapshot is None:
        raise MonthlyReportPayloadError("Недостаточно снапшотов для monthly report.")

    end_snapshot_with_totals = get_latest_snapshot_with_totals_before_date(
        session,
        resolved_account_id,
        next_month_start_date,
    )
    if end_snapshot_with_totals is None:
        raise MonthlyReportPayloadError("Не удалось определить итоговый snapshot для monthly report.")

    alias_rows = list(get_asset_alias_rows(session))
    alias_by_instrument_uid, alias_by_figi = build_asset_alias_lookup(alias_rows)

    start_positions = list(get_positions_for_snapshot(session, start_snapshot["id"]))
    end_positions = list(get_positions_for_snapshot(session, end_snapshot["id"]))
    normalized_current_positions = [
        _normalize_position_row(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        for row in end_positions
    ]
    normalized_month_start_positions = [
        _normalize_position_row(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        for row in start_positions
    ]

    positions_by_snapshot_id = {
        row["id"]: list(get_positions_for_snapshot(session, row["id"]))
        for row in month_daily_rows
    }
    instrument_eod_timeseries = build_instrument_eod_timeseries(
        month_daily_rows,
        positions_by_snapshot_id,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )
    instrument_movers = build_instrument_movers(instrument_eod_timeseries)

    operations_rows = list(
        get_dataset_operations(
            session,
            account_id=resolved_account_id,
            start_dt=month_start_dt,
            end_dt=next_month_start_dt,
        )
    )
    income_rows = list(get_income_events_for_period(session, resolved_account_id, month_start_date, month_end_date))
    coupon_net, dividend_net = get_income_for_period(session, resolved_account_id, month_start_date, month_end_date)
    deposits_ytd = Decimal(
        str(
            get_deposits_for_period(
                session,
                resolved_account_id,
                datetime.combine(date(resolved_year, 1, 1), time.min),
                next_month_start_dt,
            )
        )
    )

    operations_top = _build_operations_top(
        operations_rows,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )
    income_events = _build_income_events(income_rows, alias_by_figi=alias_by_figi)
    position_flow_groups = build_position_flow_groups(
        normalized_month_start_positions,
        normalized_current_positions,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )
    open_pl_end = _build_open_pl_end(normalized_current_positions)

    deposits_by_day: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
    withdrawals_by_day: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
    commissions_by_day: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
    operation_taxes_by_day: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
    income_net_by_day: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
    income_taxes_by_day: dict[date, Decimal] = defaultdict(lambda: Decimal("0"))
    unknown_operation_groups = 0
    mojibake_detected_count = 0

    income_net_total = Decimal("0")
    for row in income_events:
        event_date = row["event_date"]
        income_net = _decimal_or_zero(row["net_amount"])
        income_tax = abs(_decimal_or_zero(row["tax_amount"]))
        income_net_total += income_net
        income_net_by_day[event_date] += income_net
        income_taxes_by_day[event_date] += income_tax

    for row in operations_rows:
        local_date = to_local_market_date(row.get("date"))
        group = classify_operation_group(row.get("operation_type"))
        if group == "other":
            unknown_operation_groups += 1
        description = row.get("description") or ""
        if has_mojibake(description):
            mojibake_detected_count += 1
        amount = _decimal_or_zero(row.get("amount"))
        amount_abs = abs(amount)
        if local_date is None:
            continue
        if group == "deposit":
            deposits_by_day[local_date] += amount_abs
        elif group == "withdrawal":
            withdrawals_by_day[local_date] += amount_abs
        elif group == "commission":
            commissions_by_day[local_date] += amount_abs
        elif group == "income_tax" and not is_income_event_backed_tax_operation(row.get("operation_type")):
            operation_taxes_by_day[local_date] += amount_abs

    daily_rows: list[dict[str, Any]] = []
    previous_value = _decimal_or_zero(start_snapshot["total_value"])
    twr_input_rows = [
        {
            "snapshot_date": start_snapshot["snapshot_date"],
            "total_value": previous_value,
        }
    ]
    for row in month_daily_rows:
        snapshot_date = row["snapshot_date"]
        portfolio_value = _decimal_or_zero(row["total_value"])
        deposits = deposits_by_day.get(snapshot_date, Decimal("0"))
        withdrawals = withdrawals_by_day.get(snapshot_date, Decimal("0"))
        income_net = income_net_by_day.get(snapshot_date, Decimal("0"))
        commissions = commissions_by_day.get(snapshot_date, Decimal("0"))
        operation_taxes = operation_taxes_by_day.get(snapshot_date, Decimal("0"))
        income_taxes = income_taxes_by_day.get(snapshot_date, Decimal("0"))
        net_cashflow = deposits - withdrawals + income_net - commissions - operation_taxes
        day_pnl = portfolio_value - previous_value - net_cashflow
        previous_value = portfolio_value

        daily_rows.append(
            {
                "snapshot_id": row["id"],
                "snapshot_at_utc": row["snapshot_at"],
                "date": snapshot_date,
                "portfolio_value": portfolio_value,
                "expected_yield": _decimal_or_zero(row.get("expected_yield")),
                "expected_yield_pct": _decimal_or_none(row.get("expected_yield_pct")),
                "deposits": deposits,
                "withdrawals": withdrawals,
                "income_net": income_net,
                "commissions": commissions,
                "operation_taxes": operation_taxes,
                "income_taxes": income_taxes,
                "net_cashflow": net_cashflow,
                "day_pnl": day_pnl,
                "twr_pct": None,
            }
        )
        twr_input_rows.append(
            {
                "snapshot_date": snapshot_date,
                "total_value": portfolio_value,
            }
        )

    external_cashflow_by_day: dict[date, float] = defaultdict(float)
    for row in operations_rows:
        group = classify_operation_group(row.get("operation_type"))
        local_date = to_local_market_date(row.get("date"))
        if local_date is None or group not in {"deposit", "withdrawal"}:
            continue
        amount = abs(float(row.get("amount") or 0.0))
        if group == "deposit":
            external_cashflow_by_day[local_date] += amount
        else:
            external_cashflow_by_day[local_date] -= amount
    twr_data = compute_twr_series(twr_input_rows, external_cashflow_by_day)
    month_twr_pct = None
    if twr_data is not None:
        _dates, _values, twr_series = twr_data
        month_twr_pct = Decimal(str(twr_series[-1] * 100.0))
        for idx, row in enumerate(daily_rows):
            row["twr_pct"] = Decimal(str(twr_series[idx + 1] * 100.0))

    reconciliation_rows, positions_value_sum, reconciliation_gap_abs = build_reconciliation_by_asset_type(
        end_snapshot_with_totals,
        end_positions,
    )

    targets = get_rebalance_targets(session, resolved_account_id)
    if targets is None or not targets:
        rebalance_snapshot: dict[str, Any] = {}
        has_rebalance_targets = bool(targets)
    else:
        class_values, other_groups = aggregate_rebalance_values_by_class(normalized_current_positions)
        rebalance_plan = compute_rebalance_plan(class_values, targets)
        total_portfolio_value = sum(class_values.values(), Decimal("0")) + sum(other_groups.values(), Decimal("0"))
        rebalance_snapshot = {
            "snapshot_date": end_snapshot_with_totals["snapshot_date"],
            "total_portfolio_value": total_portfolio_value,
            "rebalanceable_base": rebalance_plan["rebalanceable_base"],
            "class_values": class_values,
            "other_groups": other_groups,
            "rows": rebalance_plan["rows"],
        }
        has_rebalance_targets = True

    summary_metrics = _build_summary_metrics(
        start_snapshot=start_snapshot,
        end_snapshot=end_snapshot_with_totals,
        daily_rows=daily_rows,
        positions_end=end_positions,
        operations_rows=operations_rows,
        income_rows=income_rows,
        period_year=resolved_year,
        period_month=resolved_month,
        period_end_date=month_end_date,
        month_twr_pct=month_twr_pct,
        reconciliation_gap_abs=reconciliation_gap_abs,
        positions_value_sum=positions_value_sum,
        coupon_net=coupon_net,
        dividend_net=dividend_net,
        deposits_ytd=deposits_ytd,
    )

    payload = {
        "schema_version": MONTHLY_REPORT_SCHEMA_VERSION,
        "meta": {
            "report_kind": MONTHLY_REPORT_KIND,
            "account_id": resolved_account_id,
            "account_friendly_name": ACCOUNT_FRIENDLY_NAME,
            "timezone": TZ_NAME,
            "currency": end_snapshot_with_totals.get("currency"),
            "period_year": resolved_year,
            "period_month": resolved_month,
            "period_label_ru": _month_label_ru(resolved_year, resolved_month),
            "period_start": month_start_date,
            "period_end": month_end_date,
            "generated_at_utc": datetime.now(timezone.utc),
            "has_ai_narrative": False,
            "data_schema_version": MONTHLY_REPORT_SCHEMA_VERSION,
            "source_snapshot_start_id": start_snapshot["id"],
            "source_snapshot_end_id": end_snapshot["id"],
            "source_snapshot_count": len(month_daily_rows),
            "notes": [],
        },
        "summary_metrics": summary_metrics,
        "timeseries_daily": daily_rows,
        "positions_current": normalized_current_positions,
        "positions_month_start": normalized_month_start_positions,
        "positions_month_end": normalized_current_positions,
        "position_flow_groups": position_flow_groups,
        "instrument_eod_timeseries": instrument_eod_timeseries,
        "instrument_movers": instrument_movers,
        "realized_by_asset": [
            {
                "logical_asset_id": _build_monthly_logical_asset_id(
                    asset_uid=(alias_by_figi.get(row.get("figi")) or {}).get("asset_uid"),
                    instrument_uid=(alias_by_figi.get(row.get("figi")) or {}).get("instrument_uid"),
                    figi=row.get("figi"),
                    ticker=row.get("ticker"),
                ),
                "figi": row.get("figi"),
                "ticker": row.get("ticker"),
                "name": row.get("name"),
                "amount": _decimal_or_zero(row.get("amount")),
            }
            for row in compute_realized_by_asset(
                session,
                resolved_account_id,
                month_start_dt,
                next_month_start_dt,
            )[0]
        ],
        "income_by_asset": [
            {
                "logical_asset_id": _build_monthly_logical_asset_id(
                    asset_uid=(alias_by_figi.get(row.get("figi")) or {}).get("asset_uid"),
                    instrument_uid=(alias_by_figi.get(row.get("figi")) or {}).get("instrument_uid"),
                    figi=row.get("figi"),
                    ticker=row.get("ticker"),
                ),
                "figi": row.get("figi"),
                "ticker": row.get("ticker"),
                "name": row.get("name"),
                "amount": _decimal_or_zero(row.get("amount")),
                "income_kind": row.get("income_kind"),
            }
            for row in compute_income_by_asset_net(
                session,
                resolved_account_id,
                month_start_dt,
                next_month_start_dt,
            )[0]
        ],
        "open_pl_end": _build_open_pl_end(end_positions),
        "operations_top": operations_top,
        "income_events": income_events,
        "reconciliation_by_asset_type": reconciliation_rows,
        "data_quality": {
            "unknown_operation_group_count": unknown_operation_groups,
            "mojibake_detected_count": mojibake_detected_count,
            "positions_missing_label_count": sum(
                1 for row in normalized_current_positions if not (row.get("ticker") or row.get("name"))
            ),
            "has_full_history_from_zero": _decimal_or_zero(start_snapshot["total_value"]) == Decimal("0"),
            "income_events_available": True,
            "asset_alias_rows_count": len(alias_rows),
            "has_rebalance_targets": has_rebalance_targets,
        },
        "rebalance_snapshot": rebalance_snapshot,
    }

    return payload


def build_monthly_report_payload(
    session,
    *,
    year: int | None = None,
    month: int | None = None,
    account_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    raw_payload = build_monthly_report_payload_raw(
        session,
        year=year,
        month=month,
        account_id=account_id,
        now=now,
    )
    return serialize_monthly_report_payload(raw_payload)
