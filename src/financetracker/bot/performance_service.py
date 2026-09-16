from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal

from financetracker.domain.performance import (
    compute_twr_series,
    compute_xirr,
    project_run_rate_value,
)
from financetracker.bot.financials_repository import get_external_cashflows_raw
from financetracker.bot.portfolio_repository import get_latest_snapshot_with_id, get_portfolio_timeseries
from financetracker.bot.runtime import (
    DEPOSIT_OPERATION_TYPES,
    IIS_TAX_DEDUCTION_CATEGORY,
    WITHDRAWAL_OPERATION_TYPES,
    fmt_decimal_rub,
    fmt_pct,
    fmt_rub,
    normalize_decimal,
    to_local_market_date,
)


def append_tax_refund_line(text_value: str, tax_refunds: Decimal | float | int) -> str:
    normalized_refunds = normalize_decimal(tax_refunds)
    if normalized_refunds <= 0:
        return text_value
    return f"{text_value}\nВозврат налога: {fmt_decimal_rub(normalized_refunds)}."


def append_income_currency_breakdown(
    text_value: str,
    rows: list[dict[str, Decimal | str]],
) -> str:
    if not rows:
        return text_value

    lines = [text_value, "", "💱 Доходы и налоги по валютам"]
    has_unknown = False
    for row in rows:
        currency = str(row.get("currency") or "UNKNOWN").strip().upper() or "UNKNOWN"
        has_unknown = has_unknown or currency == "UNKNOWN"

        def amount(field: str) -> str:
            value = normalize_decimal(row.get(field))
            if currency == "RUB":
                return fmt_decimal_rub(value)
            return f"{value:,.2f} {currency}".replace(",", " ")

        lines.append(
            f"• {currency}: купоны {amount('coupons')}; "
            f"дивиденды {amount('dividends')}; "
            f"налоги {amount('taxes')}; "
            f"возвраты {amount('tax_refunds')}"
        )
    if has_unknown:
        lines.append(
            "⚠️ UNKNOWN: валюта не определена; сумма не включена "
            "в базовые итоги."
        )
    return "\n".join(lines)


def render_twr_summary_text(
    *,
    last_date: date,
    last_value: float | None,
    last_twr_pct: float,
    xirr_value: float | None,
    projected_value: float | None,
    projection_date: date | None,
) -> str:
    calc_date_text = last_date.strftime("%d.%m.%Y")
    projection_date_text = projection_date.strftime("%d.%m.%Y") if projection_date is not None else "—"
    xirr_text = fmt_pct(xirr_value * 100.0, precision=2) if xirr_value is not None else "—"
    projection_text = fmt_rub(projected_value) if projected_value is not None else "—"

    return (
        "📈 *TWR и run-rate*\n"
        f"Дата расчёта: {calc_date_text}\n\n"
        f"*TWR периода*: {fmt_pct(last_twr_pct, precision=2)}\n"
        f"*Текущая стоимость*: {fmt_rub(last_value)}\n\n"
        f"*XIRR*: {xirr_text} годовых\n"
        f"*Run-rate на {projection_date_text}*: {projection_text}\n"
        "_Сценарий: без новых пополнений и выводов._"
    )


def build_net_external_flow_by_day(external_cashflows: list[dict]) -> dict[date, float]:
    net_external_flow_by_day: dict[date, float] = {}
    for row in external_cashflows:
        dt = row.get("date")
        local_date = to_local_market_date(dt)
        if local_date is None:
            continue

        amount = abs(float(row.get("amount") or 0.0))
        operation_type = (row.get("operation_type") or "").strip()
        if (
            operation_type in DEPOSIT_OPERATION_TYPES
            and row.get("cashflow_category") == IIS_TAX_DEDUCTION_CATEGORY
        ):
            continue
        if operation_type in DEPOSIT_OPERATION_TYPES:
            signed_amount = amount
        elif operation_type in WITHDRAWAL_OPERATION_TYPES:
            signed_amount = -amount
        else:
            continue

        net_external_flow_by_day[local_date] = net_external_flow_by_day.get(local_date, 0.0) + signed_amount

    return net_external_flow_by_day


def compute_twr_timeseries(session, account_id: str):
    snapshot_rows = get_portfolio_timeseries(session, account_id)
    external_cashflows = get_external_cashflows_raw(session, account_id)
    net_external_flow_by_day = build_net_external_flow_by_day(external_cashflows)
    return compute_twr_series(snapshot_rows, net_external_flow_by_day)


def build_xirr_external_cashflows(external_cashflows: list[dict]) -> list[tuple[datetime, float]]:
    cashflows: list[tuple[datetime, float]] = []
    for row in external_cashflows:
        dt = row.get("date")
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        amount = abs(float(row.get("amount") or 0.0))
        operation_type = (row.get("operation_type") or "").strip()
        if (
            operation_type in DEPOSIT_OPERATION_TYPES
            and row.get("cashflow_category") == IIS_TAX_DEDUCTION_CATEGORY
        ):
            continue
        if operation_type in DEPOSIT_OPERATION_TYPES:
            cashflows.append((dt, -amount))
        elif operation_type in WITHDRAWAL_OPERATION_TYPES:
            cashflows.append((dt, amount))
    return cashflows


def compute_portfolio_xirr_and_run_rate(
    session,
    account_id: str,
) -> tuple[float | None, float | None, date | None]:
    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    if latest_snapshot is None or latest_snapshot.get("total_value") is None:
        return None, None, None

    cashflows = build_xirr_external_cashflows(get_external_cashflows_raw(session, account_id))

    if latest_snapshot["snapshot_at"] is not None:
        terminal_dt = latest_snapshot["snapshot_at"]
        if terminal_dt.tzinfo is None:
            terminal_dt = terminal_dt.replace(tzinfo=timezone.utc)
    else:
        terminal_dt = datetime.combine(
            latest_snapshot["snapshot_date"],
            time.max,
        ).replace(tzinfo=timezone.utc)

    current_value = float(latest_snapshot["total_value"])
    cashflows.append((terminal_dt, current_value))

    xirr_value = compute_xirr(cashflows)
    projection_date = date(latest_snapshot["snapshot_date"].year, 12, 31)
    projected_value = project_run_rate_value(
        current_value,
        xirr_value,
        latest_snapshot["snapshot_date"],
        projection_date,
    )
    return xirr_value, projected_value, projection_date
