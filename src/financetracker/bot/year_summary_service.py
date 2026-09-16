from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from financetracker.domain.performance import (
    compute_period_delta_excluding_external_flow,
)
from financetracker.bot.financials_repository import get_net_external_flow_for_period
from financetracker.bot.operations_repository import (
    compute_income_by_asset_net,
    compute_realized_by_asset,
    get_unrealized_at_period_end,
    get_year_financials_from_operations,
)
from financetracker.bot.portfolio_repository import get_period_snapshots
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.runtime import (
    PLAN_ANNUAL_CONTRIB_RUB,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TZ,
    db_session,
    fmt_decimal_rub,
    fmt_pct,
    fmt_rub,
    local_reporting_bounds_utc,
)


from financetracker.bot.summary_service import compute_positions_diff_grouped

YEAR_REPORT_TOP_N = 5


def get_year_period(year: int | None) -> tuple[datetime, datetime, str, bool]:
    today = datetime.now(TZ).date()
    is_ytd = year is None
    period_year = today.year if is_ytd else int(year)

    period_start_date = date(period_year, 1, 1)
    if is_ytd:
        to_date_inclusive = today
        label = f"{period_year} YTD"
    else:
        to_date_inclusive = date(period_year, 12, 31)
        label = str(period_year)

    from_dt, to_dt = local_reporting_bounds_utc(
        period_start_date,
        to_date_inclusive + timedelta(days=1),
    )
    return from_dt, to_dt, label, is_ytd


def _format_asset_lines(rows: list[dict], total: Decimal, title: str, top_n: int = YEAR_REPORT_TOP_N) -> list[str]:
    lines = [title]
    for row in rows[:top_n]:
        ticker = row.get("ticker") or "—"
        name = row.get("name") or row.get("figi") or "—"
        lines.append(f"• {name} ({ticker})  {fmt_decimal_rub(row.get('amount'))}")

    lines.append(f"Итого: {fmt_decimal_rub(total)}")
    return lines


def build_year_summary(year: int | None) -> tuple[str, str, str | None]:
    period_start_dt, period_end_dt_exclusive, label, _ = get_year_period(year)
    period_year = datetime.now(TZ).year if year is None else int(year)
    period_start = date(period_year, 1, 1)
    period_end_inclusive = (
        datetime.now(TZ).date() if year is None else date(period_year, 12, 31)
    )
    period_end_exclusive_date = period_end_inclusive + timedelta(days=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

        year_financials = get_year_financials_from_operations(
            session,
            account_id,
            period_start_dt,
            period_end_dt_exclusive,
        )
        net_external_flow = get_net_external_flow_for_period(
            session,
            account_id,
            period_start_dt,
            period_end_dt_exclusive,
        )
        start_snap, end_snap = get_period_snapshots(
            session,
            account_id,
            period_start,
            period_end_exclusive_date,
        )
        diff_lines, diff_error = compute_positions_diff_grouped(session, account_id, period_start_dt, period_end_dt_exclusive)
        realized_by_asset, realized_total = compute_realized_by_asset(
            session,
            account_id,
            period_start_dt,
            period_end_dt_exclusive,
        )
        income_by_asset_net, income_total_net = compute_income_by_asset_net(
            session,
            account_id,
            period_start_dt,
            period_end_dt_exclusive,
        )
        unrealized = get_unrealized_at_period_end(session, account_id, period_end_dt_exclusive)

    dep_year = float(year_financials["deposits"])
    income_total_net = year_financials["income_net"]
    iis_tax_deduction_income = year_financials["iis_tax_deduction_income"]

    current_value = float(end_snap["total_value"]) if end_snap else 0.0
    delta_abs = None
    delta_pct = None
    if start_snap and end_snap:
        start_val = float(start_snap["total_value"])
        end_val = float(end_snap["total_value"])
        delta_abs, delta_pct = compute_period_delta_excluding_external_flow(
            start_val,
            end_val,
            net_external_flow,
        )

    plan = PLAN_ANNUAL_CONTRIB_RUB
    plan_pct = dep_year / plan * 100.0 if plan > 0 else 0.0

    if start_snap and end_snap:
        delta_line = f"Изменение стоимости: {fmt_rub(delta_abs)} ({fmt_pct(delta_pct, precision=2) if delta_pct is not None else '—'})"
    else:
        delta_line = "Изменение стоимости: нет данных (нет снапшота в начале периода)"

    summary_lines = [
        f"📅 *Команда /year {period_start.year}*",
        f"Период: {period_start.strftime('%d.%m.%Y')} — {period_end_inclusive.strftime('%d.%m.%Y')} ({label})",
        "",
        f"Стоимость портфеля на конец периода: *{fmt_rub(current_value)}*",
        delta_line,
        f"Прогресс годового плана: {plan_pct:.1f} % ({fmt_rub(dep_year)} / {fmt_rub(plan)})",
        "",
        f"По открытым позициям: {fmt_decimal_rub(unrealized)}",
        "",
    ]
    if realized_by_asset or realized_total != 0:
        summary_lines.extend(_format_asset_lines(realized_by_asset, realized_total, "💰 Реализовано"))

    if income_by_asset_net or income_total_net != 0:
        if summary_lines and summary_lines[-1] != "":
            summary_lines.append("")
        summary_lines.extend(_format_asset_lines(income_by_asset_net, income_total_net, "🧾 Дивиденды/купоны"))

    if iis_tax_deduction_income != 0:
        if summary_lines and summary_lines[-1] != "":
            summary_lines.append("")
        summary_lines.extend(
            [
                "🏛 Налоговый вычет ИИС",
                f"Итого: {fmt_decimal_rub(iis_tax_deduction_income)}",
            ]
        )

    while summary_lines and summary_lines[-1] == "":
        summary_lines.pop()

    summary_text = "\n".join(summary_lines)

    if diff_error:
        diff_text = f"📦 {diff_error}"
    elif diff_lines:
        diff_text = f"📦 Изменения позиций за {label}\n\n" + "\n".join(diff_lines)
    else:
        diff_text = f"📦 За период {label} изменений по позициям не найдено."

    return summary_text, diff_text, label
