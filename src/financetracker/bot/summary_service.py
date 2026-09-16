from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from financetracker.bot.month_templates import MonthContext, render_month_text
from financetracker.domain.assets import build_logical_asset_id
from financetracker.domain.rebalance import (
    instrument_type_to_group,
)
from financetracker.domain.performance import (
    compute_cost_basis_pnl_pct,
    compute_period_delta_excluding_external_flow,
)
from financetracker.bot.financials_repository import (
    get_commissions_for_period,
    get_deposits_for_period,
    get_income_currency_breakdown_for_period,
    get_income_for_period,
    get_iis_tax_deductions_for_period,
    get_net_external_flow_for_period,
    get_net_external_contributions,
    get_taxes_for_period,
    get_tax_refunds_for_period,
)
from financetracker.bot.portfolio_repository import (
    get_last_snapshot_before_date,
    get_latest_snapshot_with_id,
    get_latest_snapshots,
    get_month_snapshots,
    get_positions_for_snapshot,
    get_positions_diff_rows,
    get_positions_diff_snapshot_bounds,
)
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.runtime import (
    ACCOUNT_FRIENDLY_NAME,
    MONTHS_RU,
    PLAN_ANNUAL_CONTRIB_RUB,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    TZ,
    db_session,
    decimal_to_str,
    fmt_decimal_rub,
    fmt_pct,
    fmt_rub,
    normalize_decimal,
    local_reporting_bounds_utc,
)
from financetracker.bot.today_templates import TodayContext, render_today_text
from financetracker.bot.week_templates import WeekContext, render_week_text


from financetracker.bot.performance_service import (
    append_income_currency_breakdown,
    append_tax_refund_line,
)

PHRASES_AHEAD = [
    "Вы идёте чуть впереди ориентировочного графика — так держать 💪",
    "По взносам вы уже обгоняете план — можно даже немного выдохнуть 😊",
    "График пополнений опережает плановый — отличный темп!",
    "Вы двигаетесь быстрее ориентирного графика — это даёт хороший запас по времени.",
    "План по пополнениям сейчас даже немного обгоняете — очень комфортная позиция.",
]
PHRASES_ON_TRACK = [
    "Вы идёте примерно по ориентировочному графику — всё в порядке 👍",
    "Пополнения близки к графику, можно продолжать в том же духе.",
    "Вы держите нормальный темп, ориентир по году пока соблюдается.",
    "По взносам вы сейчас около плановой траектории — всё идёт по плану.",
    "Глобально вы находитесь рядом с графиком — можно спокойно продолжать.",
]
PHRASES_BEHIND = [
    "Сейчас вы чуть позади ориентировочного графика, но это легко наверстать.",
    "Темп пополнений пока ниже плана, но у вас есть время выровнять траекторию.",
    "Есть небольшой лаг по взносам — можно подумать, как закрыть его в ближайшие месяцы.",
    "Сейчас вы чуть отстаёте от ориентирного графика, но критичного отставания нет.",
    "Темп взносов ниже планового, но с учётом горизонта это ещё поправимо.",
]

def build_help_text() -> str:
    schedule_label = globals().get("DAILY_JOB_SCHEDULE_LABEL", "18:00")
    peak_schedule_label = globals().get("YESTERDAY_PEAK_ALERT_SCHEDULE_LABEL", "08:00")
    return (
        "Доступные команды:\n\n"
        "/today — сводка по портфелю на сегодня\n"
        "/week — сводка по текущей неделе\n"
        "/month — отчёт по текущему месяцу\n"
        "/monthpdf — PDF-отчёт по текущему месяцу\n"
        "/year [YYYY] — отчёт за год (без аргумента: текущий год YTD)\n"
        "/dataset — архив json+csv+md для AI-анализа\n"
        "/structure — текущая структура портфеля по позициям\n"
        "/history — график стоимости портфеля и суммы пополнений\n"
        "/twr — TWR, XIRR и run-rate на конец года + график по дням\n"
        "/calendar — ожидаемые купоны и дивиденды на следующие 90 дней\n"
        "/targets — показать текущие таргеты аллокации\n"
        "/targets set stocks=50 bonds=30 cash=20 — задать таргеты по классам\n"
        "/rebalance — показать отклонения и buy/sell для возврата к таргетам\n"
        "/invest <sum> — подсказать, как распределить новое пополнение\n"
        "/help — эта подсказка\n\n"
        "Автоматически:\n"
        f"• каждый день в {peak_schedule_label} — проверка максимума по итогам вчерашнего дня\n"
        f"• каждый день в {schedule_label} — проверка годового плана\n"
        "• каждый понедельник в 10:00 МСК — выплаты, ожидаемые на этой неделе\n"
        f"• по пятницам в {schedule_label} — недельный отчёт\n"
        f"• в последний день месяца в {schedule_label} — дополнительный отчёт за месяц\n"
        "• каждое новое пополнение счёта — подсказка, как распределить пополнение по таргетам."
    )


def compute_positions_diff_lines(start_positions, end_positions) -> list[str]:
    def _qty(value) -> float:
        if value is None:
            return 0.0
        return float(value)

    def _ticker(pos: dict) -> str:
        return (pos.get("ticker") or pos.get("figi") or "UNKNOWN").strip()

    start_map = {
        str(pos.get("figi")): pos
        for pos in start_positions
        if pos.get("figi") is not None
    }
    end_map = {
        str(pos.get("figi")): pos
        for pos in end_positions
        if pos.get("figi") is not None
    }

    new_items: list[tuple[str, str]] = []
    closed_items: list[tuple[str, str]] = []
    up_items: list[tuple[str, str]] = []
    down_items: list[tuple[str, str]] = []

    all_figis = sorted(set(start_map.keys()) | set(end_map.keys()))
    for figi in all_figis:
        start_pos = start_map.get(figi)
        end_pos = end_map.get(figi)

        if start_pos is None and end_pos is not None:
            end_qty = _qty(end_pos.get("quantity"))
            ticker = _ticker(end_pos)
            new_items.append((ticker, f"+ {ticker} — {end_qty:.0f} шт (новая)"))
            continue

        if start_pos is not None and end_pos is None:
            start_qty = _qty(start_pos.get("quantity"))
            ticker = _ticker(start_pos)
            closed_items.append((ticker, f"- {ticker} — {start_qty:.0f} шт (закрыта)"))
            continue

        start_qty = _qty(start_pos.get("quantity"))
        end_qty = _qty(end_pos.get("quantity"))
        qty_diff = end_qty - start_qty
        if qty_diff > 0:
            ticker = _ticker(end_pos)
            up_items.append(
                (ticker, f"↑ {ticker} — +{qty_diff:.0f} шт ({start_qty:.0f} → {end_qty:.0f})")
            )
        elif qty_diff < 0:
            ticker = _ticker(end_pos)
            down_items.append(
                (ticker, f"↓ {ticker} — -{abs(qty_diff):.0f} шт ({start_qty:.0f} → {end_qty:.0f})")
            )

    new_lines = [line for _, line in sorted(new_items, key=lambda item: item[0])]
    closed_lines = [line for _, line in sorted(closed_items, key=lambda item: item[0])]
    up_lines = [line for _, line in sorted(up_items, key=lambda item: item[0])]
    down_lines = [line for _, line in sorted(down_items, key=lambda item: item[0])]

    return new_lines + closed_lines + up_lines + down_lines


def compute_positions_diff_grouped(
    session,
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
) -> tuple[list[str], str | None]:
    def _qty(value) -> float:
        if value is None:
            return 0.0
        return float(value)

    def _fmt_qty(value: float) -> str:
        if value.is_integer():
            return f"{int(value)}"
        return f"{value:.6f}".rstrip("0").rstrip(".")

    def _asset_key(pos: dict) -> str:
        ticker = (pos.get("instrument_ticker") or pos.get("position_ticker") or "").strip()
        name = (pos.get("instrument_name") or pos.get("position_name") or "").strip()
        figi = (pos.get("figi") or "UNKNOWN").strip()
        if ticker:
            return ticker
        if name:
            return f"NAME:{name}"
        return f"FIGI:{figi}"

    def _display_name(pos: dict) -> str:
        ticker = (pos.get("instrument_ticker") or pos.get("position_ticker") or "").strip()
        name = (pos.get("instrument_name") or pos.get("position_name") or "").strip()
        figi = (pos.get("figi") or "UNKNOWN").strip()
        if name and ticker:
            return f"{name} ({ticker})"
        if name:
            return name
        if ticker:
            return ticker
        return figi

    snapshot_bounds = get_positions_diff_snapshot_bounds(
        session,
        account_id,
        from_dt,
        to_dt,
    )

    if len(snapshot_bounds) < 2:
        return [], "За выбранный период недостаточно снапшотов для сравнения позиций."

    start_snapshot = snapshot_bounds[0]
    end_snapshot = snapshot_bounds[-1]
    start_snapshot_id = start_snapshot["id"]
    end_snapshot_id = end_snapshot["id"]
    show_new_block = start_snapshot["snapshot_date"] == date(from_dt.year, 1, 1)

    rows = get_positions_diff_rows(
        session,
        start_snapshot_id=start_snapshot_id,
        end_snapshot_id=end_snapshot_id,
    )

    start_qty_by_key: dict[str, float] = {}
    end_qty_by_key: dict[str, float] = {}
    start_figis_by_key: dict[str, set[str]] = {}
    end_figis_by_key: dict[str, set[str]] = {}
    display_by_key: dict[str, str] = {}

    for row in rows:
        figi = str(row.get("figi") or "").strip()
        if not figi:
            continue

        instrument_type = (row.get("instrument_type") or row.get("position_instrument_type") or "")
        if figi == "RUB000UTSTOM" or str(instrument_type).lower() == "currency":
            continue

        key = _asset_key(row)
        display_by_key.setdefault(key, _display_name(row))
        qty = _qty(row.get("quantity"))

        if row["snapshot_id"] == start_snapshot_id:
            start_qty_by_key[key] = start_qty_by_key.get(key, 0.0) + qty
            start_figis_by_key.setdefault(key, set()).add(figi)
        elif row["snapshot_id"] == end_snapshot_id:
            end_qty_by_key[key] = end_qty_by_key.get(key, 0.0) + qty
            end_figis_by_key.setdefault(key, set()).add(figi)

    grouped: list[tuple[str, str, str]] = []
    all_keys = sorted(set(start_qty_by_key.keys()) | set(end_qty_by_key.keys()))
    for key in all_keys:
        qty0 = start_qty_by_key.get(key, 0.0)
        qty1 = end_qty_by_key.get(key, 0.0)
        name = display_by_key.get(key, key)

        if qty0 == 0 and qty1 > 0:
            grouped.append(("🆕 Новые", name, f"+ {name}: {_fmt_qty(0.0)} → {_fmt_qty(qty1)} шт"))
        elif qty0 > 0 and qty1 == 0:
            grouped.append(("✅ Закрыли", name, f"- {name}: {_fmt_qty(qty0)} → {_fmt_qty(0.0)} шт"))
        elif qty1 > qty0:
            grouped.append(("📈 Докупили", name, f"↑ {name}: {_fmt_qty(qty0)} → {_fmt_qty(qty1)} шт"))
        elif qty1 < qty0 and qty1 > 0:
            grouped.append(("📉 Продали часть", name, f"↓ {name}: {_fmt_qty(qty0)} → {_fmt_qty(qty1)} шт"))

    categories = ["📈 Докупили", "📉 Продали часть", "✅ Закрыли"]
    if show_new_block:
        categories = ["🆕 Новые", *categories]
    grouped_lines: list[str] = []
    for category in categories:
        category_items = sorted([item for item in grouped if item[0] == category], key=lambda item: item[1])
        if not category_items:
            continue
        if grouped_lines:
            grouped_lines.append("")
        grouped_lines.append(category)
        grouped_lines.extend(line for _, _, line in category_items)

    return grouped_lines, None


def build_today_summary() -> str:
    now_local = datetime.now(TZ)
    day_start, day_end_exclusive = local_reporting_bounds_utc(
        now_local.date(),
        now_local.date() + timedelta(days=1),
    )
    day_end = day_end_exclusive - timedelta(microseconds=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        snaps = get_latest_snapshots(session, account_id, limit=2)
        net_external_flow_today = 0.0
        if len(snaps) >= 2:
            interval_start, interval_end_exclusive = local_reporting_bounds_utc(
                snaps[1]["snapshot_date"] + timedelta(days=1),
                snaps[0]["snapshot_date"] + timedelta(days=1),
            )
            net_external_flow_today = get_net_external_flow_for_period(
                session,
                account_id,
                interval_start,
                interval_end_exclusive,
            )
        net_external_contributions = get_net_external_contributions(session, account_id)
        coupons, dividends = get_income_for_period(session, account_id, day_start, day_end)
        iis_tax_deductions = get_iis_tax_deductions_for_period(
            session, account_id, day_start, day_end_exclusive
        )
        commissions = get_commissions_for_period(session, account_id, day_start, day_end)
        taxes = get_taxes_for_period(session, account_id, day_start, day_end)
        tax_refunds = get_tax_refunds_for_period(session, account_id, day_start, day_end)
        income_by_currency = get_income_currency_breakdown_for_period(
            session, account_id, day_start, day_end
        )

    if not snaps:
        return "Пока нет ни одного снапшота портфеля."

    last = snaps[0]
    last_value = float(last["total_value"]) if last["total_value"] is not None else None
    if last["snapshot_at"]:
        dt = last["snapshot_at"]
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        snapshot_dt_local = dt.astimezone(TZ)
        snapshot_dt_str = snapshot_dt_local.strftime("%d.%m.%y %H:%M")
    else:
        snapshot_dt_str = last["snapshot_date"].strftime("%d.%m.%y")

    prev_value = None
    if len(snaps) >= 2:
        prev = snaps[1]
        prev_value = float(prev["total_value"]) if prev["total_value"] is not None else None

    delta_abs, delta_pct = compute_period_delta_excluding_external_flow(
        prev_value,
        last_value,
        net_external_flow_today,
    )

    pnl_abs = None
    pnl_pct = None
    if last_value is not None:
        pnl_abs = last_value - net_external_contributions
    if pnl_abs is not None and net_external_contributions > 0:
        pnl_pct = pnl_abs / net_external_contributions * 100.0

    ctx = TodayContext(
        snapshot_dt=snapshot_dt_str,
        current_value=fmt_rub(last_value),
        delta_abs=fmt_rub(delta_abs) if delta_abs is not None else "—",
        delta_pct=fmt_pct(delta_pct) if delta_pct is not None else "—",
        pnl_abs=fmt_rub(pnl_abs) if pnl_abs is not None else "—",
        pnl_pct=fmt_pct(pnl_pct) if pnl_pct is not None else "—",
        coupons=fmt_decimal_rub(coupons),
        dividends=fmt_decimal_rub(dividends),
        iis_tax_deductions=fmt_decimal_rub(iis_tax_deductions),
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    return append_income_currency_breakdown(
        append_tax_refund_line(render_today_text(ctx), tax_refunds),
        income_by_currency,
    )


def build_week_summary() -> str:
    now_local = datetime.now(TZ)
    week_start_date = now_local.date() - timedelta(days=now_local.weekday())
    week_end_date = week_start_date + timedelta(days=4)
    week_start, week_end_exclusive = local_reporting_bounds_utc(
        week_start_date,
        week_end_date + timedelta(days=1),
    )
    week_end = week_end_exclusive - timedelta(microseconds=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        latest_snap = get_latest_snapshot_with_id(session, account_id)
        if not latest_snap:
            return "Пока нет ни одного снапшота портфеля."

        if week_start_date.month == week_end_date.month:
            month_name = MONTHS_RU.get(week_end_date.month, str(week_end_date.month))
            week_label = f"{week_start_date.day}–{week_end_date.day} {month_name} {week_end_date.year}"
        else:
            start_month_name = MONTHS_RU.get(week_start_date.month, str(week_start_date.month))
            end_month_name = MONTHS_RU.get(week_end_date.month, str(week_end_date.month))
            week_label = (
                f"{week_start_date.day} {start_month_name}–"
                f"{week_end_date.day} {end_month_name} {week_end_date.year}"
            )

        current_value = float(latest_snap["total_value"]) if latest_snap["total_value"] is not None else 0.0
        start_row = get_last_snapshot_before_date(session, account_id, week_start_date)
        start_val_row = start_row["total_value"] if start_row is not None else None
        start_value = float(start_val_row) if start_val_row is not None else 0.0
        net_external_flow_week = get_net_external_flow_for_period(
            session,
            account_id,
            week_start,
            week_end_exclusive,
        )

        week_delta_abs = None
        week_delta_pct = None
        if start_val_row is not None and start_value != 0:
            week_delta_abs, week_delta_pct = compute_period_delta_excluding_external_flow(
                start_value,
                current_value,
                net_external_flow_week,
            )
        elif start_val_row is None:
            week_delta_abs = current_value - net_external_flow_week
            week_delta_pct = 0.0

        dep_week = get_deposits_for_period(session, account_id, week_start, week_end_exclusive)
        coupons, dividends = get_income_for_period(session, account_id, week_start, week_end)
        iis_tax_deductions = get_iis_tax_deductions_for_period(
            session, account_id, week_start, week_end_exclusive
        )
        commissions = get_commissions_for_period(session, account_id, week_start, week_end)
        taxes = get_taxes_for_period(session, account_id, week_start, week_end)
        tax_refunds = get_tax_refunds_for_period(session, account_id, week_start, week_end)
        income_by_currency = get_income_currency_breakdown_for_period(
            session, account_id, week_start, week_end
        )

        year_start, _ = local_reporting_bounds_utc(
            date(week_end_date.year, 1, 1),
            date(week_end_date.year + 1, 1, 1),
        )
        dep_year = get_deposits_for_period(session, account_id, year_start, week_end_exclusive)

        plan = PLAN_ANNUAL_CONTRIB_RUB
        plan_pct = (dep_year / plan * 100.0) if plan > 0 else 0.0

    ctx = WeekContext(
        week_label=week_label,
        current_value=fmt_rub(current_value),
        week_delta_abs=fmt_rub(week_delta_abs) if week_delta_abs is not None else "—",
        week_delta_pct=fmt_pct(week_delta_pct) if week_delta_pct is not None else "—",
        dep_week=fmt_rub(dep_week),
        plan_progress_pct=f"{plan_pct:.1f} %",
        coupons=fmt_decimal_rub(coupons),
        dividends=fmt_decimal_rub(dividends),
        iis_tax_deductions=fmt_decimal_rub(iis_tax_deductions),
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    return append_income_currency_breakdown(
        append_tax_refund_line(render_week_text(ctx), tax_refunds),
        income_by_currency,
    )


def build_month_summary() -> str:
    now_local = datetime.now(TZ)
    today = now_local.date()
    year = today.year
    month = today.month

    month_start = date(year, month, 1)
    if month == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month + 1, 1)

    month_start_dt, month_end_exclusive = local_reporting_bounds_utc(
        month_start,
        next_month_start,
    )
    month_end_dt = month_end_exclusive - timedelta(microseconds=1)

    year_start = date(year, 1, 1)
    next_year_start = date(year + 1, 1, 1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        dep_month = get_deposits_for_period(
            session,
            account_id=account_id,
            start_dt=month_start_dt,
            end_dt=month_end_exclusive,
        )
        net_external_flow_month = get_net_external_flow_for_period(
            session,
            account_id=account_id,
            start_dt=month_start_dt,
            end_dt=month_end_exclusive,
        )
        coupons, dividends = get_income_for_period(session, account_id, month_start_dt, month_end_dt)
        iis_tax_deductions = get_iis_tax_deductions_for_period(
            session, account_id, month_start_dt, month_end_exclusive
        )
        commissions = get_commissions_for_period(session, account_id, month_start_dt, month_end_dt)
        taxes = get_taxes_for_period(session, account_id, month_start_dt, month_end_dt)
        tax_refunds = get_tax_refunds_for_period(session, account_id, month_start_dt, month_end_dt)
        income_by_currency = get_income_currency_breakdown_for_period(
            session, account_id, month_start_dt, month_end_dt
        )
        dep_year = get_deposits_for_period(
            session,
            account_id=account_id,
            start_dt=local_reporting_bounds_utc(year_start, next_year_start)[0],
            end_dt=month_end_exclusive,
        )
        start_snap, end_snap = get_month_snapshots(session, account_id, year, month)

        start_positions = []
        end_positions = []
        if start_snap:
            start_positions = get_positions_for_snapshot(session, start_snap["id"])
        if end_snap:
            end_positions = get_positions_for_snapshot(session, end_snap["id"])

    plan = PLAN_ANNUAL_CONTRIB_RUB
    year_pct = dep_year / plan * 100.0 if plan > 0 else 0.0

    days_in_year = (next_year_start - year_start).days
    days_passed = (today - year_start).days + 1
    target_to_date = plan * days_passed / days_in_year if days_in_year > 0 else None

    status_phrase = ""
    if target_to_date is not None and plan > 0:
        if dep_year >= target_to_date * 1.05:
            status_phrase = random.choice(PHRASES_AHEAD)
        elif dep_year >= target_to_date * 0.95:
            status_phrase = random.choice(PHRASES_ON_TRACK)
        else:
            status_phrase = random.choice(PHRASES_BEHIND)

    delta_abs = None
    delta_pct = None
    current_value = 0.0
    if end_snap:
        current_value = float(end_snap["total_value"])

    if start_snap and end_snap:
        start_val = float(start_snap["total_value"])
        end_val = float(end_snap["total_value"])
        delta_abs, delta_pct = compute_period_delta_excluding_external_flow(
            start_val,
            end_val,
            net_external_flow_month,
        )

    month_name = MONTHS_RU.get(month, str(month))
    month_year_label = f"{month_name} {year}"

    ctx = MonthContext(
        month_year_label=month_year_label,
        current_value=fmt_rub(current_value),
        dep_month=fmt_rub(dep_month),
        dep_year=fmt_rub(dep_year),
        year_plan=fmt_rub(plan),
        year_progress_pct=f"{year_pct:.1f} %",
        delta_month_abs=fmt_rub(delta_abs) if delta_abs is not None else "—",
        delta_month_pct=fmt_pct(delta_pct, precision=2) if delta_pct is not None else "—",
        plan_status_phrase=status_phrase,
        coupons=fmt_decimal_rub(coupons),
        dividends=fmt_decimal_rub(dividends),
        iis_tax_deductions=fmt_decimal_rub(iis_tax_deductions),
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    month_text = append_income_currency_breakdown(
        append_tax_refund_line(render_month_text(ctx), tax_refunds),
        income_by_currency,
    )
    if end_snap:
        diff_lines = compute_positions_diff_lines(start_positions, end_positions)
        if diff_lines:
            month_text += "\n\n📦 Изменения позиций за месяц\n" + "\n".join(diff_lines)

    return month_text


def _resolve_monthly_asset_identity(
    row: dict,
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
) -> dict[str, str | None]:
    instrument_uid = row.get("instrument_uid")
    figi = row.get("figi")
    alias_row = None
    if instrument_uid:
        alias_row = alias_by_instrument_uid.get(instrument_uid)
    if alias_row is None and figi:
        alias_row = alias_by_figi.get(figi)

    asset_uid = row.get("asset_uid") or (alias_row.get("asset_uid") if alias_row is not None else None)
    ticker = (row.get("ticker") or "").strip()
    if not ticker and alias_row is not None:
        ticker = (alias_row.get("ticker") or "").strip()
    if not ticker and figi:
        ticker = figi

    name = (row.get("name") or row.get("instrument_name") or "").strip()
    if not name and alias_row is not None:
        name = (alias_row.get("name") or "").strip()
    if not name:
        name = ticker or (figi or "")

    logical_asset_id = build_logical_asset_id(
        asset_uid=asset_uid,
        instrument_uid=instrument_uid,
        figi=figi,
    ) or ticker or figi or name

    return {
        "logical_asset_id": logical_asset_id,
        "asset_uid": asset_uid,
        "instrument_uid": instrument_uid,
        "figi": figi,
        "ticker": ticker,
        "name": name,
    }


def _build_monthly_position_flow_groups(
    start_positions: list[dict],
    end_positions: list[dict],
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
) -> dict[str, list[dict]]:
    def _key(row: dict) -> str:
        identity = _resolve_monthly_asset_identity(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        return str(identity["logical_asset_id"] or identity["ticker"] or identity["figi"] or "")

    def _quantity(row: dict) -> Decimal:
        return normalize_decimal(row.get("quantity"))

    def _position_value(row: dict) -> Decimal:
        return normalize_decimal(row.get("position_value"))

    def _record(start_row, end_row):
        source_row = end_row or start_row or {}
        identity = _resolve_monthly_asset_identity(
            source_row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        start_qty = _quantity(start_row) if start_row is not None else Decimal("0")
        end_qty = _quantity(end_row) if end_row is not None else Decimal("0")
        start_value = _position_value(start_row) if start_row is not None else Decimal("0")
        end_value = _position_value(end_row) if end_row is not None else Decimal("0")
        return {
            "logical_asset_id": identity["logical_asset_id"],
            "ticker": identity["ticker"],
            "name": identity["name"],
            "instrument_type": source_row.get("instrument_type") or "",
            "start_qty": decimal_to_str(start_qty),
            "end_qty": decimal_to_str(end_qty),
            "delta_qty": decimal_to_str(end_qty - start_qty),
            "start_value": decimal_to_str(start_value),
            "end_value": decimal_to_str(end_value),
            "delta_value": decimal_to_str(end_value - start_value),
        }

    start_map: dict[str, dict] = {}
    end_map: dict[str, dict] = {}
    for row in start_positions:
        start_map[_key(row)] = row
    for row in end_positions:
        end_map[_key(row)] = row

    grouped = {
        "new": [],
        "closed": [],
        "increased": [],
        "decreased": [],
    }

    for key in sorted(set(start_map.keys()) | set(end_map.keys())):
        start_row = start_map.get(key)
        end_row = end_map.get(key)
        start_qty = _quantity(start_row) if start_row is not None else Decimal("0")
        end_qty = _quantity(end_row) if end_row is not None else Decimal("0")

        if start_qty == 0 and end_qty == 0:
            continue

        record = _record(start_row, end_row)
        if start_qty == 0 and end_qty > 0:
            grouped["new"].append(record)
        elif start_qty > 0 and end_qty == 0:
            grouped["closed"].append(record)
        elif end_qty > start_qty:
            grouped["increased"].append(record)
        elif end_qty < start_qty:
            grouped["decreased"].append(record)

    for bucket in grouped.values():
        bucket.sort(
            key=lambda row: (
                -normalize_decimal(row["delta_value"]).copy_abs(),
                row["ticker"],
                row["name"],
            ),
        )

    return grouped


def _build_monthly_instrument_payload(
    eod_rows: list[dict],
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
) -> tuple[list[dict], dict[str, list[dict]]]:
    grouped: dict[str, dict] = {}

    for row in eod_rows:
        snapshot_date = row.get("snapshot_date")
        if snapshot_date is None:
            continue

        identity = _resolve_monthly_asset_identity(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        logical_asset_id = str(identity["logical_asset_id"] or identity["ticker"] or identity["figi"] or "")
        if not logical_asset_id:
            continue

        point = {
            "date": snapshot_date.isoformat(),
            "snapshot_id": row.get("snapshot_id"),
            "quantity": decimal_to_str(row.get("quantity")),
            "position_value": decimal_to_str(row.get("position_value")),
            "expected_yield": decimal_to_str(row.get("expected_yield")),
            "expected_yield_pct": decimal_to_str(row.get("expected_yield_pct")),
            "weight_pct": decimal_to_str(row.get("weight_pct")),
        }

        entry = grouped.get(logical_asset_id)
        if entry is None:
            entry = {
                "logical_asset_id": logical_asset_id,
                "asset_uid": identity["asset_uid"],
                "instrument_uid": identity["instrument_uid"],
                "figi": identity["figi"],
                "ticker": identity["ticker"],
                "name": identity["name"],
                "instrument_type": row.get("instrument_type") or "",
                "series": [],
            }
            grouped[logical_asset_id] = entry

        entry["series"].append(point)

    payload_rows: list[dict] = []
    for entry in grouped.values():
        series = sorted(entry["series"], key=lambda point: point["date"])
        values = [normalize_decimal(point["position_value"]) for point in series if point.get("position_value") is not None]
        if not values:
            continue

        min_index = min(range(len(series)), key=lambda idx: normalize_decimal(series[idx]["position_value"]))
        max_index = max(range(len(series)), key=lambda idx: normalize_decimal(series[idx]["position_value"]))
        end_index = len(series) - 1
        min_yield_index = min(range(len(series)), key=lambda idx: normalize_decimal(series[idx]["expected_yield"]) if series[idx].get("expected_yield") is not None else Decimal("0"))
        max_yield_index = max(range(len(series)), key=lambda idx: normalize_decimal(series[idx]["expected_yield"]) if series[idx].get("expected_yield") is not None else Decimal("0"))

        min_value = normalize_decimal(series[min_index]["position_value"])
        max_value = normalize_decimal(series[max_index]["position_value"])
        end_value = normalize_decimal(series[end_index]["position_value"])

        min_expected_yield = normalize_decimal(series[min_yield_index]["expected_yield"]) if series[min_yield_index].get("expected_yield") is not None else Decimal("0")
        max_expected_yield = normalize_decimal(series[max_yield_index]["expected_yield"]) if series[max_yield_index].get("expected_yield") is not None else Decimal("0")
        end_expected_yield = normalize_decimal(series[end_index]["expected_yield"]) if series[end_index].get("expected_yield") is not None else Decimal("0")

        stats = {
            "eod_min_position_value": decimal_to_str(min_value),
            "eod_min_position_value_date": series[min_index]["date"],
            "eod_min_expected_yield": decimal_to_str(min_expected_yield),
            "eod_min_expected_yield_pct": decimal_to_str(series[min_yield_index]["expected_yield_pct"]) if series[min_yield_index].get("expected_yield_pct") is not None else None,
            "eod_min_expected_yield_date": series[min_yield_index]["date"],
            "eod_max_position_value": decimal_to_str(max_value),
            "eod_max_position_value_date": series[max_index]["date"],
            "eod_max_expected_yield": decimal_to_str(max_expected_yield),
            "eod_max_expected_yield_pct": decimal_to_str(series[max_yield_index]["expected_yield_pct"]) if series[max_yield_index].get("expected_yield_pct") is not None else None,
            "eod_max_expected_yield_date": series[max_yield_index]["date"],
            "eod_end_position_value": decimal_to_str(end_value),
            "eod_end_position_value_date": series[end_index]["date"],
            "eod_end_expected_yield": decimal_to_str(end_expected_yield),
            "eod_end_expected_yield_pct": decimal_to_str(series[end_index]["expected_yield_pct"]) if series[end_index].get("expected_yield_pct") is not None else None,
            "eod_end_expected_yield_date": series[end_index]["date"],
            "max_rise_abs": decimal_to_str(max_value - min_value),
            "max_drawdown_abs": decimal_to_str(min_value - max_value),
        }

        payload_rows.append(
            {
                "logical_asset_id": entry["logical_asset_id"],
                "asset_uid": entry["asset_uid"],
                "instrument_uid": entry["instrument_uid"],
                "figi": entry["figi"],
                "ticker": entry["ticker"],
                "name": entry["name"],
                "instrument_type": entry["instrument_type"],
                "series": series,
                "stats": stats,
            }
        )

    payload_rows.sort(
        key=lambda item: normalize_decimal(item["series"][-1]["position_value"]),
        reverse=True,
    )

    top_growth = []
    top_drawdown = []
    for item in payload_rows:
        stats = item["stats"]
        top_growth.append(
            {
                "logical_asset_id": item["logical_asset_id"],
                "ticker": item["ticker"],
                "name": item["name"],
                "metric_kind": "growth",
                "rise_abs": stats["max_rise_abs"],
                "start_date": stats["eod_min_position_value_date"],
                "end_date": stats["eod_max_position_value_date"],
                "end_expected_yield": stats["eod_max_expected_yield"],
                "end_expected_yield_pct": stats["eod_max_expected_yield_pct"],
            }
        )
        top_drawdown.append(
            {
                "logical_asset_id": item["logical_asset_id"],
                "ticker": item["ticker"],
                "name": item["name"],
                "metric_kind": "drawdown",
                "drawdown_abs": stats["max_drawdown_abs"],
                "start_date": stats["eod_max_position_value_date"],
                "end_date": stats["eod_min_position_value_date"],
                "end_expected_yield": stats["eod_min_expected_yield"],
                "end_expected_yield_pct": stats["eod_min_expected_yield_pct"],
            }
        )

    top_growth.sort(key=lambda row: normalize_decimal(row["rise_abs"]).copy_abs(), reverse=True)
    top_drawdown.sort(key=lambda row: normalize_decimal(row["drawdown_abs"]))

    return payload_rows, {"top_growth": top_growth[:10], "top_drawdown": top_drawdown[:10]}

def build_structure_text() -> str:
    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        snap = get_latest_snapshot_with_id(session, account_id)
        if not snap:
            return "Нет ни одного снапшота портфеля."

        positions = get_positions_for_snapshot(session, snap["id"])

    if not positions:
        return "В последнем снапшоте нет позиций."

    snap_date: date = snap["snapshot_date"]
    total_value = float(snap["total_value"])
    groups: dict[str, dict] = {}
    for pos in positions:
        instr_type = pos.get("instrument_type")
        group_name = instrument_type_to_group(instr_type)

        qty = float(pos["quantity"]) if pos["quantity"] is not None else 0.0
        price = float(pos["current_price"]) if pos["current_price"] is not None else 0.0
        value = float(pos["position_value"]) if pos["position_value"] is not None else 0.0
        pl = float(pos["expected_yield"]) if pos["expected_yield"] is not None else 0.0
        pl_pct = compute_cost_basis_pnl_pct(value, pl)
        weight = float(pos["weight_pct"]) if pos["weight_pct"] is not None else None

        ticker = pos["ticker"] or pos["figi"]
        name = pos["name"] or ticker

        if group_name not in groups:
            groups[group_name] = {
                "total_value": 0.0,
                "total_pl": 0.0,
                "positions": [],
            }

        groups[group_name]["total_value"] += value
        groups[group_name]["total_pl"] += pl
        groups[group_name]["positions"].append(
            {
                "ticker": ticker,
                "name": name,
                "qty": qty,
                "price": price,
                "value": value,
                "pl": pl,
                "pl_pct": pl_pct,
                "weight": weight,
            }
        )

    group_list = []
    for group_name, group in groups.items():
        group_value = group["total_value"]
        group_pl = group["total_pl"]
        share_pct = group_value / total_value * 100.0 if total_value > 0 else 0.0
        pl_pct = compute_cost_basis_pnl_pct(group_value, group_pl)
        group_list.append(
            {
                "name": group_name,
                "value": group_value,
                "pl": group_pl,
                "share_pct": share_pct,
                "pl_pct": pl_pct,
                "positions": group["positions"],
            }
        )

    group_list.sort(key=lambda item: item["value"], reverse=True)
    for group in group_list:
        group["positions"].sort(key=lambda position: position["value"], reverse=True)

    lines: list[str] = []
    lines.append(
        f"📂 Структура портфеля *{ACCOUNT_FRIENDLY_NAME}* "
        f"(на {snap_date.isoformat()})"
    )
    lines.append("")
    lines.append("Сводка по типам:")

    for group in group_list:
        group_pl_pct = (
            f"{group['pl_pct']:+.1f} %"
            if group["pl_pct"] is not None
            else "—"
        )
        lines.append(
            f"- {group['name']} — {fmt_rub(group['value'])} "
            f"({group['share_pct']:.1f} % портфеля), "
            f"P&L: {fmt_rub(group['pl'])} ({group_pl_pct})"
        )

    lines.append("")
    lines.append("По сути сейчас структура выглядит так:")

    for group in group_list:
        lines.append("")
        lines.append(f"{group['name']}:")
        for position in group["positions"]:
            name = position["name"]
            ticker = position["ticker"]
            qty = position["qty"]
            price = position["price"]
            value = position["value"]
            pl = position["pl"]
            pl_pct = position["pl_pct"]

            qty_str = f"{qty:,.0f}".replace(",", " ")
            price_str = fmt_rub(price, precision=2)
            value_str = fmt_rub(value, precision=0)
            pl_str = fmt_rub(pl, precision=0)
            pl_pct_str = f"{pl_pct:+.1f} %" if pl_pct is not None else "—"

            lines.append(f"- {name} [{ticker}]")
            lines.append(
                f"  {price_str} × {qty_str} шт = {value_str} / доход: {pl_str} ({pl_pct_str})"
            )

    total_pl = sum(group["pl"] for group in group_list)
    positions_total_value = sum(group["value"] for group in group_list)
    total_pl_pct = compute_cost_basis_pnl_pct(positions_total_value, total_pl)

    lines.append("")
    lines.append("Итог:")
    lines.append(f"- Общая стоимость портфеля: *{fmt_rub(total_value)}*")
    lines.append(
        f"- Совокупный результат по всем бумагам: "
        f"{fmt_rub(total_pl)} "
        f"({f'{total_pl_pct:+.1f} %' if total_pl_pct is not None else '—'})"
    )

    return "\n".join(lines)
