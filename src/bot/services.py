from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from month_templates import MonthContext, render_month_text
from common.text_utils import has_mojibake
from queries import (
    get_asset_alias_rows,
    compute_income_by_asset_net,
    compute_realized_by_asset,
    get_dataset_operations,
    get_commissions_for_period,
    get_instrument_eod_rows,
    get_deposits_for_period,
    get_external_cashflows_raw,
    get_income_for_period,
    get_last_snapshot_before_date,
    get_latest_snapshot_with_id,
    get_latest_snapshots,
    get_max_value_before_date,
    get_net_external_flow_for_period,
    get_month_snapshots,
    get_period_daily_snapshot_rows,
    get_period_snapshots,
    get_portfolio_timeseries_agg_by_date,
    get_positions_diff_rows,
    get_positions_diff_snapshot_bounds,
    get_positions_for_snapshot,
    get_rebalance_targets as query_get_rebalance_targets,
    get_taxes_for_period,
    get_total_deposits,
    get_unrealized_at_period_end,
    get_year_financials_from_operations,
    replace_rebalance_targets as query_replace_rebalance_targets,
    resolve_reporting_account_id,
)
from runtime import (
    ACCOUNT_FRIENDLY_NAME,
    BUY_OPERATION_TYPES,
    COMMISSION_OPERATION_TYPES,
    DAILY_JOB_SCHEDULE_LABEL,
    DEPOSIT_OPERATION_TYPES,
    INCOME_EVENT_TAX_OPERATION_TYPES,
    INCOME_TAX_OPERATION_TYPES,
    MONTHS_RU,
    MONTHS_RU_GENITIVE,
    PLAN_ANNUAL_CONTRIB_RUB,
    REBALANCE_FEATURE_UNAVAILABLE_TEXT,
    REBALANCE_TARGETS_NOT_CONFIGURED_TEXT,
    REPORTING_ACCOUNT_UNAVAILABLE_TEXT,
    SELL_OPERATION_TYPES,
    TARGETS_USAGE_TEXT,
    TZ,
    WITHDRAWAL_OPERATION_TYPES,
    db_session,
    decimal_to_str,
    fmt_decimal_rub,
    fmt_pct,
    fmt_rub,
    normalize_decimal,
    to_iso_datetime,
    to_local_market_date,
)
from today_templates import TodayContext, render_today_text
from week_templates import WeekContext, render_week_text


YEAR_REPORT_TOP_N = 5
SNAPSHOT_TOTAL_FIELDS = {
    "share": "total_shares",
    "bond": "total_bonds",
    "etf": "total_etf",
    "currency": "total_currencies",
    "futures": "total_futures",
    "future": "total_futures",
}

REBALANCE_ASSET_CLASSES: tuple[str, ...] = ("stocks", "bonds", "etf", "currency")
REBALANCE_TARGET_ALIASES = {
    "stocks": "stocks",
    "bonds": "bonds",
    "etf": "etf",
    "cash": "currency",
    "currency": "currency",
}
REBALANCE_CLASS_LABELS = {
    "stocks": "Акции",
    "bonds": "Облигации",
    "etf": "ETF",
    "currency": "Валюта",
}
REBALANCE_GROUP_TO_CLASS = {
    "Акции": "stocks",
    "Облигации": "bonds",
    "ETF": "etf",
    "Валюта": "currency",
}
REBALANCE_TOLERANCE_PCT = Decimal("5.0")

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


def classify_operation_group(operation_type: str | None) -> str:
    op_type = (operation_type or "").strip()
    if op_type in DEPOSIT_OPERATION_TYPES:
        return "deposit"
    if op_type in WITHDRAWAL_OPERATION_TYPES:
        return "withdrawal"
    if op_type in BUY_OPERATION_TYPES:
        return "buy"
    if op_type in SELL_OPERATION_TYPES:
        return "sell"
    if op_type in COMMISSION_OPERATION_TYPES:
        return "commission"
    if op_type in INCOME_TAX_OPERATION_TYPES:
        return "income_tax"
    if op_type == "OPERATION_TYPE_DIVIDEND":
        return "dividend"
    if op_type == "OPERATION_TYPE_COUPON":
        return "coupon"
    return "other"


def is_income_event_backed_tax_operation(operation_type: str | None) -> bool:
    op_type = (operation_type or "").strip()
    return op_type in INCOME_EVENT_TAX_OPERATION_TYPES


def build_logical_asset_id(
    *,
    asset_uid: str | None,
    instrument_uid: str | None,
    figi: str | None,
) -> str | None:
    return asset_uid or instrument_uid or figi


def build_help_text() -> str:
    schedule_label = globals().get("DAILY_JOB_SCHEDULE_LABEL", "18:00")
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
        "/targets — показать текущие таргеты аллокации\n"
        "/targets set stocks=50 bonds=30 cash=20 — задать таргеты по классам\n"
        "/rebalance — показать отклонения и buy/sell для возврата к таргетам\n"
        "/invest <sum> — подсказать, как распределить новое пополнение\n"
        "/help — эта подсказка\n\n"
        "Автоматически:\n"
        f"• каждый день в {schedule_label} — проверка триггеров (максимум, годовой план)\n"
        f"• по пятницам в {schedule_label} — недельный отчёт\n"
        f"• в последний день месяца в {schedule_label} — дополнительный отчёт за месяц\n"
        "• каждое новое пополнение счёта — подсказка, как распределить пополнение по таргетам."
    )


def compute_period_delta_excluding_external_flow(
    start_value: float | None,
    end_value: float | None,
    net_external_flow: float,
) -> tuple[float | None, float | None]:
    if start_value in (None, 0) or end_value is None:
        return None, None

    delta_abs = end_value - start_value - net_external_flow
    delta_pct = delta_abs / start_value * 100.0
    return delta_abs, delta_pct


def compute_twr_series(
    snapshot_rows: list[dict],
    net_external_flow_by_day: dict[date, float],
) -> tuple[list[date], list[float | None], list[float]] | None:
    if len(snapshot_rows) < 2:
        return None

    dates: list[date] = []
    values: list[float | None] = []
    for row in snapshot_rows:
        dates.append(row["snapshot_date"])
        total_value = row.get("total_value")
        values.append(float(total_value) if total_value is not None else None)

    cumulative_multiplier = 1.0
    twr: list[float] = [0.0]
    for idx in range(1, len(dates)):
        previous_value = values[idx - 1]
        current_value = values[idx]
        net_external_flow = net_external_flow_by_day.get(dates[idx], 0.0)

        if previous_value in (None, 0) or current_value is None:
            twr.append(cumulative_multiplier - 1.0)
            continue

        period_return = (current_value - net_external_flow) / previous_value - 1.0
        cumulative_multiplier *= 1.0 + period_return
        twr.append(cumulative_multiplier - 1.0)

    return dates, values, twr


def compute_xnpv(rate: float, cashflows: list[tuple[datetime, float]]) -> float:
    base_dt = cashflows[0][0]
    total = 0.0
    for dt, amount in cashflows:
        years = (dt - base_dt).total_seconds() / (365.0 * 24 * 3600)
        total += amount / ((1.0 + rate) ** years)
    return total


def compute_xirr(cashflows: list[tuple[datetime, float]]) -> float | None:
    if not cashflows:
        return None
    if not any(amount < 0 for _, amount in cashflows):
        return None
    if not any(amount > 0 for _, amount in cashflows):
        return None

    low = -0.9999
    high = 10.0
    low_value = compute_xnpv(low, cashflows)
    high_value = compute_xnpv(high, cashflows)

    expansions = 0
    while low_value * high_value > 0 and expansions < 20:
        high *= 2
        high_value = compute_xnpv(high, cashflows)
        expansions += 1

    if low_value * high_value > 0:
        return None

    for _ in range(200):
        mid = (low + high) / 2.0
        mid_value = compute_xnpv(mid, cashflows)
        if abs(mid_value) < 1e-8:
            return mid
        if low_value * mid_value <= 0:
            high = mid
            high_value = mid_value
        else:
            low = mid
            low_value = mid_value

    return (low + high) / 2.0


def project_run_rate_value(
    current_value: float | None,
    annual_rate: float | None,
    from_date: date,
    to_date: date,
) -> float | None:
    if current_value is None or annual_rate is None:
        return None

    day_count = (to_date - from_date).days
    if day_count < 0:
        return None
    if day_count == 0:
        return current_value

    return current_value * ((1.0 + annual_rate) ** (day_count / 365.0))


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


def get_year_period(year: int | None) -> tuple[datetime, datetime, str, bool]:
    today = datetime.now(TZ).date()
    is_ytd = year is None
    period_year = today.year if is_ytd else int(year)

    from_dt = datetime(period_year, 1, 1)
    if is_ytd:
        to_date_inclusive = today
        label = f"{period_year} YTD"
    else:
        to_date_inclusive = date(period_year, 12, 31)
        label = str(period_year)

    to_dt = datetime.combine(to_date_inclusive + timedelta(days=1), time.min)
    return from_dt, to_dt, label, is_ytd


def build_asset_alias_lookup(alias_rows: list[dict]) -> tuple[dict[str, dict], dict[str, dict]]:
    by_instrument_uid: dict[str, dict] = {}
    by_figi: dict[str, dict] = {}
    for row in alias_rows:
        instrument_uid = row.get("instrument_uid")
        figi = row.get("figi")
        if instrument_uid and instrument_uid not in by_instrument_uid:
            by_instrument_uid[instrument_uid] = row
        if figi and figi not in by_figi:
            by_figi[figi] = row
    return by_instrument_uid, by_figi


def build_reconciliation_by_asset_type(
    latest_snapshot: dict,
    positions_rows: list[dict],
) -> tuple[list[dict], Decimal, Decimal]:
    positions_sum_total = Decimal("0")
    grouped_position_sums: dict[str, Decimal] = {}
    grouped_current_nkd_sums: dict[str, Decimal] = {}

    for row in positions_rows:
        instrument_type = (row.get("instrument_type") or "other").strip().lower()
        position_value = normalize_decimal(row.get("position_value"))
        current_nkd = normalize_decimal(row.get("current_nkd"))
        positions_sum_total += position_value
        grouped_position_sums[instrument_type] = grouped_position_sums.get(instrument_type, Decimal("0")) + position_value
        grouped_current_nkd_sums[instrument_type] = grouped_current_nkd_sums.get(instrument_type, Decimal("0")) + current_nkd

    reconciliation_rows: list[dict] = []
    for instrument_type, snapshot_field in SNAPSHOT_TOTAL_FIELDS.items():
        if instrument_type == "future":
            continue
        snapshot_total = normalize_decimal(latest_snapshot.get(snapshot_field))
        positions_sum = grouped_position_sums.get(instrument_type, Decimal("0"))
        current_nkd_sum = grouped_current_nkd_sums.get(instrument_type, Decimal("0"))
        reconciliation_rows.append(
            {
                "asset_type": instrument_type,
                "snapshot_total": snapshot_total,
                "positions_sum": positions_sum,
                "gap_abs": snapshot_total - positions_sum,
                "observed_current_nkd_sum": current_nkd_sum,
            }
        )

    reconciliation_gap_abs = normalize_decimal(latest_snapshot.get("total_value")) - positions_sum_total
    return reconciliation_rows, positions_sum_total, reconciliation_gap_abs


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
    day_start = datetime.combine(now_local.date(), time.min)
    day_end_exclusive = day_start + timedelta(days=1)
    day_end = day_end_exclusive - timedelta(microseconds=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return REPORTING_ACCOUNT_UNAVAILABLE_TEXT

        snaps = get_latest_snapshots(session, account_id, limit=2)
        net_external_flow_today = get_net_external_flow_for_period(
            session,
            account_id,
            day_start,
            day_end_exclusive,
        )
        total_deposits = get_total_deposits(session, account_id)
        coupons, dividends = get_income_for_period(session, account_id, day_start, day_end)
        commissions = get_commissions_for_period(session, account_id, day_start, day_end)
        taxes = get_taxes_for_period(session, account_id, day_start, day_end)

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
    if last_value is not None and total_deposits > 0:
        pnl_abs = last_value - total_deposits
        pnl_pct = pnl_abs / total_deposits * 100.0

    ctx = TodayContext(
        snapshot_dt=snapshot_dt_str,
        current_value=fmt_rub(last_value),
        delta_abs=fmt_rub(delta_abs) if delta_abs is not None else "—",
        delta_pct=fmt_pct(delta_pct) if delta_pct is not None else "—",
        pnl_abs=fmt_rub(pnl_abs) if pnl_abs is not None else "—",
        pnl_pct=fmt_pct(pnl_pct) if pnl_pct is not None else "—",
        coupons=fmt_decimal_rub(coupons),
        dividends=fmt_decimal_rub(dividends),
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    return render_today_text(ctx)


def build_week_summary() -> str:
    now_local = datetime.now(TZ)
    week_start_date = now_local.date() - timedelta(days=now_local.weekday())
    week_end_date = week_start_date + timedelta(days=4)
    week_start = datetime.combine(week_start_date, time.min)
    week_end_exclusive = datetime.combine(week_end_date + timedelta(days=1), time.min)
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
        commissions = get_commissions_for_period(session, account_id, week_start, week_end)
        taxes = get_taxes_for_period(session, account_id, week_start, week_end)

        year_start = datetime(week_end_date.year, 1, 1)
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
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    return render_week_text(ctx)


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

    month_start_dt = datetime.combine(month_start, time.min)
    month_end_exclusive = datetime.combine(next_month_start, time.min)
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
        commissions = get_commissions_for_period(session, account_id, month_start_dt, month_end_dt)
        taxes = get_taxes_for_period(session, account_id, month_start_dt, month_end_dt)
        dep_year = get_deposits_for_period(
            session,
            account_id=account_id,
            start_dt=datetime(year, 1, 1),
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
        commissions=fmt_decimal_rub(commissions),
        taxes=fmt_decimal_rub(taxes),
    )

    month_text = render_month_text(ctx)
    if end_snap:
        diff_lines = compute_positions_diff_lines(start_positions, end_positions)
        if diff_lines:
            month_text += "\n\n📦 Изменения позиций за месяц\n" + "\n".join(diff_lines)

    return month_text


def _resolve_month_report_period(
    year: int | None = None,
    month: int | None = None,
) -> tuple[int, int, date, date, datetime, datetime, str]:
    today_local = datetime.now(TZ).date()
    period_year = today_local.year if year is None else int(year)
    period_month = today_local.month if month is None else int(month)

    period_start_date = date(period_year, period_month, 1)
    if period_month == 12:
        calendar_month_end_date = date(period_year + 1, 1, 1) - timedelta(days=1)
    else:
        calendar_month_end_date = date(period_year, period_month + 1, 1) - timedelta(days=1)

    if (period_year, period_month) == (today_local.year, today_local.month):
        period_end_date = min(today_local, calendar_month_end_date)
    else:
        period_end_date = calendar_month_end_date

    period_start_dt = datetime.combine(period_start_date, time.min)
    period_end_exclusive = datetime.combine(period_end_date + timedelta(days=1), time.min)
    period_label_ru = f"{MONTHS_RU.get(period_month, str(period_month))} {period_year}"
    return (
        period_year,
        period_month,
        period_start_date,
        period_end_date,
        period_start_dt,
        period_end_exclusive,
        period_label_ru,
    )


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


def _serialize_monthly_position_row(
    row: dict,
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
    snapshot_id: int | None = None,
    snapshot_date: date | None = None,
) -> dict:
    identity = _resolve_monthly_asset_identity(
        row,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )

    payload = {
        **identity,
        "instrument_type": row.get("instrument_type") or "",
        "quantity": decimal_to_str(row.get("quantity")),
        "currency": row.get("currency") or "",
        "position_value": decimal_to_str(row.get("position_value")),
        "expected_yield": decimal_to_str(row.get("expected_yield")),
        "expected_yield_pct": decimal_to_str(row.get("expected_yield_pct")),
        "weight_pct": decimal_to_str(row.get("weight_pct")),
    }
    if snapshot_id is not None:
        payload["snapshot_id"] = snapshot_id
    if snapshot_date is not None:
        payload["snapshot_date"] = snapshot_date.isoformat()
    return payload


def _build_monthly_position_list(
    positions_rows: list[dict],
    *,
    alias_by_instrument_uid: dict[str, dict],
    alias_by_figi: dict[str, dict],
) -> list[dict]:
    rows = [
        _serialize_monthly_position_row(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        for row in positions_rows
    ]
    rows.sort(key=lambda row: normalize_decimal(row.get("position_value")), reverse=True)
    return rows


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


def build_monthly_report_payload(
    session,
    year: int | None = None,
    month: int | None = None,
) -> dict:
    account_id = resolve_reporting_account_id(session)
    if account_id is None:
        raise ValueError(REPORTING_ACCOUNT_UNAVAILABLE_TEXT)

    (
        period_year,
        period_month,
        period_start_date,
        period_end_date,
        period_start_dt,
        period_end_exclusive,
        period_label_ru,
    ) = _resolve_month_report_period(year, month)

    alias_rows = list(get_asset_alias_rows(session))
    alias_by_instrument_uid, alias_by_figi = build_asset_alias_lookup(alias_rows)

    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    if latest_snapshot is None:
        raise ValueError("Пока нет снапшотов для monthly report.")

    start_snap, end_snap = get_period_snapshots(
        session,
        account_id,
        period_start_date,
        period_end_exclusive.date(),
    )
    daily_rows = list(
        get_period_daily_snapshot_rows(
            session,
            account_id,
            period_start_date,
            period_end_exclusive.date(),
        )
    )
    if not daily_rows:
        raise ValueError("Пока нет дневных снапшотов для monthly report.")

    current_positions_raw = list(get_positions_for_snapshot(session, latest_snapshot["id"]))
    month_start_positions_raw = list(get_positions_for_snapshot(session, start_snap["id"])) if start_snap else []
    month_end_positions_raw = list(get_positions_for_snapshot(session, end_snap["id"])) if end_snap else []

    current_positions = _build_monthly_position_list(
        current_positions_raw,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )
    month_start_positions = _build_monthly_position_list(
        month_start_positions_raw,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )
    month_end_positions = _build_monthly_position_list(
        month_end_positions_raw,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )

    operations_rows = list(
        get_dataset_operations(
            session,
            account_id=account_id,
            start_dt=period_start_dt,
            end_dt=period_end_exclusive,
        )
    )
    income_rows = list(
        get_income_events_for_period(
            session,
            account_id,
            period_start_date,
            period_end_date,
        )
    )
    eod_rows = list(
        get_instrument_eod_rows(
            session,
            account_id,
            period_start_date,
            period_end_exclusive.date(),
        )
    )

    operations_by_day: dict[date, dict[str, Decimal]] = {}
    income_net_by_day: dict[date, Decimal] = {}
    income_tax_by_day: dict[date, Decimal] = {}
    unknown_operation_groups = 0
    mojibake_detected_count = 0
    operations_top_rows: list[dict] = []
    income_payload_rows: list[dict] = []

    def _day_bucket(target_date: date) -> dict[str, Decimal]:
        bucket = operations_by_day.get(target_date)
        if bucket is None:
            bucket = {
                "deposits": Decimal("0"),
                "withdrawals": Decimal("0"),
                "commissions": Decimal("0"),
                "operation_taxes": Decimal("0"),
            }
            operations_by_day[target_date] = bucket
        return bucket

    for row in operations_rows:
        local_date = to_local_market_date(row.get("date"))
        operation_group = classify_operation_group(row.get("operation_type"))
        if operation_group == "other":
            unknown_operation_groups += 1

        identity = _resolve_monthly_asset_identity(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        amount = normalize_decimal(row.get("amount"))
        quantity = row.get("quantity")
        description = row.get("description")
        if local_date is not None:
            bucket = _day_bucket(local_date)
            if operation_group == "deposit":
                bucket["deposits"] += amount
            elif operation_group == "withdrawal":
                bucket["withdrawals"] += abs(amount)
            elif operation_group == "commission":
                bucket["commissions"] += abs(amount)
            elif operation_group == "income_tax":
                bucket["operation_taxes"] += abs(amount)

        if description and has_mojibake(str(description)):
            mojibake_detected_count += 1

        operations_top_rows.append(
            {
                "date_utc": to_iso_datetime(row.get("date")),
                "local_date": local_date.isoformat() if local_date is not None else None,
                "operation_type": row.get("operation_type"),
                "operation_group": operation_group,
                "logical_asset_id": identity["logical_asset_id"],
                "ticker": identity["ticker"],
                "name": identity["name"],
                "amount": decimal_to_str(amount),
                "quantity": decimal_to_str(quantity),
                "description": description,
            }
        )

    for row in income_rows:
        event_date = row.get("event_date")
        if event_date is None:
            continue

        identity = _resolve_monthly_asset_identity(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        net_amount = normalize_decimal(row.get("net_amount"))
        tax_amount = normalize_decimal(row.get("tax_amount"))
        income_net_by_day[event_date] = income_net_by_day.get(event_date, Decimal("0")) + net_amount
        income_tax_by_day[event_date] = income_tax_by_day.get(event_date, Decimal("0")) + abs(tax_amount)
        income_payload_rows.append(
            {
                "event_date": event_date.isoformat(),
                "event_type": row.get("event_type"),
                "logical_asset_id": identity["logical_asset_id"],
                "figi": row.get("figi"),
                "ticker": identity["ticker"],
                "instrument_name": row.get("instrument_name"),
                "gross_amount": decimal_to_str(row.get("gross_amount")),
                "tax_amount": decimal_to_str(row.get("tax_amount")),
                "net_amount": decimal_to_str(net_amount),
                "net_yield_pct": decimal_to_str(row.get("net_yield_pct")),
                "notified": row.get("notified"),
            }
        )

    daily_rows_payload: list[dict] = []
    twr_data = compute_twr_timeseries(session, account_id)
    twr_by_date: dict[date, str] = {}
    if twr_data is not None:
        dates, _values, twr_series = twr_data
        twr_by_date = {
            dt: decimal_to_str(round(value * 100.0, 6))
            for dt, value in zip(dates, twr_series)
        }

    previous_value = normalize_decimal(start_snap["total_value"]) if start_snap and start_snap.get("total_value") is not None else None
    best_day: tuple[date, Decimal] | None = None
    worst_day: tuple[date, Decimal] | None = None

    for row in daily_rows:
        snapshot_date = row["snapshot_date"]
        portfolio_value = normalize_decimal(row.get("total_value"))
        bucket = operations_by_day.get(snapshot_date, {})
        deposits = bucket.get("deposits", Decimal("0"))
        withdrawals = bucket.get("withdrawals", Decimal("0"))
        commissions = bucket.get("commissions", Decimal("0"))
        operation_taxes = bucket.get("operation_taxes", Decimal("0"))
        income_net = income_net_by_day.get(snapshot_date, Decimal("0"))
        income_taxes = income_tax_by_day.get(snapshot_date, Decimal("0"))
        net_cashflow = deposits - withdrawals + income_net - commissions - operation_taxes
        if previous_value is None:
            day_pnl = Decimal("0")
        else:
            day_pnl = portfolio_value - previous_value - net_cashflow
        previous_value = portfolio_value

        if best_day is None or day_pnl > best_day[1]:
            best_day = (snapshot_date, day_pnl)
        if worst_day is None or day_pnl < worst_day[1]:
            worst_day = (snapshot_date, day_pnl)

        daily_rows_payload.append(
            {
                "date": snapshot_date.isoformat(),
                "snapshot_id": row["id"],
                "snapshot_at_utc": to_iso_datetime(row.get("snapshot_at")),
                "portfolio_value": decimal_to_str(portfolio_value),
                "expected_yield": decimal_to_str(row.get("expected_yield")),
                "expected_yield_pct": decimal_to_str(row.get("expected_yield_pct")),
                "deposits": decimal_to_str(deposits),
                "withdrawals": decimal_to_str(withdrawals),
                "income_net": decimal_to_str(income_net),
                "commissions": decimal_to_str(commissions),
                "operation_taxes": decimal_to_str(operation_taxes),
                "income_taxes": decimal_to_str(income_taxes),
                "net_cashflow": decimal_to_str(net_cashflow),
                "day_pnl": decimal_to_str(day_pnl),
                "twr_pct": twr_by_date.get(snapshot_date),
            }
        )

    deposits_total = sum((bucket["deposits"] for bucket in operations_by_day.values()), Decimal("0"))
    withdrawals_total = sum((bucket["withdrawals"] for bucket in operations_by_day.values()), Decimal("0"))
    commissions_total = sum((bucket["commissions"] for bucket in operations_by_day.values()), Decimal("0"))
    operation_taxes_total = sum((bucket["operation_taxes"] for bucket in operations_by_day.values()), Decimal("0"))
    income_net_total = sum(income_net_by_day.values(), Decimal("0"))
    income_tax_total = sum(income_tax_by_day.values(), Decimal("0"))
    taxes_total = operation_taxes_total + income_tax_total
    net_external_flow = deposits_total - withdrawals_total
    period_net_cashflow = net_external_flow + income_net_total - commissions_total - operation_taxes_total

    start_value = normalize_decimal(start_snap["total_value"]) if start_snap and start_snap.get("total_value") is not None else normalize_decimal(daily_rows_payload[0]["portfolio_value"])
    end_value = normalize_decimal(end_snap["total_value"]) if end_snap and end_snap.get("total_value") is not None else normalize_decimal(daily_rows_payload[-1]["portfolio_value"])
    current_value = normalize_decimal(latest_snapshot.get("total_value"))
    period_pnl_abs = end_value - start_value - period_net_cashflow
    period_pnl_pct = (period_pnl_abs / start_value * Decimal("100")) if start_value != 0 else None

    twr_period_value = twr_by_date.get(daily_rows[-1]["snapshot_date"])
    if twr_period_value is None:
        twr_period_value = twr_by_date.get(period_end_date)

    year_start_dt = datetime(period_year, 1, 1)
    deposits_ytd = get_deposits_for_period(
        session,
        account_id=account_id,
        start_dt=year_start_dt,
        end_dt=period_end_exclusive,
    )
    plan_annual_contrib = Decimal(str(PLAN_ANNUAL_CONTRIB_RUB))
    plan_progress_pct = (normalize_decimal(deposits_ytd) / plan_annual_contrib * Decimal("100")) if plan_annual_contrib > 0 else None
    days_in_year = (date(period_year + 1, 1, 1) - date(period_year, 1, 1)).days
    days_passed = (period_end_date - date(period_year, 1, 1)).days + 1
    target_to_date = (plan_annual_contrib * Decimal(days_passed) / Decimal(days_in_year)) if days_in_year > 0 else None

    positions_value_sum = sum((normalize_decimal(row.get("position_value")) for row in current_positions_raw), Decimal("0"))
    current_positions_sorted = current_positions
    top_holding = current_positions_sorted[0] if current_positions_sorted else None
    reconciliation_rows, _positions_value_sum_check, reconciliation_gap_abs = build_reconciliation_by_asset_type(
        latest_snapshot,
        current_positions_raw,
    )
    realized_by_asset_rows, realized_total = compute_realized_by_asset(
        session,
        account_id,
        period_start_dt,
        period_end_exclusive,
    )
    income_by_asset_rows, income_total_net = compute_income_by_asset_net(
        session,
        account_id,
        period_start_dt,
        period_end_exclusive,
    )
    unrealized_total = get_unrealized_at_period_end(session, account_id, period_end_exclusive)

    def _map_asset_rows(rows: list[dict], amount_key: str) -> list[dict]:
        payload_rows: list[dict] = []
        for row in rows:
            identity = _resolve_monthly_asset_identity(
                row,
                alias_by_instrument_uid=alias_by_instrument_uid,
                alias_by_figi=alias_by_figi,
            )
            payload_rows.append(
                {
                    "logical_asset_id": identity["logical_asset_id"],
                    "figi": row.get("figi"),
                    "ticker": identity["ticker"],
                    "name": identity["name"],
                    amount_key: decimal_to_str(row.get("amount")),
                }
            )
        return payload_rows

    realized_payload_rows = _map_asset_rows(realized_by_asset_rows, "amount")
    income_payload_rows_by_asset = []
    for row in income_by_asset_rows:
        identity = _resolve_monthly_asset_identity(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        income_payload_rows_by_asset.append(
            {
                "logical_asset_id": identity["logical_asset_id"],
                "figi": row.get("figi"),
                "ticker": identity["ticker"],
                "name": identity["name"],
                "amount": decimal_to_str(row.get("amount")),
                "income_kind": "income_net",
            }
        )

    open_pl_end_rows: list[dict] = []
    open_pl_total = sum((normalize_decimal(row.get("expected_yield")) for row in month_end_positions_raw), Decimal("0"))
    for row in month_end_positions:
        amount = normalize_decimal(row.get("expected_yield"))
        identity = _resolve_monthly_asset_identity(
            row,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        )
        amount_pct = (amount / open_pl_total * Decimal("100")) if open_pl_total != 0 else Decimal("0")
        open_pl_end_rows.append(
            {
                "logical_asset_id": identity["logical_asset_id"],
                "ticker": identity["ticker"],
                "name": identity["name"],
                "amount": decimal_to_str(amount),
                "amount_pct": decimal_to_str(amount_pct),
            }
        )

    instrument_eod_rows, instrument_movers = _build_monthly_instrument_payload(
        eod_rows,
        alias_by_instrument_uid=alias_by_instrument_uid,
        alias_by_figi=alias_by_figi,
    )

    operations_top_rows.sort(
        key=lambda row: (
            {
                "deposit": 0,
                "withdrawal": 1,
                "sell": 2,
                "buy": 3,
                "commission": 4,
                "income_tax": 5,
                "dividend": 6,
                "coupon": 7,
                "other": 8,
            }.get(row["operation_group"], 8),
            -normalize_decimal(row["amount"]).copy_abs(),
        ),
    )

    monthly_targets = get_rebalance_targets(session, account_id)
    rebalance_snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_rows = []
    if monthly_targets:
        rebalance_plan = compute_rebalance_plan(rebalance_snapshot["class_values"], monthly_targets)
        rebalance_rows = [
            {
                "asset_class": row["asset_class"],
                "label": row["label"],
                "current_value": decimal_to_str(row["current_value"]),
                "current_pct": decimal_to_str(row["current_pct"]),
                "target_pct": decimal_to_str(row["target_pct"]),
                "delta_pct": decimal_to_str(row["delta_pct"]),
                "target_value": decimal_to_str(row["target_value"]),
                "delta_value": decimal_to_str(row["delta_value"]),
                "status": row["status"],
            }
            for row in rebalance_plan["rows"]
        ]

    has_full_history_from_zero = start_value == 0
    positions_missing_labels = sum(1 for row in current_positions_raw if not (row.get("ticker") or row.get("name")))

    payload = {
        "schema_version": "monthly_report_payload.v1",
        "meta": {
            "report_kind": "monthly_review",
            "account_id": account_id,
            "account_friendly_name": ACCOUNT_FRIENDLY_NAME,
            "timezone": TZ.key,
            "currency": latest_snapshot.get("currency"),
            "period_year": period_year,
            "period_month": period_month,
            "period_label_ru": period_label_ru,
            "period_start": period_start_date.isoformat(),
            "period_end": period_end_date.isoformat(),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "has_ai_narrative": False,
            "data_schema_version": 1,
            "source_snapshot_start_id": start_snap["id"] if start_snap else None,
            "source_snapshot_end_id": end_snap["id"] if end_snap else None,
            "source_snapshot_count": len(daily_rows),
        },
        "summary_metrics": {
            "start_value": decimal_to_str(start_value),
            "end_value": decimal_to_str(end_value),
            "current_value": decimal_to_str(current_value),
            "period_pnl_abs": decimal_to_str(period_pnl_abs),
            "period_pnl_pct": decimal_to_str(period_pnl_pct) if period_pnl_pct is not None else None,
            "period_twr_pct": twr_period_value,
            "net_external_flow": decimal_to_str(net_external_flow),
            "deposits": decimal_to_str(deposits_total),
            "withdrawals": decimal_to_str(withdrawals_total),
            "income_net": decimal_to_str(income_net_total),
            "coupon_net": decimal_to_str(sum((normalize_decimal(row.get("net_amount")) for row in income_rows if row.get("event_type") == "coupon"), Decimal("0"))),
            "dividend_net": decimal_to_str(sum((normalize_decimal(row.get("net_amount")) for row in income_rows if row.get("event_type") == "dividend"), Decimal("0"))),
            "commissions": decimal_to_str(commissions_total),
            "taxes": decimal_to_str(taxes_total),
            "deposits_ytd": decimal_to_str(deposits_ytd),
            "plan_annual_contrib": decimal_to_str(plan_annual_contrib),
            "plan_progress_pct": decimal_to_str(plan_progress_pct) if plan_progress_pct is not None else None,
            "target_to_date": decimal_to_str(target_to_date) if target_to_date is not None else None,
            "reconciliation_gap_abs": decimal_to_str(reconciliation_gap_abs),
            "positions_value_sum": decimal_to_str(positions_value_sum),
            "top_holding_name": top_holding["name"] if top_holding else None,
            "top_holding_value": top_holding["position_value"] if top_holding else None,
            "top_holding_weight_pct": top_holding["weight_pct"] if top_holding else None,
            "best_day_date": best_day[0].isoformat() if best_day else None,
            "best_day_pnl": decimal_to_str(best_day[1]) if best_day else None,
            "worst_day_date": worst_day[0].isoformat() if worst_day else None,
            "worst_day_pnl": decimal_to_str(worst_day[1]) if worst_day else None,
            "income_events_count": len(income_payload_rows),
            "open_pl_end_total": decimal_to_str(unrealized_total),
        },
        "timeseries_daily": daily_rows_payload,
        "positions_current": current_positions,
        "positions_month_start": month_start_positions,
        "positions_month_end": month_end_positions,
        "position_flow_groups": _build_monthly_position_flow_groups(
            month_start_positions_raw,
            month_end_positions_raw,
            alias_by_instrument_uid=alias_by_instrument_uid,
            alias_by_figi=alias_by_figi,
        ),
        "instrument_eod_timeseries": instrument_eod_rows,
        "instrument_movers": instrument_movers,
        "realized_by_asset": realized_payload_rows,
        "income_by_asset": income_payload_rows_by_asset,
        "open_pl_end": open_pl_end_rows,
        "operations_top": operations_top_rows[:20],
        "income_events": income_payload_rows,
        "reconciliation_by_asset_type": [
            {
                "instrument_type": row["asset_type"],
                "positions_value_sum": decimal_to_str(row["positions_sum"]),
                "snapshot_total": decimal_to_str(row["snapshot_total"]),
                "delta_abs": decimal_to_str(row["gap_abs"]),
            }
            for row in reconciliation_rows
        ],
        "data_quality": {
            "unknown_operation_group_count": unknown_operation_groups,
            "mojibake_detected_count": mojibake_detected_count,
            "positions_missing_label_count": positions_missing_labels,
            "has_full_history_from_zero": has_full_history_from_zero,
            "income_events_available": True,
            "asset_alias_rows_count": len(alias_rows),
            "has_rebalance_targets": bool(monthly_targets),
        },
        "rebalance_snapshot": {
            "available": bool(monthly_targets),
            "snapshot_date": rebalance_snapshot["snapshot_date"].isoformat() if rebalance_snapshot["snapshot_date"] is not None else None,
            "total_portfolio_value": decimal_to_str(rebalance_snapshot["total_portfolio_value"]),
            "rebalanceable_base": decimal_to_str(sum(rebalance_snapshot["class_values"].values(), Decimal("0"))),
            "rows": rebalance_rows,
        },
    }

    return payload


def _instrument_type_to_group(instr_type: str | None) -> str:
    if not instr_type:
        return "Другое"

    lowered = instr_type.lower()
    if "share" in lowered or "stock" in lowered:
        return "Акции"
    if "bond" in lowered:
        return "Облигации"
    if "etf" in lowered or "fund" in lowered:
        return "ETF"
    if "currency" in lowered:
        return "Валюта"
    if "futures" in lowered or "future" in lowered:
        return "Фьючерсы"
    return "Другое"


def quantize_ruble_amount(value: Decimal | float | int | None) -> Decimal:
    return normalize_decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def format_decimal_number(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    decimal_value = normalize_decimal(value)
    quantizer = Decimal("1") if precision == 0 else Decimal(f"1.{'0' * precision}")
    quantized = decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
    text_value = format(quantized, "f")
    if "." in text_value:
        text_value = text_value.rstrip("0").rstrip(".")
    if signed and quantized >= 0:
        text_value = f"+{text_value}"
    return text_value


def format_decimal_pct(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    return f"{format_decimal_number(value, precision=precision, signed=signed)} %"


def format_decimal_pp(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    return f"{format_decimal_number(value, precision=precision, signed=signed)} п.п."


def format_rebalance_weight(value: Decimal | float | int | None) -> str:
    decimal_value = normalize_decimal(value).quantize(Decimal("1.0"), rounding=ROUND_HALF_UP)
    return f"{format(decimal_value, '.1f').replace('.', ',')}%"


def format_human_date_ru(value: date | None) -> str:
    if value is None:
        return ""
    return f"{value.day} {MONTHS_RU_GENITIVE[value.month]} {value.year}"


def parse_decimal_input(raw_value: str, *, allow_zero: bool = True) -> Decimal:
    cleaned = raw_value.strip().replace(" ", "").replace(",", ".")
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    if cleaned.endswith("₽"):
        cleaned = cleaned[:-1]
    if not cleaned:
        raise ValueError("Пустое значение недопустимо.")
    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Не удалось разобрать число: {raw_value}") from exc
    if not allow_zero and value <= 0:
        raise ValueError("Сумма должна быть положительной.")
    return value


def parse_rebalance_targets_args(args: list[str]) -> dict[str, Decimal]:
    if not args:
        raise ValueError(TARGETS_USAGE_TEXT)

    targets: dict[str, Decimal] = {}
    for token in args:
        if "=" not in token:
            raise ValueError(TARGETS_USAGE_TEXT)
        raw_key, raw_value = token.split("=", 1)
        key = raw_key.strip().lower()
        asset_class = REBALANCE_TARGET_ALIASES.get(key)
        if asset_class is None:
            raise ValueError(f"Неизвестный класс `{raw_key}`. Поддерживаются: stocks, bonds, etf, cash.")
        if asset_class in targets:
            raise ValueError(f"Класс `{asset_class}` указан несколько раз.")

        value = parse_decimal_input(raw_value)
        if value < 0:
            raise ValueError("Таргеты не могут быть отрицательными.")
        targets[asset_class] = value

    normalized = {
        asset_class: targets.get(asset_class, Decimal("0"))
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    if sum(normalized.values()) != Decimal("100"):
        raise ValueError("Сумма таргетов должна быть ровно 100.")
    return normalized


def aggregate_rebalance_values_by_class(
    positions: list[dict] | None,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    class_values = {
        asset_class: Decimal("0")
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    other_groups: dict[str, Decimal] = {}

    for pos in positions or []:
        group_name = _instrument_type_to_group(pos.get("instrument_type"))
        position_value = normalize_decimal(pos.get("position_value"))
        asset_class = REBALANCE_GROUP_TO_CLASS.get(group_name)
        if asset_class is not None:
            class_values[asset_class] += position_value
            continue
        other_groups[group_name] = other_groups.get(group_name, Decimal("0")) + position_value

    return class_values, other_groups


def compute_rebalance_plan(
    class_values: dict[str, Decimal],
    target_weights: dict[str, Decimal],
) -> dict[str, Decimal | list[dict[str, Decimal | str]]]:
    rebalanceable_base = sum(class_values.get(asset_class, Decimal("0")) for asset_class in REBALANCE_ASSET_CLASSES)
    rows: list[dict[str, Decimal | str]] = []

    for asset_class in REBALANCE_ASSET_CLASSES:
        current_value = normalize_decimal(class_values.get(asset_class))
        target_pct = normalize_decimal(target_weights.get(asset_class))
        current_pct = (
            current_value * Decimal("100") / rebalanceable_base
            if rebalanceable_base > 0
            else Decimal("0")
        )
        delta_pct = current_pct - target_pct
        target_value = rebalanceable_base * target_pct / Decimal("100")
        delta_value = target_value - current_value
        rows.append(
            {
                "asset_class": asset_class,
                "label": REBALANCE_CLASS_LABELS[asset_class],
                "current_value": current_value,
                "current_pct": current_pct,
                "target_pct": target_pct,
                "delta_pct": delta_pct,
                "target_value": target_value,
                "delta_value": delta_value,
                "status": "в норме" if abs(delta_pct) <= REBALANCE_TOLERANCE_PCT else "вне нормы",
            }
        )

    return {
        "rebalanceable_base": rebalanceable_base,
        "rows": rows,
    }


def compute_invest_plan(
    class_values: dict[str, Decimal],
    target_weights: dict[str, Decimal],
    deposit_amount: Decimal | float | int,
) -> dict[str, Decimal | dict[str, Decimal]]:
    rounded_deposit = quantize_ruble_amount(deposit_amount)
    if rounded_deposit <= 0:
        raise ValueError("Сумма должна быть положительной и не меньше 1 ₽.")

    rebalanceable_base = sum(class_values.get(asset_class, Decimal("0")) for asset_class in REBALANCE_ASSET_CLASSES)
    deficits = {asset_class: Decimal("0") for asset_class in REBALANCE_ASSET_CLASSES}
    raw_allocations = {asset_class: Decimal("0") for asset_class in REBALANCE_ASSET_CLASSES}

    if rebalanceable_base <= 0:
        for asset_class in REBALANCE_ASSET_CLASSES:
            raw_allocations[asset_class] = rounded_deposit * normalize_decimal(target_weights.get(asset_class)) / Decimal("100")
            deficits[asset_class] = raw_allocations[asset_class]
    else:
        new_base = rebalanceable_base + rounded_deposit
        for asset_class in REBALANCE_ASSET_CLASSES:
            desired_value = new_base * normalize_decimal(target_weights.get(asset_class)) / Decimal("100")
            deficits[asset_class] = max(desired_value - normalize_decimal(class_values.get(asset_class)), Decimal("0"))

        total_deficit = sum(deficits.values())
        if total_deficit > 0:
            for asset_class in REBALANCE_ASSET_CLASSES:
                raw_allocations[asset_class] = rounded_deposit * deficits[asset_class] / total_deficit
        else:
            for asset_class in REBALANCE_ASSET_CLASSES:
                raw_allocations[asset_class] = rounded_deposit * normalize_decimal(target_weights.get(asset_class)) / Decimal("100")
                deficits[asset_class] = raw_allocations[asset_class]

    allocations = {
        asset_class: quantize_ruble_amount(raw_allocations.get(asset_class))
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    residue = rounded_deposit - sum(allocations.values())
    if residue != 0:
        residue_asset_class = max(
            REBALANCE_ASSET_CLASSES,
            key=lambda asset_class: (
                deficits.get(asset_class, Decimal("0")),
                normalize_decimal(target_weights.get(asset_class)),
            ),
        )
        allocations[residue_asset_class] += residue

    return {
        "deposit_amount": rounded_deposit,
        "rebalanceable_base": rebalanceable_base,
        "deficits": deficits,
        "allocations": allocations,
    }


def get_rebalance_targets(session, account_id: str) -> dict[str, Decimal] | None:
    return query_get_rebalance_targets(session, account_id, REBALANCE_ASSET_CLASSES)


def replace_rebalance_targets(session, account_id: str, targets: dict[str, Decimal]) -> bool:
    return query_replace_rebalance_targets(
        session,
        account_id,
        targets,
        REBALANCE_ASSET_CLASSES,
    )


def get_latest_rebalance_snapshot(session, account_id: str) -> dict:
    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    positions: list[dict] = []
    snapshot_date: date | None = None
    total_portfolio_value = Decimal("0")
    if latest_snapshot:
        snapshot_date = latest_snapshot["snapshot_date"]
        total_portfolio_value = normalize_decimal(latest_snapshot["total_value"])
        positions = get_positions_for_snapshot(session, latest_snapshot["id"])

    class_values, other_groups = aggregate_rebalance_values_by_class(positions)
    if total_portfolio_value <= 0:
        total_portfolio_value = sum(class_values.values()) + sum(other_groups.values())

    return {
        "snapshot_date": snapshot_date,
        "total_portfolio_value": total_portfolio_value,
        "class_values": class_values,
        "other_groups": other_groups,
    }


def _build_rebalance_diff_lines(rows: list[dict[str, Decimal | str]]) -> list[str]:
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"- {'✅' if row['status'] == 'в норме' else '⚠️'} "
            f"{row['label']}: {format_rebalance_weight(row['current_pct'])} / "
            f"{format_rebalance_weight(row['target_pct'])}"
        )
    return lines


def _build_out_of_model_lines(
    other_groups: dict[str, Decimal],
    total_portfolio_value: Decimal,
) -> list[str]:
    if not other_groups:
        return []

    lines = ["Вне модели:"]
    sorted_groups = sorted(other_groups.items(), key=lambda item: item[1], reverse=True)
    for group_name, group_value in sorted_groups:
        share_pct = (
            group_value * Decimal("100") / total_portfolio_value
            if total_portfolio_value > 0
            else Decimal("0")
        )
        lines.append(
            f"- {group_name}: {fmt_decimal_rub(group_value, precision=0)} "
            f"({format_decimal_pct(share_pct, precision=1)} портфеля)"
        )
    return lines


def build_targets_text_for_account(session, account_id: str) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    snapshot_date = snapshot["snapshot_date"]

    header = "🎯 Таргеты аллокации"
    if snapshot_date is not None:
        header += f" (на {snapshot_date.isoformat()})"

    lines = [header, "", "Текущие таргеты (факт / план):"]
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    if snapshot_date is None:
        lines.append("")
        lines.append("Фактическая структура появится после первого снапшота.")

    return "\n".join(lines)


def build_rebalance_text_for_account(session, account_id: str) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    snapshot_date = snapshot["snapshot_date"]

    header = "⚖️ Ребаланс"
    lines = [header]
    if snapshot_date is not None:
        lines.extend(["", format_human_date_ru(snapshot_date)])
    lines.extend(["", "Текущие таргеты (факт / план):"])
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    sell_rows: list[tuple[str, Decimal]] = []
    buy_rows: list[tuple[str, Decimal]] = []
    for row in rebalance_plan["rows"]:
        delta_value = quantize_ruble_amount(row["delta_value"])
        if delta_value > 0:
            buy_rows.append((row["label"], delta_value))
        elif delta_value < 0:
            sell_rows.append((row["label"], abs(delta_value)))

    sell_rows.sort(key=lambda item: item[1], reverse=True)
    buy_rows.sort(key=lambda item: item[1], reverse=True)

    lines.append("")
    lines.append("Чтобы поймать баланс сейчас:")
    if sell_rows:
        lines.extend(["", "📉 Продать:"])
        for label, amount in sell_rows:
            lines.append(f"- {label}: {fmt_decimal_rub(amount, precision=0)}")
    if buy_rows:
        lines.extend(["", "📈 Купить:"])
        for label, amount in buy_rows:
            lines.append(f"- {label}: {fmt_decimal_rub(amount, precision=0)}")
    if not sell_rows and not buy_rows:
        lines.append("")
        lines.append("Баланс уже близок к целевому, действий не требуется.")

    if snapshot["other_groups"]:
        lines.append("")
        lines.append(
            "Расчёт buy/sell сделан по ребалансируемой части портфеля: "
            f"{fmt_decimal_rub(rebalance_plan['rebalanceable_base'], precision=0)}."
        )

    return "\n".join(lines)


def build_invest_text_for_account(
    session,
    account_id: str,
    deposit_amount: Decimal | float | int,
    *,
    header: str | None = None,
) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    invest_plan = compute_invest_plan(snapshot["class_values"], targets, deposit_amount)

    lines = [header or f"💸 Как распределить пополнение {fmt_decimal_rub(invest_plan['deposit_amount'], precision=0)}"]
    lines.append("")
    lines.append("Текущие таргеты (факт / план):")
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    lines.append("")
    lines.append(f"Распределение пополнения {fmt_decimal_rub(invest_plan['deposit_amount'], precision=0)}:")
    for asset_class in REBALANCE_ASSET_CLASSES:
        allocation = invest_plan["allocations"][asset_class]
        lines.append(
            f"- {REBALANCE_CLASS_LABELS[asset_class]}: {fmt_decimal_rub(allocation, precision=0)}"
        )

    if snapshot["other_groups"]:
        lines.append("")
        lines.append("Классы вне модели не участвуют в распределении пополнения.")

    return "\n".join(lines)


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
        group_name = _instrument_type_to_group(instr_type)

        qty = float(pos["quantity"]) if pos["quantity"] is not None else 0.0
        price = float(pos["current_price"]) if pos["current_price"] is not None else 0.0
        value = float(pos["position_value"]) if pos["position_value"] is not None else 0.0
        pl = float(pos["expected_yield"]) if pos["expected_yield"] is not None else 0.0
        pl_pct = float(pos["expected_yield_pct"]) if pos["expected_yield_pct"] is not None else 0.0
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
        pl_pct = group_pl / group_value * 100.0 if group_value > 0 else 0.0
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
        lines.append(
            f"- {group['name']} — {fmt_rub(group['value'])} "
            f"({group['share_pct']:.1f} % портфеля), "
            f"P&L: {fmt_rub(group['pl'])} ({group['pl_pct']:+.1f} %)"
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
            pl_pct_str = f"{pl_pct:+.1f} %"

            lines.append(f"- {name} [{ticker}]")
            lines.append(
                f"  {price_str} × {qty_str} шт = {value_str} / доход: {pl_str} ({pl_pct_str})"
            )

    total_pl = sum(group["pl"] for group in group_list)
    total_pl_pct = total_pl / total_value * 100.0 if total_value > 0 else 0.0

    lines.append("")
    lines.append("Итог:")
    lines.append(f"- Общая стоимость портфеля: *{fmt_rub(total_value)}*")
    lines.append(
        f"- Совокупный результат по всем бумагам: "
        f"{fmt_rub(total_pl)} ({total_pl_pct:+.1f} %)"
    )

    return "\n".join(lines)


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
    period_start = period_start_dt.date()
    period_end_inclusive = period_end_dt_exclusive.date() - timedelta(days=1)

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
        start_snap, end_snap = get_period_snapshots(session, account_id, period_start, period_end_dt_exclusive.date())
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

    current_value = float(end_snap["total_value"]) if end_snap else 0.0
    delta_abs = None
    delta_pct = None
    if start_snap and end_snap:
        start_val = float(start_snap["total_value"])
        end_val = float(end_snap["total_value"])
        delta_abs = end_val - start_val
        if start_val != 0:
            delta_pct = delta_abs / start_val * 100.0

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


def build_net_external_flow_by_day(external_cashflows: list[dict]) -> dict[date, float]:
    net_external_flow_by_day: dict[date, float] = {}
    for row in external_cashflows:
        dt = row.get("date")
        local_date = to_local_market_date(dt)
        if local_date is None:
            continue

        amount = abs(float(row.get("amount") or 0.0))
        operation_type = (row.get("operation_type") or "").strip()
        if operation_type in DEPOSIT_OPERATION_TYPES:
            signed_amount = amount
        elif operation_type in WITHDRAWAL_OPERATION_TYPES:
            signed_amount = -amount
        else:
            continue

        net_external_flow_by_day[local_date] = net_external_flow_by_day.get(local_date, 0.0) + signed_amount

    return net_external_flow_by_day


def compute_twr_timeseries(session, account_id: str):
    snapshot_rows = get_portfolio_timeseries_agg_by_date(session, account_id)
    external_cashflows = get_external_cashflows_raw(session, account_id)
    net_external_flow_by_day = build_net_external_flow_by_day(external_cashflows)
    return compute_twr_series(snapshot_rows, net_external_flow_by_day)


def compute_portfolio_xirr_and_run_rate(
    session,
    account_id: str,
) -> tuple[float | None, float | None, date | None]:
    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    if latest_snapshot is None or latest_snapshot.get("total_value") is None:
        return None, None, None

    cashflows: list[tuple[datetime, float]] = []
    for row in get_external_cashflows_raw(session, account_id):
        dt = row.get("date")
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        amount = abs(float(row.get("amount") or 0.0))
        operation_type = (row.get("operation_type") or "").strip()
        if operation_type in DEPOSIT_OPERATION_TYPES:
            cashflows.append((dt, -amount))
        elif operation_type in WITHDRAWAL_OPERATION_TYPES:
            cashflows.append((dt, amount))

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


def build_triggers_messages() -> list[str]:
    now_local = datetime.now(TZ)
    today = now_local.date()
    year = today.year

    messages: list[str] = []

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return messages

        snaps = get_latest_snapshots(session, account_id, limit=2)
        if not snaps:
            return messages

        last = snaps[0]
        last_value = float(last["total_value"])
        last_date = last["snapshot_date"]

        max_before_last = get_max_value_before_date(session, account_id, last_date)

        year_start = datetime(year, 1, 1)
        today_start = datetime(year, today.month, today.day)
        tomorrow_start = today_start + timedelta(days=1)

        dep_prev = get_deposits_for_period(session, account_id, year_start, today_start)
        dep_now = get_deposits_for_period(session, account_id, year_start, tomorrow_start)

    if max_before_last is not None and last_value > max_before_last:
        messages.append(
            "🎉 Новый максимум стоимости портфеля!\n\n"
            f"Текущая оценка: *{fmt_rub(last_value)}*\n"
            f"Предыдущий максимум: {fmt_rub(max_before_last)}."
        )

    if PLAN_ANNUAL_CONTRIB_RUB > 0:
        plan = PLAN_ANNUAL_CONTRIB_RUB
        if dep_prev < plan <= dep_now:
            messages.append(
                f"✅ За год внесено *{fmt_rub(dep_now)}* — годовой план "
                f"по пополнениям ({fmt_rub(plan)}) выполнен! 👏"
            )

    return messages
