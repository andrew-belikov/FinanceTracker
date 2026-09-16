from __future__ import annotations

from datetime import date, datetime, timedelta

from financetracker.bot.financials_repository import get_deposits_for_period
from financetracker.bot.portfolio_repository import (
    get_latest_snapshots,
    get_max_snapshot_before_date,
    get_snapshot_for_date,
)
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.runtime import (
    PLAN_ANNUAL_CONTRIB_RUB,
    TZ,
    db_session,
    fmt_rub,
    local_reporting_bounds_utc,
)


def build_triggers_messages() -> list[str]:
    now_local = datetime.now(TZ)
    today = now_local.date()
    year = today.year

    messages: list[str] = []

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return messages

        snaps = get_latest_snapshots(session, account_id, limit=1)
        if not snaps:
            return messages

        year_start = local_reporting_bounds_utc(
            date(year, 1, 1),
            date(year + 1, 1, 1),
        )[0]
        today_start, tomorrow_start = local_reporting_bounds_utc(
            today,
            today + timedelta(days=1),
        )

        dep_prev = get_deposits_for_period(session, account_id, year_start, today_start)
        dep_now = get_deposits_for_period(session, account_id, year_start, tomorrow_start)

    if PLAN_ANNUAL_CONTRIB_RUB > 0:
        plan = PLAN_ANNUAL_CONTRIB_RUB
        if dep_prev < plan <= dep_now:
            messages.append(
                f"✅ За год внесено *{fmt_rub(dep_now)}* — годовой план "
                f"по пополнениям ({fmt_rub(plan)}) выполнен! 👏"
            )

    return messages


def _format_alert_date(value: date | None) -> str:
    if value is None:
        return "—"
    return value.strftime("%d.%m.%y")


def build_yesterday_peak_alert_message(*, now_local: datetime | None = None) -> str | None:
    now_local = now_local or datetime.now(TZ)
    target_date = now_local.date() - timedelta(days=1)

    with db_session() as session:
        account_id = resolve_reporting_account_id(session)
        if account_id is None:
            return None

        yesterday_snapshot = get_snapshot_for_date(session, account_id, target_date)
        previous_peak = get_max_snapshot_before_date(session, account_id, target_date)

    if yesterday_snapshot is None or previous_peak is None:
        return None
    if yesterday_snapshot["total_value"] is None or previous_peak["total_value"] is None:
        return None

    yesterday_value = float(yesterday_snapshot["total_value"])
    previous_peak_value = float(previous_peak["total_value"])
    if yesterday_value <= previous_peak_value:
        return None

    previous_peak_date = previous_peak["snapshot_date"]
    return (
        "🎉 Вчера был достигнут новый максимум стоимости портфеля!\n\n"
        f"Итоговая оценка за {_format_alert_date(target_date)}: *{fmt_rub(yesterday_value)}*\n"
        f"Предыдущий максимум: {fmt_rub(previous_peak_value)}.\n"
        f"Дата предыдущего максимума: {_format_alert_date(previous_peak_date)}."
    )
