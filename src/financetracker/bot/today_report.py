"""On-demand daily view using the same calculation as the evening digest."""
from __future__ import annotations

from financetracker.bot.daily_report_repository import get_daily_report_events, load_today_day, report_bounds
from financetracker.bot.financials_repository import get_income_currency_breakdown_for_period, get_net_external_contributions
from financetracker.bot.intraday_daily_push import _pct, _rub, _stamp, build_intraday_daily_push_payload_from_day
from financetracker.bot.performance_service import append_income_currency_breakdown
from financetracker.bot.reporting_account import resolve_reporting_account_id
from financetracker.bot.runtime import REPORTING_ACCOUNT_UNAVAILABLE_TEXT, TZ, db_session
from financetracker.domain.daily_change import decimal


def load_today_inputs(session):
    """Load the same snapshot pair and operations for both daily views."""
    account = resolve_reporting_account_id(session)
    if account is None:
        return REPORTING_ACCOUNT_UNAVAILABLE_TEXT
    day = load_today_day(session, account)
    if day is None:
        return "Пока нет ни одного снапшота портфеля."
    current = day.snapshots[-1]
    if current.currency.upper() != "RUB":
        return "Дневная расшифровка для валюты этого счёта пока не поддерживается; пересчёт валют не выполнялся."
    if current.total_value is None:
        return "Последний снимок не содержит стоимости портфеля; результат не рассчитан."
    start, end = report_bounds(day)
    events = get_daily_report_events(session, account, day.observed_date, current.currency, start=start, end=end)
    return day, events


def build_today_change(day, events):
    return build_intraday_daily_push_payload_from_day(
        day=day, cash_events=events, local_tz=TZ, minimum_observations=1,
    )


def build_today_report():
    with db_session() as session:
        inputs = load_today_inputs(session)
        if isinstance(inputs, str):
            return inputs
        day, events = inputs
        account = day.account_id
        current = day.snapshots[-1]
        contributions = decimal(get_net_external_contributions(session, account, current.currency))
        other_currencies = get_income_currency_breakdown_for_period(session, account, day.observed_date, day.observed_date)
    payload = build_today_change(day, events)
    if payload is None:
        text = f"Портфель на {_stamp(current.observed_at, TZ)}: {_rub(current.total_value, precision=2)}.\nНет предыдущей оценки для сравнения."
    else:
        text = payload["text"]
    pnl = current.total_value - contributions
    pct = _pct(pnl / contributions * 100) if contributions > 0 else "—"
    text += f"\n\nРезультат за всё время по текущей оценке: {_rub(pnl, signed=True, precision=2)} ({pct})."
    return append_income_currency_breakdown(text, other_currencies)
