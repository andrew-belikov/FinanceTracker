from __future__ import annotations

from datetime import date

from financetracker.bot.payout_calendar import (
    render_payout_calendar_text as render_bot_payout_calendar_text,
)
from financetracker.bot.payout_repository import get_payout_calendar_events
from financetracker.bot.runtime import (
    MONTHS_RU,
    PAYOUT_CALENDAR_TAX_RATE_PCT,
    TZ,
)


PAYOUT_CALENDAR_MAX_LISTED_EVENTS = 30


def render_payout_calendar_text(rows: list[dict] | None, *, start_date: date, end_date: date, heading: str) -> str:
    return render_bot_payout_calendar_text(
        rows,
        start_date=start_date,
        end_date=end_date,
        heading=heading,
        tax_rate_pct=PAYOUT_CALENDAR_TAX_RATE_PCT,
        display_timezone=TZ,
        month_names=MONTHS_RU,
        max_listed_events=PAYOUT_CALENDAR_MAX_LISTED_EVENTS,
    )


def build_payout_calendar_text_for_account(
    session,
    account_id: str,
    *,
    start_date: date,
    end_date: date,
    heading: str,
) -> str:
    rows = get_payout_calendar_events(
        session,
        account_id,
        start_date,
        end_date,
    )
    return render_payout_calendar_text(
        rows,
        start_date=start_date,
        end_date=end_date,
        heading=heading,
    )
