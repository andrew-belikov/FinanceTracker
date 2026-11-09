"""Bounded read-only inputs for the shared daily reconciliation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import joinedload

from financetracker.bot.runtime import (
    COMMISSION_OPERATION_TYPES, DEPOSIT_OPERATION_TYPES, EXECUTED_OPERATION_STATE,
    IIS_TAX_DEDUCTION_CATEGORY, INCOME_TAX_OPERATION_TYPES, TZ, WITHDRAWAL_OPERATION_TYPES,
)
from financetracker.database.models import PayoutCalendarEvent, PortfolioSnapshot
from financetracker.database.operations import operations_dedup_statement
from financetracker.domain.daily_change import pair_positions


def load_today_day(session, account_id):
    # One statement keeps mutable daily totals and their positions consistent.
    from financetracker.database.intraday import IntradayDay, IntradayPosition, IntradaySnapshot, snapshot_positions

    rows = (session.query(PortfolioSnapshot).options(joinedload(PortfolioSnapshot.positions))
            .filter(PortfolioSnapshot.account_id == account_id)
            .order_by(PortfolioSnapshot.snapshot_date.desc(), PortfolioSnapshot.snapshot_at.desc())
            .limit(2).all())
    if not rows:
        return None
    current = rows[0]
    previous = rows[1] if len(rows) > 1 and rows[1].total_value is not None else None
    return IntradayDay(
        account_id, current.snapshot_date,
        (IntradaySnapshot(current.snapshot_at, current.total_value, current.currency),),
        tuple(IntradayPosition(**row) for row in pair_positions(
            snapshot_positions(previous) if previous else [], snapshot_positions(current))),
        baseline=IntradaySnapshot(previous.snapshot_at, previous.total_value, previous.currency) if previous else None,
    )


def report_bounds(day):
    valued = [row for row in day.snapshots if row.total_value is not None]
    first = day.baseline if day.baseline and day.baseline.total_value is not None else valued[0]
    return first.observed_at, valued[-1].observed_at


def get_daily_report_events(session, account_id, report_date, currency, *, start=None, end=None):
    """Use executed operations in (start, end]; schedules are informational only."""
    start = start or datetime.combine(report_date, datetime.min.time(), tzinfo=TZ)
    end = end or start + timedelta(days=1)
    rows = session.execute(operations_dedup_statement("""
        SELECT d.figi, d.name, d.operation_type, d.cashflow_category, d.amount, d.currency,
               o.quantity_done, o.quantity, o.price, o.accrued_int, o.instrument_type
        FROM operations_dedup d JOIN operations o ON o.id = d.id
        WHERE d.account_id = :account_id AND d.date > :start AND d.date <= :end
          AND d.state = :state
        ORDER BY d.date, d.id
    """), {"account_id": account_id, "start": start, "end": end,
           "state": EXECUTED_OPERATION_STATE}).mappings().all()
    events = []
    for row in rows:
        typ, amount = row["operation_type"], row["amount"]
        if str(row.get("currency") or currency).upper() != currency.upper():
            events.append({"kind": "unsupported_currency", "amount": 0,
                           "flow": typ in DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES})
            continue
        if typ in DEPOSIT_OPERATION_TYPES:
            kind = "deduction" if row["cashflow_category"] == IIS_TAX_DEDUCTION_CATEGORY else "deposit"
            amount = abs(amount)
        elif typ in WITHDRAWAL_OPERATION_TYPES:
            kind, amount = "withdrawal", -abs(amount)
        elif typ in {"OPERATION_TYPE_COUPON", "OPERATION_TYPE_DIVIDEND"}:
            kind = "coupon" if typ == "OPERATION_TYPE_COUPON" else "dividend"
        elif typ in INCOME_TAX_OPERATION_TYPES:
            kind = "tax_refund" if amount > 0 else "tax"
        elif typ in COMMISSION_OPERATION_TYPES:
            kind = "commission"
        elif typ in {"OPERATION_TYPE_BUY", "OPERATION_TYPE_BUY_CARD", "OPERATION_TYPE_SELL"}:
            kind = "trade"
        else:
            continue
        event = {"kind": kind, "amount": amount, "figi": row["figi"]}
        if kind == "trade":
            quantity = row.get("quantity_done") if row.get("quantity_done") is not None else row.get("quantity")
            event.update(quantity_delta=(quantity if typ != "OPERATION_TYPE_SELL" else -quantity) if quantity is not None else None,
                         unit_price=row.get("price"), accrued_int=row.get("accrued_int"),
                         instrument_type=row.get("instrument_type"), name=row.get("name"), currency=currency)
        events.append(event)
    schedules = (session.query(PayoutCalendarEvent)
                 .filter(PayoutCalendarEvent.account_id == account_id,
                         PayoutCalendarEvent.event_type == "coupon",
                         PayoutCalendarEvent.currency == currency.upper(),
                         PayoutCalendarEvent.payment_date >= report_date - timedelta(days=1),
                         PayoutCalendarEvent.payment_date <= report_date + timedelta(days=7))
                 .order_by(PayoutCalendarEvent.payment_date).all())
    for row in schedules:
        # Do not explain historical replays using schedules fetched in the future.
        fetched = row.fetched_at
        fetched = fetched.replace(tzinfo=timezone.utc) if fetched.tzinfo is None else fetched
        bound = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end
        if fetched <= bound:
            events.append({"kind": "coupon_schedule", "amount": 0, "figi": row.figi,
                           "payment_date": row.payment_date, "amount_per_unit": row.amount_per_unit})
    return events
