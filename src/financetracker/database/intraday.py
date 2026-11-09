"""Read, compact, and purge temporary intraday portfolio observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy.orm import joinedload

from financetracker.domain.daily_change import pair_positions

from financetracker.common.time_utils import utc_now
from financetracker.database.models import (
    IntradayDailySummary,
    IntradayPortfolioPosition,
    IntradayPortfolioSnapshot,
    PortfolioSnapshot,
)


@dataclass(frozen=True)
class IntradaySnapshot:
    observed_at: datetime
    total_value: Decimal | None
    currency: str


@dataclass(frozen=True)
class IntradayPosition:
    identity: str
    figi: str
    ticker: str | None
    name: str | None
    first_value: Decimal | None
    last_value: Decimal | None
    currency: str | None = None
    first_quantity: Decimal | None = None
    last_quantity: Decimal | None = None
    first_nkd: Decimal | None = None
    last_nkd: Decimal | None = None
    instrument_type: str | None = None


@dataclass(frozen=True)
class IntradayDay:
    account_id: str
    observed_date: date
    snapshots: tuple[IntradaySnapshot, ...]
    positions: tuple[IntradayPosition, ...]
    baseline: IntradaySnapshot | None = None


def get_intraday_day(session, account_id: str, observed_date: date) -> IntradayDay:
    """Return one account day's raw observations ordered for a daily report."""
    rows = (
        session.query(IntradayPortfolioSnapshot)
        .filter(
            IntradayPortfolioSnapshot.account_id == account_id,
            IntradayPortfolioSnapshot.snapshot_date == observed_date,
        )
        .order_by(
            IntradayPortfolioSnapshot.snapshot_at.asc(),
            IntradayPortfolioSnapshot.id.asc(),
        )
        .all()
    )
    snapshots = tuple(
        IntradaySnapshot(
            observed_at=row.snapshot_at,
            total_value=row.total_value,
            currency=row.currency,
        )
        for row in rows
    )

    previous = (session.query(PortfolioSnapshot).options(joinedload(PortfolioSnapshot.positions))
                .filter(PortfolioSnapshot.account_id == account_id,
                        PortfolioSnapshot.snapshot_date < observed_date,
                        PortfolioSnapshot.total_value.isnot(None))
                .order_by(PortfolioSnapshot.snapshot_date.desc(), PortfolioSnapshot.snapshot_at.desc())
                .first())
    baseline = IntradaySnapshot(previous.snapshot_at, previous.total_value, previous.currency) if previous else None
    valued_rows = [row for row in rows if row.total_value is not None]
    first = previous or (valued_rows[0] if valued_rows else None)
    last = valued_rows[-1] if valued_rows else None
    positions = tuple(IntradayPosition(**row) for row in pair_positions(
        snapshot_positions(first) if first else [], snapshot_positions(last) if last else [])) if valued_rows else ()
    return IntradayDay(account_id, observed_date, snapshots, positions, baseline)


def snapshot_positions(snapshot):
    fields = ("figi", "ticker", "name", "asset_uid", "instrument_uid", "currency", "quantity", "position_value", "current_nkd", "instrument_type")
    return [{key: getattr(row, key) for key in fields} for row in snapshot.positions]



def upsert_intraday_daily_summary(session, day: IntradayDay) -> IntradayDailySummary:
    """Persist the compact day result before raw observations are purged."""
    valued_snapshots = tuple(
        snapshot for snapshot in day.snapshots if snapshot.total_value is not None
    )
    if not valued_snapshots:
        raise ValueError("Intraday daily summary requires at least one valued snapshot")
    currencies = {snapshot.currency for snapshot in valued_snapshots}
    if len(currencies) != 1:
        raise ValueError("Intraday daily summary requires one portfolio currency")

    first = valued_snapshots[0]
    last = valued_snapshots[-1]
    values = tuple(snapshot.total_value for snapshot in valued_snapshots)
    summary = (
        session.query(IntradayDailySummary)
        .filter(
            IntradayDailySummary.account_id == day.account_id,
            IntradayDailySummary.summary_date == day.observed_date,
        )
        .one_or_none()
    )
    if summary is None:
        summary = IntradayDailySummary(
            account_id=day.account_id,
            summary_date=day.observed_date,
            currency=first.currency,
            first_observed_at=first.observed_at,
            last_observed_at=last.observed_at,
            first_value=first.total_value,
            last_value=last.total_value,
            min_value=min(values),
            max_value=max(values),
            total_value_delta=last.total_value - first.total_value,
            observation_count=len(valued_snapshots),
        )
        session.add(summary)
    else:
        summary.currency = first.currency
        summary.first_observed_at = first.observed_at
        summary.last_observed_at = last.observed_at
        summary.first_value = first.total_value
        summary.last_value = last.total_value
        summary.min_value = min(values)
        summary.max_value = max(values)
        summary.total_value_delta = last.total_value - first.total_value
        summary.observation_count = len(valued_snapshots)
        summary.updated_at = utc_now()
    session.flush()
    return summary


def purge_delivered_intraday_days_before(session, account_id: str, observed_date: date) -> int:
    """Purge older delivered days in the next capture's transaction."""
    snapshot_ids = [row[0] for row in (
        session.query(IntradayPortfolioSnapshot.id)
        .join(
            IntradayDailySummary,
            (IntradayDailySummary.account_id == IntradayPortfolioSnapshot.account_id)
            & (IntradayDailySummary.summary_date == IntradayPortfolioSnapshot.snapshot_date),
        )
        .filter(
            IntradayPortfolioSnapshot.account_id == account_id,
            IntradayPortfolioSnapshot.snapshot_date < observed_date,
            IntradayDailySummary.delivered_at.isnot(None),
        )
        .all()
    )]
    if not snapshot_ids:
        return 0
    session.query(IntradayPortfolioPosition).filter(
        IntradayPortfolioPosition.snapshot_id.in_(snapshot_ids)
    ).delete(synchronize_session=False)
    session.query(IntradayPortfolioSnapshot).filter(
        IntradayPortfolioSnapshot.id.in_(snapshot_ids)
    ).delete(synchronize_session=False)
    session.flush()
    return len(snapshot_ids)


def mark_intraday_daily_summary_delivered(
    session,
    account_id: str,
    observed_date: date,
) -> bool:
    """Record that all delivery-ledger entries for a compact day succeeded."""
    summary = (
        session.query(IntradayDailySummary)
        .filter(
            IntradayDailySummary.account_id == account_id,
            IntradayDailySummary.summary_date == observed_date,
        )
        .one_or_none()
    )
    if summary is None:
        return False
    now = utc_now()
    summary.delivered_at = now
    summary.updated_at = now
    session.flush()
    return True
