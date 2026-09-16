"""Portfolio snapshot and position read model."""

from datetime import date
from sqlalchemy import text

def get_latest_snapshots(session, account_id: str, limit: int = 2):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, snapshot_at, total_value, currency
        FROM portfolio_snapshots
        WHERE account_id = :account_id
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT :limit
        """
            ),
            {"account_id": account_id, "limit": limit},
        )
        .mappings()
        .all()
    )
    return list(rows)


def get_latest_snapshot_date(session, account_id: str):
    return session.execute(
        text("SELECT MAX(snapshot_date) FROM portfolio_snapshots WHERE account_id = :account_id"),
        {"account_id": account_id},
    ).scalar_one()


def get_snapshot_for_date(session, account_id: str, snapshot_date: date | None):
    if snapshot_date is None:
        return None
    return (
        session.execute(
            text(
                """
                SELECT id, snapshot_date, snapshot_at, total_value
                FROM portfolio_snapshots
                WHERE account_id = :account_id AND snapshot_date = :snapshot_date
                ORDER BY snapshot_at DESC, id DESC
                LIMIT 1
                """
            ),
            {"account_id": account_id, "snapshot_date": snapshot_date},
        )
        .mappings()
        .first()
    )


def get_max_snapshot_before_date(session, account_id: str, snapshot_date: date | None):
    if snapshot_date is None:
        return None
    return (
        session.execute(
            text(
                """
                SELECT id, snapshot_date, snapshot_at, total_value
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_date < :snapshot_date
                  AND total_value IS NOT NULL
                ORDER BY total_value DESC, snapshot_date DESC, snapshot_at DESC, id DESC
                LIMIT 1
                """
            ),
            {"account_id": account_id, "snapshot_date": snapshot_date},
        )
        .mappings()
        .first()
    )


def get_month_snapshots(session, account_id: str, year: int, month: int):
    month_start = date(year, month, 1)
    if month == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month + 1, 1)

    start_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :start
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": month_start, "end": next_month_start},
        )
        .mappings()
        .first()
    )

    end_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date >= :start
          AND snapshot_date < :end
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": month_start, "end": next_month_start},
        )
        .mappings()
        .first()
    )

    return start_row, end_row


def get_period_snapshots(session, account_id: str, start_date: date, end_date_exclusive: date):
    start_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :start
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": start_date},
        )
        .mappings()
        .first()
    )

    end_row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date >= :start
          AND snapshot_date < :end
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "start": start_date, "end": end_date_exclusive},
        )
        .mappings()
        .first()
    )

    return start_row, end_row


def get_latest_snapshot_with_id(session, account_id: str):
    row = (
        session.execute(
            text(
                """
        SELECT
            id,
            account_id,
            snapshot_date,
            snapshot_at,
            total_value,
            currency,
            total_shares,
            total_bonds,
            total_etf,
            total_currencies,
            total_futures
        FROM portfolio_snapshots
        WHERE account_id = :account_id
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .first()
    )
    return row


def get_latest_snapshot_with_totals_before_date(session, account_id: str, to_date: date):
    row = (
        session.execute(
            text(
                """
        SELECT
            id,
            account_id,
            snapshot_date,
            snapshot_at,
            total_value,
            currency,
            total_shares,
            total_bonds,
            total_etf,
            total_currencies,
            total_futures
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :to_date
        ORDER BY snapshot_date DESC, snapshot_at DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "to_date": to_date},
        )
        .mappings()
        .first()
    )
    return row


def get_dataset_bounds(session, account_id: str):
    row = (
        session.execute(
            text(
                """
                SELECT
                    MIN(snapshot_date) AS min_date,
                    MAX(snapshot_date) AS max_date
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .one()
    )
    return row


def get_daily_snapshot_rows(session, account_id: str):
    rows = (
        session.execute(
            text(
                """
                SELECT
                    id,
                    snapshot_date,
                    snapshot_at,
                    currency,
                    total_value,
                    expected_yield,
                    expected_yield_pct
                FROM (
                    SELECT
                        id,
                        snapshot_date,
                        snapshot_at,
                        currency,
                        total_value,
                        expected_yield,
                        expected_yield_pct,
                        ROW_NUMBER() OVER (
                            PARTITION BY snapshot_date
                            ORDER BY snapshot_at DESC, id DESC
                        ) AS rn
                    FROM portfolio_snapshots
                    WHERE account_id = :account_id
                ) daily
                WHERE rn = 1
                ORDER BY snapshot_date ASC
                """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .all()
    )
    return rows


def get_period_daily_snapshot_rows(session, account_id: str, start_date: date, end_date_exclusive: date):
    rows = (
        session.execute(
            text(
                """
                SELECT
                    id,
                    snapshot_date,
                    snapshot_at,
                    currency,
                    total_value,
                    expected_yield,
                    expected_yield_pct
                FROM (
                    SELECT
                        id,
                        snapshot_date,
                        snapshot_at,
                        currency,
                        total_value,
                        expected_yield,
                        expected_yield_pct,
                        ROW_NUMBER() OVER (
                            PARTITION BY snapshot_date
                            ORDER BY snapshot_at DESC, id DESC
                        ) AS rn
                    FROM portfolio_snapshots
                    WHERE account_id = :account_id
                      AND snapshot_date >= :start_date
                      AND snapshot_date < :end_date_exclusive
                ) daily
                WHERE rn = 1
                ORDER BY snapshot_date ASC
                """
            ),
            {
                "account_id": account_id,
                "start_date": start_date,
                "end_date_exclusive": end_date_exclusive,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_positions_for_snapshot(session, snapshot_id: int):
    query = """
        SELECT
            figi,
            COALESCE(ticker, '') AS ticker,
            COALESCE(name, '')   AS name,
            instrument_uid,
            position_uid,
            asset_uid,
            instrument_type,
            quantity,
            currency,
            current_price,
            current_nkd,
            position_value,
            expected_yield,
            expected_yield_pct,
            weight_pct
        FROM portfolio_positions
        WHERE snapshot_id = :sid
        ORDER BY position_value DESC
    """
    return session.execute(text(query), {"sid": snapshot_id}).mappings().all()


def get_instrument_eod_rows(
    session,
    account_id: str,
    start_date: date,
    end_date_exclusive: date,
):
    query = """
        WITH daily_snapshots AS (
            SELECT
                id,
                snapshot_date,
                snapshot_at,
                ROW_NUMBER() OVER (
                    PARTITION BY snapshot_date
                    ORDER BY snapshot_at DESC, id DESC
                ) AS rn
            FROM portfolio_snapshots
            WHERE account_id = :account_id
              AND snapshot_date >= :start_date
              AND snapshot_date < :end_date_exclusive
        )
        SELECT
            ds.id AS snapshot_id,
            ds.snapshot_date,
            ds.snapshot_at,
            pp.figi,
            COALESCE(pp.ticker, '') AS ticker,
            COALESCE(pp.name, '') AS name,
            pp.instrument_uid,
            pp.position_uid,
            pp.asset_uid,
            pp.instrument_type,
            pp.quantity,
            pp.currency,
            pp.position_value,
            pp.expected_yield,
            pp.expected_yield_pct,
            pp.weight_pct
        FROM daily_snapshots ds
        JOIN portfolio_positions pp ON pp.snapshot_id = ds.id
        WHERE ds.rn = 1
        ORDER BY ds.snapshot_date ASC, pp.position_value DESC, COALESCE(pp.figi, '') ASC
    """
    params = {
        "account_id": account_id,
        "start_date": start_date,
        "end_date_exclusive": end_date_exclusive,
    }
    return session.execute(text(query), params).mappings().all()


def get_positions_diff_snapshot_bounds(session, account_id: str, from_dt, to_dt):
    return session.execute(
        text("""
            SELECT id, snapshot_date, snapshot_at FROM portfolio_snapshots
            WHERE account_id = :account_id AND snapshot_at >= :from_dt AND snapshot_at < :to_dt
            ORDER BY snapshot_date ASC, snapshot_at ASC
        """),
        {"account_id": account_id, "from_dt": from_dt, "to_dt": to_dt},
    ).mappings().all()


def get_positions_diff_rows(session, *, start_snapshot_id: int, end_snapshot_id: int):
    return session.execute(
        text("""
            SELECT pp.snapshot_id, pp.figi, pp.quantity, pp.ticker AS position_ticker,
                   pp.name AS position_name, pp.instrument_type AS position_instrument_type,
                   i.ticker AS instrument_ticker, i.name AS instrument_name, i.instrument_type AS instrument_type
            FROM portfolio_positions pp LEFT JOIN instruments i ON i.figi = pp.figi
            WHERE pp.snapshot_id IN (:start_snapshot_id, :end_snapshot_id)
        """),
        {"start_snapshot_id": start_snapshot_id, "end_snapshot_id": end_snapshot_id},
    ).mappings().all()


def get_last_snapshot_before_date(session, account_id: str, d: date):
    return session.execute(
        text("""
            SELECT snapshot_date, total_value FROM portfolio_snapshots
            WHERE account_id = :account_id AND snapshot_date < :d
            ORDER BY snapshot_date DESC, snapshot_at DESC LIMIT 1
        """),
        {"account_id": account_id, "d": d},
    ).mappings().first()


def get_portfolio_timeseries(session, account_id: str):
    return session.execute(
        text("""
            SELECT snapshot_date, total_value FROM (
                SELECT snapshot_date, total_value,
                    ROW_NUMBER() OVER (PARTITION BY snapshot_date ORDER BY snapshot_at DESC, id DESC) AS rn
                FROM portfolio_snapshots WHERE account_id = :account_id
            ) daily WHERE rn = 1 ORDER BY snapshot_date ASC
        """),
        {"account_id": account_id},
    ).mappings().all()
