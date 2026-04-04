from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import bindparam, text
from sqlalchemy.exc import ProgrammingError

from runtime import (
    COMMISSION_OPERATION_TYPES,
    DEPOSIT_OPERATION_TYPES,
    EXECUTED_OPERATION_STATE,
    OPERATIONS_DEDUP_CTE,
    TAX_OPERATION_TYPES,
    TINKOFF_ACCOUNT_ID,
    WITHDRAWAL_OPERATION_TYPES,
    decimal_to_str,
    normalize_decimal,
)


def normalize_reporting_account_id(raw_value: str | None) -> str | None:
    value = (raw_value or "").strip()
    if not value or value.lower() == "auto":
        return None
    return value


def choose_reporting_account_id(
    explicit_account_id: str | None,
    latest_snapshot_account_id: str | None,
) -> str | None:
    normalized_explicit = normalize_reporting_account_id(explicit_account_id)
    if normalized_explicit:
        return normalized_explicit

    latest_value = (latest_snapshot_account_id or "").strip()
    return latest_value or None


def get_latest_snapshot_account_id(session) -> str | None:
    return session.execute(
        text(
            """
            SELECT account_id
            FROM portfolio_snapshots
            ORDER BY snapshot_date DESC, snapshot_at DESC, id DESC
            LIMIT 1
            """
        )
    ).scalar()


def resolve_reporting_account_id(session) -> str | None:
    return choose_reporting_account_id(
        TINKOFF_ACCOUNT_ID,
        get_latest_snapshot_account_id(session),
    )


def get_latest_snapshots(session, account_id: str, limit: int = 2):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, snapshot_at, total_value
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


def get_latest_deposit_date(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    return session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT MAX(date::date)
            FROM operations_dedup
            WHERE account_id = :account_id
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "operation_types": operation_types,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()


def get_total_deposits(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
) -> float:
    row = session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(amount), 0) AS s
            FROM operations_dedup
            WHERE account_id = :account_id
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "operation_types": operation_types,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)


def get_deposits_for_period(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
) -> float:
    row = session.execute(
        text(
            f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT COALESCE(SUM(amount), 0) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND date >= :start_dt
          AND date < :end_dt
          AND operation_type IN :operation_types
          AND state = :executed_state
        """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "operation_types": operation_types,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)


def get_net_external_flow_for_period(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
) -> float:
    row = session.execute(
        text(
            f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT COALESCE(
            SUM(
                CASE
                    WHEN operation_type IN :deposit_types THEN ABS(amount)
                    WHEN operation_type IN :withdrawal_types THEN -ABS(amount)
                    ELSE 0
                END
            ),
            0
        ) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND date >= :start_dt
          AND date < :end_dt
          AND operation_type IN :operation_types
          AND state = :executed_state
        """
        ).bindparams(
            bindparam("deposit_types", expanding=True),
            bindparam("withdrawal_types", expanding=True),
            bindparam("operation_types", expanding=True),
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "deposit_types": DEPOSIT_OPERATION_TYPES,
            "withdrawal_types": WITHDRAWAL_OPERATION_TYPES,
            "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)


def _is_undefined_table_error(exc: Exception, table_name: str) -> bool:
    if not isinstance(exc, ProgrammingError):
        return False
    if getattr(exc.orig, "pgcode", None) == "42P01":
        return True
    return f'relation "{table_name}" does not exist' in str(exc).lower()


def get_income_for_period(db, account_id: str, start_date, end_date) -> tuple[Decimal, Decimal]:
    try:
        row = db.execute(
            text(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN event_type = 'coupon' THEN net_amount ELSE 0 END), 0) AS coupons,
                    COALESCE(SUM(CASE WHEN event_type = 'dividend' THEN net_amount ELSE 0 END), 0) AS dividends
                FROM income_events
                WHERE account_id = :account_id
                  AND event_date >= :start_date
                  AND event_date <= :end_date
                """
            ),
            {"account_id": account_id, "start_date": start_date, "end_date": end_date},
        ).mappings().one()
    except Exception as exc:
        if _is_undefined_table_error(exc, "income_events"):
            return Decimal("0"), Decimal("0")
        raise

    return Decimal(row["coupons"] or 0), Decimal(row["dividends"] or 0)


def get_commissions_for_period(db, account_id: str, start_date, end_date) -> Decimal:
    total = db.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": COMMISSION_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return abs(Decimal(total or 0))


def get_taxes_for_period(db, account_id: str, start_date, end_date) -> Decimal:
    income_taxes = Decimal("0")
    try:
        income_taxes_row = db.execute(
            text(
                """
                SELECT COALESCE(SUM(tax_amount), 0) AS total
                FROM income_events
                WHERE account_id = :account_id
                  AND event_date >= :start_date
                  AND event_date <= :end_date
                """
            ),
            {"account_id": account_id, "start_date": start_date, "end_date": end_date},
        ).scalar_one()
        income_taxes = Decimal(income_taxes_row or 0)
    except Exception as exc:
        if not _is_undefined_table_error(exc, "income_events"):
            raise

    operation_taxes = db.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(ABS(amount)), 0) AS total
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": TAX_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return income_taxes + Decimal(operation_taxes or 0)


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
    fallback_query = """
        SELECT
            figi,
            COALESCE(ticker, '') AS ticker,
            COALESCE(name, '')   AS name,
            NULL AS instrument_uid,
            NULL AS position_uid,
            NULL AS asset_uid,
            instrument_type,
            quantity,
            currency,
            current_price,
            NULL AS current_nkd,
            position_value,
            expected_yield,
            expected_yield_pct,
            weight_pct
        FROM portfolio_positions
        WHERE snapshot_id = :sid
        ORDER BY position_value DESC
    """
    try:
        rows = (
            session.execute(text(query), {"sid": snapshot_id})
            .mappings()
            .all()
        )
    except Exception as exc:
        if not _is_undefined_table_error(exc, "portfolio_positions") and "column" not in str(exc).lower():
            raise
        session.rollback()
        rows = (
            session.execute(text(fallback_query), {"sid": snapshot_id})
            .mappings()
            .all()
        )
    return rows


def get_dataset_operations(session, account_id: str, start_dt: datetime, end_dt: datetime):
    rows = (
        session.execute(
            text(
                """
                WITH operations_dedup AS (
                    SELECT DISTINCT ON (account_id, COALESCE(operation_id, id::text))
                        account_id,
                        operation_id,
                        date,
                        amount,
                        currency,
                        operation_type,
                        state,
                        instrument_uid,
                        asset_uid,
                        figi,
                        name,
                        commission,
                        yield,
                        description,
                        source,
                        price,
                        quantity
                    FROM operations
                    ORDER BY account_id, COALESCE(operation_id, id::text), id DESC
                )
                SELECT
                    operation_id,
                    date,
                    amount,
                    currency,
                    operation_type,
                    state,
                    instrument_uid,
                    asset_uid,
                    figi,
                    name,
                    commission,
                    yield,
                    description,
                    source,
                    price,
                    quantity
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :start_dt
                  AND date < :end_dt
                  AND state = :executed_state
                ORDER BY date ASC, operation_id ASC NULLS LAST
                """
            ),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_asset_alias_rows(session):
    try:
        rows = (
            session.execute(
                text(
                    """
                    SELECT
                        asset_uid,
                        instrument_uid,
                        figi,
                        ticker,
                        name,
                        first_seen_at,
                        last_seen_at
                    FROM asset_aliases
                    ORDER BY asset_uid ASC, last_seen_at DESC
                    """
                )
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "asset_aliases"):
            session.rollback()
            return []
        raise
    return rows


def get_income_events_for_period(session, account_id: str, start_date: date, end_date: date):
    try:
        rows = (
            session.execute(
                text(
                    """
                    SELECT
                        ie.event_date,
                        ie.event_type,
                        ie.figi,
                        COALESCE(i.ticker, '') AS ticker,
                        COALESCE(i.name, ie.figi) AS instrument_name,
                        ie.gross_amount,
                        ie.tax_amount,
                        ie.net_amount,
                        ie.net_yield_pct,
                        ie.notified
                    FROM income_events ie
                    LEFT JOIN instruments i ON i.figi = ie.figi
                    WHERE ie.account_id = :account_id
                      AND ie.event_date >= :start_date
                      AND ie.event_date <= :end_date
                    ORDER BY ie.event_date ASC, ie.figi ASC, ie.event_type ASC
                    """
                ),
                {"account_id": account_id, "start_date": start_date, "end_date": end_date},
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "income_events"):
            session.rollback()
            return []
        raise
    return rows


def get_positions_diff_snapshot_bounds(
    session,
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
):
    return (
        session.execute(
            text(
                """
                SELECT id, snapshot_date, snapshot_at
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_at >= :from_dt
                  AND snapshot_at < :to_dt
                ORDER BY snapshot_date ASC, snapshot_at ASC
                """
            ),
            {"account_id": account_id, "from_dt": from_dt, "to_dt": to_dt},
        )
        .mappings()
        .all()
    )


def get_positions_diff_rows(
    session,
    *,
    start_snapshot_id: int,
    end_snapshot_id: int,
):
    return (
        session.execute(
            text(
                """
                SELECT
                    pp.snapshot_id,
                    pp.figi,
                    pp.quantity,
                    pp.ticker AS position_ticker,
                    pp.name AS position_name,
                    pp.instrument_type AS position_instrument_type,
                    i.ticker AS instrument_ticker,
                    i.name AS instrument_name,
                    i.instrument_type AS instrument_type
                FROM portfolio_positions pp
                LEFT JOIN instruments i ON i.figi = pp.figi
                WHERE pp.snapshot_id IN (:start_snapshot_id, :end_snapshot_id)
                """
            ),
            {"start_snapshot_id": start_snapshot_id, "end_snapshot_id": end_snapshot_id},
        )
        .mappings()
        .all()
    )


def get_portfolio_timeseries(session, account_id: str):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
        ORDER BY snapshot_date ASC
        """
            ),
            {"account_id": account_id},
        )
        .mappings()
        .all()
    )
    return rows


def get_deposits_by_date(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    rows = (
        session.execute(
            text(
                f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT date::date AS d, SUM(amount) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND operation_type IN :operation_types
          AND state = :executed_state
        GROUP BY date::date
        ORDER BY d ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_types": operation_types,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_year_financials_from_operations(session, account_id: str, start_dt: datetime, end_dt: datetime) -> dict[str, Decimal]:
    row = session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT
                COALESCE(SUM(CASE WHEN operation_type IN :deposit_types THEN amount ELSE 0 END), 0) AS deposits,
                COALESCE(SUM(CASE
                    WHEN operation_type IN ('OPERATION_TYPE_DIVIDEND', 'OPERATION_TYPE_DIVIDEND_TAX') THEN amount
                    ELSE 0
                END), 0) AS dividend_net,
                COALESCE(SUM(CASE
                    WHEN operation_type IN ('OPERATION_TYPE_COUPON', 'OPERATION_TYPE_COUPON_TAX') THEN amount
                    ELSE 0
                END), 0) AS coupon_net
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_dt
              AND date < :end_dt
              AND state = :executed_state
            """
        ).bindparams(
            bindparam("deposit_types", expanding=True),
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "deposit_types": DEPOSIT_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).mappings().one()

    dividend_net = Decimal(row["dividend_net"] or 0)
    coupon_net = Decimal(row["coupon_net"] or 0)
    income_net = dividend_net + coupon_net

    return {
        "deposits": Decimal(row["deposits"] or 0),
        "income_net": income_net,
        "dividend_net": dividend_net,
        "coupon_net": coupon_net,
    }


def compute_realized_by_asset(session, account_id: str, start_dt: datetime, end_dt: datetime) -> tuple[list[dict], Decimal]:
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT
                    od.figi,
                    COALESCE(NULLIF(MAX(NULLIF(od.name, '')), ''), NULLIF(MAX(NULLIF(i.name, '')), ''), od.figi) AS name,
                    COALESCE(NULLIF(MAX(NULLIF(i.ticker, '')), ''), '') AS ticker,
                    COALESCE(SUM(COALESCE(od.yield, 0) + COALESCE(od.commission, 0)), 0) AS amount
                FROM operations_dedup od
                LEFT JOIN instruments i ON i.figi = od.figi
                WHERE od.account_id = :account_id
                  AND od.date >= :start_dt
                  AND od.date < :end_dt
                  AND od.state = :executed_state
                  AND od.operation_type = 'OPERATION_TYPE_SELL'
                  AND od.figi IS NOT NULL
                GROUP BY od.figi
                ORDER BY amount DESC
                """
            ),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )

    parsed = []
    total = Decimal("0")
    for row in rows:
        amount = Decimal(row["amount"] or 0)
        total += amount
        parsed.append(
            {
                "figi": row["figi"],
                "name": row["name"] or row["figi"],
                "ticker": row["ticker"] or "",
                "amount": amount,
            }
        )
    return parsed, total


def compute_income_by_asset_net(session, account_id: str, start_dt: datetime, end_dt: datetime) -> tuple[list[dict], Decimal]:
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT
                    od.figi,
                    COALESCE(NULLIF(MAX(NULLIF(od.name, '')), ''), NULLIF(MAX(NULLIF(i.name, '')), ''), od.figi) AS name,
                    COALESCE(NULLIF(MAX(NULLIF(i.ticker, '')), ''), '') AS ticker,
                    COALESCE(SUM(
                        CASE
                            WHEN od.operation_type IN ('OPERATION_TYPE_DIVIDEND', 'OPERATION_TYPE_COUPON') THEN od.amount
                            WHEN od.operation_type IN ('OPERATION_TYPE_DIVIDEND_TAX', 'OPERATION_TYPE_COUPON_TAX') THEN od.amount
                            ELSE 0
                        END
                    ), 0) AS net_amount
                FROM operations_dedup od
                LEFT JOIN instruments i ON i.figi = od.figi
                WHERE od.account_id = :account_id
                  AND od.date >= :start_dt
                  AND od.date < :end_dt
                  AND od.state = :executed_state
                  AND od.operation_type IN (
                      'OPERATION_TYPE_DIVIDEND',
                      'OPERATION_TYPE_DIVIDEND_TAX',
                      'OPERATION_TYPE_COUPON',
                      'OPERATION_TYPE_COUPON_TAX'
                  )
                  AND od.figi IS NOT NULL
                GROUP BY od.figi
                ORDER BY net_amount DESC
                """
            ),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )

    parsed = []
    total = Decimal("0")
    for row in rows:
        amount = Decimal(row["net_amount"] or 0)
        total += amount
        parsed.append(
            {
                "figi": row["figi"],
                "name": row["name"] or row["figi"],
                "ticker": row["ticker"] or "",
                "amount": amount,
            }
        )
    return parsed, total


def get_unrealized_at_period_end(session, account_id: str, to_dt: datetime) -> Decimal:
    to_date = to_dt.date() - timedelta(days=1)
    snap = (
        session.execute(
            text(
                """
                SELECT id, expected_yield
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_date <= :to_date
                ORDER BY snapshot_date DESC, snapshot_at DESC
                LIMIT 1
                """
            ),
            {"account_id": account_id, "to_date": to_date},
        )
        .mappings()
        .first()
    )
    if not snap:
        return Decimal("0")

    positions_sum = session.execute(
        text(
            """
            SELECT SUM(expected_yield)
            FROM portfolio_positions
            WHERE snapshot_id = :sid
            """
        ),
        {"sid": snap["id"]},
    ).scalar_one()

    if positions_sum is not None:
        return Decimal(positions_sum)

    snapshot_yield = snap.get("expected_yield")
    if snapshot_yield is None:
        return Decimal("0")
    return Decimal(snapshot_yield)


def get_year_deposits_by_date(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
):
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT date::date AS d, SUM(amount) AS s
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :start_dt
                  AND date < :end_dt
                  AND operation_type IN :operation_types
                  AND state = :executed_state
                GROUP BY date::date
                ORDER BY d ASC
                """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "operation_types": DEPOSIT_OPERATION_TYPES,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_monthly_portfolio_values(
    session,
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
    is_ytd: bool,
):
    rows = (
        session.execute(
            text(
                """
                SELECT month_start, total_value
                FROM (
                    SELECT
                        date_trunc('month', snapshot_date)::date AS month_start,
                        total_value,
                        ROW_NUMBER() OVER (
                            PARTITION BY date_trunc('month', snapshot_date)
                            ORDER BY snapshot_date DESC, snapshot_at DESC
                        ) AS rn
                    FROM portfolio_snapshots
                    WHERE account_id = :account_id
                      AND snapshot_date >= :from_date
                      AND snapshot_date < :to_date
                ) month_snaps
                WHERE rn = 1
                ORDER BY month_start ASC
                """
            ),
            {
                "account_id": account_id,
                "from_date": from_dt.date(),
                "to_date": to_dt.date(),
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_monthly_deposits(session, account_id: str, from_dt: datetime, to_dt: datetime):
    rows = (
        session.execute(
            text(
                f"""
                {OPERATIONS_DEDUP_CTE}
                SELECT
                    date_trunc('month', date)::date AS month_start,
                    SUM(amount) AS amount
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :from_dt
                  AND date < :to_dt
                  AND state = :executed_state
                  AND operation_type = 'OPERATION_TYPE_INPUT'
                GROUP BY month_start
                ORDER BY month_start ASC
                """
            ),
            {
                "account_id": account_id,
                "from_dt": from_dt,
                "to_dt": to_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_last_snapshot_before_date(session, account_id: str, d: date):
    return (
        session.execute(
            text(
                """
                SELECT snapshot_date, total_value
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_date < :d
                ORDER BY snapshot_date DESC, snapshot_at DESC
                LIMIT 1
                """
            ),
            {"account_id": account_id, "d": d},
        )
        .mappings()
        .first()
    )


def get_first_snapshot_in_period(session, account_id: str, from_date: date, to_date: date):
    return (
        session.execute(
            text(
                """
                SELECT snapshot_date, total_value
                FROM portfolio_snapshots
                WHERE account_id = :account_id
                  AND snapshot_date >= :from_date
                  AND snapshot_date < :to_date
                ORDER BY snapshot_date ASC, snapshot_at ASC
                LIMIT 1
                """
            ),
            {"account_id": account_id, "from_date": from_date, "to_date": to_date},
        )
        .mappings()
        .first()
    )


def get_deposits_sum_for_period(session, account_id: str, start_dt: datetime, end_dt: datetime) -> float:
    row = session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            SELECT COALESCE(SUM(amount), 0) AS s
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_dt
              AND date < :end_dt
              AND state = :executed_state
              AND operation_type = 'OPERATION_TYPE_INPUT'
            """
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).scalar_one()
    return float(row or 0)


def get_portfolio_timeseries_agg_by_date(session, account_id: str):
    rows = (
        session.execute(
            text(
                """
        SELECT snapshot_date, total_value
        FROM (
            SELECT
                snapshot_date,
                total_value,
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


def get_external_cashflows_raw(session, account_id: str):
    rows = (
        session.execute(
            text(
                f"""
        {OPERATIONS_DEDUP_CTE}
        SELECT date, amount, operation_type
        FROM operations_dedup
        WHERE account_id = :account_id
          AND operation_type IN :operation_types
          AND state = :executed_state
        ORDER BY date ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_max_value_before_date(session, account_id: str, d: date | None):
    if d is None:
        return None
    row = session.execute(
        text(
            """
        SELECT MAX(total_value) AS m
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :d
        """
        ),
        {"account_id": account_id, "d": d},
    ).scalar_one()
    return float(row) if row is not None else None


def get_max_value_to_date(session, account_id: str, d: date | None):
    if d is None:
        return None
    row = session.execute(
        text(
            """
        SELECT MAX(total_value) AS m
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date <= :d
        """
        ),
        {"account_id": account_id, "d": d},
    ).scalar_one()
    return float(row) if row is not None else None


def get_rebalance_targets(
    session,
    account_id: str,
    asset_classes: tuple[str, ...],
) -> dict[str, Decimal] | None:
    try:
        rows = (
            session.execute(
                text(
                    """
                    SELECT asset_class, target_weight_pct
                    FROM rebalance_targets
                    WHERE account_id = :account_id
                    """
                ),
                {"account_id": account_id},
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "rebalance_targets"):
            session.rollback()
            return None
        raise

    if not rows:
        return {}

    targets = {asset_class: Decimal("0") for asset_class in asset_classes}
    for row in rows:
        asset_class = row["asset_class"]
        if asset_class in targets:
            targets[asset_class] = normalize_decimal(row["target_weight_pct"])
    return targets


def replace_rebalance_targets(
    session,
    account_id: str,
    targets: dict[str, Decimal],
    asset_classes: tuple[str, ...],
) -> bool:
    try:
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        session.execute(
            text("DELETE FROM rebalance_targets WHERE account_id = :account_id"),
            {"account_id": account_id},
        )
        session.execute(
            text(
                """
                INSERT INTO rebalance_targets (
                    account_id,
                    asset_class,
                    target_weight_pct,
                    created_at,
                    updated_at
                )
                VALUES (
                    :account_id,
                    :asset_class,
                    :target_weight_pct,
                    :created_at,
                    :updated_at
                )
                """
            ),
            [
                {
                    "account_id": account_id,
                    "asset_class": asset_class,
                    "target_weight_pct": decimal_to_str(targets[asset_class]),
                    "created_at": now_utc,
                    "updated_at": now_utc,
                }
                for asset_class in asset_classes
            ],
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "rebalance_targets"):
            return False
        raise
    return True


def bootstrap_invest_notifications(session, account_id: str) -> bool:
    try:
        existing_count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM invest_notifications
                WHERE account_id = :account_id
                """
            ),
            {"account_id": account_id},
        ).scalar_one()
    except Exception as exc:
        if _is_undefined_table_error(exc, "invest_notifications"):
            session.rollback()
            return False
        raise

    if existing_count:
        return True

    bootstrap_cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=2)
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    session.execute(
        text(
            f"""
            {OPERATIONS_DEDUP_CTE}
            INSERT INTO invest_notifications (
                account_id,
                operation_id,
                operation_date,
                amount,
                created_at
            )
            SELECT
                operations_dedup.account_id,
                operations_dedup.operation_id,
                operations_dedup.date,
                ABS(operations_dedup.amount),
                :created_at
            FROM operations_dedup
            WHERE operations_dedup.account_id = :account_id
              AND operations_dedup.operation_type IN :operation_types
              AND operations_dedup.state = :executed_state
              AND operations_dedup.date < :bootstrap_cutoff
            ON CONFLICT (account_id, operation_id) DO NOTHING
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "operation_types": DEPOSIT_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
            "bootstrap_cutoff": bootstrap_cutoff,
            "created_at": created_at,
        },
    )
    session.commit()
    return True


def get_pending_invest_notifications(session, account_id: str) -> list[dict] | None:
    bootstrapped = bootstrap_invest_notifications(session, account_id)
    if not bootstrapped:
        return None

    try:
        rows = (
            session.execute(
                text(
                    f"""
                    {OPERATIONS_DEDUP_CTE}
                    SELECT
                        operations_dedup.operation_id,
                        operations_dedup.date,
                        ABS(operations_dedup.amount) AS amount
                    FROM operations_dedup
                    LEFT JOIN invest_notifications notified
                      ON notified.account_id = operations_dedup.account_id
                     AND notified.operation_id = operations_dedup.operation_id
                    WHERE operations_dedup.account_id = :account_id
                      AND operations_dedup.operation_type IN :operation_types
                      AND operations_dedup.state = :executed_state
                      AND notified.operation_id IS NULL
                    ORDER BY operations_dedup.date ASC
                    """
                ).bindparams(bindparam("operation_types", expanding=True)),
                {
                    "account_id": account_id,
                    "operation_types": DEPOSIT_OPERATION_TYPES,
                    "executed_state": EXECUTED_OPERATION_STATE,
                },
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "invest_notifications"):
            session.rollback()
            return None
        raise
    return rows


def mark_invest_notification_sent(
    session,
    *,
    account_id: str,
    operation_id: str,
    operation_date: datetime,
    amount: Decimal,
) -> bool:
    try:
        created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        session.execute(
            text(
                """
                INSERT INTO invest_notifications (
                    account_id,
                    operation_id,
                    operation_date,
                    amount,
                    created_at
                )
                VALUES (
                    :account_id,
                    :operation_id,
                    :operation_date,
                    :amount,
                    :created_at
                )
                ON CONFLICT (account_id, operation_id) DO NOTHING
                """
            ),
            {
                "account_id": account_id,
                "operation_id": operation_id,
                "operation_date": operation_date,
                "amount": decimal_to_str(amount),
                "created_at": created_at,
            },
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "invest_notifications"):
            return False
        raise
    return True


def claim_daily_job_run(
    session,
    *,
    job_name: str,
    run_date: date,
) -> bool | None:
    try:
        created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        result = session.execute(
            text(
                """
                INSERT INTO bot_daily_job_runs (
                    job_name,
                    run_date,
                    status,
                    created_at
                )
                VALUES (
                    :job_name,
                    :run_date,
                    :status,
                    :created_at
                )
                ON CONFLICT (job_name, run_date) DO NOTHING
                """
            ),
            {
                "job_name": job_name,
                "run_date": run_date,
                "status": "started",
                "created_at": created_at,
            },
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "bot_daily_job_runs"):
            return None
        raise
    return bool(result.rowcount)


def complete_daily_job_run(
    session,
    *,
    job_name: str,
    run_date: date,
    sent_total: int,
    failed_total: int,
) -> bool | None:
    try:
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        result = session.execute(
            text(
                """
                UPDATE bot_daily_job_runs
                SET status = :status,
                    completed_at = :completed_at,
                    sent_total = :sent_total,
                    failed_total = :failed_total
                WHERE job_name = :job_name
                  AND run_date = :run_date
                """
            ),
            {
                "job_name": job_name,
                "run_date": run_date,
                "status": "completed",
                "completed_at": completed_at,
                "sent_total": sent_total,
                "failed_total": failed_total,
            },
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "bot_daily_job_runs"):
            return None
        raise
    return bool(result.rowcount)


def release_daily_job_run(
    session,
    *,
    job_name: str,
    run_date: date,
) -> bool | None:
    try:
        result = session.execute(
            text(
                """
                DELETE FROM bot_daily_job_runs
                WHERE job_name = :job_name
                  AND run_date = :run_date
                  AND completed_at IS NULL
                """
            ),
            {
                "job_name": job_name,
                "run_date": run_date,
            },
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "bot_daily_job_runs"):
            return None
        raise
    return bool(result.rowcount)


def get_unnotified_income_events(session, account_id: str) -> list[dict]:
    try:
        return (
            session.execute(
                text(
                    """
                    SELECT
                        ie.id,
                        ie.figi,
                        ie.event_type,
                        ie.net_amount,
                        ie.net_yield_pct,
                        COALESCE(i.name, i.ticker, ie.figi) AS instrument_name
                    FROM income_events ie
                    LEFT JOIN instruments i ON i.figi = ie.figi
                    WHERE ie.account_id = :account_id
                      AND ie.notified = false
                    ORDER BY ie.created_at ASC
                    """
                ),
                {"account_id": account_id},
            )
            .mappings()
            .all()
        )
    except Exception as exc:
        if _is_undefined_table_error(exc, "income_events"):
            session.rollback()
            return []
        raise


def mark_income_event_notified(session, income_event_id: int) -> bool:
    try:
        session.execute(
            text("UPDATE income_events SET notified = true WHERE id = :id"),
            {"id": income_event_id},
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        if _is_undefined_table_error(exc, "income_events"):
            return False
        raise
    return True
