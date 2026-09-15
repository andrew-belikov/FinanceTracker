"""Read-only PostgreSQL queries owned by the reporting use case."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import bindparam, text

from financetracker.common.time_utils import utc_to_local_date
from financetracker.database.operations import operations_dedup_statement
from financetracker.reporting.runtime import TZ, normalize_decimal

TINKOFF_ACCOUNT_ID = os.getenv("TINKOFF_ACCOUNT_ID", "auto").strip()
DEPOSIT_OPERATION_TYPES: tuple[str, ...] = ("OPERATION_TYPE_INPUT",)
WITHDRAWAL_OPERATION_TYPES: tuple[str, ...] = ("OPERATION_TYPE_OUTPUT",)
COMMISSION_OPERATION_TYPES: tuple[str, ...] = ("OPERATION_TYPE_BROKER_FEE", "OPERATION_TYPE_MARGIN_FEE", "OPERATION_TYPE_SUCCESS_FEE", "OPERATION_TYPE_WITHDRAW_COMMISSION", "OPERATION_TYPE_OTHER_FEE")
TAX_OPERATION_TYPES: tuple[str, ...] = ("OPERATION_TYPE_TAX", "OPERATION_TYPE_TAX_PROGRESSIVE", "OPERATION_TYPE_TAX_COUPON", "OPERATION_TYPE_TAX_DIVIDEND")
IIS_TAX_DEDUCTION_CATEGORY = "iis_tax_deduction"
EXECUTED_OPERATION_STATE = "OPERATION_STATE_EXECUTED"
REBALANCE_ASSET_CLASSES: tuple[str, ...] = ("stocks", "bonds", "etf", "currency")


class CurrencyAggregationError(RuntimeError):
    """Raised when nominal values would otherwise be added across currencies."""

@contextmanager
def _required_relation_savepoint(session):
    """Keep a required-relation failure from aborting the outer transaction."""
    with session.begin_nested():
        yield


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

def get_deposits_for_period(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
    currency: str | None = None,
) -> float:
    row = session.execute(
        operations_dedup_statement(
            """
        SELECT COALESCE(SUM(amount), 0) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND date >= :start_dt
          AND date < :end_dt
          AND operation_type IN :operation_types
          AND COALESCE(cashflow_category, '') <> :deduction_category
          AND state = :executed_state
          AND UPPER(COALESCE(currency, '')) = COALESCE(
              :currency,
              (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
               WHERE ps.account_id = :account_id
               ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
          )
        """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "operation_types": operation_types,
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return float(row or 0)


def get_iis_tax_deductions_for_period(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
    currency: str | None = None,
) -> Decimal:
    row = session.execute(
        operations_dedup_statement(
            """
            SELECT COALESCE(SUM(ABS(amount)), 0)
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_dt
              AND date < :end_dt
              AND operation_type IN :deposit_types
              AND cashflow_category = :deduction_category
              AND state = :executed_state
              AND UPPER(COALESCE(currency, '')) = COALESCE(
                  :currency,
                  (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
                   WHERE ps.account_id = :account_id
                   ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
              )
            """
        ).bindparams(bindparam("deposit_types", expanding=True)),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "deposit_types": DEPOSIT_OPERATION_TYPES,
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return Decimal(row or 0)


def get_income_for_period(
    db,
    account_id: str,
    start_date,
    end_date,
    currency: str | None = None,
) -> tuple[Decimal, Decimal]:
    try:
        with _required_relation_savepoint(db):
            rows = db.execute(
                text(
                    """
                SELECT
                    COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN') AS currency,
                    COALESCE(SUM(CASE WHEN event_type = 'coupon' THEN net_amount ELSE 0 END), 0) AS coupons,
                    COALESCE(SUM(CASE WHEN event_type = 'dividend' THEN net_amount ELSE 0 END), 0) AS dividends
                FROM income_events
                WHERE account_id = :account_id
                  AND event_date >= :start_date
                  AND event_date <= :end_date
                  AND UPPER(COALESCE(currency, '')) = COALESCE(
                      :currency,
                      (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
                       WHERE ps.account_id = :account_id
                       ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
                  )
                GROUP BY COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN')
                """
                ),
                {
                    "account_id": account_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "currency": (currency or "").strip().upper() or None,
                },
            ).mappings().all()
    except Exception:
        raise

    if not rows:
        return Decimal("0"), Decimal("0")
    if len(rows) != 1 or rows[0]["currency"] == "UNKNOWN":
        raise CurrencyAggregationError(
            "Income currencies require an explicit selection; implicit FX is disabled"
        )
    row = rows[0]
    return Decimal(row["coupons"] or 0), Decimal(row["dividends"] or 0)


def get_income_currency_breakdown_for_period(
    db,
    account_id: str,
    start_date,
    end_date,
) -> list[dict[str, Decimal | str]]:
    """Return nominal income/tax facts per currency without FX conversion."""
    local_start = (
        utc_to_local_date(start_date, TZ)
        if isinstance(start_date, datetime)
        else start_date
    )
    local_end = (
        utc_to_local_date(end_date, TZ)
        if isinstance(end_date, datetime)
        else end_date
    )
    totals: dict[str, dict[str, Decimal | str]] = {}

    try:
        with _required_relation_savepoint(db):
            income_rows = db.execute(
                text(
                    """
                    SELECT
                        COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN') AS currency,
                        COALESCE(SUM(CASE WHEN event_type = 'coupon' THEN net_amount ELSE 0 END), 0) AS coupons,
                        COALESCE(SUM(CASE WHEN event_type = 'dividend' THEN net_amount ELSE 0 END), 0) AS dividends,
                        COALESCE(SUM(CASE WHEN tax_amount < 0 THEN ABS(tax_amount) ELSE 0 END), 0) AS taxes,
                        COALESCE(SUM(CASE WHEN tax_amount > 0 THEN tax_amount ELSE 0 END), 0) AS tax_refunds
                    FROM income_events
                    WHERE account_id = :account_id
                      AND event_date >= :start_date
                      AND event_date <= :end_date
                    GROUP BY COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN')
                    """
                ),
                {
                    "account_id": account_id,
                    "start_date": local_start,
                    "end_date": local_end,
                },
            ).mappings().all()
    except Exception:
        raise

    for row in income_rows:
        currency = str(row["currency"] or "UNKNOWN").upper()
        totals[currency] = {
            "currency": currency,
            "coupons": Decimal(row["coupons"] or 0),
            "dividends": Decimal(row["dividends"] or 0),
            "taxes": Decimal(row["taxes"] or 0),
            "tax_refunds": Decimal(row["tax_refunds"] or 0),
        }

    operation_rows = db.execute(
        operations_dedup_statement(
            """
            SELECT
                COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN') AS currency,
                COALESCE(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 0) AS taxes,
                COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS tax_refunds
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
            GROUP BY COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN')
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": TAX_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).mappings().all()
    for row in operation_rows:
        currency = str(row["currency"] or "UNKNOWN").upper()
        values = totals.setdefault(
            currency,
            {
                "currency": currency,
                "coupons": Decimal("0"),
                "dividends": Decimal("0"),
                "taxes": Decimal("0"),
                "tax_refunds": Decimal("0"),
            },
        )
        values["taxes"] = Decimal(values["taxes"]) + Decimal(row["taxes"] or 0)
        values["tax_refunds"] = Decimal(values["tax_refunds"]) + Decimal(row["tax_refunds"] or 0)

    return [totals[currency] for currency in sorted(totals)]


def get_commissions_for_period(
    db, account_id: str, start_date, end_date, currency: str | None = None
) -> Decimal:
    total = db.execute(
        operations_dedup_statement(
            """
            SELECT COALESCE(SUM(amount), 0) AS total
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
              AND UPPER(COALESCE(currency, '')) = COALESCE(
                  :currency,
                  (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
                   WHERE ps.account_id = :account_id
                   ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
              )
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": COMMISSION_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return abs(Decimal(total or 0))


def get_taxes_for_period(
    db, account_id: str, start_date, end_date, currency: str | None = None
) -> Decimal:
    income_taxes = Decimal("0")
    try:
        with _required_relation_savepoint(db):
            income_taxes_row = db.execute(
                text(
                    """
                SELECT COALESCE(
                    SUM(CASE WHEN tax_amount < 0 THEN ABS(tax_amount) ELSE 0 END),
                    0
                ) AS total
                FROM income_events
                WHERE account_id = :account_id
                  AND event_date >= :start_date
                  AND event_date <= :end_date
                  AND UPPER(COALESCE(currency, '')) = COALESCE(
                      :currency,
                      (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
                       WHERE ps.account_id = :account_id
                       ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
                  )
                """
                ),
                {
                    "account_id": account_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "currency": (currency or "").strip().upper() or None,
                },
            ).scalar_one()
        income_taxes = abs(Decimal(income_taxes_row or 0))
    except Exception:
        raise

    operation_taxes = db.execute(
        operations_dedup_statement(
            """
            SELECT COALESCE(
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END),
                0
            ) AS total
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
              AND UPPER(COALESCE(currency, '')) = COALESCE(
                  :currency,
                  (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
                   WHERE ps.account_id = :account_id
                   ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
              )
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": TAX_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return income_taxes + abs(Decimal(operation_taxes or 0))


def get_tax_refunds_for_period(
    db, account_id: str, start_date, end_date, currency: str | None = None
) -> Decimal:
    income_refunds = Decimal("0")
    try:
        with _required_relation_savepoint(db):
            income_refunds_row = db.execute(
                text(
                    """
                SELECT COALESCE(
                    SUM(CASE WHEN tax_amount > 0 THEN tax_amount ELSE 0 END),
                    0
                ) AS total
                FROM income_events
                WHERE account_id = :account_id
                  AND event_date >= :start_date
                  AND event_date <= :end_date
                  AND UPPER(COALESCE(currency, '')) = COALESCE(
                      :currency,
                      (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
                       WHERE ps.account_id = :account_id
                       ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
                  )
                """
                ),
                {
                    "account_id": account_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "currency": (currency or "").strip().upper() or None,
                },
            ).scalar_one()
        income_refunds = max(Decimal(income_refunds_row or 0), Decimal("0"))
    except Exception:
        raise

    operation_refunds = db.execute(
        operations_dedup_statement(
            """
            SELECT COALESCE(
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END),
                0
            ) AS total
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_date
              AND date <= :end_date
              AND operation_type IN :operation_types
              AND state = :executed_state
              AND UPPER(COALESCE(currency, '')) = COALESCE(
                  :currency,
                  (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
                   WHERE ps.account_id = :account_id
                   ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
              )
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "start_date": start_date,
            "end_date": end_date,
            "operation_types": TAX_OPERATION_TYPES,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return income_refunds + max(Decimal(operation_refunds or 0), Decimal("0"))


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
                        cashflow_category,
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
                    cashflow_category,
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
    except Exception:
        raise
    return rows


def get_income_events_for_period(session, account_id: str, start_date: date, end_date: date):
    try:
        with _required_relation_savepoint(session):
            rows = (
                session.execute(
                    text(
                    """
                    SELECT
                        ie.event_date,
                        ie.event_type,
                        ie.currency,
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
    except Exception:
        raise
    return rows


def compute_realized_by_asset(session, account_id: str, start_dt: datetime, end_dt: datetime) -> tuple[list[dict], Decimal]:
    rows = (
        session.execute(
            operations_dedup_statement(
                """
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
                  AND UPPER(COALESCE(od.currency, '')) = (
                      SELECT UPPER(ps.currency)
                      FROM portfolio_snapshots ps
                      WHERE ps.account_id = :account_id
                      ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                      LIMIT 1
                  )
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
            operations_dedup_statement(
                """
                SELECT
                    od.figi,
                    COALESCE(NULLIF(MAX(NULLIF(od.name, '')), ''), NULLIF(MAX(NULLIF(i.name, '')), ''), od.figi) AS name,
                    COALESCE(NULLIF(MAX(NULLIF(i.ticker, '')), ''), '') AS ticker,
                    COALESCE(SUM(
                        CASE
                            WHEN od.operation_type IN ('OPERATION_TYPE_DIVIDEND', 'OPERATION_TYPE_COUPON') THEN od.amount
                            WHEN od.operation_type IN (
                                'OPERATION_TYPE_DIVIDEND_TAX',
                                'OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE',
                                'OPERATION_TYPE_COUPON_TAX',
                                'OPERATION_TYPE_BOND_TAX',
                                'OPERATION_TYPE_BOND_TAX_PROGRESSIVE'
                            ) THEN od.amount
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
                      'OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE',
                      'OPERATION_TYPE_COUPON',
                      'OPERATION_TYPE_COUPON_TAX',
                      'OPERATION_TYPE_BOND_TAX',
                      'OPERATION_TYPE_BOND_TAX_PROGRESSIVE'
                  )
                  AND od.figi IS NOT NULL
                  AND UPPER(COALESCE(od.currency, '')) = (
                      SELECT UPPER(ps.currency)
                      FROM portfolio_snapshots ps
                      WHERE ps.account_id = :account_id
                      ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                      LIMIT 1
                  )
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
    to_date = utc_to_local_date(to_dt, TZ) - timedelta(days=1)
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


def get_external_cashflows_raw(
    session,
    account_id: str,
    currency: str | None = None,
):
    rows = (
        session.execute(
            operations_dedup_statement(
                """
        SELECT date, amount, currency, operation_type, cashflow_category
        FROM operations_dedup
        WHERE account_id = :account_id
          AND operation_type IN :operation_types
          AND state = :executed_state
          AND UPPER(COALESCE(currency, '')) = COALESCE(
              :currency,
              (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
               WHERE ps.account_id = :account_id
               ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
          )
        ORDER BY date ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES,
                "executed_state": EXECUTED_OPERATION_STATE,
                "currency": (currency or "").strip().upper() or None,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_rebalance_targets(
    session,
    account_id: str,
    asset_classes: tuple[str, ...] = REBALANCE_ASSET_CLASSES,
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
    except Exception:
        raise

    if not rows:
        return {}

    targets = {asset_class: Decimal("0") for asset_class in asset_classes}
    for row in rows:
        asset_class = row["asset_class"]
        if asset_class in targets:
            targets[asset_class] = normalize_decimal(row["target_weight_pct"])
    return targets


def _to_local_market_date(value: datetime | None) -> date | None:
    return utc_to_local_date(value, TZ) if value is not None else None


def _build_net_external_flow_by_day(external_cashflows: list[dict]) -> dict[date, float]:
    flows: dict[date, float] = {}
    for row in external_cashflows:
        local_date = _to_local_market_date(row.get("date"))
        if local_date is None:
            continue
        amount = abs(float(row.get("amount") or 0.0))
        operation_type = (row.get("operation_type") or "").strip()
        if (
            operation_type in DEPOSIT_OPERATION_TYPES
            and row.get("cashflow_category") == IIS_TAX_DEDUCTION_CATEGORY
        ):
            continue
        if operation_type in DEPOSIT_OPERATION_TYPES:
            signed_amount = amount
        elif operation_type in WITHDRAWAL_OPERATION_TYPES:
            signed_amount = -amount
        else:
            continue
        flows[local_date] = flows.get(local_date, 0.0) + signed_amount
    return flows


def _compute_twr_series(
    snapshot_rows: list[dict],
    net_external_flow_by_day: dict[date, float],
) -> tuple[list[date], list[float | None], list[float]] | None:
    if len(snapshot_rows) < 2:
        return None
    dates = [row["snapshot_date"] for row in snapshot_rows]
    values = [float(row["total_value"]) if row.get("total_value") is not None else None for row in snapshot_rows]
    cumulative_multiplier = 1.0
    twr: list[float] = [0.0]
    for index in range(1, len(dates)):
        previous_value = values[index - 1]
        current_value = values[index]
        net_external_flow = sum(
            flow
            for flow_date, flow in net_external_flow_by_day.items()
            if dates[index - 1] < flow_date <= dates[index]
        )
        if previous_value in (None, 0) or current_value is None:
            twr.append(cumulative_multiplier - 1.0)
            continue
        cumulative_multiplier *= 1.0 + (current_value - net_external_flow) / previous_value - 1.0
        twr.append(cumulative_multiplier - 1.0)
    return dates, values, twr


def compute_twr_timeseries(session, account_id: str):
    return _compute_twr_series(
        get_portfolio_timeseries_agg_by_date(session, account_id),
        _build_net_external_flow_by_day(get_external_cashflows_raw(session, account_id)),
    )
