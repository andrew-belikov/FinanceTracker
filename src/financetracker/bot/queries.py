from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import TypedDict

from sqlalchemy import bindparam, text

from financetracker.bot.runtime import (
    COMMISSION_OPERATION_TYPES,
    DEPOSIT_OPERATION_TYPES,
    EXECUTED_OPERATION_STATE,
    IIS_TAX_DEDUCTION_CATEGORY,
    TAX_OPERATION_TYPES,
    TZ,
    TZ_NAME,
    WITHDRAWAL_OPERATION_TYPES,
)
from financetracker.common.time_utils import utc_to_local_date
from financetracker.database.operations import operations_dedup_statement
from financetracker.bot.portfolio_repository import (  # noqa: F401
    get_last_snapshot_before_date as _get_last_snapshot_before_date,
    get_portfolio_timeseries as _get_portfolio_timeseries,
    get_positions_diff_rows as _get_positions_diff_rows,
    get_positions_diff_snapshot_bounds as _get_positions_diff_snapshot_bounds,
)
from financetracker.bot.portfolio_repository import (  # noqa: F401
    get_daily_snapshot_rows,
    get_dataset_bounds,
    get_instrument_eod_rows,
    get_latest_snapshot_date,
    get_latest_snapshot_with_id,
    get_latest_snapshot_with_totals_before_date,
    get_latest_snapshots,
    get_month_snapshots,
    get_period_daily_snapshot_rows,
    get_period_snapshots,
    get_positions_for_snapshot,
)
from financetracker.bot.operations_repository import (  # noqa: F401
    compute_income_by_asset_net,
    compute_realized_by_asset,
    get_unrealized_at_period_end,
    get_year_financials_from_operations,
)
from financetracker.bot.dataset_repository import (  # noqa: F401
    get_asset_alias_rows,
    get_dataset_operations,
    get_income_events_for_period,
)
# Query and repository owner for chart and scheduling reads.
from financetracker.bot.reporting_account import resolve_reporting_account_id  # noqa: F401
# Compatibility re-exports for callers migrating to notification_repository.
from financetracker.bot.notification_repository import (  # noqa: F401
    bootstrap_invest_notifications,
    claim_daily_job_run,
    claim_notification_delivery,
    complete_daily_job_run,
    complete_notification_delivery,
    get_notification_delivery_status,
    get_pending_invest_notifications,
    get_unnotified_income_events,
    heartbeat_daily_job_run,
    mark_income_event_notified,
    mark_invest_notification_sent,
    mark_notification_delivery_uncertain,
    notification_deliveries_complete,
    release_daily_job_run,
    release_notification_delivery,
    set_iis_tax_deduction_category,
)


class IncomeNotificationRow(TypedDict):
    id: int
    figi: str
    event_type: str
    currency: str
    net_amount: Decimal
    net_yield_pct: Decimal
    coupon_period_days: int | None
    instrument_name: str


class InvestNotificationRow(TypedDict):
    operation_id: str
    date: datetime
    amount: Decimal
    cashflow_category: str | None


class CurrencyAggregationError(RuntimeError):
    """Raised when nominal values would otherwise be added across currencies."""


@contextmanager
def _required_relation_savepoint(session):
    """Keep a required-relation failure from aborting the outer transaction."""
    with session.begin_nested():
        yield



def get_latest_deposit_date(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
):
    return session.execute(
        operations_dedup_statement(
            """
            SELECT MAX((date AT TIME ZONE :timezone)::date)
            FROM operations_dedup
            WHERE account_id = :account_id
              AND operation_type IN :operation_types
              AND COALESCE(cashflow_category, '') <> :deduction_category
              AND state = :executed_state
            """
        ).bindparams(bindparam("operation_types", expanding=True)),
        {
            "account_id": account_id,
            "operation_types": operation_types,
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
            "executed_state": EXECUTED_OPERATION_STATE,
            "timezone": TZ_NAME,
        },
    ).scalar_one()


def get_total_deposits(
    session,
    account_id: str,
    operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES,
    currency: str | None = None,
) -> float:
    row = session.execute(
        operations_dedup_statement(
            """
            SELECT COALESCE(SUM(amount), 0) AS s
            FROM operations_dedup
            WHERE account_id = :account_id
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
            "operation_types": operation_types,
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return float(row or 0)


def get_net_external_contributions(
    session,
    account_id: str,
    currency: str | None = None,
) -> float:
    row = session.execute(
        operations_dedup_statement(
            """
            SELECT COALESCE(
                SUM(
                    CASE
                        WHEN operation_type IN :deposit_types
                             AND COALESCE(cashflow_category, '') <> :deduction_category
                        THEN ABS(amount)
                        WHEN operation_type IN :withdrawal_types THEN -ABS(amount)
                        ELSE 0
                    END
                ),
                0
            )
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
            """
        ).bindparams(
            bindparam("deposit_types", expanding=True),
            bindparam("withdrawal_types", expanding=True),
            bindparam("operation_types", expanding=True),
        ),
        {
            "account_id": account_id,
            "deposit_types": DEPOSIT_OPERATION_TYPES,
            "withdrawal_types": WITHDRAWAL_OPERATION_TYPES,
            "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES,
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return float(row or 0)


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


def get_net_external_flow_for_period(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
    currency: str | None = None,
) -> float:
    row = session.execute(
        operations_dedup_statement(
            """
        SELECT COALESCE(
            SUM(
                CASE
                    WHEN operation_type IN :deposit_types
                         AND COALESCE(cashflow_category, '') <> :deduction_category
                    THEN ABS(amount)
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
          AND UPPER(COALESCE(currency, '')) = COALESCE(
              :currency,
              (SELECT UPPER(ps.currency) FROM portfolio_snapshots ps
               WHERE ps.account_id = :account_id
               ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1)
          )
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
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
            "executed_state": EXECUTED_OPERATION_STATE,
            "currency": (currency or "").strip().upper() or None,
        },
    ).scalar_one()
    return float(row or 0)


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
        # income_events is required by the versioned schema manifest. Returning
        # an empty financial fact here would silently misstate the report.
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
            operations_dedup_statement(
                """
        SELECT (date AT TIME ZONE :timezone)::date AS d, SUM(amount) AS s
        FROM operations_dedup
        WHERE account_id = :account_id
          AND operation_type IN :operation_types
          AND COALESCE(cashflow_category, '') <> :deduction_category
          AND state = :executed_state
          AND UPPER(COALESCE(currency, '')) = (
              SELECT UPPER(ps.currency)
              FROM portfolio_snapshots ps
              WHERE ps.account_id = :account_id
              ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
              LIMIT 1
          )
        GROUP BY (date AT TIME ZONE :timezone)::date
        ORDER BY d ASC
        """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_types": operation_types,
                "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
                "executed_state": EXECUTED_OPERATION_STATE,
                "timezone": TZ_NAME,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_iis_tax_deductions_by_date(session, account_id: str):
    return (
        session.execute(
            operations_dedup_statement(
                """
                SELECT (date AT TIME ZONE :timezone)::date AS d, SUM(ABS(amount)) AS s
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND operation_type IN :operation_types
                  AND cashflow_category = :deduction_category
                  AND state = :executed_state
                  AND UPPER(COALESCE(currency, '')) = (
                      SELECT UPPER(ps.currency)
                      FROM portfolio_snapshots ps
                      WHERE ps.account_id = :account_id
                      ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                      LIMIT 1
                  )
                GROUP BY (date AT TIME ZONE :timezone)::date
                ORDER BY d ASC
                """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_types": DEPOSIT_OPERATION_TYPES,
                "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
                "executed_state": EXECUTED_OPERATION_STATE,
                "timezone": TZ_NAME,
            },
        )
        .mappings()
        .all()
    )



def get_year_deposits_by_date(
    session,
    account_id: str,
    start_dt: datetime,
    end_dt: datetime,
):
    rows = (
        session.execute(
            operations_dedup_statement(
                """
                SELECT (date AT TIME ZONE :timezone)::date AS d, SUM(amount) AS s
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :start_dt
                  AND date < :end_dt
                  AND operation_type IN :operation_types
                  AND COALESCE(cashflow_category, '') <> :deduction_category
                  AND state = :executed_state
                  AND UPPER(COALESCE(currency, '')) = (
                      SELECT UPPER(ps.currency)
                      FROM portfolio_snapshots ps
                      WHERE ps.account_id = :account_id
                      ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                      LIMIT 1
                  )
                GROUP BY (date AT TIME ZONE :timezone)::date
                ORDER BY d ASC
                """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "operation_types": DEPOSIT_OPERATION_TYPES,
                "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
                "executed_state": EXECUTED_OPERATION_STATE,
                "timezone": TZ_NAME,
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
            operations_dedup_statement(
                """
                SELECT
                    date_trunc('month', date AT TIME ZONE :timezone)::date AS month_start,
                    SUM(amount) AS amount
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :from_dt
                  AND date < :to_dt
                  AND state = :executed_state
                  AND operation_type = 'OPERATION_TYPE_INPUT'
                  AND COALESCE(cashflow_category, '') <> :deduction_category
                  AND UPPER(COALESCE(currency, '')) = (
                      SELECT UPPER(ps.currency)
                      FROM portfolio_snapshots ps
                      WHERE ps.account_id = :account_id
                      ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                      LIMIT 1
                  )
                GROUP BY month_start
                ORDER BY month_start ASC
                """
            ),
            {
                "account_id": account_id,
                "from_dt": from_dt,
                "to_dt": to_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
                "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
                "timezone": TZ_NAME,
            },
        )
        .mappings()
        .all()
    )
    return rows


def get_monthly_net_external_flows(
    session,
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
):
    return (
        session.execute(
            operations_dedup_statement(
                """
                SELECT
                    date_trunc('month', date AT TIME ZONE :timezone)::date AS month_start,
                    SUM(
                        CASE
                            WHEN operation_type IN :deposit_types
                                 AND COALESCE(cashflow_category, '') <> :deduction_category
                            THEN ABS(amount)
                            WHEN operation_type IN :withdrawal_types THEN -ABS(amount)
                            ELSE 0
                        END
                    ) AS amount
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :from_dt
                  AND date < :to_dt
                  AND state = :executed_state
                  AND operation_type IN :operation_types
                  AND UPPER(COALESCE(currency, '')) = (
                      SELECT UPPER(ps.currency)
                      FROM portfolio_snapshots ps
                      WHERE ps.account_id = :account_id
                      ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                      LIMIT 1
                  )
                GROUP BY month_start
                ORDER BY month_start ASC
                """
            ).bindparams(
                bindparam("deposit_types", expanding=True),
                bindparam("withdrawal_types", expanding=True),
                bindparam("operation_types", expanding=True),
            ),
            {
                "account_id": account_id,
                "from_dt": from_dt,
                "to_dt": to_dt,
                "deposit_types": DEPOSIT_OPERATION_TYPES,
                "withdrawal_types": WITHDRAWAL_OPERATION_TYPES,
                "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES,
                "executed_state": EXECUTED_OPERATION_STATE,
                "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
                "timezone": TZ_NAME,
            },
        )
        .mappings()
        .all()
    )


def get_monthly_iis_tax_deductions(
    session,
    account_id: str,
    from_dt: datetime,
    to_dt: datetime,
):
    return (
        session.execute(
            operations_dedup_statement(
                """
                SELECT
                    date_trunc('month', date AT TIME ZONE :timezone)::date AS month_start,
                    SUM(ABS(amount)) AS amount
                FROM operations_dedup
                WHERE account_id = :account_id
                  AND date >= :from_dt
                  AND date < :to_dt
                  AND state = :executed_state
                  AND operation_type = 'OPERATION_TYPE_INPUT'
                  AND cashflow_category = :deduction_category
                  AND UPPER(COALESCE(currency, '')) = (
                      SELECT UPPER(ps.currency)
                      FROM portfolio_snapshots ps
                      WHERE ps.account_id = :account_id
                      ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                      LIMIT 1
                  )
                GROUP BY month_start
                ORDER BY month_start ASC
                """
            ),
            {
                "account_id": account_id,
                "from_dt": from_dt,
                "to_dt": to_dt,
                "executed_state": EXECUTED_OPERATION_STATE,
                "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
                "timezone": TZ_NAME,
            },
        )
        .mappings()
        .all()
    )


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
        operations_dedup_statement(
            """
            SELECT COALESCE(SUM(amount), 0) AS s
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_dt
              AND date < :end_dt
              AND state = :executed_state
              AND operation_type = 'OPERATION_TYPE_INPUT'
              AND COALESCE(cashflow_category, '') <> :deduction_category
              AND UPPER(COALESCE(currency, '')) = (
                  SELECT UPPER(ps.currency)
                  FROM portfolio_snapshots ps
                  WHERE ps.account_id = :account_id
                  ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                  LIMIT 1
              )
            """
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "executed_state": EXECUTED_OPERATION_STATE,
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
        },
    ).scalar_one()
    return float(row or 0)


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
def get_snapshot_for_date(session, account_id: str, d: date | None):
    if d is None:
        return None
    row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date = :d
        ORDER BY snapshot_at DESC, id DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "d": d},
        )
        .mappings()
        .first()
    )
    return row


def get_max_snapshot_before_date(session, account_id: str, d: date | None):
    if d is None:
        return None
    row = (
        session.execute(
            text(
                """
        SELECT id, snapshot_date, snapshot_at, total_value
        FROM portfolio_snapshots
        WHERE account_id = :account_id
          AND snapshot_date < :d
          AND total_value IS NOT NULL
        ORDER BY total_value DESC, snapshot_date DESC, snapshot_at DESC, id DESC
        LIMIT 1
        """
            ),
            {"account_id": account_id, "d": d},
        )
        .mappings()
        .first()
    )
    return row


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
