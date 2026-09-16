"""Operations and cashflow read model."""

from datetime import datetime, timedelta
from decimal import Decimal
from sqlalchemy import bindparam, text

from financetracker.bot.runtime import (
    DEPOSIT_OPERATION_TYPES,
    EXECUTED_OPERATION_STATE,
    IIS_TAX_DEDUCTION_CATEGORY,
    TZ,
)
from financetracker.common.time_utils import utc_to_local_date
from financetracker.database.operations import operations_dedup_statement

def get_year_financials_from_operations(session, account_id: str, start_dt: datetime, end_dt: datetime) -> dict[str, Decimal]:
    row = session.execute(
        operations_dedup_statement(
            """
            SELECT
                COALESCE(SUM(CASE
                    WHEN operation_type IN :deposit_types
                         AND COALESCE(cashflow_category, '') <> :deduction_category
                    THEN amount ELSE 0 END), 0) AS deposits,
                COALESCE(SUM(CASE
                    WHEN operation_type IN :deposit_types
                         AND cashflow_category = :deduction_category
                    THEN ABS(amount) ELSE 0 END), 0) AS iis_tax_deduction_income,
                COALESCE(SUM(CASE
                    WHEN operation_type IN (
                        'OPERATION_TYPE_DIVIDEND',
                        'OPERATION_TYPE_DIVIDEND_TAX',
                        'OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE'
                    ) THEN amount
                    ELSE 0
                END), 0) AS dividend_net,
                COALESCE(SUM(CASE
                    WHEN operation_type IN (
                        'OPERATION_TYPE_COUPON',
                        'OPERATION_TYPE_COUPON_TAX',
                        'OPERATION_TYPE_BOND_TAX',
                        'OPERATION_TYPE_BOND_TAX_PROGRESSIVE'
                    ) THEN amount
                    ELSE 0
                END), 0) AS coupon_net
            FROM operations_dedup
            WHERE account_id = :account_id
              AND date >= :start_dt
              AND date < :end_dt
              AND state = :executed_state
              AND UPPER(COALESCE(currency, '')) = (
                  SELECT UPPER(ps.currency)
                  FROM portfolio_snapshots ps
                  WHERE ps.account_id = :account_id
                  ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                  LIMIT 1
              )
            """
        ).bindparams(
            bindparam("deposit_types", expanding=True),
        ),
        {
            "account_id": account_id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "deposit_types": DEPOSIT_OPERATION_TYPES,
            "deduction_category": IIS_TAX_DEDUCTION_CATEGORY,
            "executed_state": EXECUTED_OPERATION_STATE,
        },
    ).mappings().one()

    dividend_net = Decimal(row["dividend_net"] or 0)
    coupon_net = Decimal(row["coupon_net"] or 0)
    iis_tax_deduction_income = Decimal(row["iis_tax_deduction_income"] or 0)
    income_net = dividend_net + coupon_net

    return {
        "deposits": Decimal(row["deposits"] or 0),
        "income_net": income_net,
        "dividend_net": dividend_net,
        "coupon_net": coupon_net,
        "iis_tax_deduction_income": iis_tax_deduction_income,
    }


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
