"""Read-only queries used exclusively by the dataset export."""

from datetime import date, datetime

from sqlalchemy import text

from financetracker.bot.runtime import EXECUTED_OPERATION_STATE


def get_dataset_operations(session, account_id: str, start_dt: datetime, end_dt: datetime):
    return (
        session.execute(
            text(
                """
                WITH operations_dedup AS (
                    SELECT DISTINCT ON (account_id, COALESCE(operation_id, id::text))
                        account_id, operation_id, date, amount, currency, operation_type,
                        cashflow_category, state, instrument_uid, asset_uid, figi, name,
                        commission, yield, description, source, price, quantity
                    FROM operations
                    ORDER BY account_id, COALESCE(operation_id, id::text), id DESC
                )
                SELECT operation_id, date, amount, currency, operation_type,
                       cashflow_category, state, instrument_uid, asset_uid, figi, name,
                       commission, yield, description, source, price, quantity
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


def get_asset_alias_rows(session):
    return (
        session.execute(
            text(
                """
                SELECT asset_uid, instrument_uid, figi, ticker, name,
                       first_seen_at, last_seen_at
                FROM asset_aliases
                ORDER BY asset_uid ASC, last_seen_at DESC
                """
            )
        )
        .mappings()
        .all()
    )


def get_income_events_for_period(
    session,
    account_id: str,
    start_date: date,
    end_date: date,
):
    return (
        session.execute(
            text(
                """
                SELECT ie.event_date, ie.event_type, ie.currency, ie.figi,
                       COALESCE(i.ticker, '') AS ticker,
                       COALESCE(i.name, ie.figi) AS instrument_name,
                       ie.gross_amount, ie.tax_amount, ie.net_amount,
                       ie.net_yield_pct, ie.notified
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
