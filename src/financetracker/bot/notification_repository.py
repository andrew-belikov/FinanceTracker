from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import TypedDict

from sqlalchemy import bindparam, text

from financetracker.bot.sql import operations_dedup_statement
from financetracker.bot.runtime import (
    DEPOSIT_OPERATION_TYPES,
    EXECUTED_OPERATION_STATE,
    IIS_TAX_DEDUCTION_CATEGORY,
    decimal_to_str,
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


__all__ = [
    "bootstrap_invest_notifications",
    "claim_daily_job_run",
    "claim_notification_delivery",
    "complete_daily_job_run",
    "complete_notification_delivery",
    "get_notification_delivery_status",
    "get_pending_invest_notifications",
    "get_unnotified_income_events",
    "heartbeat_daily_job_run",
    "mark_income_event_notified",
    "mark_invest_notification_sent",
    "mark_notification_delivery_uncertain",
    "notification_deliveries_complete",
    "release_daily_job_run",
    "release_notification_delivery",
    "set_iis_tax_deduction_category",
]

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
    except Exception:
        session.rollback()
        raise

    if existing_count:
        return True

    bootstrap_cutoff = datetime.now(timezone.utc) - timedelta(minutes=2)
    created_at = datetime.now(timezone.utc)
    session.execute(
        operations_dedup_statement(
            """
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


def get_pending_invest_notifications(
    session,
    account_id: str,
) -> list[InvestNotificationRow] | None:
    bootstrapped = bootstrap_invest_notifications(session, account_id)
    if not bootstrapped:
        return None

    try:
        rows = (
            session.execute(
                operations_dedup_statement(
                    """
                    SELECT
                        operations_dedup.operation_id,
                        operations_dedup.date,
                        ABS(operations_dedup.amount) AS amount,
                        operations_dedup.cashflow_category
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
    except Exception:
        session.rollback()
        raise
    return rows


def set_iis_tax_deduction_category(
    session,
    *,
    account_id: str,
    operation_id: str,
    enabled: bool,
) -> str:
    row = (
        session.execute(
            text(
                """
                SELECT cashflow_category
                FROM operations
                WHERE account_id = :account_id
                  AND operation_id = :operation_id
                  AND operation_type IN :operation_types
                  AND state = :executed_state
                FOR UPDATE
                """
            ).bindparams(bindparam("operation_types", expanding=True)),
            {
                "account_id": account_id,
                "operation_id": operation_id,
                "operation_types": DEPOSIT_OPERATION_TYPES,
                "executed_state": EXECUTED_OPERATION_STATE,
            },
        )
        .mappings()
        .first()
    )
    if row is None:
        session.rollback()
        return "not_found"

    current_category = row.get("cashflow_category")
    desired_category = IIS_TAX_DEDUCTION_CATEGORY if enabled else None
    if current_category == desired_category:
        session.commit()
        return "unchanged"
    if not enabled and current_category != IIS_TAX_DEDUCTION_CATEGORY:
        session.commit()
        return "unchanged"

    session.execute(
        text(
            """
            UPDATE operations
            SET cashflow_category = :cashflow_category
            WHERE account_id = :account_id
              AND operation_id = :operation_id
            """
        ),
        {
            "account_id": account_id,
            "operation_id": operation_id,
            "cashflow_category": desired_category,
        },
    )
    session.commit()
    return "marked" if enabled else "unmarked"


def mark_invest_notification_sent(
    session,
    *,
    account_id: str,
    operation_id: str,
    operation_date: datetime,
    amount: Decimal,
) -> bool:
    try:
        created_at = datetime.now(timezone.utc)
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
    except Exception:
        session.rollback()
        raise
    return True


def claim_daily_job_run(
    session,
    *,
    job_name: str,
    run_date: date,
    attempt_id: str,
    now_utc: datetime | None = None,
    lease_timeout: timedelta = timedelta(minutes=15),
) -> bool | None:
    try:
        claimed_at = now_utc or datetime.now(timezone.utc)
        stale_before = claimed_at - lease_timeout
        result = session.execute(
            text(
                """
                INSERT INTO bot_daily_job_runs (
                    job_name,
                    run_date,
                    status,
                    attempt_id,
                    claimed_at,
                    heartbeat_at,
                    created_at
                )
                VALUES (
                    :job_name,
                    :run_date,
                    :status,
                    :attempt_id,
                    :claimed_at,
                    :claimed_at,
                    :claimed_at
                )
                ON CONFLICT (job_name, run_date) DO UPDATE
                SET status = 'started',
                    attempt_id = EXCLUDED.attempt_id,
                    claimed_at = EXCLUDED.claimed_at,
                    heartbeat_at = EXCLUDED.heartbeat_at,
                    completed_at = NULL
                WHERE bot_daily_job_runs.status <> 'completed'
                  AND bot_daily_job_runs.heartbeat_at < :stale_before
                RETURNING attempt_id
                """
            ),
            {
                "job_name": job_name,
                "run_date": run_date,
                "status": "started",
                "attempt_id": attempt_id,
                "claimed_at": claimed_at,
                "stale_before": stale_before,
            },
        )
        claimed = result.mappings().first()
        session.commit()
    except Exception:
        session.rollback()
        raise
    return claimed is not None


def complete_daily_job_run(
    session,
    *,
    job_name: str,
    run_date: date,
    attempt_id: str,
    sent_total: int,
    failed_total: int,
) -> bool | None:
    try:
        completed_at = datetime.now(timezone.utc)
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
                  AND attempt_id = :attempt_id
                  AND status = 'started'
                """
            ),
            {
                "job_name": job_name,
                "run_date": run_date,
                "attempt_id": attempt_id,
                "status": "completed",
                "completed_at": completed_at,
                "sent_total": sent_total,
                "failed_total": failed_total,
            },
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return bool(result.rowcount)


def heartbeat_daily_job_run(
    session,
    *,
    job_name: str,
    run_date: date,
    attempt_id: str,
) -> bool | None:
    try:
        heartbeat_at = datetime.now(timezone.utc)
        result = session.execute(
            text(
                """
                UPDATE bot_daily_job_runs
                SET heartbeat_at = :heartbeat_at
                WHERE job_name = :job_name
                  AND run_date = :run_date
                  AND attempt_id = :attempt_id
                  AND status = 'started'
                """
            ),
            {
                "job_name": job_name,
                "run_date": run_date,
                "attempt_id": attempt_id,
                "heartbeat_at": heartbeat_at,
            },
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return bool(result.rowcount)


def release_daily_job_run(
    session,
    *,
    job_name: str,
    run_date: date,
    attempt_id: str,
) -> bool | None:
    try:
        result = session.execute(
            text(
                """
                DELETE FROM bot_daily_job_runs
                WHERE job_name = :job_name
                  AND run_date = :run_date
                  AND attempt_id = :attempt_id
                  AND status = 'started'
                  AND completed_at IS NULL
                """
            ),
            {
                "job_name": job_name,
                "run_date": run_date,
                "attempt_id": attempt_id,
            },
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return bool(result.rowcount)


def claim_notification_delivery(
    session,
    *,
    notification_kind: str,
    notification_key: str,
    chat_id: int,
    message_type: str,
    attempt_id: str,
    now_utc: datetime | None = None,
    lease_timeout: timedelta = timedelta(minutes=15),
    reclaim_stale: bool = True,
) -> bool | None:
    try:
        claimed_at = now_utc or datetime.now(timezone.utc)
        stale_before = claimed_at - lease_timeout
        result = session.execute(
            text(
                """
                INSERT INTO bot_notification_deliveries (
                    notification_kind,
                    notification_key,
                    chat_id,
                    message_type,
                    status,
                    attempt_id,
                    claimed_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    :notification_kind,
                    :notification_key,
                    :chat_id,
                    :message_type,
                    'started',
                    :attempt_id,
                    :claimed_at,
                    :claimed_at,
                    :claimed_at
                )
                ON CONFLICT (notification_kind, notification_key, chat_id, message_type)
                DO UPDATE
                SET status = 'started',
                    attempt_id = EXCLUDED.attempt_id,
                    claimed_at = EXCLUDED.claimed_at,
                    delivered_at = NULL,
                    updated_at = EXCLUDED.updated_at
                WHERE :reclaim_stale
                  AND bot_notification_deliveries.status = 'started'
                  AND bot_notification_deliveries.claimed_at < :stale_before
                RETURNING attempt_id
                """
            ),
            {
                "notification_kind": notification_kind,
                "notification_key": notification_key,
                "chat_id": chat_id,
                "message_type": message_type,
                "attempt_id": attempt_id,
                "claimed_at": claimed_at,
                "stale_before": stale_before,
                "reclaim_stale": reclaim_stale,
            },
        )
        claimed = result.mappings().first()
        session.commit()
    except Exception:
        session.rollback()
        raise
    return claimed is not None


def complete_notification_delivery(
    session,
    *,
    notification_kind: str,
    notification_key: str,
    chat_id: int,
    message_type: str,
    attempt_id: str,
) -> bool | None:
    try:
        delivered_at = datetime.now(timezone.utc)
        result = session.execute(
            text(
                """
                UPDATE bot_notification_deliveries
                SET status = 'sent',
                    delivered_at = :delivered_at,
                    updated_at = :delivered_at
                WHERE notification_kind = :notification_kind
                  AND notification_key = :notification_key
                  AND chat_id = :chat_id
                  AND message_type = :message_type
                  AND attempt_id = :attempt_id
                  AND status = 'started'
                """
            ),
            {
                "notification_kind": notification_kind,
                "notification_key": notification_key,
                "chat_id": chat_id,
                "message_type": message_type,
                "attempt_id": attempt_id,
                "delivered_at": delivered_at,
            },
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return bool(result.rowcount)


def release_notification_delivery(
    session,
    *,
    notification_kind: str,
    notification_key: str,
    chat_id: int,
    message_type: str,
    attempt_id: str,
) -> bool | None:
    try:
        result = session.execute(
            text(
                """
                DELETE FROM bot_notification_deliveries
                WHERE notification_kind = :notification_kind
                  AND notification_key = :notification_key
                  AND chat_id = :chat_id
                  AND message_type = :message_type
                  AND attempt_id = :attempt_id
                  AND status = 'started'
                """
            ),
            {
                "notification_kind": notification_kind,
                "notification_key": notification_key,
                "chat_id": chat_id,
                "message_type": message_type,
                "attempt_id": attempt_id,
            },
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return bool(result.rowcount)


def mark_notification_delivery_uncertain(
    session,
    *,
    notification_kind: str,
    notification_key: str,
    chat_id: int,
    message_type: str,
    attempt_id: str,
) -> bool | None:
    try:
        updated_at = datetime.now(timezone.utc)
        result = session.execute(
            text(
                """
                UPDATE bot_notification_deliveries
                SET status = 'uncertain',
                    updated_at = :updated_at
                WHERE notification_kind = :notification_kind
                  AND notification_key = :notification_key
                  AND chat_id = :chat_id
                  AND message_type = :message_type
                  AND attempt_id = :attempt_id
                  AND status = 'started'
                """
            ),
            {
                "notification_kind": notification_kind,
                "notification_key": notification_key,
                "chat_id": chat_id,
                "message_type": message_type,
                "attempt_id": attempt_id,
                "updated_at": updated_at,
            },
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return bool(result.rowcount)


def notification_deliveries_complete(
    session,
    *,
    notification_kind: str,
    notification_key: str,
    chat_ids: set[int],
    message_types: set[str],
) -> bool | None:
    if not chat_ids or not message_types:
        return True
    try:
        sent_count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM bot_notification_deliveries
                WHERE notification_kind = :notification_kind
                  AND notification_key = :notification_key
                  AND chat_id IN :chat_ids
                  AND message_type IN :message_types
                  AND status = 'sent'
                """
            ).bindparams(
                bindparam("chat_ids", expanding=True),
                bindparam("message_types", expanding=True),
            ),
            {
                "notification_kind": notification_kind,
                "notification_key": notification_key,
                "chat_ids": tuple(chat_ids),
                "message_types": tuple(message_types),
            },
        ).scalar_one()
    except Exception:
        session.rollback()
        raise
    return int(sent_count) == len(chat_ids) * len(message_types)


def get_notification_delivery_status(
    session,
    *,
    notification_kind: str,
    notification_key: str,
    chat_id: int,
    message_type: str,
) -> str | None:
    try:
        return session.execute(
            text(
                """
                SELECT status
                FROM bot_notification_deliveries
                WHERE notification_kind = :notification_kind
                  AND notification_key = :notification_key
                  AND chat_id = :chat_id
                  AND message_type = :message_type
                """
            ),
            {
                "notification_kind": notification_kind,
                "notification_key": notification_key,
                "chat_id": chat_id,
                "message_type": message_type,
            },
        ).scalar_one_or_none()
    except Exception:
        session.rollback()
        raise


def get_unnotified_income_events(
    session,
    account_id: str,
) -> list[IncomeNotificationRow]:
    try:
        with _required_relation_savepoint(session):
            return (
                session.execute(
                    text(
                    """
                    SELECT
                        ie.id,
                        ie.figi,
                        ie.event_type,
                        ie.currency,
                        ie.net_amount,
                        ie.net_yield_pct,
                        coupon.coupon_period_days,
                        COALESCE(i.name, i.ticker, ie.figi) AS instrument_name
                    FROM income_events ie
                    LEFT JOIN instruments i ON i.figi = ie.figi
                    LEFT JOIN LATERAL (
                        SELECT
                            CASE
                                WHEN COUNT(*) = 1
                                THEN MAX(
                                    pce.coupon_end_date - pce.coupon_start_date
                                )
                                ELSE NULL
                            END AS coupon_period_days
                        FROM payout_calendar_events pce
                        WHERE pce.account_id = ie.account_id
                          AND pce.figi = ie.figi
                          AND pce.event_type = 'coupon'
                          AND pce.payment_date = ie.event_date
                          AND pce.coupon_start_date IS NOT NULL
                          AND pce.coupon_end_date IS NOT NULL
                          AND pce.coupon_end_date > pce.coupon_start_date
                          AND (
                              pce.coupon_period_days IS NULL
                              OR pce.coupon_period_days = (
                                  pce.coupon_end_date - pce.coupon_start_date
                              )
                          )
                    ) coupon ON ie.event_type = 'coupon'
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
    except Exception:
        raise


def mark_income_event_notified(session, income_event_id: int) -> bool:
    try:
        session.execute(
            text("UPDATE income_events SET notified = true WHERE id = :id"),
            {"id": income_event_id},
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    return True
