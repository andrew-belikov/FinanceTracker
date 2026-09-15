"""Read model for the payout calendar shown by Telegram commands."""

from __future__ import annotations

from datetime import date

from sqlalchemy import text


def get_payout_calendar_events(
    session,
    account_id: str,
    start_date: date,
    end_date: date,
) -> list[dict]:
    return list(
        session.execute(
            text(
                """
                SELECT
                    pce.id, pce.figi,
                    COALESCE(NULLIF(pce.ticker, ''), NULLIF(pce.name, ''), pce.figi)
                        AS instrument_name,
                    pce.event_type, pce.payment_date, pce.record_date, pce.last_buy_date,
                    pce.coupon_start_date, pce.coupon_end_date, pce.coupon_period_days,
                    pce.amount_per_unit, pce.quantity, pce.expected_amount,
                    (
                        SELECT previous.amount_per_unit
                        FROM payout_calendar_events previous
                        WHERE previous.account_id = pce.account_id
                          AND previous.figi = pce.figi
                          AND previous.event_type = 'coupon'
                          AND previous.payment_date < pce.payment_date
                          AND previous.amount_per_unit > 0
                        ORDER BY previous.payment_date DESC, previous.id DESC
                        LIMIT 1
                    ) AS previous_coupon_amount_per_unit,
                    pce.currency, pce.source_event_type, pce.fetched_at,
                    (
                        SELECT pp.position_value - pp.expected_yield
                        FROM portfolio_positions pp
                        JOIN portfolio_snapshots ps ON ps.id = pp.snapshot_id
                        WHERE ps.account_id = pce.account_id AND pp.figi = pce.figi
                        ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC
                        LIMIT 1
                    ) AS cost_basis
                FROM payout_calendar_events pce
                WHERE pce.account_id = :account_id
                  AND pce.payment_date >= :start_date
                  AND pce.payment_date <= :end_date
                ORDER BY
                    pce.payment_date ASC,
                    CASE pce.event_type WHEN 'coupon' THEN 0 ELSE 1 END,
                    COALESCE(NULLIF(pce.ticker, ''), NULLIF(pce.name, ''), pce.figi) ASC,
                    pce.id ASC
                """
            ),
            {"account_id": account_id, "start_date": start_date, "end_date": end_date},
        )
        .mappings()
        .all()
    )
