"""Synchronize the declared payout calendar for an account's held instruments."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Callable, Optional

from financetracker.common.time_utils import utc_now
from financetracker.database.models import PayoutCalendarEvent, PortfolioPosition, PortfolioSnapshot
from financetracker.tracker.payloads import json_value as get_json_value, to_int


def _money_to_decimal(value: Optional[dict]) -> Optional[Decimal]:
    if not isinstance(value, dict):
        return None
    units = Decimal(to_int(value.get("units")))
    nano = Decimal(to_int(value.get("nano"))) / Decimal("1000000000")
    return units + nano


def _iso_to_local_date(value: Optional[str], *, local_tz) -> Optional[date]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(local_tz).date()


def _payout_event_uid(*parts: object) -> str:
    raw = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _normalize_payout_instrument_type(value: Optional[str]) -> str:
    normalized = (value or "").strip().lower()
    aliases = {
        "instrument_type_bond": "bond",
        "instrument_type_share": "share",
        "instrument_type_etf": "etf",
        "stock": "share",
    }
    return aliases.get(normalized, normalized)


def _to_iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _upsert_payout_calendar_event(
    db,
    *,
    account_id: str,
    position: PortfolioPosition,
    event_type: str,
    event_uid: str,
    payment_date: date,
    record_date: Optional[date],
    last_buy_date: Optional[date],
    coupon_start_date: Optional[date],
    coupon_end_date: Optional[date],
    coupon_period_days: Optional[int],
    amount_per_unit: Optional[Decimal],
    currency: Optional[str],
    source_event_type: Optional[str],
    fetched_at: datetime,
) -> None:
    quantity = Decimal(position.quantity or 0)
    expected_amount = None
    if amount_per_unit is not None:
        expected_amount = (amount_per_unit * quantity).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    row = (
        db.query(PayoutCalendarEvent)
        .filter(
            PayoutCalendarEvent.account_id == account_id,
            PayoutCalendarEvent.figi == position.figi,
            PayoutCalendarEvent.event_type == event_type,
            PayoutCalendarEvent.event_uid == event_uid,
        )
        .one_or_none()
    )
    if row is None:
        row = PayoutCalendarEvent(
            account_id=account_id,
            figi=position.figi,
            event_type=event_type,
            event_uid=event_uid,
            created_at=fetched_at,
        )
        db.add(row)

    instrument = position.instrument
    row.instrument_uid = position.instrument_uid
    row.ticker = position.ticker or (instrument.ticker if instrument is not None else None)
    row.name = position.name or (instrument.name if instrument is not None else None)
    row.instrument_type = position.instrument_type
    row.payment_date = payment_date
    row.record_date = record_date
    row.last_buy_date = last_buy_date
    row.coupon_start_date = coupon_start_date
    row.coupon_end_date = coupon_end_date
    row.coupon_period_days = coupon_period_days
    row.amount_per_unit = amount_per_unit
    row.quantity = quantity
    row.expected_amount = expected_amount
    row.currency = (currency or "").strip().upper() or "UNKNOWN"
    row.source_event_type = source_event_type
    row.fetched_at = fetched_at
    row.updated_at = fetched_at


def _replace_position_payout_events(
    db,
    *,
    account_id: str,
    position: PortfolioPosition,
    event_type: str,
    source_events: list[dict],
    period_start: date,
    period_end: date,
    fetched_at: datetime,
    local_tz,
) -> int:
    seen_uids: set[str] = set()
    stored = 0

    for event in source_events:
        if event_type == "coupon":
            payment_date = _iso_to_local_date(get_json_value(event, "coupon_date"), local_tz=local_tz)
            record_date = _iso_to_local_date(get_json_value(event, "fix_date"), local_tz=local_tz)
            last_buy_date = None
            coupon_start_date = _iso_to_local_date(get_json_value(event, "coupon_start_date"), local_tz=local_tz)
            coupon_end_date = _iso_to_local_date(get_json_value(event, "coupon_end_date"), local_tz=local_tz)
            raw_coupon_period = get_json_value(event, "coupon_period")
            try:
                coupon_period_days = int(raw_coupon_period) if raw_coupon_period is not None else None
            except (TypeError, ValueError):
                coupon_period_days = None
            money = get_json_value(event, "pay_one_bond")
            source_event_type = get_json_value(event, "coupon_type")
            event_uid = _payout_event_uid(
                event_type,
                get_json_value(event, "coupon_number"),
                get_json_value(event, "coupon_date"),
                get_json_value(event, "coupon_start_date"),
                get_json_value(event, "coupon_end_date"),
            )
        else:
            payment_date = _iso_to_local_date(get_json_value(event, "payment_date"), local_tz=local_tz)
            record_date = _iso_to_local_date(get_json_value(event, "record_date"), local_tz=local_tz)
            last_buy_date = _iso_to_local_date(get_json_value(event, "last_buy_date"), local_tz=local_tz)
            coupon_start_date = coupon_end_date = coupon_period_days = None
            money = get_json_value(event, "dividend_net")
            source_event_type = get_json_value(event, "dividend_type")
            if "cancel" in (source_event_type or "").lower():
                continue
            event_uid = _payout_event_uid(
                event_type,
                get_json_value(event, "record_date"),
                get_json_value(event, "payment_date"),
                get_json_value(event, "declared_date"),
                source_event_type,
            )

        if payment_date is None or not (period_start <= payment_date <= period_end):
            continue
        amount_per_unit = _money_to_decimal(money)
        if amount_per_unit is not None and amount_per_unit <= 0:
            amount_per_unit = None
        currency = get_json_value(money, "currency") if isinstance(money, dict) else None
        _upsert_payout_calendar_event(
            db,
            account_id=account_id,
            position=position,
            event_type=event_type,
            event_uid=event_uid,
            payment_date=payment_date,
            record_date=record_date,
            last_buy_date=last_buy_date,
            coupon_start_date=coupon_start_date,
            coupon_end_date=coupon_end_date,
            coupon_period_days=coupon_period_days,
            amount_per_unit=amount_per_unit,
            currency=currency,
            source_event_type=source_event_type,
            fetched_at=fetched_at,
        )
        seen_uids.add(event_uid)
        stored += 1

    stale_query = db.query(PayoutCalendarEvent).filter(
        PayoutCalendarEvent.account_id == account_id,
        PayoutCalendarEvent.figi == position.figi,
        PayoutCalendarEvent.event_type == event_type,
        PayoutCalendarEvent.payment_date >= period_start,
        PayoutCalendarEvent.payment_date <= period_end,
    )
    if seen_uids:
        stale_query = stale_query.filter(~PayoutCalendarEvent.event_uid.in_(seen_uids))
    stale_query.delete(synchronize_session=False)
    return stored


def sync_payout_calendar_for_account(
    db,
    account_id: str,
    *,
    api_get_bond_coupons: Callable,
    api_get_dividends: Callable,
    local_today: Callable[[], date],
    local_tz,
    horizon_days: int,
    dividend_record_lookback_days: int,
    logger,
) -> dict[str, int]:
    latest_snapshot = (
        db.query(PortfolioSnapshot)
        .filter(PortfolioSnapshot.account_id == account_id)
        .order_by(
            PortfolioSnapshot.snapshot_date.desc(),
            PortfolioSnapshot.snapshot_at.desc(),
            PortfolioSnapshot.id.desc(),
        )
        .first()
    )
    if latest_snapshot is None:
        logger.warning("payout_calendar_snapshot_missing", "Cannot sync payout calendar without a portfolio snapshot.", {"account_id": account_id})
        return {"positions": 0, "events": 0, "failed": 0}

    positions = [position for position in latest_snapshot.positions if position.figi and Decimal(position.quantity or 0) > 0]
    if not positions:
        logger.warning("payout_calendar_positions_missing", "Cannot sync payout calendar because the latest snapshot has no positions.", {"account_id": account_id, "snapshot_id": latest_snapshot.id})
        return {"positions": 0, "events": 0, "failed": 0}

    period_start = local_today()
    period_end = period_start + timedelta(days=horizon_days - 1)
    from_iso = _to_iso_z(datetime.combine(period_start, datetime.min.time(), tzinfo=local_tz))
    dividends_from_iso = _to_iso_z(datetime.combine(period_start - timedelta(days=dividend_record_lookback_days), datetime.min.time(), tzinfo=local_tz))
    to_iso = _to_iso_z(datetime.combine(period_end + timedelta(days=1), datetime.min.time(), tzinfo=local_tz))
    fetched_at = utc_now()
    held_figis = {position.figi for position in positions}
    events_stored = failed = supported_positions = 0

    for position in positions:
        instrument = position.instrument
        instrument_type = _normalize_payout_instrument_type(position.instrument_type or (instrument.instrument_type if instrument is not None else None))
        instrument_id = position.instrument_uid or position.figi
        if instrument_type == "bond":
            event_type, callback, callback_args = "coupon", api_get_bond_coupons, (instrument_id, from_iso, to_iso)
        elif instrument_type in {"share", "etf"}:
            event_type, callback, callback_args = "dividend", api_get_dividends, (instrument_id, dividends_from_iso, to_iso)
        else:
            continue
        supported_positions += 1
        try:
            source_events = callback(*callback_args)
            with db.begin_nested():
                stored = _replace_position_payout_events(
                    db, account_id=account_id, position=position, event_type=event_type,
                    source_events=source_events, period_start=period_start,
                    period_end=period_end, fetched_at=fetched_at, local_tz=local_tz,
                )
            events_stored += stored
        except Exception:
            failed += 1
            label = "bond coupon" if event_type == "coupon" else "dividend"
            logger.exception(
                "payout_calendar_instrument_sync_failed",
                f"Failed to sync {label} calendar; cached rows are preserved.",
                {"account_id": account_id, "figi": position.figi, "event_type": event_type},
            )

    db.query(PayoutCalendarEvent).filter(PayoutCalendarEvent.account_id == account_id, PayoutCalendarEvent.payment_date < period_start).delete(synchronize_session=False)
    db.query(PayoutCalendarEvent).filter(PayoutCalendarEvent.account_id == account_id, ~PayoutCalendarEvent.figi.in_(held_figis)).delete(synchronize_session=False)
    db.flush()
    return {"positions": supported_positions, "events": events_stored, "failed": failed}
