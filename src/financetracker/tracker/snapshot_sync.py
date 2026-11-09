"""Portfolio snapshot persistence use case."""

from __future__ import annotations

import json
from datetime import datetime, time
from typing import Callable, Optional

from financetracker.database.intraday import purge_delivered_intraday_days_before
from financetracker.database.models import (
    Instrument,
    IntradayPortfolioPosition,
    IntradayPortfolioSnapshot,
    PortfolioPosition,
    PortfolioSnapshot,
)
from financetracker.tracker.asset_aliases import resolve_asset_uid_for_position
from financetracker.tracker.payloads import json_value as get_json_value, money_to_float, quotation_to_float

def ensure_instrument(db, figi: str, instr_data: Optional[dict]) -> Instrument:
    inst: Optional[Instrument] = (
        db.query(Instrument).filter(Instrument.figi == figi).one_or_none()
    )
    if inst is None:
        inst = Instrument(figi=figi)
        db.add(inst)

    if instr_data:
        inst.ticker = instr_data.get("ticker") or inst.ticker
        inst.name = instr_data.get("name") or inst.name
        inst.class_code = instr_data.get("classCode") or inst.class_code
        inst.instrument_type = instr_data.get("instrumentType") or inst.instrument_type

    db.flush()
    return inst


def compute_expected_yield_pct(
    expected_yield: Optional[float],
    position_value: Optional[float],
) -> Optional[float]:
    if expected_yield is None or position_value is None:
        return None
    invested = position_value - expected_yield
    if invested == 0:
        return None
    return expected_yield / invested * 100.0


def is_intraday_capture_window(
    observed_at: datetime,
    *,
    local_tz,
    cutoff_hour: int,
    cutoff_minute: int,
) -> bool:
    """Whether an observation belongs to the weekday intraday digest window."""
    local_observed_at = observed_at.astimezone(local_tz)
    return (
        local_observed_at.weekday() < 5
        and local_observed_at.timetz().replace(tzinfo=None)
        < time(hour=cutoff_hour, minute=cutoff_minute)
    )



def take_snapshot_for_account(
    db,
    acc_data: dict,
    *,
    api_get_portfolio: Callable,
    api_get_instrument_by_figi: Callable,
    utc_now: Callable,
    local_today: Callable,
    portfolio_currency: str,
    logger,
    should_capture_intraday: Callable[[datetime], bool],
):
    """
    Делаем/перезаписываем снапшот за текущий день для одного счёта.
    """
    acc_id = str(acc_data.get("id"))
    acc_name = acc_data.get("name") or "IIS"

    portfolio = api_get_portfolio(acc_id)

    snapshot_at_utc = utc_now()
    snap_date = local_today()

    total_value = money_to_float(portfolio.get("totalAmountPortfolio"))
    total_shares = money_to_float(portfolio.get("totalAmountShares"))
    total_bonds = money_to_float(portfolio.get("totalAmountBonds"))
    total_etf = money_to_float(portfolio.get("totalAmountEtf"))
    total_currencies = money_to_float(portfolio.get("totalAmountCurrencies"))
    total_futures = money_to_float(portfolio.get("totalAmountFutures"))
    expected_yield = money_to_float(portfolio.get("expectedYield"))

    expected_yield_pct = None
    if total_value is not None and expected_yield is not None:
        invested_portfolio = total_value - expected_yield
        if invested_portfolio != 0:
            expected_yield_pct = expected_yield / invested_portfolio * 100.0

    # Ищем снапшот за этот день по этому счёту
    snap: Optional[PortfolioSnapshot] = (
        db.query(PortfolioSnapshot)
        .filter(
            PortfolioSnapshot.account_id == acc_id,
            PortfolioSnapshot.snapshot_date == snap_date,
        )
        .one_or_none()
    )
    if snap is None:
        snap = PortfolioSnapshot(
            account_id=acc_id,
            account_name=acc_name,
            snapshot_at=snapshot_at_utc,
            snapshot_date=snap_date,
            currency=portfolio_currency.upper(),
        )
        db.add(snap)
        db.flush()
    else:
        # перезаписываем снапшот текущего дня
        snap.account_name = acc_name
        snap.snapshot_at = snapshot_at_utc
        snap.currency = portfolio_currency.upper()
        # удаляем старые позиции
        db.query(PortfolioPosition).filter(
            PortfolioPosition.snapshot_id == snap.id
        ).delete()
        db.flush()

    aggregates = {
        "total_value": total_value,
        "total_shares": total_shares,
        "total_bonds": total_bonds,
        "total_etf": total_etf,
        "total_currencies": total_currencies,
        "total_futures": total_futures,
        "expected_yield": expected_yield,
        "expected_yield_pct": expected_yield_pct,
    }

    # The canonical snapshot remains one row per account/day. Intraday rows are
    # a separate append-only observation stream used by the evening summary.
    for field, value in aggregates.items():
        setattr(snap, field, value)

    capture_intraday = should_capture_intraday(snapshot_at_utc)
    intraday_snap = None
    if capture_intraday:
        has_valued_observation = db.query(IntradayPortfolioSnapshot.id).filter(
            IntradayPortfolioSnapshot.account_id == acc_id,
            IntradayPortfolioSnapshot.snapshot_date == snap_date,
            IntradayPortfolioSnapshot.total_value.isnot(None),
        ).first()
        if total_value is not None and has_valued_observation is None:
            purge_delivered_intraday_days_before(db, acc_id, snap_date)
        intraday_snap = IntradayPortfolioSnapshot(
            account_id=acc_id,
            account_name=acc_name,
            snapshot_at=snapshot_at_utc,
            snapshot_date=snap_date,
            currency=portfolio_currency.upper(),
            **aggregates,
        )
        db.add(intraday_snap)

    db.flush()

    positions = portfolio.get("positions") or []
    figi_cache: dict[str, Optional[dict]] = {}

    for pos in positions:
        figi = pos.get("figi")
        if not figi:
            continue

        # Дёргаем API за метаданными инструмента только если в БД ещё нет ticker/name.
        inst_db = db.query(Instrument).filter(Instrument.figi == figi).one_or_none()
        need_fetch = inst_db is None or not (inst_db.ticker and inst_db.name)

        instr_data = None
        if need_fetch:
            if figi not in figi_cache:
                figi_cache[figi] = api_get_instrument_by_figi(figi)
            instr_data = figi_cache[figi]

        inst = ensure_instrument(db, figi, instr_data)

        quantity = quotation_to_float(pos.get("quantity"))
        current_price = money_to_float(pos.get("currentPrice"))
        current_nkd = money_to_float(get_json_value(pos, "current_nkd"))
        position_value = None
        if quantity is not None and current_price is not None:
            position_value = quantity * current_price

        expected_yield_pos = money_to_float(pos.get("expectedYield"))
        expected_yield_pct_pos = compute_expected_yield_pct(
            expected_yield_pos,
            position_value,
        )

        weight_pct = None
        if position_value is not None and total_value not in (None, 0):
            weight_pct = position_value / total_value * 100.0

        instrument_uid = get_json_value(pos, "instrument_uid")
        position_uid = get_json_value(pos, "position_uid")
        asset_uid = resolve_asset_uid_for_position(
            db,
            asset_uid=get_json_value(pos, "asset_uid"),
            instrument_uid=instrument_uid,
            figi=figi,
        )

        position_fields = {
            "instrument_id": inst.id,
            "figi": figi,
            "instrument_uid": instrument_uid,
            "position_uid": position_uid,
            "asset_uid": asset_uid,
            "ticker": inst.ticker,
            "name": inst.name,
            "instrument_type": pos.get("instrumentType"),
            "quantity": quantity,
            "currency": portfolio_currency.upper(),
            "current_price": current_price,
            "current_nkd": current_nkd,
            "position_value": position_value,
            "expected_yield": expected_yield_pos,
            "expected_yield_pct": expected_yield_pct_pos,
            "weight_pct": weight_pct,
            "raw_payload_json": json.dumps(pos, ensure_ascii=False, sort_keys=True),
        }
        db.add(PortfolioPosition(snapshot_id=snap.id, **position_fields))
        if intraday_snap is not None:
            db.add(
                IntradayPortfolioPosition(
                    snapshot_id=intraday_snap.id,
                    **position_fields,
                )
            )

    db.flush()

    # Структурированное сообщение о сохранении снапшота
    logger.info(
        "snapshot_saved",
        "Portfolio snapshot saved.",
        {
            "account_id": acc_id,
            "account_name": acc_name,
            "positions": len(positions),
            "intraday_snapshot_id": intraday_snap.id if intraday_snap else None,
            "intraday_captured": capture_intraday,
        },
    )
