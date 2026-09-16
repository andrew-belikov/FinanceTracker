"""Portfolio snapshot persistence use case."""

from __future__ import annotations

import json
from typing import Callable, Optional

from financetracker.database.models import Instrument, PortfolioPosition, PortfolioSnapshot
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

    # обновляем агрегаты
    snap.total_value = total_value
    snap.total_shares = total_shares
    snap.total_bonds = total_bonds
    snap.total_etf = total_etf
    snap.total_currencies = total_currencies
    snap.total_futures = total_futures
    snap.expected_yield = expected_yield
    snap.expected_yield_pct = expected_yield_pct

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

        position = PortfolioPosition(
            snapshot_id=snap.id,
            instrument_id=inst.id,
            figi=figi,
            instrument_uid=instrument_uid,
            position_uid=position_uid,
            asset_uid=asset_uid,
            ticker=inst.ticker,
            name=inst.name,
            instrument_type=pos.get("instrumentType"),
            quantity=quantity,
            currency=portfolio_currency.upper(),
            current_price=current_price,
            current_nkd=current_nkd,
            position_value=position_value,
            expected_yield=expected_yield_pos,
            expected_yield_pct=expected_yield_pct_pos,
            weight_pct=weight_pct,
            raw_payload_json=json.dumps(pos, ensure_ascii=False, sort_keys=True),
        )
        db.add(position)

    db.flush()

    # Структурированное сообщение о сохранении снапшота
    logger.info(
        "snapshot_saved",
        "Portfolio snapshot saved.",
        {
            "account_id": acc_id,
            "account_name": acc_name,
            "positions": len(positions),
        },
    )
