"""Operation ingestion and income-event reconciliation use cases."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional

from sqlalchemy import func

from financetracker.common.time_utils import utc_to_local_date
from financetracker.database.models import IncomeEvent, Operation, PortfolioPosition, PortfolioSnapshot
from financetracker.tracker.asset_aliases import upsert_asset_alias
from financetracker.tracker.income_events import compute_income_net_amount, compute_income_net_yield_pct
from financetracker.tracker.payloads import json_value as get_json_value, money_to_float, quotation_to_float


def _parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def guess_deposit_source(description: Optional[str]) -> Optional[str]:
    """
    Очень грубая эвристика источника пополнения по description — на будущее.
    """
    if not description:
        return None
    desc = description.lower()
    if "перевод" in desc and "счет" in desc:
        return "transfer"
    if "перевод" in desc and "счёт" in desc:
        return "transfer"
    if "зарплат" in desc:
        return "salary"
    if "пополнени" in desc:
        return "topup"
    return None


def _upsert_operation(db, acc_id: str, op: dict) -> tuple[Optional[Operation], bool]:
    op_id = get_json_value(op, "id")
    if not op_id:
        return None, False

    existing = (
        db.query(Operation)
        .filter(
            Operation.account_id == acc_id,
            Operation.operation_id == op_id,
        )
        .one_or_none()
    )

    op_type = (
        get_json_value(op, "type")
        or get_json_value(op, "operation_type")
        or (existing.operation_type if existing is not None else None)
        or "OPERATION_TYPE_UNSPECIFIED"
    )
    payment = get_json_value(op, "payment")
    parsed_payment_value = money_to_float(payment)
    payment_value = (
        parsed_payment_value
        if parsed_payment_value is not None
        else (float(existing.amount) if existing is not None else 0.0)
    )
    payment_currency_raw = (payment or {}).get("currency")
    payment_currency = (
        str(payment_currency_raw).strip().upper()
        if payment_currency_raw
        else ((existing.currency or "UNKNOWN").upper() if existing is not None else "UNKNOWN")
    )

    op_dt_raw = _parse_iso_dt(get_json_value(op, "date"))
    if op_dt_raw is None:
        raise ValueError("Operation timestamp is missing or malformed")
    if op_dt_raw.tzinfo is None:
        op_dt = op_dt_raw.replace(tzinfo=timezone.utc)
    else:
        op_dt = op_dt_raw.astimezone(timezone.utc)

    cancel_dt = _parse_iso_dt(get_json_value(op, "cancel_date_time"))
    if cancel_dt and cancel_dt.tzinfo is None:
        cancel_dt = cancel_dt.replace(tzinfo=timezone.utc)
    elif cancel_dt:
        cancel_dt = cancel_dt.astimezone(timezone.utc)

    values = {
        "account_id": acc_id,
        "operation_id": op_id,
        "operation_type": op_type,
        "cursor": get_json_value(op, "cursor"),
        "broker_account_id": get_json_value(op, "broker_account_id"),
        "parent_operation_id": get_json_value(op, "parent_operation_id"),
        "name": get_json_value(op, "name"),
        "date": op_dt,
        "state": get_json_value(op, "state"),
        "description": get_json_value(op, "description") or get_json_value(op, "asset_uid") or "",
        "instrument_uid": get_json_value(op, "instrument_uid"),
        "figi": get_json_value(op, "figi") or (existing.figi if existing is not None else None),
        "instrument_type": get_json_value(op, "instrument_type"),
        "instrument_kind": get_json_value(op, "instrument_kind"),
        "position_uid": get_json_value(op, "position_uid"),
        "asset_uid": get_json_value(op, "asset_uid"),
        "amount": payment_value,
        "price": quotation_to_float(get_json_value(op, "price")),
        "commission": money_to_float(get_json_value(op, "commission")),
        "yield_amount": money_to_float(get_json_value(op, "yield")),
        "yield_relative": quotation_to_float(get_json_value(op, "yield_relative")),
        "accrued_int": money_to_float(get_json_value(op, "accrued_int")),
        "quantity": get_json_value(op, "quantity"),
        "quantity_rest": get_json_value(op, "quantity_rest"),
        "quantity_done": get_json_value(op, "quantity_done"),
        "currency": payment_currency,
        "cancel_date_time": cancel_dt,
        "cancel_reason": get_json_value(op, "cancel_reason"),
        "source": guess_deposit_source(get_json_value(op, "description")),
    }

    upsert_asset_alias(
        db,
        asset_uid=values["asset_uid"],
        instrument_uid=values["instrument_uid"],
        figi=values["figi"],
        name=values["name"],
        seen_at=values["date"],
    )

    if existing is None:
        operation = Operation(**values)
        db.add(operation)
        return operation, True

    for field, value in values.items():
        setattr(existing, field, value)
    return existing, False


def _sync_operations(
    db,
    account_id: str,
    from_date: Optional[str],
    *,
    operation_pages: Callable,
    local_tz,
    logger,
    affected_income_keys: Optional[set[tuple[str, date, str, str]]] = None,
) -> dict:
    """Синхронизирует операции счёта через GetOperationsByCursor и upsert в БД."""
    count_new = 0
    count_updated = 0
    loaded_total = 0

    for operations in operation_pages(account_id, from_date):
        loaded_total += len(operations)

        for op in operations:
            operation, created = _upsert_operation(db, account_id, op)
            if operation is None:
                continue
            if created:
                count_new += 1
            else:
                count_updated += 1
            if (
                affected_income_keys is not None
                and operation.figi
                and operation.operation_type in INCOME_OPERATION_TYPE_MAP
                and operation.state in {
                    EXECUTED_OPERATION_STATE,
                    CANCELED_OPERATION_STATE,
                }
            ):
                event_type, _ = INCOME_OPERATION_TYPE_MAP[operation.operation_type]
                affected_income_keys.add(
                    (
                        operation.figi,
                        utc_to_local_date(operation.date, local_tz),
                        event_type,
                        (operation.currency or "UNKNOWN").upper(),
                    )
                )

        logger.info(
            "operations_page_persisted",
            "Persisted operations page in the current transaction.",
            {
                "account_id": account_id,
                "page_items_count": len(operations),
                "loaded_total": loaded_total,
            },
        )

    return {"loaded": loaded_total, "new": count_new, "updated": count_updated}


INCOME_OPERATION_TYPE_MAP = {
    "OPERATION_TYPE_COUPON": ("coupon", "gross"),
    # COUPON_TAX сохраняется для совместимости с уже загруженными данными.
    "OPERATION_TYPE_COUPON_TAX": ("coupon", "tax"),
    "OPERATION_TYPE_BOND_TAX": ("coupon", "tax"),
    "OPERATION_TYPE_BOND_TAX_PROGRESSIVE": ("coupon", "tax"),
    "OPERATION_TYPE_DIVIDEND": ("dividend", "gross"),
    "OPERATION_TYPE_DIVIDEND_TAX": ("dividend", "tax"),
    "OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE": ("dividend", "tax"),
}
EXECUTED_OPERATION_STATE = "OPERATION_STATE_EXECUTED"
CANCELED_OPERATION_STATE = "OPERATION_STATE_CANCELED"


def _reconcile_income_events(
    db,
    account_id: str,
    affected_keys: set[tuple[str, date, str, str]],
    *,
    local_tz,
    get_cost_basis: Callable = None,
) -> dict:
    """
    Пересчитывает затронутые доходные события по полной локальной истории.

    Полная история ключа нужна для поздних налогов: узкое
    API-окно может содержать налог, но не исходную выплату.
    """
    get_cost_basis = get_cost_basis or get_latest_cost_basis
    if not affected_keys:
        return {"income_created": 0, "income_updated": 0, "income_deactivated": 0}

    income_by_key: dict[tuple[str, date, str, str], dict[str, float]] = {}

    rows = (
        db.query(Operation)
        .filter(
            Operation.account_id == account_id,
            Operation.state == EXECUTED_OPERATION_STATE,
            Operation.operation_type.in_(tuple(INCOME_OPERATION_TYPE_MAP)),
        )
        .all()
    )
    for row in rows:
        if not row.figi:
            continue
        event_type, amount_kind = INCOME_OPERATION_TYPE_MAP[row.operation_type]
        currency = (row.currency or "UNKNOWN").upper()
        key = (
            row.figi,
            utc_to_local_date(row.date, local_tz),
            event_type,
            currency,
        )
        if key not in affected_keys:
            continue
        if key not in income_by_key:
            income_by_key[key] = {"gross": 0.0, "tax": 0.0}
        income_by_key[key][amount_kind] += float(row.amount or 0)

    existing_events = (
        db.query(IncomeEvent)
        .filter(IncomeEvent.account_id == account_id)
        .all()
    )
    existing_by_key = {
        (row.figi, row.event_date, row.event_type, row.currency): row
        for row in existing_events
        if (row.figi, row.event_date, row.event_type, row.currency) in affected_keys
    }

    cost_basis_by_event: dict[tuple[str, date], Optional[float]] = {}
    created = 0
    updated = 0
    deactivated = 0

    for (figi, event_date, event_type, currency), amounts in income_by_key.items():
        gross_sum = amounts["gross"]
        tax_sum = amounts["tax"]
        net_amount = compute_income_net_amount(gross_sum, tax_sum)
        key = (figi, event_date, event_type, currency)
        existing = existing_by_key.get(key)
        if net_amount <= 0:
            if existing is not None:
                db.delete(existing)
                deactivated += 1
            continue

        expected_amounts = {
            "gross_amount": round(gross_sum, 2),
            "tax_amount": round(tax_sum, 2),
            "net_amount": round(net_amount, 2),
        }
        if existing is None:
            cost_key = (figi, event_date)
            if cost_key not in cost_basis_by_event:
                cost_basis_by_event[cost_key] = get_cost_basis(
                    db,
                    account_id,
                    figi,
                    as_of_date=event_date,
                )
            net_yield_pct = compute_income_net_yield_pct(
                net_amount,
                cost_basis_by_event[cost_key],
            )
            db.add(
                IncomeEvent(
                    account_id=account_id,
                    figi=figi,
                    event_date=event_date,
                    event_type=event_type,
                    currency=currency,
                    notified=False,
                    net_yield_pct=round(net_yield_pct, 4),
                    **expected_amounts,
                )
            )
            created += 1
            continue

        amounts_changed = any(
            round(float(getattr(existing, field)), 2) != value
            for field, value in expected_amounts.items()
        )
        if amounts_changed:
            cost_key = (figi, event_date)
            if cost_key not in cost_basis_by_event:
                cost_basis_by_event[cost_key] = get_cost_basis(
                    db,
                    account_id,
                    figi,
                    as_of_date=event_date,
                )
            net_yield_pct = compute_income_net_yield_pct(
                net_amount,
                cost_basis_by_event[cost_key],
            )
            for field, value in expected_amounts.items():
                setattr(existing, field, value)
            existing.net_yield_pct = round(net_yield_pct, 4)
            # Уже отправленное уведомление не повторяем; отчёты и dataset
            # сразу увидят исправленные суммы после commit.
            updated += 1

    for key in affected_keys - set(income_by_key):
        existing = existing_by_key.get(key)
        if existing is not None:
            db.delete(existing)
            deactivated += 1

    return {
        "income_created": created,
        "income_updated": updated,
        "income_deactivated": deactivated,
    }


def sync_operations_for_account(
    db,
    acc_data: dict,
    *,
    operation_pages: Callable,
    dt_to_iso_z: Callable[[datetime], str],
    local_tz,
    logger,
):
    """Тянем операции и сохраняем в operations (идемпотентно по operation_id)."""
    acc_id = str(acc_data.get("id"))
    opened_iso = acc_data.get("openedDate") or acc_data.get("opened_date")

    last_dt: Optional[datetime] = (
        db.query(func.max(Operation.date))
        .filter(Operation.account_id == acc_id)
        .scalar()
    )

    from_iso = opened_iso
    if last_dt is not None:
        from_iso = dt_to_iso_z(last_dt - timedelta(days=1))

    # После расширения схемы OperationItem может потребоваться дозаполнение
    # новых колонок у исторических строк. Если видим пустые новые поля,
    # делаем backfill с даты открытия счёта.
    needs_backfill = (
        db.query(Operation.id)
        .filter(
            Operation.account_id == acc_id,
            Operation.state.is_(None),
        )
        .first()
        is not None
    )
    if needs_backfill and opened_iso:
        from_iso = opened_iso
        logger.info(
            "operations_backfill_started",
            "Detected incomplete OperationItem fields; starting backfill from account open date.",
            {"account_id": acc_id, "from": from_iso},
        )

    affected_income_keys: set[tuple[str, date, str, str]] = set()
    stats = _sync_operations(
        db,
        acc_id,
        from_iso,
        operation_pages=operation_pages,
        local_tz=local_tz,
        logger=logger,
        affected_income_keys=affected_income_keys,
    )
    stats.update(
        _reconcile_income_events(
            db,
            acc_id,
            affected_income_keys,
            local_tz=local_tz,
        )
    )

    logger.info(
        "operations_sync_completed",
        "Operations sync completed.",
        {"account_id": acc_id, **stats},
    )

def get_latest_cost_basis(
    db,
    account_id: str,
    figi: str,
    *,
    as_of_date: Optional[date] = None,
) -> Optional[float]:
    query = (
        db.query(PortfolioPosition.position_value, PortfolioPosition.expected_yield)
        .join(PortfolioSnapshot, PortfolioSnapshot.id == PortfolioPosition.snapshot_id)
        .filter(
            PortfolioSnapshot.account_id == account_id,
            PortfolioPosition.figi == figi,
        )
    )
    if as_of_date is not None:
        query = query.filter(PortfolioSnapshot.snapshot_date <= as_of_date)
    row = (
        query
        .order_by(PortfolioSnapshot.snapshot_date.desc(), PortfolioSnapshot.snapshot_at.desc())
        .first()
    )
    if not row:
        return None
    position_value, expected_yield = row
    if position_value is None or expected_yield is None:
        return None
    return float(position_value) - float(expected_yield)
