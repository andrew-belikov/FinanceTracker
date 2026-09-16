"""Read-only operation, income, tax, and cashflow queries for bot use cases."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal

from sqlalchemy import bindparam, text

from financetracker.bot.runtime import COMMISSION_OPERATION_TYPES, DEPOSIT_OPERATION_TYPES, EXECUTED_OPERATION_STATE, IIS_TAX_DEDUCTION_CATEGORY, TAX_OPERATION_TYPES, TZ, WITHDRAWAL_OPERATION_TYPES
from financetracker.common.time_utils import utc_to_local_date
from financetracker.database.operations import operations_dedup_statement


class CurrencyAggregationError(RuntimeError):
    """Raised when nominal values would otherwise be added across currencies."""


@contextmanager
def _required_relation_savepoint(session):
    with session.begin_nested():
        yield


def _selected_currency(currency: str | None) -> str | None:
    return (currency or "").strip().upper() or None


def _currency_filter() -> str:
    return """UPPER(COALESCE(currency, '')) = COALESCE(:currency, (
        SELECT UPPER(ps.currency) FROM portfolio_snapshots ps WHERE ps.account_id = :account_id
        ORDER BY ps.snapshot_date DESC, ps.snapshot_at DESC, ps.id DESC LIMIT 1))"""


def _with_currency_filter(sql: str) -> str:
    return sql.replace("{currency_filter}", _currency_filter())


def get_net_external_contributions(session, account_id: str, currency: str | None = None) -> float:
    sql = _with_currency_filter("""
        SELECT COALESCE(SUM(CASE WHEN operation_type IN :deposit_types
          AND COALESCE(cashflow_category, '') <> :deduction_category THEN ABS(amount)
          WHEN operation_type IN :withdrawal_types THEN -ABS(amount) ELSE 0 END), 0)
        FROM operations_dedup WHERE account_id = :account_id AND operation_type IN :operation_types
          AND state = :executed_state AND {currency_filter}
    """)
    row = session.execute(operations_dedup_statement(sql).bindparams(bindparam("deposit_types", expanding=True), bindparam("withdrawal_types", expanding=True), bindparam("operation_types", expanding=True)),
        {"account_id": account_id, "deposit_types": DEPOSIT_OPERATION_TYPES, "withdrawal_types": WITHDRAWAL_OPERATION_TYPES, "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES, "deduction_category": IIS_TAX_DEDUCTION_CATEGORY, "executed_state": EXECUTED_OPERATION_STATE, "currency": _selected_currency(currency)}).scalar_one()
    return float(row or 0)


def get_deposits_for_period(session, account_id: str, start_dt: datetime, end_dt: datetime, operation_types: tuple[str, ...] = DEPOSIT_OPERATION_TYPES, currency: str | None = None) -> float:
    sql = _with_currency_filter("""
        SELECT COALESCE(SUM(amount), 0) FROM operations_dedup
        WHERE account_id = :account_id AND date >= :start_dt AND date < :end_dt AND operation_type IN :operation_types
          AND COALESCE(cashflow_category, '') <> :deduction_category AND state = :executed_state AND {currency_filter}
    """)
    row = session.execute(operations_dedup_statement(sql).bindparams(bindparam("operation_types", expanding=True)),
        {"account_id": account_id, "start_dt": start_dt, "end_dt": end_dt, "operation_types": operation_types, "deduction_category": IIS_TAX_DEDUCTION_CATEGORY, "executed_state": EXECUTED_OPERATION_STATE, "currency": _selected_currency(currency)}).scalar_one()
    return float(row or 0)


def get_iis_tax_deductions_for_period(session, account_id: str, start_dt: datetime, end_dt: datetime, currency: str | None = None) -> Decimal:
    sql = _with_currency_filter("""
        SELECT COALESCE(SUM(ABS(amount)), 0) FROM operations_dedup
        WHERE account_id = :account_id AND date >= :start_dt AND date < :end_dt AND operation_type IN :deposit_types
          AND cashflow_category = :deduction_category AND state = :executed_state AND {currency_filter}
    """)
    row = session.execute(operations_dedup_statement(sql).bindparams(bindparam("deposit_types", expanding=True)),
        {"account_id": account_id, "start_dt": start_dt, "end_dt": end_dt, "deposit_types": DEPOSIT_OPERATION_TYPES, "deduction_category": IIS_TAX_DEDUCTION_CATEGORY, "executed_state": EXECUTED_OPERATION_STATE, "currency": _selected_currency(currency)}).scalar_one()
    return Decimal(row or 0)


def get_net_external_flow_for_period(session, account_id: str, start_dt: datetime, end_dt: datetime, currency: str | None = None) -> float:
    sql = _with_currency_filter("""
        SELECT COALESCE(SUM(CASE WHEN operation_type IN :deposit_types
          AND COALESCE(cashflow_category, '') <> :deduction_category THEN ABS(amount)
          WHEN operation_type IN :withdrawal_types THEN -ABS(amount) ELSE 0 END), 0)
        FROM operations_dedup WHERE account_id = :account_id AND date >= :start_dt AND date < :end_dt
          AND operation_type IN :operation_types AND state = :executed_state AND {currency_filter}
    """)
    row = session.execute(operations_dedup_statement(sql).bindparams(bindparam("deposit_types", expanding=True), bindparam("withdrawal_types", expanding=True), bindparam("operation_types", expanding=True)),
        {"account_id": account_id, "start_dt": start_dt, "end_dt": end_dt, "deposit_types": DEPOSIT_OPERATION_TYPES, "withdrawal_types": WITHDRAWAL_OPERATION_TYPES, "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES, "deduction_category": IIS_TAX_DEDUCTION_CATEGORY, "executed_state": EXECUTED_OPERATION_STATE, "currency": _selected_currency(currency)}).scalar_one()
    return float(row or 0)


def get_income_for_period(db, account_id: str, start_date, end_date, currency: str | None = None) -> tuple[Decimal, Decimal]:
    with _required_relation_savepoint(db):
        sql = _with_currency_filter("""
            SELECT COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN') AS currency,
              COALESCE(SUM(CASE WHEN event_type = 'coupon' THEN net_amount ELSE 0 END), 0) AS coupons,
              COALESCE(SUM(CASE WHEN event_type = 'dividend' THEN net_amount ELSE 0 END), 0) AS dividends
            FROM income_events WHERE account_id = :account_id AND event_date >= :start_date AND event_date <= :end_date
              AND {currency_filter} GROUP BY COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN')
        """)
        rows = db.execute(text(sql), {"account_id": account_id, "start_date": start_date, "end_date": end_date, "currency": _selected_currency(currency)}).mappings().all()
    if not rows:
        return Decimal("0"), Decimal("0")
    if len(rows) != 1 or rows[0]["currency"] == "UNKNOWN":
        raise CurrencyAggregationError("Income currencies require an explicit selection; implicit FX is disabled")
    return Decimal(rows[0]["coupons"] or 0), Decimal(rows[0]["dividends"] or 0)


def get_income_currency_breakdown_for_period(db, account_id: str, start_date, end_date) -> list[dict[str, Decimal | str]]:
    local_start = utc_to_local_date(start_date, TZ) if isinstance(start_date, datetime) else start_date
    local_end = utc_to_local_date(end_date, TZ) if isinstance(end_date, datetime) else end_date
    totals: dict[str, dict[str, Decimal | str]] = {}
    with _required_relation_savepoint(db):
        income_rows = db.execute(text("""SELECT COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN') AS currency,
            COALESCE(SUM(CASE WHEN event_type = 'coupon' THEN net_amount ELSE 0 END), 0) AS coupons,
            COALESCE(SUM(CASE WHEN event_type = 'dividend' THEN net_amount ELSE 0 END), 0) AS dividends,
            COALESCE(SUM(CASE WHEN tax_amount < 0 THEN ABS(tax_amount) ELSE 0 END), 0) AS taxes,
            COALESCE(SUM(CASE WHEN tax_amount > 0 THEN tax_amount ELSE 0 END), 0) AS tax_refunds
            FROM income_events WHERE account_id = :account_id AND event_date >= :start_date AND event_date <= :end_date
            GROUP BY COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN')"""), {"account_id": account_id, "start_date": local_start, "end_date": local_end}).mappings().all()
    for row in income_rows:
        key = str(row["currency"] or "UNKNOWN").upper()
        totals[key] = {"currency": key, "coupons": Decimal(row["coupons"] or 0), "dividends": Decimal(row["dividends"] or 0), "taxes": Decimal(row["taxes"] or 0), "tax_refunds": Decimal(row["tax_refunds"] or 0)}
    operation_rows = db.execute(operations_dedup_statement("""SELECT COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN') AS currency,
        COALESCE(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 0) AS taxes,
        COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS tax_refunds
        FROM operations_dedup WHERE account_id = :account_id AND date >= :start_date AND date <= :end_date
          AND operation_type IN :operation_types AND state = :executed_state GROUP BY COALESCE(NULLIF(UPPER(currency), ''), 'UNKNOWN')""").bindparams(bindparam("operation_types", expanding=True)), {"account_id": account_id, "start_date": start_date, "end_date": end_date, "operation_types": TAX_OPERATION_TYPES, "executed_state": EXECUTED_OPERATION_STATE}).mappings().all()
    for row in operation_rows:
        key = str(row["currency"] or "UNKNOWN").upper()
        values = totals.setdefault(key, {"currency": key, "coupons": Decimal("0"), "dividends": Decimal("0"), "taxes": Decimal("0"), "tax_refunds": Decimal("0")})
        values["taxes"] = Decimal(values["taxes"]) + Decimal(row["taxes"] or 0)
        values["tax_refunds"] = Decimal(values["tax_refunds"]) + Decimal(row["tax_refunds"] or 0)
    return [totals[key] for key in sorted(totals)]


def _tax_total(db, account_id: str, start_date, end_date, currency: str | None, *, refunds: bool, income: bool) -> Decimal:
    source, amount, date_field = ("income_events", "tax_amount", "event_date") if income else ("operations_dedup", "amount", "date")
    operation_filter = "" if income else " AND operation_type IN :operation_types AND state = :executed_state"
    comparator = ">" if refunds else "<"
    expression = amount if refunds else f"ABS({amount})"
    statement = _with_currency_filter("""SELECT COALESCE(SUM(CASE WHEN {amount} {comparator} 0 THEN {expression} ELSE 0 END), 0)
        FROM {source} WHERE account_id = :account_id AND {date_field} >= :start_date AND {date_field} <= :end_date{operation_filter}
        AND {currency_filter}""").format(
        amount=amount, comparator=comparator, expression=expression,
        source=source, date_field=date_field, operation_filter=operation_filter,
    )
    params = {"account_id": account_id, "start_date": start_date, "end_date": end_date, "currency": _selected_currency(currency)}
    if income:
        with _required_relation_savepoint(db):
            result = db.execute(text(statement), params).scalar_one()
    else:
        params.update(operation_types=TAX_OPERATION_TYPES, executed_state=EXECUTED_OPERATION_STATE)
        result = db.execute(operations_dedup_statement(statement).bindparams(bindparam("operation_types", expanding=True)), params).scalar_one()
    value = Decimal(result or 0)
    return max(value, Decimal("0")) if refunds else abs(value)


def get_commissions_for_period(db, account_id: str, start_date, end_date, currency: str | None = None) -> Decimal:
    sql = _with_currency_filter("""SELECT COALESCE(SUM(amount), 0) FROM operations_dedup
        WHERE account_id = :account_id AND date >= :start_date AND date <= :end_date AND operation_type IN :operation_types
          AND state = :executed_state AND {currency_filter}""")
    row = db.execute(operations_dedup_statement(sql).bindparams(bindparam("operation_types", expanding=True)), {"account_id": account_id, "start_date": start_date, "end_date": end_date, "operation_types": COMMISSION_OPERATION_TYPES, "executed_state": EXECUTED_OPERATION_STATE, "currency": _selected_currency(currency)}).scalar_one()
    return abs(Decimal(row or 0))


def get_taxes_for_period(db, account_id: str, start_date, end_date, currency: str | None = None) -> Decimal:
    return _tax_total(db, account_id, start_date, end_date, currency, refunds=False, income=True) + _tax_total(db, account_id, start_date, end_date, currency, refunds=False, income=False)


def get_tax_refunds_for_period(db, account_id: str, start_date, end_date, currency: str | None = None) -> Decimal:
    return _tax_total(db, account_id, start_date, end_date, currency, refunds=True, income=True) + _tax_total(db, account_id, start_date, end_date, currency, refunds=True, income=False)


def get_external_cashflows_raw(session, account_id: str, currency: str | None = None):
    sql = _with_currency_filter("""SELECT date, amount, currency, operation_type, cashflow_category
        FROM operations_dedup WHERE account_id = :account_id AND operation_type IN :operation_types AND state = :executed_state
          AND {currency_filter} ORDER BY date ASC""")
    return session.execute(operations_dedup_statement(sql).bindparams(bindparam("operation_types", expanding=True)), {"account_id": account_id, "operation_types": DEPOSIT_OPERATION_TYPES + WITHDRAWAL_OPERATION_TYPES, "executed_state": EXECUTED_OPERATION_STATE, "currency": _selected_currency(currency)}).mappings().all()
