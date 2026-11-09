"""Reconcile observed portfolio changes without inventing pending assets."""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


ZERO = Decimal("0")


def decimal(value):
    return Decimal(str(value)) if value is not None else ZERO


def pair_positions(first, last):
    """Pair complete endpoint holdings; absent endpoints remain unknown."""
    def identity(row):
        return row.get("asset_uid") or row.get("instrument_uid") or row["figi"]

    before = {identity(row): row for row in first}
    after = {identity(row): row for row in last}
    result = []
    for key in sorted(before.keys() | after.keys()):
        left, right = before.get(key, {}), after.get(key, {})
        metadata = right or left
        result.append({
            "identity": key, "figi": metadata["figi"],
            "ticker": metadata.get("ticker"), "name": metadata.get("name"),
            "currency": metadata.get("currency"), "instrument_type": metadata.get("instrument_type"),
            "first_value": left.get("position_value"), "last_value": right.get("position_value"),
            "first_quantity": left.get("quantity"), "last_quantity": right.get("quantity"),
            "first_nkd": left.get("current_nkd"), "last_nkd": right.get("current_nkd"),
        })
    return result


def position_label(row):
    name, ticker = str(row.get("name") or "").strip(), str(row.get("ticker") or "").strip()
    return f"{name} ({ticker})" if name and ticker and name != ticker else name or ticker or row.get("instrument_uid") or row.get("figi") or "Без названия"


def _trade_adjusted_position(row, trades):
    """Return clean-price and accrued-income changes only for reconciled trades."""
    first, last = row.get("first_value"), row.get("last_value")
    first_q, last_q = row.get("first_quantity"), row.get("last_quantity")
    quantity_delta = ZERO
    clean_cash = accrued_cash = ZERO
    bond = row.get("instrument_type") == "bond" or any(row.get(key) is not None for key in ("first_nkd", "last_nkd"))
    for trade in trades:
        quantity, price = trade.get("quantity_delta"), trade.get("unit_price")
        if quantity is None or not decimal(quantity) or price is None or decimal(price) < 0:
            return None
        quantity, price, amount = decimal(quantity), decimal(price), decimal(trade["amount"])
        if amount * quantity > 0:
            return None
        quantity_delta += quantity
        bond = bond or trade.get("instrument_type") == "bond"
        nkd_cash = -abs(decimal(trade.get("accrued_int"))) if quantity > 0 else abs(decimal(trade.get("accrued_int")))
        clean = amount - nkd_cash
        expected = (-quantity * price).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if abs(clean - expected) > Decimal("0.01"):
            return None
        clean_cash += clean
        accrued_cash += nkd_cash
    if first is None and first_q is None and last_q is not None and decimal(last_q) == quantity_delta:
        first, first_q = ZERO, ZERO
    if last is None and last_q is None and first_q is not None and decimal(first_q) == -quantity_delta:
        last, last_q = ZERO, ZERO
    if (first is None or last is None or first_q is None or last_q is None
            or decimal(first_q) < 0 or decimal(last_q) < 0
            or decimal(last_q) - decimal(first_q) != quantity_delta):
        return None
    accrued = ZERO
    if bond:
        if ((decimal(first_q) and row.get("first_nkd") is None)
                or (decimal(last_q) and row.get("last_nkd") is None)):
            return None
        accrued = (decimal(row.get("last_nkd")) * decimal(last_q)
                   - decimal(row.get("first_nkd")) * decimal(first_q) + accrued_cash)
    return decimal(last) - decimal(first) + clean_cash, accrued


def _positions_with_round_trips(positions, trades):
    rows = list(positions)
    known = {row.get("figi") for row in rows}
    for figi, executions in trades.items():
        # A fully opened and closed intraday holding has no endpoint position.
        if figi and figi not in known and all(row.get("quantity_delta") is not None for row in executions):
            if sum((decimal(row["quantity_delta"]) for row in executions), ZERO) == 0:
                metadata = executions[0]
                rows.append(dict(figi=figi, name=metadata.get("name"), currency=metadata.get("currency"),
                                 instrument_type=metadata.get("instrument_type"),
                                 first_value=ZERO, last_value=ZERO, first_quantity=ZERO, last_quantity=ZERO))
    return rows


def calculate_daily_change(*, opening, closing, positions, cash_events=(), currency="RUB"):
    """Reconcile price P&L and accrued income, including executed purchases/sales.

    Trade quantities must explain the endpoint holdings; cash amounts are checked
    against unit prices, with bond NKD separated. Unknown facts stay in residual.
    Coupon receipts are gross; actual tax and fee operations appear only once.
    """
    events = list(cash_events)
    external = sum((decimal(row["amount"]) for row in events if row["kind"] in {"deposit", "withdrawal"}), ZERO)
    result = decimal(closing) - decimal(opening) - external
    components = {key: ZERO for key in ("prices", "accrual", "coupon_reset", "coupon", "dividend", "deduction", "commission", "tax", "tax_refund")}
    trades = {}
    for event in events:
        if event["kind"] == "trade":
            trades.setdefault(event.get("figi"), []).append(event)
    movers, resets = [], []
    incomplete = False
    for row in _positions_with_round_trips(positions, trades):
        first, last = row.get("first_value"), row.get("last_value")
        first_q, last_q = row.get("first_quantity"), row.get("last_quantity")
        if row.get("currency") and row["currency"].upper() != currency.upper():
            incomplete = True
            continue
        if row.get("instrument_type") == "currency":
            continue
        executions = trades.get(row.get("figi"), [])
        accrued = ZERO
        if executions:
            adjusted = _trade_adjusted_position(row, executions)
            if adjusted is None:
                incomplete = True
                continue
            change, accrued = adjusted
        else:
            if first is None or last is None or first_q != last_q:
                incomplete = True
                continue
            change = decimal(last) - decimal(first)
            if row.get("first_nkd") is not None and row.get("last_nkd") is not None and first_q is not None:
                accrued = (decimal(row["last_nkd"]) - decimal(row["first_nkd"])) * decimal(first_q)
        components["prices"] += change
        if change:
            movers.append({"label": position_label(row), "contribution": change})
        if accrued < 0:
            components["coupon_reset"] += accrued
            resets.append(dict(row, removed=-accrued))
        else:
            components["accrual"] += accrued
    for event in events:
        if event["kind"] in components:
            components[event["kind"]] += decimal(event["amount"])
    result = result.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    components = {key: value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) for key, value in components.items()}
    components["other"] = result - sum(components.values(), ZERO)
    return {
        "change": result, "change_pct": result / decimal(opening) * 100 if opening else None,
        "external_flow": external, "components": components, "resets": resets,
        "movers": movers, "incomplete_attribution": incomplete,
    }
