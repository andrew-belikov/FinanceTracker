"""Pure portfolio allocation and rebalancing rules."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_HALF_UP
from typing import Any


REBALANCE_ASSET_CLASSES: tuple[str, ...] = ("stocks", "bonds", "etf", "currency")
REBALANCE_CLASS_LABELS = {
    "stocks": "Акции",
    "bonds": "Облигации",
    "etf": "ETF",
    "currency": "Валюта",
}
REBALANCE_TOLERANCE_PCT = Decimal("5.0")
REBALANCE_TARGET_ALIASES = {
    "stocks": "stocks",
    "bonds": "bonds",
    "etf": "etf",
    "cash": "currency",
    "currency": "currency",
}
_GROUP_TO_CLASS = {
    "Акции": "stocks",
    "Облигации": "bonds",
    "ETF": "etf",
    "Валюта": "currency",
}


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def instrument_type_to_group(instrument_type: str | None) -> str:
    lowered = (instrument_type or "").lower()
    if "share" in lowered or "stock" in lowered:
        return "Акции"
    if "bond" in lowered:
        return "Облигации"
    if "etf" in lowered or "fund" in lowered:
        return "ETF"
    if "currency" in lowered:
        return "Валюта"
    if "futures" in lowered or "future" in lowered:
        return "Фьючерсы"
    return "Другое"


def quantize_ruble_amount(value: Decimal | float | int | None) -> Decimal:
    return _decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def parse_decimal_input(raw_value: str, *, allow_zero: bool = True) -> Decimal:
    cleaned = raw_value.strip().replace(" ", "").replace(",", ".")
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    if cleaned.endswith("₽"):
        cleaned = cleaned[:-1]
    if not cleaned:
        raise ValueError("Пустое значение недопустимо.")
    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"Не удалось разобрать число: {raw_value}") from exc
    if not allow_zero and value <= 0:
        raise ValueError("Сумма должна быть положительной.")
    return value


def parse_rebalance_targets_args(
    args: list[str], *, usage_text: str = "Укажите таргеты в формате: stocks=50 bonds=30 etf=10 cash=10"
) -> dict[str, Decimal]:
    if not args:
        raise ValueError(usage_text)

    targets: dict[str, Decimal] = {}
    for token in args:
        if "=" not in token:
            raise ValueError(usage_text)
        raw_key, raw_value = token.split("=", 1)
        asset_class = REBALANCE_TARGET_ALIASES.get(raw_key.strip().lower())
        if asset_class is None:
            raise ValueError(
                f"Неизвестный класс `{raw_key}`. Поддерживаются: stocks, bonds, etf, cash."
            )
        if asset_class in targets:
            raise ValueError(f"Класс `{asset_class}` указан несколько раз.")
        value = parse_decimal_input(raw_value)
        if value < 0:
            raise ValueError("Таргеты не могут быть отрицательными.")
        targets[asset_class] = value

    normalized = {
        asset_class: targets.get(asset_class, Decimal("0"))
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    if sum(normalized.values()) != Decimal("100"):
        raise ValueError("Сумма таргетов должна быть ровно 100.")
    return normalized


def aggregate_rebalance_values_by_class(
    positions: list[dict[str, Any]] | None,
) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
    class_values = {asset_class: Decimal("0") for asset_class in REBALANCE_ASSET_CLASSES}
    other_groups: dict[str, Decimal] = {}
    for position in positions or []:
        group_name = instrument_type_to_group(position.get("instrument_type"))
        position_value = _decimal(position.get("position_value"))
        asset_class = _GROUP_TO_CLASS.get(group_name)
        if asset_class is not None:
            class_values[asset_class] += position_value
        else:
            other_groups[group_name] = other_groups.get(group_name, Decimal("0")) + position_value
    return class_values, other_groups


def compute_rebalance_plan(
    class_values: dict[str, Decimal],
    target_weights: dict[str, Decimal],
) -> dict[str, Decimal | list[dict[str, Decimal | str]]]:
    """Build deterministic allocation deltas without persistence or presentation."""
    rebalanceable_base = sum(
        _decimal(class_values.get(asset_class))
        for asset_class in REBALANCE_ASSET_CLASSES
    )
    rows: list[dict[str, Decimal | str]] = []
    for asset_class in REBALANCE_ASSET_CLASSES:
        current_value = _decimal(class_values.get(asset_class))
        target_pct = _decimal(target_weights.get(asset_class))
        current_pct = (
            current_value * Decimal("100") / rebalanceable_base
            if rebalanceable_base > 0
            else Decimal("0")
        )
        delta_pct = current_pct - target_pct
        target_value = rebalanceable_base * target_pct / Decimal("100")
        rows.append(
            {
                "asset_class": asset_class,
                "label": REBALANCE_CLASS_LABELS[asset_class],
                "current_value": current_value,
                "current_pct": current_pct,
                "target_pct": target_pct,
                "delta_pct": delta_pct,
                "target_value": target_value,
                "delta_value": target_value - current_value,
                "status": "в норме" if abs(delta_pct) <= REBALANCE_TOLERANCE_PCT else "вне нормы",
            }
        )
    return {"rebalanceable_base": rebalanceable_base, "rows": rows}


def compute_invest_plan(
    class_values: dict[str, Decimal],
    target_weights: dict[str, Decimal],
    deposit_amount: Decimal | float | int,
) -> dict[str, Decimal | dict[str, Decimal]]:
    """Allocate a ruble-denominated contribution toward target weights.

    The result is deterministic, integral in rubles, and leaves no unallocated
    residue. It does not read persistence or format a user-facing response.
    """
    rounded_deposit = quantize_ruble_amount(deposit_amount)
    if rounded_deposit <= 0:
        raise ValueError("Сумма должна быть положительной и не меньше 1 ₽.")

    rebalanceable_base = sum(
        _decimal(class_values.get(asset_class))
        for asset_class in REBALANCE_ASSET_CLASSES
    )
    deficits = {asset_class: Decimal("0") for asset_class in REBALANCE_ASSET_CLASSES}
    raw_allocations = {asset_class: Decimal("0") for asset_class in REBALANCE_ASSET_CLASSES}

    if rebalanceable_base <= 0:
        for asset_class in REBALANCE_ASSET_CLASSES:
            raw_allocations[asset_class] = (
                rounded_deposit * _decimal(target_weights.get(asset_class)) / Decimal("100")
            )
            deficits[asset_class] = raw_allocations[asset_class]
    else:
        new_base = rebalanceable_base + rounded_deposit
        for asset_class in REBALANCE_ASSET_CLASSES:
            desired_value = new_base * _decimal(target_weights.get(asset_class)) / Decimal("100")
            deficits[asset_class] = max(
                desired_value - _decimal(class_values.get(asset_class)), Decimal("0")
            )

        total_deficit = sum(deficits.values())
        for asset_class in REBALANCE_ASSET_CLASSES:
            if total_deficit > 0:
                raw_allocations[asset_class] = rounded_deposit * deficits[asset_class] / total_deficit
            else:
                raw_allocations[asset_class] = (
                    rounded_deposit * _decimal(target_weights.get(asset_class)) / Decimal("100")
                )
                deficits[asset_class] = raw_allocations[asset_class]

    allocations = {
        asset_class: _decimal(raw_allocations.get(asset_class)).to_integral_value(
            rounding=ROUND_FLOOR
        )
        for asset_class in REBALANCE_ASSET_CLASSES
    }
    residue_rubles = int(rounded_deposit - sum(allocations.values()))
    remainder_order = sorted(
        REBALANCE_ASSET_CLASSES,
        key=lambda asset_class: (
            _decimal(raw_allocations.get(asset_class)) - allocations[asset_class],
            deficits[asset_class],
            _decimal(target_weights.get(asset_class)),
            -REBALANCE_ASSET_CLASSES.index(asset_class),
        ),
        reverse=True,
    )
    for index in range(residue_rubles):
        allocations[remainder_order[index % len(remainder_order)]] += Decimal("1")

    return {
        "deposit_amount": rounded_deposit,
        "rebalanceable_base": rebalanceable_base,
        "deficits": deficits,
        "allocations": allocations,
    }
