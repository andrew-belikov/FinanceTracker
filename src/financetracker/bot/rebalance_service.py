from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from financetracker.bot.rebalance_repository import (
    get_rebalance_targets as query_get_rebalance_targets,
    replace_rebalance_targets as query_replace_rebalance_targets,
)
from financetracker.domain.rebalance import (
    REBALANCE_ASSET_CLASSES,
    REBALANCE_CLASS_LABELS,
    aggregate_rebalance_values_by_class,
    compute_invest_plan,
    compute_rebalance_plan,
    parse_rebalance_targets_args as parse_domain_rebalance_targets_args,
    quantize_ruble_amount,
)
from financetracker.bot.portfolio_repository import (
    get_latest_snapshot_with_id,
    get_positions_for_snapshot,
)
from financetracker.bot.runtime import (
    MONTHS_RU_GENITIVE,
    REBALANCE_FEATURE_UNAVAILABLE_TEXT,
    REBALANCE_TARGETS_NOT_CONFIGURED_TEXT,
    TARGETS_USAGE_TEXT,
    fmt_decimal_rub,
    normalize_decimal,
)


def format_decimal_number(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    decimal_value = normalize_decimal(value)
    quantizer = Decimal("1") if precision == 0 else Decimal(f"1.{'0' * precision}")
    quantized = decimal_value.quantize(quantizer, rounding=ROUND_HALF_UP)
    text_value = format(quantized, "f")
    if "." in text_value:
        text_value = text_value.rstrip("0").rstrip(".")
    if signed and quantized >= 0:
        text_value = f"+{text_value}"
    return text_value


def format_decimal_pct(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    return f"{format_decimal_number(value, precision=precision, signed=signed)} %"


def format_decimal_pp(
    value: Decimal | float | int | None,
    *,
    precision: int = 2,
    signed: bool = False,
) -> str:
    return f"{format_decimal_number(value, precision=precision, signed=signed)} п.п."


def format_rebalance_weight(value: Decimal | float | int | None) -> str:
    decimal_value = normalize_decimal(value).quantize(Decimal("1.0"), rounding=ROUND_HALF_UP)
    return f"{format(decimal_value, '.1f').replace('.', ',')}%"


def format_human_date_ru(value: date | None) -> str:
    if value is None:
        return ""
    return f"{value.day} {MONTHS_RU_GENITIVE[value.month]} {value.year}"


def parse_rebalance_targets_args(args: list[str]) -> dict[str, Decimal]:
    """Adapt domain parsing errors to the Telegram command contract."""
    return parse_domain_rebalance_targets_args(args, usage_text=TARGETS_USAGE_TEXT)


def get_rebalance_targets(session, account_id: str) -> dict[str, Decimal] | None:
    return query_get_rebalance_targets(session, account_id, REBALANCE_ASSET_CLASSES)


def replace_rebalance_targets(session, account_id: str, targets: dict[str, Decimal]) -> bool:
    return query_replace_rebalance_targets(
        session,
        account_id,
        targets,
        REBALANCE_ASSET_CLASSES,
    )


def get_latest_rebalance_snapshot(session, account_id: str) -> dict:
    latest_snapshot = get_latest_snapshot_with_id(session, account_id)
    positions: list[dict] = []
    snapshot_date: date | None = None
    total_portfolio_value = Decimal("0")
    if latest_snapshot:
        snapshot_date = latest_snapshot["snapshot_date"]
        total_portfolio_value = normalize_decimal(latest_snapshot["total_value"])
        positions = get_positions_for_snapshot(session, latest_snapshot["id"])

    class_values, other_groups = aggregate_rebalance_values_by_class(positions)
    if total_portfolio_value <= 0:
        total_portfolio_value = sum(class_values.values()) + sum(other_groups.values())

    return {
        "snapshot_date": snapshot_date,
        "total_portfolio_value": total_portfolio_value,
        "class_values": class_values,
        "other_groups": other_groups,
    }


def _build_rebalance_diff_lines(rows: list[dict[str, Decimal | str]]) -> list[str]:
    lines: list[str] = []
    for row in rows:
        lines.append(
            f"- {'✅' if row['status'] == 'в норме' else '⚠️'} "
            f"{row['label']}: {format_rebalance_weight(row['current_pct'])} / "
            f"{format_rebalance_weight(row['target_pct'])}"
        )
    return lines


def _build_out_of_model_lines(
    other_groups: dict[str, Decimal],
    total_portfolio_value: Decimal,
) -> list[str]:
    if not other_groups:
        return []

    lines = ["Вне модели:"]
    sorted_groups = sorted(other_groups.items(), key=lambda item: item[1], reverse=True)
    for group_name, group_value in sorted_groups:
        share_pct = (
            group_value * Decimal("100") / total_portfolio_value
            if total_portfolio_value > 0
            else Decimal("0")
        )
        lines.append(
            f"- {group_name}: {fmt_decimal_rub(group_value, precision=0)} "
            f"({format_decimal_pct(share_pct, precision=1)} портфеля)"
        )
    return lines


def build_targets_text_for_account(session, account_id: str) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    snapshot_date = snapshot["snapshot_date"]

    header = "🎯 Таргеты аллокации"
    if snapshot_date is not None:
        header += f" (на {snapshot_date.isoformat()})"

    lines = [header, "", "Текущие таргеты (факт / план):"]
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    if snapshot_date is None:
        lines.append("")
        lines.append("Фактическая структура появится после первого снапшота.")

    return "\n".join(lines)


def build_rebalance_text_for_account(session, account_id: str) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    snapshot_date = snapshot["snapshot_date"]

    header = "⚖️ Ребаланс"
    lines = [header]
    if snapshot_date is not None:
        lines.extend(["", format_human_date_ru(snapshot_date)])
    lines.extend(["", "Текущие таргеты (факт / план):"])
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    sell_rows: list[tuple[str, Decimal]] = []
    buy_rows: list[tuple[str, Decimal]] = []
    for row in rebalance_plan["rows"]:
        delta_value = quantize_ruble_amount(row["delta_value"])
        if delta_value > 0:
            buy_rows.append((row["label"], delta_value))
        elif delta_value < 0:
            sell_rows.append((row["label"], abs(delta_value)))

    sell_rows.sort(key=lambda item: item[1], reverse=True)
    buy_rows.sort(key=lambda item: item[1], reverse=True)

    lines.append("")
    lines.append("Чтобы поймать баланс сейчас:")
    if sell_rows:
        lines.extend(["", "📉 Продать:"])
        for label, amount in sell_rows:
            lines.append(f"- {label}: {fmt_decimal_rub(amount, precision=0)}")
    if buy_rows:
        lines.extend(["", "📈 Купить:"])
        for label, amount in buy_rows:
            lines.append(f"- {label}: {fmt_decimal_rub(amount, precision=0)}")
    if not sell_rows and not buy_rows:
        lines.append("")
        lines.append("Баланс уже близок к целевому, действий не требуется.")

    if snapshot["other_groups"]:
        lines.append("")
        lines.append(
            "Расчёт buy/sell сделан по ребалансируемой части портфеля: "
            f"{fmt_decimal_rub(rebalance_plan['rebalanceable_base'], precision=0)}."
        )

    return "\n".join(lines)


def build_invest_text_for_account(
    session,
    account_id: str,
    deposit_amount: Decimal | float | int,
    *,
    header: str | None = None,
) -> str:
    targets = get_rebalance_targets(session, account_id)
    if targets is None:
        return REBALANCE_FEATURE_UNAVAILABLE_TEXT
    if not targets:
        return REBALANCE_TARGETS_NOT_CONFIGURED_TEXT

    snapshot = get_latest_rebalance_snapshot(session, account_id)
    rebalance_plan = compute_rebalance_plan(snapshot["class_values"], targets)
    invest_plan = compute_invest_plan(snapshot["class_values"], targets, deposit_amount)

    lines = [header or f"💸 Как распределить пополнение {fmt_decimal_rub(invest_plan['deposit_amount'], precision=0)}"]
    lines.append("")
    lines.append("Текущие таргеты (факт / план):")
    lines.extend(_build_rebalance_diff_lines(rebalance_plan["rows"]))

    out_of_model_lines = _build_out_of_model_lines(
        snapshot["other_groups"],
        snapshot["total_portfolio_value"],
    )
    if out_of_model_lines:
        lines.append("")
        lines.extend(out_of_model_lines)

    lines.append("")
    lines.append(f"Распределение пополнения {fmt_decimal_rub(invest_plan['deposit_amount'], precision=0)}:")
    for asset_class in REBALANCE_ASSET_CLASSES:
        allocation = invest_plan["allocations"][asset_class]
        lines.append(
            f"- {REBALANCE_CLASS_LABELS[asset_class]}: {fmt_decimal_rub(allocation, precision=0)}"
        )

    if snapshot["other_groups"]:
        lines.append("")
        lines.append("Классы вне модели не участвуют в распределении пополнения.")

    return "\n".join(lines)
