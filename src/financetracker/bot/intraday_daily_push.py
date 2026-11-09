"""Shared presentation for observed daily changes and their reconciliation."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from financetracker.domain.daily_change import calculate_daily_change, decimal, position_label


COMPONENT_LABELS = {
    "prices": "Цены бумаг", "accrual": "Накопление купонов",
    "coupon_reset": "Снижение накопленных купонов", "coupon": "Зачисленные купоны до налога",
    "dividend": "Зачисленные дивиденды до налога", "deduction": "Налоговый вычет ИИС",
    "commission": "Комиссии", "tax": "Налоги", "tax_refund": "Возврат налога",
    "other": "Остальные изменения / данные требуют сверки",
}


def _rub(value: Decimal, *, signed: bool = False, precision: int = 0) -> str:
    value = value.quantize(Decimal("1").scaleb(-precision), rounding=ROUND_HALF_UP)
    prefix = "+" if signed and value > 0 else "−" if value < 0 else ""
    return prefix + f"{abs(value):,.{precision}f}".replace(",", " ").replace(".", ",") + " ₽"


def _pct(value: Decimal | None) -> str:
    if value is None:
        return "—"
    prefix = "+" if value > 0 else "−" if value < 0 else ""
    return f"{prefix}{abs(value):.2f}".replace(".", ",") + "%"


def _observed_at(row: Mapping[str, Any]) -> datetime:
    value = row.get("observed_at") or row.get("snapshot_at")
    if not isinstance(value, datetime):
        raise ValueError("Snapshot must contain observed_at datetime")
    return value


def _stamp(stamp, local_tz):
    stamp = stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(local_tz or timezone.utc).strftime("%d.%m %H:%M %Z")


def _coupon_notes(resets, events):
    notes = []
    for reset in sorted(resets, key=lambda row: row["removed"], reverse=True)[:3]:
        label = position_label(reset)
        notes.append(f"Снижение НКД: {label}, {_rub(-reset['removed'], precision=2)}.")
        if not reset.get("last_quantity"):
            notes.append("Позиция закрыта; ожидаемая выплата по текущему количеству не рассчитывается.")
            continue
        schedules = [event for event in events if event["kind"] == "coupon_schedule"
                     and event.get("figi") == reset.get("figi") and event.get("amount_per_unit") is not None]
        if not schedules:
            notes.append("Связь с купонной выплатой пока не подтверждена календарём.")
            continue
        event = min(schedules, key=lambda item: item["payment_date"])
        expected = decimal(event["amount_per_unit"]) * decimal(reset["last_quantity"])
        notes.append(f"По календарю выплата {event['payment_date'].strftime('%d.%m.%Y')}: {_rub(expected, precision=2)} до налога.")
        received = any(row["kind"] == "coupon" and row.get("figi") == reset.get("figi") and decimal(row["amount"]) > 0 for row in events)
        notes.append("Зачисление купона найдено в операциях периода." if received else
                     "Зачисление пока не подтверждено операциями периода; ожидаемая сумма не включена в баланс.")
    if len(resets) > 3:
        notes.append(f"Ещё {len(resets) - 3} бумаг со снижением НКД учтены в общей расшифровке.")
    return notes


def build_intraday_daily_push_payload(
    *, report_date: date, snapshots: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]], cash_events: Iterable[Mapping[str, Any]] = (),
    baseline=None, local_tz=None,
) -> dict[str, Any] | None:
    ordered = sorted(snapshots, key=_observed_at)
    if not ordered or (len(ordered) < 2 and baseline is None):
        return None
    first = baseline or ordered[0]
    currencies = {str(row.get("currency") or "RUB").upper() for row in [first, *ordered]}
    if len(currencies) != 1:
        raise ValueError("Daily report requires one portfolio currency")
    currency = currencies.pop()
    if currency != "RUB":
        raise ValueError("Daily reconciliation currently supports RUB portfolios only; implicit FX is disabled")
    values = [decimal(row.get("total_value")) for row in ordered]
    opening, closing = decimal(first.get("total_value")), values[-1]
    events = list(cash_events)
    change = calculate_daily_change(opening=opening, closing=closing, positions=positions, cash_events=events, currency=currency)
    positive = sorted((row for row in change["movers"] if row["contribution"] > 0), key=lambda row: row["contribution"], reverse=True)[:3]
    negative = sorted((row for row in change["movers"] if row["contribution"] < 0), key=lambda row: row["contribution"])[:3]
    low, high = min(values), max(values)
    amplitude_pct = (high - low) / opening * 100 if opening else Decimal("0")
    components = [{"label": COMPONENT_LABELS[key], "contribution": amount}
                  for key, amount in change["components"].items() if amount]
    unsupported = [event for event in events if event["kind"] == "unsupported_currency"]
    change_label = "Изменение"
    lines = [f"📊 {report_date.strftime('%d.%m.%Y')}", "",
             f"Портфель: {_rub(closing, precision=2)}",
             f"{change_label}: {_rub(change['change'], signed=True, precision=2)} ({_pct(change['change_pct'])})",
             f"Период: {_stamp(_observed_at(first), local_tz)} → {_stamp(_observed_at(ordered[-1]), local_tz)}",
             "", "Из чего сложилось:"]
    lines.extend(f"• {row['label']}: {_rub(row['contribution'], signed=True, precision=2)}" for row in components)
    if not components:
        lines.append("• Изменений нет: 0,00 ₽")
    if change["external_flow"]:
        lines.append(f"Пополнения / выводы: {_rub(change['external_flow'], signed=True, precision=2)} — исключены из изменения.")
    notes = _coupon_notes(change["resets"], events)
    if any(event["flow"] for event in unsupported):
        notes.append("Валютные пополнения и выводы исключены не полностью; результат требует сверки.")
    if unsupported:
        notes.append("Есть операции в другой валюте: их рублёвый вклад не рассчитан; требуется сверка без неявного пересчёта валют.")
    if change["incomplete_attribution"] and change["components"]["other"]:
        notes.append("Не удалось полностью сопоставить позиции и операции; неподтверждённые изменения включены в остаток.")
    if notes:
        lines.extend(["", *notes])
    if len(ordered) >= 2:
        lines.extend(["", f"Диапазон наблюдений: {_rub(low)} → {_rub(high)}", f"Амплитуда: {_pct(amplitude_pct)}"])
    for title, movers in (("📈 Лидеры роста цен:", positive), ("📉 Лидеры снижения цен:", negative)):
        if movers:
            lines.extend(["", title])
            lines.extend(f"{row['label']} {_rub(row['contribution'], signed=True)}" for row in movers)
    return {
        "text": "\n".join(lines), "snapshots": ([first] if baseline else []) + ordered,
        "movers": positive + negative, "positive_movers": positive, "negative_movers": negative,
        "currency": currency, "opening_value": opening, "closing_value": closing,
        "low_value": low, "high_value": high, "amplitude_pct": amplitude_pct,
        "market_change": change["change"], "market_pct": change["change_pct"],
        "change": change["change"], "components": components, "reconciliation": change,
    }


def build_intraday_daily_push_payload_from_day(*, day: Any, cash_events=(), local_tz=None, minimum_observations=2):
    valued = [snapshot for snapshot in day.snapshots if snapshot.total_value is not None]
    if len(valued) < minimum_observations:
        return None
    baseline = day.baseline
    if baseline and baseline.total_value is None:
        baseline = None
    return build_intraday_daily_push_payload(
        report_date=day.observed_date,
        snapshots=[{"observed_at": row.observed_at, "total_value": row.total_value, "currency": row.currency} for row in valued],
        positions=[asdict(row) for row in day.positions], cash_events=cash_events, local_tz=local_tz,
        baseline={"observed_at": baseline.observed_at, "total_value": baseline.total_value, "currency": baseline.currency} if baseline else None,
    )
