"""Pure Telegram presentation logic for the upcoming payout calendar."""

from __future__ import annotations

from datetime import date, datetime, timezone, tzinfo
from decimal import Decimal
from typing import Mapping

from financetracker.common.finance import annualize_simple_yield_pct
from financetracker.domain.payouts import resolve_payout_amount


DEFAULT_MAX_LISTED_EVENTS = 30


def _decimal(value: object) -> Decimal:
    return Decimal("0") if value is None else Decimal(str(value))


def _format_plain_pct(value: Decimal) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_rub(value: Decimal) -> str:
    return f"{float(value):,.2f} ₽".replace(",", " ")


def _format_payout_amount(amount: Decimal | None, currency: str | None, *, estimated: bool = False) -> str:
    if amount is None:
        return "сумма уточняется"
    prefix = "~ " if estimated else ""
    normalized_currency = (currency or "").upper()
    if normalized_currency == "RUB":
        return f"{prefix}{_format_rub(amount)}"
    return f"{prefix}{amount:,.2f}".replace(",", " ") + f" {normalized_currency or '—'}"


def _monthly_totals(rows: list[Mapping[str, object]], *, tax_rate_pct: Decimal, month_names: Mapping[int, str]) -> list[str]:
    totals: dict[tuple[int, int], dict[str, Decimal]] = {}
    unknowns: dict[tuple[int, int], int] = {}
    estimated: dict[tuple[int, int], set[str]] = {}
    for row in rows:
        payment_date = row["payment_date"]
        if not isinstance(payment_date, date):
            raise TypeError("payment_date must be a date")
        key = (payment_date.year, payment_date.month)
        amount, is_estimated = resolve_payout_amount(row, tax_rate_pct)
        if amount is None:
            unknowns[key] = unknowns.get(key, 0) + 1
            totals.setdefault(key, {})
            continue
        currency = str(row.get("currency") or "—").upper()
        if is_estimated:
            estimated.setdefault(key, set()).add(currency)
        totals.setdefault(key, {})[currency] = totals.setdefault(key, {}).get(currency, Decimal("0")) + amount
    result: list[str] = []
    for year, month in sorted(set(totals) | set(unknowns)):
        key = (year, month)
        parts = [_format_payout_amount(amount, currency, estimated=currency in estimated.get(key, set())) for currency, amount in sorted(totals.get(key, {}).items())]
        if count := unknowns.get(key, 0):
            parts.append(f"без известной суммы: {count}")
        result.append(f"{month_names[month].capitalize()} {year}: {' · '.join(parts)}")
    return result


def render_payout_calendar_text(
    rows: list[dict] | None,
    *,
    start_date: date,
    end_date: date,
    heading: str,
    tax_rate_pct: Decimal,
    display_timezone: tzinfo,
    month_names: Mapping[int, str],
    max_listed_events: int = DEFAULT_MAX_LISTED_EVENTS,
) -> str:
    """Render data returned by the payout read model without Telegram dependencies."""
    period_label = f"{start_date:%d.%m.%Y} — {end_date:%d.%m.%Y}"
    if rows is None:
        return f"{heading}\n{period_label}\n\nКалендарь выплат пока недоступен. Нужно применить миграцию и дождаться синхронизации tracker."
    if not rows:
        return f"{heading}\n{period_label}\n\nОжидаемых купонов и объявленных дивидендов нет.\n\nДивиденды появляются только после официального объявления."
    typed_rows: list[Mapping[str, object]] = rows
    totals: dict[str, Decimal] = {}
    estimated_currencies: set[str] = set()
    unknown_amounts = 0
    for row in typed_rows:
        amount, estimated = resolve_payout_amount(row, tax_rate_pct)
        if amount is None:
            unknown_amounts += 1
            continue
        currency = str(row.get("currency") or "—").upper()
        if estimated:
            estimated_currencies.add(currency)
        totals[currency] = totals.get(currency, Decimal("0")) + amount
    tax_rate_label = _format_plain_pct(tax_rate_pct)
    lines = [heading, period_label, ""]
    if totals:
        parts = [_format_payout_amount(amount, currency, estimated=currency in estimated_currencies) for currency, amount in sorted(totals.items())]
        lines.append(f"Ожидаемая сумма после расчётного налога {tax_rate_label} %: {' · '.join(parts)}")
    if unknown_amounts:
        lines.append(f"Без известной суммы: {unknown_amounts}")
    lines.extend(["", "По месяцам:", *_monthly_totals(typed_rows, tax_rate_pct=tax_rate_pct, month_names=month_names), "", "Ближайшие выплаты:"])
    listed_rows = typed_rows[:max_listed_events]
    for row in listed_rows:
        payment_date = row["payment_date"]
        if not isinstance(payment_date, date):
            raise TypeError("payment_date must be a date")
        event_label = "купон" if row.get("event_type") == "coupon" else "дивиденды"
        net_amount, estimated = resolve_payout_amount(row, tax_rate_pct)
        instrument_name = str(row.get("instrument_name") or row.get("figi") or "Инструмент")
        if len(instrument_name) > 64:
            instrument_name = instrument_name[:61] + "..."
        annualized_label = ""
        start, end = row.get("coupon_start_date"), row.get("coupon_end_date")
        if row.get("event_type") == "coupon" and isinstance(start, date) and isinstance(end, date):
            days = (end - start).days
            expected_days = row.get("coupon_period_days")
            if days > 0 and (expected_days is None or expected_days == days):
                cost_basis = _decimal(row.get("cost_basis")); expected_amount = _decimal(net_amount)
                if cost_basis > 0 and expected_amount > 0:
                    annualized = annualize_simple_yield_pct(expected_amount / cost_basis * Decimal("100"), days)
                    if annualized is not None:
                        annualized_label = f" · ≈ {_format_plain_pct(annualized)} % годовых"
        lines.append(f"{payment_date:%d.%m} · {event_label} · {instrument_name} · {_format_payout_amount(net_amount, row.get('currency') if isinstance(row.get('currency'), str) else None, estimated=estimated)}{annualized_label}{' · по предыдущему купону' if estimated else ''}")
    if hidden_count := len(typed_rows) - len(listed_rows):
        lines.append(f"…ещё {hidden_count} выплат")
    fetched_values = [value for row in typed_rows if isinstance((value := row.get("fetched_at")), datetime)]
    if fetched_values:
        oldest = min(fetched_values)
        oldest = oldest.replace(tzinfo=timezone.utc) if oldest.tzinfo is None else oldest
        freshness_label = oldest.astimezone(display_timezone).strftime("%d.%m.%Y %H:%M")
    else:
        freshness_label = None
    lines.extend(["Расчёт сделан по текущему количеству бумаг и расчётной ставке налога " + tax_rate_label + " %. Проценты — простой годовой эквивалент ожидаемого купона после этого налога к cost basis, без реинвестирования; это не YTM и не прогноз доходности. Суммы с `~` оценены по предыдущему купону. Фактическая сумма и налог могут отличаться."])
    if freshness_label:
        lines.append(f"Данные обновлены: {freshness_label} МСК.")
    return "\n".join(lines)
