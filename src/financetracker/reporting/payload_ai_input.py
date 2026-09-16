"""Display-ready facts and bounded AI input for monthly reports."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from financetracker.reporting.runtime import decimal_to_str, fmt_decimal_rub, fmt_pct


MONTHLY_AI_INPUT_SCHEMA_VERSION = "monthly_ai_input.v1"
AI_INPUT_STYLE = "calm precise non-promotional"
DEFAULT_AI_INPUT_MAX_CHARS = 12_000
DEFAULT_AI_TOP_LIMIT = 5


def _format_display_date(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    return datetime.fromisoformat(raw_value).strftime("%d.%m.%Y")


def _format_display_day(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    return datetime.fromisoformat(raw_value).strftime("%d.%m")


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _display_rub(value: Any, *, precision: int = 0) -> str:
    if value is None:
        return "—"
    return fmt_decimal_rub(value, precision=precision)


def _display_nominal_currency(value: Any, currency: str) -> str:
    normalized_currency = (currency or "UNKNOWN").strip().upper() or "UNKNOWN"
    if normalized_currency == "RUB":
        return fmt_decimal_rub(value, precision=2)
    return f"{_to_decimal(value):.2f} {normalized_currency}"


def _build_overview_facts(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload["summary_metrics"]
    highlights = []
    highlights.append(
        f"Стоимость на конец месяца: {fmt_decimal_rub(summary.get('current_value'), precision=0)}."
    )
    if summary.get("period_pnl_abs") is not None:
        highlights.append(
            f"Результат периода: {fmt_decimal_rub(summary.get('period_pnl_abs'), precision=0)} "
            f"({fmt_pct(float(_to_decimal(summary.get('period_pnl_pct'))), precision=2) if summary.get('period_pnl_pct') is not None else '—'})."
        )
    highlights.append(
        f"Внешний поток: {fmt_decimal_rub(summary.get('net_external_flow'), precision=0)}; "
        f"пополнения {fmt_decimal_rub(summary.get('deposits'), precision=0)}, "
        f"выводы {fmt_decimal_rub(summary.get('withdrawals'), precision=0)}."
    )
    if _to_decimal(summary.get("tax_refunds")) != 0:
        highlights.append(
            f"Возврат налога: {fmt_decimal_rub(summary.get('tax_refunds'), precision=2)}."
        )
    if summary.get("top_holding_name"):
        highlights.append(
            f"Крупнейшая позиция: {summary.get('top_holding_name')} "
            f"{fmt_decimal_rub(summary.get('top_holding_value'), precision=0)} "
            f"({fmt_pct(float(_to_decimal(summary.get('top_holding_weight_pct'))), precision=1)})."
        )

    return {
        "current_value": _display_rub(summary.get("current_value"), precision=0),
        "period_pnl_abs": _display_rub(summary.get("period_pnl_abs"), precision=0),
        "period_pnl_pct": fmt_pct(float(_to_decimal(summary.get("period_pnl_pct"))), precision=2)
        if summary.get("period_pnl_pct") is not None
        else "—",
        "period_twr_pct": fmt_pct(float(_to_decimal(summary.get("period_twr_pct"))), precision=2)
        if summary.get("period_twr_pct") is not None
        else "—",
        "net_external_flow": _display_rub(summary.get("net_external_flow"), precision=0),
        "income_net": _display_rub(summary.get("income_net"), precision=2),
        "iis_tax_deduction_income": _display_rub(summary.get("iis_tax_deduction_income"), precision=2),
        "total_income_net": _display_rub(summary.get("total_income_net"), precision=2),
        "commissions": _display_rub(summary.get("commissions"), precision=2),
        "taxes": _display_rub(summary.get("taxes"), precision=2),
        "tax_refunds": _display_rub(summary.get("tax_refunds"), precision=2),
        "top_holding_name": summary.get("top_holding_name"),
        "top_holding_value": _display_rub(summary.get("top_holding_value"), precision=0),
        "top_holding_weight_pct": fmt_pct(float(_to_decimal(summary.get("top_holding_weight_pct"))), precision=1)
        if summary.get("top_holding_weight_pct") is not None
        else "—",
        "best_day": {
            "date": _format_display_date(summary.get("best_day_date")),
            "pnl": _display_rub(summary.get("best_day_pnl"), precision=0),
        },
        "worst_day": {
            "date": _format_display_date(summary.get("worst_day_date")),
            "pnl": _display_rub(summary.get("worst_day_pnl"), precision=0),
        },
        "highlights": highlights[:5],
    }


def _build_performance_facts(payload: dict[str, Any]) -> dict[str, Any]:
    daily_rows = payload["timeseries_daily"]
    if daily_rows:
        peak = max(daily_rows, key=lambda row: _to_decimal(row.get("portfolio_value")))
        trough = min(daily_rows, key=lambda row: _to_decimal(row.get("portfolio_value")))
    else:
        peak = {}
        trough = {}

    summary = payload["summary_metrics"]
    return {
        "period_twr_pct": fmt_pct(float(_to_decimal(summary.get("period_twr_pct"))), precision=2)
        if summary.get("period_twr_pct") is not None
        else "—",
        "period_pnl_abs": _display_rub(summary.get("period_pnl_abs"), precision=0),
        "period_pnl_pct": fmt_pct(float(_to_decimal(summary.get("period_pnl_pct"))), precision=2)
        if summary.get("period_pnl_pct") is not None
        else "—",
        "best_day": {
            "date": _format_display_date(summary.get("best_day_date")),
            "pnl": _display_rub(summary.get("best_day_pnl"), precision=0),
        },
        "worst_day": {
            "date": _format_display_date(summary.get("worst_day_date")),
            "pnl": _display_rub(summary.get("worst_day_pnl"), precision=0),
        },
        "portfolio_peak": {
            "date": _format_display_date(peak.get("date")),
            "value": _display_rub(peak.get("portfolio_value"), precision=0),
        },
        "portfolio_trough": {
            "date": _format_display_date(trough.get("date")),
            "value": _display_rub(trough.get("portfolio_value"), precision=0),
        },
    }


def _build_structure_facts(payload: dict[str, Any]) -> dict[str, Any]:
    positions = payload["positions_current"]
    top_positions = positions[:DEFAULT_AI_TOP_LIMIT]
    concentration_top3 = sum((_to_decimal(row.get("weight_pct")) for row in positions[:3]), Decimal("0"))
    asset_mix = [
        {
            "instrument_type": row.get("instrument_type"),
            "snapshot_total": _display_rub(row.get("snapshot_total"), precision=0),
            "delta_abs": _display_rub(row.get("delta_abs"), precision=0),
        }
        for row in payload["reconciliation_by_asset_type"]
        if _to_decimal(row.get("snapshot_total")) != 0
    ]
    return {
        "top_positions": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "position_value": fmt_decimal_rub(row.get("position_value"), precision=0),
                "weight_pct": fmt_pct(float(_to_decimal(row.get("weight_pct"))), precision=1),
            }
            for row in top_positions
        ],
        "concentration_top3_weight_pct": fmt_pct(float(concentration_top3), precision=1),
        "asset_mix": asset_mix,
    }


def _build_position_flow_facts(payload: dict[str, Any]) -> dict[str, Any]:
    grouped = payload["position_flow_groups"]

    def _normalize(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "delta_qty": decimal_to_str(_to_decimal(row.get("delta_qty"))),
                "delta_value": fmt_decimal_rub(row.get("delta_value"), precision=0),
            }
            for row in items[:DEFAULT_AI_TOP_LIMIT]
        ]

    return {
        "new": _normalize(grouped.get("new", [])),
        "closed": _normalize(grouped.get("closed", [])),
        "increased": _normalize(grouped.get("increased", [])),
        "decreased": _normalize(grouped.get("decreased", [])),
    }


def _build_mover_facts(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "top_growth": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "rise_abs": fmt_decimal_rub(row.get("rise_abs"), precision=0),
                "window": f"{_format_display_day(row.get('start_date'))} → {_format_display_day(row.get('end_date'))}",
            }
            for row in payload["instrument_movers"].get("top_growth", [])[:DEFAULT_AI_TOP_LIMIT]
        ],
        "top_drawdown": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "drawdown_abs": fmt_decimal_rub(row.get("drawdown_abs"), precision=0),
                "window": f"{_format_display_day(row.get('start_date'))} → {_format_display_day(row.get('end_date'))}",
            }
            for row in payload["instrument_movers"].get("top_drawdown", [])[:DEFAULT_AI_TOP_LIMIT]
        ],
    }


def _build_contribution_facts(payload: dict[str, Any]) -> dict[str, Any]:
    realized_positive = [row for row in payload["realized_by_asset"] if _to_decimal(row.get("amount")) > 0]
    realized_negative = [row for row in payload["realized_by_asset"] if _to_decimal(row.get("amount")) < 0]
    open_pl_positive = [row for row in payload["open_pl_end"] if _to_decimal(row.get("amount")) > 0]
    open_pl_negative = [row for row in payload["open_pl_end"] if _to_decimal(row.get("amount")) < 0]

    return {
        "realized_winners": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in realized_positive[:DEFAULT_AI_TOP_LIMIT]
        ],
        "realized_losers": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in realized_negative[:DEFAULT_AI_TOP_LIMIT]
        ],
        "income_contributors": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "income_kind": row.get("income_kind"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=2),
            }
            for row in payload["income_by_asset"][:DEFAULT_AI_TOP_LIMIT]
        ],
        "open_pl_leaders": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in open_pl_positive[:DEFAULT_AI_TOP_LIMIT]
        ],
        "open_pl_laggards": [
            {
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in open_pl_negative[:DEFAULT_AI_TOP_LIMIT]
        ],
    }


def _build_cashflow_facts(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload["summary_metrics"]
    income_by_currency = payload.get("income_by_currency") or []
    return {
        "deposits": _display_rub(summary.get("deposits"), precision=0),
        "withdrawals": _display_rub(summary.get("withdrawals"), precision=0),
        "income_net": _display_rub(summary.get("income_net"), precision=2),
        "iis_tax_deduction_income": _display_rub(summary.get("iis_tax_deduction_income"), precision=2),
        "total_income_net": _display_rub(summary.get("total_income_net"), precision=2),
        "commissions": _display_rub(summary.get("commissions"), precision=2),
        "taxes": _display_rub(summary.get("taxes"), precision=2),
        "tax_refunds": _display_rub(summary.get("tax_refunds"), precision=2),
        "income_by_currency": [
            {
                "currency": str(row.get("currency") or "UNKNOWN").upper(),
                "coupons": _display_nominal_currency(
                    row.get("coupons"), str(row.get("currency") or "UNKNOWN")
                ),
                "dividends": _display_nominal_currency(
                    row.get("dividends"), str(row.get("currency") or "UNKNOWN")
                ),
                "taxes": _display_nominal_currency(
                    row.get("taxes"), str(row.get("currency") or "UNKNOWN")
                ),
                "tax_refunds": _display_nominal_currency(
                    row.get("tax_refunds"), str(row.get("currency") or "UNKNOWN")
                ),
            }
            for row in income_by_currency
        ],
        "unknown_income_currency_warning": any(
            str(row.get("currency") or "UNKNOWN").upper() == "UNKNOWN"
            for row in income_by_currency
        ),
        "operations_top": [
            {
                "local_date": _format_display_date(row.get("local_date")),
                "operation_group": row.get("operation_group"),
                "ticker": row.get("ticker") or "",
                "name": row.get("name"),
                "amount": fmt_decimal_rub(row.get("amount"), precision=0),
            }
            for row in payload["operations_top"][:DEFAULT_AI_TOP_LIMIT]
        ],
    }


def _build_quality_facts(payload: dict[str, Any]) -> dict[str, Any]:
    quality = payload["data_quality"]
    return {
        "reconciliation_gap_abs": _display_rub(payload["summary_metrics"].get("reconciliation_gap_abs"), precision=0),
        "unknown_operation_group_count": quality.get("unknown_operation_group_count"),
        "mojibake_detected_count": quality.get("mojibake_detected_count"),
        "positions_missing_label_count": quality.get("positions_missing_label_count"),
        "asset_alias_rows_count": quality.get("asset_alias_rows_count"),
        "has_full_history_from_zero": quality.get("has_full_history_from_zero"),
        "income_events_available": quality.get("income_events_available"),
        "has_rebalance_targets": quality.get("has_rebalance_targets"),
    }


def _trim_ai_input(ai_input: dict[str, Any], max_input_chars: int | None) -> dict[str, Any]:
    if max_input_chars is None:
        return ai_input

    def _size() -> int:
        return len(json.dumps(ai_input, ensure_ascii=False))

    if _size() <= max_input_chars:
        return ai_input

    trim_paths = [
        ("cashflow_facts", "operations_top", 3),
        ("structure_facts", "top_positions", 3),
        ("mover_facts", "top_growth", 2),
        ("mover_facts", "top_drawdown", 2),
        ("contribution_facts", "income_contributors", 2),
        ("contribution_facts", "realized_winners", 2),
        ("contribution_facts", "realized_losers", 2),
        ("contribution_facts", "open_pl_leaders", 2),
        ("contribution_facts", "open_pl_laggards", 2),
        ("position_flow_facts", "new", 2),
        ("position_flow_facts", "closed", 2),
        ("position_flow_facts", "increased", 2),
        ("position_flow_facts", "decreased", 2),
    ]

    for section_name, field_name, min_keep in trim_paths:
        while _size() > max_input_chars and len(ai_input[section_name][field_name]) > min_keep:
            ai_input[section_name][field_name].pop()

    return ai_input


def build_monthly_ai_input(
    payload: dict[str, Any],
    *,
    max_input_chars: int | None = DEFAULT_AI_INPUT_MAX_CHARS,
) -> dict[str, Any]:
    ai_input = {
        "schema_version": MONTHLY_AI_INPUT_SCHEMA_VERSION,
        "meta": {
            "period_label_ru": payload["meta"]["period_label_ru"],
            "account_friendly_name": payload["meta"]["account_friendly_name"],
            "currency": payload["meta"]["currency"],
            "timezone": payload["meta"]["timezone"],
            "style": AI_INPUT_STYLE,
        },
        "overview_facts": _build_overview_facts(payload),
        "performance_facts": _build_performance_facts(payload),
        "structure_facts": _build_structure_facts(payload),
        "position_flow_facts": _build_position_flow_facts(payload),
        "mover_facts": _build_mover_facts(payload),
        "contribution_facts": _build_contribution_facts(payload),
        "cashflow_facts": _build_cashflow_facts(payload),
        "quality_facts": _build_quality_facts(payload),
    }
    return _trim_ai_input(ai_input, max_input_chars=max_input_chars)
