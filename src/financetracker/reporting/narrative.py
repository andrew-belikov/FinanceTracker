"""Deterministic monthly-report narrative without rendering dependencies."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from financetracker.reporting.formatting import display_date, display_pct, display_rub, to_decimal


def report_title_default(payload: dict[str, Any]) -> str:
    return f"{payload['meta']['period_label_ru'].capitalize()}: обзор портфеля"


def build_deterministic_monthly_narrative(payload: dict[str, Any]) -> dict[str, Any]:
    summary, flow = payload["summary_metrics"], payload["position_flow_groups"]
    movers, quality = payload["instrument_movers"], payload["data_quality"]
    takeaways: list[str] = []
    for key, label in (("new", "Новые позиции"), ("closed", "Полностью закрыты")):
        if flow.get(key):
            takeaways.append(label + ": " + ", ".join(row.get("ticker") or row.get("name") or "—" for row in flow[key][:3]) + ".")
    if movers.get("top_growth"):
        row = movers["top_growth"][0]
        takeaways.append(f"Сильнейший рост по внутримесячному открытому результату показал {row.get('ticker') or row.get('name')} ({display_rub(row.get('rise_abs'), precision=0)}).")
    if movers.get("top_drawdown"):
        row = movers["top_drawdown"][0]
        takeaways.append(f"Самая глубокая просадка внутри месяца пришлась на {row.get('ticker') or row.get('name')} ({display_rub(row.get('drawdown_abs'), precision=0)}).")
    quality_notes: list[str] = []
    if to_decimal(summary.get("reconciliation_gap_abs")) != 0:
        quality_notes.append(f"Расхождение между снапшотом и суммой позиций на конец месяца: {display_rub(summary.get('reconciliation_gap_abs'), precision=0)}.")
    if quality.get("positions_missing_label_count"):
        quality_notes.append(f"Позиции без нормальной подписи: {quality.get('positions_missing_label_count')}.")
    if quality.get("mojibake_detected_count"):
        quality_notes.append(f"Подозрительные описания операций: {quality.get('mojibake_detected_count')}.")
    risks: list[str] = []
    if to_decimal(summary.get("top_holding_weight_pct")) >= Decimal("25"):
        risks.append("Один инструмент заметно концентрирует вес портфеля.")
    if not quality.get("has_full_history_from_zero"):
        risks.append("Отчёт нельзя трактовать как полную историю портфеля с нулевой базы.")
    if not quality.get("has_rebalance_targets"):
        risks.append("Таргеты аллокации не заданы, поэтому блок по ребалансу носит справочный характер.")
    return {
        "schema_version": "monthly_fallback_narrative.v1",
        "report_title": report_title_default(payload),
        "executive_summary": [
            f"На конец месяца портфель оценён в {display_rub(summary.get('current_value'), precision=0)}.",
            f"Результат периода составил {display_rub(summary.get('period_pnl_abs'), precision=0)} при TWR {display_pct(summary.get('period_twr_pct'), precision=2)}.",
            f"Чистый внешний поток за месяц: {display_rub(summary.get('net_external_flow'), precision=0)}.",
        ],
        "performance_commentary": [
            f"Лучший день месяца: {display_date(summary.get('best_day_date'))} с результатом {display_rub(summary.get('best_day_pnl'), precision=0)}.",
            f"Самый слабый день: {display_date(summary.get('worst_day_date'))} с результатом {display_rub(summary.get('worst_day_pnl'), precision=0)}.",
        ],
        "instrument_takeaways": takeaways[:5],
        "cashflow_notes": [
            f"Пополнения: {display_rub(summary.get('deposits'), precision=0)}, выводы: {display_rub(summary.get('withdrawals'), precision=0)}.",
            f"Купоны и дивиденды: {display_rub(summary.get('income_net'), precision=2)}, вычет ИИС: {display_rub(summary.get('iis_tax_deduction_income'), precision=2)}, всего доходов: {display_rub(summary.get('total_income_net'), precision=2)}, комиссии: {display_rub(summary.get('commissions'), precision=2)}, налоги: {display_rub(summary.get('taxes'), precision=2)}.",
            f"Возврат налога: {display_rub(summary.get('tax_refunds'), precision=2)}.",
        ],
        "quality_notes": quality_notes[:3], "risk_notes": risks[:4], "warnings": [],
    }
