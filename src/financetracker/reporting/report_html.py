"""HTML document assembly for monthly reports."""

from __future__ import annotations

from html import escape
from typing import Any

from financetracker.common.logging_setup import get_logger
from financetracker.reporting.formatting import (
    display_date as _display_date,
    display_day as _display_day,
    display_pct as _display_pct,
    display_pct_compact as _display_pct_compact,
    display_rub as _display_rub,
    display_timestamp as _display_timestamp,
)
from financetracker.reporting.narrative import build_deterministic_monthly_narrative
from financetracker.reporting.report_charts import build_monthly_report_charts
from financetracker.reporting.report_html_parts import (
    _build_asset_class_summary_rows,
    _build_day_count_fact,
    _build_plan_pace_fact,
    _build_weight_transition_map,
    _classify_day_pnl_rows,
    _compute_average_day_pnl,
    _render_asset_cell,
    _render_bullet_list,
    _render_chart_stage,
    _render_fact_group,
    _render_fact_stack,
    _render_image_block,
    _render_muted,
    _render_nowrap,
    _render_num_cell,
    _render_rows_table,
    _render_structure_summary,
    _render_target_drift,
    _render_visual_chart,
    _resolve_report_title,
    _to_decimal,
)


logger = get_logger(__name__)


def build_monthly_report_html(
    payload: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    charts: dict[str, str | None] | None = None,
) -> str:
    narrative = narrative or build_deterministic_monthly_narrative(payload)
    charts = charts or build_monthly_report_charts(payload)
    meta = payload["meta"]
    summary = payload["summary_metrics"]
    report_title = _resolve_report_title(payload, narrative)
    summary_subline = (
        f"Результат месяца {_display_rub(summary.get('period_pnl_abs'), precision=0)} "
        f"• TWR {_display_pct(summary.get('period_twr_pct'), precision=2)} "
        f"• Пополнения {_display_rub(summary.get('deposits'), precision=0)}"
    )

    daily_rows = payload.get("timeseries_daily") or []
    peak_row = max(daily_rows, key=lambda row: _to_decimal(row.get("portfolio_value"))) if daily_rows else None
    trough_row = min(daily_rows, key=lambda row: _to_decimal(row.get("portfolio_value"))) if daily_rows else None
    day_stats = _classify_day_pnl_rows(daily_rows)
    classified_days_total = day_stats["positive"]["total"]
    positive_day_fact = _build_day_count_fact(day_stats["positive"]["count"], classified_days_total)
    negative_day_fact = _build_day_count_fact(day_stats["negative"]["count"], classified_days_total)
    avg_positive_day = _compute_average_day_pnl(daily_rows, positive=True)
    avg_negative_day = _compute_average_day_pnl(daily_rows, positive=False)
    plan_pace_primary, plan_pace_secondary = _build_plan_pace_fact(summary)
    weight_transitions = _build_weight_transition_map(
        payload.get("positions_month_start") or [],
        payload.get("positions_current") or [],
    )
    asset_class_summary_rows = _build_asset_class_summary_rows(payload.get("positions_current") or [])

    top_positions_rows = [
        [
            _render_asset_cell(row.get("ticker"), row.get("name")),
            _render_num_cell(_display_rub(row.get("position_value"), precision=0)),
            _render_num_cell(_display_pct_compact(row.get("weight_pct"), precision=1)),
            _render_nowrap(weight_transitions.get(str(row.get("logical_asset_id") or ""), f"0,0% → {_display_pct_compact(row.get('weight_pct'), precision=1)}")),
            _render_num_cell(_display_rub(row.get("expected_yield"), precision=0)),
        ]
        for row in payload["positions_current"][:10]
    ]
    target_drift_rows = [
        {
            "asset_class": row.get("asset_class"),
            "label": row.get("label"),
            "current_pct": row.get("current_pct"),
            "target_pct": row.get("target_pct"),
            "status": row.get("status"),
        }
        for row in payload["rebalance_snapshot"].get("rows", [])
    ]

    flow_panels = []
    for key, title in (
        ("new", "Новые позиции"),
        ("closed", "Полностью закрыты"),
        ("increased", "Увеличены"),
        ("decreased", "Сокращены"),
    ):
        rows = payload["position_flow_groups"].get(key, [])[:5]
        table = _render_rows_table(
            ["Актив", "Изм. кол-ва", "Изм., ₽"],
            [
                [
                    _render_asset_cell(row.get("ticker"), row.get("name")),
                    _render_num_cell(str(row.get("delta_qty") or "—")),
                    _render_num_cell(_display_rub(row.get("delta_value"), precision=0)),
                ]
                for row in rows
            ],
            empty_label="Нет изменений.",
            column_classes=["", "numeric", "numeric"],
        )
        flow_panels.append(f'<div class="panel"><h3>{escape(title)}</h3>{table}</div>')

    movers_rows = [
        [
            _render_asset_cell(row.get("ticker"), row.get("name")),
            _render_num_cell(_display_rub(row.get("rise_abs"), precision=0)),
            _render_nowrap(f"{_display_day(row.get('start_date'))} → {_display_day(row.get('end_date'))}"),
        ]
        for row in payload["instrument_movers"].get("top_growth", [])[:4]
    ]
    drawdown_rows = [
        [
            _render_asset_cell(row.get("ticker"), row.get("name")),
            _render_num_cell(_display_rub(row.get("drawdown_abs"), precision=0)),
            _render_nowrap(f"{_display_day(row.get('start_date'))} → {_display_day(row.get('end_date'))}"),
        ]
        for row in payload["instrument_movers"].get("top_drawdown", [])[:4]
    ]

    realized_profit_rows = [
        [
            _render_asset_cell(row.get("ticker"), row.get("name")),
            _render_num_cell(_display_rub(row.get("amount"), precision=0)),
        ]
        for row in payload["realized_by_asset"]
        if _to_decimal(row.get("amount")) > 0
    ][:4]
    realized_loss_rows = [
        [
            _render_asset_cell(row.get("ticker"), row.get("name")),
            _render_num_cell(_display_rub(row.get("amount"), precision=0)),
        ]
        for row in payload["realized_by_asset"]
        if _to_decimal(row.get("amount")) < 0
    ][:4]
    income_by_asset_rows = [
        [
            _render_asset_cell(row.get("ticker"), row.get("name")),
            _render_muted(row.get("income_kind") or "Доход"),
            _render_num_cell(_display_rub(row.get("amount"), precision=2)),
        ]
        for row in payload["income_by_asset"][:4]
    ]

    operations_rows = [
        [
            _render_nowrap(_display_date(row.get("local_date"))),
            escape(row.get("operation_group") or "—"),
            _render_asset_cell(row.get("ticker"), row.get("name")),
            _render_num_cell(_display_rub(row.get("amount"), precision=0)),
        ]
        for row in payload["operations_top"][:10]
    ]
    income_rows = [
        [
            _render_nowrap(_display_date(row.get("event_date"))),
            escape(row.get("event_type") or "—"),
            _render_asset_cell(row.get("ticker"), row.get("instrument_name")),
            _render_num_cell(_display_rub(row.get("net_amount"), precision=2)),
        ]
        for row in payload["income_events"][:8]
    ]
    quality_rows = [
        ["Расхождение снапшота", _render_num_cell(_display_rub(summary.get("reconciliation_gap_abs"), precision=0))],
        ["Неизвестные группы операций", _render_num_cell(str(payload["data_quality"].get("unknown_operation_group_count", 0)))],
        ["Подозрительные описания", _render_num_cell(str(payload["data_quality"].get("mojibake_detected_count", 0)))],
        ["Позиции без ярлыка", _render_num_cell(str(payload["data_quality"].get("positions_missing_label_count", 0)))],
        ["Строки алиасов", _render_num_cell(str(payload["data_quality"].get("asset_alias_rows_count", 0)))],
        ["Полная история с нуля", _render_nowrap("да" if payload["data_quality"].get("has_full_history_from_zero") else "нет")],
        ["Таргеты ребаланса", _render_nowrap("да" if payload["data_quality"].get("has_rebalance_targets") else "нет")],
    ]

    page_one_fact_groups = [
        (
            "Дни месяца",
            [
                (
                    "Лучший день",
                    _render_fact_stack(
                        _display_rub(summary.get("best_day_pnl"), precision=0),
                        secondary=_display_day(summary.get("best_day_date")),
                    ),
                ),
                (
                    "Худший день",
                    _render_fact_stack(
                        _display_rub(summary.get("worst_day_pnl"), precision=0),
                        secondary=_display_day(summary.get("worst_day_date")),
                    ),
                ),
            ],
        ),
        (
            "Диапазон стоимости",
            [
                (
                    "Пик месяца",
                    _render_fact_stack(
                        _display_rub(peak_row.get("portfolio_value"), precision=0),
                        secondary=_display_day(peak_row.get("date")),
                    ) if peak_row else _render_fact_stack("—", muted=True),
                ),
                (
                    "Минимум месяца",
                    _render_fact_stack(
                        _display_rub(trough_row.get("portfolio_value"), precision=0),
                        secondary=_display_day(trough_row.get("date")),
                    ) if trough_row else _render_fact_stack("—", muted=True),
                ),
            ],
        ),
        (
            "Денежный поток",
            [
                ("Пополнения", _render_fact_stack(_display_rub(summary.get("deposits"), precision=0))),
                ("Купоны и дивиденды", _render_fact_stack(_display_rub(summary.get("income_net"), precision=2))),
                ("Налоговый вычет ИИС", _render_fact_stack(_display_rub(summary.get("iis_tax_deduction_income"), precision=2))),
                ("Доходы всего", _render_fact_stack(_display_rub(summary.get("total_income_net"), precision=2))),
            ],
        ),
    ]
    page_two_fact_groups = [
        (
            "Баланс дней",
            [
                (
                    "Ростовых дней",
                    _render_fact_stack(positive_day_fact[0], secondary=positive_day_fact[1]),
                ),
                (
                    "Снижающихся дней",
                    _render_fact_stack(negative_day_fact[0], secondary=negative_day_fact[1]),
                ),
            ],
        ),
        (
            "Сила движения",
            [
                (
                    "Средний плюс-день",
                    _render_fact_stack(_display_rub(avg_positive_day, precision=0)) if avg_positive_day is not None else _render_fact_stack("—", muted=True),
                ),
                (
                    "Средний минус-день",
                    _render_fact_stack(_display_rub(avg_negative_day, precision=0)) if avg_negative_day is not None else _render_fact_stack("—", muted=True),
                ),
            ],
        ),
        (
            "Годовой план",
            [
                (
                    "Внесено с начала года",
                    _render_fact_stack(
                        _display_rub(summary.get("deposits_ytd"), precision=0),
                        secondary="с начала года",
                    ),
                ),
                (
                    "Темп к дате",
                    _render_fact_stack(plan_pace_primary, secondary=plan_pace_secondary),
                ),
            ],
        ),
    ]

    html = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>{escape(report_title)}</title>
  <style>
    @page {{
      size: A4 portrait;
      margin: 16mm 14mm 16mm 14mm;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: #18222c;
      font-family: "DejaVu Sans", "Liberation Sans", sans-serif;
      background: #ffffff;
    }}
    h1, h2, h3 {{
      margin: 0;
      color: #18222c;
    }}
    h4 {{
      margin: 0;
      color: #314252;
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    p, li, td, th, div {{
      font-size: 10.4px;
      line-height: 1.42;
    }}
    h1 {{ font-size: 27px; font-family: "DejaVu Serif", Georgia, serif; }}
    h2 {{ font-size: 17px; margin-bottom: 10px; font-family: "DejaVu Serif", Georgia, serif; }}
    h3 {{ font-size: 12px; margin-bottom: 8px; }}
    .page {{
      min-height: 257mm;
      display: flex;
      flex-direction: column;
      justify-content: flex-start;
      page-break-after: always;
      padding: 2mm 0;
    }}
    .page:last-child {{ page-break-after: auto; }}
    .page--cover {{
      display: block;
      padding-top: 0;
    }}
    .hero-card, .panel {{
      background: #fbfaf7;
      border: 1px solid rgba(24, 34, 44, 0.12);
      border-radius: 15px;
      padding: 12px 14px;
      page-break-inside: avoid;
    }}
    .hero-card--cover {{
      padding: 18px 20px 20px;
      text-align: center;
    }}
    .cover-header {{
      background: #f7f0e2;
      border-radius: 18px;
      padding: 9px 14px 10px;
      border: 1px solid rgba(24, 34, 44, 0.08);
    }}
    .cover-header .subtle {{
      font-size: 9.4px;
    }}
    .cover-hero {{
      text-align: center;
      padding: 14mm 10mm 0;
    }}
    .cover-eyebrow {{
      color: #63707c;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-weight: 600;
    }}
    .hero-value {{
      font-size: 108px;
      line-height: 1;
      margin-top: 12px;
      font-family: "DejaVu Serif", Georgia, serif;
      white-space: nowrap;
    }}
    .subtle {{
      color: #56616c;
      font-size: 9.4px;
    }}
    .hero-summary-line {{
      margin-top: 16px;
      color: #314252;
      font-size: 16px;
      line-height: 1.32;
      text-align: center;
    }}
    .cover-facts {{
      margin-top: 14mm;
      padding-top: 12px;
      border-top: 1px solid rgba(24, 34, 44, 0.08);
      width: 100%;
      overflow: hidden;
    }}
    .cover-title {{
      color: #18222c;
      font-size: 20px;
      font-weight: 700;
      margin-bottom: 16px;
    }}
    .cover-fact-groups {{
      display: grid;
      gap: 14px;
      width: 100%;
    }}
    .fact-pair-group {{
      width: 100%;
    }}
    .fact-pair-title {{
      color: #4d5b68;
      font-size: 14px;
      font-weight: 700;
      text-transform: none;
      letter-spacing: 0.01em;
      line-height: 1.2;
      margin-bottom: 9px;
    }}
    .fact-grid {{
      display: grid;
      gap: 12px;
      margin-top: 4px;
      width: 100%;
      min-width: 0;
    }}
    .fact-grid--2 {{
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }}
    .fact-grid--3 {{
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }}
    .fact-card {{
      padding: 18px 20px;
      border-radius: 14px;
      background: #f2f7f8;
      border: 1px solid rgba(31, 111, 139, 0.12);
      min-height: 132px;
      overflow: hidden;
      min-width: 0;
    }}
    .fact-label {{
      color: #546270;
      font-size: 15px;
      text-transform: none;
      letter-spacing: 0.01em;
      line-height: 1.18;
      font-weight: 700;
    }}
    .fact-value {{
      margin-top: 8px;
      min-width: 0;
    }}
    .fact-meta {{
      color: #6a737c;
      font-size: 13px;
      line-height: 1.24;
      margin-bottom: 10px;
    }}
    .fact-amount {{
      display: block;
      font-size: 28px;
      font-weight: 700;
      line-height: 1.12;
      letter-spacing: -0.01em;
      color: #18222c;
      white-space: nowrap;
    }}
    .fact-amount--muted {{
      color: #7b8490;
    }}
    .cover-facts .fact-grid {{
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }}
    .cover-facts .fact-pair-title,
    .cover-facts .fact-card,
    .cover-facts .fact-label,
    .cover-facts .fact-meta,
    .cover-facts .fact-amount {{
      text-align: center;
    }}
    .cover-facts .fact-value {{
      display: flex;
      min-height: 100%;
      flex-direction: column;
      justify-content: center;
      align-items: center;
    }}
    .cover-facts .fact-amount {{
      max-width: 100%;
      white-space: nowrap;
      overflow-wrap: normal;
    }}
    .two-col {{
      display: grid;
      grid-template-columns: 1.16fr 0.84fr;
      gap: 12px;
      margin-top: 12px;
    }}
    .page-subtitle {{
      margin: 2px 0 0;
      color: #6a737c;
      font-size: 10px;
      line-height: 1.35;
    }}
    .cover-grid {{
      align-items: start;
      grid-template-columns: 0.92fr 1.08fr;
    }}
    .cover-grid .panel {{
      align-self: start;
    }}
    .equal-col {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }}
    .flow-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }}
    .panel-section + .panel-section {{
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid rgba(24, 34, 44, 0.1);
    }}
    .chart {{
      width: 100%;
      height: auto;
      border-radius: 10px;
      display: block;
      margin-top: 4px;
    }}
    .chart-stage {{
      margin-top: 4px;
      width: 100%;
      page-break-inside: avoid;
    }}
    .chart-stage--empty {{
      padding: 14px 16px;
      border: 1px solid rgba(24, 34, 44, 0.08);
      border-radius: 14px;
      background: #fbfaf7;
    }}
    .chart-stage-title {{
      color: #5e6975;
      font-size: 9.6px;
      margin-bottom: 4px;
    }}
    .chart--stage {{
      margin-top: 0;
      border-radius: 14px;
    }}
    .chart-caption {{
      margin-top: 5px;
      color: #6a737c;
      font-size: 8.8px;
      line-height: 1.34;
    }}
    .page-two-facts {{
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px solid rgba(24, 34, 44, 0.08);
      width: 100%;
    }}
    .page-two-facts .cover-title {{
      margin-bottom: 10px;
      font-size: 15px;
    }}
    .page-two-facts .cover-fact-groups {{
      gap: 8px;
    }}
    .page-two-facts .fact-pair-title {{
      font-size: 11.5px;
      margin-bottom: 5px;
    }}
    .page-two-facts .fact-grid {{
      gap: 8px;
    }}
    .page-two-facts .fact-pair-title,
    .page-two-facts .fact-card,
    .page-two-facts .fact-label,
    .page-two-facts .fact-meta,
    .page-two-facts .fact-amount {{
      text-align: center;
    }}
    .page-two-facts .fact-value {{
      display: flex;
      min-height: 100%;
      flex-direction: column;
      justify-content: center;
      align-items: center;
    }}
    .page-two-facts .fact-card {{
      min-height: 94px;
      padding: 12px 14px;
      border-radius: 12px;
    }}
    .page-two-facts .fact-label {{
      font-size: 13px;
      line-height: 1.14;
    }}
    .page-two-facts .fact-meta {{
      font-size: 11px;
      margin-bottom: 6px;
    }}
    .page-two-facts .fact-amount {{
      font-size: 24px;
      line-height: 1.08;
    }}
    .page--structure .page-subtitle {{
      margin-bottom: 10px;
    }}
    .structure-top {{
      display: grid;
      grid-template-columns: 0.92fr 1.08fr;
      gap: 18px;
      align-items: center;
      margin-top: 8px;
    }}
    .structure-visual {{
      display: flex;
      justify-content: center;
      align-items: center;
      min-height: 250px;
    }}
    .structure-chart {{
      margin: 0;
      max-width: 300px;
      width: 100%;
    }}
    .structure-summary h3,
    .structure-section h3 {{
      margin: 0 0 10px;
      font-size: 16px;
      line-height: 1.15;
    }}
    .structure-ledger {{
      display: flex;
      flex-direction: column;
      gap: 3px;
    }}
    .structure-line {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto auto;
      gap: 10px;
      align-items: center;
      padding: 7px 0;
      border-bottom: 1px solid rgba(24, 34, 44, 0.08);
    }}
    .structure-line:last-child {{
      border-bottom: 0;
    }}
    .structure-line-main,
    .target-row-label {{
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }}
    .structure-line-label {{
      font-size: 11.2px;
      line-height: 1.24;
      color: #18222c;
      font-weight: 600;
    }}
    .structure-line-amount,
    .structure-line-share {{
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }}
    .structure-line-amount {{
      font-size: 11.2px;
      font-weight: 600;
      color: #18222c;
    }}
    .structure-line-share {{
      font-size: 10.4px;
      color: #65707b;
      text-align: right;
    }}
    .structure-swatch {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      flex: 0 0 auto;
    }}
    .structure-swatch--stocks {{
      background: #0e7c95;
    }}
    .structure-swatch--bonds {{
      background: #c79a4a;
    }}
    .structure-swatch--etf {{
      background: #5b67c8;
    }}
    .structure-swatch--currency {{
      background: #2f7a4a;
    }}
    .structure-swatch--other {{
      background: #8a9198;
    }}
    .structure-section {{
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px solid rgba(24, 34, 44, 0.08);
    }}
    .target-drift-grid {{
      display: flex;
      flex-direction: column;
      gap: 11px;
    }}
    .target-row-top {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto auto auto;
      gap: 10px;
      align-items: center;
    }}
    .target-row-label {{
      font-size: 11px;
      font-weight: 600;
      color: #18222c;
    }}
    .target-row-current,
    .target-row-target,
    .target-row-delta {{
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }}
    .target-row-current {{
      font-size: 10.8px;
      color: #18222c;
      font-weight: 600;
    }}
    .target-row-target {{
      font-size: 9.7px;
      color: #65707b;
    }}
    .target-row-delta {{
      font-size: 10.2px;
      text-align: right;
      font-weight: 700;
      color: #4d5a68;
    }}
    .structure-delta--positive {{
      color: #1c6a82;
    }}
    .structure-delta--negative {{
      color: #8d4a34;
    }}
    .structure-delta--warn {{
      color: #c47b10;
    }}
    .structure-delta--neutral {{
      color: #4d5a68;
    }}
    .target-track {{
      position: relative;
      margin-top: 6px;
      height: 10px;
      border-radius: 999px;
      background: #edf1f3;
      overflow: hidden;
    }}
    .target-fill {{
      position: absolute;
      inset: 0 auto 0 0;
      border-radius: 999px;
      opacity: 0.3;
    }}
    .target-fill--stocks {{
      background: #0e7c95;
    }}
    .target-fill--bonds {{
      background: #c79a4a;
    }}
    .target-fill--etf {{
      background: #5b67c8;
    }}
    .target-fill--currency {{
      background: #2f7a4a;
    }}
    .target-fill--other {{
      background: #8a9198;
    }}
    .target-marker {{
      position: absolute;
      top: -1px;
      bottom: -1px;
      width: 2px;
      margin-left: -1px;
      border-radius: 999px;
      background: #18222c;
      opacity: 0.9;
    }}
    .report-table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 4px;
    }}
    .report-table th {{
      text-align: left;
      color: #65707b;
      font-size: 9px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      padding-bottom: 8px;
      padding-right: 8px;
      border-bottom: 1px solid rgba(24, 34, 44, 0.1);
    }}
    .report-table td {{
      padding: 7px 0;
      padding-right: 8px;
      border-bottom: 1px solid rgba(24, 34, 44, 0.07);
      vertical-align: top;
    }}
    .report-table th:last-child,
    .report-table td:last-child {{
      padding-right: 0;
    }}
    .report-table .numeric {{
      text-align: right;
      white-space: nowrap;
    }}
    .report-table .status-cell {{
      text-align: center;
      width: 32px;
      white-space: nowrap;
    }}
    .nowrap, .metric-inline, .num {{
      white-space: nowrap;
    }}
    .num {{
      font-variant-numeric: tabular-nums;
    }}
    .asset-cell {{
      display: flex;
      flex-direction: column;
      gap: 2px;
      min-width: 0;
    }}
    .asset-ticker {{
      font-weight: 700;
      color: #18222c;
    }}
    .asset-name, .cell-muted {{
      color: #65707b;
      font-size: 9.2px;
      line-height: 1.28;
    }}
    .status-dot {{
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      vertical-align: middle;
    }}
    .status-dot--ok {{
      background: #2f7a4a;
    }}
    .status-dot--warn {{
      background: #c47b10;
    }}
    .status-dot--neutral {{
      background: #8a9198;
    }}
    ul {{
      margin: 0;
      padding-left: 18px;
    }}
    li {{
      margin-bottom: 4px;
    }}
    li:last-child {{
      margin-bottom: 0;
    }}
    .empty {{
      color: #7b8490;
      margin: 0;
    }}
    .footer {{
      margin-top: auto;
      padding-top: 10px;
      color: #6a737c;
      font-size: 9px;
    }}
  </style>
</head>
<body>
  <section class="page page--cover">
    <div class="cover-header">
      <div class="subtle">{escape(meta['account_friendly_name'])}</div>
      <h1>{escape(report_title)}</h1>
      <div class="subtle">Период: {_display_date(meta['period_start'])} — {_display_date(meta['period_end'])} • Сформировано: {escape(_display_timestamp(meta['generated_at_utc']))}</div>
    </div>
    <div class="cover-hero">
      <div class="cover-eyebrow">Стоимость портфеля на конец периода</div>
      <div class="hero-value">{escape(_display_rub(summary.get('current_value'), precision=0))}</div>
      <div class="hero-summary-line">{escape(summary_subline)}</div>
    </div>
    <div class="cover-facts">
      <div class="cover-title">Факты месяца</div>
      <div class="cover-fact-groups">
        {''.join(_render_fact_group(title, items) for title, items in page_one_fact_groups)}
      </div>
    </div>
  </section>

  <section class="page">
    <h2>Динамика за месяц</h2>
    {_render_chart_stage(
        "Стоимость портфеля и дневной результат",
        charts.get("performance"),
        caption="Сверху — стоимость на конец дня, снизу — дневной результат по торговым дням.",
    )}
    <div class="page-two-facts">
      <div class="cover-title">Ритм месяца</div>
      <div class="cover-fact-groups">
        {''.join(_render_fact_group(title, items) for title, items in page_two_fact_groups)}
      </div>
    </div>
  </section>

  <section class="page page--structure">
    <h2>Структура портфеля</h2>
    <p class="page-subtitle">Срез на конец месяца</p>
    <div class="structure-top">
      <div class="structure-visual">
        {_render_visual_chart(
          charts.get("allocation"),
          alt="Структура портфеля по классам активов",
          empty_label="Недостаточно данных для графика структуры.",
        )}
      </div>
      <div class="structure-summary">
        <h3>Классы активов</h3>
        {_render_structure_summary(asset_class_summary_rows)}
      </div>
    </div>
    <div class="structure-section">
      <h3>Отклонение от цели</h3>
      {_render_target_drift(target_drift_rows)}
    </div>
    <div class="structure-section">
      <h3>Крупнейшие позиции</h3>
      {_render_rows_table(
        ["Актив", "Стоимость", "Вес", "Изм. доли", "Нереализованный результат"],
        top_positions_rows,
        column_classes=["", "numeric", "numeric", "numeric", "numeric"],
      )}
    </div>
  </section>

  <section class="page">
    <h2>Инструменты за месяц</h2>
    <div class="two-col">
      {_render_image_block("Открытый результат на конец месяца", charts.get("open_pl"))}
      <div class="panel">
        <h3>Ключевые выводы по инструментам</h3>
        {_render_bullet_list(narrative.get("instrument_takeaways", []))}
      </div>
    </div>
    <div class="flow-grid">
      {''.join(flow_panels)}
    </div>
    <div class="equal-col">
      <div class="panel">
        <h3>Лидеры и просадки месяца</h3>
        <div class="panel-section">
          <h4>Сильнейший рост</h4>
          {_render_rows_table(["Актив", "Рост", "Окно"], movers_rows, column_classes=["", "numeric", "nowrap"])}
        </div>
        <div class="panel-section">
          <h4>Сильнейшая просадка</h4>
          {_render_rows_table(["Актив", "Просадка", "Окно"], drawdown_rows, column_classes=["", "numeric", "nowrap"])}
        </div>
      </div>
      <div class="panel">
        <h3>Что внесло вклад в результат</h3>
        <div class="panel-section">
          <h4>Реализованная прибыль</h4>
          {_render_rows_table(["Актив", "Сумма"], realized_profit_rows, empty_label="Реализованной прибыли не было.", column_classes=["", "numeric"])}
        </div>
        <div class="panel-section">
          <h4>Реализованный убыток</h4>
          {_render_rows_table(["Актив", "Сумма"], realized_loss_rows, empty_label="Реализованных убытков не было.", column_classes=["", "numeric"])}
        </div>
        <div class="panel-section">
          <h4>Дивиденды и купоны</h4>
          {_render_rows_table(["Актив", "Тип", "Сумма"], income_by_asset_rows, empty_label="Доходов по инструментам не было.", column_classes=["", "", "numeric"])}
        </div>
      </div>
    </div>
  </section>

  <section class="page">
    <h2>Операции, доходы и качество</h2>
    <div class="two-col">
      <div class="panel">
        <h3>Крупнейшие операции месяца</h3>
        {_render_rows_table(["Дата", "Группа", "Актив", "Сумма"], operations_rows, column_classes=["nowrap", "", "", "numeric"])}
      </div>
      <div class="panel">
        <h3>Доходные события</h3>
        {_render_rows_table(["Дата", "Тип", "Актив", "Сумма"], income_rows, empty_label="Доходных событий за месяц не было.", column_classes=["nowrap", "", "", "numeric"])}
      </div>
    </div>
    <div class="two-col">
      <div class="panel">
        <h3>Примечания по денежному потоку</h3>
        {_render_bullet_list(narrative.get("cashflow_notes", []))}
        <h3 style="margin-top: 12px;">Оговорки по качеству данных</h3>
        {_render_bullet_list(narrative.get("quality_notes", []), empty_label="Замечаний по качеству данных нет.")}
      </div>
      <div class="panel">
        <h3>Качество данных</h3>
        {_render_rows_table(["Флаг", "Значение"], quality_rows, column_classes=["", "numeric"])}
        <h3 style="margin-top: 12px;">Риск-сигналы</h3>
        {_render_bullet_list(narrative.get("risk_notes", []), empty_label="Явных риск-сигналов не выделено.")}
      </div>
    </div>
    <div class="footer">
      Таймзона: {escape(meta['timezone'])} • Валюта: {escape(meta['currency'])} •
      Снапшотов: {escape(str(meta.get('source_snapshot_count') or 0))}
    </div>
  </section>
</body>
</html>"""
    logger.info(
        "monthly_report_html_built",
        "Built deterministic monthly report HTML.",
        {
            "period": f"{meta['period_year']}-{meta['period_month']:02d}",
            "has_performance_chart": charts.get("performance") is not None,
            "has_allocation_chart": charts.get("allocation") is not None,
            "has_open_pl_chart": charts.get("open_pl") is not None,
        },
    )
    return html
