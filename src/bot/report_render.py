from __future__ import annotations

import base64
import io
import os
import re
import tempfile
from datetime import date, datetime
from decimal import Decimal
from html import escape
from typing import Any, Callable

import matplotlib.pyplot as plt

from charts import (
    CHART_COLORS,
    annotate_bar_values,
    annotate_series_last_point,
    apply_chart_style,
    build_date_ticks,
    rub_axis_formatter,
    set_value_axis_limits,
    set_chart_header,
)
from common.logging_setup import get_logger
from report_payload import create_monthly_report_payload
from runtime import fmt_decimal_rub, fmt_pct
from services import (
    REBALANCE_ASSET_CLASSES,
    REBALANCE_CLASS_LABELS,
    aggregate_rebalance_values_by_class,
)


REPORT_DEBUG_SAVE_HTML = os.getenv("REPORT_DEBUG_SAVE_HTML", "false").strip().lower() in {"1", "true", "yes", "on"}

logger = get_logger(__name__)

_LATIN_WORD_RE = re.compile(r"[A-Za-z]{3,}")


class ReportRenderError(RuntimeError):
    pass


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _display_rub(value: Any, *, precision: int = 0) -> str:
    if value is None:
        return "—"
    return fmt_decimal_rub(value, precision=precision)


def _display_pct(value: Any, *, precision: int = 2) -> str:
    if value is None:
        return "—"
    return fmt_pct(float(_to_decimal(value)), precision=precision)


def _display_pct_compact(value: Any, *, precision: int = 1) -> str:
    if value is None:
        return "—"
    quantizer = Decimal("1") if precision == 0 else Decimal(f"1.{'0' * precision}")
    decimal_value = _to_decimal(value).quantize(quantizer)
    return f"{format(decimal_value, f'.{precision}f').replace('.', ',')}%"


def _display_date(value: str | None) -> str:
    if not value:
        return "—"
    return datetime.fromisoformat(value).strftime("%d.%m.%Y")


def _display_day(value: str | None) -> str:
    if not value:
        return "—"
    return datetime.fromisoformat(value).strftime("%d.%m")


def _display_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "—"
    if isinstance(value, datetime):
        dt_value = value
    else:
        dt_value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt_value.tzinfo is not None:
        return dt_value.strftime("%d.%m.%Y %H:%M %Z").strip()
    return dt_value.strftime("%d.%m.%Y %H:%M")


def _report_title_default(payload: dict[str, Any]) -> str:
    return f"{payload['meta']['period_label_ru'].capitalize()}: обзор портфеля"


def _resolve_report_title(payload: dict[str, Any], narrative: dict[str, Any]) -> str:
    title = str(narrative.get("report_title") or "").strip()
    if not title:
        return _report_title_default(payload)
    lower_title = title.lower()
    if "monthly review" in lower_title or "executive summary" in lower_title:
        return _report_title_default(payload)
    if _LATIN_WORD_RE.search(title) and not re.search(r"[А-Яа-яЁё]", title):
        return _report_title_default(payload)
    return title


def _render_nowrap(value: str, *, extra_class: str = "") -> str:
    classes = "nowrap"
    if extra_class:
        classes = f"{classes} {extra_class}"
    return f'<span class="{classes}">{escape(value)}</span>'


def _render_metric_value(value: str) -> str:
    return _render_nowrap(value, extra_class="metric-inline")


def _render_num_cell(value: str) -> str:
    return _render_nowrap(value, extra_class="num")


def _render_muted(value: str) -> str:
    return f'<span class="cell-muted">{escape(value)}</span>'


def _render_asset_cell(ticker: str | None, name: str | None) -> str:
    ticker_text = (ticker or "").strip()
    name_text = (name or "").strip()
    if not ticker_text and not name_text:
        return _render_muted("—")

    parts: list[str] = ['<div class="asset-cell">']
    if ticker_text:
        parts.append(f'<div class="asset-ticker nowrap">{escape(ticker_text)}</div>')
    if name_text and name_text != ticker_text:
        parts.append(f'<div class="asset-name">{escape(name_text)}</div>')
    parts.append("</div>")
    return "".join(parts)


def _render_status_dot(status: str | None) -> str:
    status_text = (status or "нет данных").strip() or "нет данных"
    normalized = status_text.lower()
    if normalized == "в норме":
        tone = "ok"
    elif normalized == "вне нормы":
        tone = "warn"
    else:
        tone = "neutral"
    return (
        f'<span class="status-dot status-dot--{tone}" '
        f'title="{escape(status_text)}" aria-label="{escape(status_text)}"></span>'
    )


def _render_fact_card(label: str, value: str) -> str:
    return (
        '<div class="fact-card">'
        f'<div class="fact-label">{escape(label)}</div>'
        f'<div class="fact-value">{value}</div>'
        "</div>"
    )


def _render_fact_grid(items: list[tuple[str, str]], *, columns: int = 2) -> str:
    if not items:
        return '<p class="empty">Нет данных.</p>'
    cards = "".join(_render_fact_card(label, value) for label, value in items)
    return f'<div class="fact-grid fact-grid--{columns}">{cards}</div>'


def _share_text(count: int, total: int) -> str:
    if total <= 0:
        return f"{count} из {total} (0%)"
    share_pct = (Decimal(count) * Decimal("100") / Decimal(total)).quantize(Decimal("1"))
    return f"{count} из {total} ({share_pct}%)"


def _classify_day_pnl_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    relevant_rows = rows[1:] if len(rows) > 1 else rows
    counts = {"positive": 0, "negative": 0, "neutral": 0}
    for row in relevant_rows:
        value = _to_decimal(row.get("day_pnl"))
        if value > 0:
            counts["positive"] += 1
        elif value < 0:
            counts["negative"] += 1
        else:
            counts["neutral"] += 1
    total = len(relevant_rows)
    return {
        key: {
            "count": value,
            "total": total,
        }
        for key, value in counts.items()
    }


def _build_weight_transition_map(
    start_positions: list[dict[str, Any]],
    current_positions: list[dict[str, Any]],
) -> dict[str, str]:
    start_weights = {
        str(row.get("logical_asset_id") or ""): _to_decimal(row.get("weight_pct"))
        for row in start_positions
        if row.get("logical_asset_id")
    }
    transitions: dict[str, str] = {}
    for row in current_positions:
        logical_asset_id = str(row.get("logical_asset_id") or "")
        if not logical_asset_id:
            continue
        end_weight = _to_decimal(row.get("weight_pct"))
        start_weight = start_weights.get(logical_asset_id, Decimal("0"))
        transitions[logical_asset_id] = (
            f"{_display_pct_compact(start_weight, precision=1)} "
            f"→ {_display_pct_compact(end_weight, precision=1)}"
        )
    return transitions


def _build_asset_class_breakdown(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    class_values, other_groups = aggregate_rebalance_values_by_class(positions)
    rows: list[dict[str, Any]] = []
    for asset_class in REBALANCE_ASSET_CLASSES:
        value = class_values.get(asset_class, Decimal("0"))
        if value > 0:
            rows.append(
                {
                    "key": asset_class,
                    "label": REBALANCE_CLASS_LABELS[asset_class],
                    "value": value,
                }
            )

    other_total = sum(other_groups.values(), Decimal("0"))
    if other_total > 0:
        rows.append(
            {
                "key": "other",
                "label": "Другое",
                "value": other_total,
            }
        )
    return rows


def _annotate_point(
    ax,
    x_value,
    y_value: float,
    label: str,
    color: str,
    *,
    x_offset: int,
    y_offset: int,
) -> None:
    ax.scatter([x_value], [y_value], color=color, s=22, zorder=5)
    ax.annotate(
        label,
        xy=(x_value, y_value),
        xytext=(x_offset, y_offset),
        textcoords="offset points",
        ha="left" if x_offset >= 0 else "right",
        va="center",
        fontsize=8,
        color=CHART_COLORS["text"],
        bbox={
            "boxstyle": "round,pad=0.26",
            "fc": "white",
            "ec": color,
            "lw": 1,
            "alpha": 0.95,
        },
        clip_on=False,
        zorder=6,
    )


def _chart_to_data_uri(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=170, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _build_performance_chart(payload: dict[str, Any]) -> str | None:
    rows = payload.get("timeseries_daily") or []
    if len(rows) < 2:
        return None

    dates = [date.fromisoformat(row["date"]) for row in rows]
    portfolio_values = [float(_to_decimal(row.get("portfolio_value"))) for row in rows]
    day_pnl = [float(_to_decimal(row.get("day_pnl"))) for row in rows]

    fig, (ax_value, ax_flow) = plt.subplots(
        2,
        1,
        figsize=(10.5, 6.0),
        sharex=True,
        gridspec_kw={"height_ratios": [2.1, 1.4]},
    )
    set_chart_header(
        fig,
        "Динамика портфеля за месяц",
        "Сверху — стоимость на конец дня, снизу — дневной результат по торговым дням.",
    )
    apply_chart_style(ax_value, rub_axis_formatter)
    apply_chart_style(ax_flow, rub_axis_formatter)

    ax_value.fill_between(dates, portfolio_values, color=CHART_COLORS["portfolio_fill"], alpha=0.72, zorder=1)
    ax_value.plot(dates, portfolio_values, color=CHART_COLORS["portfolio"], linewidth=2.6, zorder=3)
    ax_value.set_ylabel("Стоимость")
    ax_value.margins(x=0.03, y=0.08)
    set_value_axis_limits(ax_value, portfolio_values, min_padding_ratio=0.11, flat_padding_ratio=0.02)
    annotate_series_last_point(
        ax_value,
        dates,
        portfolio_values,
        f"{_display_rub(rows[-1].get('portfolio_value'), precision=0)}",
        CHART_COLORS["portfolio"],
        y_offset=12,
    )
    peak_index = max(range(len(portfolio_values)), key=portfolio_values.__getitem__)
    trough_index = min(range(len(portfolio_values)), key=portfolio_values.__getitem__)
    if peak_index != len(portfolio_values) - 1:
        _annotate_point(
            ax_value,
            dates[peak_index],
            portfolio_values[peak_index],
            f"Пик {_display_rub(rows[peak_index].get('portfolio_value'), precision=0)}",
            CHART_COLORS["positive"],
            x_offset=-12,
            y_offset=-16,
        )
    if trough_index != len(portfolio_values) - 1 and trough_index != peak_index:
        _annotate_point(
            ax_value,
            dates[trough_index],
            portfolio_values[trough_index],
            f"Минимум {_display_rub(rows[trough_index].get('portfolio_value'), precision=0)}",
            CHART_COLORS["negative"],
            x_offset=14,
            y_offset=16,
        )

    bar_colors = [
        CHART_COLORS["positive"] if value > 0 else CHART_COLORS["negative"] if value < 0 else CHART_COLORS["neutral"]
        for value in day_pnl
    ]
    ax_flow.bar(dates, day_pnl, width=0.85, color=bar_colors, alpha=0.72, zorder=2)
    ax_flow.axhline(0, color=CHART_COLORS["spine"], linewidth=1, zorder=1)
    ax_flow.set_ylabel("Дневной результат")
    ax_flow.margins(x=0.03, y=0.12)
    set_value_axis_limits(ax_flow, day_pnl, min_padding_ratio=0.18, flat_padding_ratio=0.08)
    best_day_index = max(range(len(day_pnl)), key=day_pnl.__getitem__)
    worst_day_index = min(range(len(day_pnl)), key=day_pnl.__getitem__)
    if best_day_index != len(day_pnl) - 1:
        _annotate_point(
            ax_flow,
            dates[best_day_index],
            day_pnl[best_day_index],
            f"Лучший день {_display_rub(rows[best_day_index].get('day_pnl'), precision=0)}",
            CHART_COLORS["positive"],
            x_offset=-14,
            y_offset=16,
        )
    if worst_day_index != len(day_pnl) - 1 and worst_day_index != best_day_index:
        _annotate_point(
            ax_flow,
            dates[worst_day_index],
            day_pnl[worst_day_index],
            f"Просадка {_display_rub(rows[worst_day_index].get('day_pnl'), precision=0)}",
            CHART_COLORS["negative"],
            x_offset=14,
            y_offset=-16,
        )

    tick_dates, tick_labels = build_date_ticks(dates, max_ticks=7)
    ax_flow.set_xticks(tick_dates)
    ax_flow.set_xticklabels(tick_labels)

    fig.tight_layout(rect=(0, 0, 1, 0.9), h_pad=1.1)
    return _chart_to_data_uri(fig)


def _build_allocation_chart(payload: dict[str, Any]) -> str | None:
    breakdown = _build_asset_class_breakdown(payload.get("positions_current") or [])
    if not breakdown:
        return None

    labels = [row["label"] for row in breakdown]
    values = [float(row["value"]) for row in breakdown]
    colors_by_key = {
        "stocks": CHART_COLORS["portfolio"],
        "bonds": CHART_COLORS["deposits"],
        "etf": CHART_COLORS["twr"],
        "currency": CHART_COLORS["positive"],
        "other": CHART_COLORS["neutral"],
    }
    colors = [colors_by_key.get(str(row["key"]), CHART_COLORS["neutral"]) for row in breakdown]

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    set_chart_header(
        fig,
        "Структура портфеля по классам активов",
        "Распределение текущей стоимости по основным классам активов.",
    )
    ax.set_facecolor("white")
    ax.pie(
        values,
        colors=colors,
        startangle=90,
        counterclock=False,
        wedgeprops={"width": 0.42, "edgecolor": "white", "linewidth": 2},
        autopct=lambda pct: f"{pct:.0f}%" if pct >= 4 else "",
        pctdistance=0.78,
        textprops={"fontsize": 8, "color": CHART_COLORS["text"]},
    )
    ax.text(0, 0, "Классы\nактивов", ha="center", va="center", fontsize=10, color=CHART_COLORS["muted"])
    ax.legend(
        labels,
        loc="center left",
        bbox_to_anchor=(1.0, 0.5),
        frameon=False,
        fontsize=8,
    )
    ax.set_aspect("equal")
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    return _chart_to_data_uri(fig)


def _build_open_pl_chart(payload: dict[str, Any]) -> str | None:
    rows = payload.get("open_pl_end") or []
    if not rows:
        return None

    chart_rows = rows[:6]
    labels = [row.get("ticker") or row.get("name") or "—" for row in chart_rows]
    amounts = [float(_to_decimal(row.get("amount"))) for row in chart_rows]
    x_values = list(range(len(chart_rows)))

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    set_chart_header(
        fig,
        "Открытый результат на конец месяца",
        "Позиции с наибольшим вкладом по открытому результату на конец месяца.",
    )
    apply_chart_style(ax, rub_axis_formatter)

    colors = [
        CHART_COLORS["positive"] if amount >= 0 else CHART_COLORS["negative"]
        for amount in amounts
    ]
    ax.bar(x_values, amounts, width=0.62, color=colors, edgecolor="none", zorder=3)
    ax.axhline(0, color=CHART_COLORS["spine"], linewidth=1, zorder=1)
    ax.set_xticks(x_values)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Открытый результат")
    ax.margins(x=0.05, y=0.12)
    set_value_axis_limits(ax, amounts, min_padding_ratio=0.18, flat_padding_ratio=0.08)
    annotate_bar_values(
        ax,
        x_values,
        amounts,
        lambda value: _display_rub(value, precision=0),
    )
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    return _chart_to_data_uri(fig)


def build_monthly_report_charts(payload: dict[str, Any]) -> dict[str, str | None]:
    charts = {
        "performance": _build_performance_chart(payload),
        "allocation": _build_allocation_chart(payload),
        "open_pl": _build_open_pl_chart(payload),
    }
    logger.info(
        "monthly_report_charts_built",
        "Built deterministic monthly report charts.",
        {
            "performance_chart": charts["performance"] is not None,
            "allocation_chart": charts["allocation"] is not None,
            "open_pl_chart": charts["open_pl"] is not None,
        },
    )
    return charts


def build_deterministic_monthly_narrative(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload["summary_metrics"]
    flow = payload["position_flow_groups"]
    movers = payload["instrument_movers"]
    quality = payload["data_quality"]

    executive_summary = [
        f"На конец месяца портфель оценён в {_display_rub(summary.get('current_value'), precision=0)}.",
        f"Результат периода составил {_display_rub(summary.get('period_pnl_abs'), precision=0)} "
        f"при TWR {_display_pct(summary.get('period_twr_pct'), precision=2)}.",
        f"Чистый внешний поток за месяц: {_display_rub(summary.get('net_external_flow'), precision=0)}.",
    ]

    performance_commentary = [
        f"Лучший день месяца: {_display_date(summary.get('best_day_date'))} "
        f"с результатом {_display_rub(summary.get('best_day_pnl'), precision=0)}.",
        f"Самый слабый день: {_display_date(summary.get('worst_day_date'))} "
        f"с результатом {_display_rub(summary.get('worst_day_pnl'), precision=0)}.",
    ]

    instrument_takeaways: list[str] = []
    if flow.get("new"):
        instrument_takeaways.append(
            "Новые позиции: "
            + ", ".join(row.get("ticker") or row.get("name") or "—" for row in flow["new"][:3])
            + "."
        )
    if flow.get("closed"):
        instrument_takeaways.append(
            "Полностью закрыты: "
            + ", ".join(row.get("ticker") or row.get("name") or "—" for row in flow["closed"][:3])
            + "."
        )
    if movers.get("top_growth"):
        top_growth = movers["top_growth"][0]
        instrument_takeaways.append(
            f"Сильнейший рост по внутримесячному открытому результату показал "
            f"{top_growth.get('ticker') or top_growth.get('name')} "
            f"({_display_rub(top_growth.get('rise_abs'), precision=0)})."
        )
    if movers.get("top_drawdown"):
        top_drawdown = movers["top_drawdown"][0]
        instrument_takeaways.append(
            f"Самая глубокая просадка внутри месяца пришлась на "
            f"{top_drawdown.get('ticker') or top_drawdown.get('name')} "
            f"({_display_rub(top_drawdown.get('drawdown_abs'), precision=0)})."
        )

    cashflow_notes = [
        f"Пополнения: {_display_rub(summary.get('deposits'), precision=0)}, "
        f"выводы: {_display_rub(summary.get('withdrawals'), precision=0)}.",
        f"Доходы за месяц: {_display_rub(summary.get('income_net'), precision=2)}, "
        f"комиссии: {_display_rub(summary.get('commissions'), precision=2)}, "
        f"налоги: {_display_rub(summary.get('taxes'), precision=2)}.",
    ]

    quality_notes: list[str] = []
    if _to_decimal(summary.get("reconciliation_gap_abs")) != 0:
        quality_notes.append(
            f"Расхождение между снапшотом и суммой позиций на конец месяца: {_display_rub(summary.get('reconciliation_gap_abs'), precision=0)}."
        )
    if quality.get("positions_missing_label_count"):
        quality_notes.append(
            f"Позиции без нормальной подписи: {quality.get('positions_missing_label_count')}."
        )
    if quality.get("mojibake_detected_count"):
        quality_notes.append(
            f"Подозрительные описания операций: {quality.get('mojibake_detected_count')}."
        )

    risk_notes: list[str] = []
    if _to_decimal(summary.get("top_holding_weight_pct")) >= Decimal("25"):
        risk_notes.append("Один инструмент заметно концентрирует вес портфеля.")
    if not quality.get("has_full_history_from_zero"):
        risk_notes.append("Отчёт нельзя трактовать как полную историю портфеля с нулевой базы.")
    if not quality.get("has_rebalance_targets"):
        risk_notes.append("Таргеты аллокации не заданы, поэтому блок по ребалансу носит справочный характер.")

    return {
        "schema_version": "monthly_fallback_narrative.v1",
        "report_title": _report_title_default(payload),
        "executive_summary": executive_summary[:4],
        "performance_commentary": performance_commentary[:4],
        "instrument_takeaways": instrument_takeaways[:5],
        "cashflow_notes": cashflow_notes[:3],
        "quality_notes": quality_notes[:3],
        "risk_notes": risk_notes[:4],
        "warnings": [],
    }


def _render_bullet_list(items: list[str], empty_label: str = "Нет данных.") -> str:
    if not items:
        return f'<p class="empty">{escape(empty_label)}</p>'
    bullets = "".join(f"<li>{escape(item)}</li>" for item in items)
    return f"<ul>{bullets}</ul>"


def _render_metric(label: str, value: str) -> str:
    return (
        '<div class="metric">'
        f'<div class="metric-label">{escape(label)}</div>'
        f'<div class="metric-value">{_render_metric_value(value)}</div>'
        "</div>"
    )


def _render_rows_table(
    headers: list[str],
    rows: list[list[str]],
    empty_label: str = "Нет данных.",
    *,
    column_classes: list[str] | None = None,
) -> str:
    if not rows:
        return f'<p class="empty">{escape(empty_label)}</p>'
    column_classes = column_classes or ["" for _ in headers]
    normalized_classes = list(column_classes) + [""] * max(0, len(headers) - len(column_classes))
    header_html = "".join(
        f'<th class="{escape(normalized_classes[idx])}">{escape(header)}</th>' if normalized_classes[idx] else f"<th>{escape(header)}</th>"
        for idx, header in enumerate(headers)
    )
    body_parts: list[str] = []
    for row in rows:
        cells: list[str] = []
        for idx, cell in enumerate(row):
            cell_class = normalized_classes[idx] if idx < len(normalized_classes) else ""
            if cell_class:
                cells.append(f'<td class="{escape(cell_class)}">{cell}</td>')
            else:
                cells.append(f"<td>{cell}</td>")
        body_parts.append("<tr>" + "".join(cells) + "</tr>")
    body_html = "".join(body_parts)
    return (
        '<table class="report-table">'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{body_html}</tbody>"
        "</table>"
    )


def _render_image_block(title: str, data_uri: str | None) -> str:
    if not data_uri:
        return (
            '<div class="panel">'
            f'<h3>{escape(title)}</h3>'
            '<p class="empty">Недостаточно данных для графика.</p>'
            "</div>"
        )
    return (
        '<div class="panel">'
        f'<h3>{escape(title)}</h3>'
        f'<img class="chart" src="{data_uri}" alt="{escape(title)}">'
        "</div>"
    )


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
    weight_transitions = _build_weight_transition_map(
        payload.get("positions_month_start") or [],
        payload.get("positions_current") or [],
    )

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
    rebalance_rows = [
        [
            escape(row.get("label") or "—"),
            _render_num_cell(_display_pct_compact(row.get("current_pct"), precision=1)),
            _render_num_cell(_display_pct_compact(row.get("target_pct"), precision=1)),
            _render_num_cell(_display_rub(row.get("delta_value"), precision=0)),
            _render_status_dot(row.get("status")),
        ]
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

    page_one_facts = [
        ("Лучший день", _render_nowrap(f"{_display_day(summary.get('best_day_date'))} • {_display_rub(summary.get('best_day_pnl'), precision=0)}")),
        ("Худший день", _render_nowrap(f"{_display_day(summary.get('worst_day_date'))} • {_display_rub(summary.get('worst_day_pnl'), precision=0)}")),
        (
            "Пик месяца",
            _render_nowrap(
                f"{_display_day(peak_row.get('date'))} • {_display_rub(peak_row.get('portfolio_value'), precision=0)}"
            ) if peak_row else _render_muted("—"),
        ),
        (
            "Минимум месяца",
            _render_nowrap(
                f"{_display_day(trough_row.get('date'))} • {_display_rub(trough_row.get('portfolio_value'), precision=0)}"
            ) if trough_row else _render_muted("—"),
        ),
        ("Пополнения", _render_num_cell(_display_rub(summary.get("deposits"), precision=0))),
        ("Доходы за месяц", _render_num_cell(_display_rub(summary.get("income_net"), precision=2))),
    ]
    page_two_facts = [
        ("Лучший день", _render_nowrap(f"{_display_day(summary.get('best_day_date'))} • {_display_rub(summary.get('best_day_pnl'), precision=0)}")),
        ("Худший день", _render_nowrap(f"{_display_day(summary.get('worst_day_date'))} • {_display_rub(summary.get('worst_day_pnl'), precision=0)}")),
        (
            "Пик месяца",
            _render_nowrap(
                f"{_display_day(peak_row.get('date'))} • {_display_rub(peak_row.get('portfolio_value'), precision=0)}"
            ) if peak_row else _render_muted("—"),
        ),
        (
            "Минимум месяца",
            _render_nowrap(
                f"{_display_day(trough_row.get('date'))} • {_display_rub(trough_row.get('portfolio_value'), precision=0)}"
            ) if trough_row else _render_muted("—"),
        ),
        ("Положительных дней", _render_nowrap(_share_text(day_stats["positive"]["count"], classified_days_total))),
        ("Отрицательных дней", _render_nowrap(_share_text(day_stats["negative"]["count"], classified_days_total))),
        ("Нейтральных дней", _render_nowrap(_share_text(day_stats["neutral"]["count"], classified_days_total))),
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
      background: #f7f2e8;
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
    .hero-card, .panel {{
      background: rgba(255, 255, 255, 0.78);
      border: 1px solid rgba(24, 34, 44, 0.12);
      border-radius: 15px;
      padding: 12px 14px;
      page-break-inside: avoid;
    }}
    .hero-card--cover {{
      padding: 18px 20px 20px;
      text-align: center;
    }}
    .hero-value {{
      font-size: 62px;
      line-height: 1;
      margin-top: 12px;
      font-family: "DejaVu Serif", Georgia, serif;
    }}
    .subtle {{
      color: #56616c;
      font-size: 9.4px;
    }}
    .hero-summary-line {{
      margin-top: 14px;
      color: #314252;
      font-size: 11.6px;
      line-height: 1.35;
      text-align: center;
    }}
    .fact-grid {{
      display: grid;
      gap: 8px;
      margin-top: 4px;
    }}
    .fact-grid--2 {{
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }}
    .fact-grid--3 {{
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }}
    .fact-card {{
      padding: 10px 11px;
      border-radius: 11px;
      background: rgba(31, 111, 139, 0.08);
      border: 1px solid rgba(31, 111, 139, 0.08);
      min-height: 58px;
    }}
    .fact-label {{
      color: #6a737c;
      font-size: 8.5px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .fact-value {{
      margin-top: 5px;
      font-size: 12px;
      font-weight: 600;
      color: #18222c;
    }}
    .two-col {{
      display: grid;
      grid-template-columns: 1.16fr 0.84fr;
      gap: 12px;
      margin-top: 12px;
    }}
    .cover-grid {{
      align-items: start;
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
  <section class="page">
    <div class="subtle">{escape(meta['account_friendly_name'])}</div>
    <h1>{escape(report_title)}</h1>
    <div class="subtle">Период: {_display_date(meta['period_start'])} — {_display_date(meta['period_end'])} • Сформировано: {escape(_display_timestamp(meta['generated_at_utc']))}</div>
    <div class="hero-card hero-card--cover" style="margin-top: 12px;">
      <div class="subtle">Стоимость портфеля на конец периода</div>
      <div class="hero-value">{escape(_display_rub(summary.get('current_value'), precision=0))}</div>
      <div class="hero-summary-line">{escape(summary_subline)}</div>
    </div>
    <div class="two-col cover-grid">
      <div class="panel">
        <h3>Коротко о месяце</h3>
        {_render_bullet_list(narrative.get("executive_summary", []))}
      </div>
      <div class="panel">
        <h3>Факты месяца</h3>
        {_render_fact_grid(page_one_facts, columns=3)}
      </div>
    </div>
  </section>

  <section class="page">
    <h2>Динамика за месяц</h2>
    {_render_image_block("Стоимость портфеля и дневной результат", charts.get("performance"))}
    <div class="panel" style="margin-top: 12px;">
      <h3>Факты в цифрах</h3>
      {_render_fact_grid(page_two_facts, columns=3)}
    </div>
  </section>

  <section class="page">
    <h2>Структура на конец месяца</h2>
    <div class="two-col">
      {_render_image_block("Классы активов", charts.get("allocation"))}
      <div class="panel">
        <h3>Отклонение от таргетов</h3>
        {_render_rows_table(
          ["Класс", "Факт", "Цель", "Δ к цели", "Статус"],
          rebalance_rows,
          empty_label="Таргеты не настроены.",
          column_classes=["", "numeric", "numeric", "numeric", "status-cell"],
        )}
      </div>
    </div>
    <div class="panel" style="margin-top: 14px;">
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


def save_debug_report_html(html: str) -> str:
    handle = tempfile.NamedTemporaryFile(
        prefix="monthly_report_",
        suffix=".html",
        delete=False,
    )
    with open(handle.name, "w", encoding="utf-8") as file_obj:
        file_obj.write(html)
    return handle.name


def build_monthly_report_pdf_bytes(
    payload: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    charts: dict[str, str | None] | None = None,
    html: str | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
) -> bytes:
    html = html or build_monthly_report_html(
        payload,
        narrative=narrative,
        charts=charts,
    )
    if REPORT_DEBUG_SAVE_HTML:
        debug_path = save_debug_report_html(html)
        logger.info(
            "monthly_report_html_debug_saved",
            "Saved monthly report HTML to a debug file.",
            {
                "path": debug_path,
            },
        )

    if pdf_renderer is not None:
        return pdf_renderer(html)

    try:
        from weasyprint import HTML
    except Exception as exc:  # pragma: no cover - exercised in reporter runtime
        raise ReportRenderError("Не удалось импортировать WeasyPrint для PDF-рендера.") from exc

    try:
        pdf_bytes = HTML(string=html).write_pdf()
    except Exception as exc:  # pragma: no cover - exercised in reporter runtime
        raise ReportRenderError("Не удалось собрать PDF через WeasyPrint.") from exc

    logger.info(
        "monthly_report_pdf_built",
        "Built deterministic monthly report PDF bytes.",
        {
            "period": f"{payload['meta']['period_year']}-{payload['meta']['period_month']:02d}",
            "size_bytes": len(pdf_bytes),
        },
    )
    return pdf_bytes


def build_monthly_report_filename(payload: dict[str, Any]) -> str:
    return f"fintracker_monthly_{payload['meta']['period_year']}-{payload['meta']['period_month']:02d}.pdf"


def build_monthly_report_artifact(
    payload: dict[str, Any],
    *,
    narrative: dict[str, Any] | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
) -> dict[str, Any]:
    charts = build_monthly_report_charts(payload)
    resolved_narrative = narrative or build_deterministic_monthly_narrative(payload)
    html = build_monthly_report_html(payload, narrative=resolved_narrative, charts=charts)
    pdf_bytes = build_monthly_report_pdf_bytes(
        payload,
        narrative=resolved_narrative,
        charts=charts,
        html=html,
        pdf_renderer=pdf_renderer,
    )
    return {
        "schema_version": "monthly_report_artifact.v1",
        "payload": payload,
        "narrative": resolved_narrative,
        "charts": charts,
        "html": html,
        "pdf_bytes": pdf_bytes,
        "filename": build_monthly_report_filename(payload),
    }


def create_monthly_report_artifact(
    *,
    year: int | None = None,
    month: int | None = None,
    pdf_renderer: Callable[[str], bytes] | None = None,
) -> dict[str, Any]:
    payload = create_monthly_report_payload(year=year, month=month)
    return build_monthly_report_artifact(payload, pdf_renderer=pdf_renderer)
