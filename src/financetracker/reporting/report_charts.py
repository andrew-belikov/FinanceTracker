"""Deterministic chart builders for the monthly report payload."""

from __future__ import annotations

import base64
import io
from datetime import date
from decimal import Decimal
from typing import Any

import matplotlib.pyplot as plt

from financetracker.common.logging_setup import get_logger
from financetracker.domain.rebalance import (
    REBALANCE_ASSET_CLASSES,
    REBALANCE_CLASS_LABELS,
    aggregate_rebalance_values_by_class,
)
from financetracker.reporting.chart_style import (
    CHART_COLORS,
    annotate_bar_values,
    annotate_series_last_point,
    apply_chart_style,
    build_date_ticks,
    rub_axis_formatter,
    set_chart_header,
    set_value_axis_limits,
)
from financetracker.reporting.formatting import display_rub, to_decimal


logger = get_logger(__name__)


def build_asset_class_breakdown(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    portfolio_values = [float(to_decimal(row.get("portfolio_value"))) for row in rows]
    day_pnl = [float(to_decimal(row.get("day_pnl"))) for row in rows]

    fig, (ax_value, ax_flow) = plt.subplots(
        2,
        1,
        figsize=(10.8, 6.35),
        sharex=True,
        gridspec_kw={"height_ratios": [2.2, 1.38]},
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
        f"{display_rub(rows[-1].get('portfolio_value'), precision=0)}",
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
            f"Пик {display_rub(rows[peak_index].get('portfolio_value'), precision=0)}",
            CHART_COLORS["positive"],
            x_offset=-12,
            y_offset=-16,
        )
    if trough_index != len(portfolio_values) - 1 and trough_index != peak_index:
        _annotate_point(
            ax_value,
            dates[trough_index],
            portfolio_values[trough_index],
            f"Минимум {display_rub(rows[trough_index].get('portfolio_value'), precision=0)}",
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
            f"Лучший день {display_rub(rows[best_day_index].get('day_pnl'), precision=0)}",
            CHART_COLORS["positive"],
            x_offset=-14,
            y_offset=16,
        )
    if worst_day_index != len(day_pnl) - 1 and worst_day_index != best_day_index:
        _annotate_point(
            ax_flow,
            dates[worst_day_index],
            day_pnl[worst_day_index],
            f"Просадка {display_rub(rows[worst_day_index].get('day_pnl'), precision=0)}",
            CHART_COLORS["negative"],
            x_offset=14,
            y_offset=-16,
        )

    tick_dates, tick_labels = build_date_ticks(dates, max_ticks=7)
    ax_flow.set_xticks(tick_dates)
    ax_flow.set_xticklabels(tick_labels)

    fig.tight_layout(rect=(0, 0.01, 1, 0.992), h_pad=1.0)
    return _chart_to_data_uri(fig)


def _build_allocation_chart(payload: dict[str, Any]) -> str | None:
    breakdown = build_asset_class_breakdown(payload.get("positions_current") or [])
    if not breakdown:
        return None

    values = [float(row["value"]) for row in breakdown]
    colors_by_key = {
        "stocks": CHART_COLORS["portfolio"],
        "bonds": CHART_COLORS["deposits"],
        "etf": CHART_COLORS["twr"],
        "currency": CHART_COLORS["positive"],
        "other": CHART_COLORS["neutral"],
    }
    colors = [colors_by_key.get(str(row["key"]), CHART_COLORS["neutral"]) for row in breakdown]

    fig, ax = plt.subplots(figsize=(4.8, 4.8))
    ax.set_facecolor("white")
    ax.pie(
        values,
        colors=colors,
        startangle=90,
        counterclock=False,
        wedgeprops={"width": 0.42, "edgecolor": "white", "linewidth": 2},
        autopct=lambda pct: f"{pct:.0f}%" if pct >= 6 else "",
        pctdistance=0.78,
        textprops={"fontsize": 8.6, "color": CHART_COLORS["text"]},
    )
    ax.text(0, 0, "Структура", ha="center", va="center", fontsize=11, color=CHART_COLORS["muted"])
    ax.set_aspect("equal")
    fig.tight_layout(pad=0.3)
    return _chart_to_data_uri(fig)


def _build_open_pl_chart(payload: dict[str, Any]) -> str | None:
    rows = payload.get("open_pl_end") or []
    if not rows:
        return None

    chart_rows = rows[:6]
    labels = [row.get("ticker") or row.get("name") or "—" for row in chart_rows]
    amounts = [float(to_decimal(row.get("amount"))) for row in chart_rows]
    x_values = list(range(len(chart_rows)))

    fig, ax = plt.subplots(figsize=(10.5, 4.8))
    set_chart_header(
        fig,
        "Открытый результат на конец месяца",
        "Позиции с наибольшим вкладом по открытому результату на конец месяца.",
    )
    apply_chart_style(ax, rub_axis_formatter)

    colors = [CHART_COLORS["positive"] if amount >= 0 else CHART_COLORS["negative"] for amount in amounts]
    ax.bar(x_values, amounts, width=0.62, color=colors, edgecolor="none", zorder=3)
    ax.axhline(0, color=CHART_COLORS["spine"], linewidth=1, zorder=1)
    ax.set_xticks(x_values)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Открытый результат")
    ax.margins(x=0.05, y=0.12)
    set_value_axis_limits(ax, amounts, min_padding_ratio=0.18, flat_padding_ratio=0.08)
    annotate_bar_values(ax, x_values, amounts, lambda value: display_rub(value, precision=0))
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
