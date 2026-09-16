from __future__ import annotations

import re
from decimal import Decimal
from html import escape
from typing import Any

from financetracker.reporting.formatting import (
    display_delta_pp as _display_delta_pp,
    display_pct_compact as _display_pct_compact,
    display_rub as _display_rub,
    to_decimal as _to_decimal,
)
from financetracker.reporting.report_charts import build_asset_class_breakdown as _build_asset_class_breakdown

_LATIN_WORD_RE = re.compile(r"[A-Za-z]{3,}")


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


def _render_fact_group(title: str, items: list[tuple[str, str]]) -> str:
    if not items:
        return ""
    return (
        '<div class="fact-pair-group">'
        f'<div class="fact-pair-title">{escape(title)}</div>'
        f'{_render_fact_grid(items, columns=2)}'
        "</div>"
    )


def _render_fact_stack(
    primary: str | None,
    *,
    secondary: str | None = None,
    muted: bool = False,
) -> str:
    if not primary:
        return _render_muted("—")

    secondary_html = ""
    if secondary:
        secondary_html = f'<div class="fact-meta">{escape(secondary)}</div>'

    primary_class = "fact-amount"
    if muted:
        primary_class = f"{primary_class} fact-amount--muted"

    primary_html = escape(primary).replace(" ₽", "&nbsp;₽")
    return secondary_html + f'<div class="{primary_class}">{primary_html}</div>'


def _share_text(count: int, total: int) -> str:
    if total <= 0:
        return f"{count} из {total} (0%)"
    share_pct = (Decimal(count) * Decimal("100") / Decimal(total)).quantize(Decimal("1"))
    return f"{count} из {total} ({share_pct}%)"


def _relevant_day_pnl_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return rows[1:] if len(rows) > 1 else rows


def _classify_day_pnl_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    relevant_rows = _relevant_day_pnl_rows(rows)
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


def _build_day_count_fact(count: int, total: int) -> tuple[str, str]:
    if total <= 0:
        return "0 из 0", "0% дней"
    share_pct = (Decimal(count) * Decimal("100") / Decimal(total)).quantize(Decimal("1"))
    return f"{count} из {total}", f"{str(share_pct).replace('.', ',')}% дней"


def _compute_average_day_pnl(rows: list[dict[str, Any]], *, positive: bool) -> Decimal | None:
    relevant_rows = _relevant_day_pnl_rows(rows)
    values = [
        _to_decimal(row.get("day_pnl"))
        for row in relevant_rows
        if (_to_decimal(row.get("day_pnl")) > 0 if positive else _to_decimal(row.get("day_pnl")) < 0)
    ]
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _build_plan_pace_fact(summary: dict[str, Any]) -> tuple[str, str]:
    deposits_ytd = _to_decimal(summary.get("deposits_ytd"))
    target_to_date = _to_decimal(summary.get("target_to_date"))
    if target_to_date > 0:
        pace_pct = deposits_ytd * Decimal("100") / target_to_date
        return _display_pct_compact(pace_pct, precision=1), f"цель к дате: {_display_rub(target_to_date, precision=0)}"

    plan_progress_pct = summary.get("plan_progress_pct")
    plan_total = _display_rub(summary.get("plan_annual_contrib"), precision=0)
    if plan_progress_pct not in (None, ""):
        return _display_pct_compact(plan_progress_pct, precision=1), f"из плана {plan_total}"

    return "—", f"из плана {plan_total}"


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


def _build_asset_class_summary_rows(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    breakdown = _build_asset_class_breakdown(positions)
    total_value = sum((row["value"] for row in breakdown), Decimal("0"))
    if total_value <= 0:
        return []

    rows: list[dict[str, Any]] = []
    for row in breakdown:
        share_pct = (row["value"] * Decimal("100") / total_value) if total_value else Decimal("0")
        rows.append(
            {
                "key": row["key"],
                "label": row["label"],
                "value": row["value"],
                "share_pct": share_pct,
            }
        )
    return rows


def _structure_color_modifier(asset_key: str) -> str:
    mapping = {
        "stocks": "stocks",
        "bonds": "bonds",
        "etf": "etf",
        "currency": "currency",
        "other": "other",
    }
    return mapping.get(asset_key, "other")


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


def _render_visual_chart(data_uri: str | None, *, alt: str, empty_label: str) -> str:
    if not data_uri:
        return f'<p class="empty">{escape(empty_label)}</p>'
    return f'<img class="chart structure-chart" src="{data_uri}" alt="{escape(alt)}">'


def _render_structure_summary(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">Нет данных по структуре.</p>'

    rendered_rows = []
    for row in rows:
        tone = _structure_color_modifier(str(row.get("key") or "other"))
        rendered_rows.append(
            '<div class="structure-line">'
            '<div class="structure-line-main">'
            f'<span class="structure-swatch structure-swatch--{tone}"></span>'
            f'<span class="structure-line-label">{escape(str(row.get("label") or "—"))}</span>'
            "</div>"
            f'<div class="structure-line-amount">{_render_num_cell(_display_rub(row.get("value"), precision=0))}</div>'
            f'<div class="structure-line-share">{_render_num_cell(_display_pct_compact(row.get("share_pct"), precision=1))}</div>'
            "</div>"
        )
    return '<div class="structure-ledger">' + "".join(rendered_rows) + "</div>"


def _render_target_drift(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="empty">Таргеты не настроены.</p>'

    rendered_rows: list[str] = []
    for row in rows:
        asset_class = str(row.get("asset_class") or "other")
        tone = _structure_color_modifier(asset_class)
        current_pct = _to_decimal(row.get("current_pct"))
        target_pct = _to_decimal(row.get("target_pct"))
        delta_pct = current_pct - target_pct
        current_clamped = max(Decimal("0"), min(Decimal("100"), current_pct))
        target_clamped = max(Decimal("0"), min(Decimal("100"), target_pct))
        status = str(row.get("status") or "").strip().lower()
        delta_class = "structure-delta--neutral"
        if status == "вне нормы":
            delta_class = "structure-delta--warn"
        elif delta_pct > 0:
            delta_class = "structure-delta--positive"
        elif delta_pct < 0:
            delta_class = "structure-delta--negative"

        rendered_rows.append(
            '<div class="target-row">'
            '<div class="target-row-top">'
            '<div class="target-row-label">'
            f'<span class="structure-swatch structure-swatch--{tone}"></span>'
            f'{escape(str(row.get("label") or "—"))}'
            "</div>"
            f'<div class="target-row-current">{_render_num_cell(_display_pct_compact(current_pct, precision=1))}</div>'
            f'<div class="target-row-target">цель {_display_pct_compact(target_pct, precision=1)}</div>'
            f'<div class="target-row-delta {delta_class}">{escape(_display_delta_pp(delta_pct, precision=1))}</div>'
            "</div>"
            '<div class="target-track">'
            f'<span class="target-fill target-fill--{tone}" style="width: {current_clamped:.2f}%"></span>'
            f'<span class="target-marker" style="left: {target_clamped:.2f}%"></span>'
            "</div>"
            "</div>"
        )
    return '<div class="target-drift-grid">' + "".join(rendered_rows) + "</div>"


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


def _render_chart_stage(title: str, data_uri: str | None, *, caption: str | None = None) -> str:
    if not data_uri:
        return (
            '<div class="chart-stage chart-stage--empty">'
            f'<div class="chart-stage-title">{escape(title)}</div>'
            '<p class="empty">Недостаточно данных для графика.</p>'
            "</div>"
        )

    caption_html = ""
    if caption:
        caption_html = f'<div class="chart-caption">{escape(caption)}</div>'

    return (
        '<div class="chart-stage">'
        f'<div class="chart-stage-title">{escape(title)}</div>'
        f'<img class="chart chart--stage" src="{data_uri}" alt="{escape(title)}">'
        f"{caption_html}"
        "</div>"
    )
